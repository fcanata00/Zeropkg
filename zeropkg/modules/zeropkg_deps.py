#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_deps.py — Dependency graph manager and resolver for Zeropkg (enhanced)

Features implemented:
 - DependencyGraph with topological sort, cycle detection, reverse deps
 - Scan recipes (TOML/YAML) and build graph
 - Cache by combined hash of recipe files (fast invalidation)
 - Support for optional deps and alternatives:
     * In recipe, dependency can be string "pkgname", list ["pkgA","pkgB"] (interpreted as OR),
       or "pkgA || pkgB" textual alternative
 - Integration with zeropkg_vuln to check CVEs for dependencies (best-effort)
 - Export graph as DOT (Graphviz) and JSON
 - Impact analysis: list packages affected by change/removal
 - resolve_and_build() to call zeropkg_builder for the build sequence
 - depclean_system() to list orphaned packages and optionally remove them (dry-run)
 - Thread-safe caching and reasonable defaults

Usage (examples):
    dm = DepsManager()
    dm.scan_recipes("/usr/ports")
    order = dm.resolve(["gcc", "glibc"])
    dm.resolve_and_build(["gcc"], dry_run=True)
    dm.export_dot("/tmp/zeropkg_deps.dot")
"""

from __future__ import annotations
import os
import sys
import json
import time
import glob
import hashlib
import logging
import threading
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any, Iterable, Union
from collections import defaultdict, deque

# Optional internal imports from the Zeropkg project
try:
    from zeropkg_toml import load_recipe
    TOML_AVAILABLE = True
except Exception:
    TOML_AVAILABLE = False
    def load_recipe(path):
        # fallback minimal parser (best-effort)
        try:
            import tomllib
            with open(path, "rb") as f:
                return tomllib.load(f)
        except Exception:
            with open(path, "r", encoding="utf-8") as f:
                return {"package": {"name": Path(path).stem}}

try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        return {"paths": {"ports_dir": "/usr/ports", "cache_dir": "/var/cache/zeropkg"}, "deps": {"max_workers": 4}}

try:
    from zeropkg_logger import get_logger, perf_timer, log_event
    LOG_AVAILABLE = True
except Exception:
    LOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    def perf_timer(name, op):
        def deco(f):
            return f
        return deco
    def log_event(pkg, stage, msg, level="info", extra=None):
        logging.getLogger("zeropkg").info(f"{pkg}:{stage} - {msg}")

try:
    from zeropkg_db import ZeroPKGDB, _get_default_db
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False
    ZeroPKGDB = None
    _get_default_db = None

try:
    from zeropkg_vuln import ZeroPKGVulnManager
    VULN_AVAILABLE = True
except Exception:
    VULN_AVAILABLE = False
    ZeroPKGVulnManager = None

try:
    from zeropkg_builder import ZeropkgBuilder
    BUILDER_AVAILABLE = True
except Exception:
    BUILDER_AVAILABLE = False
    ZeropkgBuilder = None

# Module-wide config and logger
CFG = load_config()
PORTS_DIR = Path(CFG.get("paths", {}).get("ports_dir", "/usr/ports"))
CACHE_DIR = Path(CFG.get("paths", {}).get("cache_dir", "/var/cache/zeropkg"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEPS_CACHE_FILE = CACHE_DIR / "deps_graph_cache.json"
DEPS_HASH_FILE = CACHE_DIR / "deps_graph_hash.txt"

logger = get_logger("zeropkg.deps")
LOCK = threading.RLock()

# -------------------------
# Utilities
# -------------------------
def _sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def _file_list_hash(paths: Iterable[Path]) -> str:
    h = hashlib.sha1()
    for p in sorted(str(x) for x in paths):
        try:
            content_hash = _sha1_of_file(Path(p))
            h.update(str(p).encode("utf-8"))
            h.update(content_hash.encode("utf-8"))
        except Exception:
            # if unreadable, include path and mtime
            try:
                m = Path(p).stat().st_mtime
                h.update(f"{p}:{m}".encode("utf-8"))
            except Exception:
                h.update(str(p).encode("utf-8"))
    return h.hexdigest()

def _normalize_dep_entry(dep: Union[str, List[str], Dict[str,Any]]) -> List[List[str]]:
    """
    Normalize dependency specification to a list of alternatives lists.
    Examples:
      "pkg" -> [["pkg"]]
      "pkgA || pkgB" -> [["pkgA","pkgB"]]
      ["pkgA","pkgB"] -> [["pkgA","pkgB"]]  (treated as OR)
      {"name":"pkg","optional":True} -> [["pkg"]]
    Return: list of alternative-groups (each group a list of package names)
    """
    if isinstance(dep, dict):
        name = dep.get("name") or dep.get("pkg") or dep.get("package")
        if isinstance(name, str) and "||" in name:
            alts = [x.strip() for x in name.split("||")]
            return [alts]
        if isinstance(name, list):
            return [name]
        return [[name]]
    if isinstance(dep, list):
        # treat list as alternatives OR
        return [list(dep)]
    if isinstance(dep, str):
        if "||" in dep:
            parts = [x.strip() for x in dep.split("||")]
            return [parts]
        # single package
        return [[dep]]
    return []

# -------------------------
# Graph data structure
# -------------------------
class DependencyGraph:
    def __init__(self):
        # adjacency: node -> set of dependee names (edges node -> dependee)
        self.adj: Dict[str, Set[str]] = defaultdict(set)
        # reverse adjacency: dependee -> set(nodes that depend on it)
        self.rev: Dict[str, Set[str]] = defaultdict(set)
        # metadata per node
        self.meta: Dict[str, Dict[str,Any]] = {}
        # all nodes
        self.nodes: Set[str] = set()

    def add_node(self, name: str, meta: Optional[Dict[str,Any]] = None):
        if name not in self.nodes:
            self.nodes.add(name)
            self.adj.setdefault(name, set())
            self.rev.setdefault(name, set())
        if meta:
            self.meta.setdefault(name, {}).update(meta)

    def add_edge(self, pkg: str, dependee: str):
        self.add_node(pkg)
        self.add_node(dependee)
        if dependee not in self.adj[pkg]:
            self.adj[pkg].add(dependee)
            self.rev[dependee].add(pkg)

    def remove_node(self, name: str):
        if name not in self.nodes:
            return
        # remove edges
        for d in list(self.adj.get(name, [])):
            self.rev.get(d, set()).discard(name)
        for p in list(self.rev.get(name, [])):
            self.adj.get(p, set()).discard(name)
        self.adj.pop(name, None)
        self.rev.pop(name, None)
        self.nodes.discard(name)
        self.meta.pop(name, None)

    def out_edges(self, name: str) -> Set[str]:
        return set(self.adj.get(name, set()))

    def in_edges(self, name: str) -> Set[str]:
        return set(self.rev.get(name, set()))

    def topo_sort(self, subset: Optional[Set[str]] = None) -> Tuple[bool, List[str], Optional[List[List[str]]]]:
        """
        Topological sort of the graph or given subset.
        Returns (ok, order_list, levels) where levels is a list of lists (parallel build groups).
        If a cycle is found, ok=False and order_list contains nodes in partial order.
        """
        if subset is None:
            subset = set(self.nodes)
        indeg = {}
        for n in subset:
            indeg[n] = 0
        for n in subset:
            for m in self.adj.get(n, []):
                if m in subset:
                    indeg[m] = indeg.get(m, 0) + 1
        q = deque([n for n, d in indeg.items() if d == 0])
        order = []
        levels = []
        while q:
            level_size = len(q)
            level = []
            for _ in range(level_size):
                n = q.popleft()
                order.append(n)
                level.append(n)
                for m in self.adj.get(n, []):
                    if m in indeg:
                        indeg[m] -= 1
                        if indeg[m] == 0:
                            q.append(m)
            levels.append(level)
        if len(order) != len(subset):
            # cycle detected
            return False, order, levels
        return True, order, levels

    def find_cycles(self) -> List[List[str]]:
        """
        Detect simple cycles using DFS (Tarjan would be more complete; this is reasonable).
        Returns list of cycles (each cycle list of nodes).
        """
        visited = set()
        stack = []
        onstack = set()
        cycles = []
        def dfs(u):
            visited.add(u)
            stack.append(u)
            onstack.add(u)
            for v in self.adj.get(u, []):
                if v not in visited:
                    dfs(v)
                elif v in onstack:
                    # found cycle: nodes from v..end of stack
                    try:
                        idx = stack.index(v)
                        cycles.append(stack[idx:].copy())
                    except ValueError:
                        pass
            stack.pop()
            onstack.discard(u)
        for node in list(self.nodes):
            if node not in visited:
                dfs(node)
        return cycles

    def to_dot(self) -> str:
        lines = ["digraph deps {"]
        for n in sorted(self.nodes):
            label = n
            lines.append(f'  "{n}" [label="{label}"];')
        for a, targets in self.adj.items():
            for b in targets:
                lines.append(f'  "{a}" -> "{b}";')
        lines.append("}")
        return "\n".join(lines)

    def to_json(self) -> Dict[str,Any]:
        return {"nodes": list(sorted(self.nodes)), "edges": {n: sorted(list(self.adj.get(n, []))) for n in sorted(self.nodes)}, "meta": self.meta}

# -------------------------
# DepsManager
# -------------------------
class DepsManager:
    def __init__(self, ports_dir: Optional[Path] = None, cache_file: Optional[Path] = None):
        self.ports_dir = Path(ports_dir or PORTS_DIR)
        self.cache_file = Path(cache_file or DEPS_CACHE_FILE)
        self.hash_file = Path(DEPS_HASH_FILE)
        self.graph = DependencyGraph()
        self._recipes_index: Dict[str, Path] = {}  # pkg_name -> recipe_path
        self._cache_meta: Dict[str, Any] = {}
        self._vuln = ZeroPKGVulnManager() if VULN_AVAILABLE else None
        self._db = _get_default_db() if DB_AVAILABLE and _get_default_db else None
        self.max_workers = int(CFG.get("deps", {}).get("max_workers", CFG.get("deps", {}).get("max_workers", 4)))
        self.logger = logger

        # load cache if valid
        self._load_cache_if_valid()

    # -------------------------
    # Scanning and caching
    # -------------------------
    def _find_recipe_files(self) -> List[Path]:
        """Finds recipe files under ports_dir (toml or yaml)."""
        # typical layout: /usr/ports/*/*/*.toml or *.yaml
        res = []
        for ext in ("*.toml", "*.yaml", "*.yml"):
            res.extend(self.ports_dir.rglob(ext))
        return sorted(res)

    def _compute_sources_hash(self, file_list: Iterable[Path]) -> str:
        try:
            return _file_list_hash(file_list)
        except Exception:
            # fallback simple
            h = hashlib.sha1()
            for p in sorted(str(x) for x in file_list):
                try:
                    h.update(p.encode("utf-8"))
                    h.update(str(Path(p).stat().st_mtime).encode("utf-8"))
                except Exception:
                    h.update(p.encode("utf-8"))
            return h.hexdigest()

    def _load_cache_if_valid(self):
        """Load cache only if hash matches current recipe set hash."""
        try:
            recipe_files = self._find_recipe_files()
            current_hash = self._compute_sources_hash(recipe_files)
            if self.hash_file.exists():
                stored = self.hash_file.read_text().strip()
                if stored == current_hash and self.cache_file.exists():
                    try:
                        data = json.loads(self.cache_file.read_text(encoding="utf-8"))
                        self._restore_from_cache(data)
                        self.logger.debug("Deps cache loaded (valid)")
                        return
                    except Exception as e:
                        self.logger.debug(f"Failed to load deps cache: {e}")
            # else no valid cache
            self.logger.debug("No valid deps cache found")
        except Exception as e:
            self.logger.debug(f"Error in _load_cache_if_valid: {e}")

    def _restore_from_cache(self, data: Dict[str,Any]):
        """Restore graph and recipes index from cached JSON structure."""
        nodes = data.get("nodes", [])
        edges = data.get("edges", {})
        meta = data.get("meta", {})
        self.graph = DependencyGraph()
        for n in nodes:
            self.graph.add_node(n, meta.get(n))
        for a, targets in edges.items():
            for b in targets:
                self.graph.add_edge(a, b)
        self.graph.meta = meta
        self._recipes_index = data.get("recipes_index", {})

    def _save_cache(self, recipe_files: Iterable[Path]):
        """Persist graph and index plus current hash to disk."""
        try:
            nodes = list(sorted(self.graph.nodes))
            edges = {n: sorted(list(self.graph.adj.get(n, []))) for n in nodes}
            meta = self.graph.meta
            data = {"nodes": nodes, "edges": edges, "meta": meta, "recipes_index": self._recipes_index}
            tmp = self.cache_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
            tmp.replace(self.cache_file)
            # hash
            h = self._compute_sources_hash(recipe_files)
            self.hash_file.write_text(h, encoding="utf-8")
            self.logger.debug("Deps cache saved")
        except Exception as e:
            self.logger.warning(f"Failed to save deps cache: {e}")

    # -------------------------
    # Public: scan recipes and build graph
    # -------------------------
    @perf_timer("deps", "scan_recipes")
    def scan_recipes(self, ports_dir: Optional[Path] = None, force: bool = False) -> None:
        """
        Scan recipe files, parse dependencies and build dependency graph.
        If force=True, rebuild cache regardless of stored hash.
        """
        with LOCK:
            self.ports_dir = Path(ports_dir or self.ports_dir)
            recipe_files = self._find_recipe_files()
            if not recipe_files:
                self.logger.warning(f"No recipes found under {self.ports_dir}")
                return
            if not force:
                # if cache valid it was already loaded in ctor
                if self.graph.nodes:
                    self.logger.debug("Using loaded graph (no force rebuild)")
                    return

            # rebuild from scratch
            self.graph = DependencyGraph()
            self._recipes_index = {}
            for rf in recipe_files:
                try:
                    recipe = load_recipe(str(rf))
                    name = recipe.get("package", {}).get("name") or rf.stem
                    self._recipes_index[name] = str(rf)
                    self.graph.add_node(name, {"recipe": str(rf), "version": recipe.get("package", {}).get("version")})
                    # collect dependencies entries
                    deps_raw = recipe.get("dependencies") or recipe.get("depends") or []
                    # allow dictionary keyed dependencies in some recipes
                    if isinstance(deps_raw, dict):
                        # convert to list of dicts
                        deps_parsed = []
                        for k, v in deps_raw.items():
                            deps_parsed.append({"name": k, "req": v})
                        deps_raw = deps_parsed
                    for d in deps_raw:
                        alts = _normalize_dep_entry(d)
                        # each alt group means "this package depends on (one of) list"
                        # we will add edges to all alternatives to represent potential dependencies (conservative)
                        for group in alts:
                            for candidate in group:
                                if candidate:
                                    # normalize candidate name (strip version specifiers if any)
                                    # e.g. "libfoo>=1.2" -> "libfoo"
                                    cand = candidate.split()[0].split(">=")[0].split("==")[0].split("<=")[0].split("!=")[0]
                                    cand = cand.strip()
                                    if cand:
                                        self.graph.add_edge(name, cand)
                except Exception as e:
                    self.logger.debug(f"Failed to parse recipe {rf}: {e}")
            # save cache
            self._save_cache(recipe_files)

    # -------------------------
    # Resolve dependencies
    # -------------------------
    def resolve(self, pkgs: Iterable[str], include_optional: bool = True) -> Dict[str,Any]:
        """
        Resolve dependencies for given packages.
        Returns dict:
            { "ok": bool, "order": [pkg...], "levels": [[...],[...]], "cycles": [...], "missing": [...] }
        """
        with LOCK:
            requested = set(pkgs)
            # check existence in graph
            missing = [p for p in requested if p not in self.graph.nodes]
            if missing:
                self.logger.debug(f"Missing recipes for: {missing}")
            # build subset: all nodes reachable from requested (BFS)
            subset = set()
            q = deque()
            for p in requested:
                if p in self.graph.nodes:
                    q.append(p)
            while q:
                n = q.popleft()
                if n in subset:
                    continue
                subset.add(n)
                for dep in self.graph.out_edges(n):
                    if dep not in subset:
                        q.append(dep)
            # run topo sort
            ok, order, levels = self.graph.topo_sort(subset)
            cycles = []
            if not ok:
                cycles = self.graph.find_cycles()
            return {"ok": ok, "order": order, "levels": levels, "cycles": cycles, "missing": missing}

    # -------------------------
    # Resolve & build integration
    # -------------------------
    @perf_timer("deps", "resolve_and_build")
    def resolve_and_build(self, pkgs: Iterable[str], dry_run: bool = False, parallel_install: bool = False, builder_ctx: Optional[Any] = None, keep_going: bool = False) -> Dict[str,Any]:
        """
        Resolve dependencies and call builder to build packages in order.
        If builder not available, returns planned order.
        """
        with LOCK:
            res = self.resolve(pkgs)
            if not res["ok"]:
                self.logger.warning(f"Dependency resolution has cycles: {res['cycles']}")
                if not keep_going:
                    return {"ok": False, "reason": "cycles", "cycles": res["cycles"]}
            order = res["order"]
            # if builder not present, return planned sequence
            if not BUILDER_AVAILABLE:
                return {"ok": True, "dry_run": dry_run, "plan": order}

            builder = builder_ctx or ZeropkgBuilder()
            results = []
            # Build in topological order (order is from requested -> dependencies; we need to build dependencies first)
            # The topo order returned earlier lists nodes in a sequence where dependents appear before dependee in our implementation.
            # We want to invert so dependees first: simple reverse.
            build_sequence = list(reversed(order))
            for pkg in build_sequence:
                try:
                    self.logger.info(f"Building package: {pkg}")
                    if dry_run:
                        results.append({"pkg": pkg, "status": "planned"})
                        continue
                    # find recipe path
                    recipe_path = self._recipes_index.get(pkg)
                    if recipe_path:
                        out = builder.build(recipe_path, dry_run=dry_run)
                    else:
                        out = {"pkg": pkg, "status": "missing_recipe"}
                    results.append({"pkg": pkg, "result": out})
                except Exception as e:
                    self.logger.error(f"Build failed for {pkg}: {e}")
                    results.append({"pkg": pkg, "error": str(e)})
                    if not keep_going:
                        break
            return {"ok": True, "results": results, "plan": build_sequence}

    # -------------------------
    # Depclean: find and optionally remove orphaned packages
    # -------------------------
    def depclean_system(self, dry_run: bool = True, keep_essentials: Optional[Set[str]] = None) -> Dict[str,Any]:
        """
        Identify orphan packages (installed but not required by others).
        If dry_run is False and DB available, will remove them via zeropkg_remover if present.
        """
        with LOCK:
            if not DB_AVAILABLE or not self._db:
                self.logger.warning("DB not available; cannot perform depclean")
                return {"ok": False, "reason": "no_db"}
            keep_essentials = keep_essentials or set()
            installed = {r["name"] for r in self._db.list_installed_quick()}
            # compute all dependee names referenced in dependencies table
            referenced = set()
            # traverse graph edges: if a package is in graph and there is an edge from P->D, D is referenced
            for a in self.graph.nodes:
                for d in self.graph.out_edges(a):
                    referenced.add(d)
            # packages that are installed but never referenced are orphans (conservative)
            orphans = sorted([p for p in installed if p not in referenced and p not in keep_essentials])
            report = {"installed_count": len(installed), "orphans": orphans}
            if dry_run:
                return {"ok": True, "dry_run": True, "report": report}
            # attempt removal
            removed = []
            errors = []
            try:
                # try import remover module
                from zeropkg_remover import ZeropkgRemover
                remover = ZeropkgRemover()
            except Exception:
                remover = None
            for p in orphans:
                try:
                    if remover:
                        ok = remover.remove(p, dry_run=False)
                        if ok:
                            removed.append(p)
                        else:
                            errors.append({"pkg": p, "error": "removal-failed"})
                    else:
                        # fallback: remove entry from DB only
                        self._db.remove_package_quick(p)
                        removed.append(p + " (db-only)")
                except Exception as e:
                    errors.append({"pkg": p, "error": str(e)})
            report["removed"] = removed
            report["errors"] = errors
            return {"ok": True, "dry_run": False, "report": report}

    # -------------------------
    # Missing dependencies list (recipes not present)
    # -------------------------
    def missing_dependencies(self) -> List[str]:
        missing = []
        with LOCK:
            for a in sorted(self.graph.nodes):
                for d in self.graph.out_edges(a):
                    if d not in self.graph.nodes:
                        missing.append(d)
        return sorted(set(missing))

    # -------------------------
    # Export graph
    # -------------------------
    def export_dot(self, dest: Union[str, Path]) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dot = self.graph.to_dot()
        dest.write_text(dot, encoding="utf-8")
        return dest

    def export_json(self, dest: Union[str, Path]) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        data = self.graph.to_json()
        dest.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return dest

    # -------------------------
    # Impact analysis: which packages depend on given package (recursively)
    # -------------------------
    def impact_analysis(self, pkg_name: str) -> Dict[str,Any]:
        with LOCK:
            if pkg_name not in self.graph.nodes:
                return {"ok": False, "reason": "not_found", "pkg": pkg_name}
            impacted = set()
            q = deque([pkg_name])
            while q:
                cur = q.popleft()
                for rev in self.graph.in_edges(cur):
                    if rev not in impacted:
                        impacted.add(rev)
                        q.append(rev)
            return {"ok": True, "pkg": pkg_name, "impacted_count": len(impacted), "impacted": sorted(list(impacted))}

    # -------------------------
    # CVE check helper (best-effort)
    # -------------------------
    def check_vulns_for_list(self, pkgs: Iterable[str]) -> Dict[str,Any]:
        if not VULN_AVAILABLE or not self._vuln:
            return {"ok": False, "reason": "vuln_module_missing"}
        res = {}
        for p in pkgs:
            try:
                vulns = self._vuln.vulndb.get_vulns(p) if hasattr(self._vuln, "vulndb") else self._vuln.scan_package(p)
                res[p] = vulns
            except Exception as e:
                res[p] = {"error": str(e)}
        return {"ok": True, "results": res}

    # -------------------------
    # Utility: show planned build groups
    # -------------------------
    def build_plan(self, pkgs: Iterable[str]) -> Dict[str,Any]:
        res = self.resolve(pkgs)
        if not res["ok"]:
            return res
        # levels are groups where each group can be built in parallel
        return {"ok": True, "levels": res["levels"], "plan_len": len(res["order"])}

# -------------------------
# CLI for quick usage
# -------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-deps", description="Zeropkg dependency manager")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("scan", help="Scan recipes and rebuild dependency graph")
    rcmd = sub.add_parser("resolve", help="Resolve dependencies for packages")
    rcmd.add_argument("packages", nargs="+")
    rcmd.add_argument("--export-dot", help="Write DOT file")
    rcmd.add_argument("--export-json", help="Write JSON file")
    rcmd.add_argument("--build", action="store_true", help="Attempt to build using builder (if available)")
    sub.add_parser("missing", help="List missing dependencies (recipes not found)")
    depclean = sub.add_parser("depclean", help="Find orphan packages")
    depclean.add_argument("--apply", action="store_true", help="Remove orphans (requires DB/remover)")
    args = parser.parse_args()

    dm = DepsManager()
    if args.cmd == "scan":
        dm.scan_recipes(force=True)
        print("Scanned.")
    elif args.cmd == "resolve":
        out = dm.resolve(args.packages)
        print(json.dumps(out, indent=2))
        if args.export_dot:
            dm.export_dot(args.export_dot)
        if args.export_json:
            dm.export_json(args.export_json)
        if getattr(args, "build", False):
            res = dm.resolve_and_build(args.packages, dry_run=True)
            print(json.dumps(res, indent=2))
    elif args.cmd == "missing":
        print(json.dumps(dm.missing_dependencies(), indent=2))
    elif args.cmd == "depclean":
        res = dm.depclean_system(dry_run=not args.apply)
        print(json.dumps(res, indent=2))
    else:
        parser.print_help()

if __name__ == "__main__":
    _cli()
