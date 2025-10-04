#!/usr/bin/env python3
# zeropkg_deps.py
# Zeropkg - dependency resolver, graph, depclean, and builder integration
# Versão: completa, integrada e funcional

from __future__ import annotations
import os
import sys
import json
import time
import threading
import traceback
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Safe imports from project (with graceful fallback)
try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        return {
            "paths": {
                "recipes_dir": "/usr/ports",
                "cache_dir": "/var/cache/zeropkg",
                "log_dir": "/var/log/zeropkg"
            },
            "build": {"jobs": 4}
        }

try:
    # logger should expose get_logger or ZeropkgLogger class; support both
    from zeropkg_logger import get_logger, ZeropkgLogger
    try:
        logger = get_logger("deps")
    except Exception:
        zl = ZeropkgLogger()
        logger = zl.logger
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_deps")
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    def _log(level, msg):
        getattr(logger, level)(msg)
    logger.info = lambda m: _log("info", m)
    logger.debug = lambda m: _log("debug", m)
    logger.warning = lambda m: _log("warning", m)
    logger.error = lambda m: _log("error", m)

try:
    from zeropkg_toml import ZeropkgTOML, resolve_macros
except Exception:
    ZeropkgTOML = None
    def resolve_macros(x, env=None): return x

try:
    from zeropkg_db import list_installed_quick, get_orphaned_packages, find_revdeps
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False

try:
    from zeropkg_builder import Builder
    BUILDER_AVAILABLE = True
except Exception:
    BUILDER_AVAILABLE = False

# Constants & cache paths
CFG = load_config()
CACHE_DIR = Path(CFG.get("paths", {}).get("cache_dir", "/var/cache/zeropkg"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEPS_CACHE_FILE = CACHE_DIR / "deps-cache.json"
RECIPE_SCAN_DIR = Path(CFG.get("paths", {}).get("recipes_dir", "/usr/ports"))

_LOCK = threading.RLock()

# ---------------------------
# Utilities
# ---------------------------
def _save_cache(data: Dict[str, Any]):
    try:
        DEPS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(DEPS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "data": data}, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save deps cache: {e}")

def _load_cache() -> Dict[str, Any]:
    if not DEPS_CACHE_FILE.exists():
        return {}
    try:
        with open(DEPS_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj.get("data", {}) or {}
    except Exception as e:
        logger.warning(f"Failed to load deps cache: {e}")
        return {}

def _list_recipe_files(recipes_dir: Path) -> List[Path]:
    # Accept TOML files under recipes_dir; scan non-recursively and recursively
    out = []
    if not recipes_dir.exists():
        return out
    for p in recipes_dir.rglob("*.toml"):
        out.append(p)
    return out

def _read_recipe_minimal(path: Path) -> Optional[Dict[str,Any]]:
    # Minimal parse: try to extract package.name and dependencies quickly (avoid full TOML lib calls if not available)
    try:
        if ZeropkgTOML:
            parser = ZeropkgTOML()
            r = parser.load(path)
            name = r.get("package", {}).get("name")
            version = r.get("package", {}).get("version")
            deps = r.get("dependencies", []) or []
            return {"path": str(path), "name": name, "version": version, "dependencies": deps, "mtime": path.stat().st_mtime}
        else:
            # fallback naive parse: look for lines 'name' or 'dependencies'
            name = None
            version = None
            deps = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    l = line.strip()
                    if l.startswith("name") and "=" in l:
                        name = l.split("=",1)[1].strip().strip('"').strip("'")
                    if l.startswith("version") and "=" in l:
                        version = l.split("=",1)[1].strip().strip('"').strip("'")
                    if l.startswith("[[dependencies]]") or l.startswith("dependencies"):
                        # fallback skip complex parsing
                        pass
            return {"path": str(path), "name": name, "version": version, "dependencies": deps, "mtime": path.stat().st_mtime}
    except Exception as e:
        logger.warning(f"Failed to parse recipe {path}: {e}")
        return None

# ---------------------------
# Graph building & resolution
# ---------------------------
class DependencyGraph:
    """
    Directed graph representation: nodes are package names (string).
    edges: pkg -> dependency (i.e., pkg depends on dep)
    """
    def __init__(self):
        self.nodes: Set[str] = set()
        self.adj: Dict[str, Set[str]] = {}   # pkg -> set(deps)
        self.meta: Dict[str, Dict[str,Any]] = {}  # pkg -> metadata: version, path, recipe_mtime

    def add_node(self, pkg: str, version: Optional[str] = None, recipe_path: Optional[str] = None, recipe_mtime: Optional[float] = None):
        self.nodes.add(pkg)
        if pkg not in self.adj:
            self.adj[pkg] = set()
        self.meta.setdefault(pkg, {})
        if version:
            self.meta[pkg]["version"] = version
        if recipe_path:
            self.meta[pkg]["path"] = recipe_path
        if recipe_mtime:
            self.meta[pkg]["recipe_mtime"] = recipe_mtime

    def add_edge(self, pkg: str, dep: str):
        self.add_node(pkg)
        self.add_node(dep)
        self.adj.setdefault(pkg, set()).add(dep)

    def to_dict(self) -> Dict[str,Any]:
        return {"nodes": list(self.nodes), "edges": {k:list(v) for k,v in self.adj.items()}, "meta": self.meta}

    def reverse_graph(self) -> Dict[str, Set[str]]:
        rev = {n:set() for n in self.nodes}
        for a, deps in self.adj.items():
            for d in deps:
                rev.setdefault(d, set()).add(a)
        return rev

    def topological_sort(self) -> Tuple[bool, List[str], Optional[List[List[str]]]]:
        """
        Return (ok, ordered_list, groups)
        ok == False => cycle detected
        ordered_list is a topological order (if ok)
        groups is list of lists where each list can be built in parallel (level order)
        """
        # Kahn's algorithm
        indeg = {n:0 for n in self.nodes}
        for a,deps in self.adj.items():
            for d in deps:
                indeg[d] = indeg.get(d,0) + 1
        zero = [n for n,d in indeg.items() if d==0]
        order = []
        groups = []
        while zero:
            groups.append(sorted(zero))
            next_zero = []
            for n in zero:
                order.append(n)
                for m in self.adj.get(n, []):
                    indeg[m] -= 1
                    if indeg[m] == 0:
                        next_zero.append(m)
            zero = next_zero
        if len(order) != len(self.nodes):
            # cycle exists; detect nodes in cycle by indeg>0
            cycle_nodes = [n for n,d in indeg.items() if d>0]
            return False, order, [cycle_nodes]
        return True, order, groups

    def detect_cycles(self) -> List[List[str]]:
        ok, order, groups = self.topological_sort()
        if ok:
            return []
        return groups or []

# ---------------------------
# High level manager
# ---------------------------
class DepsManager:
    def __init__(self, recipes_dir: Optional[str] = None, config: Optional[Dict]=None):
        self.cfg = config or CFG
        self.recipes_dir = Path(recipes_dir or self.cfg.get("paths", {}).get("recipes_dir", "/usr/ports"))
        self.cache = _load_cache()
        self.graph = DependencyGraph()
        self._load_graph_from_cache_if_valid()

    # -------------------------
    # Scan recipes and build graph
    # -------------------------
    def scan_recipes(self, force: bool = False) -> DependencyGraph:
        """
        Scan recipe files and build dependency graph.
        Uses cache to speed up unless force=True.
        """
        with _LOCK:
            recipes = _list_recipe_files(self.recipes_dir)
            modified = False
            cached = self.cache.get("recipes", {})
            new_cache_recipes = {}
            for p in recipes:
                try:
                    info = _read_recipe_minimal(p)
                    if not info or not info.get("name"):
                        continue
                    name = info["name"]
                    mtime = info.get("mtime", 0)
                    cached_entry = cached.get(info["path"])
                    if cached_entry and cached_entry.get("mtime") == mtime and not force:
                        # reuse cached metadata
                        new_cache_recipes[info["path"]] = cached_entry
                        parsed_deps = cached_entry.get("dependencies", [])
                    else:
                        # parse fully using ZeropkgTOML if available
                        if ZeropkgTOML:
                            try:
                                parser = ZeropkgTOML()
                                rec = parser.load(p)
                                deps = rec.get("dependencies", []) or []
                                # normalize deps to names (strings or dicts)
                                parsed_deps = []
                                for d in deps:
                                    if isinstance(d, str):
                                        parsed_deps.append(d)
                                    elif isinstance(d, dict):
                                        parsed_deps.append(d.get("name"))
                                new_cache_recipes[str(p)] = {"name": name, "version": info.get("version"), "mtime": mtime, "dependencies": parsed_deps}
                            except Exception as e:
                                logger.warning(f"Parser failed for {p}: {e}")
                                parsed_deps = []
                        else:
                            parsed_deps = [d if isinstance(d, str) else d.get("name") for d in (info.get("dependencies") or [])]
                            new_cache_recipes[str(p)] = {"name": name, "version": info.get("version"), "mtime": mtime, "dependencies": parsed_deps}

                    # add to graph
                    self.graph.add_node(name, version=info.get("version"), recipe_path=str(p), recipe_mtime=mtime)
                    for dep in parsed_deps:
                        if not dep:
                            continue
                        depn = dep.split()[0] if isinstance(dep, str) else dep
                        self.graph.add_edge(name, depn)
                except Exception as e:
                    logger.warning(f"Error scanning {p}: {e}")
                    continue
            # update cache
            self.cache["recipes"] = new_cache_recipes
            _save_cache(self.cache)
            return self.graph

    # -------------------------
    # Resolve dependencies and return build order or groups
    # -------------------------
    def resolve(self, targets: Optional[List[str]] = None, include_optional: bool = False) -> Dict[str,Any]:
        """
        If targets provided, reduce graph to reachable nodes from targets.
        Returns dict with keys: ok, order, groups, cycles, missing (deps with no recipe)
        """
        with _LOCK:
            if not self.graph.nodes:
                self.scan_recipes()

            # if targets, compute reachable set
            if targets:
                reachable = set()
                stack = list(targets)
                while stack:
                    cur = stack.pop()
                    if cur in reachable:
                        continue
                    reachable.add(cur)
                    for d in self.graph.adj.get(cur, []):
                        if d not in reachable:
                            stack.append(d)
                # build subgraph
                sub = DependencyGraph()
                for n in reachable:
                    meta = self.graph.meta.get(n, {})
                    sub.add_node(n, version=meta.get("version"), recipe_path=meta.get("path"), recipe_mtime=meta.get("recipe_mtime"))
                    for d in self.graph.adj.get(n, []):
                        if d in reachable:
                            sub.add_edge(n,d)
                ok, order, groups = sub.topological_sort()
                cycles = [] if ok else sub.detect_cycles()
                # missing = nodes that appear as deps but have no recipe (we added nodes for them, but meta/path empty)
                missing = [n for n in sub.nodes if not sub.meta.get(n,{}).get("path")]
                return {"ok": ok, "order": order, "groups": groups, "cycles": cycles, "missing": missing}
            else:
                ok, order, groups = self.graph.topological_sort()
                cycles = [] if ok else self.graph.detect_cycles()
                missing = [n for n in self.graph.nodes if not self.graph.meta.get(n,{}).get("path")]
                return {"ok": ok, "order": order, "groups": groups, "cycles": cycles, "missing": missing}

    # -------------------------
    # Reverse dependencies
    # -------------------------
    def revdeps(self, pkg: str) -> List[str]:
        with _LOCK:
            if not self.graph.nodes:
                self.scan_recipes()
            rev = self.graph.reverse_graph()
            return sorted(list(rev.get(pkg, set())))

    # -------------------------
    # Build order integration with builder
    # -------------------------
    def resolve_and_build(self, targets: List[str], jobs: Optional[int] = None, dry_run: bool = False, continue_on_error: bool = False) -> Dict[str,Any]:
        """
        Resolve graph restricted to targets, then build in group levels using ThreadPoolExecutor.
        Each group contains packages that can be built in parallel.
        Tries to call Builder.build(package_name) for each package if builder available.
        Returns dict of results per package.
        """
        if not targets:
            raise ValueError("No targets provided for build")

        res = self.resolve(targets)
        if not res["ok"]:
            logger.error(f"Cycle detected in dependencies: {res['cycles']}")
            return {"ok": False, "error": "cycles", "cycles": res.get("cycles")}

        groups = res["groups"] or []
        jobs = jobs or int(self.cfg.get("build", {}).get("jobs", 4))
        results = {}
        builder = Builder() if BUILDER_AVAILABLE else None

        for level, group in enumerate(groups):
            logger.info(f"Building group {level+1}/{len(groups)}: {group}")
            if dry_run:
                for pkg in group:
                    results[pkg] = {"status": "dry-run"}
                continue

            # build in parallel within this group
            with ThreadPoolExecutor(max_workers=jobs) as ex:
                futures = {}
                for pkg in group:
                    if builder:
                        fut = ex.submit(self._build_with_builder, builder, pkg)
                    else:
                        fut = ex.submit(self._fake_build, pkg)
                    futures[fut] = pkg
                for fut in as_completed(futures):
                    pkg = futures[fut]
                    try:
                        out = fut.result()
                        results[pkg] = {"status": "ok", "detail": out}
                    except Exception as e:
                        logger.error(f"Build failed for {pkg}: {e}")
                        results[pkg] = {"status": "failed", "error": str(e)}
                        if not continue_on_error:
                            logger.error("Stopping builds due to error and continue_on_error=False")
                            return {"ok": False, "results": results}
        return {"ok": True, "results": results}

    def _build_with_builder(self, builder: Any, pkg: str) -> Any:
        try:
            # builder.build_package signature is not strictly enforced — attempt common ones
            if hasattr(builder, "build"):
                return builder.build(pkg)
            if hasattr(builder, "build_package"):
                return builder.build_package(pkg)
            if hasattr(builder, "build_and_install"):
                return builder.build_and_install(pkg)
            # fallback: call builder.main like interface
            if hasattr(builder, "main"):
                return builder.main(["build", pkg])
            raise RuntimeError("Builder available but has no known build entrypoint")
        except Exception as e:
            logger.error(f"Exception while building {pkg} with builder: {e}")
            raise

    def _fake_build(self, pkg: str) -> str:
        # placeholder action when builder not available
        logger.info(f"[fake-build] would build: {pkg}")
        return "fake-built"

    # -------------------------
    # Depclean / orphan removal
    # -------------------------
    def depclean_system(self, dry_run: bool = True, auto_confirm: bool = False) -> Dict[str,Any]:
        """
        Use DB to find orphaned packages and remove them (via zeropkg_remover if available)
        Returns dict with removed/missing/errors
        """
        if not DB_AVAILABLE:
            raise RuntimeError("Database (zeropkg_db) not available for depclean")

        orphans = get_orphaned_packages()
        logger.info(f"Orphan candidates: {orphans}")
        report = {"orphans": orphans, "removed": [], "skipped": [], "errors": []}
        if not orphans:
            return report

        # prompt if not auto_confirm and not dry_run
        if not auto_confirm and not dry_run:
            ans = input(f"Remove {len(orphans)} orphan packages? [y/N] ")
            if ans.strip().lower() not in ("y","yes"):
                logger.info("User aborted depclean")
                report["skipped"] = orphans
                return report

        # Attempt removal by calling zeropkg_remover module if present
        try:
            from zeropkg_remover import Remover
            remover = Remover()
        except Exception:
            remover = None

        for pkg in orphans:
            if dry_run:
                report["skipped"].append(pkg)
                continue
            try:
                if remover:
                    r = remover.remove(pkg)
                    report["removed"].append(pkg)
                else:
                    # fallback: remove DB entry only
                    from zeropkg_db import remove_package_quick
                    remove_package_quick(pkg)
                    report["removed"].append(pkg)
                logger.info(f"Removed orphan {pkg}")
            except Exception as e:
                report["errors"].append({"pkg": pkg, "error": str(e)})
        return report

    # -------------------------
    # Graph exporting
    # -------------------------
    def export_dot(self, out_path: str, include_versions: bool = True, highlight_installed: bool = True) -> str:
        """
        Export the current graph to Graphviz DOT format.
        """
        installed = set()
        if DB_AVAILABLE:
            try:
                inst = list_installed_quick()
                installed = set([f"{p['name']}" for p in inst])
            except Exception:
                installed = set()

        with open(out_path, "w", encoding="utf-8") as f:
            f.write("digraph zeropkg_deps {\n")
            f.write("  rankdir=LR;\n")
            for n in sorted(self.graph.nodes):
                label = n
                meta = self.graph.meta.get(n, {})
                if include_versions and meta.get("version"):
                    label = f"{n}\\n{meta.get('version')}"
                attrs = []
                if highlight_installed and n in installed:
                    attrs.append('style=filled')
                    attrs.append('fillcolor="#ccffcc"')
                f.write(f'  "{n}" [{",".join(attrs)} label="{label}"];\n')
            for a, deps in self.graph.adj.items():
                for d in deps:
                    f.write(f'  "{a}" -> "{d}";\n')
            f.write("}\n")
        logger.info(f"Graph exported to {out_path}")
        return out_path

    # -------------------------
    # Misc helpers
    # -------------------------
    def missing_dependencies(self) -> List[str]:
        """Return list of dependency names present in graph that have no recipe (no path)"""
        missing = [n for n in self.graph.nodes if not self.graph.meta.get(n,{}).get("path")]
        return missing

# ---------------------------
# CLI wrapper
# ---------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(description="Zeropkg dependency resolver and helper")
    parser.add_argument("--recipes", "-r", help="recipes dir (default from config)", default=None)
    parser.add_argument("--scan", action="store_true", help="scan recipes and build graph")
    parser.add_argument("--resolve", nargs="+", help="resolve dependencies for given targets")
    parser.add_argument("--build-order", nargs="+", help="print build order for targets")
    parser.add_argument("--build", nargs="+", help="resolve and build targets (uses builder)", default=None)
    parser.add_argument("--jobs", "-j", type=int, help="parallel jobs for build")
    parser.add_argument("--dry-run", action="store_true", help="dry-run for build/depclean")
    parser.add_argument("--depclean", action="store_true", help="run depclean using DB or dry-run")
    parser.add_argument("--graph-deps", help="export dependency graph to DOT file")
    parser.add_argument("--revdep", help="show reverse dependencies for package")
    parser.add_argument("--missing", action="store_true", help="list missing dependencies")
    parser.add_argument("--continue-on-error", action="store_true", help="continue builds on error")
    args = parser.parse_args()

    dm = DepsManager(recipes_dir=args.recipes)
    if args.scan:
        dm.scan_recipes(force=False)
        print(json.dumps(dm.graph.to_dict(), indent=2))
        return

    if args.resolve:
        out = dm.resolve(args.resolve)
        print(json.dumps(out, indent=2))
        return

    if args.build_order:
        out = dm.resolve(args.build_order)
        print(json.dumps({"order": out.get("order"), "groups": out.get("groups"), "cycles": out.get("cycles")}, indent=2))
        return

    if args.build:
        res = dm.resolve_and_build(args.build, jobs=args.jobs, dry_run=args.dry_run, continue_on_error=args.continue_on_error)
        print(json.dumps(res, indent=2))
        return

    if args.depclean:
        res = dm.depclean_system(dry_run=args.dry_run, auto_confirm=True)
        print(json.dumps(res, indent=2))
        return

    if args.graph_deps:
        path = args.graph_deps
        dm.scan_recipes()
        dm.export_dot(path)
        print(f"graph exported to {path}")
        return

    if args.revdep:
        print(json.dumps({"revdeps": dm.revdeps(args.revdep)}, indent=2))
        return

    if args.missing:
        dm.scan_recipes()
        print(json.dumps({"missing": dm.missing_dependencies()}, indent=2))
        return

    parser.print_help()

if __name__ == "__main__":
    _cli()
