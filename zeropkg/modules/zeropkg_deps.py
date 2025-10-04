#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_deps.py — Dependency graph, resolver and depclean utilities for Zeropkg
Pattern B: integrated, lean, functional.

Features:
- build graph scanning ports (TOML recipes)
- resolve dependencies with version constraint satisfaction (basic)
- topological sort and cycle detection
- reverse-deps and orphan detection
- depclean (dry-run supported)
- export/import JSON and DOT
- cache management (rebuild_cache/load_cache)
- integration with zeropkg_config, zeropkg_logger and zeropkg_db (optional)
"""

from __future__ import annotations
import os
import sys
import json
import time
import traceback
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set, Any, Iterable

# Optional integrations
try:
    from zeropkg_config import load_config, get_ports_dirs
except Exception:
    def load_config(*a, **k):
        return {"paths": {"state_dir": "/var/lib/zeropkg", "ports_dir": "/usr/ports"}}
    def get_ports_dirs(cfg=None):
        cfg = cfg or {}
        pd = cfg.get("paths", {}).get("ports_dir", "/usr/ports")
        return [pd]

try:
    from zeropkg_logger import log_event, log_global, get_logger
    _logger = get_logger("deps")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_deps")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(msg)

try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# We'll lazy-import the TOML parser utilities from zeropkg_toml when needed
# State files
DEFAULT_STATE_DIR = Path(load_config().get("paths", {}).get("state_dir", "/var/lib/zeropkg"))
CACHE_PATH = DEFAULT_STATE_DIR / "deps_cache.json"

# Basic version parsing utilities (simple semver-ish)
def parse_version(ver: Optional[str]) -> Tuple:
    if not ver:
        return ()
    ver = str(ver).strip()
    # remove leading 'v'
    if ver.startswith("v"):
        ver = ver[1:]
    parts = []
    for p in ver.split("."):
        try:
            parts.append(int(p))
        except Exception:
            # keep non-numeric as string chunk to allow some ordering
            parts.append(p)
    return tuple(parts)

def cmp_versions(a: Optional[str], b: Optional[str]) -> int:
    """Return -1 if a<b, 0 if equal, 1 if a>b, unknown treated lexicographically."""
    pa = parse_version(a)
    pb = parse_version(b)
    if pa == pb:
        return 0
    # compare elementwise
    for xa, xb in zip(pa, pb):
        if isinstance(xa, int) and isinstance(xb, int):
            if xa < xb:
                return -1
            if xa > xb:
                return 1
        else:
            sa, sb = str(xa), str(xb)
            if sa < sb:
                return -1
            if sa > sb:
                return 1
    # fallback to length
    if len(pa) < len(pb):
        return -1
    if len(pa) > len(pb):
        return 1
    return 0

# Simple constraint matching: supports >=, <=, ==, >, <, ~= (compatible)
def match_constraint(version: Optional[str], constraint: Optional[str]) -> bool:
    if not constraint:
        return True
    if not version:
        return False
    c = str(constraint).strip()
    ops = [">=", "<=", "==", ">", "<", "~="]
    for op in ops:
        if c.startswith(op):
            req = c[len(op):].strip()
            cmpv = cmp_versions(version, req)
            if op == ">=":
                return cmpv >= 0
            if op == "<=":
                return cmpv <= 0
            if op == "==":
                return cmpv == 0
            if op == ">":
                return cmpv > 0
            if op == "<":
                return cmpv < 0
            if op == "~=":
                # compatible: same major and >= req
                pv = parse_version(version)
                pr = parse_version(req)
                if not pv or not pr:
                    return version == req
                return pv[0] == pr[0] and cmp_versions(version, req) >= 0
    # fallback: bare version equals
    return version == c

# Graph structure
# nodes: package name (string); store version and meta
# edges: pkg -> dependency_name
class DepGraph:
    def __init__(self):
        # nodes mapped to metadata dict (including 'version' if known)
        self.nodes: Dict[str, Dict[str, Any]] = {}
        # adjacency list: node -> set(dependency_names)
        self.edges: Dict[str, Set[str]] = {}
        # reverse adjacency for quick revdeps
        self.rev_edges: Dict[str, Set[str]] = {}

    def add_node(self, name: str, meta: Optional[Dict[str,Any]] = None):
        if name not in self.nodes:
            self.nodes[name] = meta or {}
        else:
            # merge metadata shallowly
            if meta:
                self.nodes[name].update(meta)
        self.edges.setdefault(name, set())
        self.rev_edges.setdefault(name, set())

    def add_edge(self, src: str, dst: str):
        self.add_node(src)
        self.add_node(dst)
        self.edges.setdefault(src, set()).add(dst)
        self.rev_edges.setdefault(dst, set()).add(src)

    def remove_node(self, name: str):
        if name in self.nodes:
            # remove outgoing edges
            for dep in list(self.edges.get(name, [])):
                self.rev_edges.get(dep, set()).discard(name)
            # remove incoming edges
            for src in list(self.rev_edges.get(name, [])):
                self.edges.get(src, set()).discard(name)
            self.edges.pop(name, None)
            self.rev_edges.pop(name, None)
            self.nodes.pop(name, None)

    def get_deps(self, name: str) -> List[str]:
        return sorted(self.edges.get(name, set()))

    def get_revdeps(self, name: str) -> List[str]:
        return sorted(self.rev_edges.get(name, set()))

    def to_dict(self) -> Dict[str, Any]:
        return {"nodes": self.nodes, "edges": {k: list(v) for k,v in self.edges.items()}}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DepGraph":
        g = cls()
        for n, meta in (data.get("nodes") or {}).items():
            g.add_node(n, meta)
        for src, dsts in (data.get("edges") or {}).items():
            for d in dsts:
                g.add_edge(src, d)
        return g

# Utilities scanning ports for metadata
def _collect_ports_meta(cfg: Optional[Dict[str,Any]] = None) -> Dict[str, Dict[str,Any]]:
    """
    Walk ports directories and load toml metas.
    Returns mapping: package_name -> meta (as dict).
    """
    cfg = cfg or load_config()
    ports_dirs = get_ports_dirs(cfg)
    out: Dict[str, Dict[str,Any]] = {}
    for pd in ports_dirs:
        pd = str(pd)
        if not os.path.isdir(pd):
            continue
        for root, dirs, files in os.walk(pd):
            for fn in files:
                if fn.endswith(".toml"):
                    full = os.path.join(root, fn)
                    try:
                        # lazy import to avoid hard dependency if module not present yet
                        from zeropkg_toml import load_toml, get_package_meta
                        meta = load_toml(full)
                        name, version = get_package_meta(meta)
                        if name:
                            out[name] = meta
                    except Exception:
                        # skip invalid toml but log at debug
                        log_global(f"Skipping invalid toml {full}", "debug")
                        continue
    return out

# Build graph from recipe metadata
def build_graph_from_ports(cfg: Optional[Dict[str,Any]] = None, save_cache: bool = True) -> DepGraph:
    cfg = cfg or load_config()
    DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
    metas = _collect_ports_meta(cfg)
    graph = DepGraph()
    for name, meta in metas.items():
        pkg = meta.get("package", {}) or {}
        version = pkg.get("version")
        graph.add_node(name, {"version": version, "meta": meta})
    # add edges
    for name, meta in metas.items():
        deps = meta.get("dependencies") or []
        for d in deps:
            # d expected either {"name":..., "version_req": ...} or string
            if isinstance(d, dict):
                dep_name = d.get("name") or d.get("dep_name")
            else:
                dep_name = str(d)
            if dep_name:
                graph.add_edge(name, dep_name)
    # optionally save cache
    if save_cache:
        try:
            save_cache(graph)
            log_global(f"Deps graph cached to {CACHE_PATH}", "debug")
        except Exception:
            log_global("Failed to save deps cache", "warning")
    return graph

def save_cache(graph: DepGraph, path: Optional[Path]=None):
    path = path or CACHE_PATH
    DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
    data = graph.to_dict()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log_global(f"Saved deps cache to {path}", "debug")

def load_cache(path: Optional[Path]=None) -> DepGraph:
    path = path or CACHE_PATH
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    g = DepGraph.from_dict(data)
    return g

# Resolve dependencies: topological sort for a set of root packages
def topo_sort_subgraph(graph: DepGraph, roots: Iterable[str]) -> Tuple[List[str], List[List[str]]]:
    """
    Returns (ordered_list, cycles_list). cycles_list is non-empty if cycles detected.
    Standard Kahn's algorithm restricted to subgraph reachable from roots.
    """
    # compute reachable set via DFS
    reachable = set()
    def dfs(n):
        if n in reachable:
            return
        reachable.add(n)
        for d in graph.edges.get(n, []):
            dfs(d)
    for r in roots:
        if r in graph.nodes:
            dfs(r)
    # build indegree for reachable nodes
    indeg = {}
    for n in reachable:
        indeg[n] = 0
    for n in reachable:
        for d in graph.edges.get(n, []):
            if d in indeg:
                indeg[d] += 1
    # Kahn
    queue = [n for n,k in indeg.items() if k==0]
    order = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for m in list(graph.edges.get(node, [])):
            if m in indeg:
                indeg[m] -= 1
                if indeg[m] == 0:
                    queue.append(m)
    # if some nodes not in order -> cycles
    cycles = []
    remaining = [n for n in indeg.keys() if n not in order]
    if remaining:
        # naive cycle detection: try to produce cycles via DFS stack
        visited = set()
        stack = []
        def visit(node):
            if node in stack:
                idx = stack.index(node)
                cycles.append(stack[idx:] + [node])
                return
            if node in visited:
                return
            visited.add(node)
            stack.append(node)
            for child in graph.edges.get(node, []):
                if child in indeg:
                    visit(child)
            stack.pop()
        for n in remaining:
            visit(n)
    return order, cycles

def resolve_install_order(graph: DepGraph, targets: Iterable[str], require_constraints: bool = False) -> Dict[str, Any]:
    """
    Given a graph and target package names, returns:
        { "order": [pkg...], "cycles": [...], "missing": [...], "unsat_constraints": [...] }
    If require_constraints True, will attempt to enforce version constraints present in recipe dependencies.
    """
    missing = []
    for t in targets:
        if t not in graph.nodes:
            missing.append(t)
    if missing:
        return {"order": [], "cycles": [], "missing": missing, "unsat_constraints": []}
    order, cycles = topo_sort_subgraph(graph, targets)
    unsat = []
    if require_constraints:
        # check each edge for version requirements if present in source meta
        for src, deps in graph.edges.items():
            src_meta = graph.nodes.get(src, {}).get("meta", {}) or {}
            dep_entries = src_meta.get("dependencies", []) or []
            # map by name for declared constraints
            for d in dep_entries:
                if isinstance(d, dict):
                    dep_name = d.get("name") or d.get("dep_name")
                    ver_req = d.get("version_req") or d.get("dep_version_req") or d.get("version")
                else:
                    # string possibly with operator inlined; skip heavy parsing here
                    dep_name = str(d)
                    ver_req = None
                if not dep_name:
                    continue
                # if graph has node and version, test
                dep_node = graph.nodes.get(dep_name)
                if dep_node:
                    dep_ver = dep_node.get("version")
                    if ver_req and not match_constraint(dep_ver, ver_req):
                        unsat.append({"package": src, "dep": dep_name, "required": ver_req, "found": dep_ver})
    return {"order": order, "cycles": cycles, "missing": missing, "unsat_constraints": unsat}

# Reverse deps
def find_revdeps(graph: DepGraph, package_name: str, deep: bool = True) -> List[str]:
    """
    Return list of packages depending on `package_name`.
    If deep True, returns entire transitive reverse deps.
    """
    if package_name not in graph.nodes:
        return []
    res = set()
    stack = [package_name]
    while stack:
        n = stack.pop()
        for r in graph.rev_edges.get(n, []):
            if r not in res:
                res.add(r)
                if deep:
                    stack.append(r)
    return sorted(res)

# Orphan detection: packages with zero reverse deps and not "manual"
def find_orphans(graph: DepGraph, exclude_manual: bool = True) -> List[str]:
    orphans = []
    for pkg in graph.nodes.keys():
        revs = graph.rev_edges.get(pkg, set())
        if not revs:
            meta = graph.nodes.get(pkg, {}).get("meta") or {}
            try:
                metadata = meta.get("package") or {}
            except Exception:
                metadata = {}
            # consider manual flag in metadata.install or metadata.package
            manual = False
            mblock = meta.get("metadata") or meta.get("install") or {}
            if isinstance(mblock, dict) and mblock.get("manual"):
                manual = True
            if exclude_manual and manual:
                continue
            orphans.append(pkg)
    return sorted(orphans)

# Depclean: remove orphan packages (integrates with DB and logs)
def depclean(graph: DepGraph, dry_run: bool = True, remove_callback: Optional[Any] = None) -> Dict[str,Any]:
    """
    Identify and optionally remove orphan packages.
    remove_callback(package_name) will be called to perform removal (installer/remover).
    Returns a report dict.
    """
    orphans = find_orphans(graph)
    report = {"orphans": orphans, "removed": [], "skipped": []}
    for pkg in orphans:
        if dry_run:
            log_event(pkg, "depclean", "Orphan candidate (dry-run)")
            report["skipped"].append(pkg)
        else:
            try:
                if remove_callback:
                    remove_callback(pkg)
                else:
                    # best-effort: log and remove node from graph
                    log_event(pkg, "depclean", "Removing orphan package")
                graph.remove_node(pkg)
                report["removed"].append(pkg)
                # record DB event
                if DBManager:
                    try:
                        with DBManager() as db:
                            db._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                                        (pkg, "depclean.remove", json.dumps({"pkg": pkg}), int(time.time())))
                    except Exception:
                        pass
            except Exception as e:
                report.setdefault("errors", []).append({pkg: str(e)})
    return report

# Export to DOT
def export_to_dot(graph: DepGraph, outpath: str):
    lines = ["digraph zeropkg_deps {"]
    for n, meta in graph.nodes.items():
        label = n
        ver = meta.get("version")
        if ver:
            label = f"{n}\\n{ver}"
        lines.append(f'  "{n}" [label="{label}"];')
    for src, dsts in graph.edges.items():
        for d in dsts:
            lines.append(f'  "{src}" -> "{d}";')
    lines.append("}")
    Path(outpath).write_text("\n".join(lines), encoding="utf-8")
    log_global(f"Exported deps graph to {outpath}", "info")

# Utility: rebuild cache
def rebuild_cache(cfg: Optional[Dict[str,Any]] = None, save: bool = True) -> DepGraph:
    cfg = cfg or load_config()
    graph = build_graph_from_ports(cfg, save_cache=save)
    log_global("Rebuilt dependency cache", "info")
    return graph

# Check missing packages referenced in graph but not present
def find_missing_nodes(graph: DepGraph) -> List[str]:
    missing = []
    for src, deps in graph.edges.items():
        for d in deps:
            if d not in graph.nodes:
                missing.append(d)
    return sorted(set(missing))

# Convenience wrappers for typical CLI / integration usage
def ensure_graph_loaded(cache_path: Optional[str]=None) -> DepGraph:
    try:
        return load_cache(Path(cache_path) if cache_path else None)
    except Exception:
        return rebuild_cache()

# CLI entrypoint
def _cli():
    import argparse, pprint
    p = argparse.ArgumentParser(prog="zeropkg-deps", description="Zeropkg dependency utilities")
    p.add_argument("--rebuild-cache", action="store_true", help="Rebuild deps cache scanning ports")
    p.add_argument("--graph-deps", help="Export graph to DOT file", metavar="OUT")
    p.add_argument("--resolve", nargs="+", help="Resolve install order for listed packages")
    p.add_argument("--depclean", action="store_true", help="Find orphan packages (dry-run by default)")
    p.add_argument("--do-depclean", action="store_true", help="Actually remove orphans (calls callback if provided)")
    p.add_argument("--cache", help="Override cache path")
    args = p.parse_args()

    graph = None
    if args.rebuild_cache:
        graph = rebuild_cache()
    else:
        try:
            graph = ensure_graph_loaded(args.cache)
        except Exception as e:
            print("Failed to load or build cache:", e)
            sys.exit(1)

    if args.graph_deps:
        export_to_dot(graph, args.graph_deps)
        print("DOT exported to", args.graph_deps)
    if args.resolve:
        res = resolve_install_order(graph, args.resolve, require_constraints=True)
        pprint.pprint(res)
    if args.depclean:
        rpt = depclean(graph, dry_run=True)
        print("Orphans (dry-run):", rpt["orphans"])
    if args.do_depclean:
        # best-effort removal: here we just remove from graph (no system uninstall)
        rpt = depclean(graph, dry_run=False, remove_callback=lambda pkg: log_event(pkg, "depclean", "removed by CLI"))
        print("Depclean result:", rpt)

if __name__ == "__main__":
    _cli()
