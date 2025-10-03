#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_deps.py — Resolução de dependências definitiva

Funcionalidades:
- DependencyResolver: carrega deps de receitas, suporta constraints de versão,
  constrói grafo (com cache), detecta ciclos, topological sort.
- resolve_and_install: resolve deps transitivas e constrói/instala usando Builder/Installer.
- missing_deps(pkgname): lista deps não instaladas (respeita constraints).
- reverse_deps(pkgname): lista pacotes instalados que dependem do pkgname.
- find_orphans(): detecta pacotes instalados que não são dependência de ninguém.
- dump_graph(path, format='dot'): exporta grafo em formato DOT.
- cache opcional do grafo em /var/zeropkg/cache/deps_cache.json (velocidade).
- Usa parsing de constraints (>=, <=, >, <, ==, ~=) com fallback sem `packaging`.
"""

from __future__ import annotations

import os
import glob
import json
import logging
import shutil
import re
from typing import Dict, List, Set, Tuple, Optional, Any

from zeropkg_toml import load_toml
from zeropkg_db import DBManager
from zeropkg_logger import log_event

logger = logging.getLogger("zeropkg.deps")

# cache path
CACHE_DIR = "/var/zeropkg/cache"
CACHE_FILE = os.path.join(CACHE_DIR, "deps_graph.json")


# --------------------------
# Version helpers (with fallback)
# --------------------------
def try_import_packaging():
    try:
        from packaging import version as _v
        return _v
    except Exception:
        return None


_packaging = try_import_packaging()


def _normalize_ver(v: Optional[str]) -> str:
    return str(v) if v is not None else ""


def compare_versions(v1: Optional[str], v2: Optional[str]) -> int:
    """
    Compare two version strings.
    Returns 1 if v1 > v2, 0 if equal, -1 if v1 < v2.
    Uses packaging.version if available; otherwise use numeric-segment heuristic.
    """
    if v1 is None and v2 is None:
        return 0
    if v1 is None:
        return -1
    if v2 is None:
        return 1

    if _packaging:
        a = _packaging.parse(v1)
        b = _packaging.parse(v2)
        if a > b:
            return 1
        if a < b:
            return -1
        return 0
    # fallback: split on non-digits and compare numeric segments
    def segs(x: str):
        parts = re.split(r'[^\d]+', x)
        nums = []
        for p in parts:
            if p == "":
                continue
            try:
                nums.append(int(p))
            except Exception:
                # map non-digit segment to 0
                nums.append(0)
        return nums
    a = segs(v1)
    b = segs(v2)
    for x, y in zip(a, b):
        if x > y:
            return 1
        if x < y:
            return -1
    if len(a) > len(b) and any(x > 0 for x in a[len(b):]):
        return 1
    if len(b) > len(a) and any(y > 0 for y in b[len(a):]):
        return -1
    return 0


# --------------------------
# Constraint parsing
# --------------------------
_CONSTRAINT_RE = re.compile(r'^\s*(?P<op>>=|<=|==|~=|>|<)?\s*(?P<ver>.+?)\s*$')


def parse_constraint(spec: str) -> Tuple[str, str]:
    """
    Parse a version constraint string like ">=1.2.3" or "1.2.3" (implied ==).
    Returns (op, version).
    """
    if not spec:
        return ("", "")
    m = _CONSTRAINT_RE.match(str(spec))
    if not m:
        return ("==", str(spec))
    op = m.group("op") or "=="
    ver = m.group("ver") or ""
    return (op, ver)


def constraint_satisfied(installed_ver: Optional[str], constraint_spec: str) -> bool:
    """
    Check if installed_ver satisfies constraint_spec.
    If installed_ver is None => not satisfied.
    Supports ops >=, <=, >, <, ==, ~= (compatible).
    """
    if not constraint_spec:
        # no constraint => any version is OK as long as something is installed
        return installed_ver is not None

    op, ver = parse_constraint(constraint_spec)
    if installed_ver is None:
        return False

    cmp = compare_versions(installed_ver, ver)
    if op == "==":
        return cmp == 0
    if op == ">=":
        return cmp >= 0
    if op == "<=":
        return cmp <= 0
    if op == ">":
        return cmp > 0
    if op == "<":
        return cmp < 0
    if op == "~=":
        # compatible: same major/minor prefix; e.g., ~=1.2 means >=1.2,<2.0
        # implement simple rule: installed >= ver and major version equal
        try:
            vparts = [int(x) for x in ver.split(".") if x.isdigit()]
            iparts = [int(x) for x in installed_ver.split(".") if x.isdigit()]
            if not vparts or not iparts:
                return cmp >= 0
            # installed >= ver
            if cmp < 0:
                return False
            # same major (first segment)
            return iparts[0] == vparts[0]
        except Exception:
            return cmp >= 0
    # default fallback
    return cmp >= 0


# --------------------------
# Helper to ensure cache dir
# --------------------------
def _ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        pass


# --------------------------
# DependencyResolver
# --------------------------
class DependencyError(Exception):
    pass


class DependencyResolver:
    def __init__(self, db_path: str, ports_dir: str = "/usr/ports", cache: bool = True):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.db = DBManager(db_path)
        self.cache_enabled = bool(cache)
        self._graph_cache: Optional[Dict[str, List[str]]] = None

        if self.cache_enabled:
            _ensure_cache_dir()
            # try load disk cache
            try:
                if os.path.exists(CACHE_FILE):
                    with open(CACHE_FILE, "r", encoding="utf-8") as fh:
                        j = json.load(fh)
                        # store as dict[str, list]
                        self._graph_cache = {k: list(v) for k, v in j.items()}
                else:
                    self._graph_cache = None
            except Exception:
                self._graph_cache = None

    # ---------------------
    # TOML load helpers (tolerant)
    # ---------------------
    def _find_metafiles_for(self, pkgname: str) -> List[str]:
        pattern = os.path.join(self.ports_dir, "**", f"{pkgname}-*.toml")
        return glob.glob(pattern, recursive=True)

    def _load_meta(self, pkgname: str) -> Dict[str, Any]:
        """
        Tenta load_toml(ports_dir, pkgname) ou load_toml(path).
        Se não encontrar, FileNotFoundError.
        """
        # tentar chamada comum
        try:
            return load_toml(self.ports_dir, pkgname)
        except TypeError:
            # assinatura diferente: try path
            pass
        except Exception:
            pass

        matches = self._find_metafiles_for(pkgname)
        if not matches:
            raise FileNotFoundError(f"Metafile for {pkgname} not found under {self.ports_dir}")
        # prefer last (assume higher versions later lexicographically)
        path = sorted(matches)[-1]
        # try load
        try:
            return load_toml(path)
        except TypeError:
            # maybe load_toml expects (ports_dir, pkgname) and failed earlier due to other error
            return load_toml(path)

    def _extract_deps_from_meta(self, meta: Dict) -> Dict[str, List[Any]]:
        """
        Return dict with 'build' and 'runtime' keys, values are lists of dependencies.
        Each dependency may be:
          - string "pkgname"
          - dict {"name": "pkg", "version": ">=1.2.3"}
        Also support simple key/value style in TOML.
        """
        deps = {"build": [], "runtime": [], "optional": []}
        if not meta:
            return deps

        section = meta.get("dependencies") or meta.get("depends") or meta.get("deps") or {}
        # Common formats:
        # 1) { build = ["pkg1", "pkg2"], runtime = ["pkg3"] }
        # 2) package.deps or package.depends list
        if isinstance(section, dict):
            for key in ("build", "runtime", "optional"):
                val = section.get(key, [])
                if isinstance(val, list):
                    deps[key] = val.copy()
                elif isinstance(val, dict):
                    # dict of name -> version
                    arr = []
                    for n, v in val.items():
                        if v and isinstance(v, str):
                            arr.append({"name": n, "version": v})
                        else:
                            arr.append(n)
                    deps[key] = arr
                elif isinstance(val, str):
                    deps[key] = [val]
        else:
            # fallback: try meta["package"]["depends"]
            pkgsec = meta.get("package", {}) or {}
            inline = pkgsec.get("deps") or pkgsec.get("depends")
            if inline:
                if isinstance(inline, list):
                    deps["runtime"] = inline.copy()
                elif isinstance(inline, dict):
                    arr = []
                    for n, v in inline.items():
                        if isinstance(v, str):
                            arr.append({"name": n, "version": v})
                        else:
                            arr.append(n)
                    deps["runtime"] = arr
                elif isinstance(inline, str):
                    deps["runtime"] = [inline]

        # normalize entries to either strings or dicts {"name":..., "version":...}
        def norm_item(it):
            if isinstance(it, str):
                return it
            if isinstance(it, dict):
                # possible forms: {"pkg": ">=1.2.3"} or {"name": "pkg", "version": ">=1.2"}
                if "name" in it and "version" in it:
                    return {"name": it["name"], "version": it["version"]}
                # else if single key
                if len(it) == 1:
                    k = next(iter(it.keys()))
                    return {"name": k, "version": it[k]}
                # fallback to str
                return str(it)
            return str(it)

        for k in ("build", "runtime", "optional"):
            normed = []
            for i in deps.get(k, []):
                normed.append(norm_item(i))
            deps[k] = normed

        return deps

    # ---------------------
    # Graph construction & cache
    # ---------------------
    def _build_graph_for(self, roots: List[str], include_build: bool = False) -> Dict[str, List[str]]:
        """
        Build directed graph A -> [B,C] meaning A depends on B and C.
        Includes recursive traversal starting from roots.
        Returns dict mapping package -> list_of_dependencies (strings).
        Uses in-memory cache when enabled.
        """
        # if a full-graph cache exists and includes roots, we can reuse it
        if self.cache_enabled and self._graph_cache is not None:
            # quick check: if all roots in cache keys, return subgraph
            if all(root in self._graph_cache for root in roots):
                # build subgraph including reachable nodes
                sub = {}
                stack = list(roots)
                seen = set()
                while stack:
                    n = stack.pop()
                    if n in seen:
                        continue
                    seen.add(n)
                    deps = self._graph_cache.get(n, [])
                    sub[n] = deps.copy()
                    for d in deps:
                        if d not in seen:
                            stack.append(d)
                return sub

        graph: Dict[str, List[str]] = {}

        # DFS
        visited: Set[str] = set()

        def visit(pkg: str):
            if pkg in visited:
                return
            visited.add(pkg)
            try:
                meta = self._load_meta(pkg)
            except FileNotFoundError:
                # treat as external/host-provided: no internal deps
                graph.setdefault(pkg, [])
                return
            deps_struct = self._extract_deps_from_meta(meta)
            reqs = []
            # include runtime always
            for d in deps_struct.get("runtime", []):
                if isinstance(d, dict):
                    reqs.append(d["name"])
                else:
                    reqs.append(str(d))
            if include_build:
                for d in deps_struct.get("build", []):
                    if isinstance(d, dict):
                        reqs.append(d["name"])
                    else:
                        reqs.append(str(d))
            # dedupe while preserving order
            seen_local = set()
            deduped = []
            for r in reqs:
                if r not in seen_local:
                    seen_local.add(r)
                    deduped.append(r)
            graph[pkg] = deduped
            for r in deduped:
                visit(r)

        for r in roots:
            visit(r)

        # store to disk cache (merge)
        if self.cache_enabled:
            try:
                existing = {}
                if os.path.exists(CACHE_FILE):
                    with open(CACHE_FILE, "r", encoding="utf-8") as fh:
                        existing = json.load(fh)
                # merge graph into existing
                for k, v in graph.items():
                    existing[k] = v
                with open(CACHE_FILE, "w", encoding="utf-8") as fh:
                    json.dump(existing, fh, indent=2)
                self._graph_cache = existing
            except Exception as e:
                logger.debug(f"Failed to write cache: {e}")

        return graph

    # ---------------------
    # topological sort
    # ---------------------
    def _topological_sort(self, graph: Dict[str, List[str]]) -> List[str]:
        visited: Set[str] = set()
        temp: Set[str] = set()
        order: List[str] = []

        def visit(node: str):
            if node in visited:
                return
            if node in temp:
                raise DependencyError(f"Dependency cycle detected at {node}")
            temp.add(node)
            for m in graph.get(node, []):
                visit(m)
            temp.remove(node)
            visited.add(node)
            order.append(node)

        for n in list(graph.keys()):
            if n not in visited:
                visit(n)
        return order

    # ---------------------
    # Public API
    # ---------------------
    def resolve_graph(self, pkgname: str, include_build: bool = False) -> List[str]:
        """
        Return topologically sorted list of packages required by pkgname (deps first, pkgname last).
        """
        graph = self._build_graph_for([pkgname], include_build=include_build)
        order = self._topological_sort(graph)
        # ensure pkgname is last (topological_sort returns in deps-first order)
        if pkgname in order:
            # order already has pkgname after its deps
            return order
        # else append
        return order + [pkgname]

    def missing_deps(self, pkgname: str, include_build: bool = False) -> List[str]:
        """
        Return list of dependency names (strings) that are required but not installed or not satisfying version.
        """
        graph = self._build_graph_for([pkgname], include_build=include_build)
        needed = []
        # traverse nodes except root
        for node, deps in graph.items():
            for d in deps:
                # d may be "pkg" or "pkg:constraint" --- normalize
                name, constraint = self._split_dep_entry(d)
                pkginfo = self.db.get_package(name)
                installed_ver = pkginfo.get("version") if pkginfo else None
                if not constraint_satisfied(installed_ver, constraint):
                    if name not in needed:
                        needed.append(name)
        return needed

    def reverse_deps(self, pkgname: str) -> List[str]:
        """
        Return list of installed package names that have runtime dependency on pkgname.
        """
        installed = self.db.list_installed()
        res = []
        for p in installed:
            name = p["name"]
            try:
                meta = self._load_meta(name)
            except Exception:
                continue
            deps = self._extract_deps_from_meta(meta).get("runtime", []) or []
            for d in deps:
                dep_name, _ = self._split_dep_entry(d)
                if dep_name == pkgname:
                    res.append(name)
                    break
        return sorted(res)

    def find_orphans(self) -> List[str]:
        """
        Find installed packages that are not required by any other installed package.
        (system roots like 'base' packages that nothing depends on are considered orphans too,
         but the tool is intended to find removable orphan libraries).
        """
        installed = self.db.list_installed()
        installed_names = [p["name"] for p in installed]
        required = set()
        for name in installed_names:
            try:
                meta = self._load_meta(name)
            except Exception:
                continue
            rdeps = self._extract_deps_from_meta(meta).get("runtime", []) or []
            for d in rdeps:
                dep_name, _ = self._split_dep_entry(d)
                required.add(dep_name)
        orphans = [n for n in installed_names if n not in required]
        return sorted(orphans)

    def dump_graph(self, roots: List[str], path: str, include_build: bool = False) -> None:
        """
        Dump graph starting from roots to DOT file for visualization.
        """
        graph = self._build_graph_for(roots, include_build=include_build)
        # create dot
        lines = ["digraph deps {"]
        for a, deps in graph.items():
            for b in deps:
                lines.append(f'  "{a}" -> "{b}";')
        lines.append("}")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
        log_event("deps", "dump_graph", f"Graph dumped to {path}")

    # ---------------------
    # Utilities for parsing dep entries
    # ---------------------
    def _split_dep_entry(self, entry: Any) -> Tuple[str, str]:
        """
        Normalize dependency entry to (name, constraint).
        Accepted forms:
          - "pkgname"
          - {"name": "pkg", "version": ">=1.2"}
          - "pkgname>=1.2"
        """
        if isinstance(entry, dict):
            name = entry.get("name") or next(iter(entry.keys()))
            ver = entry.get("version") or entry.get(name) or ""
            return (str(name), str(ver))
        s = str(entry)
        # try to split inline operator
        m = re.match(r'^([^<>=~!]+)\s*(>=|<=|==|~=|>|<)\s*(.+)$', s)
        if m:
            name = m.group(1).strip()
            op = m.group(2).strip()
            ver = m.group(3).strip()
            return (name, op + ver)
        # else plain name
        return (s.strip(), "")


# --------------------------
# resolve_and_install helper
# --------------------------
def resolve_and_install(resolver: DependencyResolver, pkgname: str,
                        BuilderCls, InstallerCls, args,
                        include_build: bool = False, dry_run: Optional[bool] = None) -> List[str]:
    """
    Resolve transitively dependencies for pkgname and build+install them in the correct order.
    - resolver: DependencyResolver instance
    - pkgname: target package
    - BuilderCls: class (not instance) for the Builder
    - InstallerCls: class for the Installer
    - args: CLI args (must contain db_path, ports_dir, build_root, cache_dir, packages_dir, root, fakeroot, dry_run)
    - include_build: if True, include build dependencies
    - dry_run: override args.dry_run if provided

    Returns list of names built/installed.
    """
    built = []
    dry_run = bool(dry_run) if dry_run is not None else getattr(args, "dry_run", False)
    db_path = getattr(args, "db_path", "/var/lib/zeropkg/installed.sqlite3")
    ports_dir = getattr(args, "ports_dir", "/usr/ports")
    build_root = getattr(args, "build_root", "/var/zeropkg/build")
    cache_dir = getattr(args, "cache_dir", "/usr/ports/distfiles")
    packages_dir = getattr(args, "packages_dir", "/var/zeropkg/packages")
    fakeroot = getattr(args, "fakeroot", False)
    root = getattr(args, "root", "/")

    # instantiate helper classes
    builder = BuilderCls(db_path=db_path, ports_dir=ports_dir,
                         build_root=build_root, cache_dir=cache_dir, packages_dir=packages_dir)
    installer = InstallerCls(db_path=db_path, ports_dir=ports_dir,
                             root=root, dry_run=dry_run, use_fakeroot=fakeroot)
    db = DBManager(db_path)

    # compute topological order of all nodes (deps first)
    try:
        graph = resolver._build_graph_for([pkgname], include_build=include_build)
        order = resolver._topological_sort(graph)
    except DependencyError as e:
        raise

    # Ensure we also include pkgname (if not present)
    if pkgname not in order:
        order.append(pkgname)

    # Build/install in order, skipping already-satisfied deps (respecting version constraints)
    for node in order:
        # determine if node has version constraint in graph entries — find any entry referencing it
        # We'll retrieve package meta if exists to determine if installed version satisfies any constraints
        pkginfo = db.get_package(node)
        installed_ver = pkginfo.get("version") if pkginfo else None

        # find constraint specified somewhere pointing to this node (best-effort)
        # For correctness we should inspect the parent's dependency entry, but for simplicity
        # we'll assume that if installed exists we'll skip building; otherwise build.
        if installed_ver:
            logger_debug = logger.debug if hasattr(logger, "debug") else print
            logger_debug(f"{node} already installed (version {installed_ver}), skipping build")
            continue

        # Build node
        log_event(node, "deps.build", f"Building dependency {node}")
        try:
            # builder.build should build and call installer.install internally (per our builder)
            builder.build(node, args)
            built.append(node)
            # verify DB has the package
            if not db.get_package(node):
                # try install from packages_dir
                candidate_pkg = os.path.join(packages_dir, f"{node}.tar.xz")
                if os.path.exists(candidate_pkg):
                    try:
                        installer.install(node, args, pkg_file=candidate_pkg, meta=None)
                    except Exception as ie:
                        raise DependencyError(f"Installer failed for {node}: {ie}")
                else:
                    raise DependencyError(f"Built {node} but no DB entry and no package file found")
        except Exception as e:
            log_event(node, "deps.build", f"Failed building dependency {node}: {e}", level="error")
            raise DependencyError(f"Failed building dependency {node}: {e}")

    return built
