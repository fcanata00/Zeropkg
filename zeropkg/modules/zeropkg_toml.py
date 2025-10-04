#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_toml.py — TOML parser / normalizer for Zeropkg recipes
Pattern B: integrated, lean, functional.

Public API:
- load_toml(path) -> dict (normalized recipe)
- validate_recipe(recipe) -> (ok, issues)
- dump_meta(recipe, outpath)
- get_package_meta(recipe) -> (name, version)
- resolve_macros(value, env_map)
"""

from __future__ import annotations
import os
import sys
import json
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple, Union
from pathlib import Path

# tomllib in Python 3.11+
try:
    import tomllib as _tomllib  # type: ignore
except Exception:
    try:
        import tomli as _tomllib  # type: ignore
    except Exception:
        _tomllib = None

# integrate with config/logger if available (non-fatal)
try:
    from zeropkg_config import load_config, get_build_root
except Exception:
    load_config = lambda *a, **k: {}
    get_build_root = lambda cfg=None: "/var/zeropkg/build"

try:
    from zeropkg_logger import log_event, get_logger
    _log = get_logger("toml")
    def _log_event(pkg, stage, msg, level="info"):
        log_event(pkg, stage, msg, level)
except Exception:
    import logging
    _log = logging.getLogger("zeropkg_toml")
    if not _log.handlers:
        _log.addHandler(logging.StreamHandler(sys.stdout))
    def _log_event(pkg, stage, msg, level="info"):
        getattr(_log, level if hasattr(_log, level) else "info")(f"{pkg}:{stage} {msg}")

# -----------------------
# Dataclasses for structure
# -----------------------
@dataclass
class SourceEntry:
    url: str
    checksum: Optional[str] = None
    algo: str = "sha256"
    extract_to: Optional[str] = None
    method: Optional[str] = None  # e.g., git, http
    subpath: Optional[str] = None

@dataclass
class PatchEntry:
    path: str
    strip: Optional[int] = 1
    applied_to: Optional[str] = None

# -----------------------
# Utilities
# -----------------------
def _read_toml(path: str) -> Dict[str, Any]:
    if _tomllib is None:
        raise RuntimeError("No TOML parser available (tomllib/tomli).")
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    with p.open("rb") as f:
        data = _tomllib.load(f)
    return data if isinstance(data, dict) else {}

def _normalize_checksum(c: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Accept formats: "sha256:abcd...", "abcd..." -> default algo sha256
    Returns (algo, hex)
    """
    if not c:
        return None, None
    cs = str(c).strip()
    if ":" in cs:
        algo, val = cs.split(":", 1)
        return algo.lower(), val.strip()
    return "sha256", cs

def _detect_method(url: str) -> str:
    if url.startswith("git+"):
        return "git"
    if url.startswith("file://"):
        return "file"
    return "http"

def _ensure_list(v: Any) -> List:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

def _safe_get(d: Dict, key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default

# Macro substitution: ${VAR} or @VAR@ style
import re
_macro_re = re.compile(r"\$\{([^}]+)\}|\@([A-Za-z0-9_]+)\@")

def resolve_macros(value: Any, env_map: Dict[str, str]) -> Any:
    if isinstance(value, str):
        def _rep(m):
            k = m.group(1) or m.group(2)
            return str(env_map.get(k, m.group(0)))
        return _macro_re.sub(_rep, value)
    if isinstance(value, list):
        return [resolve_macros(x, env_map) for x in value]
    if isinstance(value, dict):
        return {k: resolve_macros(v, env_map) for k, v in value.items()}
    return value

# -----------------------
# Core parser/normalizer
# -----------------------
def load_toml(path: str, *, resolve_env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Load a recipe TOML and return a normalized dict with keys:
      package: {name, version, category}
      sources: [SourceEntry as dict]
      patches: [PatchEntry as dict]
      build: {commands:[], chroot:bool, fakeroot:bool, environment:{}}
      hooks: {pre_configure, post_build, pre_install, post_install, pre_remove, post_remove}
      dependencies: [ {name, version_req} ... ]
      raw: original parsed toml
    """
    cfg = load_config()
    cfg_build_root = get_build_root(cfg)
    raw = _read_toml(path)
    meta: Dict[str, Any] = {"raw": raw, "parsed_at": int(time.time()), "source_file": str(path)}

    # package block
    package = {}
    package_block = _safe_get(raw, "package", {}) or {}
    package["name"] = package_block.get("name") or package_block.get("pkg") or Path(path).stem
    package["version"] = package_block.get("version") or package_block.get("ver") or ""
    package["category"] = package_block.get("category") or package_block.get("group") or "main"
    meta["package"] = package

    # environment defaults and macro map
    env_map = {}
    env_map.update(os.environ)
    env_map.update({
        "PKG_NAME": package["name"],
        "PKG_VERSION": package["version"],
        "BUILD_ROOT": cfg_build_root,
        "LFS": os.environ.get("LFS", "/mnt/lfs"),
    })
    # merge recipe environment
    env_block = _safe_get(raw, "environment", {}) or {}
    env_block = {k: str(v) for k, v in env_block.items()}
    env_map.update(env_block)
    meta["environment"] = env_block

    # sources
    normalized_sources: List[Dict[str, Any]] = []
    raw_sources = _safe_get(raw, "sources", None) or _safe_get(raw, "source", None) or []
    raw_sources_list = _ensure_list(raw_sources)
    for s in raw_sources_list:
        # Accept string or table with url/checksum/extract_to/method
        if isinstance(s, str):
            url = s
            checksum = None
            extract_to = None
            method = None
        elif isinstance(s, dict):
            url = s.get("url") or s.get("src") or ""
            checksum = s.get("checksum") or s.get("hash")
            extract_to = s.get("extract_to") or s.get("extract_to_dir") or s.get("dst")
            method = s.get("method")
        else:
            continue
        url = resolve_macros(url, env_map) if url else url
        method = method or _detect_method(url)
        algo, chk = _normalize_checksum(checksum)
        if chk:
            checksum = chk
            algo = algo or "sha256"
        else:
            checksum = None
            algo = "sha256"
        if extract_to:
            extract_to = resolve_macros(extract_to, env_map)
        # support subpath specification like url@subdir or url#subdir
        subpath = None
        if "@" in url and url.count("@") == 1 and not url.startswith("git+"):
            # avoid confusing with git+ssh user@host
            u, sp = url.split("@", 1)
            if "/" in sp or "." in sp:
                # heuristic: only accept if looks like path
                url = u
                subpath = sp
        # normalized dict
        sdict = {
            "url": url,
            "checksum": checksum,
            "algo": algo,
            "extract_to": extract_to,
            "method": method,
            "subpath": subpath,
        }
        normalized_sources.append(sdict)
    meta["sources"] = normalized_sources

    # patches
    normalized_patches: List[Dict[str, Any]] = []
    raw_patches = _safe_get(raw, "patches", None) or []
    rp_list = _ensure_list(raw_patches)
    for p in rp_list:
        if isinstance(p, str):
            pathp = p
            strip = 1
        elif isinstance(p, dict):
            pathp = p.get("path") or p.get("file")
            strip = p.get("strip", 1)
        else:
            continue
        if pathp:
            pathp = resolve_macros(pathp, env_map)
            normalized_patches.append({"path": pathp, "strip": int(strip)})
    meta["patches"] = normalized_patches

    # build block: commands, chroot, fakeroot, install options, extract flags
    build_block = _safe_get(raw, "build", {}) or {}
    build_commands = build_block.get("commands") or _ensure_list(build_block.get("command")) or []
    # allow older style keys
    configure = build_block.get("configure")
    make = build_block.get("make")
    install_cmd = build_block.get("install")
    if not build_commands:
        if configure:
            build_commands.append(resolve_macros(configure, env_map))
        if make:
            build_commands.append(resolve_macros(make, env_map))
        if install_cmd:
            build_commands.append(resolve_macros(install_cmd, env_map))
    # normalize commands (resolve macros)
    build_commands = [resolve_macros(c, env_map) for c in build_commands]
    build_norm = {
        "commands": build_commands,
        "chroot": bool(build_block.get("chroot", False)),
        "fakeroot": bool(build_block.get("fakeroot", True)),
        "cleanup_sources": bool(build_block.get("cleanup_sources", True)),
        "dir_install": bool(build_block.get("dir_install", False)),
        "overlay": build_block.get("overlay", False),
        "overlay_dir": build_block.get("overlay_dir"),
    }
    # attach environment for build (merged)
    build_env = {}
    build_env.update(env_block)
    build_env.update(build_block.get("environment", {}) or {})
    # resolve macros in environment values
    build_env = {k: resolve_macros(v, env_map) for k, v in build_env.items()}
    build_norm["environment"] = build_env
    meta["build"] = build_norm

    # hooks
    hooks_block = _safe_get(raw, "hooks", {}) or {}
    hooks = {}
    for key in ("pre_configure", "post_build", "pre_install", "post_install", "pre_remove", "post_remove"):
        v = hooks_block.get(key) or hooks_block.get(key.replace("_", "-"))
        if v:
            hooks[key] = resolve_macros(v, env_map)
    meta["hooks"] = hooks

    # dependencies
    deps_block = _safe_get(raw, "dependencies", None) or _safe_get(raw, "requires", None) or []
    deps_list: List[Dict[str, str]] = []
    for d in _ensure_list(deps_block):
        if isinstance(d, str):
            # support "libc>=2.35" or "pkgname"
            if any(op in d for op in [">=", "<=", "==", ">", "<", "~="]):
                # naive split
                for op in [">=", "<=", "==", ">", "<", "~="]:
                    if op in d:
                        name, ver = d.split(op, 1)
                        deps_list.append({"name": name.strip(), "version_req": op + ver.strip()})
                        break
            else:
                deps_list.append({"name": d.strip(), "version_req": None})
        elif isinstance(d, dict):
            nm = d.get("name") or d.get("pkg") or d.get("package")
            ver = d.get("version") or d.get("version_req")
            deps_list.append({"name": nm, "version_req": ver})
    meta["dependencies"] = deps_list

    # install block (packaging/install metadata)
    install_block = _safe_get(raw, "install", {}) or {}
    meta["install"] = {k: resolve_macros(v, env_map) for k, v in (install_block.items() if isinstance(install_block, dict) else [])}

    # options & misc
    options = _safe_get(raw, "options", {}) or {}
    meta["options"] = options

    # final validation hints
    ok, issues = validate_recipe(meta)
    if not ok:
        _log_event(package["name"], "parse", f"Recipe validation issues: {issues}", "warning")
    return meta

# -----------------------
# Validation
# -----------------------
def validate_recipe(recipe: Dict[str, Any]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    pkg = recipe.get("package", {})
    if not pkg.get("name"):
        issues.append("package.name missing")
    if not pkg.get("version"):
        issues.append("package.version missing (recommended)")
    # sources basic checks
    sources = recipe.get("sources", []) or []
    if not sources:
        issues.append("no sources declared")
    for s in sources:
        if not s.get("url"):
            issues.append("a source entry is missing url")
        else:
            if s.get("method") == "git" and not s["url"].startswith("git+"):
                # allow https git urls without git+ but warn
                issues.append(f"git source probably should use git+ prefix: {s['url']}")
        # checksum format
        if s.get("checksum"):
            algo = s.get("algo", "sha256")
            if algo not in ("sha256", "sha512", "md5"):
                issues.append(f"unknown checksum algorithm: {algo}")
    # patches exist?
    for p in recipe.get("patches", []) or []:
        if not p.get("path"):
            issues.append("patch entry missing path")
    # build commands
    build = recipe.get("build", {}) or {}
    if not build.get("commands"):
        issues.append("build.commands empty — builder may do nothing")
    return (len(issues) == 0), issues

# -----------------------
# Helpers for external use
# -----------------------
def dump_meta(recipe: Dict[str, Any], outpath: str):
    p = Path(outpath)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(recipe, f, indent=2, ensure_ascii=False)

def get_package_meta(recipe: Dict[str, Any]) -> Tuple[str, str]:
    pkg = recipe.get("package", {})
    return pkg.get("name", ""), pkg.get("version", "")

# convenience: read recipe and return package id string
def package_fullname_from_file(path: str) -> str:
    recipe = load_toml(path)
    name, ver = get_package_meta(recipe)
    return f"{name}-{ver}" if ver else name

# -----------------------
# Small CLI/debug helper
# -----------------------
def _cli_main():
    import argparse, json
    p = argparse.ArgumentParser(prog="zeropkg-toml", description="Parse and normalize Zeropkg TOML")
    p.add_argument("file", help="recipe toml file")
    p.add_argument("--dump", help="dump normalized json to path")
    p.add_argument("--print", action="store_true", help="print normalized json to stdout")
    args = p.parse_args()
    meta = load_toml(args.file)
    if args.dump:
        dump_meta(meta, args.dump)
        print("Dumped to", args.dump)
    if args.print:
        print(json.dumps(meta, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli_main()
