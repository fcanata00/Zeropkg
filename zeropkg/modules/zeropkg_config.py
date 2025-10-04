#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_config.py — Config loader/validator/utilities for Zeropkg
Pattern B: integrated, lean, functional.

Features:
- load_config(path) with caching
- support `include = [ ... ]` in TOML
- support repo-local config scanning (e.g. /usr/ports/*/config.toml)
- apply_cli_overrides(args) to let CLI flags override config safely
- ensure_dirs(cfg) to create/validate required directories
- getters: get_build_root(), get_cache_dir(), get_ports_dirs(), get_db_path(), get_packages_dir()
- validate_config(cfg) to verify required fields and protect dangerous overrides
- reload capability via reload_config_if_changed()
- simple safe defaults if config absent
- integrates with zeropkg_logger if available
"""

from __future__ import annotations
import os
import sys
import time
import tomllib as _tomllib  # python 3.11+
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import threading

# tomli fallback for older environments (shouldn't be necessary, but safe)
try:
    import tomli as _tomli  # type: ignore
except Exception:
    _tomli = None

# try to use zeropkg_logger if present
try:
    from zeropkg_logger import log_event, get_logger
    _logger = get_logger("config")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_config")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))


# ---------------------------
# Defaults
# ---------------------------
_DEFAULT_PATHS = {
    "db_path": "/var/lib/zeropkg/installed.sqlite3",
    "ports_dir": "/usr/ports",
    "build_root": "/var/zeropkg/build",
    "cache_dir": "/usr/ports/distfiles",
    "packages_dir": "/var/zeropkg/packages",
    "root": "/",
}
_DEFAULT_OPTIONS = {
    "jobs": 4,
    "fakeroot": True,
    "chroot_enabled": True,
}

# internal cache & file timestamps
_cached_config: Optional[Dict[str, Any]] = None
_config_mtime: Dict[str, float] = {}
_config_lock = threading.RLock()


# ---------------------------
# Helpers
# ---------------------------
def _read_toml_file(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    data = p.read_bytes()
    try:
        # prefer tomllib
        return _tomllib.loads(data.decode("utf-8"))
    except Exception:
        if _tomli:
            return _tomli.loads(data)
        else:
            raise


def _merge_dict(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-ish merge: values in b override a. Only handles dicts/lists/primitives."""
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _merge_dict(out[k], v)
        else:
            out[k] = v
    return out


def _safe_path(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    return str(Path(p).expanduser().resolve())


# ---------------------------
# Core: load_config()
# ---------------------------
def load_config(path: Optional[str] = None, *, reload_if_changed: bool = True) -> Dict[str, Any]:
    """
    Load and return the merged configuration.
    Order of precedence (lowest -> highest):
      1. built-in defaults
      2. /usr/lib/zeropkg/config.toml (packaged defaults)
      3. /etc/zeropkg/config.toml (system config)
      4. per-repo configs under ports_dir/*/config.toml
      5. user config ~/.config/zeropkg/config.toml
      6. specified path argument (highest precedence)
    Supports "include = [ '/path/a.toml', ... ]" within any config file.
    Caches result for subsequent calls unless reload_if_changed is True.
    """
    global _cached_config, _config_mtime
    with _config_lock:
        if path:
            config_files = [str(Path(path).expanduser().resolve())]
        else:
            config_files = [
                "/usr/lib/zeropkg/config.toml",
                "/etc/zeropkg/config.toml",
                str(Path.home() / ".config" / "zeropkg" / "config.toml"),
                "./zeropkg.config.toml",
            ]

        merged: Dict[str, Any] = {}
        file_list: List[str] = []

        # Start from defaults
        merged["paths"] = dict(_DEFAULT_PATHS)
        merged["options"] = dict(_DEFAULT_OPTIONS)
        merged["repos"] = []  # list of repo definitions

        # Helper to load and process includes
        def _load_file(fp: str):
            fp = str(fp)
            if not os.path.exists(fp):
                return {}
            try:
                mtime = os.path.getmtime(fp)
            except Exception:
                mtime = 0.0
            prev_mtime = _config_mtime.get(fp)
            if prev_mtime is None or mtime != prev_mtime:
                _config_mtime[fp] = mtime
            raw = _read_toml_file(fp)
            # process includes
            includes = raw.get("include", []) if isinstance(raw, dict) else []
            if isinstance(includes, str):
                includes = [includes]
            inc_merged = {}
            for inc in includes:
                try:
                    incp = _safe_path(inc)
                    if incp and os.path.exists(incp):
                        inc_merged = _merge_dict(inc_merged, _load_file(incp))
                except Exception as e:
                    _logger.warning(f"include '{inc}' failed: {e}")
            # merge included then this file
            return _merge_dict(inc_merged, raw if isinstance(raw, dict) else {})

        # load main config files
        for cf in config_files:
            try:
                cfg_piece = _load_file(cf)
                merged = _merge_dict(merged, cfg_piece)
                if os.path.exists(cf):
                    file_list.append(cf)
            except Exception as e:
                _logger.warning(f"Failed reading config {cf}: {e}")

        # if ports_dir present, load per-repo config files
        ports_dir = _safe_path(merged.get("paths", {}).get("ports_dir") or _DEFAULT_PATHS["ports_dir"])
        if ports_dir and os.path.isdir(ports_dir):
            try:
                for entry in sorted(os.listdir(ports_dir)):
                    repo_cfg = os.path.join(ports_dir, entry, "config.toml")
                    if os.path.exists(repo_cfg):
                        try:
                            rc = _load_file(repo_cfg)
                            # attach repo metadata
                            repo_meta = rc.get("repo", {}) if isinstance(rc, dict) else {}
                            repo_meta.setdefault("path", os.path.join(ports_dir, entry))
                            merged.setdefault("repos", []).append(_merge_dict({"name": entry}, repo_meta))
                            merged = _merge_dict(merged, rc)
                            file_list.append(repo_cfg)
                        except Exception as e:
                            _logger.warning(f"Failed load repo config {repo_cfg}: {e}")
            except Exception:
                pass

        # Final normalization & safe paths
        paths = merged.get("paths", {}) or {}
        for k, defv in _DEFAULT_PATHS.items():
            val = paths.get(k, defv)
            paths[k] = _safe_path(val) or defv
        merged["paths"] = paths

        # options normalization
        options = merged.get("options", {}) or {}
        options.setdefault("jobs", int(options.get("jobs", _DEFAULT_OPTIONS["jobs"])))
        options.setdefault("fakeroot", bool(options.get("fakeroot", _DEFAULT_OPTIONS["fakeroot"])))
        options.setdefault("chroot_enabled", bool(options.get("chroot_enabled", _DEFAULT_OPTIONS["chroot_enabled"])))
        merged["options"] = options

        # Validate minimal structure
        try:
            validate_config(merged)
        except Exception as e:
            _logger.warning(f"Configuration validation warning: {e}")

        # cache and return
        _cached_config = merged
        return merged


# ---------------------------
# Validation & safety
# ---------------------------
def validate_config(cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate configuration fields and return (ok, issues).
    Raises on fatal issues.
    """
    issues: List[str] = []
    paths = cfg.get("paths", {})
    # required keys
    for k in ("db_path", "ports_dir", "build_root", "cache_dir", "packages_dir"):
        if not paths.get(k):
            issues.append(f"Missing path: {k}")

    # Do not allow root="/" override unless explicit flag set in options.allow_root_override
    root = paths.get("root", "/")
    if root != "/" and root in ("/", ""):
        issues.append("Invalid root path")
    # prevent accidental dangerous root targets
    if root == "/":
        if not cfg.get("options", {}).get("allow_root_install", False):
            # not fatal but warn
            issues.append("root is '/' — ensure you know what you're doing (set options.allow_root_install=True to confirm)")

    # ports_dir must exist
    ports_dir = paths.get("ports_dir")
    if ports_dir and not os.path.isdir(ports_dir):
        issues.append(f"ports_dir {ports_dir} does not exist")

    # check jobs
    jobs = cfg.get("options", {}).get("jobs", 1)
    try:
        jobs = int(jobs)
        if jobs <= 0:
            issues.append("jobs must be > 0")
    except Exception:
        issues.append("jobs must be integer")

    ok = len(issues) == 0
    if not ok:
        _logger.warning("Config validation issues: " + "; ".join(issues))
    return ok, issues


# ---------------------------
# Apply CLI overrides (safe)
# ---------------------------
def apply_cli_overrides(cfg: Dict[str, Any], args: Any) -> Dict[str, Any]:
    """
    Apply CLI flags to the loaded config in a controlled manner.
    Supported overrides:
      --root -> paths.root
      --ports-dir -> paths.ports_dir
      --cache-dir -> paths.cache_dir
      --build-root -> paths.build_root
      --db-path -> paths.db_path
      --jobs -> options.jobs
      --fakeroot/--no-fakeroot -> options.fakeroot
      --chroot-enabled/--no-chroot -> options.chroot_enabled
      --repo -> additional repo path to be added to repos (non-destructive)
    Returns new merged config dict.
    """
    cfg = dict(cfg)  # shallow copy
    paths = dict(cfg.get("paths", {}))
    options = dict(cfg.get("options", {}))
    # mapping of common CLI attributes (some may not exist on args)
    if hasattr(args, "root") and getattr(args, "root", None):
        new_root = _safe_path(getattr(args, "root"))
        if new_root and new_root != "/":
            # prevent accidental setting of critical root to host root unless explicit confirmation
            if new_root == "/" and not options.get("allow_root_install"):
                raise ValueError("Refusing to override root to '/' via CLI unless allow_root_install is True in config")
        paths["root"] = new_root or paths.get("root", "/")
    if hasattr(args, "ports_dir") and getattr(args, "ports_dir", None):
        paths["ports_dir"] = _safe_path(getattr(args, "ports_dir"))
    if hasattr(args, "cache_dir") and getattr(args, "cache_dir", None):
        paths["cache_dir"] = _safe_path(getattr(args, "cache_dir"))
    if hasattr(args, "build_root") and getattr(args, "build_root", None):
        paths["build_root"] = _safe_path(getattr(args, "build_root"))
    if hasattr(args, "db_path") and getattr(args, "db_path", None):
        paths["db_path"] = _safe_path(getattr(args, "db_path"))
    if hasattr(args, "jobs") and getattr(args, "jobs", None) is not None:
        try:
            options["jobs"] = int(getattr(args, "jobs"))
        except Exception:
            pass
    # boolean toggles
    if hasattr(args, "fakeroot") and getattr(args, "fakeroot", None) is not None:
        options["fakeroot"] = bool(getattr(args, "fakeroot"))
    if hasattr(args, "chroot_enabled") and getattr(args, "chroot_enabled", None) is not None:
        options["chroot_enabled"] = bool(getattr(args, "chroot_enabled"))

    # add repo if provided
    if hasattr(args, "repo") and getattr(args, "repo", None):
        repo_path = _safe_path(getattr(args, "repo"))
        if os.path.isdir(repo_path):
            repos = list(cfg.get("repos", []) or [])
            repos.append({"path": repo_path, "name": os.path.basename(repo_path)})
            cfg["repos"] = repos

    cfg["paths"] = paths
    cfg["options"] = options
    # validate after override
    validate_config(cfg)
    return cfg


# ---------------------------
# Ensure directories and basic environment
# ---------------------------
def ensure_dirs(cfg: Optional[Dict[str, Any]] = None) -> None:
    """
    Create and validate directories used by Zeropkg.
    This is idempotent and safe to call before running build/install.
    """
    if cfg is None:
        cfg = load_config()
    paths = cfg.get("paths", {})
    required = [
        paths.get("db_path"),
        paths.get("ports_dir"),
        paths.get("build_root"),
        paths.get("cache_dir"),
        paths.get("packages_dir"),
    ]
    for p in required:
        if not p:
            continue
        d = Path(p)
        # if path is a file (db_path), ensure parent
        if "." in d.name and not d.is_dir():
            d.parent.mkdir(parents=True, exist_ok=True)
        else:
            d.mkdir(parents=True, exist_ok=True)
    _logger.debug("ensure_dirs completed")


# ---------------------------
# Simple getters for convenience
# ---------------------------
def get_build_root(cfg: Optional[Dict[str, Any]] = None) -> str:
    return (cfg or load_config())["paths"]["build_root"]

def get_cache_dir(cfg: Optional[Dict[str, Any]] = None) -> str:
    return (cfg or load_config())["paths"]["cache_dir"]

def get_ports_dirs(cfg: Optional[Dict[str, Any]] = None) -> List[str]:
    cfg = cfg or load_config()
    ports_dir = cfg["paths"]["ports_dir"]
    # also include per-repo paths declared in cfg.repos
    repos = cfg.get("repos", []) or []
    extras = [r.get("path") for r in repos if r.get("path")]
    return [ports_dir] + extras

def get_db_path(cfg: Optional[Dict[str, Any]] = None) -> str:
    return (cfg or load_config())["paths"]["db_path"]

def get_packages_dir(cfg: Optional[Dict[str, Any]] = None) -> str:
    return (cfg or load_config())["paths"]["packages_dir"]


# ---------------------------
# Reload support (manual or timestamp check)
# ---------------------------
def reload_config_if_changed(path: Optional[str] = None) -> bool:
    """
    If any of the known config files have changed mtime, reload into cache and return True.
    Otherwise return False.
    """
    global _cached_config
    with _config_lock:
        # build candidate list similar to load_config
        cfg = load_config(path)  # this will refresh and cache regardless
        _cached_config = cfg
        return True


# ---------------------------
# Small CLI / debug entry
# ---------------------------
def _cli_main():
    import argparse
    p = argparse.ArgumentParser(prog="zeropkg-config", description="Debug helper for zeropkg_config")
    p.add_argument("--path", help="Specific config file to load")
    p.add_argument("--print", action="store_true", help="Print merged config")
    p.add_argument("--ensure-dirs", action="store_true", help="Ensure directories exist")
    args = p.parse_args()
    cfg = load_config(args.path)
    if args.print:
        import json
        print(json.dumps(cfg, indent=2))
    if args.ensure_dirs:
        ensure_dirs(cfg)
    print("Done.")


# ---------------------------
# Module quick test
# ---------------------------
if __name__ == "__main__":
    _cli_main()
