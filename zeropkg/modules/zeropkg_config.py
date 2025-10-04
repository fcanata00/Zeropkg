#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_config.py — Config loader and validator for Zeropkg

Features implemented:
 - hierarchical config load (file, env, CLI overrides)
 - persistent cache at /var/lib/zeropkg/config.cache.json
 - repo validation (checks for ports layout)
 - security block support with defaults
 - build profiles support (default, minimal, custom)
 - host distro auto-detection (/etc/os-release)
 - permissions checks for key dirs
 - helpers: get_ports_roots, get_distfiles_dir, get_cache_dir, get_db_path, ensure_dirs
 - atomic cache writes and robust fallbacks
 - integrates with zeropkg_logger and zeropkg_db when available
"""

from __future__ import annotations
import os
import sys
import json
import errno
import shutil
import stat
import tempfile
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# safe imports
def _safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

logger_mod = _safe_import("zeropkg_logger")
db_mod = _safe_import("zeropkg_db")

# logger fallback
if logger_mod and hasattr(logger_mod, "get_logger"):
    log = logger_mod.get_logger("config")
    try:
        log_event = logger_mod.log_event
    except Exception:
        def log_event(*a, **k): pass
else:
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO)
    log = _logging.getLogger("zeropkg.config")
    def log_event(*a, **k):
        pass

# default locations (can be overridden by config file or env)
DEFAULT_SYS_CONFIG = Path("/etc/zeropkg/config.toml")
DEFAULT_USER_CONFIG = Path.home() / ".config" / "zeropkg" / "config.toml"
DEFAULT_PORTS_DIR = Path("/usr/ports")
DEFAULT_DISTFILES_DIR = DEFAULT_PORTS_DIR / "distfiles"
DEFAULT_CACHE = Path("/var/cache/zeropkg")
DEFAULT_STATE = Path("/var/lib/zeropkg")
DEFAULT_LOGDIR = Path("/var/log/zeropkg")
DEFAULT_DB = DEFAULT_STATE / "zeropkg.db"
CACHE_CONFIG_JSON = DEFAULT_STATE / "config.cache.json"

# default config skeleton
DEFAULT_CONFIG: Dict[str, Any] = {
    "paths": {
        "ports_dir": str(DEFAULT_PORTS_DIR),
        "distfiles_dir": str(DEFAULT_DISTFILES_DIR),
        "cache_dir": str(DEFAULT_CACHE),
        "state_dir": str(DEFAULT_STATE),
        "log_dir": str(DEFAULT_LOGDIR),
        "db_path": str(DEFAULT_DB),
    },
    "repos": {
        "roots": [str(DEFAULT_PORTS_DIR)]
    },
    "security": {
        "gpg_required": False,
        "sandbox_builds": True,
        "verify_signatures_on_fetch": False
    },
    "cli": {
        "default_jobs": 4,
        "prompt_confirm": True
    },
    "profiles": {
        "default": {"jobs": 4, "fakeroot": True, "parallel_install": True},
        "minimal": {"jobs": 1, "fakeroot": False, "parallel_install": False}
    },
    "vuln": {
        "sources": []
    }
}

# atomic write helper
def _atomic_write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)

# read cache if exists
def _read_cache(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log.warning("failed to read config cache %s: %s", path, e)
    return None

# detect host distro
def detect_host_distro() -> Dict[str, str]:
    info = {}
    try:
        p = Path("/etc/os-release")
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    info[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass
    return info

# minimal toml loader if tomllib or toml available; else fallback to json (if user supplied)
try:
    import tomllib  # py3.11+
    def _load_toml_text(path: Path) -> Dict[str, Any]:
        with open(path, "rb") as f:
            return tomllib.load(f)
except Exception:
    try:
        import toml
        def _load_toml_text(path: Path) -> Dict[str, Any]:
            return toml.load(str(path))
    except Exception:
        def _load_toml_text(path: Path) -> Dict[str, Any]:
            # last resort: try json (not ideal)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                raise RuntimeError("No toml support available; install 'toml' package")

# merge deep util
def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in b.items():
        if k in a and isinstance(a[k], dict) and isinstance(v, dict):
            a[k] = _deep_merge(a[k], v)
        else:
            a[k] = v
    return a

# validate repo path heuristics
def _validate_ports_root(path: Path) -> Tuple[bool, str]:
    """
    Returns (ok, reason) — ok True means path looks like a ports tree (best-effort).
    Checks minimal: exists, contains subdirs, has at least one .toml file or Makefile.in or distfiles dir.
    """
    if not path.exists():
        return False, "missing"
    if not path.is_dir():
        return False, "not-a-dir"
    # check typical structure quickly
    try:
        # distfiles subdir
        dist = path / "distfiles"
        if dist.exists() and dist.is_dir():
            return True, "ok"
        # find any .toml recursively up to depth 3
        found = False
        depth = 0
        for p in path.iterdir():
            if p.is_dir():
                for f in p.rglob("*.toml"):
                    found = True
                    break
            if found:
                break
        if found:
            return True, "ok"
        # check for Makefile at top-level
        if any((path / n).exists() for n in ("Makefile", "mk")):
            return True, "ok"
    except Exception:
        pass
    return False, "structure-not-recognized"

# permission check helper
def _check_directory_perms(path: Path, want_writable: bool = True) -> Tuple[bool, str]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.mkdir(parents=True, exist_ok=True)
    except Exception:
        # parent creation fail: continue to check existence
        pass
    if not path.exists():
        return False, "missing"
    if want_writable:
        try:
            testfile = path / ".zeropkg_write_test"
            with open(testfile, "w", encoding="utf-8") as f:
                f.write("x")
            testfile.unlink(missing_ok=True)
            return True, "ok"
        except Exception:
            return False, "not-writable"
    else:
        return True, "ok"

# main ConfigManager
class ConfigManager:
    def __init__(self, sys_config: Optional[Path] = None, user_config: Optional[Path] = None):
        self.sys_config = Path(sys_config) if sys_config else DEFAULT_SYS_CONFIG
        self.user_config = Path(user_config) if user_config else DEFAULT_USER_CONFIG
        self.cache_path = CACHE_CONFIG_JSON
        self.config: Dict[str, Any] = {}
        self.host_info = detect_host_distro()
        self.loaded_from: List[Path] = []
        # load on init
        self.load()

    def load(self, force_reload: bool = False):
        """
        Load config from:
          1) cache (fast)
          2) system config (/etc/zeropkg/config.toml)
          3) user config (~/.config/zeropkg/config.toml)
          4) env overrides (ZEROPKG_*)
        Applies deep merges and stores final config in self.config.
        """
        # try cache
        if not force_reload:
            cached = _read_cache(self.cache_path)
            if cached:
                self.config = cached
                log.info("config: loaded from cache %s", self.cache_path)
                return self.config

        cfg = {}
        # start from default skeleton
        cfg = _deep_merge({}, DEFAULT_CONFIG)

        # load system config if exists
        try:
            if self.sys_config.exists():
                log.info("loading system config: %s", self.sys_config)
                loaded = _load_toml_text(self.sys_config)
                cfg = _deep_merge(cfg, loaded or {})
                self.loaded_from.append(self.sys_config)
        except Exception as e:
            log.warning("failed load sys config %s: %s", self.sys_config, e)

        # load user config
        try:
            if self.user_config.exists():
                log.info("loading user config: %s", self.user_config)
                loaded = _load_toml_text(self.user_config)
                cfg = _deep_merge(cfg, loaded or {})
                self.loaded_from.append(self.user_config)
        except Exception as e:
            log.warning("failed load user config %s: %s", self.user_config, e)

        # environment overrides: variables prefixed with ZEROPKG_
        for k, v in os.environ.items():
            if not k.startswith("ZEROPKG_"):
                continue
            # convert name ZEROPKG_PATHS_CACHE_DIR -> paths.cache_dir
            key = k[len("ZEROPKG_"):].lower()
            parts = key.split("__") if "__" in key else key.split("_")
            d = cfg
            for p in parts[:-1]:
                if p not in d or not isinstance(d[p], dict):
                    d[p] = {}
                d = d[p]
            d[parts[-1]] = v

        # annotate with host info
        cfg["_host"] = {"os_release": self.host_info}

        # make sure mandatory paths are absolute and present
        paths = cfg.get("paths", {})
        # ensure types are str
        for k in ("ports_dir", "distfiles_dir", "cache_dir", "state_dir", "log_dir", "db_path"):
            if k not in paths:
                paths[k] = str(DEFAULT_CONFIG["paths"][k])
            else:
                paths[k] = str(paths[k])
        cfg["paths"] = paths

        # normalize repos roots
        repos = cfg.get("repos", {})
        roots = repos.get("roots") or [paths["ports_dir"]]
        # ensure absolute
        roots_norm = []
        for r in roots:
            rp = str(r)
            if not Path(rp).is_absolute():
                rp = os.path.abspath(rp)
            roots_norm.append(rp)
        repos["roots"] = roots_norm
        cfg["repos"] = repos

        # apply defaults for security and profiles if missing
        if "security" not in cfg:
            cfg["security"] = DEFAULT_CONFIG["security"]
        else:
            for k,v in DEFAULT_CONFIG["security"].items():
                cfg["security"].setdefault(k, v)

        if "profiles" not in cfg:
            cfg["profiles"] = DEFAULT_CONFIG["profiles"]
        else:
            # ensure default exists
            if "default" not in cfg["profiles"]:
                cfg["profiles"]["default"] = DEFAULT_CONFIG["profiles"]["default"]

        # persist cache atomically
        try:
            txt = json.dumps(cfg, indent=2, ensure_ascii=False)
            _atomic_write = _atomic_write_text
            _atomic_write(self.cache_path, txt)
        except Exception as e:
            log.warning("failed to persist config cache: %s", e)

        self.config = cfg
        log.info("config loaded: %s (from %s)", self.cache_path, self.loaded_from or "defaults")
        return self.config

    def reload(self):
        return self.load(force_reload=True)

    # CLI override application (simple)
    def apply_cli_overrides(self, args: Any):
        """
        args: argparse Namespace from CLI. Supports:
          --config (path) pointer, --jobs/-j, --profile (profile name), --ports-dir, --cache-dir
        """
        if not args:
            return
        try:
            if getattr(args, "config", None):
                p = Path(args.config)
                if p.exists():
                    try:
                        loaded = _load_toml_text(p)
                        self.config = _deep_merge(self.config, loaded or {})
                        log.info("applied CLI config override %s", p)
                    except Exception as e:
                        log.warning("failed to apply CLI config file %s: %s", p, e)
            if getattr(args, "jobs", None):
                self.config.setdefault("cli", {})["default_jobs"] = int(args.jobs)
            if getattr(args, "profile", None):
                prof = args.profile
                if prof in self.config.get("profiles", {}):
                    self.config["_active_profile"] = prof
                else:
                    log.warning("profile %s not found in config", prof)
            if getattr(args, "ports_dir", None):
                self.config["paths"]["ports_dir"] = str(args.ports_dir)
                # update repos
                roots = self.config.get("repos", {}).get("roots", [])
                if str(args.ports_dir) not in roots:
                    roots.insert(0, str(args.ports_dir))
                    self.config["repos"]["roots"] = roots
            if getattr(args, "cache_dir", None):
                self.config["paths"]["cache_dir"] = str(args.cache_dir)
        except Exception as e:
            log.warning("apply_cli_overrides exception: %s", e)

    # helpers
    def get(self, *keys, default=None):
        cfg = self.config
        for k in keys:
            if not cfg or k not in cfg:
                return default
            cfg = cfg[k]
        return cfg

    def get_ports_roots(self) -> List[Path]:
        roots = self.config.get("repos", {}).get("roots", [])
        return [Path(r) for r in roots]

    def get_distfiles_dir(self) -> Path:
        return Path(self.config["paths"].get("distfiles_dir", DEFAULT_DISTFILES_DIR))

    def get_cache_dir(self) -> Path:
        return Path(self.config["paths"].get("cache_dir", DEFAULT_CACHE))

    def get_state_dir(self) -> Path:
        return Path(self.config["paths"].get("state_dir", DEFAULT_STATE))

    def get_log_dir(self) -> Path:
        return Path(self.config["paths"].get("log_dir", DEFAULT_LOGDIR))

    def get_db_path(self) -> Path:
        return Path(self.config["paths"].get("db_path", DEFAULT_DB))

    def ensure_dirs(self) -> Dict[str, Tuple[bool, str]]:
        """
        Create and check main directories; returns dict of checks
        e.g. {"cache": (True,"ok"), "state": (False,"not-writable"), ...}
        """
        results: Dict[str, Tuple[bool, str]] = {}
        checks = [
            ("ports", Path(self.config["paths"].get("ports_dir"))),
            ("distfiles", self.get_distfiles_dir()),
            ("cache", self.get_cache_dir()),
            ("state", self.get_state_dir()),
            ("log", self.get_log_dir()),
        ]
        for name, path in checks:
            ok, reason = _check_directory_perms(path, want_writable=(name != "ports"))
            results[name] = (ok, reason)
            if not ok:
                log.warning("ensure_dirs: %s -> %s (%s)", name, path, reason)
        return results

    def validate_repos(self) -> Dict[str, Dict[str, Any]]:
        """
        Validate each configured ports root and return a map:
          { "/usr/ports": {"ok": True, "reason":"ok"}, ...}
        """
        roots = self.get_ports_roots()
        out: Dict[str, Dict[str, Any]] = {}
        for r in roots:
            ok, reason = _validate_ports_root(r)
            out[str(r)] = {"ok": ok, "reason": reason}
            if not ok:
                log.warning("repo validation: %s -> %s", r, reason)
        return out

    def get_active_profile(self) -> Dict[str, Any]:
        prof = self.config.get("_active_profile") or "default"
        profiles = self.config.get("profiles", {})
        return profiles.get(prof, profiles.get("default", {}))

    def set_active_profile(self, name: str) -> bool:
        if name in self.config.get("profiles", {}):
            self.config["_active_profile"] = name
            return True
        return False

    def security_settings(self) -> Dict[str, Any]:
        return self.config.get("security", DEFAULT_CONFIG["security"])

    def summary(self) -> Dict[str, Any]:
        s = {
            "loaded_from": [str(p) for p in self.loaded_from],
            "ports_roots": [str(p) for p in self.get_ports_roots()],
            "distfiles_dir": str(self.get_distfiles_dir()),
            "cache_dir": str(self.get_cache_dir()),
            "state_dir": str(self.get_state_dir()),
            "log_dir": str(self.get_log_dir()),
            "db_path": str(self.get_db_path()),
            "profiles": list(self.config.get("profiles", {}).keys()),
            "active_profile": self.config.get("_active_profile", "default"),
            "security": self.security_settings()
        }
        return s

# single instance convenience
_DEFAULT_MANAGER: Optional[ConfigManager] = None

def get_config_manager(force_reload: bool = False) -> ConfigManager:
    global _DEFAULT_MANAGER
    if _DEFAULT_MANAGER is None or force_reload:
        _DEFAULT_MANAGER = ConfigManager()
    if force_reload:
        _DEFAULT_MANAGER.reload()
    return _DEFAULT_MANAGER

# immediate ensure of directories (best-effort)
if __name__ != "__main__":
    try:
        mgr = get_config_manager()
        mgr.ensure_dirs()
    except Exception:
        pass

# simple CLI for config inspection
def _cli():
    import argparse
    p = argparse.ArgumentParser(prog="zeropkg-config", description="Zeropkg configuration inspector")
    p.add_argument("--reload", action="store_true", help="Reload config from files (ignore cache)")
    p.add_argument("--profile", help="Set active profile (temporary)")
    p.add_argument("--show", action="store_true", help="Print final merged config as JSON")
    p.add_argument("--validate-repos", action="store_true", help="Validate ports roots and print results")
    args = p.parse_args()
    mgr = get_config_manager(force_reload=args.reload)
    if args.profile:
        ok = mgr.set_active_profile(args.profile)
        print("profile set->", ok)
    if args.show:
        print(json.dumps(mgr.config, indent=2, ensure_ascii=False))
    if args.validate_repos:
        vr = mgr.validate_repos()
        print(json.dumps(vr, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
