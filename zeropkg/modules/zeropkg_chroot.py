#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_chroot.py — chroot / mount manager for Zeropkg (Pattern B: integrated, lean, functional)

Provides:
  - prepare_chroot(root, copy_resolv=True, dry_run=False)
  - cleanup_chroot(root, force_lazy=False, dry_run=False)
  - auto_prepare(meta, root=None, copy_resolv=True, dry_run=False)
  - exec_in_chroot(root, cmd, env=None, use_fakeroot=False, dry_run=False)
  - run_in_chroot(root, cmd, cwd=None, env=None, use_shell=False, dry_run=False)
  - get_active_chroots() -> list
  - force_cleanup_all(dry_run=False)
  - ensure_network(root, dry_run=False)
  - is_chroot_ready(root) -> bool

Persists active chroots info in /var/lib/zeropkg/chroots.json
Integrates with zeropkg_config, zeropkg_logger and zeropkg_db when available.
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

# Optional integrations
try:
    from zeropkg_config import load_config, get_build_root, ensure_dirs
except Exception:
    def load_config(*a, **k): return {"paths": {"db_path": "/var/lib/zeropkg/installed.sqlite3", "build_root": "/var/zeropkg/build", "log_dir": "/var/log/zeropkg", "packages_dir": "/var/zeropkg/packages"}, "options": {"chroot_enabled": True}}
    def get_build_root(cfg=None): return "/var/zeropkg/build"
    def ensure_dirs(cfg=None): pass

try:
    from zeropkg_logger import log_event, log_global, get_logger
    _logger = get_logger("chroot")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_chroot")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(msg)

# DB optional
try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# Constants
_CHROOT_STATE = Path("/var/lib/zeropkg/chroots.json")
_DEFAULT_MOUNTS = [
    ("/proc", "proc", None),
    ("/sys", "sysfs", None),
    ("/dev", "bind", None),
    ("/dev/pts", "devpts", None),
    ("/run", "bind", None),
]
# overlay support: create work/upper if requested
_OVERLAY_META = ("overlay",)  # marker if overlay requested in meta

# Helpers for safe operations
def _atomic_write(path: Path, data: str):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(str(tmp), str(path))

def _load_state() -> Dict[str, Any]:
    try:
        if not _CHROOT_STATE.exists():
            return {}
        return json.loads(_CHROOT_STATE.read_text(encoding="utf-8"))
    except Exception as e:
        log_global(f"Failed to load chroot state: {e}", "warning")
        return {}

def _save_state(state: Dict[str, Any]):
    try:
        _CHROOT_STATE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_CHROOT_STATE, json.dumps(state, indent=2))
    except Exception as e:
        log_global(f"Failed to save chroot state: {e}", "warning")

def _record_chroot_active(root: str, mounts: List[Dict[str, Any]]):
    state = _load_state()
    key = str(root)
    state[key] = {"root": key, "mounts": mounts, "ts": int(time.time())}
    _save_state(state)

def _remove_chroot_record(root: str):
    state = _load_state()
    key = str(root)
    if key in state:
        del state[key]
        _save_state(state)

def get_active_chroots() -> List[Dict[str, Any]]:
    s = _load_state()
    return list(s.values())

# Low-level shell helpers
def _run(cmd: Union[str, List[str]], check: bool = True, dry_run: bool = False, env: Optional[Dict[str, str]] = None):
    if dry_run:
        log_global(f"[dry-run] would run: {cmd}")
        return 0
    if isinstance(cmd, list):
        proc = subprocess.run(cmd, check=check, env=env)
        return proc.returncode
    proc = subprocess.run(cmd, shell=True, check=check, env=env)
    return proc.returncode

def _is_mount_point(path: str) -> bool:
    try:
        return os.path.ismount(path)
    except Exception:
        return False

def _safe_ensure_dir(path: str, mode: int = 0o755):
    p = Path(path)
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)
        try:
            p.chmod(mode)
        except Exception:
            pass

# Security checks
def _ensure_root_user():
    if os.geteuid() != 0:
        raise PermissionError("Operation requires root privileges")

def _same_device(path1: str, path2: str) -> bool:
    try:
        return os.stat(path1).st_dev == os.stat(path2).st_dev
    except Exception:
        return False

# Public API
def prepare_chroot(root: str, copy_resolv: bool = True, overlay: bool = False, dry_run: bool = False, ensure_dirs_flag: bool = True) -> Dict[str, Any]:
    """
    Prepare a chroot at `root`:
      - create standard mountpoints
      - mount /proc, /sys, bind /dev and /run
      - optionally create overlay dirs (work/upper)
      - optionally copy /etc/resolv.conf to root/etc/resolv.conf for network
    Returns a dict with details (mounts list).
    Raises on error (unless dry_run).
    """
    cfg = load_config()
    if not cfg.get("options", {}).get("chroot_enabled", True):
        log_global("Chroot support disabled in config", "warning")
        return {}

    root = str(Path(root).resolve())
    if not dry_run:
        _ensure_root_user()

    # Basic safety: if root is "/" and not explicitly allowed, reject
    if root == "/":
        allow = cfg.get("options", {}).get("allow_root_install", False)
        if not allow:
            raise PermissionError("Refusing to prepare chroot for root '/' (enable options.allow_root_install to override)")

    # ensure directories
    if ensure_dirs_flag:
        ensure_dirs(cfg)

    # ensure root exists
    Path(root).mkdir(parents=True, exist_ok=True)

    mounts = []
    try:
        # validate device for safety: recommend same device as root parent (avoid cross-device complexities)
        host_root_dev = os.stat("/").st_dev
        try:
            target_dev = os.stat(root).st_dev
            if target_dev != host_root_dev:
                log_global(f"Warning: chroot root {root} is on a different device — overlay/mount behaviour may vary", "warning")
        except Exception:
            pass

        # Standard mounts
        for rel, fstype, extra in _DEFAULT_MOUNTS:
            dest = os.path.join(root, rel.lstrip("/"))
            _safe_ensure_dir(dest)
            if _is_mount_point(dest):
                log_global(f"{dest} already mounted; skipping", "debug")
                mounts.append({"src": rel, "dst": dest, "type": "existing"})
                continue
            # choose mount command
            if fstype == "bind":
                cmd = ["mount", "--bind", rel, dest]
            else:
                cmd = ["mount", "-t", fstype, fstype if fstype in ("proc", "sysfs") else rel, dest] if fstype in ("proc", "sysfs") else ["mount", "-t", fstype, rel, dest]
            log_global(f"Mounting {rel} -> {dest}")
            _run(cmd, dry_run=dry_run)
            mounts.append({"src": rel, "dst": dest, "type": fstype})

        # dev/pts might need special mount options
        # overlay support: create upper/work if requested
        if overlay:
            upper = os.path.join(root, "var", "lib", "zeropkg", "overlay", "upper")
            work = os.path.join(root, "var", "lib", "zeropkg", "overlay", "work")
            merged = os.path.join(root, "merged")
            _safe_ensure_dir(upper)
            _safe_ensure_dir(work)
            _safe_ensure_dir(merged)
            overlay_cmd = f"mount -t overlay overlay -o lowerdir={root},upperdir={upper},workdir={work} {merged}"
            log_global(f"Mounting overlay (lower={root}) -> {merged}")
            _run(overlay_cmd, dry_run=dry_run)
            mounts.append({"src": "overlay", "dst": merged, "type": "overlay", "upper": upper, "work": work})

        # copy resolv.conf for DNS if requested
        if copy_resolv:
            host_resolv = "/etc/resolv.conf"
            dest_etc = os.path.join(root, "etc")
            Path(dest_etc).mkdir(parents=True, exist_ok=True)
            dest_resolv = os.path.join(dest_etc, "resolv.conf")
            if os.path.exists(dest_resolv):
                log_global(f"{dest_resolv} already exists; not overwriting", "debug")
            else:
                if os.path.exists(host_resolv):
                    log_global(f"Copying {host_resolv} -> {dest_resolv}")
                    if dry_run:
                        pass
                    else:
                        shutil.copy2(host_resolv, dest_resolv)
                else:
                    log_global("Host resolv.conf not found; network inside chroot may not work", "warning")

        # persistence record
        _record_chroot_active(root, mounts)
        log_global(f"Chroot prepared at {root} with mounts: {mounts}", "info")
        return {"root": root, "mounts": mounts, "ts": int(time.time())}
    except Exception as e:
        log_global(f"Failed to prepare chroot {root}: {e}", "error")
        # attempt best-effort cleanup
        try:
            cleanup_chroot(root, force_lazy=True, dry_run=dry_run)
        except Exception:
            pass
        raise

def cleanup_chroot(root: str, force_lazy: bool = False, dry_run: bool = False) -> None:
    """
    Cleanup mounted filesystems under root. If force_lazy True, use lazy unmount for stubborn mounts.
    """
    root = str(Path(root).resolve())
    if dry_run:
        log_global(f"[dry-run] would cleanup chroot at {root}")
        return

    _ensure_root_user()
    # attempt to unmount in reverse order of mount points recorded
    state = _load_state()
    entry = state.get(root) or state.get(str(Path(root)))
    if not entry:
        # attempt to detect and unmount common points anyway
        candidates = [os.path.join(root, p.lstrip("/")) for p, _, _ in _DEFAULT_MOUNTS]
    else:
        candidates = [m.get("dst") for m in entry.get("mounts", []) if m.get("dst")]
    # reverse sort by path length to unmount children first
    candidates = sorted([c for c in candidates if c], key=lambda x: len(x), reverse=True)
    errors = []
    for mnt in candidates:
        try:
            if _is_mount_point(mnt):
                if force_lazy:
                    cmd = ["umount", "-l", mnt]
                else:
                    cmd = ["umount", mnt]
                log_global(f"Unmounting {mnt}")
                _run(cmd)
            else:
                log_global(f"{mnt} not mounted; skipping", "debug")
        except Exception as e:
            errors.append((mnt, str(e)))
            log_global(f"Failed to unmount {mnt}: {e}", "warning")
    # remove record if all good
    _remove_chroot_record(root)
    if errors:
        raise RuntimeError(f"Errors while cleaning chroot {root}: {errors}")
    log_global(f"Chroot at {root} cleaned", "info")

def auto_prepare(meta: Dict[str, Any], root: Optional[str] = None, copy_resolv: bool = True, dry_run: bool = False):
    """
    High-level convenience: if meta["build"]["chroot"] is True, prepare chroot automatically.
    Returns the prepare_chroot() result or {} if not required.
    """
    root = root or meta.get("build", {}).get("root") or "/mnt/lfs"
    build_cfg = meta.get("build", {}) or {}
    if not build_cfg.get("chroot", False):
        log_global("auto_prepare: meta does not request chroot; skipping", "debug")
        return {}
    overlay = bool(build_cfg.get("overlay", False))
    return prepare_chroot(root, copy_resolv=copy_resolv, overlay=overlay, dry_run=dry_run)

def exec_in_chroot(root: str, cmd: Union[str, List[str]], env: Optional[Dict[str, str]] = None, use_fakeroot: bool = False, dry_run: bool = False):
    """
    Execute a command inside chroot using chroot(8). cmd can be string or list.
    If use_fakeroot True, wraps with fakeroot.
    """
    root = str(Path(root).resolve())
    if dry_run:
        log_global(f"[dry-run] exec_in_chroot {root}: {cmd}")
        return 0
    _ensure_root_user()
    # build chroot command
    if isinstance(cmd, list):
        cmdline = subprocess.list2cmdline(cmd)
    else:
        cmdline = cmd
    # wrap with fakeroot if requested
    if use_fakeroot:
        inner = f"fakeroot bash -lc {shlex_quote(cmdline)}"
    else:
        inner = f"bash -lc {shlex_quote(cmdline)}"
    full = ["chroot", root, "/bin/bash", "-lc", inner]
    log_global(f"Running in chroot {root}: {cmdline}")
    return _run(full)

def run_in_chroot(root: str, cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None, use_shell: bool = False, dry_run: bool = False, use_fakeroot: bool = False):
    """
    More flexible runner: uses chroot then executes the command. If use_shell True, cmd passed to shell.
    cwd is relative to chroot root.
    """
    root = str(Path(root).resolve())
    if dry_run:
        log_global(f"[dry-run] run_in_chroot {root}: {cmd}")
        return 0
    _ensure_root_user()
    chroot_cmd = []
    # if cwd is specified, prefix with cd
    if cwd:
        cmd = f"cd {shlex_quote(cwd)} && {cmd}"
    if use_fakeroot:
        cmd = f"fakeroot bash -lc {shlex_quote(cmd)}"
    if use_shell:
        chroot_cmd = ["chroot", root, "/bin/bash", "-lc", cmd]
    else:
        chroot_cmd = ["chroot", root, "/bin/bash", "-lc", cmd]
    log_global(f"run_in_chroot {root} -> {cmd}")
    return _run(chroot_cmd)

def is_chroot_ready(root: str) -> bool:
    """
    Simple readiness check: minimal mounts present and /etc/resolv.conf exists inside root.
    """
    root = str(Path(root).resolve())
    try:
        checks = []
        for rel, fstype, _ in _DEFAULT_MOUNTS:
            path = os.path.join(root, rel.lstrip("/"))
            checks.append(_is_mount_point(path))
        resolv = os.path.exists(os.path.join(root, "etc", "resolv.conf"))
        return all(checks) and resolv
    except Exception:
        return False

def ensure_network(root: str, dry_run: bool = False):
    """
    Ensure network inside chroot by copying resolver and optionally /etc/hosts.
    """
    root = str(Path(root).resolve())
    dest_resolv = os.path.join(root, "etc", "resolv.conf")
    if dry_run:
        log_global(f"[dry-run] would ensure network by copying /etc/resolv.conf to {dest_resolv}")
        return
    if not os.path.exists(dest_resolv):
        if os.path.exists("/etc/resolv.conf"):
            shutil.copy2("/etc/resolv.conf", dest_resolv)
            log_global(f"Copied /etc/resolv.conf -> {dest_resolv}")
        else:
            log_global("Host /etc/resolv.conf not found; cannot provide DNS to chroot", "warning")
    else:
        log_global(f"{dest_resolv} exists; not overwriting", "debug")

def force_cleanup_all(dry_run: bool = False):
    """
    Force cleanup of all recorded chroots (best-effort).
    """
    _ensure_root_user()
    state = _load_state()
    roots = list(state.keys())
    errors = []
    for r in roots:
        try:
            cleanup_chroot(r, force_lazy=True, dry_run=dry_run)
        except Exception as e:
            errors.append((r, str(e)))
            log_global(f"force_cleanup_all: failed for {r}: {e}", "warning")
    # clear state file
    try:
        _save_state({})
    except Exception:
        pass
    if errors:
        raise RuntimeError(f"Errors cleaning chroots: {errors}")
    log_global("force_cleanup_all: all chroots cleaned", "info")

# small utility
def shlex_quote(s: str) -> str:
    # basic wrapper to avoid importing shlex at top-level heavy in some contexts
    import shlex
    return shlex.quote(s)

# CLI debug helper
if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser(prog="zeropkg-chroot", description="Manage chroot mounts for Zeropkg (debug CLI)")
    sp = p.add_subparsers(dest="cmd")
    sp_p = sp.add_parser("prepare")
    sp_p.add_argument("root")
    sp_p.add_argument("--dry-run", action="store_true")
    sp_c = sp.add_parser("cleanup")
    sp_c.add_argument("root")
    sp_c.add_argument("--force-lazy", action="store_true")
    sp_c.add_argument("--dry-run", action="store_true")
    sp_ls = sp.add_parser("list")
    sp_force = sp.add_parser("force-clean")
    sp_force.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.cmd == "prepare":
        print(json.dumps(prepare_chroot(args.root, dry_run=args.dry_run), indent=2))
    elif args.cmd == "cleanup":
        cleanup_chroot(args.root, force_lazy=args.force_lazy, dry_run=args.dry_run)
        print("cleanup done")
    elif args.cmd == "list":
        print(json.dumps(get_active_chroots(), indent=2))
    elif args.cmd == "force-clean":
        force_cleanup_all(dry_run=args.dry_run)
        print("force cleanup attempted")
    else:
        p.print_help()
