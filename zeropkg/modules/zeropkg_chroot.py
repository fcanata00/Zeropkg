#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_chroot.py — Chroot manager for Zeropkg

Features included:
 - prepare_chroot(root, profile="lfs", bind_dirs=[...], overlay=False, dry_run=False)
 - cleanup_chroot(root, mounts, lazy=False, dry_run=False)
 - run_in_chroot(cfg_or_root, command, env=None, fakeroot=False, dry_run=False)
 - exec_in_chroot(root, argv, env=None, fakeroot=False, dry_run=False)
 - is_chroot_ready(root)
 - ensure_network(root)
 - verify_chroot(root) -> diagnostic report
 - force_cleanup_all(dry_run=False)
 - persistent state stored in /var/lib/zeropkg/chroots.json (atomic writes)
 - profiles: "lfs", "blfs", "x11", "minimal" (customizable)
 - monitoring helpers: list_chroots(), cleanup_stale(threshold_secs)
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import errno
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

# Try import logger
try:
    from zeropkg_logger import get_logger
    logger = get_logger("chroot")
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_chroot")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)

STATE_PATH = Path("/var/lib/zeropkg/chroots.json")
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
DEFAULT_TIMEOUT = 5  # seconds for mount operations retry
SAFE_BIND_DIRS = ["/etc/hosts", "/etc/resolv.conf", "/etc/nsswitch.conf"]

# ---- Utilities -----------------------------------------------------------
def _atomic_write(p: Path, data: Any):
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(p)

def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {"chroots": {}}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load chroot state: {e}")
        return {"chroots": {}}

def _save_state(state: Dict[str, Any]):
    try:
        _atomic_write(STATE_PATH, state)
    except Exception as e:
        logger.error(f"Failed to save chroot state: {e}")

def _is_mounted(path: Path) -> bool:
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and os.path.realpath(parts[1]) == str(path):
                    return True
    except Exception:
        pass
    return False

def _run(cmd: List[str], check=True, capture=False, timeout=None):
    logger.debug(f"shell: {' '.join(cmd)}")
    try:
        if capture:
            res = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
            return res.returncode, res.stdout.strip(), res.stderr.strip()
        else:
            subprocess.run(cmd, check=check, timeout=timeout)
            return 0, "", ""
    except subprocess.CalledProcessError as e:
        logger.debug(f"Command failed: {e}")
        return e.returncode, "", str(e)
    except Exception as e:
        logger.debug(f"Command exception: {e}")
        return 1, "", str(e)

# ---- Profiles ------------------------------------------------------------
DEFAULT_PROFILES: Dict[str, Dict[str, Any]] = {
    "lfs": {
        "binds": ["/dev", "/dev/pts", "/proc", "/sys", "/run"],
        "mount_proc": True,
        "overlay": False,
        "extra_binds": SAFE_BIND_DIRS
    },
    "minimal": {
        "binds": ["/dev", "/proc"],
        "mount_proc": True,
        "overlay": False,
        "extra_binds": ["/etc/resolv.conf"]
    },
    "blfs": {
        "binds": ["/dev", "/dev/pts", "/proc", "/sys", "/run"],
        "mount_proc": True,
        "overlay": True,
        "extra_binds": SAFE_BIND_DIRS
    },
    "x11": {
        "binds": ["/dev", "/dev/pts", "/proc", "/sys", "/run", "/tmp"],
        "mount_proc": True,
        "overlay": True,
        "extra_binds": SAFE_BIND_DIRS + ["/tmp/.X11-unix"]
    }
}

# ---- Core functions -----------------------------------------------------
def prepare_chroot(root: Path, profile: str = "lfs", bind_dirs: Optional[List[str]] = None,
                   overlay: Optional[bool] = None, copy_resolv: bool = True,
                   dry_run: bool = False, force: bool = False) -> List[Dict[str, Any]]:
    """
    Prepare a chroot at 'root' according to profile.
    Returns a list of mount records: [{type, src, dst, opts}, ...]
    """
    root = Path(root).resolve()
    if str(root) == "/":
        if not force:
            raise ValueError("Refusing to prepare chroot on '/' without force=True")
    profile_cfg = DEFAULT_PROFILES.get(profile, {})
    binds = list(profile_cfg.get("binds", []))
    if bind_dirs:
        binds += bind_dirs
    extra_binds = list(profile_cfg.get("extra_binds", []))
    mount_proc = profile_cfg.get("mount_proc", True)
    if overlay is None:
        overlay = profile_cfg.get("overlay", False)

    logger.info(f"Preparing chroot {root} profile={profile} overlay={overlay} dry_run={dry_run}")
    mounts = []
    if dry_run:
        logger.info(f"[dry-run] Would create {root} and perform mounts: {binds + extra_binds}")
        return [{"type": "dry-run", "dst": str(root), "binds": binds + extra_binds, "overlay": overlay}]

    root.mkdir(parents=True, exist_ok=True)

    # If overlay requested, create lower/upper/work and mount a tmpfs or use overlay mount
    if overlay:
        upper = root / "upper"
        work = root / "work"
        merged = root / "merged"
        for p in (upper, work, merged):
            p.mkdir(parents=True, exist_ok=True)
        # mount overlay: lower is root (original), upper/work new dirs, merged is final
        # We'll perform a bind of root to lower and mount overlay over merged
        lower = root / "lower"
        if not lower.exists():
            lower.mkdir(parents=True, exist_ok=True)
            # bind mount current root into lower
            rc, out, err = _run(["mount", "--bind", str(root), str(lower)], check=False)
            if rc != 0:
                logger.warning(f"bind lower failed: {err or out}")
        # mount overlay
        opts = f"lowerdir={lower},upperdir={upper},workdir={work}"
        rc, out, err = _run(["mount", "-t", "overlay", "overlay", "-o", opts, str(merged)], check=False)
        if rc != 0:
            logger.warning(f"overlay mount failed: {err or out}; continuing without overlay")
            overlay = False
        else:
            mounts.append({"type": "overlay", "src": "overlay", "dst": str(merged), "opts": opts})
            # set root to merged for further binds
            root = merged

    # Bind mounts for essential dirs
    for d in binds:
        src = Path(d)
        dst = root / src.relative_to("/")  # e.g. root/dev
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            rc, out, err = _run(["mount", "--bind", str(src), str(dst)], check=False)
            if rc != 0:
                logger.warning(f"bind {src} -> {dst} failed: {err or out}")
            else:
                mounts.append({"type": "bind", "src": str(src), "dst": str(dst)})
        except Exception as e:
            logger.error(f"Exception binding {src}: {e}")

    # mount proc/sys if requested via explicit mount types (prefer proper vfstype)
    if mount_proc:
        proc_dst = root / "proc"
        proc_dst.mkdir(parents=True, exist_ok=True)
        rc, out, err = _run(["mount", "-t", "proc", "proc", str(proc_dst)], check=False)
        if rc != 0:
            logger.warning(f"mount proc failed: {err or out}")
        else:
            mounts.append({"type": "proc", "src": "proc", "dst": str(proc_dst)})

    sys_dst = root / "sys"
    sys_dst.mkdir(parents=True, exist_ok=True)
    rc, out, err = _run(["mount", "-t", "sysfs", "sys", str(sys_dst)], check=False)
    if rc == 0:
        mounts.append({"type": "sysfs", "src": "sys", "dst": str(sys_dst)})
    else:
        logger.warning(f"mount sysfs failed: {err or out}")

    # extra safe binds (hosts/resolv)
    for extra in extra_binds:
        src = Path(extra)
        if not src.exists():
            continue
        dst = root / src.relative_to("/")
        dst.parent.mkdir(parents=True, exist_ok=True)
        rc, out, err = _run(["mount", "--bind", str(src), str(dst)], check=False)
        if rc == 0:
            mounts.append({"type": "bind", "src": str(src), "dst": str(dst)})
        else:
            logger.warning(f"bind extra {src} -> {dst} failed: {err or out}")

    # copy resolv.conf if requested (fallback if bind didn't work)
    if copy_resolv:
        try:
            dst = root / "etc" / "resolv.conf"
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not (root / "etc" / "resolv.conf").exists():
                shutil.copy2("/etc/resolv.conf", dst)
                mounts.append({"type": "copy", "src": "/etc/resolv.conf", "dst": str(dst)})
        except Exception as e:
            logger.warning(f"Failed to copy resolv.conf: {e}")

    # record state
    state = _load_state()
    state["chroots"].setdefault(str(root), {})
    state["chroots"][str(root)].update({
        "root": str(root),
        "profile": profile,
        "overlay": overlay,
        "mounts": mounts,
        "prepared_at": int(time.time())
    })
    _save_state(state)
    logger.info(f"Chroot prepared at {root}; mounts={len(mounts)}")
    return mounts

def cleanup_chroot(root: Path, mounts: Optional[List[Dict[str,Any]]] = None, lazy: bool = True, dry_run: bool = False) -> List[Dict[str,Any]]:
    """
    Unmount mounts created by prepare_chroot. If mounts is None, check state file to determine mounts.
    Returns list of unmounted records.
    """
    root = Path(root).resolve()
    logger.info(f"Cleaning up chroot {root} lazy={lazy} dry_run={dry_run}")
    if dry_run:
        logger.info(f"[dry-run] Would cleanup chroot {root}")
        return []

    state = _load_state()
    rec = state.get("chroots", {}).get(str(root))
    if not rec and not mounts:
        logger.warning("No record of mounts; attempting autodetect and best-effort unmount")
        mounts = _detect_mounts_under(root)

    if rec and not mounts:
        mounts = rec.get("mounts", [])

    # Unmount in reverse order for safety
    unmounted = []
    for m in reversed(mounts):
        dst = Path(m.get("dst"))
        if not dst.exists():
            continue
        try:
            if _is_mounted(dst):
                cmd = ["umount", "-l" if lazy else "", str(dst)]
                # remove empty strings
                cmd = [c for c in cmd if c]
                rc, out, err = _run(cmd, check=False)
                if rc == 0:
                    unmounted.append(m)
                    logger.debug(f"Unmounted {dst}")
                else:
                    logger.warning(f"Failed to unmount {dst}: {err or out}")
            else:
                logger.debug(f"Not mounted: {dst}")
        except Exception as e:
            logger.warning(f"Exception unmounting {dst}: {e}")

    # remove overlay tmp dirs if present (upper/work/merged)
    # best-effort: if merged exists and is dir, try to remove it if empty
    try:
        # remove record
        if str(root) in state.get("chroots", {}):
            del state["chroots"][str(root)]
            _save_state(state)
    except Exception:
        pass

    logger.info(f"Cleanup finished for {root}; unmounted={len(unmounted)}")
    return unmounted

def _detect_mounts_under(root: Path) -> List[Dict[str,Any]]:
    """Scan /proc/mounts for entries under root"""
    mounts = []
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                mpoint = os.path.realpath(parts[1])
                if mpoint.startswith(str(root)):
                    mounts.append({"src": parts[0], "dst": mpoint, "type": parts[2]})
    except Exception:
        pass
    return mounts

def list_chroots() -> Dict[str, Any]:
    state = _load_state()
    return state.get("chroots", {})

def is_chroot_ready(root: Path) -> bool:
    root = Path(root).resolve()
    rec = _load_state().get("chroots", {}).get(str(root))
    if not rec:
        # quick auto check
        mounts = _detect_mounts_under(root)
        return any(mounts)
    # verify mounts exist and are mounted
    for m in rec.get("mounts", []):
        dst = Path(m.get("dst"))
        if not _is_mounted(dst):
            return False
    return True

def ensure_network(root: Path, timeout: int = 10) -> bool:
    """
    Ensure DNS resolution inside chroot works by testing /etc/resolv.conf and pinging a host.
    Returns True if network usable.
    """
    root = Path(root).resolve()
    resolv = root / "etc" / "resolv.conf"
    if not resolv.exists():
        logger.warning("resolv.conf missing inside chroot")
        return False
    # try simple DNS resolve using host command inside chroot or fallback to querying via host network
    try:
        rc, out, err = _run(["chroot", str(root), "sh", "-c", "getent hosts ftp.gnu.org >/dev/null 2>&1"], check=False, capture=True, timeout=timeout)
        ok = rc == 0
        logger.debug(f"DNS check inside chroot returned rc={rc}")
        return ok
    except Exception:
        return False

def run_in_chroot(cfg_or_root: Any, command: str, env: Optional[Dict[str,str]] = None,
                  fakeroot: bool = False, dry_run: bool = False, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    """
    Run a shell command inside the chroot. cfg_or_root may be either config dict with path or direct path.
    Returns (rc, stdout, stderr).
    """
    if isinstance(cfg_or_root, dict):
        root = Path(cfg_or_root.get("paths", {}).get("root", "/"))
    else:
        root = Path(cfg_or_root)
    root = root.resolve()

    if dry_run:
        logger.info(f"[dry-run] run in chroot: {command}")
        return 0, "", ""

    # ensure chroot ready
    if not is_chroot_ready(root):
        raise RuntimeError("Chroot not prepared; call prepare_chroot first")

    # build command for chroot: use env and fakeroot wrapper if requested
    cmd = ["chroot", str(root), "sh", "-c", command]
    if fakeroot:
        # wrap with fakeroot if available
        if shutil.which("fakeroot"):
            cmd = ["fakeroot", "--"] + cmd
        else:
            logger.warning("fakeroot requested but not found; proceeding without fakeroot")

    return _run(cmd, check=False, capture=True, timeout=timeout)

def exec_in_chroot(root: Path, argv: List[str], env: Optional[Dict[str,str]] = None,
                   fakeroot: bool = False, dry_run: bool = False) -> int:
    """
    Exec a binary (argv list) inside the chroot. Returns exit code.
    """
    root = Path(root).resolve()
    if dry_run:
        logger.info(f"[dry-run] exec in chroot: {' '.join(argv)}")
        return 0
    if not is_chroot_ready(root):
        raise RuntimeError("Chroot not prepared")
    cmd = ["chroot", str(root)] + argv
    if fakeroot and shutil.which("fakeroot"):
        cmd = ["fakeroot", "--"] + cmd
    rc, out, err = _run(cmd, check=False, capture=True)
    if rc != 0:
        logger.warning(f"exec_in_chroot rc={rc} out={out} err={err}")
    return rc

def verify_chroot(root: Path, full: bool = False) -> Dict[str, Any]:
    """
    Run diagnostics on a chroot. Returns a report dict with mounted, missing, problems.
    """
    root = Path(root).resolve()
    rec = _load_state().get("chroots", {}).get(str(root), {})
    mounts = rec.get("mounts", []) if rec else _detect_mounts_under(root)
    problems = []
    mounted = []
    for m in mounts:
        dst = Path(m.get("dst"))
        ok = _is_mounted(dst)
        mounted.append({"dst": m.get("dst"), "mounted": ok, "type": m.get("type")})
        if not ok:
            problems.append(f"Not mounted: {dst}")
    net_ok = ensure_network(root)
    if not net_ok:
        problems.append("Network (DNS) appears not working inside chroot")
    report = {"root": str(root), "mounted": mounted, "problems": problems, "checked_at": int(time.time())}
    if full:
        # extra checks: check /proc entries and /dev presence
        proc_ok = (root / "proc").exists() and _is_mounted(root / "proc")
        dev_ok = (root / "dev").exists()
        report["proc_ok"] = proc_ok
        report["dev_ok"] = dev_ok
    return report

def force_cleanup_all(dry_run: bool = False) -> List[str]:
    """
    Force cleanup of all recorded chroots in state file. Returns list of cleaned roots.
    """
    state = _load_state()
    roots = list(state.get("chroots", {}).keys())
    cleaned = []
    for r in roots:
        try:
            if dry_run:
                logger.info(f"[dry-run] would cleanup {r}")
                cleaned.append(r)
                continue
            rec = state["chroots"].get(r, {})
            mounts = rec.get("mounts", [])
            cleanup_chroot(Path(r), mounts=mounts, lazy=True, dry_run=False)
            # remove entry
            state = _load_state()
            state.get("chroots", {}).pop(r, None)
            _save_state(state)
            cleaned.append(r)
        except Exception as e:
            logger.warning(f"Failed to cleanup {r}: {e}")
    return cleaned

def cleanup_stale(threshold_secs: int = 3600, dry_run: bool = False) -> List[str]:
    """
    Cleanup chroots older than threshold (based on prepared_at). Returns list cleaned.
    """
    state = _load_state()
    now = int(time.time())
    cleaned = []
    for r, rec in list(state.get("chroots", {}).items()):
        prepared = rec.get("prepared_at", 0)
        if now - prepared > threshold_secs:
            if dry_run:
                logger.info(f"[dry-run] stale chroot {r} would be cleaned (age={now-prepared}s)")
                cleaned.append(r)
                continue
            try:
                cleanup_chroot(Path(r), mounts=rec.get("mounts", []), lazy=True, dry_run=False)
                state = _load_state()
                state.get("chroots", {}).pop(r, None)
                _save_state(state)
                cleaned.append(r)
            except Exception as e:
                logger.warning(f"Failed cleaning stale chroot {r}: {e}")
    return cleaned

# ---- CLI ----------------------------------------------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-chroot", description="Manage safe chroots for building")
    sub = parser.add_subparsers(dest="cmd")
    p_prep = sub.add_parser("prepare")
    p_prep.add_argument("root", help="root path to prepare")
    p_prep.add_argument("--profile", default="lfs", help="profile (lfs|blfs|x11|minimal)")
    p_prep.add_argument("--overlay", action="store_true", help="use overlayfs")
    p_prep.add_argument("--dry-run", action="store_true")
    p_prep.add_argument("--force", action="store_true")

    p_clean = sub.add_parser("cleanup")
    p_clean.add_argument("root", help="root path to cleanup")
    p_clean.add_argument("--no-lazy", action="store_true", help="do not use lazy unmount")
    p_clean.add_argument("--dry-run", action="store_true")

    p_list = sub.add_parser("list")
    p_verify = sub.add_parser("verify")
    p_verify.add_argument("root", help="root to verify")
    p_verify.add_argument("--full", action="store_true")
    p_force = sub.add_parser("force-clean")
    p_force.add_argument("--dry-run", action="store_true")

    p_stale = sub.add_parser("cleanup-stale")
    p_stale.add_argument("--age", type=int, default=3600)
    p_stale.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    if args.cmd == "prepare":
        mounts = prepare_chroot(Path(args.root), profile=args.profile, overlay=args.overlay, dry_run=args.dry_run, force=args.force)
        print(json.dumps(mounts, indent=2))
    elif args.cmd == "cleanup":
        mounts = cleanup_chroot(Path(args.root), lazy=not args.no_lazy, dry_run=args.dry_run)
        print(json.dumps(mounts, indent=2))
    elif args.cmd == "list":
        print(json.dumps(list_chroots(), indent=2))
    elif args.cmd == "verify":
        print(json.dumps(verify_chroot(Path(args.root), full=args.full), indent=2))
    elif args.cmd == "force-clean":
        res = force_cleanup_all(dry_run=args.dry_run)
        print(json.dumps(res, indent=2))
    elif args.cmd == "cleanup-stale":
        res = cleanup_stale(threshold_secs=args.age, dry_run=args.dry_run)
        print(json.dumps(res, indent=2))
    else:
        parser.print_help()

if __name__ == "__main__":
    _cli()
