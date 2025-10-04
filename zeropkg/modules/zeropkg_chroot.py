#!/usr/bin/env python3
# zeropkg_chroot.py — helpers robustos para preparar/usar/limpar chroot seguro
# -----------------------------------------------------------------------------
# Features:
#  - prepare_chroot(root, copy_resolv=True, persist_network=False, use_overlay=False, overlay_dir=None, dry_run=False)
#  - cleanup_chroot(root, force_lazy=False, dry_run=False)
#  - enter_chroot(root, command, env=None, cwd='/', use_shell=False, dry_run=False)
#  - run_in_chroot(...) convenience wrapper returning exit code
#  - atexit and signal handlers to ensure cleanup
#  - optional overlayfs support (use_overlay=True)
#  - logs via zeropkg_logger.log_event if available
#
# IMPORTANT: mounting/unmounting requires root privileges. This module
# is defensive: it checks mounts, creates directories with safe modes,
# and refuses to operate on suspicious paths.
# -----------------------------------------------------------------------------

from __future__ import annotations
import os
import sys
import stat
import shutil
import subprocess
import tempfile
import atexit
import signal
import time
from typing import Optional, Sequence, Union

# Try to integrate with zeropkg_logger if present
try:
    from zeropkg_logger import log_event, get_logger
    logger = get_logger("chroot")
except Exception:
    def log_event(pkg, stage, msg, level="info"):
        print(f"[{level.upper()}] {pkg}:{stage} {msg}")
    import logging
    logger = logging.getLogger("zeropkg_chroot")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        logger.addHandler(h)

# Internal registry of prepared chroots to allow safe cleanup
_PREPARED_CHROOTS = {}
_SIGNAL_HANDLERS_INSTALLED = False


class ChrootError(RuntimeError):
    pass


# -----------------------
# Utility helpers
# -----------------------
def _is_root() -> bool:
    try:
        return os.geteuid() == 0
    except AttributeError:
        # non-unix environment - be conservative
        return False


def _safe_mkdir(path: str, mode: int = 0o755):
    os.makedirs(path, exist_ok=True)
    try:
        os.chmod(path, mode)
    except Exception:
        pass


def _is_mounted(target: str) -> bool:
    target = os.path.realpath(target)
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and os.path.realpath(parts[1]) == target:
                    return True
    except FileNotFoundError:
        # no /proc (non-linux or restricted) - fallback to False
        return False
    return False


def _mount(source: str, target: str, fstype: Optional[str] = None, options: Optional[str] = None, dry_run: bool = False):
    if dry_run:
        log_event("chroot", "mount", f"[dry-run] mount {source} -> {target} type={fstype} opts={options}")
        return
    args = ["mount"]
    if fstype:
        args += ["-t", fstype]
    if options:
        args += ["-o", options]
    args += [source, target]
    subprocess.run(args, check=True)


def _umount(target: str, lazy: bool = False, dry_run: bool = False) -> bool:
    if dry_run:
        log_event("chroot", "umount", f"[dry-run] umount {'-l ' if lazy else ''}{target}")
        return True
    cmd = ["umount"]
    if lazy:
        cmd.append("-l")
    cmd.append(target)
    res = subprocess.run(cmd)
    return res.returncode == 0


def _bind_mount(source: str, target: str, readonly: bool = False, dry_run: bool = False):
    _safe_mkdir(target)
    if _is_mounted(target):
        log_event("chroot", "mount", f"{target} already mounted, skipping bind")
        return
    if dry_run:
        log_event("chroot", "bind", f"[dry-run] bind {source} -> {target} (ro={readonly})")
        return
    # bind mount
    subprocess.run(["mount", "--bind", source, target], check=True)
    if readonly:
        subprocess.run(["mount", "-o", "remount,bind,ro", target], check=True)


def _install_signal_handlers():
    global _SIGNAL_HANDLERS_INSTALLED
    if _SIGNAL_HANDLERS_INSTALLED:
        return
    def _handler(signum, frame):
        logger.warning(f"Received signal {signum}, attempting safe cleanup of chroots")
        # attempt cleanup of all prepared chroots (lazy)
        for root in list(_PREPARED_CHROOTS.keys()):
            try:
                cleanup_chroot(root, force_lazy=True, dry_run=False)
            except Exception as e:
                logger.warning(f"Failed to cleanup {root} during signal: {e}")
        # restore default and re-raise
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)
    for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(s, _handler)
        except Exception:
            pass
    _SIGNAL_HANDLERS_INSTALLED = True


# -----------------------
# Public API
# -----------------------
def prepare_chroot(root: str,
                   copy_resolv: bool = True,
                   persist_network: bool = False,
                   use_overlay: bool = False,
                   overlay_dir: Optional[str] = None,
                   readonly_dirs: Optional[Sequence[str]] = None,
                   dry_run: bool = False,
                   require_root: bool = True) -> dict:
    """
    Prepare a secure chroot at `root`.

    Parameters:
      - root: path to chroot root (e.g., /mnt/lfs)
      - copy_resolv: copy /etc/resolv.conf into chroot so it has DNS
      - persist_network: if True, preserves network namespace details (best-effort)
      - use_overlay: if True, create an overlay mount so we don't modify base files
      - overlay_dir: directory to hold upperdir/workdir for overlay; if None, created in /var/tmp
      - readonly_dirs: list of dirs to bind-mount read-only (relative to host), default includes /usr
      - dry_run: simulate actions
      - require_root: raise if not run as root (set False for testing)
    Returns:
      dict with metadata about mounts and overlay (for later cleanup)
    """
    root = os.path.abspath(root)
    if require_root and not _is_root():
        raise ChrootError("prepare_chroot requires root privileges")

    if not os.path.isdir(root):
        raise ChrootError(f"Chroot root does not exist or not a directory: {root}")

    # Safety: prevent accidental operations on '/'
    if os.path.realpath(root) in ("/", ""):
        raise ChrootError("Refusing to operate on /")

    _install_signal_handlers()

    meta = {
        "root": root,
        "overlay": None,
        "mounted": [],
        "readonly": [],
    }

    log_event("chroot", "prepare", f"Preparing chroot: {root}")

    # Optional overlay
    if use_overlay:
        if overlay_dir is None:
            overlay_dir = tempfile.mkdtemp(prefix="zeropkg_overlay_")
        else:
            os.makedirs(overlay_dir, exist_ok=True)
        upper = os.path.join(overlay_dir, "upper")
        work = os.path.join(overlay_dir, "work")
        os.makedirs(upper, exist_ok=True)
        os.makedirs(work, exist_ok=True)
        # mount overlay: lowerdir=root, upperdir=upper, workdir=work, mount point=root
        if dry_run:
            log_event("chroot", "overlay", f"[dry-run] overlay lower={root} upper={upper} work={work} -> {root}")
        else:
            # We mount overlay on the root itself: mount -t overlay overlay -o lowerdir=ROOT,upperdir=UPPER,workdir=WORK ROOT
            opts = f"lowerdir={root},upperdir={upper},workdir={work}"
            subprocess.run(["mount", "-t", "overlay", "overlay", "-o", opts, root], check=True)
        meta["overlay"] = {"overlay_dir": overlay_dir, "upper": upper, "work": work}
        log_event("chroot", "overlay", f"Overlay prepared at {overlay_dir}")

    # default readonly dirs (if provided)
    if readonly_dirs is None:
        readonly_dirs = ["/usr", "/lib", "/lib64", "/opt"]  # won't error if they don't exist

    # Standard pseudo-filesystems and mounts inside chroot
    # Ensure target dirs exist
    for d in ("dev", "proc", "sys", "run", "dev/pts", "dev/shm", "tmp"):
        tgt = os.path.join(root, d)
        _safe_mkdir(tgt, mode=0o755)

    # Bind /dev
    try:
        _bind_mount("/dev", os.path.join(root, "dev"), readonly=False, dry_run=dry_run)
        meta["mounted"].append(os.path.join(root, "dev"))
    except Exception as e:
        raise ChrootError(f"Failed to bind-mount /dev into {root}: {e}")

    # Mount /proc
    try:
        if not _is_mounted(os.path.join(root, "proc")):
            _mount("proc", os.path.join(root, "proc"), fstype="proc", dry_run=dry_run)
            meta["mounted"].append(os.path.join(root, "proc"))
    except Exception as e:
        raise ChrootError(f"Failed to mount proc in {root}: {e}")

    # Mount /sys
    try:
        if not _is_mounted(os.path.join(root, "sys")):
            _mount("sysfs", os.path.join(root, "sys"), fstype="sysfs", dry_run=dry_run)
            meta["mounted"].append(os.path.join(root, "sys"))
    except Exception as e:
        raise ChrootError(f"Failed to mount sysfs in {root}: {e}")

    # Mount /run (tmpfs) if not present
    try:
        if not _is_mounted(os.path.join(root, "run")):
            _mount("tmpfs", os.path.join(root, "run"), fstype="tmpfs", options="mode=0755", dry_run=dry_run)
            meta["mounted"].append(os.path.join(root, "run"))
    except Exception as e:
        raise ChrootError(f"Failed to mount run tmpfs in {root}: {e}")

    # Bind dev/pts and dev/shm
    try:
        _bind_mount("/dev/pts", os.path.join(root, "dev/pts"), readonly=False, dry_run=dry_run)
        meta["mounted"].append(os.path.join(root, "dev/pts"))
    except Exception as e:
        logger.warning(f"dev/pts bind failed: {e}")

    try:
        _bind_mount("/dev/shm", os.path.join(root, "dev/shm"), readonly=False, dry_run=dry_run)
        meta["mounted"].append(os.path.join(root, "dev/shm"))
    except Exception as e:
        logger.warning(f"dev/shm bind failed: {e}")

    # Copy resolv.conf for DNS inside chroot
    if copy_resolv:
        src = "/etc/resolv.conf"
        dst = os.path.join(root, "etc", "resolv.conf")
        _safe_mkdir(os.path.dirname(dst), mode=0o755)
        try:
            if dry_run:
                log_event("chroot", "resolv", f"[dry-run] would copy {src} -> {dst}")
            else:
                shutil.copy2(src, dst)
                log_event("chroot", "resolv", f"Copied {src} -> {dst}")
        except Exception as e:
            logger.warning(f"Failed to copy resolv.conf: {e}")

    # Bind read-only host dirs into chroot as requested
    for rdr in readonly_dirs:
        if not rdr:
            continue
        if not os.path.exists(rdr):
            continue
        targ = os.path.join(root, rdr.lstrip("/"))
        try:
            _bind_mount(rdr, targ, readonly=True, dry_run=dry_run)
            meta["readonly"].append(targ)
        except Exception as e:
            logger.warning(f"Failed to bind-readonly {rdr} -> {targ}: {e}")

    # mark as prepared
    meta_record = {
        "meta": meta,
        "timestamp": time.time()
    }
    _PREPARED_CHROOTS[root] = meta_record
    atexit.register(lambda r=root: cleanup_chroot(r, force_lazy=True, dry_run=False))

    log_event("chroot", "prepare", f"Chroot prepared: {root} (mounted: {meta['mounted']}, overlay={meta['overlay'] is not None})")
    return meta_record


def cleanup_chroot(root: str, force_lazy: bool = False, dry_run: bool = False) -> bool:
    """
    Cleanup a prepared chroot. Attempts to unmount in reverse order and remove overlay metadata.
    force_lazy: try lazy umount (-l) if normal umount fails.
    Returns True on complete success, False if partial.
    """
    root = os.path.abspath(root)
    if root not in _PREPARED_CHROOTS:
        # still try a best-effort cleanup based on common mounts
        log_event("chroot", "cleanup", f"No registry found for {root}, attempting best-effort cleanup")
    else:
        log_event("chroot", "cleanup", f"Cleaning up chroot: {root}")

    success = True
    # Unmount order: dev/pts, dev/shm, dev, proc, sys, run, readonly binds, then overlay
    candidates = [
        os.path.join(root, "dev/pts"),
        os.path.join(root, "dev/shm"),
        os.path.join(root, "dev"),
        os.path.join(root, "proc"),
        os.path.join(root, "sys"),
        os.path.join(root, "run"),
    ]

    # include readonly binds discovered earlier
    meta_record = _PREPARED_CHROOTS.get(root)
    if meta_record:
        meta = meta_record.get("meta", {})
        readonly = meta.get("readonly", [])
        # put readonly after normal mounts
        candidates += readonly
        overlay_info = meta.get("overlay")
        if overlay_info:
            candidates.append(root)  # overlay mounted on root itself
    else:
        overlay_info = None

    # Unmount sequence
    for tgt in candidates:
        if not os.path.exists(tgt):
            continue
        if not _is_mounted(tgt):
            continue
        try:
            if dry_run:
                log_event("chroot", "umount", f"[dry-run] umount {tgt}")
            else:
                res = _umount(tgt, lazy=False, dry_run=dry_run)
                if not res and force_lazy:
                    logger.warning(f"Normal umount failed for {tgt}, trying lazy")
                    res = _umount(tgt, lazy=True, dry_run=dry_run)
                if not res:
                    logger.warning(f"Failed to unmount {tgt}")
                    success = False
                else:
                    log_event("chroot", "umount", f"Unmounted {tgt}")
        except Exception as e:
            logger.warning(f"Exception while unmounting {tgt}: {e}")
            success = False

    # Remove copied resolv.conf if exists (best effort)
    try:
        rconf = os.path.join(root, "etc", "resolv.conf")
        if os.path.exists(rconf):
            try:
                if dry_run:
                    log_event("chroot", "cleanup", f"[dry-run] remove {rconf}")
                else:
                    os.remove(rconf)
                    log_event("chroot", "cleanup", f"Removed {rconf}")
            except Exception:
                pass
    except Exception:
        pass

    # If overlay was used, attempt to unmount and remove upper/work dirs
    if overlay_info:
        try:
            # root itself may be overlay-mounted; try umount root (lazy if necessary)
            if dry_run:
                log_event("chroot", "overlay_cleanup", f"[dry-run] umount overlay @ {root}")
            else:
                if _is_mounted(root):
                    res = _umount(root, lazy=False, dry_run=dry_run)
                    if not res and force_lazy:
                        res = _umount(root, lazy=True, dry_run=dry_run)
                # remove overlay dirs
                ovdir = overlay_info.get("overlay_dir")
                if ovdir and os.path.exists(ovdir):
                    shutil.rmtree(ovdir, ignore_errors=True)
                    log_event("chroot", "overlay_cleanup", f"Removed overlay dir {ovdir}")
        except Exception as e:
            logger.warning(f"Failed cleaning overlay: {e}")
            success = False

    # Finally remove registry entry
    if root in _PREPARED_CHROOTS:
        try:
            del _PREPARED_CHROOTS[root]
        except Exception:
            pass

    log_event("chroot", "cleanup", f"Cleanup of {root} completed (success={success})")
    return success


def enter_chroot(root: str,
                 command: Union[str, Sequence[str]],
                 env: Optional[dict] = None,
                 cwd: str = "/",
                 use_shell: bool = False,
                 dry_run: bool = False,
                 require_root: bool = True) -> int:
    """
    Execute a command inside the prepared chroot.

    - command: string (if use_shell True) or list of args
    - env: dict of environment variables to set (merged with minimal safe environment)
    - cwd: working directory inside chroot
    - use_shell: if True and command is a string, execute via /bin/sh -c
    Returns: exit code of command.
    """

    root = os.path.abspath(root)
    if require_root and not _is_root():
        raise ChrootError("enter_chroot requires root privileges")

    # Ensure chroot appears prepared
    if root not in _PREPARED_CHROOTS:
        logger.warning(f"Chroot {root} not registered as prepared — attempting best-effort prepare")
        prepare_chroot(root, copy_resolv=True, dry_run=dry_run, require_root=require_root)

    # Build environment: start from a minimal safe env
    safe_env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": "/root",
        "TERM": os.environ.get("TERM", "xterm"),
        "LC_ALL": os.environ.get("LC_ALL", "C"),
        "LANG": os.environ.get("LANG", "C")
    }
    if env:
        safe_env.update({str(k): str(v) for k, v in env.items()})

    # Accept string or sequence
    if isinstance(command, str) and not use_shell:
        # prefer to split into args safely
        cmd = ["/bin/sh", "-lc", command]
        use_shell = False
    elif isinstance(command, str) and use_shell:
        cmd = ["/bin/sh", "-c", command]
    else:
        # assume sequence
        cmd = list(command)

    # Build the nsenter / chroot command
    # Use chroot syscall: chroot ROOT && cd CWD && exec ... ; run via /usr/sbin/chroot or "chroot"
    # We'll use: chroot ROOT /bin/sh -c "cd CWD && exec <cmd...>"
    if isinstance(cmd, list):
        inner = " ".join([sh_quote(c) for c in cmd]) if not use_shell else cmd[0]
        chroot_cmd = f"cd {sh_quote(cwd)} && exec {inner}"
        full = ["/usr/sbin/chroot", root, "/bin/sh", "-lc", chroot_cmd]
    else:
        # fallback
        full = ["/usr/sbin/chroot", root, "/bin/sh", "-lc", cmd]

    log_event("chroot", "exec", f"Running in chroot {root}: {command}")
    if dry_run:
        log_event("chroot", "exec", f"[dry-run] {' '.join(full)}")
        return 0

    # Execute
    proc = subprocess.run(full, env=safe_env)
    return proc.returncode


def run_in_chroot(root: str,
                  command: Union[str, Sequence[str]],
                  env: Optional[dict] = None,
                  cwd: str = "/",
                  use_shell: bool = False,
                  dry_run: bool = False) -> int:
    """
    Convenience wrapper returning exit code. Alias of enter_chroot.
    """
    return enter_chroot(root, command, env=env, cwd=cwd, use_shell=use_shell, dry_run=dry_run)


# -----------------------
# Small helpers
# -----------------------
def sh_quote(s: str) -> str:
    # Extremely simple sh quoting for safety
    if not s:
        return "''"
    if all(ch.isalnum() or ch in "-_./" for ch in s):
        return s
    return "'" + s.replace("'", "'\"'\"'") + "'"


# -----------------------
# Module quick-test
# -----------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(prog="zeropkg_chroot", description="Prepare/cleanup chroot helper (dry-run supported)")
    p.add_argument("action", choices=["prepare", "cleanup", "exec"], help="action")
    p.add_argument("--root", required=True, help="chroot root (eg /mnt/lfs)")
    p.add_argument("--cmd", help="command to run inside chroot (for exec)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.action == "prepare":
        print("Preparing:", args.root)
        prepare_chroot(args.root, dry_run=args.dry_run)
    elif args.action == "cleanup":
        print("Cleaning up:", args.root)
        cleanup_chroot(args.root, force_lazy=True, dry_run=args.dry_run)
    else:
        if not args.cmd:
            print("Specify --cmd for exec")
            sys.exit(2)
        rc = enter_chroot(args.root, args.cmd, use_shell=True, dry_run=args.dry_run)
        print("Exit code:", rc)
