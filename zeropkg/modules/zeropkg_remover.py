#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_remover.py — safe package removal for Zeropkg

Features:
 - Remover class with remove(pkg, dry_run, force, backup, use_fakeroot)
 - Pre/post remove hooks execution
 - Backup before remove (integrates with depclean backup or local strategy)
 - Integration with zeropkg_chroot to ensure safe environment
 - Integration with zeropkg_deps to optionally remove dependents
 - DB event logging and zeropkg_logger integration
 - Module-level convenience function remove_package(...) for other modules
 - CLI helper for interactive usage
"""

from __future__ import annotations
import os
import sys
import json
import shutil
import tarfile
import time
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Tuple

# Optional integrations (non-fatal)
try:
    from zeropkg_config import load_config
except Exception:
    def load_config(*a, **k):
        return {
            "paths": {
                "state_dir": "/var/lib/zeropkg",
                "backup_dir": "/var/lib/zeropkg/backups",
                "packages_dir": "/var/zeropkg/packages",
                "db_path": "/var/lib/zeropkg/installed.sqlite3"
            },
            "remove": {"protect_base": True, "protected": ["bash", "coreutils", "glibc", "gcc"]}
        }

try:
    from zeropkg_logger import log_event, log_global, get_logger
    _logger = get_logger("remover")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_remover")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(msg)

# DB optional manager
try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# deps and depclean integration (optional)
try:
    from zeropkg_deps import ensure_graph_loaded, find_revdeps, rebuild_cache
except Exception:
    ensure_graph_loaded = None
    find_revdeps = None
    rebuild_cache = None

# patcher, installer, chroot integration
try:
    from zeropkg_patcher import Patcher, apply_all_stages_from_meta
except Exception:
    Patcher = None
    apply_all_stages_from_meta = None

try:
    from zeropkg_depclean import Depcleaner
except Exception:
    Depcleaner = None

try:
    from zeropkg_chroot import is_chroot_ready
except Exception:
    is_chroot_ready = None

# -------------------------
# Helpers
# -------------------------
def _state_dir(cfg: Optional[Dict[str,Any]] = None) -> Path:
    cfg = cfg or load_config()
    sd = Path(cfg.get("paths", {}).get("state_dir", "/var/lib/zeropkg"))
    sd.mkdir(parents=True, exist_ok=True)
    return sd

def _backup_dir(cfg: Optional[Dict[str,Any]] = None) -> Path:
    cfg = cfg or load_config()
    bd = Path(cfg.get("paths", {}).get("backup_dir", "/var/lib/zeropkg/backups"))
    bd.mkdir(parents=True, exist_ok=True)
    return bd

def _packages_dir(cfg: Optional[Dict[str,Any]] = None) -> Path:
    cfg = cfg or load_config()
    pd = Path(cfg.get("paths", {}).get("packages_dir", "/var/zeropkg/packages"))
    pd.mkdir(parents=True, exist_ok=True)
    return pd

def _now_ts() -> int:
    return int(time.time())

# Safe tar creation helper
def _create_tar_xz(paths: List[str], dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(dest, "w:xz") as tar:
        for p in paths:
            ppath = Path(p)
            if not ppath.exists():
                continue
            # add preserving basename to avoid absolute paths
            tar.add(str(ppath), arcname=ppath.name)
    return dest

# -------------------------
# Remover
# -------------------------
class Remover:
    def __init__(self, cfg: Optional[Dict[str,Any]] = None):
        self.cfg = cfg or load_config()
        remove_cfg = self.cfg.get("remove", {}) or {}
        self.protected = set(remove_cfg.get("protected", []))
        if remove_cfg.get("protect_base", True):
            self.protected.update({"bash", "coreutils", "glibc", "gcc", "linux-headers", "binutils"})
        self.state_dir = _state_dir(self.cfg)
        self.backup_dir = _backup_dir(self.cfg)
        self.packages_dir = _packages_dir(self.cfg)
        self.db_path = Path(self.cfg.get("paths", {}).get("db_path", "/var/lib/zeropkg/installed.sqlite3"))
        self._db = DBManager
        # callbacks/hooks (user can set)
        self.pre_remove_hook: Optional[Callable[[str], None]] = None
        self.post_remove_hook: Optional[Callable[[str], None]] = None

    # -------------------------
    # Protections & checks
    # -------------------------
    def is_protected(self, pkg: str) -> bool:
        return pkg in self.protected

    def check_chroot_ready(self, root: Optional[str] = None) -> bool:
        if is_chroot_ready is None:
            return True
        try:
            return is_chroot_ready(root or self.cfg.get("paths", {}).get("lfs_root", "/mnt/lfs"))
        except Exception:
            return False

    # -------------------------
    # Backup before removal
    # -------------------------
    def backup_package(self, pkg: str, locations: Optional[List[str]] = None, dry_run: bool = True) -> Optional[Path]:
        """
        Create a tar.xz backup of likely package locations.
        Returns Path to backup file (or simulated path in dry-run).
        """
        ts = _now_ts()
        backup_name = f"{pkg}-{ts}.tar.xz"
        backup_path = self.backup_dir / backup_name
        if dry_run:
            log_event(pkg, "backup", f"[dry-run] would create backup at {backup_path}", "info")
            return backup_path

        # if locations provided, use them, else guess
        paths = []
        if locations:
            for p in locations:
                if os.path.exists(p):
                    paths.append(p)
        else:
            # guess typical install dirs
            guess = [
                str(self.packages_dir / pkg),
                f"/usr/local/{pkg}",
                f"/opt/{pkg}",
                f"/usr/{pkg}",
            ]
            for g in guess:
                if os.path.exists(g):
                    paths.append(g)
            # attempt to read installed_files from db
            if self._db:
                try:
                    with self._db() as db:
                        cur = db.conn.cursor()
                        cur.execute("SELECT files FROM installed_files WHERE pkg_name=?", (pkg,))
                        row = cur.fetchone()
                        if row and row[0]:
                            try:
                                files = json.loads(row[0])
                                for f in files:
                                    if os.path.exists(f):
                                        paths.append(f)
                            except Exception:
                                pass
                except Exception:
                    pass

        if not paths:
            log_event(pkg, "backup", "No files found to include in backup; skipping", "warning")
            return None

        try:
            path = _create_tar_xz(paths, backup_path)
            log_event(pkg, "backup", f"Backup created at {path}", "info")
            return path
        except Exception as e:
            log_event(pkg, "backup", f"Backup failed: {e}", "error")
            return None

    # -------------------------
    # Core remove logic
    # -------------------------
    def _call_pre_hook(self, pkg: str, dry_run: bool = False):
        if self.pre_remove_hook:
            try:
                if dry_run:
                    log_event(pkg, "hook.pre_remove", f"[dry-run] would call pre_remove_hook", "debug")
                else:
                    self.pre_remove_hook(pkg)
            except Exception as e:
                log_event(pkg, "hook.pre_remove", f"pre-remove hook failed: {e}", "error")
                raise

    def _call_post_hook(self, pkg: str, dry_run: bool = False):
        if self.post_remove_hook:
            try:
                if dry_run:
                    log_event(pkg, "hook.post_remove", f"[dry-run] would call post_remove_hook", "debug")
                else:
                    self.post_remove_hook(pkg)
            except Exception as e:
                log_event(pkg, "hook.post_remove", f"post-remove hook failed: {e}", "error")
                raise

    def _remove_files_from_db_list(self, pkg: str, dry_run: bool = False) -> Tuple[int,int]:
        """
        Attempt to remove files listed in installed_files table for pkg.
        Returns (removed_count, errors_count).
        """
        removed = 0
        errors = 0
        if not self._db:
            log_event(pkg, "remove.files", "No DBManager available; skipping file list removal", "debug")
            return removed, errors
        try:
            with self._db() as db:
                cur = db.conn.cursor()
                cur.execute("SELECT files FROM installed_files WHERE pkg_name=?", (pkg,))
                row = cur.fetchone()
                if not row or not row[0]:
                    log_event(pkg, "remove.files", "No installed_files entry; skipping", "debug")
                    return removed, errors
                try:
                    files = json.loads(row[0])
                except Exception:
                    files = []
                for f in files:
                    try:
                        if dry_run:
                            log_event(pkg, "remove.file", f"[dry-run] would remove {f}", "info")
                            removed += 1
                        else:
                            if os.path.isdir(f):
                                shutil.rmtree(f)
                            elif os.path.exists(f):
                                os.remove(f)
                            else:
                                log_event(pkg, "remove.file", f"File not found: {f}", "debug")
                                errors += 1
                            removed += 1
                    except Exception as e:
                        errors += 1
                        log_event(pkg, "remove.file", f"Failed to remove {f}: {e}", "warning")
                # remove DB entries
                if not dry_run:
                    cur.execute("DELETE FROM installed_files WHERE pkg_name=?", (pkg,))
                    db.conn.commit()
            return removed, errors
        except Exception as e:
            log_event(pkg, "remove.files", f"DB file removal failed: {e}", "error")
            return removed, errors

    def _remove_package_impl(self, pkg: str, dry_run: bool = True, force: bool = False, use_fakeroot: bool = False) -> bool:
        """
        Core destructive action. Tries to remove files via DB file list, fallback to removing package dir.
        Returns True if operation considered successful.
        """
        # if DB has uninstall script record, try to run it (experimental)
        # call pre-hook
        self._call_pre_hook(pkg, dry_run=dry_run)

        # attempt to remove via installed_files
        removed, errs = self._remove_files_from_db_list(pkg, dry_run=dry_run)
        if removed > 0 and errs == 0:
            log_event(pkg, "remove", f"Removed {removed} files (from DB list)", "info")
        elif removed == 0:
            # fallback: remove package dir under packages_dir
            candidate = self.packages_dir / pkg
            if candidate.exists():
                try:
                    if dry_run:
                        log_event(pkg, "remove", f"[dry-run] would remove directory {candidate}", "info")
                    else:
                        shutil.rmtree(candidate)
                        log_event(pkg, "remove", f"Removed directory {candidate}", "info")
                except Exception as e:
                    log_event(pkg, "remove", f"Failed to remove directory {candidate}: {e}", "error")
                    if not force:
                        return False
            else:
                log_event(pkg, "remove", "No files removed and no package directory found; assuming already removed", "debug")

        # remove DB installed package record if present
        if self._db and not dry_run:
            try:
                with self._db() as db:
                    cur = db.conn.cursor()
                    cur.execute("DELETE FROM installed_packages WHERE name=?", (pkg,))
                    db.conn.commit()
            except Exception:
                log_event(pkg, "remove", "Failed to remove installed_packages db entry (non-fatal)", "warning")

        # call post hook
        self._call_post_hook(pkg, dry_run=dry_run)

        return True

    def remove(self, pkg: str, dry_run: bool = True, force: bool = False, backup: bool = True, backups_locations: Optional[List[str]] = None, use_fakeroot: bool = False, with_dependents: bool = False) -> Dict[str, Any]:
        """
        Public API: remove a package.
        Returns report: {pkg, ok, dry_run, backup: path_or_None, removed_dependents: [], errors: []}
        """
        report = {"pkg": pkg, "ok": False, "dry_run": bool(dry_run), "backup": None, "removed_dependents": [], "errors": []}
        try:
            if self.is_protected(pkg) and not force:
                msg = "Package is protected and cannot be removed (use force to override)"
                report["errors"].append(msg)
                log_event(pkg, "remove", msg, "warning")
                return report

            # check chroot readiness if requested by config
            if not self.check_chroot_ready():
                log_event(pkg, "remove", "Chroot or environment not ready; continuing but this may be unsafe", "warning")

            # backup if requested
            bpath = None
            if backup:
                bpath = self.backup_package(pkg, locations=backups_locations, dry_run=dry_run)
                report["backup"] = str(bpath) if bpath else None

            # if removing with dependents, compute list
            dependents = []
            if with_dependents and find_revdeps:
                try:
                    dependents = find_revdeps(ensure_graph_loaded(), pkg, deep=True)
                    # remove pkg itself first from the list? We'll remove dependents as well
                    dependents = [d for d in dependents if d != pkg]
                except Exception:
                    dependents = []

            # actual removal for dependents first (so leaf packages removed first)
            all_to_remove = ([pkg] + dependents) if dependents else [pkg]
            # ensure reverse order (dependents first) - graph usually lists dependents upstream, but we want leaf removal
            # we'll remove dependents first
            for rpkg in list(all_to_remove)[1:]:
                # remove dependent
                try:
                    ok = self._remove_package_impl(rpkg, dry_run=dry_run, force=force, use_fakeroot=use_fakeroot)
                    if ok:
                        report["removed_dependents"].append(rpkg)
                    else:
                        report["errors"].append(f"failed to remove dependent {rpkg}")
                        if not force:
                            return report
                except Exception as e:
                    report["errors"].append(str(e))
                    if not force:
                        return report

            # now remove main pkg
            ok_main = self._remove_package_impl(pkg, dry_run=dry_run, force=force, use_fakeroot=use_fakeroot)
            report["ok"] = bool(ok_main)
            if not ok_main:
                report["errors"].append("Failed to remove main package")

            # rebuild deps cache if available and not dry_run
            if rebuild_cache and not dry_run:
                try:
                    rebuild_cache()
                    log_global("Dependency cache rebuilt after removal", "debug")
                except Exception as e:
                    log_global(f"Failed to rebuild dependency cache: {e}", "warning")

            # record event to DB
            if self._db and not dry_run:
                try:
                    with self._db() as db:
                        payload = {"pkg": pkg, "backup": str(bpath) if bpath else None, "dependents": report["removed_dependents"]}
                        db._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                                    (pkg, "remove", json.dumps(payload), _now_ts()))
                except Exception:
                    log_event(pkg, "remove", "Failed to log event to DB (non-fatal)", "warning")

            return report
        except Exception as e:
            report["errors"].append(str(e))
            log_event(pkg, "remove", f"Exception during removal: {e}\n{traceback.format_exc()}", "error")
            return report

    # Convenience: remove multiple packages
    def remove_multiple(self, pkgs: List[str], dry_run: bool = True, force: bool = False, backup: bool = True, use_fakeroot: bool = False) -> Dict[str,Any]:
        results = {}
        for p in pkgs:
            results[p] = self.remove(p, dry_run=dry_run, force=force, backup=backup, use_fakeroot=use_fakeroot)
        return results

# -------------------------
# Module-level convenience function
# -------------------------
_global_remover = Remover()

def remove_package(pkg: str, dry_run: bool = True, force: bool = False, backup: bool = True, use_fakeroot: bool = False, with_dependents: bool = False) -> Dict[str, Any]:
    """
    Module-level function used by other modules to remove a package.
    Returns same report as Remover.remove()
    """
    return _global_remover.remove(pkg, dry_run=dry_run, force=force, backup=backup, use_fakeroot=use_fakeroot, with_dependents=with_dependents)

# -------------------------
# CLI helper
# -------------------------
def main():
    import argparse, pprint
    p = argparse.ArgumentParser(prog="zeropkg-remove", description="Remove installed packages safely with Zeropkg")
    p.add_argument("packages", nargs="+", help="Package names to remove")
    p.add_argument("--do-it", action="store_true", help="Actually remove (default is dry-run)")
    p.add_argument("--force", action="store_true", help="Force removal (override protections)")
    p.add_argument("--no-backup", dest="backup", action="store_false", help="Do not create backup before removing")
    p.add_argument("--fakeroot", action="store_true", help="Use fakeroot when removing (if supported)")
    p.add_argument("--with-dependents", action="store_true", help="Also remove packages that depend on the targets")
    args = p.parse_args()

    remover = Remover()
    remover.pre_remove_hook = None
    remover.post_remove_hook = None

    reports = {}
    for pkg in args.packages:
        rep = remover.remove(pkg, dry_run=not args.do_it, force=args.force, backup=args.backup, use_fakeroot=args.fakeroot, with_dependents=args.with_dependents)
        reports[pkg] = rep

    pprint.pprint(reports)
    # exit code non-zero if any failed removal and --do-it used
    if args.do_it and any(not r.get("ok") for r in reports.values()):
        sys.exit(2)
    sys.exit(0)

if __name__ == "__main__":
    main()
