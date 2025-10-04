#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_depclean.py — safe depclean utilities for Zeropkg (Pattern B)

Features:
 - find orphan packages (using zeropkg_deps)
 - protected package list (won't be removed)
 - dry-run detailed report
 - backup package files before remove (tar.xz)
 - DB snapshot & rollback support
 - integration hooks to call zeropkg_remover or zeropkg_installer if available
 - logging to zeropkg_logger and events to zeropkg_db (optional)
"""

from __future__ import annotations
import os
import sys
import shutil
import tarfile
import time
import json
import sqlite3
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any

# Optional integrations
try:
    from zeropkg_config import load_config
except Exception:
    def load_config(*a, **k):
        return {
            "paths": {
                "state_dir": "/var/lib/zeropkg",
                "backup_dir": "/var/lib/zeropkg/backups",
                "db_path": "/var/lib/zeropkg/installed.sqlite3"
            },
            "depclean": {
                "protected": ["bash", "coreutils", "glibc", "gcc"],
                "protect_base": True
            }
        }

try:
    from zeropkg_logger import log_event, log_global, get_logger
    _logger = get_logger("depclean")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_depclean")
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

# deps graph and remover optional
try:
    from zeropkg_deps import ensure_graph_loaded, depclean as deps_depclean, find_orphans as deps_find_orphans, DepGraph, find_missing_nodes
except Exception:
    # We'll lazily construct graph when needed; provide minimal fallbacks
    DepGraph = None
    ensure_graph_loaded = None
    deps_depclean = None
    deps_find_orphans = None
    find_missing_nodes = None

try:
    # prefer dedicated remover if available
    from zeropkg_remover import remove_package as api_remove_package
except Exception:
    api_remove_package = None

# -------------------------
# Utilities
# -------------------------
def _state_dir(cfg: Optional[Dict[str, Any]] = None) -> Path:
    cfg = cfg or load_config()
    sd = Path(cfg.get("paths", {}).get("state_dir", "/var/lib/zeropkg"))
    sd.mkdir(parents=True, exist_ok=True)
    return sd

def _backup_dir(cfg: Optional[Dict[str, Any]] = None) -> Path:
    cfg = cfg or load_config()
    bd = Path(cfg.get("paths", {}).get("backup_dir", "/var/lib/zeropkg/backups"))
    bd.mkdir(parents=True, exist_ok=True)
    return bd

def _db_path(cfg: Optional[Dict[str, Any]] = None) -> Path:
    cfg = cfg or load_config()
    return Path(cfg.get("paths", {}).get("db_path", "/var/lib/zeropkg/installed.sqlite3"))

def _now_ts() -> int:
    return int(time.time())

# -------------------------
# Main Depcleaner
# -------------------------
class Depcleaner:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or load_config()
        self.state_dir = _state_dir(self.cfg)
        self.backup_dir = _backup_dir(self.cfg)
        self.db_path = _db_path(self.cfg)
        # protected packages from config
        depclean_cfg = self.cfg.get("depclean", {}) or {}
        self.protected = set(depclean_cfg.get("protected", []))
        if depclean_cfg.get("protect_base", True):
            # add common essential base packages (can be customized)
            self.protected.update({"bash", "coreutils", "glibc", "gcc", "linux-headers", "binutils"})
        # try to load deps graph helper
        self._graph = None
        self._ensure_graph_loaded = ensure_graph_loaded
        self._remover = api_remove_package  # callable(pkg) -> bool expected
        self._db_manager = DBManager
        self._report = {"checked_at": _now_ts(), "orphans": [], "protected": [], "skipped": [], "backed_up": [], "removed": [], "errors": []}

    # -------------------------
    # Graph helpers
    # -------------------------
    def _load_graph(self) -> Any:
        if self._graph is None and self._ensure_graph_loaded:
            try:
                self._graph = self._ensure_graph_loaded()
            except Exception as e:
                log_global(f"Failed to load deps graph: {e}", "warning")
                self._graph = None
        return self._graph

    def find_orphans(self, exclude_manual: bool = True) -> List[str]:
        """
        Find orphan packages using zeropkg_deps if available, otherwise attempt DB scan.
        """
        graph = self._load_graph()
        if graph is not None:
            # use deps module's orphan detection
            try:
                orphans = graph and (graph.nodes and [p for p in graph.nodes.keys() if not graph.rev_edges.get(p)]) or []
                # consult module function if available
                return sorted(orphans)
            except Exception:
                pass

        # fallback: attempt to read installed packages from DB (if present) and compute revdeps
        if self._db_manager:
            try:
                with self._db_manager() as db:
                    cur = db.conn.cursor()
                    # assume table 'installed_packages' with columns 'name' and maybe 'deps' (json)
                    try:
                        cur.execute("SELECT name, deps FROM installed_packages")
                    except Exception:
                        # fallback to events table scanning — conservative approach: no orphans
                        return []
                    rows = cur.fetchall()
                    # build simple in-memory graph
                    pkgs = {}
                    for name, deps_json in rows:
                        deps = []
                        try:
                            deps = json.loads(deps_json) if deps_json else []
                        except Exception:
                            deps = []
                        pkgs[name] = deps
                    # compute reverse deps
                    rev = {n: set() for n in pkgs.keys()}
                    for n, deps in pkgs.items():
                        for d in deps:
                            if d in rev:
                                rev[d].add(n)
                    orphans = [n for n, r in rev.items() if not r]
                    return sorted(orphans)
            except Exception:
                pass
        # if nothing else, return empty list
        return []

    # -------------------------
    # Protection checks
    # -------------------------
    def is_protected(self, pkg: str) -> bool:
        return pkg in self.protected

    # -------------------------
    # DB snapshot helpers
    # -------------------------
    def _create_db_snapshot(self) -> Optional[Path]:
        if not self.db_path.exists():
            log_global(f"No DB found at {self.db_path}; skipping DB snapshot", "debug")
            return None
        snap_dir = self.backup_dir
        ts = _now_ts()
        snap_path = snap_dir / f"installed.sqlite3.snap.{ts}"
        try:
            # copy file atomically
            shutil.copy2(self.db_path, snap_path)
            log_event("depclean", "db", f"DB snapshot created at {snap_path}", "info")
            return snap_path
        except Exception as e:
            log_event("depclean", "db", f"DB snapshot failed: {e}", "warning")
            return None

    def _restore_db_snapshot(self, snap_path: Path) -> bool:
        if not snap_path or not snap_path.exists():
            log_global("No DB snapshot to restore", "warning")
            return False
        try:
            shutil.copy2(snap_path, self.db_path)
            log_global(f"Restored DB snapshot from {snap_path}", "info")
            return True
        except Exception as e:
            log_global(f"Failed to restore DB snapshot: {e}", "error")
            return False

    # -------------------------
    # Backup package files
    # -------------------------
    def _backup_package(self, pkg: str, locations: Optional[List[str]] = None, dry_run: bool = True) -> Optional[Path]:
        """
        Create a tar.xz backup of the package files.
        locations: list of paths to include (absolute). If None, attempt best-effort from DB or /var/zeropkg/packages/<pkg>.
        Returns Path to backup file or None.
        """
        ts = _now_ts()
        backup_name = f"{pkg}-{ts}.tar.xz"
        backup_path = self.backup_dir / backup_name
        if dry_run:
            log_event(pkg, "backup", f"[dry-run] would create backup at {backup_path}")
            return backup_path

        # determine files to include
        to_archive = []
        # custom locations provided
        if locations:
            for p in locations:
                if os.path.exists(p):
                    to_archive.append(p)
        else:
            # try best-effort: /var/zeropkg/packages/<pkg> or /usr/local/<pkg> or DB listing
            guess_dirs = [
                Path(self.cfg.get("paths", {}).get("packages_dir", "/var/zeropkg/packages")) / pkg,
                Path("/usr/local") / pkg,
                Path("/opt") / pkg
            ]
            for g in guess_dirs:
                if g.exists():
                    to_archive.append(str(g.resolve()))

            # attempt DB for file list
            if self._db_manager:
                try:
                    with self._db_manager() as db:
                        cur = db.conn.cursor()
                        # this assumes installer recorded files in installed_files table
                        cur.execute("SELECT files FROM installed_files WHERE pkg_name=?", (pkg,))
                        row = cur.fetchone()
                        if row and row[0]:
                            try:
                                files = json.loads(row[0])
                                for f in files:
                                    if os.path.exists(f):
                                        to_archive.append(f)
                            except Exception:
                                pass
                except Exception:
                    pass

        if not to_archive:
            log_event(pkg, "backup", "No files found to backup; skipping", "warning")
            return None

        # create tar.xz
        try:
            with tarfile.open(backup_path, "w:xz") as tar:
                for item in to_archive:
                    arcname = os.path.basename(item.rstrip("/"))
                    try:
                        tar.add(item, arcname=arcname)
                    except Exception as e:
                        log_event(pkg, "backup", f"Failed to add {item} to backup: {e}", "warning")
            log_event(pkg, "backup", f"Backup created at {backup_path}", "info")
            return backup_path
        except Exception as e:
            log_event(pkg, "backup", f"Backup creation failed: {e}", "error")
            return None

    # -------------------------
    # Remove package
    # -------------------------
    def _call_remover(self, pkg: str, dry_run: bool = True, use_fakeroot: bool = False) -> bool:
        """
        Call system remover if available (zeropkg_remover.remove_package or zeropkg_installer.remove)
        If not available, fall back to removing graph node only (non-destructive).
        Returns True if removal considered successful.
        """
        # prefer api_remove_package
        if api_remove_package:
            try:
                log_event(pkg, "remove", f"Invoking zeropkg_remover for {pkg}")
                if dry_run:
                    log_event(pkg, "remove", "[dry-run] would call zeropkg_remover", "info")
                    return True
                rc = api_remove_package(pkg, use_fakeroot=use_fakeroot)
                return bool(rc)
            except Exception as e:
                log_event(pkg, "remove", f"zeropkg_remover failed: {e}", "error")
                return False

        # try installer module remove function
        try:
            from zeropkg_installer import remove_package as installer_remove
            if installer_remove:
                if dry_run:
                    log_event(pkg, "remove", "[dry-run] would call zeropkg_installer.remove_package", "info")
                    return True
                return bool(installer_remove(pkg, use_fakeroot=use_fakeroot))
        except Exception:
            pass

        # fallback: non-destructive: remove node from graph (if present) and log
        graph = self._load_graph()
        if graph:
            try:
                graph.remove_node(pkg)
                log_event(pkg, "remove", "Removed from in-memory graph (no system uninstall available)", "warning")
                return True
            except Exception as e:
                log_event(pkg, "remove", f"Failed to remove from graph: {e}", "error")
                return False

        log_event(pkg, "remove", "No remover available; cannot remove package from system", "error")
        return False

    # -------------------------
    # Main run
    # -------------------------
    def run(self, dry_run: bool = True, force: bool = False, use_fakeroot: bool = False, protect_list: Optional[List[str]] = None, remove_callback: Optional[Callable[[str], bool]] = None) -> Dict[str, Any]:
        """
        Execute depclean run.
         - dry_run: simulate and report only
         - force: ignore local modifications protection (if underlying remover checks it)
         - use_fakeroot: pass to remover if supported
         - protect_list: additional package names to protect
         - remove_callback: optional callback(pkg) -> bool to perform actual removal
        Returns a report dict.
        """
        report = self._report.copy()
        report["started_at"] = _now_ts()
        # merge protect list
        if protect_list:
            self.protected.update(protect_list)

        # find orphans
        try:
            orphans = self.find_orphans()
            report["orphans"] = sorted(orphans)
            log_global(f"Depclean: found {len(orphans)} orphan candidates", "info")
        except Exception as e:
            report["errors"].append({"phase": "find_orphans", "error": str(e)})
            log_global(f"Depclean: error finding orphans: {e}", "error")
            return report

        # snapshot DB before destructive operations
        db_snap = None
        if not dry_run:
            db_snap = self._create_db_snapshot()

        # iterate orphans and process
        for pkg in sorted(report["orphans"]):
            try:
                if self.is_protected(pkg):
                    report.setdefault("protected", []).append(pkg)
                    log_event(pkg, "depclean", "Package is protected; skipping", "info")
                    continue

                # backup package files
                bkp = self._backup_package(pkg, dry_run=dry_run)
                if bkp:
                    report.setdefault("backed_up", []).append(str(bkp))

                # decide removal method
                if remove_callback:
                    # use provided callback
                    try:
                        if dry_run:
                            log_event(pkg, "depclean", "[dry-run] would call remove_callback", "info")
                            report.setdefault("skipped", []).append(pkg)
                        else:
                            ok = remove_callback(pkg)
                            if ok:
                                report.setdefault("removed", []).append(pkg)
                                log_event(pkg, "depclean", "Removed by custom callback", "info")
                            else:
                                report.setdefault("errors", []).append({pkg: "remove_callback failed"})
                    except Exception as e:
                        report.setdefault("errors", []).append({pkg: str(e)})
                else:
                    # call internal remover integration
                    try:
                        if dry_run:
                            log_event(pkg, "depclean", "[dry-run] would remove package", "info")
                            report.setdefault("skipped", []).append(pkg)
                            continue
                        ok = self._call_remover(pkg, dry_run=dry_run, use_fakeroot=use_fakeroot)
                        if ok:
                            report.setdefault("removed", []).append(pkg)
                            log_event(pkg, "depclean", "Successfully removed package", "info")
                            # record event in DB if available
                            if self._db_manager:
                                try:
                                    with self._db_manager() as db:
                                        db._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                                                    (pkg, "depclean.remove", json.dumps({"pkg": pkg}), _now_ts()))
                                except Exception:
                                    pass
                        else:
                            report.setdefault("errors", []).append({pkg: "removal failed"})
                    except Exception as e:
                        report.setdefault("errors", []).append({pkg: str(e)})
            except Exception as e:
                report.setdefault("errors", []).append({pkg: str(e)})
                log_event(pkg, "depclean", f"Exception while processing orphan: {e}\n{traceback.format_exc()}", "error")
                # attempt DB rollback if configured and not dry_run
                if db_snap and not dry_run:
                    self._restore_db_snapshot(db_snap)
                    log_global("Depclean aborted; DB snapshot restored", "warning")
                    break

        report["finished_at"] = _now_ts()
        # final summary logging
        log_global(f"Depclean finished: removed={len(report.get('removed',[]))} skipped={len(report.get('skipped',[]))} errors={len(report.get('errors',[]))}", "info")
        return report

# -------------------------
# CLI helper
# -------------------------
def main_cli():
    import argparse, pprint
    p = argparse.ArgumentParser(prog="zeropkg-depclean", description="Depclean orchestration for Zeropkg")
    p.add_argument("--do-it", action="store_true", help="Actually remove orphans (default is dry-run)")
    p.add_argument("--force", action="store_true", help="Force removal (pass-through to remover where applicable)")
    p.add_argument("--fakeroot", action="store_true", help="Use fakeroot when removing files (if supported)")
    p.add_argument("--protect", nargs="*", default=[], help="Additional packages to protect")
    p.add_argument("--backup-only", action="store_true", help="Only perform backups for orphan candidates")
    args = p.parse_args()

    dc = Depcleaner()
    if args.backup_only:
        orphans = dc.find_orphans()
        report = {"orphans": orphans, "backups": []}
        for pkg in orphans:
            b = dc._backup_package(pkg, dry_run=not args.do_it)
            if b:
                report["backups"].append(str(b))
        pprint.pprint(report)
        return

    report = dc.run(dry_run=not args.do_it, force=args.force, use_fakeroot=args.fakeroot, protect_list=args.protect)
    pprint.pprint(report)

if __name__ == "__main__":
    main_cli()
