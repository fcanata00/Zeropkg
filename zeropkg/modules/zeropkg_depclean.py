#!/usr/bin/env python3
# zeropkg_depclean.py — Depclean completo e seguro
# -*- coding: utf-8 -*-

"""
zeropkg_depclean.py

Remove pacotes órfãos de forma segura, integrada com DependencyResolver e Installer.

Funcionalidades:
- detecta órfãos (DependencyResolver.find_orphans)
- checa reverse deps (DependencyResolver.reverse_deps)
- remove com Installer.remove (respeita chroot/fakeroot/dry-run)
- dry-run, force, logging, relatório final
- rollback: tenta restaurar pacotes removidos a partir de backups em /var/zeropkg/backups
- registra eventos via zeropkg_logger.log_event
"""

from __future__ import annotations

import os
import logging
import shutil
import time
from typing import List, Tuple, Dict, Optional

from zeropkg_deps import DependencyResolver, DependencyError
from zeropkg_installer import Installer, InstallError
from zeropkg_db import DBManager
from zeropkg_logger import log_event

LOG_PATH = "/var/log/zeropkg/depclean.log"
BACKUP_DIR = "/var/zeropkg/backups"
PKG_CACHE_DIR = "/var/zeropkg/packages"


# Setup module logger
logger = logging.getLogger("zeropkg.depclean")
logger.setLevel(logging.DEBUG)
# Add file handler if not already added
if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == LOG_PATH for h in logger.handlers):
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        fh = logging.FileHandler(LOG_PATH)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(fh)
    except Exception:
        # fallback to default logger if can't write file
        pass


class DepcleanError(Exception):
    pass


class DepCleaner:
    def __init__(
        self,
        db_path: str = "/var/lib/zeropkg/installed.sqlite3",
        ports_dir: str = "/usr/ports",
        root: str = "/",
        dry_run: bool = False,
        use_fakeroot: bool = False,
        packages_dir: str = PKG_CACHE_DIR,
        backups_dir: str = BACKUP_DIR,
    ):
        """
        db_path: caminho para o DB
        ports_dir: onde estão as recipes
        root: destino ("/" ou "/mnt/lfs")
        dry_run: True para simular apenas
        use_fakeroot: repassa para Installer
        packages_dir: diretório onde packages gerados são colocados
        backups_dir: local onde armazenar/ler backups
        """
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = os.path.abspath(root or "/")
        self.dry_run = bool(dry_run)
        self.use_fakeroot = bool(use_fakeroot)
        self.packages_dir = packages_dir
        self.backups_dir = backups_dir

        # Ensure dirs exist (for logging/backups)
        try:
            os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        except Exception:
            pass
        try:
            os.makedirs(self.backups_dir, exist_ok=True)
        except Exception:
            pass

        self.db = DBManager(db_path)
        self.resolver = DependencyResolver(db_path, ports_dir)
        self.installer = Installer(db_path=db_path, ports_dir=ports_dir, root=self.root, dry_run=self.dry_run, use_fakeroot=self.use_fakeroot)

    # ---------------------------
    # Helpers
    # ---------------------------
    def _log(self, pkg: str, action: str, msg: str, level: str = "info"):
        """
        Wrapper for logging both to file logger and zeropkg's log_event
        """
        try:
            if level == "debug":
                logger.debug(f"{pkg} {action}: {msg}")
                log_event(pkg, action, msg)
            elif level == "warning":
                logger.warning(f"{pkg} {action}: {msg}")
                log_event(pkg, action, msg, level="warning")
            elif level == "error":
                logger.error(f"{pkg} {action}: {msg}")
                log_event(pkg, action, msg, level="error")
            else:
                logger.info(f"{pkg} {action}: {msg}")
                log_event(pkg, action, msg)
        except Exception:
            # best-effort: don't fail on logging
            pass

    def _find_backup_for(self, pkgname: str) -> Optional[str]:
        """
        Try to locate a backup package tarball for pkgname in backups_dir or packages_dir.
        Returns full path if found, else None.
        """
        candidates = []
        # look for common file patterns: pkgname-*.tar.xz or pkgname*.tar.*
        for d in (self.backups_dir, self.packages_dir):
            try:
                for ext in ("tar.xz", "tar.gz", "tar.bz2", "tar"):
                    p = os.path.join(d, f"{pkgname}-*.{ext}")
                    import glob
                    found = glob.glob(p)
                    if found:
                        # prefer newest
                        candidates.extend(found)
            except Exception:
                continue
        if not candidates:
            return None
        candidates = sorted(candidates, key=lambda x: os.path.getmtime(x), reverse=True)
        return candidates[0]

    def _attempt_restore(self, pkgname: str, args) -> bool:
        """
        Try to restore a previously removed package by locating a backup and calling installer.install.
        Returns True on success.
        """
        backup = self._find_backup_for(pkgname)
        if not backup:
            self._log(pkgname, "rollback", "No backup package available for restore", level="warning")
            return False
        try:
            self._log(pkgname, "rollback", f"Attempting restore from backup {backup}")
            # call installer.install to restore
            self.installer.install(pkgname, args, pkg_file=backup, meta=None, dir_install=self.root)
            self._log(pkgname, "rollback", f"Restore succeeded from {backup}")
            return True
        except Exception as e:
            self._log(pkgname, "rollback", f"Restore failed: {e}", level="error")
            return False

    # ---------------------------
    # Core functions
    # ---------------------------
    def list_orphans(self) -> List[str]:
        """
        Return list of orphan package names detected by DependencyResolver.
        """
        try:
            orphans = self.resolver.find_orphans()
            self._log("depclean", "scan", f"Found {len(orphans)} orphans: {', '.join(orphans) if orphans else '<none>'}")
            return orphans
        except Exception as e:
            self._log("depclean", "scan", f"Failed to detect orphans: {e}", level="error")
            raise DepcleanError(f"Failed to detect orphans: {e}")

    def preview(self) -> Dict[str, List[str]]:
        """
        Return a dict with:
          - orphans: list of orphans
          - blocked: list of orphans that have reverse deps (won't be removed unless forced)
        """
        orphans = self.list_orphans()
        blocked = []
        for pkg in orphans:
            try:
                revs = self.resolver.reverse_deps(pkg)
                if revs:
                    blocked.append(pkg)
            except Exception:
                blocked.append(pkg)
        return {"orphans": orphans, "blocked": blocked}

    def clean(self, force: bool = False, args=None) -> Dict[str, List[str]]:
        """
        Perform depclean:
          - force: if True, ignore reverse deps and attempt removal
          - args: CLI args object, passed to installer for dry_run/fakeroot settings and to restore calls
        Returns summary dict: { "removed": [], "skipped": [], "failed": [] }
        """
        summary = {"removed": [], "skipped": [], "failed": []}

        orphans = self.list_orphans()
        if not orphans:
            self._log("depclean", "run", "No orphans found; nothing to do")
            return summary

        # Sort orphans to remove leaf-most first: attempt to remove packages with no dependents first.
        # We'll compute reverse_deps for each orphan and prefer those with fewer reverse deps.
        orphan_priority = []
        for o in orphans:
            try:
                revs = self.resolver.reverse_deps(o)
                orphan_priority.append((len(revs or []), o))
            except Exception:
                orphan_priority.append((999, o))
        orphan_priority.sort()  # fewer dependents first

        # Keep track of removed packages for rollback (in removal order)
        removed_stack: List[str] = []

        # Iterate and attempt removal
        for _, pkg in orphan_priority:
            try:
                revs = self.resolver.reverse_deps(pkg)
                if revs and not force:
                    # skip, report
                    self._log(pkg, "skip", f"Has reverse dependencies: {', '.join(revs)}")
                    summary["skipped"].append(pkg)
                    continue

                # attempt remove via Installer.remove
                self._log(pkg, "remove", f"Attempting removal (dry_run={self.dry_run}, force={force})")
                if self.dry_run:
                    # simulate remove
                    summary["removed"].append(pkg)
                    self._log(pkg, "remove", "[dry-run] would remove")
                    continue

                # call installer.remove; it returns True/False or raises
                try:
                    ok = self.installer.remove(pkg, version=None, hooks=None, force=force)
                except Exception as e:
                    ok = False
                    self._log(pkg, "remove", f"Installer.remove raised: {e}", level="error")

                if ok:
                    removed_stack.append(pkg)
                    summary["removed"].append(pkg)
                    self._log(pkg, "remove", "Removed successfully")
                else:
                    summary["failed"].append(pkg)
                    self._log(pkg, "remove", "Removal failed (installer returned False)", level="error")
                    # decide whether to attempt rollback of what's been removed so far
                    # here we try to rollback removed_stack if any
                    if removed_stack:
                        self._log("depclean", "rollback", "Attempting rollback due to removal failure")
                        self._rollback_removed(removed_stack, args)
                    # continue to next orphan (or abort entirely? prefer continue but record)
            except Exception as e:
                summary["failed"].append(pkg)
                self._log(pkg, "remove", f"Exception during removal: {e}", level="error")
                # attempt rollback of removed_stack
                if removed_stack:
                    self._rollback_removed(removed_stack, args)

        # final summary log
        self._log("depclean", "summary", f"Removed: {len(summary['removed'])}, Skipped: {len(summary['skipped'])}, Failed: {len(summary['failed'])}")
        return summary

    def _rollback_removed(self, removed_stack: List[str], args) -> None:
        """
        Attempt to rollback removed packages in reverse order. Best-effort only.
        For each package, try to restore from backup or packages_dir.
        """
        self._log("depclean", "rollback", f"Starting rollback of {len(removed_stack)} packages")
        for pkg in reversed(removed_stack):
            try:
                ok = self._attempt_restore(pkg, args)
                if ok:
                    self._log(pkg, "rollback", "Restored successfully")
                else:
                    self._log(pkg, "rollback", "Could not restore (no backup or install failed)", level="error")
            except Exception as e:
                self._log(pkg, "rollback", f"Rollback attempt exception: {e}", level="error")
        self._log("depclean", "rollback", "Rollback attempts finished")
