#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_upgrade.py — módulo de upgrade avançado para Zeropkg

Funcionalidades:
 - upgrade de pacote(s) com build, install, verificação, e rollback
 - registro no DB de antes/depois (se disponível)
 - integração com vuln check (zeropkg_vuln) antes/depois
 - hooks globais e por-pacote (pre_upgrade/post_upgrade)
 - execução em chroot/fakeroot suportada
 - paralelismo controlável (--jobs/--parallel)
 - backup automático antes do upgrade (usando zeropkg_remover backup)
 - detecção de revdeps quebrados e tentativa de rebuild
 - relatório JSON final (/var/log/zeropkg/upgrade-report-<ts>.json)
 - dry-run mode
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import traceback
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- logger safe import ----
try:
    from zeropkg_logger import get_logger, log_event
    logger = get_logger("upgrade")
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_upgrade")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    def log_event(pkg, stage, msg, level="info"):
        getattr(logger, level)(f"{pkg}:{stage} {msg}")

# ---- optional integrations ----
try:
    from zeropkg_builder import ZeropkgBuilder
    BUILDER_AVAILABLE = True
except Exception:
    BUILDER_AVAILABLE = False
    ZeropkgBuilder = None

try:
    from zeropkg_installer import ZeropkgInstaller
    INSTALLER_AVAILABLE = True
except Exception:
    INSTALLER_AVAILABLE = False
    ZeropkgInstaller = None

try:
    from zeropkg_remover import Remover
    REMOVER_AVAILABLE = True
except Exception:
    REMOVER_AVAILABLE = False
    Remover = None

try:
    from zeropkg_db import ZeroPKGDB, record_install_quick, remove_package_quick, record_upgrade_event
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False
    ZeroPKGDB = None
    record_install_quick = None
    remove_package_quick = None
    record_upgrade_event = None

try:
    from zeropkg_vuln import ZeroPKGVulnManager
    VULN_AVAILABLE = True
except Exception:
    VULN_AVAILABLE = False
    ZeroPKGVulnManager = None

try:
    from zeropkg_deps import DepsManager
    DEPS_AVAILABLE = True
except Exception:
    DEPS_AVAILABLE = False
    DepsManager = None

try:
    from zeropkg_depclean import ZeroPKGDepClean
    DEPCLEAN_AVAILABLE = True
except Exception:
    DEPCLEAN_AVAILABLE = False
    ZeroPKGDepClean = None

try:
    from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot
    CHROOT_AVAILABLE = True
except Exception:
    CHROOT_AVAILABLE = False

# ---- config defaults ----
DEFAULT_REPORT_DIR = Path("/var/log/zeropkg")
DEFAULT_BACKUP_DIR = Path("/var/lib/zeropkg/backups")
DEFAULT_HOOKS_DIR = Path("/etc/zeropkg/hooks.d")
DEFAULT_CFG = {
    "backup": {"dir": str(DEFAULT_BACKUP_DIR), "enabled": True},
    "report_dir": str(DEFAULT_REPORT_DIR),
    "parallel_upgrades": False,
    "jobs": 2,
    "chroot": {"use_chroot": True, "root": "/mnt/lfs"},
    "hooks": {"global_dir": str(DEFAULT_HOOKS_DIR)}
}

# ensure dirs
DEFAULT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_HOOKS_DIR.mkdir(parents=True, exist_ok=True)

# ---- helpers ----
def _atomic_write(path: Path, data: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)

def _now_ts() -> int:
    return int(time.time())

def _safe_str(x):
    try:
        return str(x)
    except Exception:
        return repr(x)

# ---- Upgrade manager ----
class ZeropkgUpgrade:
    def __init__(self, cfg: Optional[Dict[str,Any]] = None):
        self.cfg = cfg or DEFAULT_CFG
        self.report_dir = Path(self.cfg.get("report_dir", DEFAULT_REPORT_DIR))
        self.backup_dir = Path(self.cfg.get("backup", {}).get("dir", DEFAULT_BACKUP_DIR))
        self.hooks_dir = Path(self.cfg.get("hooks", {}).get("global_dir", DEFAULT_HOOKS_DIR))
        self.jobs = int(self.cfg.get("jobs", 2))
        self.parallel = bool(self.cfg.get("parallel_upgrades", False))
        # integrations
        self.builder = ZeropkgBuilder() if BUILDER_AVAILABLE else None
        self.installer = ZeropkgInstaller() if INSTALLER_AVAILABLE else None
        self.remover = Remover() if REMOVER_AVAILABLE else None
        self.vuln = ZeroPKGVulnManager() if VULN_AVAILABLE else None
        self.deps = DepsManager() if DEPS_AVAILABLE else None
        self.depclean = ZeroPKGDepClean() if DEPCLEAN_AVAILABLE else None
        self.chroot_cfg = self.cfg.get("chroot", {})

    # -------------------------
    # hooks helpers: global + package-local
    # -------------------------
    def _run_hook_cmd(self, cmd: str, pkg: str, stage: str, use_chroot: bool = False, fakeroot: bool = False, dry_run: bool = False) -> Tuple[bool, str]:
        """
        Run a hook command. Return (ok, output/error).
        If use_chroot True, attempt run_in_chroot.
        """
        logger.info(f"[hook] {stage} for {pkg}: {cmd}")
        if dry_run:
            return True, "[dry-run]"
        try:
            if use_chroot and CHROOT_AVAILABLE:
                rc, out, err = run_in_chroot(self.chroot_cfg.get("root", "/"), cmd, fakeroot=fakeroot, dry_run=False)
                ok = rc == 0
                return ok, out if ok else err
            else:
                res = subprocess_run_capture(cmd)
                return res[0] == 0, res[1] if res[1] else res[2]
        except Exception as e:
            logger.warning(f"Hook failed: {e}")
            return False, str(e)

    def _run_pre_hooks(self, pkg: str, recipe_hooks: Optional[Dict[str,Any]], use_chroot: bool, fakeroot: bool, dry_run: bool):
        # global hooks (scripts in hooks_dir)
        if self.hooks_dir.exists():
            for h in sorted(self.hooks_dir.iterdir()):
                if h.is_file() and os.access(h, os.X_OK):
                    self._run_hook_cmd(str(h), pkg, "pre_upgrade_global", use_chroot, fakeroot, dry_run)
        # recipe-local
        if recipe_hooks and "pre_upgrade" in recipe_hooks:
            for cmd in recipe_hooks["pre_upgrade"]:
                self._run_hook_cmd(cmd, pkg, "pre_upgrade_local", use_chroot, fakeroot, dry_run)

    def _run_post_hooks(self, pkg: str, recipe_hooks: Optional[Dict[str,Any]], use_chroot: bool, fakeroot: bool, dry_run: bool):
        if recipe_hooks and "post_upgrade" in recipe_hooks:
            for cmd in recipe_hooks["post_upgrade"]:
                self._run_hook_cmd(cmd, pkg, "post_upgrade_local", use_chroot, fakeroot, dry_run)
        if self.hooks_dir.exists():
            for h in sorted(self.hooks_dir.iterdir()):
                if h.is_file() and os.access(h, os.X_OK):
                    self._run_hook_cmd(str(h), pkg, "post_upgrade_global", use_chroot, fakeroot, dry_run)

    # -------------------------
    # utility to run shell capturing
    # -------------------------
    def _subproc_capture(self, cmd: List[str], cwd: Optional[str] = None) -> Tuple[int, str, str]:
        return subprocess_run_capture(cmd, cwd)

    # -------------------------
    # backup wrapper (uses Remover.backup_package if available)
    # -------------------------
    def _backup_package(self, pkg: str, dry_run: bool) -> Optional[str]:
        if REMOVER_AVAILABLE and self.remover:
            try:
                b = self.remover.backup_package(pkg, dry_run=dry_run)
                if b:
                    return str(b)
                return None
            except Exception as e:
                logger.warning(f"Backup via remover failed: {e}")
                return None
        # fallback: try to tar common paths
        try:
            ts = _now_ts()
            dest = self.backup_dir / f"{pkg}-{ts}.tar.xz"
            if dry_run:
                logger.info(f"[dry-run] would backup to {dest}")
                return str(dest)
            # try to identify likely pkg paths and tar them if exist
            candidates = []
            for p in (f"/usr/{pkg}", f"/usr/local/{pkg}", f"/opt/{pkg}"):
                if Path(p).exists():
                    candidates.append(p)
            if not candidates:
                logger.info("No files to backup for package (fallback)")
                return None
            with tempfile.TemporaryDirectory() as td:
                tf = Path(td) / "pack.tar"
                import tarfile
                with tarfile.open(tf, "w") as tar:
                    for c in candidates:
                        tar.add(c, arcname=os.path.basename(c))
                # compress xz
                shutil.move(str(tf), str(dest.with_suffix(".tar")))
                # attempt compress with xz via shutil if available fallback skip
                try:
                    _ = subprocess_run_capture(["xz", "-z", str(dest.with_suffix(".tar)")])
                except Exception:
                    pass
                return str(dest)
        except Exception as e:
            logger.warning(f"Fallback backup failed: {e}")
            return None

    # -------------------------
    # build+install of single package (atomic)
    # returns dict with details and status
    # -------------------------
    def _upgrade_one(self, recipe_path: str, *,
                     dry_run: bool = False,
                     use_chroot: Optional[bool] = None,
                     fakeroot: bool = False,
                     force: bool = False,
                     create_backup: bool = True,
                     jobs: Optional[int] = None) -> Dict[str,Any]:
        """
        Steps:
         1. parse recipe (we delegate to builder._load_recipe if available)
         2. backup currently installed package
         3. run pre-upgrade hooks
         4. build package via builder
         5. install via installer
         6. run post-upgrade hooks
         7. verify (vuln check) and revdep check
         8. record DB event
         9. return structured result
        """
        rp = Path(recipe_path)
        pkg_name = rp.stem
        details = {"pkg": pkg_name, "recipe": str(rp), "start": _now_ts(), "dry_run": dry_run}
        logger.info(f"Starting upgrade for {pkg_name} (recipe={rp}) dry_run={dry_run}")

        # load recipe metadata via builder if possible
        recipe = {}
        if BUILDER_AVAILABLE and self.builder:
            try:
                recipe = self.builder._load_recipe(rp)
            except Exception as e:
                logger.debug(f"builder._load_recipe failed: {e}")
        # fallback small metadata
        if not recipe:
            recipe = {"package": {"name": pkg_name, "version": None}, "hooks": {}, "options": {}}

        current_version = None
        try:
            if DB_AVAILABLE and self.remover is None:
                # try to get installed manifest via DB (best effort)
                try:
                    db = ZeroPKGDB()
                    rec = getattr(db, "get_package_manifest", lambda n: None)(pkg_name)
                    if rec and isinstance(rec, dict):
                        current_version = rec.get("version")
                except Exception:
                    current_version = None
        except Exception:
            current_version = None

        details["current_version"] = current_version
        new_version = recipe.get("package", {}).get("version")
        details["new_version"] = new_version

        # vuln check BEFORE build: if there are critical CVEs for candidate version, abort unless force
        if VULN_AVAILABLE and self.vuln:
            try:
                vulns_for_new = self.vuln.vulndb.get_vulns(pkg_name)
                critical = [v for v in (vulns_for_new or []) if v.get("severity") == "critical"]
                # If recipe new version present in vulndb and critical, block
                if critical and not force:
                    msg = f"New version has {len(critical)} critical vulnerabilities; aborting upgrade"
                    logger.warning(msg)
                    details["ok"] = False
                    details["error"] = "vuln-critical"
                    return details
            except Exception:
                pass

        # backup current package if required
        backup_path = None
        if create_backup:
            try:
                backup_path = self._backup_package(pkg_name, dry_run)
                details["backup"] = backup_path
            except Exception as e:
                logger.warning(f"Backup step failed: {e}")

        # run pre-upgrade hooks
        try:
            self._run_pre_hooks(pkg_name, recipe.get("hooks"), use_chroot=bool(self.chroot_cfg.get("use_chroot", False)), fakeroot=fakeroot, dry_run=dry_run)
        except Exception as e:
            logger.warning(f"pre-upgrade hooks failed: {e}")

        # build via builder
        build_result = None
        try:
            if dry_run:
                logger.info(f"[dry-run] would build {pkg_name}")
                build_result = {"dry_run": True}
            else:
                if BUILDER_AVAILABLE and self.builder:
                    # try to call builder.build with recipe path
                    build_result = self.builder.build(str(rp), use_chroot=bool(self.chroot_cfg.get("use_chroot", False)), fakeroot=fakeroot, dry_run=dry_run, jobs=jobs or self.jobs)
                else:
                    raise RuntimeError("Builder not available")
            details["build_result"] = build_result
        except Exception as e:
            logger.error(f"Build failed for {pkg_name}: {e}")
            details["ok"] = False
            details["error"] = f"build-failed: {e}"
            # rollback if necessary
            if backup_path and not dry_run:
                logger.info("Attempting rollback after build failure")
                self._attempt_rollback(pkg_name, backup_path)
            return details

        # install via installer
        try:
            if dry_run:
                logger.info(f"[dry-run] would install {pkg_name}")
                install_result = {"dry_run": True}
            else:
                if INSTALLER_AVAILABLE and self.installer:
                    # If builder produced a pkg archive, prefer that; else use build pkgroot
                    archive = build_result.get("pkg_archive") if isinstance(build_result, dict) else None
                    if archive:
                        install_result = self.installer.install_from_archive(Path(archive), pkg_name=pkg_name, version=new_version, root=self.chroot_cfg.get("root", "/"), fakeroot=fakeroot, use_chroot=bool(self.chroot_cfg.get("use_chroot", False)))
                    else:
                        # If builder returns build_dir or pkgroot path, try to detect
                        build_pkgroot = None
                        if isinstance(build_result, dict):
                            # heuristics: builder may include "pkgroot" or "pkg_output" or summary
                            build_pkgroot = build_result.get("pkgroot") or build_result.get("build_output_dir")
                        if build_pkgroot:
                            install_result = self.installer.install_from_build(pkg_name, Path(build_pkgroot), version=new_version, root=self.chroot_cfg.get("root", "/"), fakeroot=fakeroot, use_chroot=bool(self.chroot_cfg.get("use_chroot", False)))
                        else:
                            # fallback: if builder saved archive to binpkg dir, find recent
                            archive_candidates = list(Path(self.builder.binpkg_dir).glob(f"{pkg_name}-*.tar.*")) if BUILDER_AVAILABLE and self.builder else []
                            archive_candidates = sorted(archive_candidates, key=lambda p: p.stat().st_mtime, reverse=True)
                            if archive_candidates:
                                install_result = self.installer.install_from_archive(archive_candidates[0], pkg_name=pkg_name, version=new_version, root=self.chroot_cfg.get("root", "/"), fakeroot=fakeroot, use_chroot=bool(self.chroot_cfg.get("use_chroot", False)))
                            else:
                                raise RuntimeError("No artifact found to install")
                else:
                    raise RuntimeError("Installer not available")
            details["install_result"] = install_result
        except Exception as e:
            logger.error(f"Install failed for {pkg_name}: {e}")
            details["ok"] = False
            details["error"] = f"install-failed: {e}"
            if backup_path and not dry_run:
                logger.info("Attempting rollback after install failure")
                self._attempt_rollback(pkg_name, backup_path)
            return details

        # post-upgrade hooks
        try:
            self._run_post_hooks(pkg_name, recipe.get("hooks"), use_chroot=bool(self.chroot_cfg.get("use_chroot", False)), fakeroot=fakeroot, dry_run=dry_run)
        except Exception as e:
            logger.warning(f"post-upgrade hooks failed: {e}")

        # vulnerability check AFTER upgrade — if upgrade introduced critical vulns and not forced, rollback
        if VULN_AVAILABLE and self.vuln:
            try:
                after_vulns = self.vuln.vulndb.get_vulns(pkg_name)
                critical_after = [v for v in (after_vulns or []) if v.get("severity") == "critical"]
                if critical_after and not force:
                    logger.warning(f"Upgrade produced critical vulnerabilities ({len(critical_after)}). Rolling back.")
                    details["ok"] = False
                    details["error"] = "vuln-introduced"
                    if backup_path and not dry_run:
                        self._attempt_rollback(pkg_name, backup_path)
                    return details
            except Exception:
                pass

        # revdep / dependency verification: check if reverse deps are broken; attempt rebuild
        revdep_issues = []
        try:
            if DEPS_AVAILABLE and self.deps:
                revs = self.deps.revdeps(pkg_name)
                # for each revdep, attempt to verify installation presence (via DB or filesystem)
                for r in revs:
                    # simple check: try to build or at least register need: we will attempt a rebuild
                    try:
                        # attempt to rebuild revdep
                        if not dry_run and BUILDER_AVAILABLE and self.builder:
                            # try to find recipe for revdep and build it
                            recp = self.builder._find_recipe_for_pkg(r) if hasattr(self.builder, "_find_recipe_for_pkg") else None
                            if recp:
                                logger.info(f"Rebuilding reverse-dependent {r} after upgrading {pkg_name}")
                                br = self.builder.build(str(recp), use_chroot=bool(self.chroot_cfg.get("use_chroot", False)), fakeroot=fakeroot, dry_run=dry_run)
                                # attempt install
                                if INSTALLER_AVAILABLE and self.installer and isinstance(br, dict):
                                    arch = br.get("pkg_archive")
                                    if arch:
                                        self.installer.install_from_archive(Path(arch), pkg_name=r, version=br.get("version"), root=self.chroot_cfg.get("root", "/"), fakeroot=fakeroot)
                    except Exception as e:
                        logger.warning(f"Revdep rebuild failed for {r}: {e}")
                        revdep_issues.append({"pkg": r, "error": _safe_str(e)})
        except Exception as e:
            logger.debug(f"revdep check failed: {e}")

        # record DB upgrade event (old->new)
        try:
            if DB_AVAILABLE and record_upgrade_event:
                ev = {"pkg": pkg_name, "old": current_version, "new": new_version, "ts": _now_ts(), "success": True}
                try:
                    record_upgrade_event(ev)
                except Exception:
                    logger.debug("record_upgrade_event failed")
        except Exception:
            pass

        details["ok"] = True
        details["revdep_issues"] = revdep_issues
        details["finished"] = _now_ts()
        logger.info(f"Upgrade completed for {pkg_name}")
        return details

    # -------------------------
    # rollback helper — tries to restore from backup path (produced earlier)
    # -------------------------
    def _attempt_rollback(self, pkg: str, backup_path: str):
        logger.info(f"Attempting rollback of {pkg} using backup {backup_path}")
        try:
            if REMOVER_AVAILABLE and self.remover:
                # use remover to restore: remover.backup_package created tar.xz or tar, so we extract into /
                p = Path(backup_path)
                if not p.exists():
                    logger.warning("Backup not found for rollback")
                    return False
                # extract into root
                try:
                    if str(p).endswith(".tar.xz") or str(p).endswith(".tar") or str(p).endswith(".tar.gz") or str(p).endswith(".tar.zst"):
                        # use tar direct
                        _code, out, err = subprocess_run_capture(["tar", "-C", "/", "-xf", str(p)])
                        if _code != 0:
                            logger.warning(f"tar extraction returned err: {err}")
                            return False
                        logger.info("Rollback extraction finished")
                        return True
                    else:
                        # fallback: try shutil.unpack_archive
                        shutil.unpack_archive(str(p), "/")
                        return True
                except Exception as e:
                    logger.error(f"Rollback extraction failed: {e}")
                    return False
            else:
                # fallback attempt: no remover interface; just warn
                logger.warning("No remover integration for rollback; manual restore required")
                return False
        except Exception as e:
            logger.error(f"Rollback attempt exception: {e}")
            return False

    # -------------------------
    # Public: upgrade list of recipe paths or package names
    # -------------------------
    def upgrade(self,
                targets: List[str],
                *,
                dry_run: bool = False,
                parallel: Optional[bool] = None,
                jobs: Optional[int] = None,
                force: bool = False,
                create_backup: bool = True,
                use_chroot: Optional[bool] = None,
                fakeroot: bool = False) -> Dict[str,Any]:
        """
        targets: list of recipe paths or package names (if name, try to find recipe via builder._find_recipe_for_pkg)
        Returns aggregated report dict and writes JSON report to report_dir.
        """
        if parallel is None:
            parallel = self.parallel

        jobs = jobs or self.jobs
        results = {}
        start_ts = _now_ts()
        logger.info(f"Starting upgrade for targets={targets} parallel={parallel} jobs={jobs} dry_run={dry_run}")

        # normalize targets to recipe paths when possible
        recipe_paths = []
        for t in targets:
            p = Path(t)
            if p.exists():
                recipe_paths.append(str(p))
            else:
                # try to find via builder
                if BUILDER_AVAILABLE and self.builder:
                    try:
                        rp = self.builder._find_recipe_for_pkg(t)
                        if rp:
                            recipe_paths.append(str(rp))
                        else:
                            recipe_paths.append(t)  # leave as name (builder may accept)
                    except Exception:
                        recipe_paths.append(t)
                else:
                    recipe_paths.append(t)

        # run in parallel or sequential
        if parallel and jobs > 1:
            with ThreadPoolExecutor(max_workers=jobs) as ex:
                futs = {}
                for rp in recipe_paths:
                    fut = ex.submit(self._upgrade_one, rp, dry_run=dry_run, use_chroot=use_chroot, fakeroot=fakeroot, force=force, create_backup=create_backup, jobs=jobs)
                    futs[fut] = rp
                for fut in as_completed(futs):
                    rp = futs[fut]
                    try:
                        res = fut.result()
                    except Exception as e:
                        res = {"ok": False, "error": _safe_str(e)}
                    results[rp] = res
        else:
            for rp in recipe_paths:
                try:
                    res = self._upgrade_one(rp, dry_run=dry_run, use_chroot=use_chroot, fakeroot=fakeroot, force=force, create_backup=create_backup, jobs=jobs)
                except Exception as e:
                    res = {"ok": False, "error": _safe_str(e)}
                results[rp] = res

        # write aggregated report
        report = {
            "ts": start_ts,
            "finished": _now_ts(),
            "targets": targets,
            "results": results
        }
        try:
            fname = self.report_dir / f"upgrade-report-{start_ts}.json"
            _atomic_write(fname, report)
            logger.info(f"Upgrade report written to {fname}")
        except Exception as e:
            logger.warning(f"Failed to write report: {e}")

        return report

# ---- small cross-module helper ----
def subprocess_run_capture(cmd, cwd=None) -> Tuple[int,str,str]:
    """
    Accepts either list of args or str command.
    Returns (rc, stdout, stderr)
    """
    import subprocess
    if isinstance(cmd, (list, tuple)):
        try:
            p = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.returncode, p.stdout.strip(), p.stderr.strip()
        except Exception as e:
            return 1, "", str(e)
    else:
        try:
            p = subprocess.run(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.returncode, p.stdout.strip(), p.stderr.strip()
        except Exception as e:
            return 1, "", str(e)

# ---- CLI ----
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-upgrade", description="Zeropkg upgrade tool")
    parser.add_argument("targets", nargs="+", help="Recipe paths or package names to upgrade")
    parser.add_argument("--dry-run", action="store_true", help="Simulate upgrade without making changes")
    parser.add_argument("--jobs", "-j", type=int, default=None, help="Parallel jobs for upgrade/build")
    parser.add_argument("--parallel", action="store_true", help="Perform upgrades in parallel (uses --jobs)")
    parser.add_argument("--no-backup", dest="create_backup", action="store_false", help="Do not create backup before upgrade")
    parser.add_argument("--force", action="store_true", help="Force upgrade despite warnings (vuln etc.)")
    parser.add_argument("--fakeroot", action="store_true", help="Use fakeroot for install operations")
    parser.add_argument("--no-chroot", dest="use_chroot", action="store_false", help="Do not use chroot even if configured")
    args = parser.parse_args()

    up = ZeropkgUpgrade()
    res = up.upgrade(args.targets, dry_run=args.dry_run, parallel=args.parallel, jobs=args.jobs, force=args.force, create_backup=args.create_backup, use_chroot=(None if args.use_chroot is None else args.use_chroot), fakeroot=args.fakeroot)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    _cli()
