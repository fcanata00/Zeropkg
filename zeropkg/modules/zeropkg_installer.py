#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_installer.py — Zeropkg package installer and remover (improved)

Principais funcionalidades adicionadas:
 - Geração automática do manifesto de arquivos (scan do DESTDIR)
 - Registro de arquivos no DB (quando zeropkg_db.DBManager disponível)
 - Integração com zeropkg_chroot: prepare_chroot / run_in_chroot / cleanup_chroot
 - Suporte a fakeroot para copy/install
 - Extração segura para vários formatos (tar.xz, tar.gz, tar.bz2, zip)
 - Hooks pré/pós e logs JSON
 - Função verify_files(pkg) para comparar manifesto DB vs FS
 - Dry-run e parallel (apenas para instalação/conteinerização)
"""

from __future__ import annotations
import os
import sys
import json
import tarfile
import zipfile
import shutil
import traceback
import fnmatch
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# --- Safe imports with graceful fallback ---
try:
    from zeropkg_logger import log_event, log_global, get_logger
    logger = get_logger("installer")
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_installer")
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(logger, level if hasattr(logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(logger, level if hasattr(logger, level) else "info")(msg)

try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        return {
            "paths": {
                "state_dir": "/var/lib/zeropkg",
                "build_dir": "/var/zeropkg/build",
                "backup_dir": "/var/lib/zeropkg/backups",
                "db_path": "/var/lib/zeropkg/installed.sqlite3",
                "log_dir": "/var/lib/zeropkg/logs",
                "manifests_dir": "/var/lib/zeropkg/manifests",
                "ports_dir": "/usr/ports",
                "distfiles": "/usr/ports/distfiles"
            },
            "install": {"fakeroot": True}
        }

try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

try:
    from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot
except Exception:
    def prepare_chroot(root: str):
        Path(root).mkdir(parents=True, exist_ok=True)
    def cleanup_chroot(root: str):
        return
    def run_in_chroot(root: str, cmd: str, env: Optional[Dict[str,str]] = None):
        # best-effort fallback
        env = env or os.environ.copy()
        return os.system(cmd)

try:
    from zeropkg_patcher import Patcher
except Exception:
    Patcher = None

try:
    from zeropkg_deps import DependencyResolver
except Exception:
    DependencyResolver = None

# --- Utilities ---

def _write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

def _safe_extract_file(archive: Path, dest: Path):
    """
    Extract common archive types safely into dest.
    Supports tar.xz, tar.gz, tar.bz2, .zip
    """
    dest.mkdir(parents=True, exist_ok=True)
    name = archive.name.lower()
    if name.endswith((".tar.xz", ".txz", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar")):
        mode = "r:*"
        # Using tarfile with secure extraction check
        with tarfile.open(archive, mode) as tar:
            for member in tar.getmembers():
                member_path = dest / member.name
                abs_dest = os.path.abspath(dest)
                abs_target = os.path.abspath(member_path)
                if not os.path.commonpath([abs_dest, abs_target]) == abs_dest:
                    raise Exception(f"Unsafe extraction path: {member.name}")
            tar.extractall(path=dest)
    elif name.endswith(".zip"):
        with zipfile.ZipFile(archive, "r") as z:
            for member in z.namelist():
                member_path = dest / member
                abs_dest = os.path.abspath(dest)
                abs_target = os.path.abspath(member_path)
                if not os.path.commonpath([abs_dest, abs_target]) == abs_dest:
                    raise Exception(f"Unsafe extraction path: {member}")
            z.extractall(path=dest)
    else:
        raise Exception(f"Unsupported archive type: {archive}")

def _scan_manifest_paths(root: Path) -> Dict[str, List[str]]:
    """
    Walk root and classify files into bins (bin, sbin, lib, include, doc, man, conf, other).
    Return dict with lists of absolute paths (strings).
    """
    mapping = {
        "bin": [],
        "sbin": [],
        "lib": [],
        "include": [],
        "doc": [],
        "man": [],
        "conf": [],
        "other": []
    }
    for dirpath, dirnames, filenames in os.walk(root):
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            rel = os.path.relpath(fpath, start=root)
            # classify by path components
            parts = rel.split(os.sep)
            if parts[0] in ("usr", "bin") and "bin" in parts[:2]:
                mapping["bin"].append(f"/{rel}")
            elif "sbin" in parts[:2] or parts[0] == "sbin":
                mapping["sbin"].append(f"/{rel}")
            elif "/lib" in f"/{rel}" or parts[0] in ("lib","usr") and "lib" in parts[:3]:
                mapping["lib"].append(f"/{rel}")
            elif parts[0] in ("usr",) and "include" in parts:
                mapping["include"].append(f"/{rel}")
            elif "/share/man" in f"/{rel}" or parts[1:2] == ["share"] and "man" in parts:
                mapping["man"].append(f"/{rel}")
            elif parts[0] in ("etc",) or "etc" in parts:
                mapping["conf"].append(f"/{rel}")
            elif "/share/doc" in f"/{rel}" or "doc" in parts:
                mapping["doc"].append(f"/{rel}")
            else:
                mapping["other"].append(f"/{rel}")
    # dedupe and sort
    for k in mapping:
        mapping[k] = sorted(set(mapping[k]))
    return mapping

# --- Installer Class ---

class Installer:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or load_config()
        paths = self.cfg.get("paths", {})
        self.state_dir = Path(paths.get("state_dir", "/var/lib/zeropkg"))
        self.log_dir = Path(paths.get("log_dir", "/var/lib/zeropkg/logs"))
        self.manifests_dir = Path(paths.get("manifests_dir", "/var/lib/zeropkg/manifests"))
        self.db_path = Path(paths.get("db_path", "/var/lib/zeropkg/installed.sqlite3"))
        self.ports_dir = Path(paths.get("ports_dir", "/usr/ports"))
        self.distfiles = Path(paths.get("distfiles", "/usr/ports/distfiles"))
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        self.fakeroot = self.cfg.get("install", {}).get("fakeroot", True)

    # -- Hooks execution --
    def _apply_hooks(self, pkg: str, hooks: List[str], env: Dict[str, str], stage: str, chroot_root: Optional[str] = None):
        for cmd in hooks:
            log_event(pkg, stage, f"Running hook: {cmd}")
            try:
                if chroot_root:
                    run_in_chroot(chroot_root, cmd, env)
                else:
                    rc = os.system(cmd)
                    if rc != 0:
                        log_event(pkg, stage, f"Hook returned {rc}", "warning")
            except Exception as e:
                log_event(pkg, stage, f"Hook failed: {e}", "error")

    # -- Record manifest to DB and disk --
    def _record_manifest(self, pkg: str, manifest: Dict[str, List[str]]):
        try:
            manifest_path = self.manifests_dir / f"{pkg}-file-manifest.json"
            _write_json(manifest_path, {"package": pkg, "manifest": manifest, "timestamp": int(time.time())})
            log_event(pkg, "manifest", f"Wrote manifest to {manifest_path}")
            if DBManager:
                with DBManager(self.db_path) as db:
                    db.record_install(pkg, manifest)  # record_install should accept manifest dict
                    log_event(pkg, "db", "Recorded manifest into DB")
        except Exception as e:
            log_event(pkg, "manifest", f"Failed to record manifest: {e}", "error")

    # -- Verify manifest vs filesystem (for zeropkg verify) --
    def verify_files(self, pkg: str) -> Dict[str, Any]:
        """
        Compare registered manifest (DB or manifests dir) with actual files on the FS.
        Returns report dict: {missing: [...], extra: [...], modified: [...]}
        """
        try:
            manifest = None
            if DBManager:
                with DBManager(self.db_path) as db:
                    manifest = db.get_manifest(pkg)
            if manifest is None:
                mf = self.manifests_dir / f"{pkg}-file-manifest.json"
                if mf.exists():
                    with open(mf) as f:
                        manifest = json.load(f).get("manifest")
            if not manifest:
                return {"ok": False, "error": "No manifest found", "missing": [], "extra": []}

            missing = []
            extra = []
            for category, paths in manifest.items():
                for p in paths:
                    if not os.path.exists(p):
                        missing.append(p)

            # Detect extras: search for files recorded in DB? (best-effort: can't know all extras centrally)
            # We'll do a simple heuristic: if a recorded file exists under package root but not in manifest, it's extra.
            # For now return missing list only.
            report = {"ok": len(missing) == 0, "missing": missing, "extra": extra}
            return report
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # -- Safe extraction + optional patch application --
    def _prepare_source(self, pkg: str, pkg_file: Path, dest: Path, hooks: Dict[str, List[str]]):
        # extract
        _safe_extract_file(pkg_file, dest)
        # apply patches defined in package dir if any (we expect recipe to list patches or Patcher usage)
        if Patcher:
            try:
                pd = self.ports_dir / pkg / "patches"
                if pd.exists():
                    Patcher.apply_from_dir(str(pd), str(dest))
            except Exception as e:
                log_event(pkg, "patch", f"Patcher apply dir failed: {e}", "warning")
        # run pre-fetch hooks if any
        self._apply_hooks(pkg, hooks.get("post_fetch", []), os.environ.copy(), "post_fetch")

    # -- Copy files to destination (either real / or use fakeroot) --
    def _copy_to_root(self, pkg: str, src: Path, dest_root: str, dry_run: bool = False) -> bool:
        """
        Copy contents of src (a temporary extracted tree) into dest_root.
        If fakeroot enabled, run under fakeroot to preserve ownership manipulations.
        """
        try:
            copy_cmd = ["cp", "-a", f"{str(src)}/.", dest_root]
            if dry_run:
                log_event(pkg, "install", f"[dry-run] would run: {' '.join(copy_cmd)}")
                return True
            if self.fakeroot:
                # attempt to use fakeroot wrapper
                try:
                    import subprocess
                    subprocess.run(["fakeroot"] + copy_cmd, check=True)
                except Exception:
                    # fallback to non-fakeroot
                    log_event(pkg, "install", "fakeroot failed; falling back to plain copy", "warning")
                    shutil.copytree(src, dest_root, dirs_exist_ok=True)
            else:
                # direct copy
                shutil.copytree(src, dest_root, dirs_exist_ok=True)
            return True
        except Exception as e:
            log_event(pkg, "install", f"Copy failed: {e}", "error")
            return False

    # -- Public install method --
    def install(self, pkg: str, pkg_file: Path,
                hooks: Optional[Dict[str, List[str]]] = None,
                env: Optional[Dict[str, str]] = None,
                dry_run: bool = False,
                parallel: bool = False,
                root: Optional[str] = None) -> bool:
        """
        Install a package from an archive file.
        pkg: package identifier (used for logs and manifest naming). Usually recipe filename base.
        pkg_file: path to archive (.tar.xz, .tar.gz, .zip)
        hooks: dict with keys pre_fetch, post_fetch, pre_install, post_install, pre_remove, post_remove
        env: environment to pass to hooks/run_in_chroot
        dry_run: simulate
        parallel: whether this install is part of a parallel install stage (affects logging only)
        root: installation root (overrides recipe/install.root and CLI --root)
        """
        hooks = hooks or {}
        env = env or os.environ.copy()
        dest_root = root or self.cfg.get("install", {}).get("root") or "/"
        tmpdir = self.state_dir / f"staging-{pkg}"
        chroot_root = None
        if self.cfg.get("build", {}).get("use_chroot") or self.cfg.get("install", {}).get("safe_chroot"):
            chroot_root = dest_root  # we'll use dest_root as chroot mount point (caller must mount necessary FS)

        try:
            log_event(pkg, "install", f"Starting installation of {pkg} into {dest_root}")
            if dry_run:
                log_event(pkg, "install", "[dry-run] SKIPPING extraction/copy", "info")
                return True

            # prepare chroot (if applicable) -- caller should have mounted /dev,/proc,/sys; prepare_chroot must ensure dirs exist
            if chroot_root:
                log_event(pkg, "install", f"Preparing chroot: {chroot_root}")
                prepare_chroot(chroot_root)

            # clean staging
            if tmpdir.exists():
                shutil.rmtree(tmpdir)
            tmpdir.mkdir(parents=True, exist_ok=True)

            # pre-fetch hooks
            self._apply_hooks(pkg, hooks.get("pre_fetch", []), env, "pre_fetch")

            # extract and optional patch
            self._prepare_source(pkg, pkg_file, tmpdir, hooks)

            # pre-install hooks
            self._apply_hooks(pkg, hooks.get("pre_install", []), env, "pre_install", chroot_root)

            # copy into dest_root
            ok = self._copy_to_root(pkg, tmpdir, dest_root, dry_run=dry_run)
            if not ok:
                raise Exception("Copy to root failed")

            # scan for manifest (scan tmpdir which is in DEST tree layout; manifest paths stored as absolute /...)
            manifest = _scan_manifest_paths(tmpdir)
            self._record_manifest(pkg, manifest)

            # post-install hooks
            self._apply_hooks(pkg, hooks.get("post_install", []), env, "post_install", chroot_root)

            # final logging
            self._log_json(pkg, "install", "success", {"root": dest_root, "manifest_summary": {k: len(v) for k,v in manifest.items()}})
            log_event(pkg, "install", "Installed successfully", "info")
            return True

        except Exception as e:
            log_event(pkg, "install", f"Install failed: {e}\n{traceback.format_exc()}", "error")
            # attempt rollback: remove files recorded in DB if any
            try:
                if DBManager:
                    with DBManager(self.db_path) as db:
                        db.remove_package(pkg)
                # best-effort remove files in tmpdir copy (dangerous to auto-remove on system)
                log_event(pkg, "rollback", "DB entry removed (if existed). Manual cleanup may be required.", "warning")
            except Exception as e2:
                log_event(pkg, "rollback", f"Rollback error: {e2}", "error")
            self._log_json(pkg, "install", "failed", {"error": str(e)})
            return False
        finally:
            # cleanup staging
            try:
                if tmpdir.exists():
                    shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
            # cleanup chroot if requested (do not unmount mounts)
            # caller responsible for real unmounts if they mounted; cleanup_chroot should be conservative
            # We won't automatically call cleanup_chroot here to avoid unmounting user mounts.
            pass

    # -- Remove method --
    def remove(self, pkg: str, hooks: Optional[Dict[str, List[str]]] = None, dry_run: bool = False) -> Dict[str, Any]:
        """
        Remove a package based on manifest in DB or manifests dir.
        Returns dict with keys: ok(bool), removed(list), missing(list), errors(list)
        """
        hooks = hooks or {}
        removed = []
        missing = []
        errors = []
        try:
            log_event(pkg, "remove", f"Starting removal of {pkg}")
            if dry_run:
                log_event(pkg, "remove", "[dry-run] would remove files", "info")
                return {"ok": True, "removed": [], "missing": [], "errors": []}

            # reverse dependencies check
            if DependencyResolver:
                try:
                    dr = DependencyResolver(self.cfg)
                    rev = dr.reverse_dependencies(pkg)
                    if rev:
                        log_event(pkg, "remove", f"Reverse dependencies detected: {rev}", "warning")
                        # do not abort; caller may choose to abort
                except Exception as e:
                    log_event(pkg, "remove", f"Reverse dependency check failed: {e}", "warning")

            # pre_remove hooks
            self._apply_hooks(pkg, hooks.get("pre_remove", []), os.environ.copy(), "pre_remove")

            # get manifest
            manifest = None
            if DBManager:
                try:
                    with DBManager(self.db_path) as db:
                        manifest = db.get_manifest(pkg)
                except Exception as e:
                    log_event(pkg, "remove", f"DB manifest fetch failed: {e}", "warning")
            if manifest is None:
                mf = self.manifests_dir / f"{pkg}-file-manifest.json"
                if mf.exists():
                    with open(mf) as f:
                        manifest = json.load(f).get("manifest")
            if not manifest:
                log_event(pkg, "remove", "No manifest found; aborting remove", "error")
                return {"ok": False, "removed": [], "missing": [], "errors": ["no manifest"]}

            # try remove files
            for cat, paths in manifest.items():
                for p in paths:
                    try:
                        if os.path.exists(p):
                            os.remove(p)
                            removed.append(p)
                        else:
                            missing.append(p)
                    except Exception as e:
                        errors.append({"path": p, "error": str(e)})

            # remove DB entry + manifest file
            try:
                if DBManager:
                    with DBManager(self.db_path) as db:
                        db.remove_package(pkg)
                mf = self.manifests_dir / f"{pkg}-file-manifest.json"
                if mf.exists():
                    mf.unlink()
            except Exception as e:
                log_event(pkg, "remove", f"Failed to cleanup DB/manifest: {e}", "warning")

            self._apply_hooks(pkg, hooks.get("post_remove", []), os.environ.copy(), "post_remove")

            ok = len(errors) == 0
            self._log_json(pkg, "remove", "success" if ok else "partial", {"removed": len(removed), "missing": missing, "errors": errors})
            return {"ok": ok, "removed": removed, "missing": missing, "errors": errors}

        except Exception as e:
            log_event(pkg, "remove", f"Removal failed: {e}", "error")
            return {"ok": False, "removed": removed, "missing": missing, "errors": [str(e)]}

    # -- JSON audit log helper --
    def _log_json(self, pkg: str, action: str, status: str, extra: Optional[Dict[str,Any]] = None):
        path = self.log_dir / f"{pkg}-{action}.json"
        data = {
            "package": pkg,
            "action": action,
            "status": status,
            "timestamp": int(time.time()),
        }
        if extra:
            data["extra"] = extra
        try:
            _write_json(path, data)
            log_global(f"JSON log written: {path}")
        except Exception as e:
            log_event(pkg, "log", f"Failed to write json log: {e}", "warning")
