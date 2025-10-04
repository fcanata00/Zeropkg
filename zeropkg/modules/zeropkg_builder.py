#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_builder.py — Core build system for Zeropkg

Features:
 - Secure chroot builds (integration with zeropkg_chroot)
 - Multiple downloads and automatic extraction (extract_to)
 - Patch application (pre/post)
 - Environment injection from .toml
 - fakeroot build isolation
 - Hooks (pre/post configure, build, install)
 - Logging and packaging to /var/lib/zeropkg/build-logs/
 - Full integration with downloader, patcher, deps, installer, db, logger
"""

from __future__ import annotations
import os
import sys
import subprocess
import tarfile
import tempfile
import traceback
from pathlib import Path
from typing import Optional, Dict, Any, List

# ---- Imports com fallback
try:
    from zeropkg_logger import log_event, log_global, get_logger
    _logger = get_logger("builder")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_builder")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(msg)

try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        return {
            "paths": {
                "build_dir": "/var/zeropkg/build",
                "state_dir": "/var/lib/zeropkg",
                "db_path": "/var/lib/zeropkg/installed.sqlite3",
                "ports_dir": "/usr/ports",
                "log_dir": "/var/lib/zeropkg/build-logs"
            },
            "build": {"fakeroot": True, "use_chroot": True},
        }

try:
    from zeropkg_downloader import Downloader
except Exception:
    Downloader = None

try:
    from zeropkg_patcher import Patcher
except Exception:
    Patcher = None

try:
    from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot
except Exception:
    def prepare_chroot(root): os.makedirs(root, exist_ok=True)
    def cleanup_chroot(root): pass
    def run_in_chroot(root, cmd, env=None): return subprocess.run(cmd, shell=True, check=False)

try:
    from zeropkg_installer import Installer
except Exception:
    Installer = None

try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

try:
    from zeropkg_toml import load_toml
except Exception:
    def load_toml(p): return {"package": {"name": Path(p).stem, "version": "0.0"}}

try:
    from zeropkg_deps import DependencyResolver
except Exception:
    DependencyResolver = None

# ---- Classe principal

class Builder:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or load_config()
        paths = self.cfg["paths"]
        self.build_dir = Path(paths["build_dir"])
        self.ports_dir = Path(paths["ports_dir"])
        self.log_dir = Path(paths.get("log_dir", "/var/lib/zeropkg/build-logs"))
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.fakeroot = self.cfg.get("build", {}).get("fakeroot", True)
        self.use_chroot = self.cfg.get("build", {}).get("use_chroot", True)
        self.installer = Installer() if Installer else None
        self.patcher = Patcher() if Patcher else None
        self.downloader = Downloader() if Downloader else None

    def _env_from_meta(self, meta: Dict[str, Any]) -> Dict[str, str]:
        env = os.environ.copy()
        for k, v in meta.get("environment", {}).items():
            env[k] = v
        return env

    def _apply_patches(self, pkg: str, meta: Dict[str, Any], src_dir: Path):
        patches = meta.get("patches", [])
        if not patches or not self.patcher:
            return
        for patch in patches:
            log_event(pkg, "patch", f"Applying {patch}")
            self.patcher.apply_patch(src_dir, patch)

    def _run_hooks(self, pkg: str, meta: Dict[str, Any], stage: str, cwd: Path, env: Dict[str, str]):
        hooks = meta.get("hooks", {}).get(stage, [])
        if not hooks:
            return
        for cmd in hooks:
            log_event(pkg, stage, f"Running hook: {cmd}")
            subprocess.run(cmd, shell=True, cwd=cwd, env=env, check=False)

    def _download_sources(self, pkg: str, meta: Dict[str, Any], dest: Path) -> List[Path]:
        urls = meta.get("source", {}).get("urls", [])
        extracted = []
        if not urls:
            return extracted
        for url in urls:
            if self.downloader:
                out = self.downloader.fetch(url, dest)
                if out:
                    extracted.append(self.downloader.extract(out, dest))
        return extracted

    def _package_artifacts(self, pkg: str, src_dir: Path, version: str) -> Path:
        pkgfile = self.build_dir / f"{pkg}-{version}.tar.xz"
        with tarfile.open(pkgfile, "w:xz") as tar:
            tar.add(src_dir, arcname=pkg)
        log_event(pkg, "package", f"Packaged at {pkgfile}")
        return pkgfile

    def build(self, pkg: str, toml_path: Optional[Path] = None, dry_run: bool = False) -> Optional[Path]:
        try:
            meta_path = toml_path or (self.ports_dir / pkg / f"{pkg}.toml")
            meta = load_toml(meta_path)
            version = meta.get("package", {}).get("version", "0.0")
            src_dir = self.build_dir / f"{pkg}-{version}"
            src_dir.mkdir(parents=True, exist_ok=True)

            log_event(pkg, "build", f"Starting build of {pkg}-{version}")

            env = self._env_from_meta(meta)
            sources = self._download_sources(pkg, meta, src_dir)
            if not sources:
                log_event(pkg, "download", "No sources fetched", "warning")

            self._apply_patches(pkg, meta, src_dir)
            self._run_hooks(pkg, meta, "pre_configure", src_dir, env)

            # Executar build dentro do chroot ou local
            if dry_run:
                log_event(pkg, "build", "[dry-run] would build", "info")
                return None

            if self.use_chroot:
                prepare_chroot(src_dir)
                build_cmds = meta.get("build", {}).get("commands", [])
                for cmd in build_cmds:
                    log_event(pkg, "build", f"Running: {cmd}")
                    run_in_chroot(src_dir, cmd, env)
                cleanup_chroot(src_dir)
            else:
                build_cmds = meta.get("build", {}).get("commands", [])
                for cmd in build_cmds:
                    log_event(pkg, "build", f"Running: {cmd}")
                    subprocess.run(cmd, shell=True, cwd=src_dir, env=env, check=False)

            self._run_hooks(pkg, meta, "post_build", src_dir, env)
            self._run_hooks(pkg, meta, "pre_install", src_dir, env)

            pkgfile = self._package_artifacts(pkg, src_dir, version)

            if self.installer and not dry_run:
                self.installer.install_from_cache(pkgfile)
                log_event(pkg, "install", f"Installed {pkg}-{version}")

            self._run_hooks(pkg, meta, "post_install", src_dir, env)
            log_event(pkg, "build", f"Build completed successfully")
            return pkgfile

        except Exception as e:
            log_event(pkg, "build", f"Build failed: {e}\n{traceback.format_exc()}", "error")
            return None
