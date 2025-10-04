#!/usr/bin/env python3
# zeropkg_builder.py — Builder completo e integrado do Zeropkg
# Suporta: deps resolve, downloader (extract_to), patcher, chroot, fakeroot,
# caching de binários, registro DB, hooks e dry-run.
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import sys
import tarfile
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

# tolerant imports of project modules
try:
    from zeropkg_config import load_config
except Exception:
    # minimal fallback: read simple toml-like dict or environment defaults
    def load_config(path="/etc/zeropkg/config.toml"):
        return {
            "paths": {
                "db_path": "/var/lib/zeropkg/installed.sqlite3",
                "ports_dir": "/usr/ports",
                "build_root": "/var/zeropkg/build",
                "cache_dir": "/usr/ports/distfiles",
                "packages_dir": "/var/zeropkg/packages",
            },
            "options": {"jobs": 4, "fakeroot": True, "chroot_enabled": True},
        }

try:
    from zeropkg_toml import load_toml
except Exception:
    def load_toml(path):
        raise RuntimeError("zeropkg_toml.load_toml not available")

try:
    from zeropkg_logger import log_event, get_logger
except Exception:
    def log_event(pkg, stage, msg, level="info"): print(f"[{level}] {pkg}:{stage} {msg}")
    def get_logger(stage="builder"):
        import logging
        l = logging.getLogger(stage)
        if not l.handlers:
            h = logging.StreamHandler(sys.stdout)
            l.addHandler(h)
        return l

logger = get_logger("builder")

# downloader (expects Downloader.fetch_sources or download_package)
Downloader = None
try:
    from zeropkg_downloader import Downloader as _Downloader
    def _downloader_fetch(pkgname, sources, build_root):
        dl = _Downloader(dist_dir=str(CONFIG["paths"]["cache_dir"]))
        return dl.fetch_sources(pkgname, sources, build_root=str(build_root))
    Downloader = _Downloader
except Exception:
    try:
        from zeropkg_downloader import fetch_sources as _fetch_sources
        def _downloader_fetch(pkgname, sources, build_root):
            return _fetch_sources(pkgname, sources, build_root)
        Downloader = None
    except Exception:
        _downloader_fetch = None

# patcher
try:
    from zeropkg_patcher import Patcher
except Exception:
    Patcher = None

# installer
try:
    from zeropkg_installer import Installer
except Exception:
    Installer = None

# deps resolver (expected api: resolve_and_install(resolver, pkgname, BuilderClass, InstallerClass, args))
DependencyResolver = None
resolve_and_install = None
try:
    from zeropkg_deps import DependencyResolver, resolve_and_install as _resolve_and_install
    DependencyResolver = DependencyResolver
    resolve_and_install = _resolve_and_install
except Exception:
    # fallback: maybe resolver exposes simple functions — we will check at runtime
    pass

# db manager (expects methods: has_package(name, version), add_package(...), record_build_start/finish)
DB = None
try:
    from zeropkg_db import DBManager as DBManagerClass
    DB = DBManagerClass
except Exception:
    DB = None

# chroot helpers
prepare_chroot = None
cleanup_chroot = None
run_in_chroot = None
try:
    from zeropkg_chroot import prepare_chroot as _pc, cleanup_chroot as _cc, run_in_chroot as _ric
    prepare_chroot = _pc
    cleanup_chroot = _cc
    run_in_chroot = _ric
except Exception:
    prepare_chroot = None

# load config early
CONFIG = load_config() if callable(load_config) else load_config

class BuildError(Exception):
    pass

class Builder:
    def __init__(self, config_path: str = "/etc/zeropkg/config.toml"):
        # reload config
        try:
            self.config = load_config(config_path)
        except Exception:
            self.config = CONFIG
        paths = self.config.get("paths", {})
        opts = self.config.get("options", {})
        self.db_path = paths.get("db_path", "/var/lib/zeropkg/installed.sqlite3")
        self.ports_dir = Path(paths.get("ports_dir", "/usr/ports"))
        self.build_root = Path(paths.get("build_root", "/var/zeropkg/build"))
        self.cache_dir = Path(paths.get("cache_dir", "/usr/ports/distfiles"))
        self.packages_dir = Path(paths.get("packages_dir", "/var/zeropkg/packages"))
        self.jobs = int(opts.get("jobs", 4))
        self.default_fakeroot = bool(opts.get("fakeroot", True))
        self.chroot_enabled = bool(opts.get("chroot_enabled", True))
        # ensure dirs
        for d in (self.build_root, self.cache_dir, self.packages_dir):
            d.mkdir(parents=True, exist_ok=True)
        # db
        self.db = DB(self.db_path) if DB else None
        # helper instances
        self.downloader = Downloader(dist_dir=str(self.cache_dir)) if Downloader else None
        self.patcher = Patcher() if Patcher else None
        self.installer = Installer(self.db_path) if Installer else None

    # ---------------------------
    # utilities
    # ---------------------------
    def _log(self, pkg, stage, msg, level="info"):
        try:
            log_event(pkg, stage, msg, level=level)
        except Exception:
            getattr(logger, level, logger.info)(f"{pkg}:{stage} {msg}")

    def _run_shell(self, cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None, chroot_root: Optional[str] = None, dry_run: bool = False):
        self._log("builder", "cmd", f"{cmd} (cwd={cwd} chroot={chroot_root})")
        if dry_run:
            return 0
        envp = os.environ.copy()
        if env:
            envp.update(env)
        if chroot_root and run_in_chroot:
            rc = run_in_chroot(chroot_root, cmd, env=envp, cwd=cwd)
            if rc != 0:
                raise BuildError(f"Command failed in chroot ({rc}): {cmd}")
            return rc
        proc = subprocess.run(cmd, shell=True, cwd=cwd, env=envp)
        if proc.returncode != 0:
            raise BuildError(f"Command failed ({proc.returncode}): {cmd}")
        return proc.returncode

    def _package_from_staging(self, staging_dir: Path, pkg_fullname: str) -> Path:
        out = self.packages_dir / f"{pkg_fullname}.tar.xz"
        # create package
        with tarfile.open(str(out), "w:xz") as tf:
            for f in staging_dir.rglob("*"):
                arcname = "/" + str(f.relative_to(staging_dir)).lstrip("/")
                tf.add(str(f), arcname=arcname)
        return out

    def _installed_check(self, pkgname: str, pkgver: str) -> bool:
        if not self.db:
            return False
        try:
            if hasattr(self.db, "has_package"):
                return self.db.has_package(pkgname, pkgver)
            # other API: query by name returns rows?
            if hasattr(self.db, "get_package"):
                rec = self.db.get_package(pkgname)
                return bool(rec and rec.get("version") == pkgver)
        except Exception:
            return False
        return False

    # ---------------------------
    # main: build
    # ---------------------------
    def build(self, target: str, args: Optional[Any] = None, dry_run: bool = False, rebuild: bool = False) -> bool:
        """
        target: path to metafile (toml) or package name (will search ports dir)
        args: CLI-like object (may contain root, fakeroot, dir_install, etc.)
        dry_run: if True do not execute shell actions
        rebuild: force rebuild even if already installed
        """
        # load metafile
        meta_path = None
        try:
            if os.path.isfile(target):
                meta_path = target
                meta = load_toml(meta_path)
            else:
                # search ports dir for pattern name-version.toml or name-*.toml
                candidates = list(self.ports_dir.rglob(f"{target}-*.toml"))
                if not candidates:
                    # try folder /usr/ports/<pkg>/pkgfile.toml
                    fallback = list(self.ports_dir.rglob(f"{target}.toml"))
                    if fallback:
                        meta_path = str(fallback[-1])
                        meta = load_toml(meta_path)
                    else:
                        raise FileNotFoundError(f"Metafile for {target} not found in {self.ports_dir}")
                else:
                    meta_path = str(sorted(candidates)[-1])
                    meta = load_toml(meta_path)
        except Exception as e:
            raise BuildError(f"Failed to load recipe for {target}: {e}")

        # normalize meta
        pkg = meta.get("package", {})
        pkgname = pkg.get("name") or target
        pkgver = pkg.get("version") or "0"
        pkgfull = f"{pkgname}-{pkgver}"

        # quick installed check
        if not rebuild and self._installed_check(pkgname, pkgver):
            self._log(pkgname, "skip", f"{pkgfull} already installed, skipping build")
            return True

        # resolve dependencies (build/runtime) using DependencyResolver if available
        try:
            if resolve_and_install and DependencyResolver:
                self._log(pkgname, "deps", "Resolving dependencies via resolver")
                resolver = DependencyResolver(self.db_path, str(self.ports_dir)) if DependencyResolver else None
                # call generic resolve_and_install(resolver, pkgname, BuilderClass, InstallerClass, args)
                resolve_and_install(resolver, pkgname, Builder, Installer, args)
                self._log(pkgname, "deps", "Dependencies resolved")
            else:
                self._log(pkgname, "deps", "No dependency resolver available; skipping automatic resolves", level="warning")
        except Exception as e:
            raise BuildError(f"Dependency resolution failed: {e}")

        # prepare dirs
        workdir = self.build_root / pkgfull
        src_root = workdir / "src"
        staging = workdir / "staging"
        os.makedirs(src_root, exist_ok=True)
        os.makedirs(staging, exist_ok=True)

        # prepare env
        env = os.environ.copy()
        build_env = {}
        build_env.update(meta.get("environment", {}) or {})
        if isinstance(build_env, dict):
            env.update({str(k): str(v) for k,v in build_env.items()})
        env["MAKEFLAGS"] = env.get("MAKEFLAGS", f"-j{self.jobs}")

        # decide chroot/fakeroot
        build_opts = meta.get("build", {}) or {}
        use_chroot = bool(build_opts.get("chroot", False)) and self.chroot_enabled
        use_fakeroot = bool(build_opts.get("fakeroot", self.default_fakeroot))
        chroot_root = args.root if (args and getattr(args, "root", None)) else env.get("LFS", None)

        # 1) download sources (downloader must support extract_to)
        sources = meta.get("sources") or meta.get("source") or []
        downloaded = []
        try:
            if _downloader_fetch:
                self._log(pkgname, "download", f"Starting download of {len(sources)} sources")
                # expecting sources as list of dicts; adapt if toml parser returns nested structure
                normalized = []
                # some toml->dict returns {"entries":[...]} — handle that
                if isinstance(sources, dict) and "entries" in sources:
                    normalized = sources["entries"]
                elif isinstance(sources, list):
                    normalized = sources
                else:
                    normalized = [sources]
                downloaded = _downloader_fetch(pkgname, normalized, build_root=src_root)
                self._log(pkgname, "download", f"Downloaded/extracted: {downloaded}")
            else:
                self._log(pkgname, "download", "No downloader available", level="warning")
        except Exception as e:
            raise BuildError(f"Download failed: {e}")

        # 2) apply patches via patcher
        try:
            patches = meta.get("patches", {}).get("files") if isinstance(meta.get("patches", {}), dict) else (meta.get("patches") or [])
            if self.patcher and patches:
                self._log(pkgname, "patch", f"Applying {len(patches)} patches")
                # Patcher may expect workdir path and list of patch files
                try:
                    self.patcher.apply_patches(patches, str(src_root))
                except Exception:
                    # fallback to stage-based API
                    self.patcher.apply_stage("pre_configure", patches=patches, hooks=meta.get("hooks", {}), workdir=str(src_root))
            else:
                if patches:
                    self._log(pkgname, "patch", "Patcher module not available; skipping patches", level="warning")
        except Exception as e:
            raise BuildError(f"Patching failed: {e}")

        # 3) prepare source root: if downloader already extracted into subdirs, find the real source root
        # heuristic: if src_root has single child dir, use it
        actual_src = src_root
        try:
            children = [p for p in src_root.iterdir() if p.is_dir() and not p.name.startswith(".")]
            if len(children) == 1:
                actual_src = children[0]
        except Exception:
            pass
        self._log(pkgname, "prepare", f"Using source root: {actual_src}")

        # 4) pre_configure hook
        hooks = meta.get("hooks", {}) or {}
        pre_cfg = hooks.get("pre_configure")
        if pre_cfg:
            self._run_shell(pre_cfg, cwd=str(actual_src), env=env, chroot_root=(chroot_root if use_chroot else None), dry_run=dry_run)

        # 5) configure/build sequence
        build_seq = build_opts.get("commands") or []
        if not build_seq:
            # try standard configure/make/install fields
            cfg_cmd = build_opts.get("configure")
            make_cmd = build_opts.get("make") or f"make -j{self.jobs}"
            inst_cmd = build_opts.get("install") or f"make DESTDIR={staging} install"
            if cfg_cmd:
                build_seq.append(cfg_cmd)
            build_seq.append(make_cmd)
            build_seq.append(inst_cmd)

        # support staged build: run each command in cwd=actual_src
        try:
            for cmd in build_seq:
                # replace tokens commonly used
                cmd = cmd.replace("@DESTDIR@", str(staging)).replace("${PKGDEST}", str(staging))
                if use_fakeroot:
                    # run under fakeroot if requested
                    wrapped = f"fakeroot bash -c \"{cmd}\""
                    self._run_shell(wrapped, cwd=str(actual_src), env=env, chroot_root=(chroot_root if use_chroot else None), dry_run=dry_run)
                else:
                    self._run_shell(cmd, cwd=str(actual_src), env=env, chroot_root=(chroot_root if use_chroot else None), dry_run=dry_run)
            self._log(pkgname, "build", "Build commands completed")
        except Exception as e:
            raise BuildError(f"Build failed: {e}")

        # 6) post_build hook
        post_build = hooks.get("post_build")
        if post_build:
            self._run_shell(post_build, cwd=str(actual_src), env=env, chroot_root=(chroot_root if use_chroot else None), dry_run=dry_run)

        # 7) package creation (from staging)
        pkg_file = None
        try:
            # If staging is empty, user may have used DESTDIR in install command; still package whatever is in staging
            if any(staging.rglob("*")):
                pkg_file = self._package_from_staging(staging, pkgfull)
                self._log(pkgname, "package", f"Created package: {pkg_file}")
            else:
                self._log(pkgname, "package", "Staging is empty — no package created", level="warning")
        except Exception as e:
            raise BuildError(f"Packaging failed: {e}")

        # 8) installer: install package into target root (args.root or /)
        try:
            if self.installer:
                install_root = getattr(args, "root", "/") if args else "/"
                # installer.install(pkgname, args, pkg_file=pkg_file, meta=meta, dir_install=None)
                # attempt many common installer signatures
                try:
                    self.installer.install(pkgname, {"pkg_file": str(pkg_file) if pkg_file else None, "root": install_root}, meta)
                except TypeError:
                    # older signature: install(pkgname, version, staging_dir,...)
                    try:
                        self.installer.install(pkgname, pkgver, str(staging), env=env, dry_run=dry_run)
                    except Exception:
                        # try final fallback: installer.install_package_file
                        if hasattr(self.installer, "install_package_file") and pkg_file:
                            self.installer.install_package_file(str(pkg_file), root=install_root, dry_run=dry_run)
                self._log(pkgname, "installer", f"Installer invoked for {pkgfull}")
            else:
                self._log(pkgname, "installer", "Installer not available; skipping install", level="warning")
        except Exception as e:
            raise BuildError(f"Installer failed: {e}")

        # 9) post_install hook
        post_inst = hooks.get("post_install")
        if post_inst:
            self._run_shell(post_inst, cwd=str(actual_src), env=env, chroot_root=(chroot_root if use_chroot else None), dry_run=dry_run)

        # 10) register in DB
        try:
            files_list = []
            for f in staging.rglob("*"):
                if f.is_file():
                    files_list.append(str(Path("/") / f.relative_to(staging)))
            deps_list = meta.get("dependencies") or {}
            if self.db and hasattr(self.db, "add_package"):
                try:
                    self.db.add_package(pkgname, pkgver, files_list, deps_list, build_meta=meta.get("build", {}))
                    self._log(pkgname, "db", "Package recorded in DB")
                except Exception as e:
                    self._log(pkgname, "db", f"DB registration failed: {e}", level="warning")
        except Exception as e:
            self._log(pkgname, "db", f"DB registration warning: {e}", level="warning")

        # 11) cleanup builddir if requested
        try:
            if build_opts.get("cleanup_sources", True):
                shutil.rmtree(workdir, ignore_errors=True)
            else:
                self._log(pkgname, "cleanup", f"Preserving workdir {workdir}")
        except Exception:
            pass

        self._log(pkgname, "build.finish", f"Build finished: {pkgfull}")
        return True

# If run directly, basic smoke test (dry-run)
if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser(prog="zeropkg-builder")
    parser.add_argument("target", help="package name or metafile")
    parser.add_argument("--config", default="/etc/zeropkg/config.toml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()
    b = Builder(config_path=args.config)
    try:
        ok = b.build(args.target, args=args, dry_run=args.dry_run, rebuild=args.rebuild)
        print("Build result:", ok)
    except Exception as e:
        print("Build failed:", e)
        sys.exit(2)
