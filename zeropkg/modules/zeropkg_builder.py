#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# zeropkg_builder.py — Builder com integração completa de chroot (prepare/cleanup)
# -----------------------------------------------------------------------------
# Versão entregue: integração explícita com zeropkg_chroot.prepare_chroot /
# zeropkg_chroot.cleanup_chroot / zeropkg_chroot.run_in_chroot (enter_chroot).
#
# Requisitos:
#  - zeropkg_config.load_config
#  - zeropkg_toml.load_toml
#  - zeropkg_downloader.Downloader (ou função compatível)
#  - zeropkg_patcher.Patcher
#  - zeropkg_installer.Installer
#  - zeropkg_db.DBManager (ou similar)
#  - zeropkg_chroot.prepare_chroot, cleanup_chroot, run_in_chroot (preferível)
#  - zeropkg_logger.log_event / get_logger (opcional)
#
# Observações:
#  - Este módulo é defensivo quanto a variações de API dos outros módulos.
#  - Recomendo testar inicialmente com --dry-run em pacotes pequenos.
# -----------------------------------------------------------------------------

from __future__ import annotations
import os
import sys
import tarfile
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

# tolerant imports (adaptação caso os módulos tenham assinaturas distintas)
try:
    from zeropkg_config import load_config
except Exception:
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

# logger
try:
    from zeropkg_logger import log_event, get_logger
    logger = get_logger("builder")
except Exception:
    def log_event(pkg, stage, msg, level="info"):
        print(f"[{level.upper()}] {pkg}:{stage} {msg}")
    import logging
    logger = logging.getLogger("zeropkg_builder")
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.stdout))

# downloader
Downloader = None
_downloader_instance = None
try:
    from zeropkg_downloader import Downloader as _DL
    Downloader = _DL
except Exception:
    Downloader = None

# patcher
Patcher = None
try:
    from zeropkg_patcher import Patcher as _Patcher
    Patcher = _Patcher
except Exception:
    Patcher = None

# installer
Installer = None
try:
    from zeropkg_installer import Installer as _Installer
    Installer = _Installer
except Exception:
    Installer = None

# deps resolver
resolve_and_install = None
DependencyResolver = None
try:
    from zeropkg_deps import resolve_and_install as _rai, DependencyResolver as _DR
    resolve_and_install = _rai
    DependencyResolver = _DR
except Exception:
    resolve_and_install = None

# db manager
DBManager = None
try:
    from zeropkg_db import DBManager as _DBManager
    DBManager = _DBManager
except Exception:
    DBManager = None

# chroot helpers (this is the important part)
prepare_chroot = None
cleanup_chroot = None
run_in_chroot = None
try:
    from zeropkg_chroot import prepare_chroot as _pc, cleanup_chroot as _cc, run_in_chroot as _ric
    prepare_chroot = _pc
    cleanup_chroot = _cc
    run_in_chroot = _ric
except Exception:
    # try alternative names
    try:
        from zeropkg_chroot import prepare as _pc2, cleanup as _cc2, enter_chroot as _ec
        prepare_chroot = _pc2
        cleanup_chroot = _cc2
        run_in_chroot = _ec
    except Exception:
        prepare_chroot = None
        cleanup_chroot = None
        run_in_chroot = None

# Load default config
_default_config = load_config()

class BuildError(Exception):
    pass

class Builder:
    def __init__(self, config_path: str = "/etc/zeropkg/config.toml"):
        try:
            self.config = load_config(config_path)
        except Exception:
            self.config = _default_config

        paths = self.config.get("paths", {})
        opts = self.config.get("options", {})

        self.ports_dir = Path(paths.get("ports_dir", "/usr/ports"))
        self.build_root = Path(paths.get("build_root", "/var/zeropkg/build"))
        self.cache_dir = Path(paths.get("cache_dir", "/usr/ports/distfiles"))
        self.packages_dir = Path(paths.get("packages_dir", "/var/zeropkg/packages"))
        self.db_path = paths.get("db_path", "/var/lib/zeropkg/installed.sqlite3")

        self.jobs = int(opts.get("jobs", 4))
        self.default_fakeroot = bool(opts.get("fakeroot", True))
        self.chroot_enabled = bool(opts.get("chroot_enabled", True))

        # ensure directories
        for d in (self.build_root, self.cache_dir, self.packages_dir):
            d.mkdir(parents=True, exist_ok=True)

        # instantiate helpers if modules available
        self.downloader = Downloader(dist_dir=str(self.cache_dir)) if Downloader else None
        self.patcher = Patcher() if Patcher else None
        self.installer = Installer(config_path) if Installer else None
        self.db = DBManager(self.db_path) if DBManager else None

    def _log(self, pkg: str, stage: str, msg: str, level: str = "info"):
        try:
            log_event(pkg, stage, msg, level=level)
        except Exception:
            getattr(logger, level, logger.info)(f"{pkg}:{stage} {msg}")

    def _run_shell(self, cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None, dry_run: bool = False, chroot_root: Optional[str] = None):
        self._log("builder", "cmd", f"{cmd} (cwd={cwd} chroot={chroot_root})")
        if dry_run:
            return 0
        envp = os.environ.copy()
        if env:
            envp.update(env)
        if chroot_root and run_in_chroot:
            # run inside chroot helper
            rc = run_in_chroot(chroot_root, cmd, env=envp, cwd=cwd, use_shell=True, dry_run=False)
            if rc != 0:
                raise BuildError(f"Command failed in chroot ({rc}): {cmd}")
            return rc
        proc = subprocess.run(cmd, shell=True, cwd=cwd, env=envp)
        if proc.returncode != 0:
            raise BuildError(f"Command failed ({proc.returncode}): {cmd}")
        return proc.returncode

    def _extract_archive(self, archive_path: str, dest_dir: str):
        self._log("builder", "extract", f"Extracting {archive_path} -> {dest_dir}")
        os.makedirs(dest_dir, exist_ok=True)
        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(dest_dir)
        else:
            self._log("builder", "extract", f"Unknown archive format: {archive_path}", level="warning")

    def _package_from_staging(self, staging_dir: Path, pkg_fullname: str) -> Optional[Path]:
        if not staging_dir.exists() or not any(staging_dir.rglob("*")):
            return None
        out = self.packages_dir / f"{pkg_fullname}.tar.xz"
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
            if hasattr(self.db, "get_package"):
                rec = self.db.get_package(pkgname)
                return bool(rec and rec.get("version") == pkgver)
        except Exception:
            return False
        return False

    def build(self, target: str, args: Optional[Any] = None, dry_run: bool = False, rebuild: bool = False) -> bool:
        """
        target: path to recipe (toml) or package-name (search in ports)
        args: optional CLI args object (may contain root, dir_install, etc.)
        """
        # load recipe
        try:
            if os.path.isfile(target):
                recipe = load_toml(target)
                recipe_path = target
            else:
                candidates = list(self.ports_dir.rglob(f"{target}-*.toml"))
                if not candidates:
                    raise BuildError(f"Recipe for {target} not found in {self.ports_dir}")
                recipe_path = str(sorted(candidates)[-1])
                recipe = load_toml(recipe_path)
        except Exception as e:
            raise BuildError(f"Failed to load recipe: {e}")

        pkg = recipe.get("package", {})
        pkgname = pkg.get("name", target)
        pkgver = pkg.get("version", "0")
        pkgfull = f"{pkgname}-{pkgver}"

        # skip if already installed
        if not rebuild and self._installed_check(pkgname, pkgver):
            self._log(pkgname, "skip", f"{pkgfull} already installed")
            return True

        # resolve dependencies if resolver available
        try:
            if resolve_and_install and DependencyResolver:
                self._log(pkgname, "deps", "Resolving dependencies")
                resolver = DependencyResolver(self.db_path, str(self.ports_dir)) if DependencyResolver else None
                # generic call; modules may vary in signature
                try:
                    resolve_and_install(resolver, pkgname, self, self.installer, args)
                except TypeError:
                    # try with simpler signature
                    resolve_and_install(pkgname, resolver)
                self._log(pkgname, "deps", "Dependencies resolved")
            else:
                self._log(pkgname, "deps", "No dependency resolver present; skipping", level="warning")
        except Exception as e:
            raise BuildError(f"Dependency resolution failed: {e}")

        workdir = self.build_root / pkgfull
        src_root = workdir / "src"
        staging = workdir / "staging"
        os.makedirs(src_root, exist_ok=True)
        os.makedirs(staging, exist_ok=True)

        env = os.environ.copy()
        build_env = {}
        build_env.update(recipe.get("environment", {}) or {})
        env.update({str(k): str(v) for k, v in (build_env.items() if isinstance(build_env, dict) else [])})
        env["MAKEFLAGS"] = env.get("MAKEFLAGS", f"-j{self.jobs}")

        build_cfg = recipe.get("build", {}) or {}
        use_chroot = bool(build_cfg.get("chroot", False)) and self.chroot_enabled
        use_fakeroot = bool(build_cfg.get("fakeroot", self.default_fakeroot))
        commands = build_cfg.get("commands") or []
        hooks = recipe.get("hooks", {}) or {}
        patches = recipe.get("patches", {}).get("files") if isinstance(recipe.get("patches", {}), dict) else (recipe.get("patches") or [])
        sources = recipe.get("sources") or recipe.get("source") or []

        # 1) Download & extract (downloader may extract using extract_to)
        downloaded = []
        try:
            if self.downloader and sources:
                # normalize sources structure
                normalized = []
                if isinstance(sources, dict) and "entries" in sources:
                    normalized = sources["entries"]
                elif isinstance(sources, list):
                    normalized = sources
                else:
                    normalized = [sources]
                self._log(pkgname, "download", f"Fetching {len(normalized)} sources")
                downloaded = self.downloader.fetch_sources(pkgname, normalized, build_root=str(src_root))
                self._log(pkgname, "download", f"Downloaded/extracted: {downloaded}")
            else:
                self._log(pkgname, "download", "No downloader or no sources; skipping download", level="warning")
        except Exception as e:
            raise BuildError(f"Download failed: {e}")

        # 2) Apply patches
        if patches:
            if self.patcher:
                try:
                    self._log(pkgname, "patch", f"Applying {len(patches)} patches")
                    self.patcher.apply_patches(patches, str(src_root))
                except Exception:
                    # try alternative API
                    try:
                        self.patcher.apply(patches, str(src_root))
                    except Exception as e:
                        raise BuildError(f"Patching failed: {e}")
            else:
                self._log(pkgname, "patch", "Patcher not available; skipping patches", level="warning")

        # 3) Prepare sources directory: if downloader already extracted into subdirs, pick that
        actual_src = src_root
        try:
            children = [p for p in src_root.iterdir() if p.is_dir() and not p.name.startswith(".")]
            if len(children) == 1:
                actual_src = children[0]
        except Exception:
            pass
        self._log(pkgname, "prepare", f"Using source root: {actual_src}")

        # 4) pre_configure hook
        if hooks.get("pre_configure"):
            self._run_shell(hooks["pre_configure"], cwd=str(actual_src), env=env, dry_run=dry_run, chroot_root=(args.root if args and getattr(args, "root", None) else None if use_chroot else None))

        # 5) Build step with optional chroot prepare/cleanup
        chroot_root = None
        if use_chroot:
            # determine chroot target: prefer args.root, then ENV LFS, fallback to /mnt/lfs
            if args and getattr(args, "root", None):
                chroot_root = args.root
            else:
                chroot_root = env.get("LFS", "/mnt/lfs")
        # ensure chroot callbacks exist if required
        if use_chroot and (not prepare_chroot or not cleanup_chroot or not run_in_chroot):
            raise BuildError("Chroot requested but zeropkg_chroot helpers not available")

        # Prepare chroot if needed
        chroot_meta = None
        if use_chroot:
            try:
                self._log(pkgname, "chroot", f"Preparing chroot at {chroot_root}")
                # allow passing some overlay options via recipe build config
                overlay = build_cfg.get("overlay", False)
                overlay_dir = build_cfg.get("overlay_dir", None)
                prepare_chroot(chroot_root, copy_resolv=True, use_overlay=overlay, overlay_dir=overlay_dir, dry_run=dry_run)
                chroot_meta = {"root": chroot_root}
            except Exception as e:
                raise BuildError(f"Failed to prepare chroot {chroot_root}: {e}")

        # Now execute build commands
        try:
            if not commands:
                # fallback: try configure/make/install fields
                cfg = build_cfg.get("configure")
                mk = build_cfg.get("make") or f"make -j{self.jobs}"
                inst = build_cfg.get("install") or f"make DESTDIR={staging} install"
                if cfg:
                    commands = [cfg, mk, inst]
                else:
                    # no commands provided - nothing to do
                    commands = []

            for cmd in commands:
                # token replace
                cmd = cmd.replace("@DESTDIR@", str(staging)).replace("${DESTDIR}", str(staging))
                if use_fakeroot and not use_chroot:
                    wrapped = f"fakeroot bash -c \"{cmd}\""
                    self._run_shell(wrapped, cwd=str(actual_src), env=env, dry_run=dry_run)
                elif use_chroot:
                    # run commands inside chroot
                    # if the command assumes being run from the source dir, we must replicate that path inside chroot
                    # prefer to copy sources into chroot's build root, but we assume recipe uses DESTDIR for install
                    # strategy: run cmd with cwd pointing to the source path inside chroot (attempt mapping)
                    # If actual_src is under our build_root, create a bind into chroot's /build/<pkgfull>
                    target_in_chroot = os.path.join(chroot_root, "build", pkgfull)
                    if not dry_run:
                        # ensure host path exists and copy sources into chroot build area (safe)
                        if os.path.exists(target_in_chroot):
                            shutil.rmtree(target_in_chroot, ignore_errors=True)
                        shutil.copytree(str(actual_src), target_in_chroot, symlinks=True)
                    internal_cwd = f"/build/{pkgfull}"
                    # run inside chroot
                    if use_fakeroot:
                        # still wrap with fakeroot inside chroot if requested
                        chcmd = f"fakeroot bash -c \"cd {internal_cwd} && {cmd}\""
                        self._run_shell(chcmd, cwd=None, env=env, dry_run=dry_run, chroot_root=chroot_root)
                    else:
                        chcmd = f"cd {internal_cwd} && {cmd}"
                        self._run_shell(chcmd, cwd=None, env=env, dry_run=dry_run, chroot_root=chroot_root)
                else:
                    # normal host build
                    self._run_shell(cmd, cwd=str(actual_src), env=env, dry_run=dry_run)
        except Exception as e:
            # if build failed, ensure we cleanup chroot and propagate error
            raise BuildError(f"Build commands failed: {e}")
        finally:
            # always cleanup chroot if we prepared it
            if use_chroot:
                try:
                    self._log(pkgname, "chroot", f"Cleaning up chroot at {chroot_root}")
                    cleanup_chroot(chroot_root, force_lazy=True, dry_run=dry_run)
                except Exception as e:
                    # log and continue
                    self._log(pkgname, "chroot", f"Chroot cleanup warning: {e}", level="warning")

        # 6) post_build hook
        if hooks.get("post_build"):
            self._run_shell(hooks["post_build"], cwd=str(actual_src), env=env, dry_run=dry_run)

        # 7) create package from staging (if any)
        pkg_file = None
        try:
            pkg_file = self._package_from_staging(staging, pkgfull)
            if pkg_file:
                self._log(pkgname, "package", f"Created package {pkg_file}")
            else:
                self._log(pkgname, "package", "No package created (staging empty)", level="warning")
        except Exception as e:
            raise BuildError(f"Packaging failed: {e}")

        # 8) installer: try multiple signatures
        try:
            if self.installer:
                install_root = getattr(args, "root", "/") if args else "/"
                try:
                    self.installer.install(pkgname, {"pkg_file": str(pkg_file) if pkg_file else None, "root": install_root, "dry_run": dry_run}, recipe)
                except TypeError:
                    # fallback signatures
                    try:
                        self.installer.install(pkgname, pkgver, str(staging), env=env, dry_run=dry_run)
                    except Exception as e:
                        if hasattr(self.installer, "install_package_file") and pkg_file:
                            self.installer.install_package_file(str(pkg_file), root=install_root, dry_run=dry_run)
                        else:
                            raise
                self._log(pkgname, "install", "Installer invoked")
            else:
                self._log(pkgname, "install", "Installer not available; skipping install", level="warning")
        except Exception as e:
            raise BuildError(f"Installer failed: {e}")

        # 9) post_install hook
        if hooks.get("post_install"):
            self._run_shell(hooks["post_install"], cwd=str(actual_src), env=env, dry_run=dry_run)

        # 10) register in DB
        try:
            if self.db:
                files_list = []
                for f in staging.rglob("*"):
                    if f.is_file():
                        files_list.append(str(Path("/") / f.relative_to(staging)))
                deps_list = recipe.get("dependencies") or {}
                try:
                    if hasattr(self.db, "add_package"):
                        self.db.add_package(pkgname, pkgver, files_list, deps_list, build_meta=build_cfg)
                        self._log(pkgname, "db", "Registered package in DB")
                    else:
                        self._log(pkgname, "db", "DB manager has no add_package; skipping", level="warning")
                except Exception as e:
                    self._log(pkgname, "db", f"DB registration failed: {e}", level="warning")
        except Exception as e:
            self._log(pkgname, "db", f"DB registration warning: {e}", level="warning")

        # 11) cleanup workdir
        try:
            if build_cfg.get("cleanup_sources", True):
                shutil.rmtree(workdir, ignore_errors=True)
            else:
                self._log(pkgname, "cleanup", f"Preserving workdir {workdir}")
        except Exception:
            pass

        self._log(pkgname, "build.finish", f"Build finished: {pkgfull}")
        return True


# quick CLI for ad-hoc testing
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-builder")
    parser.add_argument("target", help="recipe path or package name")
    parser.add_argument("--config", default="/etc/zeropkg/config.toml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--root", default=None, help="optional root for chroot/install (e.g., /mnt/lfs)")
    args = parser.parse_args()

    b = Builder(config_path=args.config)
    try:
        ok = b.build(args.target, args=args, dry_run=args.dry_run, rebuild=args.rebuild)
        print("Build result:", ok)
    except Exception as exc:
        print("Build failed:", exc)
        sys.exit(2)
