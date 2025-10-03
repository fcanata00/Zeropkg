#!/usr/bin/env python3
# zeropkg_builder.py — Builder definitivo, integrado e funcional
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import shutil
import tarfile
import subprocess
import tempfile
import glob
import logging
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, List

# Logger/DB/Other modules (tolerant imports)
from zeropkg_logger import log_event, get_logger

# TOML loader (tolerant)
try:
    from zeropkg_toml import load_toml
except Exception:
    def load_toml(path_or_a, b=None):
        raise RuntimeError("load_toml not available; adapte zeropkg_toml.py")


# Downloader adapter: prefer class Downloader.fetch_sources or function download_package
_DOWNLOADER = None
try:
    # prefer class-based Downloader
    from zeropkg_downloader import Downloader as _Downloader
    def _downloader_fetch(meta: Dict, cache_dir: str, dest: Optional[str] = None) -> List[str]:
        dl = _Downloader(dist_dir=cache_dir, env=meta.get("environment", {}))
        sources = meta.get("source") or meta.get("sources") or meta.get("distfiles") or []
        # normalize to list of dicts {url, checksum, algo}
        normalized = []
        for s in sources:
            if isinstance(s, str):
                normalized.append({"url": s})
            elif isinstance(s, dict):
                normalized.append(s)
            else:
                normalized.append({"url": str(s)})
        return dl.fetch_sources(meta.get("package", {}).get("name", "pkg"), normalized)
    _DOWNLOADER = _downloader_fetch
except Exception:
    try:
        # fallback: function download_package(meta, cache_dir)
        from zeropkg_downloader import download_package as _download_package
        def _downloader_fetch(meta: Dict, cache_dir: str, dest: Optional[str] = None) -> List[str]:
            r = _download_package(meta, cache_dir)
            if isinstance(r, list):
                return r
            return [r]
        _DOWNLOADER = _downloader_fetch
    except Exception:
        _DOWNLOADER = None


# Patcher
try:
    from zeropkg_patcher import Patcher
except Exception:
    Patcher = None

# Installer
try:
    from zeropkg_installer import Installer
except Exception:
    Installer = None

# DependencyResolver and helper
try:
    from zeropkg_deps import DependencyResolver, resolve_and_install
except Exception:
    DependencyResolver = None
    resolve_and_install = None

# DBManager
try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# Chroot helpers
try:
    from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot
except Exception:
    prepare_chroot = None
    cleanup_chroot = None
    run_in_chroot = None

logger = get_logger("builder")


class BuildError(Exception):
    pass


class Builder:
    def __init__(
        self,
        db_path: str = "/var/lib/zeropkg/installed.sqlite3",
        ports_dir: str = "/usr/ports",
        build_root: str = "/var/zeropkg/build",
        cache_dir: str = "/usr/ports/distfiles",
        packages_dir: str = "/var/zeropkg/packages",
        jobs: Optional[int] = None,
    ):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.build_root = Path(build_root)
        self.cache_dir = Path(cache_dir)
        self.packages_dir = Path(packages_dir)
        self.jobs = jobs or max(1, (os.cpu_count() or 1))
        # ensure directories
        self.build_root.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.packages_dir.mkdir(parents=True, exist_ok=True)
        # DB manager if available
        self.db = DBManager(db_path) if DBManager else None

    # -------------------------
    # Helpers
    # -------------------------
    def _log(self, pkg: str, stage: str, msg: str, level: str = "info"):
        try:
            log_event(pkg, stage, msg, level=level)
        except Exception:
            getattr(logger, level, logger.info)(f"{pkg} {stage}: {msg}")

    def _load_meta(self, target: str) -> Dict[str, Any]:
        """
        Load recipe meta. target may be a path or pkgname. Try several approaches for compatibility.
        """
        # If path to file exists
        if os.path.isfile(target):
            try:
                return load_toml(target)
            except TypeError:
                # signature mismatch in load_toml
                return load_toml(target)
        # try load_toml(ports_dir, pkgname)
        try:
            return load_toml(self.ports_dir, target)
        except TypeError:
            # fallback: find metafiles under ports_dir
            pattern = os.path.join(self.ports_dir, "**", f"{target}-*.toml")
            matches = glob.glob(pattern, recursive=True)
            if not matches:
                raise FileNotFoundError(f"Metafile for {target} not found under {self.ports_dir}")
            return load_toml(matches[-1])
        except Exception as e:
            # fallback search
            pattern = os.path.join(self.ports_dir, "**", f"{target}-*.toml")
            matches = glob.glob(pattern, recursive=True)
            if not matches:
                raise
            return load_toml(matches[-1])

    def _extract_archive(self, path: str, dest: str):
        """
        Extract tarballs (tar.gz, tar.xz, etc.) or copy a tree.
        """
        path = str(path)
        if os.path.isdir(path):
            # copy tree
            shutil.copytree(path, dest, dirs_exist_ok=True)
            return
        if tarfile.is_tarfile(path):
            with tarfile.open(path, "r:*") as tf:
                # extract preserving structure into dest; some tarballs contain top-level folder
                tf.extractall(dest)
            return
        # fallback: copy file
        os.makedirs(dest, exist_ok=True)
        shutil.copy2(path, os.path.join(dest, os.path.basename(path)))

    def _discover_src_root(self, src_dir: Path) -> Path:
        """
        Identify the source root directory inside src_dir: the single child dir or src_dir itself.
        """
        children = [p for p in src_dir.iterdir() if p.is_dir() and not p.name.startswith(".")]
        if len(children) == 1:
            return children[0]
        return src_dir

    def _run(self, cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None,
             chroot_root: Optional[str] = None, dry_run: bool = False):
        """
        Execute a shell command. If chroot_root is provided and run_in_chroot exists, use it.
        Raises BuildError on non-zero exit.
        """
        self._log("builder", "cmd", f"CMD: {cmd} (cwd={cwd}, chroot={chroot_root}, dry_run={dry_run})", level="debug")
        if dry_run:
            return 0
        if chroot_root and run_in_chroot:
            rc = run_in_chroot(chroot_root, cmd, env=env, cwd=cwd)
            if rc != 0:
                raise BuildError(f"Command in chroot failed ({rc}): {cmd}")
            return rc
        else:
            proc = subprocess.run(cmd, shell=True, cwd=cwd, env=env)
            if proc.returncode != 0:
                raise BuildError(f"Command failed ({proc.returncode}): {cmd}")
            return proc.returncode

    # -------------------------
    # Public API: build
    # -------------------------
    def build(self, target: str, args: Any, dir_install: Optional[str] = None) -> bool:
        """
        Build pipeline:
            target: package name or metafile path
            args: CLI args-like object with fields:
                  dry_run, root, fakeroot, build_root, cache_dir, packages_dir, build_only, dir_install
            dir_install: alternative install dir (overrides args.root), used by --dir-install
        """
        dry_run = getattr(args, "dry_run", False)
        fakeroot = getattr(args, "fakeroot", True)
        build_root = getattr(args, "build_root", str(self.build_root))
        cache_dir = getattr(args, "cache_dir", str(self.cache_dir))
        packages_dir = getattr(args, "packages_dir", str(self.packages_dir))
        root = dir_install if dir_install else getattr(args, "root", "/")
        # load meta
        meta = self._load_meta(target)
        pkginfo = meta.get("package", {})
        pkgname = pkginfo.get("name") or target
        pkgversion = pkginfo.get("version") or "0"
        pkgfull = f"{pkgname}-{pkgversion}"

        self._log(pkgname, "build.start", f"Starting build for {pkgfull}")

        # register build start if DB supports it
        try:
            if self.db and hasattr(self.db, "record_build_start"):
                try:
                    self.db.record_build_start(pkgname, pkgversion)
                except Exception:
                    # ignore if specific function not present
                    pass
        except Exception:
            pass

        # prepare working directories
        workdir = Path(build_root) / pkgfull
        srcdir = workdir / "src"
        builddir = workdir / "build"
        stagingdir = workdir / "staging"
        for d in (workdir, srcdir, builddir, stagingdir):
            d.mkdir(parents=True, exist_ok=True)

        # 1) resolve dependencies (use resolve_and_install if available)
        try:
            if DependencyResolver and resolve_and_install:
                resolver = DependencyResolver(self.db_path, self.ports_dir)
                # include build deps for toolchain stages if args indicates (we respect args.build_deps)
                include_build = getattr(args, "include_build_deps", False)
                # resolve_and_install will build+install missing deps (uses Builder & Installer classes)
                resolve_and_install(resolver, pkgname, Builder, Installer, args)
                self._log(pkgname, "deps", "Dependencies resolved/installed")
            else:
                self._log(pkgname, "deps", "DependencyResolver or resolve_and_install not available", level="warning")
        except Exception as e:
            self._log(pkgname, "deps", f"Dependency resolution failed: {e}", level="error")
            raise BuildError(f"Dependency resolution failed: {e}")

        # 2) download sources via downloader
        downloaded_paths: List[str] = []
        try:
            if _DOWNLOADER:
                downloaded_paths = _DOWNLOADER(meta, cache_dir, dest=str(srcdir))
                if not isinstance(downloaded_paths, list):
                    downloaded_paths = [downloaded_paths]
                self._log(pkgname, "download", f"Downloaded sources: {downloaded_paths}")
            else:
                self._log(pkgname, "download", "No downloader available; skipping download", level="warning")
        except Exception as e:
            self._log(pkgname, "download", f"Download failed: {e}", level="error")
            raise BuildError(f"Download failed: {e}")

        # 3) extract sources into srcdir (if downloader placed tarballs)
        try:
            # If downloader returned tarball paths, extract them
            for src in downloaded_paths:
                if not src:
                    continue
                if os.path.exists(src):
                    # If it's directory, copy into srcdir; if tarball, extract
                    try:
                        self._extract_archive(src, str(srcdir))
                    except Exception as e:
                        self._log(pkgname, "extract", f"Extraction warning: {e}", level="warning")
            self._log(pkgname, "extract", f"Sources prepared in {srcdir}")
        except Exception as e:
            self._log(pkgname, "extract", f"Extraction failed: {e}", level="error")
            raise BuildError(f"Extraction failed: {e}")

        # determine actual source root
        src_root = self._discover_src_root(srcdir)

        # prepare environment for build
        env = os.environ.copy()
        # inherit build env from meta
        build_env = meta.get("build", {}).get("env") or meta.get("environment") or meta.get("build_env") or {}
        if isinstance(build_env, dict):
            for k, v in build_env.items():
                env[str(k)] = str(v)
        # add parallel jobs
        env["MAKEFLAGS"] = env.get("MAKEFLAGS", f"-j{self.jobs}")
        # LFS-specific variables may exist in meta.options
        if meta.get("options", {}).get("lfs_stage"):
            # example environment additions; real variables depend on recipe
            env.setdefault("LFS", getattr(args, "lfs", "/mnt/lfs"))
            if "LFS_TGT" in meta.get("options", {}):
                env["LFS_TGT"] = meta["options"]["LFS_TGT"]

        # chroot decision
        use_chroot = meta.get("options", {}).get("chroot", False)
        chroot_root = root if use_chroot else None

        # create Patcher
        patcher = Patcher(workdir=str(src_root), ports_dir=self.ports_dir, env=build_env, pkg_name=pkgname) if Patcher else None

        # 4) apply pre_configure patches/hooks
        try:
            if patcher:
                patcher.apply_stage("pre_configure", patches=meta.get("patches", {}).get("files"), hooks=meta.get("hooks", {}))
            else:
                self._log(pkgname, "patch", "Patcher not available; skipping pre_configure", level="warning")
        except Exception as e:
            self._log(pkgname, "patch", f"pre_configure stage failed: {e}", level="error")
            raise BuildError(f"pre_configure failed: {e}")

        # 5) configure
        try:
            cfg = meta.get("build", {}).get("configure")
            if cfg:
                # expand common tokens
                cfg_cmd = cfg.replace("@DESTDIR@", str(stagingdir))
                self._run(cfg_cmd, cwd=str(src_root), env=env, chroot_root=chroot_root, dry_run=dry_run)
                self._log(pkgname, "configure", f"Ran configure: {cfg_cmd}")
        except Exception as e:
            self._log(pkgname, "configure", f"Configure failed: {e}", level="error")
            raise BuildError(f"Configure failed: {e}")

        # 6) post_configure patches/hooks
        try:
            if patcher:
                patcher.apply_stage("post_configure", patches=meta.get("patches", {}).get("files"), hooks=meta.get("hooks", {}))
        except Exception as e:
            self._log(pkgname, "patch", f"post_configure stage failed: {e}", level="warning")
            # depending on recipe, may or may not be fatal; default: warning

        # 7) make / build
        try:
            mk = meta.get("build", {}).get("make") or meta.get("build", {}).get("build") or "make"
            # allow tuple or list
            if isinstance(mk, list):
                for cmd in mk:
                    self._run(cmd, cwd=str(src_root), env=env, chroot_root=chroot_root, dry_run=dry_run)
            else:
                # run standard make with jobs if default
                if mk == "make":
                    mk_cmd = f"make -j{self.jobs}"
                else:
                    mk_cmd = mk
                self._run(mk_cmd, cwd=str(src_root), env=env, chroot_root=chroot_root, dry_run=dry_run)
            self._log(pkgname, "build", "Make completed")
        except Exception as e:
            self._log(pkgname, "build", f"Build failed: {e}", level="error")
            raise BuildError(f"Build failed: {e}")

        # 8) check/tests
        try:
            chk = meta.get("build", {}).get("check")
            if chk and meta.get("options", {}).get("run_tests", False):
                self._run(chk, cwd=str(src_root), env=env, chroot_root=chroot_root, dry_run=dry_run)
                self._log(pkgname, "check", "Tests executed")
        except Exception as e:
            self._log(pkgname, "check", f"Tests failed: {e}", level="warning")
            if meta.get("options", {}).get("fail_on_test", False):
                raise BuildError(f"Tests failed and fail_on_test set: {e}")

        # 9) pre_install patch/hook
        try:
            if patcher:
                patcher.apply_stage("pre_install", patches=meta.get("patches", {}).get("files"), hooks=meta.get("hooks", {}))
        except Exception as e:
            self._log(pkgname, "patch", f"pre_install stage failed: {e}", level="warning")

        # 10) install into staging (DESTDIR)
        try:
            inst_cmd = meta.get("build", {}).get("install") or meta.get("build", {}).get("install_cmd")
            if inst_cmd:
                dest_for_cmd = f" DESTDIR={stagingdir}"
                full_inst = inst_cmd + dest_for_cmd
                self._run(full_inst, cwd=str(src_root), env=env, chroot_root=chroot_root, dry_run=dry_run)
            else:
                # fallback: try "make install DESTDIR=staging"
                self._run(f"make DESTDIR={stagingdir} install", cwd=str(src_root), env=env, chroot_root=chroot_root, dry_run=dry_run)
            self._log(pkgname, "install_stage", f"Install to staging done at {stagingdir}")
        except Exception as e:
            self._log(pkgname, "install_stage", f"Install into staging failed: {e}", level="error")
            raise BuildError(f"Install into staging failed: {e}")

        # 11) post_install patches/hooks
        try:
            if patcher:
                patcher.apply_stage("post_install", patches=meta.get("patches", {}).get("files"), hooks=meta.get("hooks", {}))
        except Exception as e:
            self._log(pkgname, "patch", f"post_install stage failed: {e}", level="warning")

        # 12) packaging: create tar.xz from staging
        package_file = self.packages_dir / f"{pkgfull}.tar.xz"
        try:
            if not dry_run:
                with tarfile.open(str(package_file), "w:xz") as tf:
                    # Add contents of stagingdir preserving leading '/'
                    # We add each child so that archive content is the tree under staging
                    for entry in stagingdir.rglob("*"):
                        # compute arcname relative to staging root
                        arcname = "/" + str(entry.relative_to(stagingdir)).lstrip("/")
                        tf.add(str(entry), arcname=arcname)
                self._log(pkgname, "package", f"Package created: {package_file}")
            else:
                self._log(pkgname, "package", f"[dry-run] Would create package: {package_file}")
        except Exception as e:
            self._log(pkgname, "package", f"Packaging failed: {e}", level="error")
            raise BuildError(f"Packaging failed: {e}")

        # 13) optionally install to dir_install or call Installer.install
        try:
            do_install = False
            if dir_install:
                do_install = True
            elif not getattr(args, "build_only", False):
                do_install = True

            if do_install:
                if not Installer:
                    self._log(pkgname, "installer", "Installer module not available; skipping install", level="warning")
                else:
                    installer = Installer(db_path=self.db_path, ports_dir=self.ports_dir, root=root, dry_run=dry_run, use_fakeroot=fakeroot)
                    # If dir_install, pass dir_install; else installer will install into root
                    installer.install(pkgname, args, pkg_file=str(package_file), meta=meta, dir_install=dir_install)
                    self._log(pkgname, "installer", f"Installer.install called for {pkgfull}")
        except Exception as e:
            self._log(pkgname, "installer", f"Installer failed: {e}", level="error")
            raise BuildError(f"Installer failed: {e}")

        # 14) finalize DB registration
        try:
            files_recorded = []
            # try list files under staging to record
            for f in stagingdir.rglob("*"):
                if f.is_file():
                    files_recorded.append(str(Path("/") / f.relative_to(stagingdir)))
            deps_list = []
            # try extract dependencies from meta
            deps_struct = meta.get("dependencies") or meta.get("depends") or meta.get("deps") or {}
            # normalize to list of dicts
            if isinstance(deps_struct, dict):
                # combine runtime and build if present
                for key in ("runtime", "build"):
                    v = deps_struct.get(key, [])
                    if isinstance(v, list):
                        for item in v:
                            if isinstance(item, dict):
                                deps_list.append(item)
                            else:
                                deps_list.append({"name": str(item), "version": None})
                    elif isinstance(v, dict):
                        for n, ver in v.items():
                            deps_list.append({"name": n, "version": ver})
            elif isinstance(deps_struct, list):
                for item in deps_struct:
                    if isinstance(item, dict):
                        deps_list.append(item)
                    else:
                        deps_list.append({"name": str(item), "version": None})
            # add to DB via DBManager.add_package if available
            if self.db and hasattr(self.db, "add_package"):
                try:
                    self.db.add_package(pkgname, pkgversion, files_recorded, deps_list, build_options=json.dumps(build_env))
                    self._log(pkgname, "db", "Package recorded in DB")
                except Exception as e:
                    self._log(pkgname, "db", f"DB add_package failed: {e}", level="warning")
        except Exception as e:
            self._log(pkgname, "db", f"DB registration warning: {e}", level="warning")

        # 15) cleanup staging and optionally sources (if recipe asks)
        try:
            if meta.get("options", {}).get("cleanup_sources", True):
                try:
                    shutil.rmtree(srcdir, ignore_errors=True)
                except Exception:
                    pass
            try:
                shutil.rmtree(stagingdir, ignore_errors=True)
            except Exception:
                pass
        except Exception:
            pass

        # register build finish
        try:
            if self.db and hasattr(self.db, "record_build_finish"):
                try:
                    self.db.record_build_finish(pkgname, pkgversion, str(package_file))
                except Exception:
                    pass
        except Exception:
            pass

        self._log(pkgname, "build.finish", f"Build finished: {pkgfull}")
        return True


# If used as a script for basic testing
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(prog="zeropkg-builder", description="Zeropkg Builder (integrated)")
    p.add_argument("target", help="Package name or metafile")
    p.add_argument("--db-path", default="/var/lib/zeropkg/installed.sqlite3")
    p.add_argument("--ports-dir", default="/usr/ports")
    p.add_argument("--build-root", default="/var/zeropkg/build")
    p.add_argument("--cache-dir", default="/usr/ports/distfiles")
    p.add_argument("--packages-dir", default="/var/zeropkg/packages")
    p.add_argument("--root", default="/", help="Install root")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--fakeroot", action="store_true")
    p.add_argument("--build-only", action="store_true")
    p.add_argument("--dir-install", default=None, help="Install directly to this directory after build")
    args = p.parse_args()

    # create args-like object
    class ArgsObj:
        pass
    a = ArgsObj()
    for k, v in vars(args).items():
        setattr(a, k.replace("-", "_"), v)
    # set extra default fields expected by builder
    a.ports_dir = args.ports_dir
    a.db_path = args.db_path
    a.build_root = args.build_root
    a.cache_dir = args.cache_dir
    a.packages_dir = args.packages_dir
    a.dry_run = args.dry_run
    a.fakeroot = args.fakeroot
    a.build_only = args.build_only
    a.root = args.root

    builder = Builder(db_path=a.db_path, ports_dir=a.ports_dir, build_root=a.build_root, cache_dir=a.cache_dir, packages_dir=a.packages_dir)
    try:
        ok = builder.build(args.target, a, dir_install=args.dir_install)
        print("Build finished:", ok)
    except Exception as e:
        print("Build failed:", e)
        sys.exit(2)
