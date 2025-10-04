#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_installer.py — Zeropkg package installer and remover
Updated for full integration with zeropkg_db.record_install_quick()
"""

import os
import shutil
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional

from zeropkg_logger import get_logger
from zeropkg_db import record_install_quick, remove_package_quick, get_manifest_quick
from zeropkg_config import load_config
from zeropkg_chroot import prepare_chroot, cleanup_chroot
from zeropkg_patcher import apply_patches
from zeropkg_builder import build_package

log = get_logger("installer")


class InstallerError(Exception):
    pass


class ZeropkgInstaller:
    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(config_path)
        self.root_dir = Path(self.config["paths"]["root"]).resolve()
        self.cache_dir = Path(self.config["paths"]["cache"]).resolve()
        self.logs_dir = Path(self.config["paths"]["logs"]).resolve()
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.env = dict(os.environ)
        log.debug(f"Installer initialized with root={self.root_dir}")

    # ------------------------------------------------------------
    # Hooks execution
    # ------------------------------------------------------------
    def run_hook(self, hook: str, pkg_name: str, stage: str):
        """
        Run pre/post-install or remove hooks.
        """
        hook_script = Path(f"/usr/lib/zeropkg/hooks/{hook}.sh")
        if hook_script.exists():
            log.info(f"Running {hook} hook for {pkg_name}")
            try:
                subprocess.run(
                    ["/bin/bash", str(hook_script), pkg_name, stage],
                    check=True,
                    env=self.env,
                )
            except subprocess.CalledProcessError as e:
                raise InstallerError(f"Hook {hook} failed: {e}")

    # ------------------------------------------------------------
    # Binary packaging
    # ------------------------------------------------------------
    def _create_package_archive(
        self, build_dir: Path, pkg_name: str, version: str, fakeroot_env: bool = False
    ) -> Path:
        """
        Create a tar archive of the built package before installation.
        """
        tar_path = self.cache_dir / f"{pkg_name}-{version}.tar.zst"
        if not shutil.which("zstd"):
            tar_path = self.cache_dir / f"{pkg_name}-{version}.tar.gz"

        log.info(f"Packing {pkg_name}-{version} into {tar_path}")
        with tempfile.TemporaryDirectory() as tmpdir:
            pkg_temp = Path(tmpdir) / f"{pkg_name}-{version}"
            shutil.copytree(build_dir, pkg_temp, symlinks=True)
            mode = "w:gz" if tar_path.suffix == ".gz" else "w|"
            with tarfile.open(tar_path, mode, compresslevel=6) as tar:
                tar.add(pkg_temp, arcname=f"{pkg_name}-{version}")
        return tar_path

    # ------------------------------------------------------------
    # Main install entrypoint
    # ------------------------------------------------------------
    def install(
        self,
        pkg_name: str,
        build_dir: Path,
        manifest: Dict[str, List[str]],
        version: str,
        deps: Optional[List[Dict[str, str]]] = None,
        fakeroot_mode: bool = False,
        use_chroot: bool = True,
    ):
        """
        Install package into root filesystem.
        """
        log.info(f"Installing {pkg_name}-{version}")

        self.run_hook("pre_install", pkg_name, "pre")

        rootfs = self.root_dir
        tmp_mounts = []

        try:
            # prepare chroot if requested
            if use_chroot:
                tmp_mounts = prepare_chroot(rootfs)
                log.debug(f"Chroot prepared: {tmp_mounts}")

            # ensure fakeroot if requested
            install_env = dict(self.env)
            if fakeroot_mode and shutil.which("fakeroot"):
                install_cmd = ["fakeroot"]
            else:
                install_cmd = []

            # perform copy to / temporarily under fakeroot
            for category, files in manifest.items():
                for f in files:
                    src = build_dir / f.lstrip("/")
                    dest = rootfs / f.lstrip("/")
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if src.is_dir():
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                    elif src.exists():
                        shutil.copy2(src, dest, follow_symlinks=True)

            # Package archive before final move
            archive_path = self._create_package_archive(build_dir, pkg_name, version)
            log.info(f"Package archived at {archive_path}")

            # Register in DB
            record_install_quick(
                name_or_namever=f"{pkg_name}-{version}",
                manifest=manifest,
                deps=deps,
                metadata={
                    "fakeroot": fakeroot_mode,
                    "chroot": use_chroot,
                    "archive": str(archive_path),
                    "timestamp": time.time(),
                },
            )
            log.info(f"Registered {pkg_name}-{version} in DB")

            self.run_hook("post_install", pkg_name, "post")
        finally:
            if use_chroot:
                cleanup_chroot(rootfs, tmp_mounts)

    # ------------------------------------------------------------
    # Remove package
    # ------------------------------------------------------------
    def remove(self, pkg_name: str, version: Optional[str] = None, use_chroot: bool = True):
        """
        Remove installed package and unregister from DB.
        """
        log.info(f"Removing {pkg_name}-{version or '*'}")
        self.run_hook("pre_remove", pkg_name, "pre")

        rootfs = self.root_dir
        tmp_mounts = []
        try:
            if use_chroot:
                tmp_mounts = prepare_chroot(rootfs)

            manifest = get_manifest_quick(f"{pkg_name}-{version}" if version else pkg_name)
            if not manifest:
                raise InstallerError(f"Manifest not found for {pkg_name}")

            for category, files in manifest.items():
                for f in files:
                    target = rootfs / f.lstrip("/")
                    if target.exists():
                        try:
                            if target.is_dir():
                                shutil.rmtree(target)
                            else:
                                target.unlink()
                        except Exception as e:
                            log.warning(f"Failed to remove {target}: {e}")

            remove_package_quick(pkg_name, version)
            log.info(f"Removed {pkg_name}-{version or '*'} from DB")

            self.run_hook("post_remove", pkg_name, "post")
        finally:
            if use_chroot:
                cleanup_chroot(rootfs, tmp_mounts)


# ------------------------------------------------------------
# CLI entrypoint for manual testing
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Zeropkg Installer")
    parser.add_argument("--install", metavar="PKG", help="Install package from build dir")
    parser.add_argument("--remove", metavar="PKG", help="Remove package")
    parser.add_argument("--version", metavar="VER", default="1.0")
    parser.add_argument("--build-dir", metavar="DIR", default="./build")
    parser.add_argument("--fakeroot", action="store_true")
    parser.add_argument("--no-chroot", action="store_true")
    args = parser.parse_args()

    installer = ZeropkgInstaller()

    if args.install:
        fake_manifest = {
            "bin": ["/usr/bin/true"],
            "lib": ["/usr/lib/libfake.so"],
        }
        installer.install(
            args.install,
            Path(args.build_dir),
            fake_manifest,
            args.version,
            fakeroot_mode=args.fakeroot,
            use_chroot=not args.no_chroot,
        )
    elif args.remove:
        installer.remove(args.remove, args.version, use_chroot=not args.no_chroot)
    else:
        parser.print_help()
