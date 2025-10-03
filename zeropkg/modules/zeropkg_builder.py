import os
import subprocess
import shutil
import tarfile
import logging
from typing import Dict, List
from zeropkg_downloader import download_package
from zeropkg_patcher import Patcher, PatchError, HookError

logger = logging.getLogger("zeropkg.builder")

class BuildError(Exception):
    pass

class Builder:
    def __init__(self, meta, cache_dir="/usr/ports/distfiles",
                 build_root="/var/zeropkg/build", pkg_cache="/var/zeropkg/packages",
                 chroot="/var/zeropkg/chroot", dry_run=False, dir_install=None):
        self.meta = meta
        self.cache_dir = cache_dir
        self.build_root = build_root
        self.pkg_cache = pkg_cache
        self.chroot = chroot
        self.dry_run = dry_run
        self.dir_install = dir_install

        self.workdir = os.path.join(build_root, f"{meta.name}-{meta.version}")
        self.stagingdir = os.path.join(self.workdir, "staging")

    def run(self, cmd: List[str], cwd=None, env=None, fakeroot=False):
        if self.dry_run:
            logger.info("[dry-run] %s", " ".join(cmd))
            return
        full_env = os.environ.copy()
        if env:
            full_env.update(env)

        if fakeroot:
            cmd = ["fakeroot"] + cmd

        if self.chroot and os.path.exists(self.chroot):
            cmd = ["chroot", self.chroot] + cmd

        logger.info("Executando: %s", " ".join(cmd))
        subprocess.run(cmd, cwd=cwd, env=full_env, check=True)

    def fetch_sources(self):
        for src in self.meta.sources:
            download_package(self.meta, cache_dir=self.cache_dir, verbose=True)

    def extract_sources(self):
        os.makedirs(self.workdir, exist_ok=True)
        for src in self.meta.sources:
            # apenas tar.* por enquanto
            path = os.path.join(self.cache_dir, os.path.basename(src.url))
            if path.endswith((".tar.gz", ".tar.xz", ".tar.bz2")):
                logger.info(f"Extraindo {path} em {self.workdir}")
                with tarfile.open(path, "r:*") as tar:
                    tar.extractall(self.workdir)
            else:
                shutil.copy(path, self.workdir)

    def build(self):
        patches = getattr(self.meta, "patches", {})
        hooks = getattr(self.meta, "hooks", {})
        env = getattr(self.meta, "environment", {})

        patcher = Patcher(workdir=self.workdir, env=env)

        # pre_configure
        patcher.apply_stage("pre_configure", patches, hooks)

        # configure
        self.run(["./configure", f"--prefix=/usr"], cwd=self.workdir, env=env)

        # build
        patcher.apply_stage("pre_build", patches, hooks)
        self.run(["make", "-j4"], cwd=self.workdir, env=env)
        patcher.apply_stage("post_build", patches, hooks)

        # install -> staging
        os.makedirs(self.stagingdir, exist_ok=True)
        patcher.apply_stage("pre_install", patches, hooks)
        self.run(["make", f"DESTDIR={self.stagingdir}", "install"],
                 cwd=self.workdir, env=env, fakeroot=True)
        patcher.apply_stage("post_install", patches, hooks)

    def package(self):
        pkgfile = os.path.join(self.pkg_cache, f"{self.meta.name}-{self.meta.version}.tar.xz")
        os.makedirs(self.pkg_cache, exist_ok=True)
        logger.info(f"Empacotando {pkgfile}")
        with tarfile.open(pkgfile, "w:xz") as tar:
            tar.add(self.stagingdir, arcname="/")
        return pkgfile

    def deploy(self, pkgfile):
        if self.dir_install:
            dest = self.dir_install
        else:
            dest = "/"

        logger.info(f"Instalando pacote {pkgfile} em {dest}")
        if self.dry_run:
            return
        self.run(["tar", "-xpf", pkgfile, "-C", dest], fakeroot=True)
