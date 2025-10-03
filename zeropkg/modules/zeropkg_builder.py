"""
zeropkg_builder.py

Builder para Zeropkg — versão revisada e integrada.

Responsabilidades:
- Buscar fontes (Downloader)
- Extrair (multi-formato)
- Aplicar patches & hooks (Patcher)
- Executar configure/build/install no staging com fakeroot
- Empacotar (tar.xz)
- Suporte a `--dir_install`, `--dry_run`
- Logging de eventos
- Reuso de binários (cache)
- (Opcional) integração leve com DB para builds
"""

import os
import shutil
import subprocess
import tarfile
import logging
from typing import List, Optional
from zeropkg_downloader import download_package
from zeropkg_patcher import Patcher
from zeropkg_logger import log_event, setup_logger

logger = setup_logger(pkg_name=None, stage="builder")

class BuildError(Exception):
    pass

class Builder:
    def __init__(self,
                 meta,
                 cache_dir="/usr/ports/distfiles",
                 pkg_cache="/var/zeropkg/packages",
                 build_root="/var/zeropkg/build",
                 chroot: Optional[str] = None,
                 dry_run: bool = False,
                 dir_install: Optional[str] = None,
                 verbose: bool = False):
        self.meta = meta
        self.cache_dir = cache_dir
        self.pkg_cache = pkg_cache
        self.build_root = build_root
        self.chroot = chroot
        self.dry_run = dry_run
        self.dir_install = dir_install
        self.verbose = verbose

        # diretórios de trabalho e staging
        base = f"{meta.name}-{meta.version}"
        self.workdir = os.path.join(build_root, base)
        self.stagingdir = os.path.join(self.workdir, "staging")

    def run(self, cmd: List[str], cwd: Optional[str] = None,
            env: Optional[dict] = None, fakeroot: bool = False):
        """Executa um comando, respeitando dry_run, chroot e fakeroot."""
        if self.dry_run:
            log_event(self.meta.name, "builder", "[dry-run] " + " ".join(cmd))
            return

        full_env = os.environ.copy()
        if env:
            full_env.update(env)

        cmd_exec = list(cmd)
        if fakeroot:
            cmd_exec.insert(0, "fakeroot")

        if self.chroot:
            # TODO: montar bind /proc, /dev etc no chroot antes
            cmd_exec = ["chroot", self.chroot] + cmd_exec

        log_event(self.meta.name, "builder", "Executando: " + " ".join(cmd_exec))
        try:
            subprocess.run(cmd_exec, cwd=cwd, env=full_env, check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as e:
            msg = f"Erro build (cmd={' '.join(cmd_exec)}): {e.stderr}"
            log_event(self.meta.name, "builder", msg, level="error")
            raise BuildError(msg) from e

    def fetch_sources(self):
        # baixar fontes
        log_event(self.meta.name, "builder", "Buscando fontes")
        path = download_package(self.meta, cache_dir=self.cache_dir,
                                prefer_existing=True, verbose=self.verbose)
        log_event(self.meta.name, "builder", f"Fonte obtida: {path}")
        return path

    def extract_sources(self, src_path: Optional[str] = None):
        log_event(self.meta.name, "builder", "Extraindo fontes")
        os.makedirs(self.workdir, exist_ok=True)
        if src_path is None:
            # deduzir: usar downloader resolve_cache_name
            # Encontra o arquivo no cache_dir com resolve name
            # Vamos simplificar: escolher primeiro source e recomputar
            src_path = os.path.join(self.cache_dir,
                                    os.path.basename(self.meta.sources[0].url))
        # suporte tar.*, zip
        if tarfile.is_tarfile(src_path):
            with tarfile.open(src_path, "r:*") as tar:
                tar.extractall(self.workdir)
        elif src_path.lower().endswith(".zip"):
            import zipfile
            with zipfile.ZipFile(src_path, "r") as zf:
                zf.extractall(self.workdir)
        else:
            # arquivo simples: copiar
            shutil.copy(src_path, self.workdir)

    def build(self):
        patches = getattr(self.meta, "patches", {})
        hooks = getattr(self.meta, "hooks", {})
        env = getattr(self.meta, "environment", {}) or {}

        p = Patcher(workdir=self.workdir, env=env, pkg_name=self.meta.name)

        # etapa pre_configure
        p.apply_stage("pre_configure", patches, hooks)

        # configure
        cfg_cmd = self.meta.build.get("configure_cmds",
                                     ["./configure", f"--prefix=/usr"])
        for c in cfg_cmd:
            self.run(c.split(), cwd=self.workdir, env=env)

        # etapa patches/hooks pré-build
        p.apply_stage("pre_build", patches, hooks)

        # build
        build_cmds = self.meta.build.get("build_cmds", ["make", "-j4"])
        for c in build_cmds:
            self.run(c.split(), cwd=self.workdir, env=env)

        # pós-build hooks
        p.apply_stage("post_build", patches, hooks)

        # instalar em staging
        os.makedirs(self.stagingdir, exist_ok=True)
        p.apply_stage("pre_install", patches, hooks)

        install_cmds = self.meta.build.get("install_cmds",
                                           ["make", f"DESTDIR={self.stagingdir}", "install"])
        for c in install_cmds:
            self.run(c.split(), cwd=self.workdir, env=env, fakeroot=True)

        p.apply_stage("post_install", patches, hooks)

    def package(self) -> str:
        # empacotar staging em tar.xz
        os.makedirs(self.pkg_cache, exist_ok=True)
        pkgfname = f"{self.meta.name}-{self.meta.version}.tar.xz"
        pkgpath = os.path.join(self.pkg_cache, pkgfname)
        log_event(self.meta.name, "builder", f"Empacotando em {pkgpath}")
        if not self.dry_run:
            with tarfile.open(pkgpath, "w:xz") as tar:
                tar.add(self.stagingdir, arcname=".")
        return pkgpath

    def install(self) -> str:
        """Método auxiliar: construir e instalar (empacotar + retorno pkgfile)."""
        self.fetch_sources()
        self.extract_sources()
        self.build()
        pkgfile = self.package()
        return pkgfile
