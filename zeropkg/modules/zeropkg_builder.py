#!/usr/bin/env python3
"""
zeropkg_builder.py
Builder principal do Zeropkg — constrói pacotes a partir das receitas TOML.
"""

import os
import tarfile
import shutil
import subprocess
import logging

from zeropkg_logger import log_event
from zeropkg_toml import load_toml
from zeropkg_downloader import download_package
from zeropkg_patcher import Patcher
from zeropkg_installer import Installer
from zeropkg_db import DBManager
from zeropkg_deps import DependencyResolver, resolve_and_install
from zeropkg_chroot import prepare_chroot, cleanup_chroot

logger = logging.getLogger("zeropkg.builder")


class Builder:
    def __init__(self, db_path, ports_dir="/usr/ports", build_root="/var/zeropkg/build",
                 cache_dir="/usr/ports/distfiles", packages_dir="/var/zeropkg/packages"):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.build_root = build_root
        self.cache_dir = cache_dir
        self.packages_dir = packages_dir
        self.db = DBManager(db_path)

    def build(self, pkg_name, args, chroot=None, dir_install=None):
        """
        Constrói um pacote:
        - resolve dependências
        - baixa fontes
        - aplica patches/hooks
        - compila
        - instala em staging
        - empacota
        - instala (se solicitado)
        """
        meta = load_toml(self.ports_dir, pkg_name)
        pkg_fullname = f"{meta['package']['name']}-{meta['package']['version']}"

        staging_dir = os.path.join(self.build_root, f"{pkg_fullname}-staging")
        os.makedirs(staging_dir, exist_ok=True)

        log_event("builder", "start", f"Iniciando build de {pkg_fullname}")

        # --- 1. Resolver dependências ---
        resolver = DependencyResolver(self.db_path, self.ports_dir)
        resolve_and_install(resolver, meta["package"]["name"], Builder, Installer, args)

        # --- 2. Download do source ---
        source_dir = os.path.join(self.build_root, pkg_fullname)
        os.makedirs(source_dir, exist_ok=True)
        tarball = download_package(meta, self.cache_dir)
        if tarball.endswith((".tar.gz", ".tar.xz", ".tar.bz2")):
            with tarfile.open(tarball, "r:*") as tf:
                tf.extractall(source_dir)
        else:
            shutil.copy(tarball, source_dir)

        # entrar no diretório do código-fonte
        extracted_dirs = os.listdir(source_dir)
        if len(extracted_dirs) == 1:
            src_path = os.path.join(source_dir, extracted_dirs[0])
        else:
            src_path = source_dir

        # --- 3. Aplicar patches ---
        patcher = Patcher()
        patcher.apply_stage("pre_configure", src_path, meta)
        for patch in meta.get("patches", {}).get("files", []):
            patcher.apply_patch(src_path, patch)
        patcher.apply_stage("post_configure", src_path, meta)

        # --- 4. Ambiente ---
        env = os.environ.copy()
        if "build.env" in meta:
            for k, v in meta["build.env"].items():
                env[k] = v

        # --- 5. Construção ---
        def run_cmd(cmd, stage):
            if cmd:
                log_event("builder", stage, f"Executando: {cmd}")
                subprocess.run(cmd, cwd=src_path, shell=True, env=env, check=True)

        run_cmd(meta.get("build", {}).get("configure"), "configure")
        run_cmd(meta.get("build", {}).get("make"), "make")
        if meta.get("options", {}).get("run_tests"):
            run_cmd(meta.get("build", {}).get("check"), "check")

        # --- 6. Instalação em staging ---
        env["DESTDIR"] = staging_dir
        run_cmd(meta.get("build", {}).get("install"), "install")

        # --- 7. Empacotamento ---
        os.makedirs(self.packages_dir, exist_ok=True)
        pkg_file = os.path.join(self.packages_dir, f"{pkg_fullname}.tar.xz")
        with tarfile.open(pkg_file, "w:xz") as tf:
            tf.add(staging_dir, arcname="/")

        log_event("builder", "package", f"Pacote gerado: {pkg_file}")

        # --- 8. Instalar no sistema (se solicitado) ---
        if dir_install or not args.build_only:
            installer = Installer(self.db_path, self.ports_dir)
            installer.install(pkg_name, args, pkg_file=pkg_file, chroot=chroot, dir_install=dir_install)

        # --- 9. Registro no DB ---
        self.db.record_build_finish(pkg_name, meta["package"]["version"], pkg_file)

        # --- 10. Limpeza ---
        shutil.rmtree(staging_dir, ignore_errors=True)
        shutil.rmtree(source_dir, ignore_errors=True)

        log_event("builder", "finish", f"Build concluído de {pkg_fullname}")
