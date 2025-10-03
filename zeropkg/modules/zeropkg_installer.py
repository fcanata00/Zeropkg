#!/usr/bin/env python3
"""
zeropkg_installer.py
Instalador e removedor de pacotes do Zeropkg
"""

import os
import tarfile
import shutil
import logging
from pathlib import Path
from typing import Optional, Dict, List

from zeropkg_logger import log_event
from zeropkg_patcher import Patcher
from zeropkg_db import connect, record_install, remove_package
from zeropkg_chroot import prepare_chroot, cleanup_chroot
from zeropkg_deps import DependencyResolver

logger = logging.getLogger("zeropkg.installer")


class InstallError(Exception):
    pass


class Installer:
    def __init__(self, db_path: str, ports_dir: str = "/usr/ports",
                 root: str = "/", dry_run: bool = False, use_fakeroot: bool = True):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = root
        self.dry_run = dry_run
        self.use_fakeroot = use_fakeroot

    # -----------------------------------
    # Instalação
    # -----------------------------------
    def install(self, name: str, args, pkg_file: Optional[str] = None,
                meta: Optional[Dict] = None, dir_install: Optional[str] = None) -> bool:
        log_event(name, "install", f"Iniciando instalação de {name} em {self.root}")

        staging_dir = os.path.join("/tmp", f"zeropkg-install-{name}")
        os.makedirs(staging_dir, exist_ok=True)

        use_chroot = meta.get("options", {}).get("chroot", False) if meta else False
        chroot_prepared = False

        try:
            if use_chroot:
                prepare_chroot(self.root, copy_resolv=True, dry_run=self.dry_run)
                chroot_prepared = True

            # 1. Hooks pre_install
            if meta and "hooks" in meta:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("pre_install", meta["hooks"])

            # 2. Localizar pacote
            if not pkg_file and meta:
                pkg_fullname = f"{meta['package']['name']}-{meta['package']['version']}"
                pkg_file = os.path.join("/var/zeropkg/packages", f"{pkg_fullname}.tar.xz")

            if not pkg_file or not os.path.exists(pkg_file):
                raise InstallError(f"Pacote não encontrado: {pkg_file}")

            # 3. Extrair pacote no staging
            if not self.dry_run:
                with tarfile.open(pkg_file, "r:*") as tf:
                    tf.extractall(staging_dir)

            # 4. Copiar staging para root
            if not self.dry_run:
                for root_dir, _, files in os.walk(staging_dir):
                    rel = os.path.relpath(root_dir, staging_dir)
                    dest_dir = os.path.join(self.root, rel if rel != "." else "")
                    os.makedirs(dest_dir, exist_ok=True)
                    for f in files:
                        src = os.path.join(root_dir, f)
                        dst = os.path.join(dest_dir, f)
                        if self.use_fakeroot:
                            os.system(f"fakeroot cp -a {src} {dst}")
                        else:
                            shutil.copy2(src, dst)
                        log_event(name, "install", f"Instalado {dst}")

            # 5. Registrar no DB
            if not self.dry_run and meta:
                conn = connect(self.db_path)
                record_install(conn, meta, pkg_file)
                conn.close()

            # 6. Hooks post_install
            if meta and "hooks" in meta:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("post_install", meta["hooks"])

            log_event(name, "install", f"Instalação concluída de {name}")
            return True

        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
            if chroot_prepared:
                cleanup_chroot(self.root, force_lazy=True, dry_run=self.dry_run)

    # -----------------------------------
    # Remoção
    # -----------------------------------
    def remove(self, name: str, version: Optional[str] = None,
               hooks: Optional[Dict[str, List[str]]] = None,
               force: bool = False) -> bool:
        log_event(name, "remove", f"Iniciando remoção de {name} {version or ''}")

        chroot_prepared = False
        try:
            prepare_chroot(self.root, copy_resolv=True, dry_run=self.dry_run)
            chroot_prepared = True

            # 0. Verificar dependências reversas
            resolver = DependencyResolver(self.db_path, ports_dir=self.ports_dir)
            revdeps = resolver.reverse_deps(name)
            if revdeps and not force:
                log_event(name, "remove",
                          f"Abortado: {name} ainda é dependência de {', '.join(revdeps)}",
                          level="error")
                return False

            # 1. Hooks pre_remove
            if hooks:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("pre_remove", hooks)

            # 2. Remover arquivos listados no DB
            conn = connect(self.db_path)
            paths = remove_package(conn, name, version)
            conn.close()

            if not self.dry_run:
                for p in paths:
                    dst = Path(self.root) / p.lstrip("/")
                    if dst.exists():
                        try:
                            dst.unlink() if dst.is_file() else shutil.rmtree(dst)
                            log_event(name, "remove", f"Removido {dst}")
                        except Exception as e:
                            log_event(name, "remove", f"Erro ao remover {dst}: {e}", level="error")

                # Limpar diretórios vazios ascendentes
                for p in paths:
                    d = os.path.dirname(os.path.join(self.root, p.lstrip("/")))
                    while d and d != self.root and os.path.isdir(d):
                        if not os.listdir(d):
                            os.rmdir(d)
                            d = os.path.dirname(d)
                        else:
                            break

            # 3. Hooks post_remove
            if hooks:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("post_remove", hooks)

            log_event(name, "remove", f"Remoção concluída: {name} {version or ''}")
            return True

        finally:
            if chroot_prepared:
                cleanup_chroot(self.root, force_lazy=True, dry_run=self.dry_run)
