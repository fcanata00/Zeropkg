#!/usr/bin/env python3
"""
zeropkg_installer.py
Instalador e removedor de pacotes para Zeropkg
- Instala pacotes gerados pelo builder
- Remove pacotes com segurança (checando dependências reversas)
- Suporta hooks e fakeroot
"""

import os
import tarfile
import shutil
import logging
from typing import Optional, Dict, List

from zeropkg_logger import log_event
from zeropkg_patcher import Patcher
from zeropkg_db import connect, record_install, remove_package
from zeropkg_chroot import prepare_chroot, cleanup_chroot, ChrootError
from zeropkg_deps import DependencyResolver

logger = logging.getLogger("zeropkg.installer")


class InstallError(Exception):
    pass


class Installer:
    def __init__(self, db_path: str, ports_dir: str = "/usr/ports",
                 root: str = "/", dry_run: bool = False):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = root
        self.dry_run = dry_run

    # ------------------------------
    # instalação
    # ------------------------------
    def install(self, name: str, args, pkg_file: Optional[str] = None,
                meta: Optional[Dict] = None, chroot: Optional[str] = None,
                dir_install: Optional[str] = None) -> bool:
        log_event(name, "install", f"Iniciando instalação de {name} em {self.root}")

        staging_dir = os.path.join("/tmp", f"zeropkg-install-{name}")
        os.makedirs(staging_dir, exist_ok=True)

        chroot_prepared = False
        try:
            if os.path.abspath(self.root) != "/":
                try:
                    prepare_chroot(self.root, copy_resolv=True, dry_run=self.dry_run)
                    chroot_prepared = True
                except ChrootError as ce:
                    log_event(name, "install", f"Falha ao preparar chroot {self.root}: {ce}", level="warning")

            # 1. hooks pre_install
            if meta and "hooks" in meta:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("pre_install", hooks=meta["hooks"])

            # 2. extrair pacote no staging
            if not pkg_file and meta:
                pkg_fullname = f"{meta['package']['name']}-{meta['package']['version']}"
                pkg_file = os.path.join("/var/zeropkg/packages", f"{pkg_fullname}.tar.xz")

            if not pkg_file or not os.path.exists(pkg_file):
                raise InstallError(f"Pacote não encontrado: {pkg_file}")

            if not self.dry_run:
                with tarfile.open(pkg_file, "r:*") as tf:
                    tf.extractall(staging_dir)

            # 3. copiar staging para root
            if not self.dry_run:
                for root_dir, dirs, files in os.walk(staging_dir):
                    rel = os.path.relpath(root_dir, staging_dir)
                    dest_dir = os.path.join(self.root, rel if rel != "." else "")
                    os.makedirs(dest_dir, exist_ok=True)
                    for f in files:
                        src = os.path.join(root_dir, f)
                        dst = os.path.join(dest_dir, f)
                        shutil.copy2(src, dst)
                        log_event(name, "install", f"Instalado {dst}")

            # 4. registrar no DB
            if not self.dry_run and meta:
                conn = connect(self.db_path)
                record_install(conn, meta, pkg_file)
                conn.close()

            # 5. hooks post_install
            if meta and "hooks" in meta:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("post_install", hooks=meta["hooks"])

            log_event(name, "install", f"Instalação concluída de {name}")
            return True
        except Exception as e:
            log_event(name, "install", f"Erro na instalação: {e}", level="error")
            raise
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
            if chroot_prepared:
                try:
                    cleanup_chroot(self.root, force_lazy=True, dry_run=self.dry_run)
                except Exception as e:
                    log_event(name, "install", f"Erro cleanup chroot após install: {e}", level="warning")

    # ------------------------------
    # remoção
    # ------------------------------
    def remove(self, name: str, version: Optional[str] = None,
               hooks: Optional[Dict[str, List[str]]] = None,
               force: bool = False) -> bool:
        log_event(name, "remove", f"Iniciando remoção de {name} {version or ''} no root {self.root}")

        chroot_prepared = False
        try:
            if os.path.abspath(self.root) != "/":
                try:
                    prepare_chroot(self.root, copy_resolv=True, dry_run=self.dry_run)
                    chroot_prepared = True
                except ChrootError as ce:
                    log_event(name, "remove", f"Falha ao preparar chroot {self.root}: {ce}", level="warning")

            # --- 0. verificar dependências reversas ---
            resolver = DependencyResolver(self.db_path, ports_dir=self.ports_dir)
            revdeps = resolver.reverse_deps(name)
            if revdeps and not force:
                log_event(name, "remove",
                          f"Abortado: {name} ainda é dependência de {', '.join(revdeps)}",
                          level="error")
                return False

            # 1. hooks pre_remove
            if hooks:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("pre_remove", hooks=hooks)

            # 2. remover arquivos listados no DB
            conn = connect(self.db_path)
            paths = remove_package(conn, name, version)
            conn.close()

            if not self.dry_run:
                for p in paths:
                    full = os.path.join(self.root, p.lstrip("/"))
                    if os.path.exists(full):
                        try:
                            os.remove(full)
                            log_event(name, "remove", f"Removido {full}")
                        except Exception as e:
                            log_event(name, "remove", f"Erro ao remover {full}: {e}", level="error")

                # limpar diretórios vazios ascendentes
                for p in paths:
                    d = os.path.dirname(os.path.join(self.root, p.lstrip("/")))
                    while d and d != self.root and os.path.isdir(d):
                        if not os.listdir(d):
                            os.rmdir(d)
                            d = os.path.dirname(d)
                        else:
                            break

            # 3. hooks post_remove
            if hooks:
                patcher = Patcher(self.root, pkg_name=name)
                patcher.apply_stage("post_remove", hooks=hooks)

            log_event(name, "remove", f"Remoção concluída: {name} {version or ''}")
            return True
        finally:
            if chroot_prepared:
                try:
                    cleanup_chroot(self.root, force_lazy=True, dry_run=self.dry_run)
                except Exception as e:
                    log_event(name, "remove", f"Erro cleanup chroot após remove: {e}", level="warning")
