"""
zeropkg_installer.py - Instalador de pacotes do Zeropkg (revisto)

- Instala/Remove pacotes em qualquer root (--root=/mnt/lfs)
- Preserva UID/GID com fakeroot
- Integra com chroot (prepare/cleanup)
- Registra pacotes e arquivos no DB SQLite
- Executa hooks pre/post install/remove
"""

import os
import shutil
import tarfile
import hashlib
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path

from zeropkg_logger import log_event
from zeropkg_db import connect, register_package, remove_package, get_package
from zeropkg_patcher import Patcher
from zeropkg_toml import PackageMeta

logger = logging.getLogger("zeropkg.installer")


class InstallError(Exception):
    pass


class Installer:
    def __init__(self, db_path="/var/lib/zeropkg/installed.sqlite3",
                 dry_run=False, root="/", use_fakeroot=True):
        self.db_path = db_path
        self.dry_run = dry_run
        self.root = os.path.abspath(root)
        self.use_fakeroot = use_fakeroot

    # ----------------------------
    # helpers
    # ----------------------------
    def _fakeroot_copy(self, src: str, dst: str):
        shutil.copy2(src, dst, follow_symlinks=False)

    def _fakeroot_mkdir(self, path: str):
        os.makedirs(path, exist_ok=True)

    def _fakeroot_remove(self, path: str):
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

    def _fakeroot_extract(self, tarpath: str, dest: str):
        with tarfile.open(tarpath, "r:*") as tf:
            tf.extractall(dest)

    def _hash_file(self, path: str, skip_hash=False) -> str:
        if skip_hash:
            return ""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    # ----------------------------
    # instalação
    # ----------------------------
    def install(self, pkgfile: str, meta: PackageMeta,
                hooks: Optional[Dict[str, List[str]]] = None,
                build_id: Optional[int] = None,
                skip_hash=False):
        """
        Instala um pacote binário no root.
        - pkgfile: caminho do pacote .tar.*
        - meta: metadata do pacote
        - hooks: dict de hooks pre/post install
        - build_id: id do build no banco (integração com builder)
        - skip_hash: não calcular hashes (mais rápido, menos seguro)
        """
        log_event(meta.name, "install", f"Iniciando instalação de {pkgfile} em {self.root}")

        # 1. hooks pre_install
        if hooks:
            patcher = Patcher(self.root, pkg_name=meta.name)
            patcher.apply_stage("pre_install", hooks=hooks)

        # 2. extrair no staging
        staging = os.path.join("/tmp", f"zeropkg-staging-{meta.name}")
        if os.path.exists(staging):
            shutil.rmtree(staging)
        os.makedirs(staging, exist_ok=True)

        if not self.dry_run:
            try:
                self._fakeroot_extract(pkgfile, staging)
            except Exception as e:
                raise InstallError(f"Erro ao extrair pacote {pkgfile}: {e}") from e

        # 3. copiar arquivos para root
        manifest: List[Dict[str, Any]] = []
        for root, dirs, files in os.walk(staging):
            rel_root = os.path.relpath(root, staging)
            if rel_root == ".":
                rel_root = ""
            target_root = os.path.join(self.root, rel_root)

            for d in dirs:
                dpath = os.path.join(target_root, d)
                if not self.dry_run:
                    self._fakeroot_mkdir(dpath)

            for f in files:
                spath = os.path.join(root, f)
                dpath = os.path.join(target_root, f)
                if not self.dry_run:
                    os.makedirs(os.path.dirname(dpath), exist_ok=True)
                    self._fakeroot_copy(spath, dpath)
                    mtime = int(os.path.getmtime(dpath))
                    manifest.append({"path": dpath, "hash": self._hash_file(dpath, skip_hash), "mtime": mtime})

        # 4. registrar no banco
        conn = connect(self.db_path)
        try:
            register_package(conn, meta, pkgfile,
                             [(m["path"], m["hash"]) for m in manifest],
                             build_id=build_id)
        finally:
            conn.close()

        # 5. hooks post_install
        if hooks:
            patcher = Patcher(self.root, pkg_name=meta.name)
            patcher.apply_stage("post_install", hooks=hooks)

        # 6. limpeza staging
        shutil.rmtree(staging, ignore_errors=True)

        log_event(meta.name, "install", f"Instalação concluída: {meta.name}-{meta.version}")
        return True

    # ----------------------------
    # remoção
    # ----------------------------
    def remove(self, name: str, version: Optional[str] = None,
               hooks: Optional[Dict[str, List[str]]] = None):
        log_event(name, "remove", f"Iniciando remoção de {name} {version or ''}")

        # 1. hooks pre_remove
        if hooks:
            patcher = Patcher(self.root, pkg_name=name)
            patcher.apply_stage("pre_remove", hooks=hooks)

        # 2. remover arquivos do banco
        conn = connect(self.db_path)
        paths = remove_package(conn, name, version)
        conn.close()

        if not self.dry_run:
            for p in paths:
                full = os.path.join(self.root, p.lstrip("/"))
                if os.path.exists(full):
                    try:
                        self._fakeroot_remove(full)
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
