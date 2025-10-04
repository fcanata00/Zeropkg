#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_installer.py — Installer completo e integrado (versão final)
Compatível com o builder completo (fakeroot, chroot, cache, DB, hooks).
"""

import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, Dict, List

from zeropkg_logger import log_event, get_logger
from zeropkg_config import load_config
from zeropkg_db import connect, record_install, remove_package, get_package_files
from zeropkg_chroot import prepare_chroot, cleanup_chroot, ChrootError
from zeropkg_deps import DependencyResolver
from zeropkg_patcher import Patcher

logger = get_logger("installer")


class InstallError(Exception):
    pass


class Installer:
    def __init__(self, config_path: str = "/etc/zeropkg/config.toml"):
        """Lê as configurações padrão do Zeropkg"""
        cfg = load_config(config_path)
        paths = cfg["paths"]
        self.db_path = paths.get("db_path", "/var/lib/zeropkg/installed.sqlite3")
        self.ports_dir = paths.get("ports_dir", "/usr/ports")
        self.packages_dir = paths.get("packages_dir", "/var/zeropkg/packages")
        self.root = paths.get("root", "/")
        self.use_fakeroot = bool(cfg["options"].get("fakeroot", True))
        self.dry_run = False

    # -------------------------
    # Auxiliares internos
    # -------------------------
    def _copy_tree(self, src_dir: str, dest_root: str):
        """Copia diretórios preservando permissões e timestamps"""
        for root_dir, dirs, files in os.walk(src_dir):
            rel = os.path.relpath(root_dir, src_dir)
            dest_dir = os.path.join(dest_root, rel if rel != "." else "")
            os.makedirs(dest_dir, exist_ok=True)
            for fname in files:
                src = os.path.join(root_dir, fname)
                dst = os.path.join(dest_dir, fname)
                if self.use_fakeroot:
                    cmd = f"fakeroot cp -a \"{src}\" \"{dst}\""
                    os.system(cmd)
                else:
                    shutil.copy2(src, dst)
                log_event(fname, "install", f"Copied {dst}")

    def _apply_hooks(self, stage: str, meta: Optional[Dict]):
        if not meta:
            return
        hooks = meta.get("hooks", {})
        cmds = hooks.get(stage)
        if not cmds:
            return
        if isinstance(cmds, str):
            cmds = [cmds]
        for cmd in cmds:
            log_event("installer", stage, f"Running hook: {cmd}")
            os.system(cmd)

    # -------------------------
    # Instalação
    # -------------------------
    def install(self, name: str, args: Dict, meta: Optional[Dict] = None):
        """
        Instala pacote:
        - args: {"pkg_file": caminho, "root": "/mnt/lfs" (opcional)}
        - meta: dicionário da receita TOML
        """
        pkg_file = args.get("pkg_file")
        root = args.get("root", self.root)
        dry_run = args.get("dry_run", False)
        chroot_needed = bool(meta.get("build", {}).get("chroot", False)) if meta else False

        log_event(name, "install.start", f"Iniciando instalação em {root}")

        # preparar chroot se necessário
        chroot_prepared = False
        if chroot_needed and root != "/":
            try:
                prepare_chroot(root, copy_resolv=True, dry_run=dry_run)
                chroot_prepared = True
            except ChrootError as e:
                raise InstallError(f"Falha ao preparar chroot: {e}")

        staging_dir = tempfile.mkdtemp(prefix=f"zeropkg-install-{name}-")
        try:
            # extrair pacote
            if pkg_file and os.path.exists(pkg_file):
                if dry_run:
                    log_event(name, "install", f"[dry-run] Extrairia {pkg_file}")
                else:
                    with tarfile.open(pkg_file, "r:*") as tf:
                        tf.extractall(staging_dir)
            else:
                raise InstallError(f"Pacote {pkg_file} não encontrado.")

            # hooks pré-install
            self._apply_hooks("pre_install", meta)

            # copiar arquivos
            if not dry_run:
                self._copy_tree(staging_dir, root)
            else:
                log_event(name, "install", f"[dry-run] Copiaria {staging_dir} → {root}")

            # registrar instalação
            if not dry_run and meta:
                conn = connect(self.db_path)
                record_install(conn, meta, pkg_file)
                conn.close()

            # hooks pós-install
            self._apply_hooks("post_install", meta)

            log_event(name, "install.finish", f"{name} instalado com sucesso.")
            return True

        except Exception as e:
            raise InstallError(f"Erro na instalação: {e}")
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
            if chroot_prepared:
                cleanup_chroot(root, force_lazy=True, dry_run=dry_run)

    # -------------------------
    # Remoção
    # -------------------------
    def remove(self, name: str, version: Optional[str] = None, meta: Optional[Dict] = None, force: bool = False):
        """Remove pacote instalado"""
        pkg_display = f"{name}-{version}" if version else name
        log_event(pkg_display, "remove.start", f"Removendo pacote {pkg_display}")

        resolver = DependencyResolver(self.db_path, ports_dir=self.ports_dir)
        revdeps = resolver.reverse_deps(name)
        if revdeps and not force:
            log_event(pkg_display, "remove", f"Abortado: dependência reversa detectada {revdeps}", level="error")
            return False

        # hooks pré-remove
        self._apply_hooks("pre_remove", meta)

        conn = connect(self.db_path)
        paths = remove_package(conn, name, version)
        conn.close()

        for relpath in paths:
            p = os.path.join(self.root, relpath.lstrip("/"))
            if os.path.exists(p):
                try:
                    if os.path.isfile(p) or os.path.islink(p):
                        os.unlink(p)
                    elif os.path.isdir(p):
                        shutil.rmtree(p)
                    log_event(pkg_display, "remove", f"Removido {p}")
                except Exception as e:
                    log_event(pkg_display, "remove", f"Falha ao remover {p}: {e}", level="warning")

        self._apply_hooks("post_remove", meta)
        log_event(pkg_display, "remove.finish", f"{pkg_display} removido.")
        return True
