#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_installer.py — Instalador completo e integrado com builder/chroot
Compatível com fakeroot, chroot, hooks, e banco de dados.
"""

import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List

from zeropkg_logger import log_event, get_logger
from zeropkg_config import load_config
from zeropkg_db import connect, record_install, remove_package, get_package_files
from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot, ChrootError
from zeropkg_deps import DependencyResolver

logger = get_logger("installer")

class InstallError(Exception):
    pass

class Installer:
    def __init__(self, config_path: str = "/etc/zeropkg/config.toml"):
        cfg = load_config(config_path)
        paths = cfg.get("paths", {})
        opts = cfg.get("options", {})

        self.db_path = paths.get("db_path", "/var/lib/zeropkg/installed.sqlite3")
        self.ports_dir = paths.get("ports_dir", "/usr/ports")
        self.packages_dir = paths.get("packages_dir", "/var/zeropkg/packages")
        self.root = paths.get("root", "/")
        self.use_fakeroot = bool(opts.get("fakeroot", True))
        self.dry_run = False

    # -----------------------
    # Utilidades
    # -----------------------
    def _copy_tree(self, src_dir: str, dest_root: str, use_fakeroot: bool = False):
        """Copia diretórios preservando permissões"""
        for root_dir, _, files in os.walk(src_dir):
            rel = os.path.relpath(root_dir, src_dir)
            dest_dir = os.path.join(dest_root, rel if rel != "." else "")
            os.makedirs(dest_dir, exist_ok=True)
            for f in files:
                src = os.path.join(root_dir, f)
                dst = os.path.join(dest_dir, f)
                if use_fakeroot:
                    os.system(f"fakeroot cp -a \"{src}\" \"{dst}\"")
                else:
                    shutil.copy2(src, dst)
                log_event("installer", "copy", f"→ {dst}")

    def _apply_hooks(self, stage: str, meta: Optional[Dict[str, Any]], chroot_root: Optional[str] = None):
        """Executa hooks de pre/post install/remove"""
        if not meta:
            return
        hooks = meta.get("hooks", {})
        cmds = hooks.get(stage)
        if not cmds:
            return
        if isinstance(cmds, str):
            cmds = [cmds]

        for cmd in cmds:
            if chroot_root and run_in_chroot:
                run_in_chroot(chroot_root, cmd, use_shell=True)
            else:
                os.system(cmd)
            log_event("installer", stage, f"Executado hook: {cmd}")

    # -----------------------
    # Instalação
    # -----------------------
    def install(self, name: str, args: Dict[str, Any], meta: Optional[Dict[str, Any]] = None):
        pkg_file = args.get("pkg_file")
        root = args.get("root", self.root)
        dry_run = args.get("dry_run", False)
        use_chroot = bool(meta and meta.get("build", {}).get("chroot", False))
        use_fakeroot = bool(meta and meta.get("build", {}).get("fakeroot", self.use_fakeroot))

        log_event(name, "install.start", f"Iniciando instalação em {root}")

        if not pkg_file or not os.path.exists(pkg_file):
            raise InstallError(f"Pacote {pkg_file} não encontrado")

        chroot_prepared = False
        if use_chroot and root != "/":
            try:
                prepare_chroot(root, copy_resolv=True, dry_run=dry_run)
                chroot_prepared = True
            except ChrootError as e:
                raise InstallError(f"Falha ao preparar chroot: {e}")

        staging = tempfile.mkdtemp(prefix=f"zeropkg-install-{name}-")
        try:
            # Extrair pacote
            if dry_run:
                log_event(name, "extract", f"[dry-run] Extrairia {pkg_file}")
            else:
                with tarfile.open(pkg_file, "r:*") as tf:
                    tf.extractall(staging)

            # Hook pré-instalação
            self._apply_hooks("pre_install", meta, chroot_root=root if use_chroot else None)

            # Copiar arquivos
            if dry_run:
                log_event(name, "install", f"[dry-run] Copiaria {staging} → {root}")
            else:
                self._copy_tree(staging, root, use_fakeroot)

            # Registrar no DB (se existir)
            if not dry_run:
                try:
                    conn = connect(self.db_path)
                    record_install(conn, meta, pkg_file)
                    conn.close()
                    log_event(name, "db", "Registro no banco concluído")
                except Exception as e:
                    log_event(name, "db", f"Falha ao registrar no DB: {e}", level="warning")

            # Hook pós-instalação
            self._apply_hooks("post_install", meta, chroot_root=root if use_chroot else None)

            log_event(name, "install.finish", f"{name} instalado com sucesso")
            return True

        except Exception as e:
            raise InstallError(f"Erro na instalação: {e}")
        finally:
            shutil.rmtree(staging, ignore_errors=True)
            if chroot_prepared:
                cleanup_chroot(root, force_lazy=True, dry_run=dry_run)

    # -----------------------
    # Remoção
    # -----------------------
    def remove(self, name: str, version: Optional[str] = None, meta: Optional[Dict[str, Any]] = None, force: bool = False):
        pkg_display = f"{name}-{version}" if version else name
        log_event(pkg_display, "remove.start", f"Removendo {pkg_display}")

        resolver = DependencyResolver(self.db_path, ports_dir=self.ports_dir)
        revdeps = resolver.reverse_deps(name)
        if revdeps and not force:
            log_event(pkg_display, "remove", f"Abortado: dependência reversa {revdeps}", level="error")
            return False

        self._apply_hooks("pre_remove", meta)
        try:
            conn = connect(self.db_path)
            paths = remove_package(conn, name, version)
            conn.close()
        except Exception:
            paths = []

        for rel in paths:
            target = os.path.join(self.root, rel.lstrip("/"))
            if os.path.exists(target):
                try:
                    if os.path.isfile(target) or os.path.islink(target):
                        os.unlink(target)
                    elif os.path.isdir(target):
                        shutil.rmtree(target)
                    log_event(pkg_display, "remove", f"Removido {target}")
                except Exception as e:
                    log_event(pkg_display, "remove", f"Falha ao remover {target}: {e}", level="warning")

        self._apply_hooks("post_remove", meta)
        log_event(pkg_display, "remove.finish", f"{pkg_display} removido")
        return True
