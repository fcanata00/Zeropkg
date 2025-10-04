#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_remover.py — Módulo de remoção do Zeropkg (versão integrada final)
Com integração com Installer, DB, Hooks e Chroot.
"""

import os
import shutil
import argparse
from typing import Optional, List, Dict, Any

from zeropkg_toml import parse_toml
from zeropkg_installer import Installer
from zeropkg_deps import DependencyResolver
from zeropkg_db import connect, get_package_files, get_revdeps, remove_package
from zeropkg_logger import get_logger, log_event
from zeropkg_chroot import prepare_chroot, cleanup_chroot
from zeropkg_patcher import Patcher, HookError

logger = get_logger("remover")


class RemoveError(Exception):
    pass


class Remover:
    def __init__(
        self,
        db_path: str = "/var/lib/zeropkg/installed.sqlite3",
        ports_dir: str = "/usr/ports",
        root: str = "/",
        dry_run: bool = False,
        use_fakeroot: bool = True,
    ):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = os.path.abspath(root or "/")
        self.dry_run = dry_run
        self.use_fakeroot = use_fakeroot
        self.resolver = DependencyResolver(db_path, ports_dir)
        self.installer = Installer(config_path="/etc/zeropkg/config.toml")

    # ------------------------------
    # Utilidades internas
    # ------------------------------
    def _find_metafile(self, name: str) -> Optional[str]:
        """Procura por receita TOML do pacote"""
        for root, _, files in os.walk(self.ports_dir):
            for f in files:
                if f.endswith(".toml") and f.startswith(name + "-"):
                    return os.path.join(root, f)
        return None

    def _run_hooks(self, name: str, stage: str, meta: Optional[Dict]):
        if not meta:
            return
        hooks = meta.get("hooks", {}).get(stage)
        if not hooks:
            return
        patcher = Patcher(self.ports_dir)
        for cmd in hooks if isinstance(hooks, list) else [hooks]:
            try:
                patcher.run_hook(cmd, stage)
                log_event(name, stage, f"Hook executado: {cmd}")
            except HookError as e:
                log_event(name, stage, f"Falha no hook: {e}", level="error")
                raise RemoveError(f"Hook {cmd} falhou: {e}")

    # ------------------------------
    # Função principal
    # ------------------------------
    def remove(self, name: str, version: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
        """Remove pacote com hooks, chroot e atualização do DB."""
        log_event(name, "remove", f"Iniciando remoção (dry_run={self.dry_run})")

        revdeps = self.resolver.reverse_deps(name)
        if revdeps and not force:
            msg = f"Remoção bloqueada: dependentes {', '.join(revdeps)}"
            log_event(name, "remove", msg, level="warning")
            return {"status": "blocked", "revdeps": revdeps, "removed": False, "message": msg}

        # carrega receita para hooks
        meta = {}
        metafile = self._find_metafile(name)
        if metafile:
            try:
                meta = parse_toml(metafile)
            except Exception as e:
                log_event(name, "remove", f"Erro lendo {metafile}: {e}", level="warning")

        self._run_hooks(name, "pre_remove", meta)

        # buscar arquivos do pacote
        conn = connect(self.db_path)
        files = get_package_files(conn, name)
        conn.close()

        # chroot seguro
        chroot_active = False
        try:
            prepare_chroot(self.root, copy_resolv=True)
            chroot_active = True

            for path in files:
                fpath = os.path.join(self.root, path.lstrip("/"))
                if not os.path.exists(fpath):
                    continue
                if self.dry_run:
                    log_event(name, "remove", f"[dry-run] Removeria {fpath}")
                    continue
                try:
                    if os.path.isdir(fpath):
                        shutil.rmtree(fpath)
                    else:
                        os.unlink(fpath)
                    log_event(name, "remove", f"Removido: {fpath}")
                except Exception as e:
                    log_event(name, "remove", f"Falha ao remover {fpath}: {e}", level="warning")

            if not self.dry_run:
                conn = connect(self.db_path)
                remove_package(conn, name, version)
                conn.close()

        finally:
            if chroot_active:
                cleanup_chroot(self.root, force_lazy=True)

        self._run_hooks(name, "post_remove", meta)
        log_event(name, "remove.finish", f"{name} removido com sucesso.")
        return {"status": "ok", "removed": True}

    # ------------------------------
    # Remover vários pacotes
    # ------------------------------
    def remove_multiple(self, packages: List[str], force: bool = False):
        summary = {"removed": [], "failed": [], "blocked": []}
        for pkg in packages:
            try:
                res = self.remove(pkg, force=force)
                if res["status"] == "ok":
                    summary["removed"].append(pkg)
                elif res["status"] == "blocked":
                    summary["blocked"].append(pkg)
                else:
                    summary["failed"].append(pkg)
            except Exception as e:
                summary["failed"].append(pkg)
                log_event(pkg, "remove", f"Erro: {e}", level="error")
        return summary


# ------------------------------
# CLI utilitário
# ------------------------------
def _build_argparser():
    p = argparse.ArgumentParser(prog="zeropkg-remove", description="Remove pacotes do Zeropkg com segurança")
    p.add_argument("package", nargs="+", help="Pacote(s) a remover")
    p.add_argument("-v", "--version", help="Versão específica", default=None)
    p.add_argument("--root", default="/", help="Prefixo de instalação (ex: /mnt/lfs)")
    p.add_argument("--ports-dir", default="/usr/ports", help="Diretório de recipes")
    p.add_argument("--db-path", default="/var/lib/zeropkg/installed.sqlite3", help="Banco de dados")
    p.add_argument("--dry-run", action="store_true", help="Simula a remoção sem executar")
    p.add_argument("-f", "--force", action="store_true", help="Força remoção ignorando dependentes")
    return p


def main(argv: Optional[List[str]] = None):
    args = _build_argparser().parse_args(argv)
    remover = Remover(
        db_path=args.db_path,
        ports_dir=args.ports_dir,
        root=args.root,
        dry_run=args.dry_run,
    )
    summary = remover.remove_multiple(args.package, force=args.force)
    print("Resumo:")
    for k, v in summary.items():
        print(f"  {k}: {len(v)} {' '.join(v)}")


if __name__ == "__main__":
    main()
