#!/usr/bin/env python3
# zeropkg_remover.py — Módulo de remoção do Zeropkg (com hooks e rollback)
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import shutil
import argparse
import logging
from typing import Optional, List, Dict, Any

from zeropkg_toml import parse_toml, ValidationError
from zeropkg_installer import Installer
from zeropkg_deps import DependencyResolver
from zeropkg_db import DBManager
from zeropkg_logger import get_logger, log_event
from zeropkg_chroot import prepare_chroot, cleanup_chroot
from zeropkg_patcher import HookError, Patcher

LOG = get_logger(stage="remove")


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
        fatal_hooks: bool = True,
    ):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = os.path.abspath(root or "/")
        self.dry_run = dry_run
        self.use_fakeroot = use_fakeroot
        self.fatal_hooks = fatal_hooks
        self.db = DBManager(db_path)
        self.resolver = DependencyResolver(db_path, ports_dir)
        self.installer = Installer(
            db_path=db_path,
            ports_dir=ports_dir,
            root=self.root,
            dry_run=dry_run,
            use_fakeroot=use_fakeroot,
        )

    # --------------------------------------------
    # Utilidades
    # --------------------------------------------
    def _find_metafile(self, name: str) -> Optional[str]:
        """Procura por metafile do pacote."""
        for root, _, files in os.walk(self.ports_dir):
            for f in files:
                if f.endswith(".toml") and f.startswith(name + "-"):
                    return os.path.join(root, f)
        candidate = os.path.join(self.ports_dir, name, f"{name}.toml")
        return candidate if os.path.exists(candidate) else None

    def _run_hook(self, pkg: str, stage: str, hooks: Dict[str, List[str]], workdir: str):
        """Executa hooks pre_remove ou post_remove."""
        if not hooks or stage not in hooks:
            return
        patcher = Patcher(workdir=workdir, ports_dir=self.ports_dir, pkg_name=pkg)
        for script in hooks[stage]:
            try:
                patcher.run_hook(script, stage=stage)
            except HookError as e:
                log_event(pkg, stage, f"Hook falhou: {e}", level="error")
                if self.fatal_hooks:
                    raise RemoveError(f"Hook crítico falhou: {script}")

    # --------------------------------------------
    # Função principal
    # --------------------------------------------
    def remove(self, name: str, version: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
        """Remove pacote com segurança e hooks integrados."""
        log_event(name, "remove", f"Solicitada remoção ({'dry-run' if self.dry_run else 'real'})")
        pkg_info = self.db.get_package(name)
        if not pkg_info:
            msg = f"Pacote {name} não encontrado no DB"
            log_event(name, "remove", msg, level="warning")
            return {"status": "not_found", "message": msg}

        revdeps = self.db.find_revdeps(name)
        if revdeps and not force:
            msg = f"Remoção bloqueada: dependentes -> {', '.join(revdeps)}"
            log_event(name, "remove", msg, level="warning")
            return {"status": "blocked", "revdeps": revdeps, "removed": False, "message": msg}

        metafile = self._find_metafile(name)
        hooks = {}
        if metafile:
            try:
                meta = parse_toml(metafile)
                hooks = getattr(meta, "hooks", {}) or {}
            except Exception as e:
                log_event(name, "remove", f"Erro ao ler metafile: {e}", level="warning")

        # Hooks pré-remover
        self._run_hook(name, "pre_remove", hooks, self.root)

        if self.dry_run:
            files = pkg_info.get("files", [])
            log_event(name, "remove", f"[dry-run] Removeria {len(files)} arquivos de {name}")
            return {"status": "ok", "removed": False, "message": f"[dry-run] {len(files)} arquivos seriam removidos"}

        # Executar em chroot seguro
        try:
            with prepare_chroot(self.root):
                ok = self.installer.remove(name, version, hooks=hooks, force=force)
                if ok:
                    log_event(name, "remove", f"Remoção completa: {name}")
                else:
                    raise RemoveError(f"Installer.remove retornou erro para {name}")
        except Exception as e:
            log_event(name, "remove", f"Erro de remoção em chroot: {e}", level="error")
            raise

        # Hooks pós-remover
        self._run_hook(name, "post_remove", hooks, self.root)

        self.db.log_event(name, "remove", "Remoção concluída")
        return {"status": "ok", "removed": True, "message": f"Pacote {name} removido"}

    # --------------------------------------------
    # Múltiplos pacotes (depclean)
    # --------------------------------------------
    def remove_multiple(self, packages: List[str], force: bool = False) -> Dict[str, List[str]]:
        summary = {"removed": [], "failed": [], "blocked": []}
        for pkg in packages:
            try:
                res = self.remove(pkg, force=force)
                if res.get("status") == "ok":
                    summary["removed"].append(pkg)
                elif res.get("status") == "blocked":
                    summary["blocked"].append(pkg)
                else:
                    summary["failed"].append(pkg)
            except Exception as e:
                summary["failed"].append(pkg)
                log_event(pkg, "remove", f"Erro: {e}", level="error")
        return summary


# --------------------------------------------
# CLI utilitário
# --------------------------------------------
def _build_argparser():
    p = argparse.ArgumentParser(prog="zeropkg-remove", description="Remove pacotes com hooks e rollback")
    p.add_argument("package", nargs="+", help="Pacote(s) a remover")
    p.add_argument("-v", "--version", help="Versão específica", default=None)
    p.add_argument("--root", default="/", help="Prefixo (ex: /mnt/lfs)")
    p.add_argument("--ports-dir", default="/usr/ports", help="Diretório de recipes")
    p.add_argument("--db-path", default="/var/lib/zeropkg/installed.sqlite3", help="Banco de dados")
    p.add_argument("--dry-run", action="store_true", help="Simula sem remover")
    p.add_argument("-f", "--force", action="store_true", help="Ignora dependentes")
    p.add_argument("--no-fakeroot", action="store_true", help="Não usar fakeroot")
    return p


def main(argv: Optional[List[str]] = None):
    args = _build_argparser().parse_args(argv)
    remover = Remover(
        db_path=args.db_path,
        ports_dir=args.ports_dir,
        root=args.root,
        dry_run=args.dry_run,
        use_fakeroot=not args.no_fakeroot,
    )
    result = remover.remove_multiple(args.package, force=args.force)
    print("Resumo:")
    for key, items in result.items():
        print(f"  {key}: {len(items)} {' '.join(items)}")


if __name__ == "__main__":
    main()
