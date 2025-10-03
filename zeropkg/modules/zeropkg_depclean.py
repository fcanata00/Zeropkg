#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_depclean.py — Remove pacotes órfãos de forma segura

Funcionalidades:
- Detecta pacotes órfãos via DependencyResolver.find_orphans()
- Integra com Installer.remove() para execução real
- Suporta --dry-run, --force
- Chama hooks de pre/post remove
- Loga operações em /var/log/zeropkg/depclean.log
"""

import logging
from typing import List

from zeropkg_deps import DependencyResolver
from zeropkg_installer import Installer
from zeropkg_logger import log_event
from zeropkg_db import DBManager

logger = logging.getLogger("zeropkg.depclean")


class DepCleaner:
    def __init__(self, db_path: str, ports_dir: str, root: str,
                 dry_run: bool = False, use_fakeroot: bool = False):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = root
        self.dry_run = dry_run
        self.use_fakeroot = use_fakeroot
        self.db = DBManager(db_path)
        self.resolver = DependencyResolver(db_path, ports_dir)
        self.installer = Installer(db_path=db_path,
                                   ports_dir=ports_dir,
                                   root=root,
                                   dry_run=dry_run,
                                   use_fakeroot=use_fakeroot)

    def list_orphans(self) -> List[str]:
        """Retorna a lista de pacotes órfãos detectados"""
        orphans = self.resolver.find_orphans()
        return orphans

    def clean(self, force: bool = False) -> List[str]:
        """
        Remove pacotes órfãos de forma segura.
        Se force=True, ignora dependentes reversos.
        """
        orphans = self.list_orphans()
        removed = []

        if not orphans:
            print("Nenhum pacote órfão encontrado.")
            return []

        print("Pacotes órfãos detectados:", ", ".join(orphans))

        for pkg in orphans:
            revs = self.resolver.reverse_deps(pkg)
            if revs and not force:
                print(f"[SKIP] {pkg} ainda possui dependentes: {', '.join(revs)}")
                continue

            log_event(pkg, "depclean", f"Removendo órfão {pkg} (dry_run={self.dry_run})")

            if self.dry_run:
                print(f"[DRY-RUN] Removeria {pkg}")
                removed.append(pkg)
                continue

            try:
                self.installer.remove(pkg, force=force)
                removed.append(pkg)
                print(f"[OK] {pkg} removido")
            except Exception as e:
                logger.error(f"Falha ao remover {pkg}: {e}")
                print(f"[ERRO] {pkg}: {e}")

        return removed
