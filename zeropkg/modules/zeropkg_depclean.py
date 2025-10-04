#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZeroPKG - DepClean Module
Gerencia a limpeza de dependências órfãs e pacotes obsoletos.
Agora com suporte a grupos, exclusões, dry-run detalhado, integração com upgrade/remover e scanner de vulnerabilidades.
"""

import os
import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from zeropkg_logger import log_info, log_warn, log_error
from zeropkg_db import ZeroPKGDB
from zeropkg_deps import DependencyGraph
from zeropkg_remover import ZeroPKGRemover
from zeropkg_vulnscan import ZeroPKGVulnScan  # módulo futuro de segurança

DEP_PROTECTED = {"bash", "coreutils", "gcc", "glibc", "linux-headers"}

class ZeroPKGDepClean:
    def __init__(self, db_path="/var/lib/zeropkg/db.sqlite", backup_dir="/var/backups/zeropkg"):
        self.db = ZeroPKGDB(db_path)
        self.remover = ZeroPKGRemover()
        self.vulnscan = ZeroPKGVulnScan()
        self.graph = DependencyGraph()
        self.backup_dir = backup_dir
        os.makedirs(backup_dir, exist_ok=True)
        self.lock = threading.Lock()

    # -------------------------------
    # Verifica dependências órfãs
    # -------------------------------
    def find_orphans(self):
        log_info("Analisando dependências órfãs...")
        installed = self.db.get_installed_packages()
        orphans = []

        for pkg in installed:
            if pkg["name"] in DEP_PROTECTED:
                continue
            deps = self.graph.get_reverse_dependencies(pkg["name"])
            if not deps:
                orphans.append(pkg["name"])

        return sorted(set(orphans))

    # -------------------------------
    # Backup incremental antes da limpeza
    # -------------------------------
    def backup_package_files(self, pkg_name):
        files = self.db.get_package_files(pkg_name)
        if not files:
            return
        backup_path = os.path.join(self.backup_dir, f"{pkg_name}.bak")
        os.makedirs(backup_path, exist_ok=True)

        for f in files:
            if os.path.exists(f):
                dest = os.path.join(backup_path, os.path.basename(f))
                try:
                    shutil.copy2(f, dest)
                except Exception as e:
                    log_warn(f"Falha ao copiar {f}: {e}")

    # -------------------------------
    # Execução paralela
    # -------------------------------
    def clean_parallel(self, packages):
        with ThreadPoolExecutor(max_workers=4) as executor:
            for pkg in packages:
                executor.submit(self._remove_package_safe, pkg)

    # -------------------------------
    # Remoção segura
    # -------------------------------
    def _remove_package_safe(self, pkg_name):
        with self.lock:
            log_info(f"Removendo {pkg_name} com segurança...")
            self.backup_package_files(pkg_name)
            try:
                self.remover.remove(pkg_name)
            except Exception as e:
                log_error(f"Erro ao remover {pkg_name}: {e}")

    # -------------------------------
    # Modo Dry-run
    # -------------------------------
    def dry_run(self, packages):
        log_info("Simulação de remoção (dry-run):")
        for pkg in packages:
            print(f"   - {pkg}")
            files = self.db.get_package_files(pkg)
            if files:
                for f in files[:5]:
                    print(f"      • {f}")
                if len(files) > 5:
                    print(f"      ... +{len(files)-5} arquivos restantes")

    # -------------------------------
    # Relatório final
    # -------------------------------
    def generate_report(self, removed, skipped):
        report = {
            "removed": removed,
            "skipped": skipped,
            "total_removed": len(removed),
            "total_skipped": len(skipped),
        }
        path = "/var/lib/zeropkg/depclean-report.json"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(report, f, indent=4)
        log_info(f"Relatório salvo em {path}")

    # -------------------------------
    # Modo automático (pós-upgrade)
    # -------------------------------
    def auto_clean(self):
        log_info("Executando limpeza automática pós-upgrade...")
        orphans = self.find_orphans()
        self.clean_parallel(orphans)
        self.generate_report(orphans, [])

    # -------------------------------
    # Integração com vulnerabilidades
    # -------------------------------
    def clean_vulnerable(self):
        vuln_list = self.vulnscan.scan_all()
        if not vuln_list:
            log_info("Nenhum pacote vulnerável encontrado.")
            return
        log_warn(f"Pacotes vulneráveis encontrados: {', '.join(vuln_list)}")
        self.clean_parallel(vuln_list)
        self.generate_report(vuln_list, [])

    # -------------------------------
    # Limpeza seletiva
    # -------------------------------
    def clean_filtered(self, only=None, exclude=None, dry=False, parallel=False):
        orphans = self.find_orphans()
        if only:
            orphans = [o for o in orphans if o in only]
        if exclude:
            orphans = [o for o in orphans if o not in exclude]

        if dry:
            self.dry_run(orphans)
            return

        if parallel:
            self.clean_parallel(orphans)
        else:
            for pkg in orphans:
                self._remove_package_safe(pkg)

        self.generate_report(orphans, exclude or [])

# -------------------------------
# CLI Wrapper
# -------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ZeroPKG DepClean Tool")
    parser.add_argument("--auto", action="store_true", help="Executa após upgrade")
    parser.add_argument("--dry-run", action="store_true", help="Simula a limpeza")
    parser.add_argument("--parallel", action="store_true", help="Usa múltiplas threads")
    parser.add_argument("--only", nargs="+", help="Limpar apenas estes pacotes")
    parser.add_argument("--exclude", nargs="+", help="Ignorar estes pacotes")
    parser.add_argument("--vuln", action="store_true", help="Remove pacotes vulneráveis")
    args = parser.parse_args()

    cleaner = ZeroPKGDepClean()

    if args.vuln:
        cleaner.clean_vulnerable()
    elif args.auto:
        cleaner.auto_clean()
    else:
        cleaner.clean_filtered(
            only=args.only,
            exclude=args.exclude,
            dry=args.dry_run,
            parallel=args.parallel
        )
