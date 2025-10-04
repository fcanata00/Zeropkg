#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZeroPKG Vulnerability Manager (zeropkg_vuln)
--------------------------------------------
Gerencia e corrige vulnerabilidades de pacotes no sistema Zeropkg.

Recursos:
 - Escaneia pacotes instalados e detecta CVEs.
 - Atualiza automaticamente a base de vulnerabilidades.
 - Aplica patches ou upgrades corretivos.
 - Integra-se com depclean para remover versões antigas.
 - Gera relatórios JSON e HTML coloridos.
 - Envia notificações sobre vulnerabilidades críticas.
"""

import os
import json
import requests
import datetime
from pathlib import Path
from packaging.version import Version, InvalidVersion
from zeropkg_logger import log_info, log_warn, log_error
from zeropkg_db import ZeroPKGDB
from zeropkg_upgrade import ZeroPKGUpgrade
from zeropkg_patcher import ZeroPKGPatcher
from zeropkg_depclean import ZeroPKGDepClean

VULN_DB_PATH = "/var/lib/zeropkg/vulndb.json"
REPORT_PATH_JSON = "/var/lib/zeropkg/vuln-report.json"
REPORT_PATH_HTML = "/var/lib/zeropkg/vuln-report.html"


class VulnDB:
    """Gerencia base de vulnerabilidades"""
    def __init__(self, path=VULN_DB_PATH):
        self.path = Path(path)
        self.data = {}
        self.last_update = None
        self.load_local()

    def load_local(self):
        if self.path.exists():
            with open(self.path, "r") as f:
                self.data = json.load(f)
            self.last_update = self.data.get("_last_update")
        else:
            self.data = {"_last_update": str(datetime.datetime.utcnow())}

    def save_local(self):
        self.data["_last_update"] = str(datetime.datetime.utcnow())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)

    def fetch_remote(self, url):
        log_info(f"Atualizando base de vulnerabilidades: {url}")
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            remote_data = resp.json()
            self.data.update(remote_data)
            self.save_local()
            log_info("Base de vulnerabilidades atualizada com sucesso.")
        except Exception as e:
            log_error(f"Falha ao atualizar vulnerabilidades: {e}")

    def get_vulns(self, pkg):
        return self.data.get(pkg, [])


class ZeroPKGVulnManager:
    """Gerencia vulnerabilidades e correções"""
    def __init__(self):
        self.db = ZeroPKGDB()
        self.vulndb = VulnDB()
        self.upgrader = ZeroPKGUpgrade()
        self.patcher = ZeroPKGPatcher()
        self.depclean = ZeroPKGDepClean()

    def scan_installed(self):
        log_info("Iniciando escaneamento de vulnerabilidades...")
        installed = self.db.get_installed_packages()
        results = []

        for pkg in installed:
            name = pkg["name"]
            version = pkg["version"]
            vulns = self.vulndb.get_vulns(name)
            if not vulns:
                continue

            for v in vulns:
                try:
                    affected_range = v["affected"]
                    if self._is_version_vulnerable(version, affected_range):
                        v["package"] = name
                        v["installed_version"] = version
                        results.append(v)
                        log_warn(f"{name} {version} vulnerável ({v['cve']})")
                except Exception:
                    continue

        self._generate_reports(results)
        self._notify(results)
        return results

    def _is_version_vulnerable(self, version, affected_range):
        try:
            v = Version(version)
        except InvalidVersion:
            return False

        for rule in affected_range.split(","):
            rule = rule.strip()
            if rule.startswith("<") and v < Version(rule[1:]): return True
            if rule.startswith("<=") and v <= Version(rule[2:]): return True
            if rule.startswith(">") and v > Version(rule[1:]): return True
            if rule.startswith(">=") and v >= Version(rule[2:]): return True
            if rule.startswith("==") and v == Version(rule[2:]): return True
        return False

    def _generate_reports(self, vulns):
        os.makedirs("/var/lib/zeropkg", exist_ok=True)
        with open(REPORT_PATH_JSON, "w") as f:
            json.dump(vulns, f, indent=2)

        with open(REPORT_PATH_HTML, "w") as f:
            f.write("<html><body><h1>ZeroPKG Vulnerability Report</h1>")
            f.write(f"<p>Gerado em {datetime.datetime.now()}</p><table border='1'>")
            f.write("<tr><th>Pacote</th><th>Versão</th><th>CVE</th><th>Severidade</th><th>Descrição</th></tr>")
            for v in vulns:
                color = {"critical":"#ff4d4d","high":"#ff944d","medium":"#ffd24d","low":"#d9ff66"}.get(v.get("severity"), "#ffffff")
                f.write(f"<tr bgcolor='{color}'><td>{v['package']}</td><td>{v['installed_version']}</td><td>{v['cve']}</td><td>{v.get('severity','N/A')}</td><td>{v.get('description','')}</td></tr>")
            f.write("</table></body></html>")

        log_info(f"Relatórios gerados em {REPORT_PATH_JSON} e {REPORT_PATH_HTML}")

    def _notify(self, vulns):
        if not vulns:
            log_info("✅ Nenhuma vulnerabilidade encontrada.")
            return
        critical = len([v for v in vulns if v.get("severity") == "critical"])
        total = len(vulns)
        log_warn(f"⚠️ {total} vulnerabilidades detectadas ({critical} críticas).")

    def apply_fixes(self, dry_run=False):
        vulns = self.scan_installed()
        if not vulns:
            log_info("Sistema seguro, nada a corrigir.")
            return

        for v in vulns:
            pkg = v["package"]
            fix = v.get("fix")
            if not fix:
                log_warn(f"Nenhuma correção disponível para {pkg}.")
                continue

            if dry_run:
                log_info(f"[dry-run] Corrigiria {pkg} → {fix}")
                continue

            log_info(f"Aplicando correção em {pkg} ({fix})...")
            if fix.startswith("patch:"):
                patch_url = fix.split("patch:")[1]
                self.patcher.apply_patch(pkg, patch_url)
            elif fix.startswith("upgrade:"):
                self.upgrader.upgrade(pkg)
            else:
                log_warn(f"Correção desconhecida: {fix}")

        self.depclean.auto_clean()
        log_info("Correções aplicadas e limpeza concluída.")

    def update_vulndb(self, url=None):
        default_url = "https://zeropkg.org/vulnfeed.json"
        self.vulndb.fetch_remote(url or default_url)


# -------------------------------
# CLI Wrapper
# -------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ZeroPKG Vulnerability Manager")
    parser.add_argument("--fetch", nargs="?", const="https://zeropkg.org/vulnfeed.json", help="Atualiza a base de vulnerabilidades")
    parser.add_argument("--scan", action="store_true", help="Escaneia pacotes instalados")
    parser.add_argument("--apply", action="store_true", help="Aplica correções automáticas")
    parser.add_argument("--dry-run", action="store_true", help="Executa simulação sem aplicar")
    args = parser.parse_args()

    vm = ZeroPKGVulnManager()

    if args.fetch:
        vm.update_vulndb(args.fetch)
    elif args.scan:
        vm.scan_installed()
    elif args.apply:
        vm.apply_fixes(dry_run=args.dry_run)
    else:
        parser.print_help()
