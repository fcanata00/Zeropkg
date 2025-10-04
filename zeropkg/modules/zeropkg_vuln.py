#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_vuln.py — Vulnerability scanner and fixer for Zeropkg-managed packages.

Principais capacidades:
 - Carregar um banco de vulnerabilidades local (JSON) com formato simples.
 - Atualizar/baixar um feed remoto (URL configurável) para manter a DB de vulnerabilidades.
 - Escanear pacotes instalados (via zeropkg_db) e detectar correspondências por nome+versão.
 - Produzir relatórios (por pacote, por severidade) e arquivo JSON de resultado.
 - Sugerir correções: upgrade (integra com zeropkg_upgrade) ou aplicar patch (integra com zeropkg_patcher).
 - Modo interativo e modo automático (`auto_apply=True`).
 - Suporta --dry-run; tudo logado via zeropkg_logger se disponível.

Formato esperado do arquivo local/remote (simplificado):
[
  {
    "id": "CVE-2025-XXXXX",
    "package": "openssl",
    "affected_versions": ["<1.1.1k", ">=1.1.1l,<1.1.1n"],  # suportamos comparações simples
    "severity": "critical",
    "description": "Buffer overflow on ...",
    "references": ["https://cve.mitre.org/..."],
    "fix_version": "1.1.1n",
    "patch_url": "https://example.org/patches/openssl-fix.patch"
  },
  ...
]

Observação: a heurística de version range é intencionalmente simples (suporta operadores <, <=, >, >=, == e intervalos coma-separados).
"""

from __future__ import annotations
import os
import sys
import json
import shutil
import time
import subprocess
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

# Try loading helpers from your zeropkg project
try:
    from zeropkg_db import list_installed_quick, get_manifest_quick
except Exception:
    # fallback: define simple stubs that raise helpful errors
    def list_installed_quick(db_path: Optional[str] = None):
        raise RuntimeError("zeropkg_db not available. Install zeropkg_db or adjust PYTHONPATH.")

    def get_manifest_quick(name_or_namever: str, db_path: Optional[str] = None):
        raise RuntimeError("zeropkg_db not available. Install zeropkg_db or adjust PYTHONPATH.")

try:
    from zeropkg_logger import get_logger, log_event
    _logger = get_logger("vuln")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_vuln")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(f"{pkg}:{stage} {msg}")

# Optional integrations
try:
    from zeropkg_upgrade import cmd_upgrade_package  # hypothetical function to upgrade package
except Exception:
    cmd_upgrade_package = None

try:
    from zeropkg_patcher import apply_patch_file  # hypothetical function to apply patch given path/url
except Exception:
    apply_patch_file = None

# requests optional for fetching remote feeds
try:
    import requests
except Exception:
    requests = None

# -----------------------
# Utilities: version comparison helpers (simple)
# -----------------------
import re
from functools import total_ordering

@total_ordering
class Version:
    """
    Minimal version class supporting numeric segments and simple comparisons.
    Not a full semver implementation, but covers typical GNU package versions.
    """
    def __init__(self, ver: str):
        self.raw = str(ver).strip()
        # split on non-alphanumeric (keep letters) but preserve numeric groups
        parts = re.split(r'[\._\-+]', self.raw)
        self.parts = []
        for p in parts:
            if p.isdigit():
                self.parts.append(int(p))
            else:
                # split letters+digits sequences into chunks 'rc1' -> ('rc',1) approximate
                m = re.match(r'^([a-zA-Z]+)(\d*)$', p)
                if m:
                    self.parts.append(m.group(1))
                    if m.group(2):
                        self.parts.append(int(m.group(2)))
                else:
                    # fallback store str
                    self.parts.append(p)

    def _cmp_tuple(self):
        # convert parts to tuple with consistent type ordering
        tuple_parts = []
        for p in self.parts:
            if isinstance(p, int):
                tuple_parts.append( (0,p) )
            else:
                tuple_parts.append( (1,str(p)) )
        return tuple(tuple_parts)

    def __eq__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self._cmp_tuple() == other._cmp_tuple()

    def __lt__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self._cmp_tuple() < other._cmp_tuple()

    def __repr__(self):
        return f"Version({self.raw})"

def version_in_range(ver_str: str, rule: str) -> bool:
    """
    Check if version ver_str matches a single comparison rule or interval.
    Supported examples:
      "<1.2.3"
      "<=1.2"
      ">=1.0,<2.0"
      "==1.2.3"
    Returns True if ver_str satisfies the rule.
    """
    ver = Version(ver_str)
    rule = rule.strip()
    if ',' in rule:
        parts = [r.strip() for r in rule.split(',') if r.strip()]
        return all(version_in_range(ver_str, p) for p in parts)
    m = re.match(r'^(<=|>=|==|<|>)(.+)$', rule)
    if not m:
        # if rule looks like a bare version, treat as equality
        return ver == Version(rule)
    op, rv = m.group(1), m.group(2).strip()
    rv_ver = Version(rv)
    if op == '<':
        return ver < rv_ver
    elif op == '<=':
        return ver == rv_ver or ver < rv_ver
    elif op == '>':
        return ver > rv_ver
    elif op == '>=':
        return ver == rv_ver or ver > rv_ver
    elif op == '==':
        return ver == rv_ver
    return False

def is_version_affected(installed_version: str, affected_rules: List[str]) -> bool:
    """
    affected_rules: list of rules such as ["<1.1.1k", ">=1.1.1l,<1.1.1n"]
    returns True if any of the rules matches the installed_version (i.e., vulnerable).
    """
    for rule in affected_rules:
        try:
            if version_in_range(installed_version, rule):
                return True
        except Exception:
            # if parsing fails, be conservative and return False for that rule
            continue
    return False

# -----------------------
# Vulnerability DB handling
# -----------------------
class VulnDB:
    """
    Manage a simple vulnerability DB (local JSON file + optional remote fetch).
    """
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or "/var/lib/zeropkg/vulndb.json")
        self.records: List[Dict[str,Any]] = []
        self.last_updated: Optional[float] = None
        self._load_local()

    def _load_local(self):
        if self.db_path.exists():
            try:
                with open(self.db_path, "r") as f:
                    data = json.load(f)
                if isinstance(data, dict) and "records" in data:
                    self.records = data["records"]
                    self.last_updated = data.get("updated_at")
                elif isinstance(data, list):
                    self.records = data
                else:
                    self.records = []
            except Exception as e:
                _logger.error(f"Failed to load local vuln DB: {e}")
                self.records = []
        else:
            self.records = []

    def save_local(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"updated_at": time.time(), "records": self.records}
        with open(self.db_path, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        self.last_updated = payload["updated_at"]
        _logger.info(f"VulnDB saved to {self.db_path}")

    def load_from_file(self, path: str):
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(path)
        with open(p, "r") as f:
            data = json.load(f)
        if isinstance(data, dict) and "records" in data:
            self.records = data["records"]
        elif isinstance(data, list):
            self.records = data
        else:
            raise ValueError("Invalid vuln DB format")
        self.save_local()

    def fetch_remote(self, url: str, params: Optional[Dict[str,str]] = None, timeout: int = 30) -> int:
        """
        Try to download a JSON feed from `url`. Requires 'requests' package.
        Returns number of records fetched. Raises if requests not installed.
        """
        if requests is None:
            raise RuntimeError("requests library is required to fetch remote feeds")
        r = requests.get(url, params=params or {}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        # accept either list or {records: ...}
        if isinstance(data, dict) and "records" in data:
            recs = data["records"]
        elif isinstance(data, list):
            recs = data
        else:
            raise ValueError("Unsupported remote feed format")
        # naive merge: extend (could be improved: dedupe by id)
        existing_ids = {r.get("id") for r in self.records if r.get("id")}
        added = 0
        for rec in recs:
            if rec.get("id") not in existing_ids:
                self.records.append(rec)
                added += 1
        self.save_local()
        return added

    def query_by_package(self, package_name: str) -> List[Dict[str,Any]]:
        """
        Return list of vuln records whose 'package' field matches package_name (case-insensitive)
        """
        out = []
        for r in self.records:
            pkg = r.get("package") or r.get("pkg") or ""
            if pkg and pkg.lower() == package_name.lower():
                out.append(r)
        return out

    def all(self) -> List[Dict[str,Any]]:
        return self.records

# -----------------------
# High-level scanner and fixer
# -----------------------
class VulnerabilityManager:
    def __init__(self, db_path: Optional[str] = None, vulndb_path: Optional[str] = None):
        self.vulndb = VulnDB(vulndb_path)
        self.db_path = db_path
        _logger.info("VulnerabilityManager initialized")

    def update_vulndb(self, url: str, params: Optional[Dict[str,str]] = None, dry_run: bool = False) -> Dict[str,Any]:
        """
        Fetch remote feed and update local DB. Returns summary dict.
        """
        if dry_run:
            _logger.info("[dry-run] would fetch vuln feed from %s", url)
            return {"ok": True, "added": 0}
        try:
            added = self.vulndb.fetch_remote(url, params=params)
            _logger.info(f"Fetched remote vuln feed: {added} new records")
            return {"ok": True, "added": added}
        except Exception as e:
            _logger.error(f"Failed to fetch remote vuln feed: {e}")
            return {"ok": False, "error": str(e)}

    def scan_installed(self, db_path: Optional[str] = None) -> Dict[str,Any]:
        """
        Scan all installed packages from the DB and return a report:
        { package_name: [ {vuln_record, installed_version, suggested_fix}, ... ] }
        """
        report: Dict[str, List[Dict[str,Any]]] = {}
        try:
            installed = list_installed_quick(db_path)
        except Exception as e:
            raise RuntimeError(f"Failed to list installed packages: {e}")

        for row in installed:
            name = row.get("name")
            version = row.get("version") or "0"
            if not name:
                continue
            vulns = self._scan_package(name, version)
            if vulns:
                report[name] = vulns
        return {"ok": True, "report": report, "total_packages": len(installed)}

    def _scan_package(self, name: str, installed_version: str) -> List[Dict[str,Any]]:
        """
        Internal: check vuln DB for entries matching `name` and comparing installed_version.
        Returns list of dict {vuln, installed_version, is_affected, suggested_fix}
        """
        matches = []
        candidates = self.vulndb.query_by_package(name)
        for rec in candidates:
            affected_rules = rec.get("affected_versions") or []
            is_affected = False
            if affected_rules:
                try:
                    is_affected = is_version_affected(installed_version, affected_rules)
                except Exception:
                    is_affected = False
            else:
                # if no affected_versions given, conservative: mark as possibly affected
                is_affected = True
            suggestion = None
            if is_affected:
                suggestion = self._suggest_fix(rec)
            matches.append({
                "vuln": rec,
                "installed_version": installed_version,
                "is_affected": is_affected,
                "suggestion": suggestion
            })
        # only return those where is_affected True
        return [m for m in matches if m["is_affected"]]

    def _suggest_fix(self, vuln_record: Dict[str,Any]) -> Dict[str,Any]:
        """
        Suggest a fix from the vuln_record: prefer fix_version (upgrade) then patch_url (patch).
        Returns dict like {"type":"upgrade","version":"1.2.3"} or {"type":"patch","url":"..."} or {"type":"manual"}
        """
        fix_v = vuln_record.get("fix_version")
        if fix_v:
            return {"type":"upgrade", "version": str(fix_v)}
        p = vuln_record.get("patch_url")
        if p:
            return {"type":"patch", "url": p}
        return {"type":"manual", "note": "No automatic fix available"}

    # ------------------------------
    # Actions to attempt fixes
    # ------------------------------
    def apply_fixes(self, auto_apply: bool = False, db_path: Optional[str] = None, dry_run: bool = True) -> Dict[str,Any]:
        """
        High-level: scan and apply fixes. Behavior:
          - scan_installed
          - for each vulnerability:
              - if suggestion.type == upgrade and cmd_upgrade_package available: call it
              - elif suggestion.type == patch and apply_patch_file available: download and apply patch
              - else: report manual
        Returns summary with successes/failures.
        Note: default dry_run=True -> simulate
        """
        summary = {"applied": [], "skipped": [], "failed": []}
        scan = self.scan_installed(db_path)
        report = scan.get("report", {})
        for pkg_name, vulns in report.items():
            for v in vulns:
                vuln = v["vuln"]
                sugg = v.get("suggestion") or {}
                action = sugg.get("type", "manual")
                _logger.info(f"Package {pkg_name} affected by {vuln.get('id')} severity={vuln.get('severity')} -> suggestion={action}")
                if action == "upgrade":
                    if cmd_upgrade_package is None:
                        _logger.warning("Upgrade integration not available; skipping")
                        summary["skipped"].append({"pkg": pkg_name, "reason": "no-upgrade-integration", "vuln": vuln.get("id")})
                        continue
                    target_ver = sugg.get("version")
                    if dry_run:
                        _logger.info(f"[dry-run] Would upgrade {pkg_name} -> {target_ver}")
                        summary["skipped"].append({"pkg": pkg_name, "action": "upgrade", "target": target_ver})
                        continue
                    try:
                        res = cmd_upgrade_package(pkg_name, target_ver)
                        summary["applied"].append({"pkg": pkg_name, "action": "upgrade", "target": target_ver, "result": res})
                    except Exception as e:
                        summary["failed"].append({"pkg": pkg_name, "action": "upgrade", "error": str(e)})
                elif action == "patch":
                    patch_url = sugg.get("url")
                    if apply_patch_file is None:
                        _logger.warning("Patcher integration not available; skipping patch")
                        summary["skipped"].append({"pkg": pkg_name, "reason": "no-patcher", "vuln": vuln.get("id")})
                        continue
                    if dry_run:
                        _logger.info(f"[dry-run] Would fetch and apply patch {patch_url} to {pkg_name}")
                        summary["skipped"].append({"pkg": pkg_name, "action": "patch", "patch": patch_url})
                        continue
                    try:
                        # apply_patch_file can accept URL or local path; ensure function exists
                        res = apply_patch_file(patch_url, pkg_name=pkg_name)
                        summary["applied"].append({"pkg": pkg_name, "action": "patch", "patch": patch_url, "result": res})
                    except Exception as e:
                        summary["failed"].append({"pkg": pkg_name, "action": "patch", "error": str(e)})
                else:
                    summary["skipped"].append({"pkg": pkg_name, "action": "manual", "vuln": vuln.get("id")})
        return summary

    # ------------------------------
    # Reporting helpers
    # ------------------------------
    def save_report(self, report: Dict[str,Any], path: Optional[str] = None):
        p = Path(path or "/var/lib/zeropkg/vuln-report.json")
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w") as f:
            json.dump(report, f, indent=2, sort_keys=True)
        _logger.info(f"Saved vuln report to {p}")
        return str(p)

# -----------------------
# Simple CLI wrapper (optional)
# -----------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(description="Zeropkg Vulnerability Scanner & Fixer")
    parser.add_argument("--vulndb", help="local vuln db path", default="/var/lib/zeropkg/vulndb.json")
    parser.add_argument("--fetch", help="fetch remote vuln feed URL", default=None)
    parser.add_argument("--scan", action="store_true", help="scan installed packages")
    parser.add_argument("--apply", action="store_true", help="attempt to apply fixes")
    parser.add_argument("--dry-run", action="store_true", help="do not change system (default True for apply)")
    parser.add_argument("--db-path", help="path to zeropkg sqlite DB", default=None)
    parser.add_argument("--out", help="path to write report JSON", default="/var/lib/zeropkg/vuln-report.json")
    args = parser.parse_args()

    vm = VulnerabilityManager(db_path=args.db_path, vulndb_path=args.vulndb)
    if args.fetch:
        print("Fetching remote feed...", vm.update_vulndb(args.fetch))
    if args.scan:
        scan = vm.scan_installed(db_path=args.db_path)
        vm.save_report(scan, args.out)
        print("Scan saved to", args.out)
    if args.apply:
        res = vm.apply_fixes(auto_apply=True, db_path=args.db_path, dry_run=args.dry_run)
        vm.save_report(res, args.out)
        print("Apply summary saved to", args.out)

if __name__ == "__main__":
    _cli()
