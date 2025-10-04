#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_vuln.py — Gerenciador de vulnerabilidades para Zeropkg

Funcionalidades:
 - fetch_remote(): baixa/atualiza base de vulnerabilidades (melhor esforço)
 - load_local_db(): lê DB local/cache de vulnerabilidades (JSON)
 - scan_package(pkg_name): escaneia um pacote instalado/recipe por CVEs
 - scan_all(): escaneia todos os pacotes instalados
 - detect_vuln_packages(severity="ALL"): lista pacotes por severidade
 - apply_fix(pkg_name): tenta aplicar patch (patcher) ou upgrade (upgrade module)
 - generate_report(...): gera JSON + HTML com resumo e detalhes
 - integração com update/sync: hook scan_after_update()
 - CLI mínimo embutido: zeropkg vuln ...
 - suporte a dry_run, chroot, fakeroot
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import logging
import tempfile
import argparse
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path

# -------- Safe imports of project modules (graceful fallback) -------
def safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

cfg_mod = safe_import("zeropkg_config")
logger_mod = safe_import("zeropkg_logger")
db_mod = safe_import("zeropkg_db")
patcher_mod = safe_import("zeropkg_patcher")
upgrade_mod = safe_import("zeropkg_upgrade")
depclean_mod = safe_import("zeropkg_depclean")
update_mod = safe_import("zeropkg_update")
downloader_mod = safe_import("zeropkg_downloader")
chroot_mod = safe_import("zeropkg_chroot")

# -------- Basic logger setup (falls back to stdlib logging) ----------
if logger_mod and hasattr(logger_mod, "get_logger"):
    log = logger_mod.get_logger("vuln")
    log_event = getattr(logger_mod, "log_event", lambda a,b,c,**kw: None)
else:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("zeropkg.vuln")
    def log_event(pkg, stage, msg, level="info", extra=None):
        if level == "error":
            log.error(f"{pkg}:{stage} - {msg}")
        elif level == "warning":
            log.warning(f"{pkg}:{stage} - {msg}")
        else:
            log.info(f"{pkg}:{stage} - {msg}")

# -------- Config defaults & paths -----------------------------------
CONFIG = {}
try:
    if cfg_mod and hasattr(cfg_mod, "load_config"):
        CONFIG = cfg_mod.load_config()
except Exception:
    CONFIG = {}

VULN_CACHE_DIR = Path(CONFIG.get("paths", {}).get("cache_dir", "/var/cache/zeropkg")) / "vuln"
VULN_CACHE_DIR.mkdir(parents=True, exist_ok=True)

VULN_DB_JSON = VULN_CACHE_DIR / "vulndb.json"        # local copy of vulnerability DB
VULN_REPORT_DIR = Path(CONFIG.get("paths", {}).get("state_dir", "/var/lib/zeropkg")) / "vuln_reports"
VULN_REPORT_DIR.mkdir(parents=True, exist_ok=True)

# -------- Utilities -------------------------------------------------
def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _safe_write(path: Path, data: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(path)

def _read_json(path: Path) -> Optional[Dict[str,Any]]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"failed to read json {path}: {e}")
        return None

# -------- Minimal version compare helper (uses packaging if available) ---
def _cmp_versions(v1: str, v2: str) -> int:
    """
    retorna -1,0,1 se v1 < v2, v1 == v2, v1 > v2 (melhor esforço).
    usa packaging.version se disponível, fallback string compare.
    """
    try:
        pv = safe_import("packaging.version")
        if pv:
            a = pv.version.parse(v1)
            b = pv.version.parse(v2)
            if a < b: return -1
            if a > b: return 1
            return 0
    except Exception:
        pass
    # fallback: split numeric-ish parts
    try:
        def Norm(s):
            parts = []
            for p in str(s).replace('-', '.').split('.'):
                try:
                    parts.append(int(p))
                except Exception:
                    parts.append(p)
            return parts
        a = Norm(v1); b = Norm(v2)
        if a < b: return -1
        if a > b: return 1
        return 0
    except Exception:
        return 0

# -------- Vulnerability DB format expected (best-effort) -------------
# Exemplo simples esperado (vulndb.json):
# {
#   "generated": "2025-10-04T12:00:00Z",
#   "sources": [{"name":"cvelist", "url":"https://example.com/db.json"}],
#   "packages": {
#       "openssl": [
#           {"cve":"CVE-2025-XXXXX", "affected":"<1.2.3", "fixed_in":"1.2.3", "severity":"CRITICAL", "description":"..."},
#           ...
#       ],
#       "glibc": [ ... ]
#   }
# }
#
# O módulo tentará ser permissivo com formatos.

# -------- Main class: ZeroPKGVulnManager -----------------------------
class ZeroPKGVulnManager:
    def __init__(self, cache_path: Path = VULN_DB_JSON):
        self.cache_path = Path(cache_path)
        self.db = _read_json(self.cache_path) or {"generated": None, "sources": [], "packages": {}}
        # integration handles
        self._dbmod = db_mod
        self._patcher = patcher_mod.ZeropkgPatcher() if patcher_mod and hasattr(patcher_mod, "ZeropkgPatcher") else None
        self._upgrade = upgrade_mod if upgrade_mod else None
        self._depclean = depclean_mod.Depclean() if depclean_mod and hasattr(depclean_mod, "Depclean") else None
        self._downloader = downloader_mod.Downloader() if downloader_mod and hasattr(downloader_mod, "Downloader") else None
        self._chroot = chroot_mod if chroot_mod else None

    # -------- Fetch & update remote vuln DB (best-effort) -------------
    def fetch_remote(self, sources: Optional[List[Dict[str,str]]] = None, *, dry_run: bool = False, timeout: int = 30) -> Dict[str,Any]:
        """
        Baixa/atualiza uma lista de fontes para a base local.
        sources: list of {"name":..., "url":...} ; se None, tenta usar self.db["sources"] ou config.
        Retorna resumo {"ok":bool, "fetched": N, "errors": [...]}
        """
        res = {"ok": False, "fetched": 0, "errors": []}
        if dry_run:
            log.info("fetch_remote dry-run")
            return {"ok": True, "fetched": 0, "errors": []}
        # determinar fontes
        srcs = sources or self.db.get("sources") or CONFIG.get("vuln", {}).get("sources") or []
        if not srcs:
            # fallback: nenhuma fonte configurada
            log.info("no vuln sources configured")
            return {"ok": True, "fetched": 0, "errors": []}
        fetched = 0
        aggregated = {"generated": _now_iso(), "sources": srcs, "packages": {}}
        for s in srcs:
            url = s.get("url")
            if not url:
                continue
            try:
                log.info(f"fetching vuln source {s.get('name') or url}")
                # use downloader if available
                data = None
                if self._downloader:
                    try:
                        p = Path(self._downloader.fetch(url, VULN_CACHE_DIR))
                        data = _read_json(p)
                    except Exception:
                        data = None
                if data is None:
                    # fallback urllib
                    import urllib.request
                    with urllib.request.urlopen(url, timeout=timeout) as resp:
                        raw = resp.read()
                        try:
                            data = json.loads(raw.decode('utf-8'))
                        except Exception:
                            # try text -> maybe non-json, skip
                            data = None
                if not data:
                    log.warning(f"source {url} returned no usable JSON")
                    continue
                # merge packages (simple union, latest wins by cve id uniqueness)
                pkgs = data.get("packages", {})
                for pkg, entries in pkgs.items():
                    aggregated["packages"].setdefault(pkg, [])
                    # append entries (no dedupe for simplicity)
                    aggregated["packages"][pkg].extend(entries)
                fetched += 1
            except Exception as e:
                log.warning(f"failed to fetch {url}: {e}")
                res["errors"].append({"source": url, "error": str(e)})
        # persist aggregated
        try:
            self.db = aggregated
            _safe_write(self.cache_path, self.db)
            res["ok"] = True
            res["fetched"] = fetched
        except Exception as e:
            res["errors"].append({"error": str(e)})
            res["ok"] = False
        return res

    # -------- Loading & access helpers --------------------------------
    def load_local_db(self) -> Dict[str,Any]:
        """Recarrega a base do cache local."""
        self.db = _read_json(self.cache_path) or {"generated": None, "sources": [], "packages": {}}
        return self.db

    def list_sources(self) -> List[Dict[str,str]]:
        return list(self.db.get("sources", []) or [])

    # -------- Query helpers -------------------------------------------
    def _vulns_for_package(self, pkg_name: str) -> List[Dict[str,Any]]:
        """
        Retorna a lista de entradas de vulnerabilidade para um pacote (melhor esforço).
        """
        pkg = pkg_name.lower()
        entries = self.db.get("packages", {}).get(pkg) or self.db.get("packages", {}).get(pkg_name) or []
        return entries

    def detect_vuln_packages(self, severity: str = "ALL") -> Dict[str,List[Dict[str,Any]]]:
        """
        Retorna um dicionário {pkg: [vuln entries]} filtrado por severidade.
        severity in ["ALL","LOW","MEDIUM","HIGH","CRITICAL"]
        """
        sev = severity.upper()
        out = {}
        for pkg, entries in (self.db.get("packages") or {}).items():
            filtered = []
            for e in entries:
                e_sev = (e.get("severity") or e.get("cvss") or "UNKNOWN").upper()
                if sev == "ALL" or e_sev == sev:
                    filtered.append(e)
            if filtered:
                out[pkg] = filtered
        return out

    # -------- Scan helpers (installed packages via DB) ----------------
    def _get_installed_packages(self) -> List[Dict[str,Any]]:
        """
        Tenta obter lista de pacotes instalados do zeropkg_db module (melhor esforço).
        Retorna lista de {"name":..., "version":..., "manifest":...}
        """
        installed = []
        try:
            if db_mod and hasattr(db_mod, "list_installed_quick"):
                for r in db_mod.list_installed_quick():
                    installed.append({"name": r["name"], "version": r.get("version") or r.get("ver") or r.get("pkg_version"), "manifest": r})
        except Exception:
            # fallback: vazio
            pass
        return installed

    def scan_package(self, pkg_name: str, installed_version: Optional[str] = None) -> Dict[str,Any]:
        """
        Escaneia um pacote (nome) e compara com entradas da DB de vulnerabilidades.
        Retorna resumo: {"pkg":..., "installed_version":..., "vulns": [...], "ok": bool}
        """
        pkg = pkg_name
        if not installed_version:
            # try to get from DB
            try:
                if db_mod and hasattr(db_mod, "get_package_manifest"):
                    m = db_mod.get_package_manifest(pkg)
                    if m:
                        installed_version = m.get("version") or m.get("ver") or m.get("pkg_version")
            except Exception:
                pass
        entries = self._vulns_for_package(pkg)
        matches = []
        for e in entries:
            try:
                affected = e.get("affected") or e.get("affected_versions") or e.get("range") or ""
                fixed_in = e.get("fixed_in") or e.get("fix_version")
                sev = e.get("severity") or e.get("cvss") or "UNKNOWN"
                # heurística: se affected contém operator like "<1.2.3" and installed_version compare lower
                if installed_version and affected:
                    # simplest parse: look for "<", "<=", ">" etc.
                    if '<' in affected or '>' in affected or '=' in affected:
                        # try to parse: if affected contains "<X" and installed_version < X -> vulnerable
                        try:
                            # handle patterns like "<1.2.3"
                            tokens = affected.replace(' ', '').split(',')
                            vulnerable = False
                            for t in tokens:
                                if t.startswith('<'):
                                    v = t[1:]
                                    if _cmp_versions(installed_version, v) < 0:
                                        vulnerable = True
                                elif t.startswith('<='):
                                    v = t[2:]
                                    if _cmp_versions(installed_version, v) <= 0:
                                        vulnerable = True
                                elif t.startswith('>'):
                                    v = t[1:]
                                    if _cmp_versions(installed_version, v) > 0:
                                        vulnerable = True
                                elif t.startswith('>='):
                                    v = t[2:]
                                    if _cmp_versions(installed_version, v) >= 0:
                                        vulnerable = True
                                elif t.startswith('=') or '==' in t:
                                    v = t.replace('=', '').replace('==', '')
                                    if _cmp_versions(installed_version, v) == 0:
                                        vulnerable = True
                                else:
                                    # fallback substring match
                                    if v in installed_version:
                                        vulnerable = True
                            if vulnerable:
                                matches.append({"entry": e, "installed_version": installed_version})
                        except Exception:
                            # fallback: add entry conservatively
                            matches.append({"entry": e, "installed_version": installed_version})
                    else:
                        # if affected expresses a version string and installed matches substring
                        if installed_version and affected in installed_version:
                            matches.append({"entry": e, "installed_version": installed_version})
                        else:
                            # unknown format: conservatively include
                            matches.append({"entry": e, "installed_version": installed_version})
                else:
                    # no installed_version known, add as potential
                    matches.append({"entry": e, "installed_version": installed_version})
            except Exception as ex:
                log.warning(f"scan match exception for {pkg}: {ex}")
                matches.append({"entry": e, "installed_version": installed_version})
        ok = len(matches) == 0
        return {"pkg": pkg, "installed_version": installed_version, "vulns": matches, "ok": ok}

    def scan_all(self, *, severity: str = "ALL", dry_run: bool = True) -> Dict[str,Any]:
        """
        Escaneia todos os pacotes instalados e retorna relatório.
        severity: filtra severidade
        dry_run: se True, não aplica correções (scan-only)
        """
        installed = self._get_installed_packages()
        report = {"ts": _now_iso(), "severity": severity, "checked": len(installed), "results": [], "summary": {"total_vulns": 0, "packages_affected": 0}}
        for pkg in installed:
            r = self.scan_package(pkg["name"], pkg.get("version"))
            # filter by severity
            if r["vulns"]:
                filtered = []
                for m in r["vulns"]:
                    sev = (m["entry"].get("severity") or "UNKNOWN").upper()
                    if severity == "ALL" or sev == severity.upper():
                        filtered.append(m)
                r["vulns"] = filtered
            if r["vulns"]:
                report["results"].append(r)
                report["summary"]["total_vulns"] += len(r["vulns"])
        report["summary"]["packages_affected"] = len(report["results"])
        # optionally persist intermediate report
        tag = f"vuln-scan-{int(time.time())}"
        _safe_write(VULN_REPORT_DIR / f"{tag}.json", report)
        return report

    # -------- Apply fixes: patch first, else upgrade ------------------
    def apply_fix(self, pkg_name: str, *, dry_run: bool = True, use_chroot: bool = False, fakeroot: bool = False) -> Dict[str,Any]:
        """
        Tenta aplicar uma correção para um pacote:
         - procurar patches (via patcher) e aplicar;
         - se não houver patch, tentar upgrade via upgrade module;
         - se dry_run True, apenas planeja ação.
        Retorna dict com resultado e ações tomadas.
        """
        out = {"pkg": pkg_name, "actions": [], "ok": False, "errors": []}
        # 1) try patcher: see if recipe has security patches listed in recipe.hooks or recipe.patches with tag "security"
        try:
            # load recipe if possible
            rec_path = None
            if DEPS and hasattr(DEPS, "_recipes_index"):
                rec_path = DEPS._recipes_index.get(pkg_name)
            else:
                # fallback: try db manifest for recipe path
                if db_mod and hasattr(db_mod, "get_package_manifest"):
                    m = db_mod.get_package_manifest(pkg_name)
                    rec_path = (m or {}).get("recipe")
            applied_patch = False
            if rec_path and patcher_mod and hasattr(patcher_mod, "ZeropkgPatcher"):
                # inspect recipe for patches with marker "security" (best-effort)
                try:
                    toml_mod = safe_import("zeropkg_toml")
                    if toml_mod and hasattr(toml_mod, "load_recipe"):
                        recipe = toml_mod.load_recipe(rec_path)
                        patches = recipe.get("patches") or []
                        sec_patches = []
                        for p in patches:
                            if isinstance(p, dict) and (p.get("tag") == "security" or p.get("purpose") == "security" or p.get("security")):
                                sec_patches.append(p)
                        if sec_patches:
                            out["actions"].append({"type": "patch", "count": len(sec_patches)})
                            if dry_run:
                                out["ok"] = True
                                return out
                            patcher = self._patcher
                            if not patcher:
                                out["errors"].append("patcher-missing")
                            else:
                                # apply all security patches
                                for ps in sec_patches:
                                    r = patcher.apply_all(rec_path, target_dir=recipe.get("build", {}).get("directory") or ".", dry_run=dry_run, use_chroot=use_chroot, fakeroot=fakeroot, parallel=False)
                                    out["actions"].append({"patch_result": r})
                                    if r.get("ok"):
                                        applied_patch = True
                except Exception as e:
                    out["errors"].append(f"patch-inspect-failed:{e}")
            # 2) if no patch applied, try upgrade
            if not applied_patch:
                if self._upgrade and hasattr(self._upgrade, "upgrade"):
                    out["actions"].append({"type": "upgrade", "planned": True})
                    if dry_run:
                        out["ok"] = True
                        return out
                    try:
                        # call upgrade.upgrade([pkg_name]) -> expected dict
                        r = self._upgrade.upgrade([pkg_name], dry_run=dry_run)
                        out["actions"].append({"upgrade_result": r})
                        out["ok"] = r.get("ok", True)
                    except Exception as e:
                        out["errors"].append(f"upgrade-failed:{e}")
                else:
                    out["errors"].append("no-patcher-no-upgrade")
        except Exception as e:
            out["errors"].append(str(e))
        # after attempted fixes, optionally run depclean to drop old vulnerable versions
        try:
            if not dry_run and self._depclean:
                try:
                    self._depclean.execute(apply=True, dry_run=False)
                    out.setdefault("post_depclean", []).append("depclean_executed")
                except Exception as e:
                    out.setdefault("post_depclean_errors", []).append(str(e))
        except Exception:
            pass
        return out

    # -------- Generate report (JSON + HTML minimal) -------------------
    def generate_report(self, scan_report: Dict[str,Any], *, tag: Optional[str] = None) -> Dict[str,Any]:
        """
        Gera arquivos JSON e HTML com base em scan_report.
        Retorna paths written.
        """
        tag = tag or f"vuln-report-{int(time.time())}"
        js_path = VULN_REPORT_DIR / f"{tag}.json"
        html_path = VULN_REPORT_DIR / f"{tag}.html"
        try:
            _safe_write(js_path, scan_report)
        except Exception as e:
            log.warning(f"failed to write JSON report: {e}")
        # minimal HTML generation
        try:
            lines = []
            lines.append("<!doctype html><html><head><meta charset='utf-8'><title>Vuln Report</title></head><body>")
            lines.append(f"<h1>Vulnerability report — {scan_report.get('ts')}</h1>")
            lines.append(f"<p>Packages affected: {scan_report.get('summary', {}).get('packages_affected', 0)}, total vulns: {scan_report.get('summary', {}).get('total_vulns', 0)}</p>")
            lines.append("<table border='1' cellpadding='4'><thead><tr><th>Package</th><th>Installed</th><th>CVE</th><th>Severity</th><th>Fixed in</th><th>Description</th></tr></thead><tbody>")
            for r in scan_report.get("results", []):
                pkg = r.get("pkg")
                inst = r.get("installed_version") or "-"
                for m in r.get("vulns", []):
                    e = m.get("entry", {})
                    cve = e.get("cve") or e.get("id") or "-"
                    sev = e.get("severity") or "-"
                    fixed = e.get("fixed_in") or "-"
                    desc = (e.get("description") or "")[:200]
                    lines.append(f"<tr><td>{pkg}</td><td>{inst}</td><td>{cve}</td><td>{sev}</td><td>{fixed}</td><td>{desc}</td></tr>")
            lines.append("</tbody></table></body></html>")
            html_path.write_text("\n".join(lines), encoding="utf-8")
        except Exception as e:
            log.warning(f"failed to write HTML report: {e}")
        return {"json": str(js_path), "html": str(html_path)}

    # -------- Integration hook for update/sync ------------------------
    def scan_after_update(self, *, severity: str = "ALL", dry_run: bool = True) -> Dict[str,Any]:
        """
        Hook para ser chamado após `update`/`sync` — atualiza base CVE e executa scan rápido.
        """
        out = {"fetch": None, "scan": None}
        try:
            out["fetch"] = self.fetch_remote(dry_run=dry_run)
            self.load_local_db()
            out["scan"] = self.scan_all(severity=severity, dry_run=dry_run)
        except Exception as e:
            out["error"] = str(e)
        return out

# -------- CLI mínimo embutido ----------------------------------------
def _cli():
    parser = argparse.ArgumentParser(prog="zeropkg-vuln", description="Zeropkg vulnerability manager")
    parser.add_argument("action", choices=["fetch","scan","apply","report","list-sources"], help="ação")
    parser.add_argument("--severity", choices=["ALL","LOW","MEDIUM","HIGH","CRITICAL"], default="ALL")
    parser.add_argument("--package", help="package to scan/apply")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tag", help="report tag")
    args = parser.parse_args()

    vm = ZeroPKGVulnManager()
    if args.action == "fetch":
        r = vm.fetch_remote(dry_run=args.dry_run)
        print(json.dumps(r, indent=2))
        return 0
    if args.action == "list-sources":
        print(json.dumps(vm.list_sources(), indent=2))
        return 0
    if args.action == "scan":
        if args.package:
            r = vm.scan_package(args.package)
            print(json.dumps(r, indent=2))
        else:
            r = vm.scan_all(severity=args.severity, dry_run=args.dry_run)
            print(json.dumps(r, indent=2))
        return 0
    if args.action == "apply":
        if not args.package:
            print("apply requires --package")
            return 2
        r = vm.apply_fix(args.package, dry_run=args.dry_run)
        print(json.dumps(r, indent=2))
        return 0
    if args.action == "report":
        if args.package:
            s = vm.scan_package(args.package)
            rep = vm.generate_report({"ts": _now_iso(), "results":[s], "summary": {"total_vulns": len(s.get("vulns",[])), "packages_affected": 1}})
            print(json.dumps(rep, indent=2))
        else:
            s = vm.scan_all(severity=args.severity, dry_run=args.dry_run)
            rep = vm.generate_report(s, tag=args.tag)
            print(json.dumps(rep, indent=2))
        return 0
    return 0

# -------- Exported symbol -------------------------------------------
__all__ = ["ZeroPKGVulnManager"]

# If executed directly, run CLI
if __name__ == "__main__":
    sys.exit(_cli())
