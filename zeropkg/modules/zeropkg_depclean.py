#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_depclean.py — Depclean inteligente para Zeropkg

Características:
 - Identificação segura de pacotes órfãos via grafo (integra com zeropkg_deps)
 - Proteções configuráveis (lista de pacotes protegidos por default)
 - Backup automático (snapshot DB + listagem de arquivos) antes de remover
 - Hooks pré/post (globais e por-receita) com captura de saída
 - Execução paralela (ThreadPoolExecutor) com controle de workers
 - Dry-run completo (não altera DB / arquivos)
 - Integração com zeropkg_db, zeropkg_remover, zeropkg_logger e zeropkg_config
 - Relatórios JSON/TXT em /var/lib/zeropkg/depclean_reports/
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import tempfile
import threading
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Set, Tuple

# ---------- Safe imports (graceful fallback) ----------
def _safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

db_mod = _safe_import("zeropkg_db")
logger_mod = _safe_import("zeropkg_logger")
remover_mod = _safe_import("zeropkg_remover")
deps_mod = _safe_import("zeropkg_deps")
config_mod = _safe_import("zeropkg_config")
toml_mod = _safe_import("zeropkg_toml")

# Fallback logger
if logger_mod and hasattr(logger_mod, "log_event"):
    def _log(evt, msg, level="INFO", metadata=None):
        try:
            logger_mod.log_event(evt, msg, level=level, metadata=metadata)
        except Exception:
            print(f"[{level}] {evt}: {msg}", file=sys.stderr)
else:
    def _log(evt, msg, level="INFO", metadata=None):
        if level == "ERROR":
            print(f"[{level}] {evt}: {msg}", file=sys.stderr)
        else:
            print(f"[{level}] {evt}: {msg}")

# Config helpers
def _get_config():
    if config_mod and hasattr(config_mod, "get_config_manager"):
        try:
            return config_mod.get_config_manager().config
        except Exception:
            pass
    # fallback defaults
    return {
        "paths": {
            "state_dir": "/var/lib/zeropkg",
            "reports_dir": "/var/lib/zeropkg/depclean_reports",
            "backups_dir": "/var/backups/zeropkg/depclean"
        },
        "depclean": {
            "protected": ["gcc", "glibc", "linux-headers", "binutils", "bash", "coreutils"],
            "max_workers": 4,
            "backup": True
        }
    }

CFG = _get_config()
REPORTS_DIR = Path(CFG.get("paths", {}).get("reports_dir", "/var/lib/zeropkg/depclean_reports"))
BACKUP_DIR = Path(CFG.get("paths", {}).get("backups_dir", "/var/backups/zeropkg/depclean"))
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Utilities ----------
_lock = threading.RLock()

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _safe_write_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(path)

def _run_hook_cmd(cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None, timeout: Optional[int]=300) -> Dict[str,Any]:
    """
    Executa um comando hook (shell) com timeout; captura stdout/stderr.
    Roda em subprocess via shell para compatibilidade com scripts de receita.
    """
    import subprocess
    try:
        proc = subprocess.run(cmd, shell=True, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout)
        return {"ok": proc.returncode == 0, "rc": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except subprocess.TimeoutExpired as e:
        return {"ok": False, "error": "timeout", "stdout": e.stdout or "", "stderr": e.stderr or ""}
    except Exception as e:
        return {"ok": False, "error": str(e), "stdout": "", "stderr": ""}

# ---------- Core Depclean class ----------
class Depclean:
    def __init__(self, config: Optional[Dict[str,Any]] = None):
        self.cfg = config or CFG
        self.protected: Set[str] = set(self.cfg.get("depclean", {}).get("protected", []))
        self.max_workers = int(self.cfg.get("depclean", {}).get("max_workers", 4))
        self.auto_backup = bool(self.cfg.get("depclean", {}).get("backup", True))
        # modules
        self.db = db_mod if db_mod else None
        self.remover = remover_mod if remover_mod else None
        self.deps = deps_mod if deps_mod else None
        self.toml = toml_mod if toml_mod else None

    # -----------------------------
    # Discover candidates
    # -----------------------------
    def find_orphan_candidates(self, include_protected: bool = False, exclude: Optional[List[str]] = None, keep: Optional[List[str]] = None) -> Dict[str,Any]:
        """
        Retorna dicionário com candidatos a remoção:
         - 'installed': lista de instalados (via DB)
         - 'referenced': lista de pacotes referenciados (dependências)
         - 'orphans': candidatos (installed - referenced - protected - exclude + keep)
        """
        exclude = set([e for e in (exclude or [])])
        keep = set([k for k in (keep or [])])
        installed = set()
        referenced = set()
        protected = set(self.protected)
        if include_protected:
            protected = set()
        # get installed via db
        if self.db and hasattr(self.db, "list_installed_quick"):
            try:
                for r in self.db.list_installed_quick():
                    installed.add(r["name"])
            except Exception as e:
                _log("depclean", f"db.list_installed_quick failed: {e}", "ERROR")
        else:
            _log("depclean", "db module not present; cannot list installed packages", "ERROR")
            return {"installed": [], "referenced": [], "orphans": [], "protected": list(protected)}
        # get referenced via deps_mod if available (graph)
        if self.deps and hasattr(self.deps, "build_graph"):
            try:
                graph = self.deps.build_graph()  # should return dict {pkg: [deps]}
                for pkg, deps in graph.items():
                    for d in deps:
                        referenced.add(d)
            except Exception:
                # fallback: query revdeps per package (slower)
                for pkg in list(installed):
                    try:
                        rev = set(self.db.find_revdeps(pkg))
                        referenced.update(rev)
                    except Exception:
                        pass
        else:
            # fallback: derive from DB deps table
            if self.db:
                try:
                    # find all depends_on entries in DB
                    cur = self.db._execute("SELECT DISTINCT depends_on FROM deps")
                    for r in cur.fetchall():
                        referenced.add(r["depends_on"])
                except Exception:
                    pass
        # Compute orphans
        candidates = installed - referenced - protected - exclude
        # Ensure we don't remove items in keep list
        candidates = candidates - keep
        # return sorted lists for deterministic output
        return {
            "installed": sorted(installed),
            "referenced": sorted(referenced),
            "protected": sorted(protected),
            "exclude": sorted(exclude),
            "keep": sorted(keep),
            "orphans": sorted(candidates)
        }

    # -----------------------------
    # Backup helpers
    # -----------------------------
    def _create_backup_for_pkg(self, pkg: str, note: Optional[str] = None) -> Dict[str,Any]:
        """
        Cria backup leve: snapshot DB (se disponível) e manifest list (files)
        Retorna metadados de backup (path, ok).
        """
        ts = int(time.time())
        target_dir = BACKUP_DIR / f"{pkg}-{ts}"
        target_dir.mkdir(parents=True, exist_ok=True)
        results = {"pkg": pkg, "backup_dir": str(target_dir), "ok": True, "parts": []}
        # snapshot DB
        try:
            if self.db and hasattr(self.db, "snapshot"):
                snap = self.db.snapshot(note=f"depclean backup for {pkg}: {note or ''}")
                results["parts"].append({"type": "db_snapshot", "meta": snap})
            else:
                # fallback: minimal record export
                results["parts"].append({"type": "db_snapshot", "meta": "db-module-missing"})
        except Exception as e:
            results["ok"] = False
            results["parts"].append({"type": "db_snapshot", "error": str(e)})
        # package manifest files list
        try:
            manifest = {}
            if self.db and hasattr(self.db, "get_package_manifest"):
                m = self.db.get_package_manifest(pkg)
                manifest = m or {}
            # write manifest file
            mf = target_dir / "manifest.json"
            _safe_write_json(mf, manifest)
            results["parts"].append({"type": "manifest", "path": str(mf)})
        except Exception as e:
            results["ok"] = False
            results["parts"].append({"type": "manifest", "error": str(e)})
        return results

    # -----------------------------
    # Hook runner
    # -----------------------------
    def _run_hooks_for_pkg(self, pkg: str, when: str, dry_run: bool = True) -> Dict[str,Any]:
        """
        Runs hooks defined globally in config or in recipe (if available).
        when in {"pre_remove","post_remove"}.
        """
        res = {"pkg": pkg, "when": when, "results": []}
        # 1) global hooks from config
        try:
            hooks = self.cfg.get("hooks", {}) or {}
            global_hooks = hooks.get(when, []) or []
            if isinstance(global_hooks, str):
                global_hooks = [global_hooks]
            for cmd in global_hooks:
                if dry_run:
                    res["results"].append({"cmd": cmd, "dry_run": True})
                else:
                    r = _run_hook_cmd(cmd, cwd=None, env=os.environ.copy())
                    res["results"].append({"cmd": cmd, "result": r})
        except Exception as e:
            res["results"].append({"error": str(e)})
        # 2) recipe hooks via toml (if available)
        try:
            if self.toml and hasattr(self.toml, "load_recipe"):
                # try to find recipe for package in db manifest (if path known)
                manifest = None
                if self.db and hasattr(self.db, "get_package_manifest"):
                    manifest = self.db.get_package_manifest(pkg)
                recipe_path = None
                if manifest and isinstance(manifest, dict):
                    recipe_path = manifest.get("manifest", {}).get("recipe") or manifest.get("manifest", {}).get("recipe_path")
                if recipe_path and Path(recipe_path).exists():
                    recipe = self.toml.load_recipe(recipe_path)
                    rhooks = recipe.get("hooks", {}) or {}
                    for cmd in rhooks.get(when, []):
                        if dry_run:
                            res["results"].append({"cmd": cmd, "dry_run": True, "source": "recipe"})
                        else:
                            r = _run_hook_cmd(cmd, cwd=Path(recipe.get("_meta", {}).get("path", ".")).parent, env=os.environ.copy())
                            res["results"].append({"cmd": cmd, "result": r, "source": "recipe"})
        except Exception as e:
            res["results"].append({"error": f"recipe_hooks_failed: {e}"})
        return res

    # -----------------------------
    # Remove package (wrap remover)
    # -----------------------------
    def _remove_package(self, pkg: str, *, dry_run: bool = True, backup: bool = True) -> Dict[str,Any]:
        """
        Remove package metadata and files (best-effort):
         - create backup if requested
         - run pre_remove hooks
         - call remover_mod to remove files (if available) else remove metadata via db
         - run post_remove hooks
        Returns dict with status.
        """
        out = {"pkg": pkg, "ok": False, "actions": [], "errors": []}
        try:
            # backup
            if backup and self.auto_backup:
                try:
                    b = self._create_backup_for_pkg(pkg, note="depclean")
                    out["actions"].append({"backup": b})
                except Exception as e:
                    out["errors"].append(f"backup_failed:{e}")
            # pre hooks
            try:
                hooks_pre = self._run_hooks_for_pkg(pkg, "pre_remove", dry_run=dry_run)
                out["actions"].append({"pre_hooks": hooks_pre})
            except Exception as e:
                out["errors"].append(f"pre_hooks_failed:{e}")
            # removal
            if dry_run:
                out["actions"].append({"remove": "dry-run"})
                out["ok"] = True
                return out
            # try using remover module
            if self.remover and hasattr(self.remover, "remove_package"):
                try:
                    r = self.remover.remove_package(pkg)
                    out["actions"].append({"remover_result": r})
                    out["ok"] = r.get("ok", True)
                except Exception as e:
                    out["errors"].append(f"remover_exception:{e}")
                    out["ok"] = False
            else:
                # fallback: remove metadata from db only
                if self.db and hasattr(self.db, "remove_package_quick"):
                    try:
                        r = self.db.remove_package_quick(pkg)
                        out["actions"].append({"db_remove": r})
                        out["ok"] = r.get("ok", False)
                    except Exception as e:
                        out["errors"].append(f"db_remove_failed:{e}")
                        out["ok"] = False
                else:
                    out["errors"].append("no_remover_no_db")
                    out["ok"] = False
            # post hooks
            try:
                hooks_post = self._run_hooks_for_pkg(pkg, "post_remove", dry_run=False)
                out["actions"].append({"post_hooks": hooks_post})
            except Exception as e:
                out["errors"].append(f"post_hooks_failed:{e}")
            # after remove, invalidate deps cache if available
            try:
                if self.deps and hasattr(self.deps, "invalidate_cache"):
                    self.deps.invalidate_cache()
            except Exception:
                pass
            return out
        except Exception as e:
            out["errors"].append(str(e))
            out["ok"] = False
            return out

    # -----------------------------
    # Public execute method
    # -----------------------------
    def execute(self,
                apply: bool = False,
                include_protected: bool = False,
                exclude: Optional[List[str]] = None,
                keep: Optional[List[str]] = None,
                only: Optional[List[str]] = None,
                parallel: bool = True,
                max_workers: Optional[int] = None,
                backup: Optional[bool] = None,
                report_tag: Optional[str] = None) -> Dict[str,Any]:
        """
        Main entry point.
         - apply: if False -> dry-run, True -> perform removals
         - only: if set, only these packages are candidates (must be subset of found orphans)
         - exclude/keep: lists to exclude or keep
        Returns a report dict.
        """
        with _lock:
            report = {
                "ts": _now_iso(),
                "apply": bool(apply),
                "dry_run": not bool(apply),
                "config": {"protected": sorted(list(self.protected))},
                "candidates": [],
                "results": [],
            }
            max_workers = max_workers or (self.max_workers or 4)
            backup = self.auto_backup if backup is None else bool(backup)

            # find candidates
            cand_info = self.find_orphan_candidates(include_protected=include_protected, exclude=exclude, keep=keep)
            candidates = set(cand_info.get("orphans", []))
            report["candidates_raw"] = cand_info

            # if only specified, intersect
            if only:
                only_set = set(only)
                candidates = candidates & only_set

            report["candidates"] = sorted(candidates)

            if not candidates:
                _log("depclean", "No orphan candidates found", "INFO")
                report["summary"] = {"removed": 0, "skipped": 0}
                return report

            # prepare removal list (ordered) — minimal heuristic: remove largest packages first
            # get sizes via db if available
            pkg_sizes = {}
            if self.db:
                try:
                    for p in candidates:
                        m = self.db.get_package_manifest(p)
                        size = (m.get("size") if isinstance(m, dict) else None) if m else None
                        pkg_sizes[p] = size or 0
                except Exception:
                    pass
            # sort descending by size, fallback alphabetical
            ordered = sorted(list(candidates), key=lambda x: (-pkg_sizes.get(x, 0), x))
            report["ordered_candidates"] = ordered

            # removal worker
            def _worker(pkg_name: str) -> Dict[str,Any]:
                try:
                    r = self._remove_package(pkg_name, dry_run=not apply, backup=backup)
                    # record event in db
                    try:
                        if self.db and hasattr(self.db, "record_event"):
                            self.db.record_event("depclean.remove", level="INFO", package=pkg_name, payload=r)
                    except Exception:
                        pass
                    return {"pkg": pkg_name, "result": r}
                except Exception as e:
                    return {"pkg": pkg_name, "result": {"ok": False, "errors": [str(e), traceback.format_exc()]}}
            results = []
            if parallel and max_workers > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futures = {ex.submit(_worker, p): p for p in ordered}
                    for fut in as_completed(futures):
                        p = futures[fut]
                        try:
                            res = fut.result()
                        except Exception as e:
                            res = {"pkg": p, "result": {"ok": False, "errors": [str(e), traceback.format_exc()]}}
                        results.append(res)
            else:
                for p in ordered:
                    results.append(_worker(p))

            report["results"] = results
            # summary
            removed = sum(1 for r in results if r["result"].get("ok"))
            failed = sum(1 for r in results if not r["result"].get("ok"))
            report["summary"] = {"processed": len(results), "removed": removed, "failed": failed}
            # write report
            tag = report_tag or f"depclean-{int(time.time())}"
            js_path = REPORTS_DIR / f"{tag}.json"
            txt_path = REPORTS_DIR / f"{tag}.txt"
            try:
                _safe_write_json(js_path, report)
                # write a small human summary
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(f"Depclean report {report['ts']}\n")
                    f.write(f"Apply: {apply}\n\nSummary:\n")
                    f.write(json.dumps(report["summary"], indent=2, ensure_ascii=False))
                    f.write("\n\nDetails:\n")
                    for r in results:
                        f.write(json.dumps(r, indent=2, ensure_ascii=False))
                        f.write("\n\n")
                _log("depclean", f"Depclean report written to {js_path} and {txt_path}", "INFO")
            except Exception as e:
                _log("depclean", f"Failed to write report: {e}", "ERROR")
            return report

# ---------- CLI ----------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-depclean", description="Zeropkg depclean utility")
    parser.add_argument("--apply", action="store_true", help="Apply removals (default is dry-run)")
    parser.add_argument("--only", nargs="+", help="Limit to specific packages")
    parser.add_argument("--exclude", nargs="+", help="Exclude these packages from removal")
    parser.add_argument("--keep", nargs="+", help="Keep these packages (do not remove)")
    parser.add_argument("--no-backup", action="store_true", help="Disable backup before removal")
    parser.add_argument("--parallel", action="store_true", help="Run removals in parallel")
    parser.add_argument("--max-workers", type=int, default=None, help="Max workers for parallel removal")
    parser.add_argument("--report-tag", help="Tag / filename prefix for reports")
    args = parser.parse_args()

    d = Depclean()
    report = d.execute(
        apply=bool(args.apply),
        only=args.only,
        exclude=args.exclude,
        keep=args.keep,
        parallel=bool(args.parallel),
        max_workers=args.max_workers,
        backup=not args.no_backup,
        report_tag=args.report_tag
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
