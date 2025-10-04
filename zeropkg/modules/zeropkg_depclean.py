#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_depclean.py — Depclean avançado para Zeropkg

Funcionalidades:
 - Identifica pacotes órfãos e pacotes removíveis com checagem de dependências compartilhadas
 - Protege pacotes críticos configuráveis
 - Backup incremental antes de remoção
 - Hooks pré/post-clean (globais e por-receita)
 - Relatórios JSON e TXT em /var/lib/zeropkg (configurável)
 - Dry-run completo e execução paralela controlada
 - Integração com zeropkg_config, zeropkg_db, zeropkg_logger, zeropkg_remover e zeropkg_deps
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
import concurrent.futures
from pathlib import Path
from typing import List, Dict, Set, Optional, Any, Tuple

# Try to import project modules (optional) — use fallbacks if absent
try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        return {
            "paths": {
                "cache_dir": "/var/cache/zeropkg",
                "state_dir": "/var/lib/zeropkg",
                "backup_dir": "/var/backups/zeropkg",
                "report_dir": "/var/lib/zeropkg"
            },
            "depclean": {
                "auto_clean": False,
                "protected_packages": ["gcc", "glibc", "bash", "coreutils", "binutils", "linux-headers"],
                "max_workers": 4,
                "hooks_dir": "/etc/zeropkg/hooks.d/depclean"
            }
        }

try:
    from zeropkg_logger import get_logger, log_event, perf_timer
    log = get_logger("depclean")
except Exception:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("zeropkg.depclean")
    def log_event(pkg, stage, msg, level="info", extra=None):
        getattr(log, level, log.info)(f"{pkg}:{stage} - {msg}")

try:
    from zeropkg_db import ZeroPKGDB, _get_default_db
    DB_AVAILABLE = True
except Exception:
    ZeroPKGDB = None
    _get_default_db = None
    DB_AVAILABLE = False

try:
    from zeropkg_deps import DepsManager
    DEPS_AVAILABLE = True
except Exception:
    DepsManager = None
    DEPS_AVAILABLE = False

try:
    from zeropkg_remover import ZeropkgRemover
    REMOVER_AVAILABLE = True
except Exception:
    ZeropkgRemover = None
    REMOVER_AVAILABLE = False

# CONFIG
CFG = load_config()
PATHS = CFG.get("paths", {})
DEPCFG = CFG.get("depclean", {})

CACHE_DIR = Path(PATHS.get("cache_dir", "/var/cache/zeropkg"))
STATE_DIR = Path(PATHS.get("state_dir", "/var/lib/zeropkg"))
BACKUP_DIR = Path(PATHS.get("backup_dir", "/var/backups/zeropkg"))
REPORT_DIR = Path(PATHS.get("report_dir", "/var/lib/zeropkg"))

CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

HOOKS_DIR = Path(DEPCFG.get("hooks_dir", "/etc/zeropkg/hooks.d/depclean"))
HOOKS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PROTECTED = set(DEPCFG.get("protected_packages", ["gcc", "glibc", "bash", "coreutils", "binutils", "linux-headers"]))
MAX_WORKERS = int(DEPCFG.get("max_workers", 4))

# convenience DB handle
def _get_db():
    if DB_AVAILABLE and _get_default_db:
        return _get_default_db()
    return None

# ----------------------------
# Utilities
# ----------------------------
def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _safe_write(path: Path, data: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(path)

def _run_hook(path: Path, args: Optional[List[str]] = None, dry_run: bool = False) -> Tuple[bool, str]:
    """
    Run a hook script with args. Returns (ok, output).
    Hooks are best-effort and should not break depclean.
    """
    if not path.exists() or not os.access(path, os.X_OK):
        return False, f"hook-missing-or-not-executable: {path}"
    if dry_run:
        return True, "[dry-run]"
    try:
        import subprocess
        cmd = [str(path)]
        if args:
            cmd += args
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        ok = p.returncode == 0
        return ok, p.stdout or ""
    except Exception as e:
        return False, str(e)

# ----------------------------
# Core logic
# ----------------------------
class Depclean:
    def __init__(self, config: Optional[Dict[str,Any]] = None):
        self.cfg = config or CFG
        self.protected = set(DEFAULT_PROTECTED)
        self.backup_dir = BACKUP_DIR
        self.report_dir = REPORT_DIR
        self.hooks_dir = HOOKS_DIR
        self.max_workers = MAX_WORKERS
        self.db = _get_db()
        self.deps = DepsManager() if DEPS_AVAILABLE else None
        # allow dynamic overrides from config
        if "depclean" in self.cfg:
            for p in self.cfg.get("depclean", {}).get("protected_packages", []):
                self.protected.add(p)
            self.max_workers = int(self.cfg.get("depclean", {}).get("max_workers", self.max_workers))

    def _list_installed(self) -> List[str]:
        """Return list of installed package names from DB or empty list."""
        if not self.db:
            log.info("DB not available: cannot list installed packages")
            return []
        try:
            return [r["name"] for r in self.db.list_installed_quick()]
        except Exception as e:
            log_event("depclean", "db", f"list_installed failed: {e}", level="warning")
            return []

    def _collect_references(self) -> Set[str]:
        """
        Collect all referenced package names from deps graph (conservative).
        If deps module is unavailable, use DB dependencies table heuristic.
        """
        refs = set()
        if self.deps:
            # graph edges: for each node, for each out edge (dependee) add dependee as referenced
            for node in self.deps.graph.nodes:
                for d in self.deps.graph.out_edges(node):
                    refs.add(d)
            return refs
        # fallback: try DB dependencies table
        try:
            conn = self.db._connect() if self.db else None
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT DISTINCT dependee FROM dependencies")
                for r in cur.fetchall():
                    refs.add(r["dependee"])
            return refs
        except Exception:
            return refs

    def _compute_orphans(self, keep: Optional[Set[str]] = None, exclude: Optional[Set[str]] = None) -> List[str]:
        """
        Conservative orphan detection:
         - installed packages MINUS referenced packages MINUS protected packages MINUS keep/exclude
        """
        keep = keep or set()
        exclude = exclude or set()
        installed = set(self._list_installed())
        if not installed:
            return []
        referenced = self._collect_references()
        candidates = installed - referenced - self.protected - keep - exclude
        return sorted(candidates)

    def _backup_package(self, pkg: str) -> Optional[Path]:
        """
        Create a small backup (DB snapshot + optional file list) before removal.
        Returns path to backup manifest (JSON) or None.
        """
        ts = int(time.time())
        bdir = self.backup_dir / f"{pkg}-{ts}"
        try:
            bdir.mkdir(parents=True, exist_ok=True)
            manifest = {"pkg": pkg, "ts": ts, "files": [], "db_snapshot": None}
            # try to copy file list info from DB manifest if available
            try:
                manifest_data = self.db.get_package_manifest(pkg) if self.db else None
                if manifest_data:
                    manifest["files"] = manifest_data.get("files", [])
            except Exception:
                pass
            # snapshot DB
            try:
                snap = self.db.snapshot(dest_dir=bdir) if self.db else None
                manifest["db_snapshot"] = str(snap) if snap else None
            except Exception:
                manifest["db_snapshot"] = None
            # write manifest
            mf = bdir / "manifest.json"
            _safe_write(mf, manifest)
            return mf
        except Exception as e:
            log_event("depclean", "backup", f"backup failed for {pkg}: {e}", level="warning")
            return None

    def _run_global_hooks(self, stage: str, dry_run: bool = False) -> List[Dict[str,Any]]:
        """
        Run all executables in hooks_dir with stage as argument (pre/post).
        """
        results = []
        if not self.hooks_dir.exists():
            return results
        for p in sorted(self.hooks_dir.glob("*")):
            if p.is_file() and os.access(p, os.X_OK):
                ok, out = _run_hook(p, args=[stage], dry_run=dry_run)
                results.append({"hook": str(p), "ok": ok, "out": out})
                log_event("depclean", f"hook.{stage}", f"{p.name} -> ok={ok}")
        return results

    def _run_recipe_hook(self, pkg: str, stage: str, dry_run: bool = False) -> Dict[str,Any]:
        """
        If recipe contains hooks for depclean, attempt to run them.
        We try to find recipe via deps manager index.
        """
        res = {"pkg": pkg, "hook": None, "ok": None, "out": None}
        try:
            if not self.deps:
                return res
            recipe_path = self.deps._recipes_index.get(pkg)
            if not recipe_path:
                return res
            # load recipe TOML to find hooks
            from zeropkg_toml import load_recipe
            recipe = load_recipe(recipe_path)
            hooks = recipe.get("hooks", {}) or {}
            stage_cmds = hooks.get(stage) or hooks.get(f"depclean_{stage}") or []
            if isinstance(stage_cmds, str):
                stage_cmds = [stage_cmds]
            outputs = []
            ok_all = True
            for c in stage_cmds:
                # c may be a command string; run in package build dir if available
                cwd = recipe.get("build", {}).get("directory") or "."
                # run
                if dry_run:
                    outputs.append("[dry-run]")
                    continue
                import subprocess
                p = subprocess.run(c if isinstance(c, list) else ["sh", "-c", c], cwd=cwd,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                ok = p.returncode == 0
                ok_all = ok_all and ok
                outputs.append(p.stdout or "")
            res["hook"] = stage_cmds
            res["ok"] = ok_all
            res["out"] = "\n".join(outputs)
            return res
        except Exception as e:
            res["ok"] = False
            res["out"] = str(e)
            return res

    @perf_timer("depclean", "plan_orphans")
    def plan_orphans(self, keep: Optional[Set[str]] = None, exclude: Optional[Set[str]] = None) -> Dict[str,Any]:
        """
        Return a report with candidate orphans and reasons.
        """
        keep = keep or set()
        exclude = exclude or set()
        orphans = self._compute_orphans(keep=keep, exclude=exclude)
        report = {"ts": _now_iso(), "protected": sorted(list(self.protected)), "installed_count": len(self._list_installed()), "candidates": []}
        referenced = self._collect_references()
        # for each candidate gather revdeps and files
        for pkg in orphans:
            revdeps = []
            try:
                if self.deps:
                    revdeps = list(self.deps.impact_analysis(pkg).get("impacted", []))
                else:
                    revdeps = self.db.find_revdeps(pkg) if self.db else []
            except Exception:
                revdeps = []
            manifest = None
            try:
                manifest = self.db.get_package_manifest(pkg) if self.db else None
            except Exception:
                manifest = None
            report["candidates"].append({
                "pkg": pkg,
                "revdeps": revdeps,
                "files": [f.get("dst") for f in (manifest.get("files") if manifest else [])] if manifest else []
            })
        return report

    def _remove_single_pkg(self, pkg: str, dry_run: bool = True, backup: bool = True) -> Dict[str,Any]:
        """
        Attempt to remove a single package.
        Returns dict with status and details.
        Uses ZeropkgRemover if available, else DB-only removal fallback.
        """
        result = {"pkg": pkg, "ok": False, "method": None, "backup": None, "error": None}
        try:
            # ensure not protected at runtime
            if pkg in self.protected:
                result["error"] = "protected"
                return result
            if dry_run:
                # still show what would be done
                # manifest list for preview
                try:
                    manifest = self.db.get_package_manifest(pkg) if self.db else None
                except Exception:
                    manifest = None
                result["ok"] = True
                result["method"] = "dry-run"
                result["backup"] = None
                result["files"] = [f.get("dst") for f in (manifest.get("files") if manifest else [])] if manifest else []
                return result

            # backup
            if backup:
                b = self._backup_package(pkg)
                result["backup"] = str(b) if b else None

            # run pre-remove recipe hooks
            rh_pre = self._run_recipe_hook(pkg, "pre_remove", dry_run=dry_run)
            result.setdefault("recipe_hooks", {})["pre_remove"] = rh_pre

            # call remover
            if REMOVER_AVAILABLE and ZeropkgRemover:
                remover = ZeropkgRemover()
                ok = remover.remove(pkg, dry_run=False)
                result["ok"] = bool(ok)
                result["method"] = "remover"
                if not ok:
                    result["error"] = "removal_failed"
                # post-remove hooks
            else:
                # fallback: remove from DB only (not filesystem)
                try:
                    ok = self.db.remove_package_quick(pkg)
                    result["ok"] = bool(ok)
                    result["method"] = "db-only"
                    if not ok:
                        result["error"] = "db_remove_failed"
                except Exception as e:
                    result["error"] = f"exception: {e}"
                    result["ok"] = False

            rh_post = self._run_recipe_hook(pkg, "post_remove", dry_run=dry_run)
            result.setdefault("recipe_hooks", {})["post_remove"] = rh_post

            # log event
            log_event("depclean", "remove", f"{pkg} removed -> ok={result['ok']}", level="info", extra={"pkg": pkg})
            return result
        except Exception as e:
            result["error"] = str(e)
            log_event("depclean", "remove", f"exception removing {pkg}: {e}", level="error")
            return result

    @perf_timer("depclean", "execute")
    def execute(self, *, only: Optional[Set[str]] = None, exclude: Optional[Set[str]] = None,
                keep: Optional[Set[str]] = None, apply: bool = False, dry_run: bool = True,
                backup_before_remove: bool = True, parallel: bool = False,
                protected_extra: Optional[Set[str]] = None, report_tag: Optional[str] = None) -> Dict[str,Any]:
        """
        Main entry to run depclean.
        - only: only consider these packages (intersect with candidates)
        - exclude: remove these from candidates
        - keep: never remove these
        - apply: actually perform removals (False = dry-run)
        - dry_run: convenience param (if apply==False implies dry_run True)
        - backup_before_remove: create backup manifests for each removal
        - parallel: attempt parallel removals
        - protected_extra: additional packages to protect this run
        Returns detailed report with actions planned / executed.
        """
        report = {
            "ts": _now_iso(),
            "apply": bool(apply),
            "dry_run": bool(dry_run),
            "protected": sorted(list(self.protected | (protected_extra or set()))),
            "only": sorted(list(only)) if only else None,
            "exclude": sorted(list(exclude)) if exclude else None,
            "keep": sorted(list(keep)) if keep else None,
            "candidates": [],
            "results": []
        }

        protected_run = set(self.protected)
        if protected_extra:
            protected_run |= set(protected_extra)

        # run global pre-clean hooks
        report["global_pre_hooks"] = self._run_global_hooks("pre", dry_run=dry_run)

        # plan orphans
        planned = self.plan_orphans(keep=keep, exclude=exclude)
        candidates = [c["pkg"] for c in planned.get("candidates", [])]

        # apply only/intersect/exclude
        if only:
            candidates = [c for c in candidates if c in only]
        if exclude:
            candidates = [c for c in candidates if c not in exclude]
        # ensure not protected
        candidates = [c for c in candidates if c not in protected_run]

        report["candidates"] = candidates

        # if no candidates, finish
        if not candidates:
            report["note"] = "no candidates"
            report["global_post_hooks"] = self._run_global_hooks("post", dry_run=dry_run)
            # write small report
            tag = report_tag or f"depclean-{int(time.time())}"
            self._write_reports(report, tag)
            return report

        # removal (dry-run or real)
        if parallel:
            workers = min(self.max_workers, max(1, len(candidates)))
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(self._remove_single_pkg, pkg, dry_run=not apply, backup=backup_before_remove): pkg for pkg in candidates}
                for fut in concurrent.futures.as_completed(futures):
                    pkg = futures[fut]
                    try:
                        res = fut.result()
                    except Exception as e:
                        res = {"pkg": pkg, "ok": False, "error": str(e)}
                    report["results"].append(res)
        else:
            for pkg in candidates:
                res = self._remove_single_pkg(pkg, dry_run=not apply, backup=backup_before_remove)
                report["results"].append(res)

        # run global post-clean hooks
        report["global_post_hooks"] = self._run_global_hooks("post", dry_run=dry_run)

        # finalize report and write to disk
        tag = report_tag or f"depclean-{int(time.time())}"
        self._write_reports(report, tag)

        # if apply and removals occurred, optionally run depclean auto tasks (like updating caches)
        if apply:
            try:
                # invalidate deps cache if present
                if self.deps:
                    self.deps._save_cache(self.deps._find_recipe_files())
            except Exception:
                pass

        return report

    def _write_reports(self, report: Dict[str,Any], tag: str):
        """
        Write JSON and TXT reports to report_dir. Keep last N reports configurable later.
        """
        ts = int(time.time())
        js = self.report_dir / f"{tag}.json"
        txt = self.report_dir / f"{tag}.txt"
        try:
            _safe_write(js, report)
            # create human readable txt
            lines = []
            lines.append(f"Depclean report: {tag}")
            lines.append(f"timestamp: {report.get('ts')}")
            lines.append(f"apply: {report.get('apply')}, dry_run: {report.get('dry_run')}")
            lines.append(f"protected: {', '.join(report.get('protected') or [])}")
            lines.append("")
            lines.append("Candidates:")
            for c in report.get("candidates", []):
                lines.append(f"  - {c}")
            lines.append("")
            lines.append("Results:")
            for r in report.get("results", []):
                status = "OK" if r.get("ok") else "SKIP/FAIL"
                lines.append(f"  - {r.get('pkg')}: {status} (method={r.get('method')})")
                if r.get("backup"):
                    lines.append(f"      backup: {r.get('backup')}")
                if r.get("error"):
                    lines.append(f"      error: {r.get('error')}")
            txt.write_text("\n".join(lines), encoding="utf-8")
        except Exception as e:
            log_event("depclean", "report", f"failed to write report: {e}", level="warning")

# ----------------------------
# CLI
# ----------------------------
def _cli():
    parser = argparse.ArgumentParser(prog="zeropkg-depclean", description="Zeropkg depclean utility")
    parser.add_argument("--apply", action="store_true", help="Actually remove packages (default = dry-run)")
    parser.add_argument("--only", nargs="+", help="Only consider these candidate packages (space separated)")
    parser.add_argument("--exclude", nargs="+", help="Exclude these packages from removal")
    parser.add_argument("--keep", nargs="+", help="Never remove these packages (merge with protected list)")
    parser.add_argument("--protected", nargs="+", help="Additional protected packages for this run")
    parser.add_argument("--backup", action="store_true", help="Create backup before removal (default: True)")
    parser.add_argument("--no-backup", dest="backup", action="store_false", help="Do not backup before removal")
    parser.add_argument("--parallel", action="store_true", help="Attempt parallel removals")
    parser.add_argument("--max-workers", type=int, help="Override max parallel workers")
    parser.add_argument("--report-tag", help="Tag used for report filenames")
    parser.add_argument("--verbose", "-v", action="count", default=0)
    args = parser.parse_args()

    dc = Depclean()
    if args.max_workers:
        dc.max_workers = args.max_workers
    only = set(args.only) if args.only else None
    exclude = set(args.exclude) if args.exclude else None
    keep = set(args.keep) if args.keep else None
    protected = set(args.protected) if args.protected else None

    report = dc.execute(only=only, exclude=exclude, keep=keep, apply=bool(args.apply),
                        backup_before_remove=bool(args.backup), parallel=bool(args.parallel),
                        protected_extra=protected, report_tag=args.report_tag)
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
