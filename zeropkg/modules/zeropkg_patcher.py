#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_patcher.py — Aplica patches de forma segura, com rollback, GPG,
hooks globais/locais, séries, paralelismo opcional e registro em DB.

Funcionalidades principais:
 - apply_all(recipe_path, ...) aplica todos os patches definidos na receita
 - suporte a patches locais e remotos (integra com zeropkg_downloader)
 - validação de checksum SHA256
 - validação GPG via chave pública (se configurada em zeropkg_config)
 - pré/pós-hooks globais (/etc/zeropkg/hooks.d/) e locais
 - rollback transacional usando snapshot do DB (zeropkg_db)
 - registro de hash final dos arquivos alterados no DB (audit trail)
 - opção --parallel para aplicar patches em paralelo (use com cuidado)
"""

from __future__ import annotations
import os
import sys
import json
import subprocess
import shutil
import hashlib
import tempfile
import threading
import concurrent.futures
import time
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

# ---- Imports opcionais do ecossistema Zeropkg (fallbacks seguros) ----
try:
    from zeropkg_logger import log_event, get_logger, perf_timer
    log = get_logger("patcher")
except Exception:
    def log_event(pkg, stage, msg, level="info", extra=None):
        print(f"[{level.upper()}] {pkg}:{stage} - {msg}")
    import logging
    log = logging.getLogger("zeropkg.patcher")
    log.setLevel(logging.INFO)
    def perf_timer(name, op):
        def deco(f):
            return f
        return deco

try:
    from zeropkg_downloader import Downloader
    DOWNLOADER_AVAILABLE = True
except Exception:
    Downloader = None
    DOWNLOADER_AVAILABLE = False

try:
    from zeropkg_db import ZeroPKGDB, _get_default_db
    DB_AVAILABLE = True
except Exception:
    ZeroPKGDB = None
    _get_default_db = None
    DB_AVAILABLE = False

try:
    from zeropkg_toml import load_recipe
    TOML_AVAILABLE = True
except Exception:
    TOML_AVAILABLE = False
    def load_recipe(path):
        # minimal fallback: try to read json or simple toml-like
        with open(path, "r", encoding="utf-8") as f:
            return {"package": {"name": Path(path).stem}, "patches": []}

try:
    from zeropkg_chroot import run_in_chroot, prepare_chroot, cleanup_chroot, is_chroot_ready
    CHROOT_AVAILABLE = True
except Exception:
    CHROOT_AVAILABLE = False

try:
    from zeropkg_vuln import ZeroPKGVulnManager
    VULN_AVAILABLE = True
except Exception:
    VULN_AVAILABLE = False

try:
    from zeropkg_config import load_config
    CFG_AVAILABLE = True
except Exception:
    CFG_AVAILABLE = False
    def load_config(path=None):
        return {
            "paths": {
                "patch_cache": "/var/cache/zeropkg/patches",
            },
            "security": {
                "gpg_pubkeys": [],   # caminhos para chaves públicas
                "gpg_cmd": "gpg"
            },
            "patcher": {
                "hooks_dir": "/etc/zeropkg/hooks.d"
            }
        }

# ---- Configuração e caminhos ----
CFG = load_config()
PATCH_CACHE = Path(CFG.get("paths", {}).get("patch_cache", "/var/cache/zeropkg/patches"))
PATCH_CACHE.mkdir(parents=True, exist_ok=True)
HOOKS_DIR = Path(CFG.get("patcher", {}).get("hooks_dir", "/etc/zeropkg/hooks.d"))
GPG_CMD = CFG.get("security", {}).get("gpg_cmd", "gpg")
GPG_PUBKEYS = CFG.get("security", {}).get("gpg_pubkeys", [])

# ---- Utilitários ----
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def _safe_run(cmd: List[str], cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None, capture: bool = False, check: bool = False) -> Tuple[int, str, str]:
    """Executa comando com subprocess e captura saída; retorna (rc, stdout, stderr)."""
    try:
        if capture:
            p = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.returncode, p.stdout or "", p.stderr or ""
        else:
            p = subprocess.run(cmd, cwd=cwd, env=env)
            return p.returncode, "", ""
    except Exception as e:
        return 1, "", str(e)

def _download_patch(url: str, dest_dir: Path) -> Path:
    """Baixa o patch para dest_dir; integra com zeropkg_downloader se disponível."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(url).name
    dest = dest_dir / filename
    if DOWNLOADER_AVAILABLE and Downloader:
        try:
            d = Downloader()
            got = d.fetch(url, dest_dir)  # espera que fetch retorne path ou similar
            if isinstance(got, (str, Path)):
                return Path(got)
        except Exception as e:
            log_event("patcher", "download", f"Downloader failed for {url}: {e}", level="warning")
    # fallback simples
    try:
        import urllib.request
        urllib.request.urlretrieve(url, str(dest))
        return dest
    except Exception as e:
        raise RuntimeError(f"failed to download {url}: {e}")

def _verify_checksum(path: Path, expected: Optional[str]) -> bool:
    if not expected:
        return True
    try:
        actual = _sha256(path)
        return actual.lower() == expected.lower()
    except Exception:
        return False

def _verify_gpg_signature(patch_path: Path, sig_path: Optional[Path], pubkey_paths: List[str]) -> bool:
    """
    Verifica assinatura GPG (melhor esforço).
    - Se sig_path estiver ausente, tenta encontrar patch_path + .sig
    - Importa chaves públicas temporariamente (melhor usar keyring global em produção)
    """
    if not sig_path or not sig_path.exists():
        alt = patch_path.with_suffix(patch_path.suffix + ".sig")
        if alt.exists():
            sig_path = alt
    if not sig_path or not sig_path.exists():
        log_event("patcher", "gpg", f"No signature file for {patch_path}", level="debug")
        return False

    # importar chaves temporariamente em um keyring temporário
    try:
        gdir = tempfile.mkdtemp(prefix="zeropkg-gpg-")
        env = os.environ.copy()
        env["GNUPGHOME"] = gdir
        # import provided public keys
        for k in pubkey_paths:
            if not Path(k).exists():
                continue
            rc, out, err = _safe_run([GPG_CMD, "--import", str(k)], env=env, capture=True)
            if rc != 0:
                log_event("patcher", "gpg", f"gpg import failed {k}: {err}", level="warning")
        # verify signature
        rc, out, err = _safe_run([GPG_CMD, "--verify", str(sig_path), str(patch_path)], env=env, capture=True)
        # cleanup
        shutil.rmtree(gdir, ignore_errors=True)
        return rc == 0
    except Exception as e:
        log_event("patcher", "gpg", f"GPG verify exception: {e}", level="warning")
        return False

def _apply_patch_with_patch_tool(patchfile: Path, target_dir: Path, strip: int = 1) -> bool:
    """
    Tenta aplicar com 'patch -p{strip}', se falhar tenta outros strip levels e 'git apply'.
    """
    # tentar patch -p{strip}
    for p in range(strip, strip+3):
        rc, out, err = _safe_run(["patch", f"-p{p}", "-i", str(patchfile)], cwd=str(target_dir), capture=True)
        if rc == 0:
            log_event("patcher", "apply", f"applied {patchfile.name} with patch -p{p}")
            return True
    # fallback: git apply
    rc, out, err = _safe_run(["git", "apply", str(patchfile)], cwd=str(target_dir), capture=True)
    if rc == 0:
        log_event("patcher", "apply", f"applied {patchfile.name} with git apply")
        return True
    log_event("patcher", "apply", f"failed to apply {patchfile.name}: {err}", level="error")
    return False

def _run_hook_cmd(cmd: str, cwd: Optional[str] = None, use_chroot: bool = False, fakeroot: bool = False, dry_run: bool = False) -> Tuple[bool, str]:
    """Executa um comando hook; se use_chroot e CHROOT_AVAILABLE usa run_in_chroot."""
    log_event("patcher", "hook", f"running hook: {cmd}", level="debug")
    if dry_run:
        return True, "[dry-run]"
    try:
        if use_chroot and CHROOT_AVAILABLE:
            rc, out, err = run_in_chroot(cmd if isinstance(cmd, str) else " ".join(cmd), cwd=cwd, fakeroot=fakeroot, dry_run=False)
            ok = rc == 0
            return ok, out if ok else err
        else:
            rc, out, err = _safe_run(cmd if isinstance(cmd, list) else ["sh", "-c", cmd], cwd=cwd, capture=True)
            return rc == 0, out if out else err
    except Exception as e:
        return False, str(e)

# ---- Patcher class ----
class ZeropkgPatcher:
    def __init__(self, config: Optional[Dict[str,Any]] = None):
        self.cfg = config or CFG
        self.patch_cache = PATCH_CACHE
        self.hooks_dir = HOOKS_DIR
        self.gpg_pubkeys = GPG_PUBKEYS or []
        self.db = _get_default_db() if DB_AVAILABLE and _get_default_db else None
        self.vuln = ZeroPKGVulnManager() if VULN_AVAILABLE else None

    def _collect_patches_from_recipe(self, recipe: Dict[str,Any]) -> List[Dict[str,Any]]:
        """
        Normaliza a lista de patches a partir da receita:
        Cada patch -> { "src": "...", "checksum": "...", "strip": 1, "series": False, "local": True/False, "sig": "..."}
        """
        patches = []
        raw = recipe.get("patches") or []
        # suporte a "series" (arquivo listando patches) e entries simples
        for entry in raw:
            if isinstance(entry, str):
                patches.append({"src": entry})
            elif isinstance(entry, dict):
                # pode ser { "series": "series-1" } ou patch entry
                if entry.get("series"):
                    # expand series file (assume path relative to recipe)
                    base = recipe.get("_recipe_base", None)
                    serpath = Path(entry.get("series"))
                    if base and not serpath.is_absolute():
                        serpath = Path(base) / serpath
                    if serpath.exists():
                        try:
                            for ln in serpath.read_text().splitlines():
                                ln = ln.strip()
                                if not ln or ln.startswith("#"):
                                    continue
                                patches.append({"src": ln, "series": str(serpath)})
                        except Exception as e:
                            log_event("patcher", "series", f"failed to read series {serpath}: {e}", level="warning")
                else:
                    patches.append(dict(entry))  # copy
        return patches

    def _prepare_patch_file(self, patch_spec: Dict[str,Any]) -> Path:
        """
        Garante que o patch está disponível localmente (no cache) e retorna Path.
        patch_spec pode conter 'src' (url ou caminho), 'checksum', 'sig'.
        """
        src = patch_spec.get("src")
        if not src:
            raise ValueError("patch spec missing src")
        # local file?
        p = Path(src)
        if p.exists():
            return p
        # relative to recipe base?
        base = patch_spec.get("recipe_base")
        if base:
            cand = Path(base) / src
            if cand.exists():
                return cand
        # else assume URL -> download to cache
        dest_dir = self.patch_cache
        try:
            local = _download_patch(src, dest_dir)
            # if signature provided remote, download it too
            sig = patch_spec.get("sig")
            if sig:
                try:
                    _download_patch(sig, dest_dir)
                except Exception:
                    log_event("patcher", "download", f"signature download failed for {sig}", level="warning")
            return Path(local)
        except Exception as e:
            raise

    def _apply_single_patch(self, patch_spec: Dict[str,Any], target_dir: Path, *, use_chroot: bool = True, fakeroot: bool = False, dry_run: bool = False) -> Dict[str,Any]:
        """
        Aplica um patch individual e retorna dict de resultado.
        """
        result = {"spec": patch_spec, "ok": False, "applied_by": None, "error": None, "patch_path": None}
        try:
            patch_path = self._prepare_patch_file(patch_spec)
            result["patch_path"] = str(patch_path)
            # verify checksum
            checksum = patch_spec.get("checksum")
            if checksum and not _verify_checksum(patch_path, checksum):
                result["error"] = "checksum-mismatch"
                log_event("patcher", "verify", f"checksum mismatch for {patch_path}", level="error")
                return result
            # gpg verify if requested
            sig = patch_spec.get("sig")
            if patch_spec.get("require_gpg", False) or (sig and len(self.gpg_pubkeys) > 0):
                ok_sig = _verify_gpg_signature(patch_path, Path(sig) if sig else None, self.gpg_pubkeys)
                if not ok_sig:
                    result["error"] = "gpg-verify-failed"
                    log_event("patcher", "gpg", f"GPG verify failed for {patch_path}", level="error")
                    return result

            # actual apply (dry-run logs only)
            if dry_run:
                log_event("patcher", "apply", f"[dry-run] would apply {patch_path.name} to {target_dir}", level="info")
                result["ok"] = True
                result["applied_by"] = "dry-run"
                return result

            strip = int(patch_spec.get("strip", 1))
            ok = _apply_patch_with_patch_tool(patch_path, target_dir, strip=strip)
            if not ok:
                result["error"] = "apply-failed"
                return result

            # compute and record SHA256 of files touched (best-effort): if patch is a diff, we cannot easily know touched files;
            # heuristic: if patch filename contains a path prefix, attempt to read first lines and guess target files. Otherwise skip.
            try:
                touched_hashes = {}
                # simple heuristic: look for lines starting with "+++ " or "diff --git a/..."
                text = patch_path.read_text(errors="ignore")
                targets = set()
                for ln in text.splitlines():
                    if ln.startswith("+++ "):
                        fn = ln.split("+++ ",1)[1].strip()
                        if fn.startswith("b/"):
                            fn = fn[2:]
                        targets.add(fn)
                    elif ln.startswith("diff --git "):
                        parts = ln.split()
                        if len(parts) >= 3:
                            a = parts[2]
                            if a.startswith("a/"):
                                a = a[2:]
                            targets.add(a)
                for t in list(targets)[:50]:
                    fp = (target_dir / t).resolve()
                    if fp.exists() and fp.is_file():
                        touched_hashes[str(fp)] = _sha256(fp)
                result["touched_hashes"] = touched_hashes
                # record hashes in DB for audit
                if DB_AVAILABLE and self.db:
                    for fp, hs in touched_hashes.items():
                        try:
                            # register as event: type patch-file-hash
                            payload = {"file": fp, "sha256": hs, "patch": str(patch_path.name)}
                            self.db._execute("INSERT INTO events (ts, type, pkg, payload) VALUES (?, 'patch-file-hash', ?, ?)", (_now_ts(), patch_spec.get("name") or str(patch_path.name), json.dumps(payload)), commit=True)
                        except Exception:
                            pass
            except Exception as e:
                log_event("patcher", "hash", f"failed to record hashes: {e}", level="warning")

            result["ok"] = True
            result["applied_by"] = "patch|git"
            log_event("patcher", "apply", f"applied patch {patch_path.name} to {target_dir}", level="info")
            return result
        except Exception as e:
            result["error"] = str(e)
            log_event("patcher", "apply", f"exception applying patch: {e}", level="error")
            return result

    # ---------------------------
    # Hooks: globais e locais
    # ---------------------------
    def _run_global_hooks(self, stage: str, *, use_chroot: bool = False, fakeroot: bool = False, dry_run: bool = False) -> List[Dict[str,Any]]:
        results = []
        if not self.hooks_dir.exists():
            return results
        for h in sorted(self.hooks_dir.iterdir()):
            if h.is_file() and os.access(h, os.X_OK):
                ok, out = _run_hook_cmd(str(h), use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
                results.append({"hook": str(h), "ok": ok, "out": out})
                log_event("patcher", "hook.global", f"{h.name} -> ok={ok}", level="debug" if ok else "warning")
        return results

    def _run_recipe_hooks(self, recipe_hooks: Dict[str,Any], stage: str, *, target_dir: Path, use_chroot: bool = False, fakeroot: bool = False, dry_run: bool = False) -> List[Dict[str,Any]]:
        results = []
        if not recipe_hooks:
            return results
        cmds = recipe_hooks.get(stage) or []
        if isinstance(cmds, str):
            cmds = [cmds]
        for c in cmds:
            ok, out = _run_hook_cmd(c, cwd=str(target_dir), use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
            results.append({"hook": c, "ok": ok, "out": out})
            log_event("patcher", f"hook.{stage}", f"{c} -> ok={ok}", level="debug" if ok else "warning")
        return results

    # ---------------------------
    # Snapshot / rollback
    # ---------------------------
    def _snapshot_db(self) -> Optional[Path]:
        """Tira snapshot do DB e retorna path (melhor esforço)."""
        if DB_AVAILABLE and self.db:
            try:
                snap = self.db.snapshot()
                log_event("patcher", "snapshot", f"db snapshot created {snap}", level="info")
                return snap
            except Exception as e:
                log_event("patcher", "snapshot", f"snapshot failed: {e}", level="warning")
                return None
        return None

    def _rollback_db_from_snapshot(self, snap_path: Path) -> bool:
        if DB_AVAILABLE and self.db:
            try:
                ok = self.db.rollback_from_snapshot(snap_path)
                log_event("patcher", "rollback", f"db rollback from {snap_path} -> {ok}", level="warning" if ok else "error")
                return ok
            except Exception as e:
                log_event("patcher", "rollback", f"rollback failed: {e}", level="error")
                return False
        return False

    # ---------------------------
    # API pública: apply_all
    # ---------------------------
    @perf_timer("patcher", "apply_all")
    def apply_all(self, recipe_path: str, target_dir: Optional[str] = None, *,
                  dry_run: bool = False,
                  use_chroot: bool = True,
                  fakeroot: bool = False,
                  parallel: bool = False) -> Dict[str,Any]:
        """
        Aplica todos os patches definidos na receita.
        recipe_path: caminho para o .toml/.yaml com campo 'patches'
        target_dir: diretório alvo da aplicação (por default recipe.build.directory / current dir)
        """
        res_report = {"recipe": recipe_path, "results": [], "ok": False, "errors": []}
        try:
            recipe = load_recipe(recipe_path)
            # recipe base path para resolver patches relativos / series
            recipe_base = Path(recipe_path).parent
            recipe["_recipe_base"] = str(recipe_base)
            patches = self._collect_patches_from_recipe(recipe)
            if not patches:
                log_event("patcher", "apply_all", "no patches defined", level="debug")
                res_report["ok"] = True
                return res_report

            # determine target dir
            if target_dir:
                tgt = Path(target_dir)
            else:
                # try recipe build.directory or current
                tgt = Path(recipe.get("build", {}).get("directory") or ".").resolve()

            # run global pre-hooks
            self._run_global_hooks("pre", use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
            # run recipe pre-hooks
            self._run_recipe_hooks(recipe.get("hooks"), "pre_patch", target_dir=tgt, use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)

            # snapshot DB before operations
            snap = self._snapshot_db()

            # helper to apply one spec and record result
            def _worker(spec):
                try:
                    # inject recipe_base into spec for resolving relative paths
                    spec["recipe_base"] = str(recipe_base)
                    r = self._apply_single_patch(spec, tgt, use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
                    return r
                except Exception as e:
                    return {"spec": spec, "ok": False, "error": str(e)}

            # apply sequential or parallel
            results = []
            if parallel:
                # Use ThreadPoolExecutor: careful — patches often are order-dependent.
                max_workers = min(8, max(1, os.cpu_count() or 2))
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = [ex.submit(_worker, p) for p in patches]
                    for fut in concurrent.futures.as_completed(futs):
                        r = fut.result()
                        results.append(r)
                        if not r.get("ok"):
                            # if any fail and not dry_run, attempt rollback
                            log_event("patcher", "apply_all", f"patch failed: {r.get('error')}", level="error")
                            if not dry_run:
                                self._rollback_db_from_snapshot(snap) if snap else None
                                # try to run recipe post-failure hooks
                                self._run_recipe_hooks(recipe.get("hooks"), "on_patch_failure", target_dir=tgt, use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
                                res_report["errors"].append(r.get("error"))
                                res_report["results"] = results
                                res_report["ok"] = False
                                return res_report
            else:
                # sequential — recommended
                for p in patches:
                    r = _worker(p)
                    results.append(r)
                    if not r.get("ok"):
                        log_event("patcher", "apply_all", f"patch failed: {r.get('error')}", level="error")
                        if not dry_run:
                            # rollback db snapshot
                            self._rollback_db_from_snapshot(snap) if snap else None
                            self._run_recipe_hooks(recipe.get("hooks"), "on_patch_failure", target_dir=tgt, use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
                            res_report["errors"].append(r.get("error"))
                            res_report["results"] = results
                            res_report["ok"] = False
                            return res_report

            # run recipe post-hooks
            self._run_recipe_hooks(recipe.get("hooks"), "post_patch", target_dir=tgt, use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)
            # run global post-hooks
            self._run_global_hooks("post", use_chroot=use_chroot, fakeroot=fakeroot, dry_run=dry_run)

            # optional vulnerability scan
            if VULN_AVAILABLE and self.vuln and not dry_run:
                try:
                    scan_res = self.vuln.scan_package(recipe.get("package", {}).get("name"))
                    if scan_res and scan_res.get("critical"):
                        # if scan found critical new vuln, consider rollback if desired (policy)
                        log_event("patcher", "vuln", f"vuln scan found critical issues: {scan_res}", level="warning")
                        res_report["vuln_scan"] = scan_res
                except Exception as e:
                    log_event("patcher", "vuln", f"vuln scan failed: {e}", level="warning")

            res_report["results"] = results
            res_report["ok"] = all(r.get("ok") for r in results)
            # if all ok, record patch event in DB
            if DB_AVAILABLE and self.db and not dry_run:
                try:
                    self.db._execute("INSERT INTO events (ts, type, pkg, payload) VALUES (?, 'patches_applied', ?, ?)",
                                     (int(time.time()), recipe.get("package", {}).get("name"), json.dumps(results)), commit=True)
                except Exception:
                    pass

            return res_report
        except Exception as e:
            log_event("patcher", "apply_all", f"exception: {e}", level="error")
            res_report["errors"].append(str(e))
            res_report["ok"] = False
            return res_report

# -------------------------
# Helpers de tempo e compatibilidade
# -------------------------
def _now_ts():
    return int(time.time())

# -------------------------
# CLI mínimo para testes
# -------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-patcher", description="Zeropkg patcher utility")
    parser.add_argument("recipe", help="Path to recipe TOML/YAML that defines patches")
    parser.add_argument("--target", "-t", help="Target directory to apply patches (default: recipe build dir or cwd)")
    parser.add_argument("--no-chroot", dest="use_chroot", action="store_false", help="Do not use chroot for hooks/operations")
    parser.add_argument("--fakeroot", action="store_true", help="Use fakeroot where available")
    parser.add_argument("--dry-run", action="store_true", help="Do not actually apply patches")
    parser.add_argument("--parallel", action="store_true", help="Apply patches in parallel (use with care)")
    args = parser.parse_args()

    p = ZeropkgPatcher()
    out = p.apply_all(args.recipe, target_dir=args.target, dry_run=args.dry_run, use_chroot=args.use_chroot, fakeroot=args.fakeroot, parallel=args.parallel)
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    _cli()
