#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_patcher.py — Aplica patches e executa hooks conforme receitas TOML
Pattern B: integrado, enxuto, funcional.

API principal:
- Patcher(build_dir: str, pkg_name: str, env: dict)
  - apply_patch(patch_entry, dry_run=False)
  - apply_patches(patches, dry_run=False)
  - apply_hooks(stage, hooks, dry_run=False, chroot_root=None, use_fakeroot=False)
  - apply_all_stages(meta, dry_run=False, chroot_root=None)
  - rollback_last_patch(patch_entry)  # best-effort
"""

from __future__ import annotations
import os
import sys
import subprocess
import shlex
import hashlib
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

# Integrations (optional)
try:
    from zeropkg_toml import resolve_macros
except Exception:
    def resolve_macros(v, env_map): return v

try:
    from zeropkg_logger import log_event, get_logger
    logger = get_logger("patcher")
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_patcher")
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(logger, level if hasattr(logger, level) else "info")(f"{pkg}:{stage} {msg}")

# optional chroot helper
try:
    from zeropkg_chroot import run_in_chroot, prepare_chroot, cleanup_chroot
except Exception:
    run_in_chroot = None
    prepare_chroot = None
    cleanup_chroot = None

# -------------------------
# Exceptions
# -------------------------
class PatchError(RuntimeError):
    pass

class HookError(RuntimeError):
    pass

# -------------------------
# Utilities
# -------------------------
def _safe_join(base: str, rel: str) -> str:
    """Join and ensure result is inside base (prevent ../ traversal)."""
    base_p = Path(base).resolve()
    candidate = (base_p / rel).resolve()
    if not str(candidate).startswith(str(base_p)):
        raise PatchError(f"Unsafe patch path traversal: {rel}")
    return str(candidate)

def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _run_shell(cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None, dry_run: bool = False, chroot_root: Optional[str] = None, use_fakeroot: bool = False) -> int:
    """Run a command locally or inside chroot. Returns exit code or raises on failure."""
    if dry_run:
        log_event("patcher", "dry-run", f"[dry-run] {cmd}", level="info")
        return 0

    if chroot_root and run_in_chroot:
        # execute inside chroot
        if use_fakeroot:
            wrapped = f"fakeroot bash -c {shlex.quote(cmd)}"
            rc = run_in_chroot(chroot_root, wrapped, cwd=cwd, env=env, use_shell=True, dry_run=False)
        else:
            rc = run_in_chroot(chroot_root, cmd, cwd=cwd, env=env, use_shell=True, dry_run=False)
        if rc != 0:
            raise PatchError(f"Command failed in chroot (rc={rc}): {cmd}")
        return rc

    if use_fakeroot:
        cmd = f"fakeroot bash -c {shlex.quote(cmd)}"
    proc = subprocess.run(cmd, shell=True, cwd=cwd, env=env)
    if proc.returncode != 0:
        raise PatchError(f"Command failed (rc={proc.returncode}): {cmd}")
    return proc.returncode

# -------------------------
# Patcher class
# -------------------------
class Patcher:
    def __init__(self, build_dir: str, pkg_name: Optional[str] = None, env: Optional[Dict[str, str]] = None):
        self.build_dir = str(Path(build_dir).resolve())
        self.pkg = pkg_name or "unknown"
        self.env = dict(os.environ)
        if env:
            self.env.update(env)
        self._applied: List[Dict[str, Any]] = []  # record of applied patches (for rollback best-effort)
        log_event(self.pkg, "patcher.init", f"Patcher initialized for {self.build_dir}")

    def apply_patch(self, patch_entry: Dict[str, Any], dry_run: bool = False, chroot_root: Optional[str] = None, use_fakeroot: bool = False, checksum: Optional[str] = None) -> None:
        """
        patch_entry: {"path": "...", "strip": 1, "applied_to": "subdir"}
        checksum (optional): expected sha256 hex to verify patch file before applying
        """
        path_raw = patch_entry.get("path")
        if not path_raw:
            raise PatchError("Patch entry missing 'path'")

        # resolve macros in path
        try:
            path_resolved = resolve_macros(path_raw, self.env)
        except Exception:
            path_resolved = path_raw

        # compute absolute patch path and ensure it's inside allowed area (but patch files commonly live in distfiles)
        patch_path = Path(path_resolved).expanduser()
        if not patch_path.is_absolute():
            # allow relative to build_dir or current working dir
            cand = Path(self.build_dir) / path_resolved
            if cand.exists():
                patch_path = cand.resolve()
            else:
                patch_path = patch_path.resolve()

        if not patch_path.exists():
            raise PatchError(f"Patch file not found: {patch_path}")

        if checksum:
            actual = _sha256_of_file(str(patch_path))
            if actual.lower() != checksum.lower():
                raise PatchError(f"Patch checksum mismatch for {patch_path}: {actual} != {checksum}")

        strip = int(patch_entry.get("strip", 1))
        applied_to = patch_entry.get("applied_to") or patch_entry.get("target") or "."
        # ensure applied_to sits inside build_dir
        target_dir = _safe_join(self.build_dir, applied_to)

        cmd = f"patch -p{strip} -i {shlex.quote(str(patch_path))}"
        log_event(self.pkg, "patch.apply", f"Applying patch {patch_path} to {target_dir}")
        # try 'patch' first, fallback to 'git apply' if patch fails (but git apply doesn't support -p)
        try:
            _run_shell(cmd, cwd=target_dir, env=self.env, dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)
        except PatchError as e:
            # fallback to git apply if available
            log_event(self.pkg, "patch.apply", f"patch failed, trying git apply fallback: {e}", level="warning")
            try:
                gcmd = f"git apply {shlex.quote(str(patch_path))}"
                _run_shell(gcmd, cwd=target_dir, env=self.env, dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)
            except PatchError as e2:
                raise PatchError(f"Both patch and git apply failed: {e2}")

        # record applied (best-effort)
        self._applied.append({"patch": str(patch_path), "target": target_dir, "time": int(time.time()), "strip": strip})

    def apply_patches(self, patches: List[Dict[str, Any]], dry_run: bool = False, chroot_root: Optional[str] = None, use_fakeroot: bool = False) -> None:
        """
        Apply a list of patch entries. Each entry is a dict as produced by zeropkg_toml.
        """
        if not patches:
            log_event(self.pkg, "patches", "No patches to apply", level="debug")
            return
        for p in patches:
            # accept either dict or string
            if isinstance(p, str):
                patch_entry = {"path": p}
            else:
                patch_entry = dict(p)
            # support checksum in patch_entry as 'checksum' or 'sha256'
            checksum = patch_entry.get("checksum") or patch_entry.get("sha256")
            try:
                self.apply_patch(patch_entry, dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot, checksum=checksum)
            except Exception as e:
                log_event(self.pkg, "patches", f"Failed to apply patch {patch_entry.get('path')}: {e}", level="error")
                raise

    def apply_hooks(self, stage: str, hooks: Any, dry_run: bool = False, chroot_root: Optional[str] = None, use_fakeroot: bool = False, timeout: Optional[int] = None) -> None:
        """
        Execute hooks for a stage. hooks may be a string or list of strings.
        Each hook is executed with macros resolved against self.env.
        """
        if not hooks:
            return
        hooks_list = hooks if isinstance(hooks, (list, tuple)) else [hooks]
        for h in hooks_list:
            cmd = resolve_macros(h, self.env) if isinstance(h, str) else str(h)
            log_event(self.pkg, f"hook.{stage}", f"Executing hook: {cmd}")
            # execute; timeout is best-effort via shell timeout if provided
            if timeout:
                cmd = f"timeout {int(timeout)} bash -lc {shlex.quote(cmd)}"
            try:
                _run_shell(cmd, cwd=self.build_dir, env=self.env, dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)
            except Exception as e:
                log_event(self.pkg, f"hook.{stage}", f"Hook failed: {e}", level="error")
                raise HookError(f"Hook '{cmd}' failed: {e}")

    def apply_all_stages(self, meta: Dict[str, Any], dry_run: bool = False, chroot_root: Optional[str] = None) -> None:
        """
        Convenience: apply patches and hooks in canonical order:
            pre_configure -> apply patches -> post_build -> pre_install -> post_install
        Reads 'patches' and 'hooks' from meta (as normalized by zeropkg_toml.load_toml).
        """
        hooks = meta.get("hooks", {}) or {}
        patches = meta.get("patches", []) or []
        build_cfg = meta.get("build", {}) or {}
        use_fakeroot = bool(build_cfg.get("fakeroot", True))

        # pre_configure hooks
        if hooks.get("pre_configure"):
            self.apply_hooks("pre_configure", hooks["pre_configure"], dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)

        # patches
        if patches:
            self.apply_patches(patches, dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)

        # post_build hooks
        if hooks.get("post_build"):
            self.apply_hooks("post_build", hooks["post_build"], dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)

        # pre_install
        if hooks.get("pre_install"):
            self.apply_hooks("pre_install", hooks["pre_install"], dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)

        # post_install
        if hooks.get("post_install"):
            self.apply_hooks("post_install", hooks["post_install"], dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)

        log_event(self.pkg, "patcher", "All patch stages applied")

    def rollback_last_patch(self, patch_entry: Optional[Dict[str, Any]] = None, dry_run: bool = False, chroot_root: Optional[str] = None, use_fakeroot: bool = False) -> None:
        """
        Best-effort rollback: tries `patch -R` with same strip level. If no entry provided, uses last applied.
        Note: rollback may fail if patch is not reversible or files have changed.
        """
        if not patch_entry:
            if not self._applied:
                log_event(self.pkg, "patcher", "No applied patch to rollback", level="debug")
                return
            patch_entry = self._applied[-1]

        patch_file = patch_entry.get("patch")
        strip = int(patch_entry.get("strip", 1))
        target = patch_entry.get("target", self.build_dir)
        if not patch_file or not os.path.exists(patch_file):
            raise PatchError(f"Cannot rollback; patch file missing: {patch_file}")

        cmd = f"patch -R -p{strip} -i {shlex.quote(str(patch_file))}"
        log_event(self.pkg, "patcher.rollback", f"Rolling back patch {patch_file} in {target}")
        try:
            _run_shell(cmd, cwd=target, env=self.env, dry_run=dry_run, chroot_root=chroot_root, use_fakeroot=use_fakeroot)
            # remove from applied list if successful
            if self._applied and self._applied[-1].get("patch") == patch_file:
                self._applied.pop()
        except Exception as e:
            log_event(self.pkg, "patcher.rollback", f"Rollback failed: {e}", level="warning")
            raise PatchError(f"Rollback failed: {e}")

# -------------------------
# Module-level convenience
# -------------------------
def apply_all_stages_from_meta(build_dir: str, meta: Dict[str, Any], dry_run: bool = False, chroot_root: Optional[str] = None):
    p = Patcher(build_dir, pkg_name=(meta.get("package") or {}).get("name"), env=meta.get("environment"))
    p.apply_all_stages(meta, dry_run=dry_run, chroot_root=chroot_root)
    return p

# -------------------------
# CLI / test helper
# -------------------------
if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser(prog="zeropkg-patcher", description="Apply patches and hooks for a recipe")
    parser.add_argument("build_dir", help="build directory root")
    parser.add_argument("--meta", help="recipe meta JSON (dump from zeropkg_toml.load_toml)", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chroot", help="optional chroot root to run hooks/patches inside")
    args = parser.parse_args()

    meta = {}
    with open(args.meta, "r", encoding="utf-8") as f:
        meta = json.load(f)
    p = Patcher(args.build_dir, pkg_name=(meta.get("package") or {}).get("name"), env=meta.get("environment"))
    try:
        p.apply_all_stages(meta, dry_run=args.dry_run, chroot_root=args.chroot)
        print("Patches/hooks applied.")
    except Exception as e:
        print("Failed:", e)
        sys.exit(1)
