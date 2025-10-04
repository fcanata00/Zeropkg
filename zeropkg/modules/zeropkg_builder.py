#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_builder.py — Builder integrado para Zeropkg (ATUALIZADO)
Integração reforçada com zeropkg_installer (install_from_staging / Installer),
zeropkg_deps (resolver dependências), zeropkg_chroot (prepare/cleanup/run),
zeropkg_downloader, zeropkg_patcher, zeropkg_db, zeropkg_logger, zeropkg_toml.

Substitua a versão antiga por esta.
"""

from __future__ import annotations
import os
import sys
import json
import shutil
import subprocess
import tempfile
import tarfile
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# -------------------------
# Safe imports of project modules (graceful fallback)
# -------------------------
def _safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

config_mod = _safe_import("zeropkg_config")
toml_mod = _safe_import("zeropkg_toml")
downloader_mod = _safe_import("zeropkg_downloader")
patcher_mod = _safe_import("zeropkg_patcher")
chroot_mod = _safe_import("zeropkg_chroot")
installer_mod = _safe_import("zeropkg_installer")
db_mod = _safe_import("zeropkg_db")
logger_mod = _safe_import("zeropkg_logger")
deps_mod = _safe_import("zeropkg_deps")

# Logger helper
def _log(evt: str, msg: str, level: str = "INFO", metadata: Optional[Dict[str,Any]] = None):
    if logger_mod and hasattr(logger_mod, "log_event"):
        try:
            logger_mod.log_event(evt, msg, level=level, metadata=metadata)
            return
        except Exception:
            pass
    out = f"[{level}] {evt}: {msg}"
    if level == "ERROR":
        print(out, file=sys.stderr)
    else:
        print(out)

# Perf decorator fallback
def _perf_timer(func):
    if logger_mod and hasattr(logger_mod, "perf_timer"):
        return logger_mod.perf_timer(func)
    else:
        from functools import wraps
        import time as _time
        @wraps(func)
        def wrapper(*a, **k):
            start = _time.time()
            res = func(*a, **k)
            dur = _time.time() - start
            _log("perf", f"{func.__name__} executed in {dur:.2f}s", level="PERF", metadata={"duration": dur})
            return res
        return wrapper

# Downloader class fallback wrapper
DownloaderClass = None
if downloader_mod and hasattr(downloader_mod, "Downloader"):
    DownloaderClass = downloader_mod.Downloader

# Installer interfaces detection
InstallerClass = None
installer_install_from_staging = None
if installer_mod:
    if hasattr(installer_mod, "Installer"):
        try:
            InstallerClass = installer_mod.Installer
        except Exception:
            InstallerClass = None
    if hasattr(installer_mod, "install_from_staging"):
        installer_install_from_staging = installer_mod.install_from_staging

# toml loader
def _load_recipe(path: str):
    if toml_mod and hasattr(toml_mod, "load_recipe"):
        return toml_mod.load_recipe(path)
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"_meta": {"path": str(p)}, "package": {"name": p.stem, "version": "0.0"}, "build": {"commands": []}}

# patcher apply function fallback
def _apply_patches(recipe_spec: dict, workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
    if patcher_mod and hasattr(patcher_mod, "apply_patches"):
        try:
            return patcher_mod.apply_patches(recipe_spec, workdir, dry_run=dry_run)
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": True, "applied": []}

# chroot helpers fallback
def _prepare_chroot(profile: Optional[str], root: Optional[str], workdir: Optional[str]) -> Dict[str,Any]:
    if chroot_mod and hasattr(chroot_mod, "prepare_chroot"):
        try:
            return chroot_mod.prepare_chroot(profile=profile, root=root, workdir=workdir)
        except Exception as e:
            _log("chroot", f"prepare_chroot failed: {e}", level="WARNING")
            return {"ok": False, "error": str(e)}
    return {"ok": False, "fallback": "host_mode"}

def _cleanup_chroot(profile: Optional[str], root: Optional[str], workdir: Optional[str]) -> Dict[str,Any]:
    if chroot_mod and hasattr(chroot_mod, "cleanup_chroot"):
        try:
            return chroot_mod.cleanup_chroot(profile=profile, root=root, workdir=workdir)
        except Exception as e:
            _log("chroot", f"cleanup_chroot failed: {e}", level="WARNING")
            return {"ok": False, "error": str(e)}
    return {"ok": False, "fallback": "host_mode_cleanup"}

def _run_in_chroot(cmd: List[str], chroot_ctx: Dict[str,Any], cwd: Optional[str]=None, env: Optional[Dict[str,str]]=None, dry_run: bool=False) -> Dict[str,Any]:
    if chroot_mod:
        if hasattr(chroot_mod, "run_in_chroot"):
            try:
                return chroot_mod.run_in_chroot(cmd, chroot_ctx, cwd=cwd, env=env, dry_run=dry_run)
            except Exception as e:
                _log("chroot", f"run_in_chroot failed: {e}", level="WARNING")
        if hasattr(chroot_mod, "exec_in_chroot"):
            try:
                root = chroot_ctx.get("root") if isinstance(chroot_ctx, dict) else None
                return chroot_mod.exec_in_chroot(cmd, root, cwd=cwd, env=env, dry_run=dry_run)
            except Exception as e:
                _log("chroot", f"exec_in_chroot failed: {e}", level="WARNING")
    # fallback to local
    try:
        if dry_run:
            _log("cmd", f"[dry-run] {' '.join(cmd)}", level="INFO")
            return {"ok": True, "cmd": cmd, "dry_run": True}
        proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return {"ok": proc.returncode == 0, "rc": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# DB record helper
def _record_install_quick(name: str, version: str, manifest: Dict[str,Any], files: List[Dict[str,Any]], deps: List[str]=None):
    if db_mod and hasattr(db_mod, "record_install_quick"):
        try:
            return db_mod.record_install_quick(name, version, manifest, files, deps)
        except Exception as e:
            _log("db", f"record_install_quick failed: {e}", level="ERROR", metadata={"pkg": name})
            return {"ok": False, "error": str(e)}
    try:
        state_dir = Path("/var/lib/zeropkg")
        state_dir.mkdir(parents=True, exist_ok=True)
        meta_path = state_dir / f"{name}.installed.json"
        meta = {"name": name, "version": version, "manifest": manifest, "files": files, "deps": deps or []}
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"ok": True, "note": "written_to_state_dir", "path": str(meta_path)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# installer call wrapper (uses Installer class or function exposed by installer_mod)
def _installer_install_from_archive(archive_path: Path, target_root: str = "/", fakeroot: bool=False) -> Dict[str,Any]:
    """
    Install from a packaged archive (tar.gz / tar.zst) using installer_mod if available.
    """
    if installer_mod:
        # prefer high-level functions
        try:
            if InstallerClass:
                inst = InstallerClass()
                if hasattr(inst, "install_from_archive"):
                    return inst.install_from_archive(str(archive_path), root=target_root, fakeroot=fakeroot)
                if hasattr(inst, "install_from_staging_archive"):
                    return inst.install_from_staging_archive(str(archive_path), root=target_root, fakeroot=fakeroot)
            if installer_install_from_staging and hasattr(installer_mod, "install_from_archive"):
                return installer_mod.install_from_archive(str(archive_path), root=target_root, fakeroot=fakeroot)
        except Exception as e:
            return {"ok": False, "error": str(e)}
    # fallback: extract and copy
    try:
        tmp_unpack = Path(tempfile.mkdtemp(prefix="zeropkg-install-"))
        # smart extract supporting zst if python module installed, else tarfile
        try:
            import zstandard as zstd  # optional
            # fallback path: use system tar if necessary
            with tarfile.open(str(archive_path), "r:*") as tf:
                tf.extractall(str(tmp_unpack))
        except Exception:
            with tarfile.open(str(archive_path), "r:*") as tf:
                tf.extractall(str(tmp_unpack))
        # naive copy
        for src in tmp_unpack.rglob("*"):
            rel = src.relative_to(tmp_unpack)
            dest = Path(target_root) / rel
            if src.is_dir():
                dest.mkdir(parents=True, exist_ok=True)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(src), str(dest))
        shutil.rmtree(str(tmp_unpack), ignore_errors=True)
        return {"ok": True, "method": "fallback_extract_copy"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _installer_install_from_staging_dir(staging_dir: Path, target_root: str = "/", fakeroot: bool=False) -> Dict[str,Any]:
    """
    Prefer installer_mod.Installer.install_from_staging or function installer_mod.install_from_staging
    """
    if installer_mod:
        try:
            if InstallerClass:
                inst = InstallerClass()
                if hasattr(inst, "install_from_staging"):
                    return inst.install_from_staging(str(staging_dir), root=target_root, fakeroot=fakeroot)
            if installer_install_from_staging:
                return installer_install_from_staging(str(staging_dir), root=target_root, fakeroot=fakeroot)
        except Exception as e:
            return {"ok": False, "error": str(e)}
    # fallback: naive copy
    try:
        for src in staging_dir.rglob("*"):
            rel = src.relative_to(staging_dir)
            dest = Path(target_root) / rel
            if src.is_dir():
                dest.mkdir(parents=True, exist_ok=True)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(src), str(dest))
        return {"ok": True, "method": "fallback_copy"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# -------------------------
# Builder core
# -------------------------
class ZeropkgBuilder:
    def __init__(self, config: Optional[dict] = None):
        # config resolution
        self.config = config or {}
        if not self.config and config_mod and hasattr(config_mod, "get_config_manager"):
            try:
                mgr = config_mod.get_config_manager()
                self.config = mgr.config
            except Exception:
                self.config = {}
        self.downloader = None
        if DownloaderClass:
            try:
                ddist = Path(self.config.get("paths",{}).get("distfiles_dir","/usr/ports/distfiles"))
                self.downloader = DownloaderClass(distdir=ddist)
            except Exception:
                self.downloader = None
        self.tmpdir_base = Path(self.config.get("paths",{}).get("state_dir","/var/lib/zeropkg")) / "builds"
        self.tmpdir_base.mkdir(parents=True, exist_ok=True)
        self.chroot_profile_default = self.config.get("chroot",{}).get("default_profile", "lfs")

    def _mk_build_dir(self, name: str) -> Path:
        t = tempfile.mkdtemp(prefix=f"zeropkg-build-{name}-", dir=str(self.tmpdir_base))
        return Path(t)

    def _pack_staging(self, staging_dir: Path, out_dir: Optional[Path]=None) -> Path:
        out_dir = Path(out_dir or (self.tmpdir_base / "packages"))
        out_dir.mkdir(parents=True, exist_ok=True)
        pkg_name = f"{staging_dir.name}.tar.gz"
        out_path = out_dir / pkg_name
        with tarfile.open(out_path, "w:gz") as tf:
            tf.add(str(staging_dir), arcname=".")
        return out_path

    def _collect_installed_files_manifest(self, staging_dir: Path) -> List[Dict[str,Any]]:
        files = []
        for p in staging_dir.rglob("*"):
            if p.is_file():
                stat = p.stat()
                import hashlib
                h = hashlib.sha256()
                with open(p, "rb") as fh:
                    for chunk in iter(lambda: fh.read(1024*1024), b""):
                        h.update(chunk)
                files.append({
                    "path": str(p.relative_to(staging_dir)),
                    "size": stat.st_size,
                    "mode": stat.st_mode,
                    "uid": stat.st_uid,
                    "gid": stat.st_gid,
                    "sha256": h.hexdigest()
                })
        return files

    # -------------------------
    # Dependency resolution
    # -------------------------
    def resolve_dependencies(self, spec: Dict[str,Any]) -> Dict[str,Any]:
        """
        Attempt to resolve dependencies for the spec using zeropkg_deps if available.
        Returns {'ok': True, 'order': [pkg1,pkg2], 'missing': []} or a best-effort dict.
        """
        if deps_mod:
            try:
                if hasattr(deps_mod, "resolve_and_order"):
                    return deps_mod.resolve_and_order(spec)
                if hasattr(deps_mod, "build_graph") and hasattr(deps_mod, "topological_order_for"):
                    deps = deps_mod.build_graph()
                    order = deps_mod.topological_order_for(spec.get("name"), deps)
                    return {"ok": True, "order": order, "missing": []}
            except Exception as e:
                _log("deps", f"deps resolve failed: {e}", level="WARNING")
                return {"ok": False, "error": str(e)}
        # fallback: no resolution
        return {"ok": False, "error": "deps_module_missing"}

    # -------------------------
    # High level build steps (fetch/extract/patch/build/stage)
    # -------------------------
    @_perf_timer
    def fetch_sources(self, spec: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        res = {"ok": True, "fetched": [], "errors": []}
        sources = spec.get("sources", [])
        if not sources:
            _log("fetch", "no sources defined in spec", level="WARNING", metadata={"spec": spec.get("name")})
            return res
        for s in sources:
            url = s.get("url") or s.get("path")
            filename = s.get("filename")
            checksums = s.get("checksums") or s.get("hashes") or None
            mirrors = s.get("mirrors") or None
            try:
                if self.downloader:
                    dres = self.downloader.fetch(url, dest_dir=workdir / "distfiles", filename=filename, checksums=checksums, mirrors=mirrors, dry_run=dry_run)
                    if not dres.get("ok"):
                        res["errors"].append({"url": url, "error": dres.get("error")})
                    else:
                        res["fetched"].append({"url": url, "path": dres.get("path"), "action": dres.get("action")})
                else:
                    p = Path(url)
                    if p.exists():
                        dest = (workdir / "distfiles") / p.name
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        if not dry_run:
                            shutil.copy2(str(p), str(dest))
                        res["fetched"].append({"url": url, "path": str(dest)})
                    else:
                        res["errors"].append({"url": url, "error": "downloader-not-available-or-source-missing"})
            except Exception as e:
                res["errors"].append({"url": url, "error": str(e)})
        if res["errors"]:
            res["ok"] = False
        return res

    @_perf_timer
    def extract_sources(self, fetched: List[Dict[str,Any]], workdir: Path, extract_to: Optional[str]=None, dry_run: bool=False) -> Dict[str,Any]:
        res = {"ok": True, "extracted": [], "errors": []}
        target = workdir / (extract_to or "src")
        target.mkdir(parents=True, exist_ok=True)
        for f in fetched:
            path = f.get("path")
            if not path:
                res["errors"].append({"path": path, "error": "no-path"})
                continue
            p = Path(path)
            try:
                if dry_run:
                    _log("extract", f"[dry-run] would extract {p}", level="INFO")
                    res["extracted"].append(str(p))
                    continue
                if self.downloader and hasattr(self.downloader, "extract_to"):
                    er = self.downloader.extract_to(p, target, strip_components=0, dry_run=dry_run)
                    if not er.get("ok"):
                        shutil.unpack_archive(str(p), str(target))
                        res["extracted"].append(str(p))
                    else:
                        res["extracted"].append(str(p))
                else:
                    try:
                        shutil.unpack_archive(str(p), str(target))
                        res["extracted"].append(str(p))
                    except Exception:
                        import tarfile, zipfile
                        if tarfile.is_tarfile(str(p)):
                            with tarfile.open(str(p), "r:*") as tf:
                                tf.extractall(str(target))
                            res["extracted"].append(str(p))
                        elif zipfile.is_zipfile(str(p)):
                            with zipfile.ZipFile(str(p), "r") as zf:
                                zf.extractall(str(target))
                            res["extracted"].append(str(p))
                        else:
                            res["errors"].append({"path": str(p), "error": "unknown-archive"})
            except Exception as e:
                res["errors"].append({"path": str(p), "error": str(e)})
        if res["errors"]:
            res["ok"] = False
        return res

    @_perf_timer
    def apply_patches(self, spec: Dict[str,Any], srcdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        try:
            return _apply_patches(spec, srcdir, dry_run=dry_run)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @_perf_timer
    def run_build_commands(self, spec: Dict[str,Any], srcdir: Path, chroot_ctx: Optional[Dict[str,Any]]=None, use_chroot: bool=True, fakeroot: bool=False, dry_run: bool=False) -> Dict[str,Any]:
        b = spec.get("build", {}) or {}
        commands = b.get("commands", []) or []
        build_dir = b.get("directory") or str(srcdir)
        env = dict(os.environ)
        env.update(spec.get("build", {}).get("env", {} or {}))
        results = {"ok": True, "commands": []}
        for cmd in commands:
            cmd_list = ["sh", "-c", cmd]
            if fakeroot and shutil.which("fakeroot"):
                cmd_list = ["fakeroot"] + cmd_list
            try:
                if use_chroot and chroot_ctx:
                    r = _run_in_chroot(cmd_list, chroot_ctx, cwd=build_dir, env=env, dry_run=dry_run)
                else:
                    if dry_run:
                        _log("cmd", f"[dry-run] {cmd}", level="INFO")
                        r = {"ok": True, "cmd": cmd, "dry_run": True}
                    else:
                        proc = subprocess.run(cmd_list, cwd=build_dir, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        r = {"ok": proc.returncode == 0, "rc": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
                results["commands"].append({"cmd": cmd, "result": r})
                if not r.get("ok"):
                    results["ok"] = False
                    break
            except Exception as e:
                results["commands"].append({"cmd": cmd, "error": str(e)})
                results["ok"] = False
                break
        return results

    @_perf_timer
    def stage_install(self, spec: Dict[str,Any], build_dir: Path, staging_dir: Path, chroot_ctx: Optional[Dict[str,Any]]=None, use_chroot: bool=True, fakeroot: bool=False, dry_run: bool=False) -> Dict[str,Any]:
        inst = spec.get("install", {}) or {}
        commands = inst.get("commands") or []
        results = {"ok": True, "commands": []}
        if not commands:
            commands = [f"make install DESTDIR={str(staging_dir)}"]
        for cmd in commands:
            cmd_list = ["sh", "-c", cmd]
            if fakeroot and shutil.which("fakeroot"):
                cmd_list = ["fakeroot"] + cmd_list
            try:
                if use_chroot and chroot_ctx:
                    r = _run_in_chroot(cmd_list, chroot_ctx, cwd=str(build_dir), env=None, dry_run=dry_run)
                else:
                    if dry_run:
                        _log("cmd", f"[dry-run] {cmd}", level="INFO")
                        r = {"ok": True, "cmd": cmd, "dry_run": True}
                    else:
                        proc = subprocess.run(cmd_list, cwd=str(build_dir), env=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        r = {"ok": proc.returncode == 0, "rc": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
                results["commands"].append({"cmd": cmd, "result": r})
                if not r.get("ok"):
                    results["ok"] = False
                    break
            except Exception as e:
                results["commands"].append({"cmd": cmd, "error": str(e)})
                results["ok"] = False
                break
        return results

    # -------------------------
    # Top-level build flow (integrated with installer)
    # -------------------------
    def build_package(self, recipe_path: str, *,
                      use_chroot: Optional[bool] = True,
                      chroot_profile: Optional[str] = None,
                      dir_install: Optional[bool] = False,
                      staging_dir_override: Optional[str] = None,
                      fakeroot: Optional[bool] = False,
                      dry_run: bool = False,
                      install_after: bool = False,
                      install_from_cache: Optional[str] = None,
                      jobs: Optional[int] = None,
                      root_for_install: Optional[str] = "/") -> Dict[str,Any]:
        report: Dict[str,Any] = {"ok": True, "steps": [], "recipe": recipe_path}
        # load recipe
        try:
            raw = _load_recipe(recipe_path)
            spec = toml_mod.to_builder_spec(raw) if toml_mod and hasattr(toml_mod, "to_builder_spec") else raw
            name = spec.get("name") or raw.get("package",{}).get("name") or Path(recipe_path).stem
            version = spec.get("version") or raw.get("package",{}).get("version") or "0.0"
            report["pkg"] = f"{name}-{version}"
        except Exception as e:
            _log("build", f"failed to load recipe {recipe_path}: {e}", level="ERROR")
            return {"ok": False, "error": f"load_recipe: {e}"}

        # resolve dependencies first (best-effort)
        deps_res = self.resolve_dependencies(spec)
        report["deps"] = deps_res

        workdir = self._mk_build_dir(name)
        (workdir / "distfiles").mkdir(parents=True, exist_ok=True)
        (workdir / "src").mkdir(parents=True, exist_ok=True)
        report["workdir"] = str(workdir)

        chroot_profile = chroot_profile or self.chroot_profile_default
        chroot_ctx = None
        chroot_used = False
        if use_chroot:
            _log("build", f"preparing chroot profile={chroot_profile}", level="INFO")
            chprep = _prepare_chroot(profile=chroot_profile, root=None, workdir=str(workdir))
            if chprep.get("ok"):
                chroot_ctx = chprep
                chroot_used = True
                report["chroot"] = chprep
            else:
                _log("build", "chroot not available, falling back to host mode", level="WARNING")
                report["chroot"] = chprep

        # fetch sources
        fetch_res = self.fetch_sources(spec, workdir, dry_run=dry_run)
        report["steps"].append({"fetch": fetch_res})
        if not fetch_res.get("ok"):
            _log("build", f"fetch failed for {name}: {fetch_res.get('errors')}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "fetch_failed", "details": fetch_res}

        # extract
        extract_res = self.extract_sources(fetch_res.get("fetched", []), workdir, extract_to=spec.get("extract_to") or None, dry_run=dry_run)
        report["steps"].append({"extract": extract_res})
        if not extract_res.get("ok"):
            _log("build", f"extract failed for {name}: {extract_res.get('errors')}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "extract_failed", "details": extract_res}

        # determine build_dir
        src_root = workdir / (spec.get("extract_to") or "src")
        build_dir_candidate = None
        binfo = spec.get("build",{}) or {}
        if binfo.get("directory"):
            cand = Path(binfo.get("directory"))
            if not cand.is_absolute():
                cand = src_root / cand
            build_dir_candidate = cand
        else:
            entries = [p for p in src_root.iterdir() if p.is_dir()] if src_root.exists() else []
            build_dir_candidate = entries[0] if entries else src_root
        build_dir = build_dir_candidate
        report["build_dir"] = str(build_dir)

        # apply patches
        patch_res = self.apply_patches(spec, build_dir, dry_run=dry_run)
        report["steps"].append({"patch": patch_res})
        if not patch_res.get("ok"):
            _log("build", f"patching failed for {name}: {patch_res.get('error')}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "patch_failed", "details": patch_res}

        # run build commands
        run_res = self.run_build_commands(spec, build_dir, chroot_ctx=chroot_ctx, use_chroot=chroot_used, fakeroot=fakeroot, dry_run=dry_run)
        report["steps"].append({"build": run_res})
        if not run_res.get("ok"):
            _log("build", f"build commands failed for {name}", level="ERROR", metadata={"pkg": name})
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "build_failed", "details": run_res}

        # staging
        staging_dir = Path(staging_dir_override) if staging_dir_override else (workdir / "staging")
        staging_dir.mkdir(parents=True, exist_ok=True)
        stage_res = self.stage_install(spec, build_dir, staging_dir, chroot_ctx=chroot_ctx, use_chroot=chroot_used, fakeroot=fakeroot, dry_run=dry_run)
        report["steps"].append({"stage": stage_res})
        if not stage_res.get("ok"):
            _log("build", f"stage/install failed for {name}", level="ERROR", metadata={"pkg": name})
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "stage_failed", "details": stage_res}

        # package the staging tree
        try:
            pkg_archive = self._pack_staging(staging_dir, out_dir=(self.tmpdir_base / "packages"))
            report["package"] = str(pkg_archive)
        except Exception as e:
            _log("build", f"packing failed: {e}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "pack_failed", "details": str(e)}

        # collect manifest + record DB
        try:
            files_manifest = self._collect_installed_files_manifest(staging_dir)
            manifest_meta = {"recipe": recipe_path, "built_at": time.time()}
            dbres = _record_install_quick(name, version, manifest_meta, files_manifest, deps=spec.get("dependencies", []))
            report["db_record"] = dbres
        except Exception as e:
            report["db_record"] = {"ok": False, "error": str(e)}
            _log("db", f"failed to record install: {e}", level="WARNING")

        # INSTALL: prefer installer_mod if asked to install now
        if install_after and not dry_run:
            # if user provided binary cache path, prefer installer install from archive
            if install_from_cache:
                arc = Path(install_from_cache)
                if arc.exists():
                    inst_res = _installer_install_from_archive(arc, target_root=root_for_install, fakeroot=fakeroot)
                    report["install"] = inst_res
                else:
                    report["install"] = {"ok": False, "error": "install_from_cache_not_found"}
            else:
                # prefer installer_mod's API to install staging dir
                inst_res = _installer_install_from_staging_dir(staging_dir, target_root=root_for_install, fakeroot=fakeroot)
                report["install"] = inst_res
            # if install succeeded, optionally trigger depclean or other housekeeping
            if report["install"].get("ok"):
                try:
                    if deps_mod and hasattr(deps_mod, "post_install_tasks"):
                        deps_mod.post_install_tasks(name)
                except Exception:
                    pass

        # cleanup chroot if used
        if chroot_used:
            try:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            except Exception as e:
                _log("chroot", f"cleanup_chroot exception: {e}", level="WARNING")

        report["ok"] = True
        return report

# -------------------------
# CLI
# -------------------------
def _cli():
    import argparse
    p = argparse.ArgumentParser(prog="zeropkg-build", description="Zeropkg builder CLI (installer-integrated)")
    p.add_argument("-r", "--recipe", required=True, help="Path to recipe TOML")
    p.add_argument("-c", "--chroot-profile", default=None, help="Chroot profile to use (overrides config)")
    p.add_argument("--no-chroot", action="store_true", help="Do not use chroot for build")
    p.add_argument("--dir-install", action="store_true", help="Create dir install (pack staging) but do not install into /")
    p.add_argument("--staging", help="Override staging directory")
    p.add_argument("--fakeroot", action="store_true", help="Use fakeroot for install steps")
    p.add_argument("--dry-run", action="store_true", help="Dry-run — do not execute build commands")
    p.add_argument("--install-after", action="store_true", help="Install into / after build")
    p.add_argument("--install-from-cache", help="Install from provided binary archive path instead of building")
    p.add_argument("--jobs", "-j", type=int, default=None, help="Number of parallel jobs (unused here but passed to env)")
    p.add_argument("--root", default="/", help="Target root for install (default /)")
    args = p.parse_args()

    cfg_mgr = None
    try:
        if config_mod and hasattr(config_mod, "get_config_manager"):
            cfg_mgr = config_mod.get_config_manager()
    except Exception:
        cfg_mgr = None

    builder = ZeropkgBuilder(config=cfg_mgr.config if cfg_mgr else None)
    res = builder.build_package(args.recipe,
                                use_chroot=(not args.no_chroot),
                                chroot_profile=args.chroot_profile,
                                dir_install=args.dir_install,
                                staging_dir_override=args.staging,
                                fakeroot=args.fakeroot,
                                dry_run=args.dry_run,
                                install_after=args.install_after,
                                install_from_cache=args.install_from_cache,
                                jobs=args.jobs,
                                root_for_install=args.root)
    print(json.dumps(res, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
