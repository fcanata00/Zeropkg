#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_builder.py — Builder integrado para Zeropkg

Integrações/expectativas:
 - zeropkg_config.get_config_manager() or get_config_manager()
 - zeropkg_toml.load_recipe / to_builder_spec
 - zeropkg_downloader.Downloader
 - zeropkg_patcher (apply_patches)
 - zeropkg_chroot (prepare_chroot, cleanup_chroot, run_in_chroot, exec_in_chroot)
 - zeropkg_installer (Installer class or install_from_staging function)
 - zeropkg_db.record_install_quick
 - zeropkg_logger.log_event / perf_timer

Este módulo tenta usar cada integração via import seguro e tem fallbacks quando um módulo não existir.
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

# Logger fallback
def _log(evt: str, msg: str, level: str = "INFO", metadata: Optional[Dict[str,Any]]=None):
    if logger_mod and hasattr(logger_mod, "log_event"):
        try:
            logger_mod.log_event(evt, msg, level=level, metadata=metadata)
            return
        except Exception:
            pass
    # fallback to stderr
    if level == "ERROR":
        print(f"[{level}] {evt}: {msg}", file=sys.stderr)
    else:
        print(f"[{level}] {evt}: {msg}", file=sys.stdout)

# Perf decorator fallback
def _perf_timer(func):
    if logger_mod and hasattr(logger_mod, "perf_timer"):
        return logger_mod.perf_timer(func)
    else:
        # passthrough
        from functools import wraps
        @wraps(func)
        def wrapper(*a, **k):
            start = time.time()
            res = func(*a, **k)
            dur = time.time() - start
            _log("perf", f"{func.__name__} executed in {dur:.2f}s", level="PERF", metadata={"duration": dur})
            return res
        return wrapper

# Downloader class fallback wrapper
DownloaderClass = None
if downloader_mod and hasattr(downloader_mod, "Downloader"):
    DownloaderClass = downloader_mod.Downloader

# toml loader
def _load_recipe(path: str):
    if toml_mod and hasattr(toml_mod, "load_recipe"):
        return toml_mod.load_recipe(path)
    # fallback: try reading JSON/TOML minimal parse
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    try:
        # attempt JSON
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        # last resort, return dict with minimal info
        return {"_meta": {"path": str(p)}, "package": {"name": p.stem, "version": "0.0"}, "build": {"commands": []}}

# patcher apply function fallback
def _apply_patches(recipe_spec: dict, workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
    if patcher_mod and hasattr(patcher_mod, "apply_patches"):
        try:
            return patcher_mod.apply_patches(recipe_spec, workdir, dry_run=dry_run)
        except Exception as e:
            return {"ok": False, "error": str(e)}
    # fallback: no patches applied
    return {"ok": True, "applied": []}

# installer fallback
def _installer_install_from_staging(staging_dir: Path, target_root: str = "/", fakeroot: bool=False) -> Dict[str,Any]:
    if installer_mod:
        try:
            # try common interfaces
            if hasattr(installer_mod, "Installer"):
                inst = installer_mod.Installer()
                return inst.install_from_staging(str(staging_dir), root=target_root, fakeroot=fakeroot)
            if hasattr(installer_mod, "install_from_staging"):
                return installer_mod.install_from_staging(str(staging_dir), root=target_root, fakeroot=fakeroot)
        except Exception as e:
            return {"ok": False, "error": str(e)}
    # fallback: naive copy (requires root)
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

# DB record fallback
def _record_install_quick(name: str, version: str, manifest: Dict[str,Any], files: List[Dict[str,Any]], deps: List[str]=None):
    if db_mod and hasattr(db_mod, "record_install_quick"):
        try:
            return db_mod.record_install_quick(name, version, manifest, files, deps)
        except Exception as e:
            _log("db", f"record_install_quick failed: {e}", level="ERROR", metadata={"pkg": name})
            return {"ok": False, "error": str(e)}
    # fallback: write small metadata in /var/lib/zeropkg/installed/<name>.json
    try:
        state_dir = Path("/var/lib/zeropkg")
        state_dir.mkdir(parents=True, exist_ok=True)
        meta_path = state_dir / f"{name}.installed.json"
        meta = {"name": name, "version": version, "manifest": manifest, "files": files, "deps": deps or []}
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"ok": True, "note": "written_to_state_dir", "path": str(meta_path)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# chroot helpers fallback
def _prepare_chroot(profile: Optional[str], root: Optional[str], workdir: Optional[str]) -> Dict[str,Any]:
    if chroot_mod and hasattr(chroot_mod, "prepare_chroot"):
        try:
            return chroot_mod.prepare_chroot(profile=profile, root=root, workdir=workdir)
        except Exception as e:
            _log("chroot", f"prepare_chroot failed: {e}", level="WARNING")
            return {"ok": False, "error": str(e)}
    # fallback: no chroot created (host mode)
    return {"ok": False, "fallback": "host_mode"}

def _cleanup_chroot(profile: Optional[str], root: Optional[str], workdir: Optional[str]) -> Dict[str,Any]:
    if chroot_mod and hasattr(chroot_mod, "cleanup_chroot"):
        try:
            return chroot_mod.cleanup_chroot(profile=profile, root=root, workdir=workdir)
        except Exception as e:
            _log("chroot", f"cleanup_chroot failed: {e}", level="WARNING")
            return {"ok": False, "error": str(e)}
    return {"ok": False, "fallback": "host_mode_cleanup"}

def _run_cmd_local(cmd: List[str], cwd: Optional[str]=None, env: Optional[Dict[str,str]]=None, dry_run: bool=False) -> Dict[str,Any]:
    """Run a command in the local host environment (fallback)"""
    try:
        if dry_run:
            _log("cmd", f"[dry-run] {' '.join(cmd)}", level="INFO")
            return {"ok": True, "cmd": cmd, "dry_run": True}
        proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        ok = proc.returncode == 0
        return {"ok": ok, "rc": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _run_in_chroot(cmd: List[str], chroot_ctx: Dict[str,Any], cwd: Optional[str]=None, env: Optional[Dict[str,str]]=None, dry_run: bool=False) -> Dict[str,Any]:
    """
    Prefer chroot_mod.run_in_chroot / exec_in_chroot if available, else fallback to local run.
    chroot_ctx is whatever _prepare_chroot returned.
    """
    if chroot_mod:
        if hasattr(chroot_mod, "run_in_chroot"):
            try:
                return chroot_mod.run_in_chroot(cmd, chroot_ctx, cwd=cwd, env=env, dry_run=dry_run)
            except Exception as e:
                _log("chroot", f"run_in_chroot failed: {e}", level="WARNING")
        if hasattr(chroot_mod, "exec_in_chroot"):
            try:
                return chroot_mod.exec_in_chroot(cmd, chroot_ctx.get("root") if isinstance(chroot_ctx, dict) else None, cwd=cwd, env=env, dry_run=dry_run)
            except Exception as e:
                _log("chroot", f"exec_in_chroot failed: {e}", level="WARNING")
    # fallback to local
    return _run_cmd_local(cmd, cwd=cwd, env=env, dry_run=dry_run)

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
        # downloader instance
        self.downloader = DownloaderClass(distdir=Path(self.config.get("paths",{}).get("distfiles_dir","/usr/ports/distfiles"))) if DownloaderClass else None
        self.default_profile = (self.config.get("_active_profile") if self.config.get("_active_profile") else self.config.get("profiles",{}).get("default",{}))
        self.chroot_profile_default = self.config.get("chroot",{}).get("default_profile", "lfs")
        self.tmpdir_base = Path(self.config.get("paths",{}).get("state_dir","/var/lib/zeropkg")) / "builds"
        self.tmpdir_base.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Utility funcs
    # -------------------------
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
    # High level build steps
    # -------------------------
    @_perf_timer
    def fetch_sources(self, spec: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        """
        Use downloader to fetch all sources listed in spec['sources'].
        Places files in workdir/distfiles (or config distfiles).
        Returns mapping of source->local path.
        """
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
                    # fallback: copy local path if path exists
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
        """
        Extract sources into workdir/src (or specified extract_to)
        """
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
                # attempt to use downloader.extract_to if available
                if self.downloader and hasattr(self.downloader, "extract_to"):
                    er = self.downloader.extract_to(p, target, strip_components=0, dry_run=dry_run)
                    if not er.get("ok"):
                        # fallback to shutil.unpack_archive
                        shutil.unpack_archive(str(p), str(target))
                        res["extracted"].append(str(p))
                    else:
                        res["extracted"].append(str(p))
                else:
                    # use shutil.unpack_archive
                    try:
                        shutil.unpack_archive(str(p), str(target))
                        res["extracted"].append(str(p))
                    except Exception:
                        # try tarfile fallback
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
        """
        Apply patches defined in spec['patches'] using patcher module or fallback.
        """
        try:
            return _apply_patches(spec, srcdir, dry_run=dry_run)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @_perf_timer
    def run_build_commands(self, spec: Dict[str,Any], srcdir: Path, chroot_ctx: Optional[Dict[str,Any]]=None, use_chroot: bool=True, fakeroot: bool=False, dry_run: bool=False) -> Dict[str,Any]:
        """
        Run build commands inside chroot if available, else on host.
        spec['build']['commands'] is expected to be a list of shell commands.
        """
        b = spec.get("build", {}) or {}
        commands = b.get("commands", []) or []
        build_dir = b.get("directory") or str(srcdir)
        env = dict(os.environ)
        env.update(spec.get("build", {}).get("env", {} or {}))
        results = {"ok": True, "commands": []}
        for cmd in commands:
            # support string commands (shell)
            cmd_list = ["sh", "-c", cmd]
            # prefix fakeroot if requested and available on system (simple)
            if fakeroot:
                # prefer fakeroot binary if present
                if shutil.which("fakeroot"):
                    cmd_list = ["fakeroot"] + cmd_list
            try:
                if use_chroot and chroot_ctx:
                    r = _run_in_chroot(cmd_list, chroot_ctx, cwd=build_dir, env=env, dry_run=dry_run)
                else:
                    r = _run_cmd_local(cmd_list, cwd=build_dir, env=env, dry_run=dry_run)
                results["commands"].append({"cmd": cmd, "result": r})
                if not r.get("ok"):
                    results["ok"] = False
                    # stop on first failure
                    break
            except Exception as e:
                results["commands"].append({"cmd": cmd, "error": str(e)})
                results["ok"] = False
                break
        return results

    @_perf_timer
    def stage_install(self, spec: Dict[str,Any], build_dir: Path, staging_dir: Path, chroot_ctx: Optional[Dict[str,Any]]=None, use_chroot: bool=True, fakeroot: bool=False, dry_run: bool=False) -> Dict[str,Any]:
        """
        Run installation commands into staging_dir (DESTDIR-like) — installer may expect this.
        We expect spec['install']['commands'] or standard 'make install DESTDIR=...'
        """
        inst = spec.get("install", {}) or {}
        commands = inst.get("commands") or []
        results = {"ok": True, "commands": []}
        if not commands:
            # default: try 'make install DESTDIR=staging_dir'
            cmd = f"make install DESTDIR={str(staging_dir)}"
            commands = [cmd]
        for cmd in commands:
            cmd_list = ["sh", "-c", cmd]
            if fakeroot:
                if shutil.which("fakeroot"):
                    cmd_list = ["fakeroot"] + cmd_list
            try:
                if use_chroot and chroot_ctx:
                    r = _run_in_chroot(cmd_list, chroot_ctx, cwd=str(build_dir), env=None, dry_run=dry_run)
                else:
                    r = _run_cmd_local(cmd_list, cwd=str(build_dir), env=None, dry_run=dry_run)
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
    # Top-level build flow
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
        """
        Build a package from a recipe path (TOML). Returns build report.
        Steps:
         - load recipe (toml_mod.load_recipe)
         - to_builder_spec()
         - fetch_sources -> extract -> apply_patches -> prepare_chroot -> run_build_commands -> stage_install -> pack -> record_db -> optionally install
        """
        report: Dict[str,Any] = {"ok": True, "steps": [], "recipe": recipe_path}
        # 1) load recipe
        try:
            raw = _load_recipe(recipe_path)
            spec = toml_mod.to_builder_spec(raw) if toml_mod and hasattr(toml_mod, "to_builder_spec") else raw
            name = spec.get("name") or raw.get("package",{}).get("name") or Path(recipe_path).stem
            version = spec.get("version") or raw.get("package",{}).get("version") or "0.0"
            report["pkg"] = f"{name}-{version}"
        except Exception as e:
            _log("build", f"failed to load recipe {recipe_path}: {e}", level="ERROR")
            return {"ok": False, "error": f"load_recipe: {e}"}

        workdir = self._mk_build_dir(name)
        (workdir / "distfiles").mkdir(parents=True, exist_ok=True)
        (workdir / "src").mkdir(parents=True, exist_ok=True)
        report["workdir"] = str(workdir)

        # choose chroot profile
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

        # 2) fetch sources
        fetch_res = self.fetch_sources(spec, workdir, dry_run=dry_run)
        report["steps"].append({"fetch": fetch_res})
        if not fetch_res.get("ok"):
            _log("build", f"fetch failed for {name}: {fetch_res.get('errors')}", level="ERROR")
            # cleanup chroot if created
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "fetch_failed", "details": fetch_res}

        # 3) extract
        extract_res = self.extract_sources(fetch_res.get("fetched", []), workdir, extract_to=spec.get("extract_to") or None, dry_run=dry_run)
        report["steps"].append({"extract": extract_res})
        if not extract_res.get("ok"):
            _log("build", f"extract failed for {name}: {extract_res.get('errors')}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "extract_failed", "details": extract_res}

        # determine srcdir - try spec.build.directory relative to workdir/src
        src_root = workdir / (spec.get("extract_to") or "src")
        build_dir_candidate = None
        binfo = spec.get("build",{}) or {}
        if binfo.get("directory"):
            cand = Path(binfo.get("directory"))
            if not cand.is_absolute():
                # relative to src_root
                cand = src_root / cand
            build_dir_candidate = cand
        else:
            # heuristics: find top-level directory in src_root
            entries = [p for p in src_root.iterdir() if p.is_dir()]
            build_dir_candidate = entries[0] if entries else src_root

        build_dir = build_dir_candidate
        report["build_dir"] = str(build_dir)

        # 4) apply patches
        patch_res = self.apply_patches(spec, build_dir, dry_run=dry_run)
        report["steps"].append({"patch": patch_res})
        if not patch_res.get("ok"):
            _log("build", f"patching failed for {name}: {patch_res.get('error')}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "patch_failed", "details": patch_res}

        # 5) run build commands (configure, make, etc)
        run_res = self.run_build_commands(spec, build_dir, chroot_ctx=chroot_ctx, use_chroot=chroot_used, fakeroot=fakeroot, dry_run=dry_run)
        report["steps"].append({"build": run_res})
        if not run_res.get("ok"):
            _log("build", f"build commands failed for {name}", level="ERROR", metadata={"pkg": name})
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "build_failed", "details": run_res}

        # 6) staging installation
        staging_dir = Path(staging_dir_override) if staging_dir_override else (workdir / "staging")
        if dir_install:
            # user requested dir-install: simply stage and package
            staging_dir.mkdir(parents=True, exist_ok=True)
        else:
            # always create staging dir to capture files before installing into /
            staging_dir.mkdir(parents=True, exist_ok=True)

        stage_res = self.stage_install(spec, build_dir, staging_dir, chroot_ctx=chroot_ctx, use_chroot=chroot_used, fakeroot=fakeroot, dry_run=dry_run)
        report["steps"].append({"stage": stage_res})
        if not stage_res.get("ok"):
            _log("build", f"stage/install failed for {name}", level="ERROR", metadata={"pkg": name})
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "stage_failed", "details": stage_res}

        # 7) packaging: pack staging to archive
        try:
            pkg_archive = self._pack_staging(staging_dir, out_dir=(self.tmpdir_base / "packages"))
            report["package"] = str(pkg_archive)
        except Exception as e:
            _log("build", f"packing failed: {e}", level="ERROR")
            if chroot_used:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            return {"ok": False, "error": "pack_failed", "details": str(e)}

        # 8) collect files manifest and record to db
        try:
            files_manifest = self._collect_installed_files_manifest(staging_dir)
            manifest_meta = {"recipe": recipe_path, "built_at": time.time()}
            dbres = _record_install_quick(name, version, manifest_meta, files_manifest, deps=spec.get("variants",{}).get("deps", []) or spec.get("dependencies", []))
            report["db_record"] = dbres
        except Exception as e:
            report["db_record"] = {"ok": False, "error": str(e)}
            _log("db", f"failed to record install: {e}", level="WARNING")

        # 9) optionally install (copy to /) after packing
        if install_after and not dry_run:
            # if install_from_cache provided, try using that
            if install_from_cache:
                # install from binary cache path
                try:
                    src_archive = Path(install_from_cache)
                    if src_archive.exists():
                        tmp_unpack = Path(tempfile.mkdtemp(prefix="zeropkg-install-"))
                        with tarfile.open(src_archive, "r:*") as tf:
                            tf.extractall(str(tmp_unpack))
                        inst_res = _installer_install_from_staging(tmp_unpack, target_root=root_for_install, fakeroot=fakeroot)
                        report["install"] = inst_res
                        # cleanup tmp_unpack
                        shutil.rmtree(str(tmp_unpack), ignore_errors=True)
                    else:
                        report["install"] = {"ok": False, "error": "install_from_cache_not_found"}
                except Exception as e:
                    report["install"] = {"ok": False, "error": str(e)}
            else:
                inst_res = _installer_install_from_staging(staging_dir, target_root=root_for_install, fakeroot=fakeroot)
                report["install"] = inst_res

        # 10) cleanup chroot if used
        if chroot_used:
            try:
                _cleanup_chroot(chroot_profile, None, str(workdir))
            except Exception as e:
                _log("chroot", f"cleanup_chroot exception: {e}", level="WARNING")

        # final success
        report["ok"] = True
        return report

# -------------------------
# CLI
# -------------------------
def _cli():
    import argparse
    p = argparse.ArgumentParser(prog="zeropkg-build", description="Zeropkg builder CLI")
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

    # apply minimal config
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
