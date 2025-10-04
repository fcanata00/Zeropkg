#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_builder.py
Zeropkg build manager / orchestrator.

Exposes:
 - class ZeropkgBuilder(config=None)
   - build_package(recipe, use_chroot=True, chroot_profile=None, dir_install=False,
                   staging_dir_override=None, fakeroot=False, dry_run=False,
                   install_after=True, install_from_cache=None, jobs=None, root_for_install="/")
 - helper functions: fetch_sources, extract_sources, apply_patches, run_build_commands, stage_install

Design:
 - Uses safe_import to load optional modules provided in /usr/lib/zeropkg/modules/
 - Converts recipe TOML via zeropkg_toml.to_builder_spec if needed
 - Prepares chroot using zeropkg_chroot.prepare_chroot / cleanup_chroot when requested
 - Calls Installer.install_from_staging / install_from_archive when available
 - All external calls are guarded and return structured dict {"ok": bool, ...}
"""
from __future__ import annotations
import os
import shutil
import subprocess
import tempfile
import json
from pathlib import Path
from typing import Optional, List, Dict, Any

# --------------------
# Safe import helper
# --------------------
def safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

# optional modules
toml_mod = safe_import("zeropkg_toml")
downloader_mod = safe_import("zeropkg_downloader")
patcher_mod = safe_import("zeropkg_patcher")
chroot_mod = safe_import("zeropkg_chroot")
installer_mod = safe_import("zeropkg_installer")
deps_mod = safe_import("zeropkg_deps")
logger_mod = safe_import("zeropkg_logger")
db_mod = safe_import("zeropkg_db")

# logging helper
def _log(tag: str, msg: str, level: str = "info", metadata: Optional[Dict[str, Any]] = None):
    try:
        if logger_mod and hasattr(logger_mod, "log_event"):
            logger_mod.log_event(tag, msg, level=level.upper(), metadata=metadata)
            return
    except Exception:
        pass
    # fallback
    prefix = f"[{level.upper()}] {tag}:"
    print(f"{prefix} {msg}")

# --------------
# Helpers
# --------------
def _ensure_path(p: Optional[str]) -> Optional[Path]:
    if p is None:
        return None
    return Path(p).expanduser().resolve()

def _run_shell(cmd: List[str], cwd: Optional[Path] = None, env: Optional[Dict[str,str]] = None, dry_run: bool=False) -> Dict[str,Any]:
    """Run a shell command, capture output. Return dict with ok, returncode, stdout, stderr."""
    _log("builder", f"Running command: {' '.join(cmd)} (cwd={cwd})", "debug")
    if dry_run:
        return {"ok": True, "dry_run": True, "cmd": cmd}
    try:
        proc = subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        return {"ok": proc.returncode == 0, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# --------------------
# ZeropkgBuilder
# --------------------
class ZeropkgBuilder:
    def __init__(self, config: Optional[Dict[str,Any]] = None):
        self.config = config or {}
        # default paths
        paths = self.config.get("paths", {})
        self.distfiles_dir = Path(paths.get("distfiles_dir", "/usr/ports/distfiles")).expanduser()
        self.state_dir = Path(paths.get("state_dir", "/var/lib/zeropkg")).expanduser()
        self.log_dir = Path(paths.get("log_dir", "/var/log/zeropkg")).expanduser()
        self.distfiles_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            # ignore logging dir permission issues (logger will fallback)
            pass

    # --------------------
    # Top-level API
    # --------------------
    def build_package(self,
                      recipe: str,
                      use_chroot: bool = True,
                      chroot_profile: Optional[str] = None,
                      dir_install: bool = False,
                      staging_dir_override: Optional[str] = None,
                      fakeroot: bool = False,
                      dry_run: bool = False,
                      install_after: bool = True,
                      install_from_cache: Optional[str] = None,
                      jobs: Optional[int] = None,
                      root_for_install: str = "/") -> Dict[str,Any]:
        """
        Build a package described by recipe (path to TOML).
        Returns dict with keys: ok(bool), artifact (path), staging_dir, report, error.
        """
        _log("builder", f"build_package start: {recipe}", "info")
        # normalize recipe
        try:
            if toml_mod and hasattr(toml_mod, "to_builder_spec"):
                spec = toml_mod.to_builder_spec(recipe)
            elif toml_mod and hasattr(toml_mod, "load_recipe"):
                spec = toml_mod.to_builder_spec(toml_mod.load_recipe(recipe))
            else:
                # try load raw toml bytes using local loader
                raise RuntimeError("toml module missing")
        except Exception as e:
            _log("builder", f"Failed to parse recipe {recipe}: {e}", "error")
            return {"ok": False, "error": f"parse_error: {e}"}

        # Validate minimal fields
        name = spec.get("name") or Path(recipe).stem
        version = spec.get("version") or "0.0"
        pkg_id = f"{name}-{version}"

        # If install_from_cache specified and exists, install and return
        if install_from_cache:
            if Path(install_from_cache).exists():
                _log("builder", f"Installing from cache {install_from_cache}", "info")
                if dry_run:
                    return {"ok": True, "artifact": install_from_cache, "installed": False, "dry_run": True}
                # call installer
                inst_res = self._installer_install_archive(install_from_cache, root=root_for_install, fakeroot=fakeroot)
                return {"ok": inst_res.get("ok", False), "artifact": install_from_cache, "install_result": inst_res}

        # Prepare build workspace
        tmp_base = Path(tempfile.mkdtemp(prefix=f"zeropkg-build-{pkg_id}-"))
        workdir = tmp_base / "work"
        staging = Path(staging_dir_override) if staging_dir_override else tmp_base / "staging"
        workdir.mkdir(parents=True, exist_ok=True)
        staging.mkdir(parents=True, exist_ok=True)

        result = {"ok": False, "name": name, "version": version, "workdir": str(workdir), "staging": str(staging)}

        # 1) fetch sources
        fetch_res = self.fetch_sources(spec, dest_dir=self.distfiles_dir, workdir=workdir, dry_run=dry_run)
        if not fetch_res.get("ok"):
            result["error"] = "fetch_failed"
            result["fetch"] = fetch_res
            _log("builder", f"fetch failed for {pkg_id}: {fetch_res}", "error")
            # cleanup
            # do not remove staging on dry-run
            if not dry_run:
                try:
                    shutil.rmtree(tmp_base)
                except Exception:
                    pass
            return result
        result["fetch"] = fetch_res

        # 2) extract sources into workdir
        extract_res = self.extract_sources(spec, distdir=self.distfiles_dir, dest_workdir=workdir, dry_run=dry_run)
        if not extract_res.get("ok"):
            result["error"] = "extract_failed"
            result["extract"] = extract_res
            _log("builder", f"extract failed for {pkg_id}: {extract_res}", "error")
            if not dry_run:
                try: shutil.rmtree(tmp_base)
                except Exception: pass
            return result
        result["extract"] = extract_res

        # 3) apply patches
        patch_res = self.apply_patches(spec, workdir=workdir, dry_run=dry_run)
        result["patches"] = patch_res

        # 4) optionally prepare chroot
        chroot_used = False
        if use_chroot and chroot_mod and hasattr(chroot_mod, "prepare_chroot"):
            try:
                if not dry_run:
                    _log("builder", f"Preparing chroot profile={chroot_profile}", "info")
                    prep = chroot_mod.prepare_chroot(profile=chroot_profile, root=None, workdir=str(workdir))
                    chroot_used = True
                    result["chroot_prepare"] = prep
                else:
                    result["chroot_prepare"] = {"ok": True, "dry_run": True}
            except Exception as e:
                result["error"] = f"chroot_prepare_failed: {e}"
                _log("builder", f"chroot prepare failed: {e}", "error")
                if not dry_run:
                    try: shutil.rmtree(tmp_base)
                    except Exception: pass
                return result

        # 5) run build commands
        build_res = self.run_build_commands(spec, workdir=workdir, env=spec.get("environment") or {}, jobs=jobs, dry_run=dry_run, fakeroot=fakeroot)
        result["build"] = build_res
        if not build_res.get("ok"):
            result["error"] = "build_failed"
            # attempt chroot cleanup
            if chroot_used and chroot_mod and hasattr(chroot_mod, "cleanup_chroot"):
                try:
                    chroot_mod.cleanup_chroot(profile=chroot_profile, root=None, workdir=str(workdir))
                except Exception:
                    pass
            if not dry_run:
                try: shutil.rmtree(tmp_base)
                except Exception: pass
            return result

        # 6) stage install (make install DESTDIR=staging)
        stage_res = self.stage_install(spec, workdir=workdir, staging_dir=staging, dry_run=dry_run, fakeroot=fakeroot)
        result["stage"] = stage_res
        if not stage_res.get("ok"):
            result["error"] = "stage_failed"
            # cleanup chroot
            if chroot_used and chroot_mod and hasattr(chroot_mod, "cleanup_chroot"):
                try: chroot_mod.cleanup_chroot(profile=chroot_profile, root=None, workdir=str(workdir))
                except Exception: pass
            if not dry_run:
                try: shutil.rmtree(tmp_base)
                except Exception: pass
            return result

        # 7) optionally pack artifact (tar.xz) in distfiles/cache
        artifact_path = tmp_base / f"{pkg_id}.tar.xz"
        if not dry_run:
            try:
                # create artifact from staging
                cwd = staging
                # use tar via python tarfile for portability
                import tarfile
                with tarfile.open(str(artifact_path), "w:xz") as tf:
                    for f in staging.rglob("*"):
                        arcname = f.relative_to(staging)
                        tf.add(str(f), arcname=str(arcname))
                result["artifact"] = str(artifact_path)
            except Exception as e:
                result["artifact_error"] = str(e)
                _log("builder", f"artifact packing failed: {e}", "warning")
        else:
            result["artifact"] = str(artifact_path)
            result["artifact_dry_run"] = True

        # 8) install after build if requested
        if install_after:
            if dry_run:
                result["install"] = {"ok": True, "dry_run": True}
            else:
                # installer call: prefer Installer class if available
                if installer_mod:
                    try:
                        # try class-based API
                        if hasattr(installer_mod, "Installer"):
                            inst = installer_mod.Installer(config=self.config)
                            inst_res = inst.install_from_staging(str(staging), root=root_for_install, fakeroot=fakeroot)
                        elif hasattr(installer_mod, "install_from_staging"):
                            inst_res = installer_mod.install_from_staging(str(staging), root=root_for_install, fakeroot=fakeroot)
                        else:
                            inst_res = {"ok": False, "error": "installer_api_missing"}
                    except Exception as e:
                        inst_res = {"ok": False, "error": str(e)}
                else:
                    # fallback: copy files from staging to root
                    try:
                        self._fallback_copy_tree(staging, Path(root_for_install))
                        inst_res = {"ok": True, "method": "fallback_copy"}
                    except Exception as e:
                        inst_res = {"ok": False, "error": str(e)}
                result["install"] = inst_res

        # 9) cleanup chroot if used
        if chroot_used and chroot_mod and hasattr(chroot_mod, "cleanup_chroot"):
            try:
                if not dry_run:
                    chroot_mod.cleanup_chroot(profile=chroot_profile, root=None, workdir=str(workdir))
                    result["chroot_cleanup"] = {"ok": True}
                else:
                    result["chroot_cleanup"] = {"ok": True, "dry_run": True}
            except Exception as e:
                result["chroot_cleanup"] = {"ok": False, "error": str(e)}

        # 10) persist build record to DB if available
        if db_mod and hasattr(db_mod, "record_install_quick"):
            try:
                db_mod.record_install_quick(name, version, spec, files=[str(p) for p in staging.rglob("*")], deps=spec.get("dependencies"))
                result["db_recorded"] = True
            except Exception as e:
                result["db_recorded"] = False
                result["db_error"] = str(e)

        result["ok"] = True
        # keep staged dir/artifact for inspection unless caller wants cleanup
        return result

    # --------------------
    # Subtasks
    # --------------------
    def fetch_sources(self, spec: Dict[str,Any], dest_dir: Path, workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        """
        Downloads all spec['sources'] into dest_dir. Returns dict listing fetched files.
        """
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        sources = spec.get("sources", []) or []
        fetched = []
        errors = []
        if not sources:
            _log("builder", f"No sources declared for {spec.get('name')}", "info")
            return {"ok": True, "fetched": [], "warnings": ["no_sources"]}
        # prefer downloader.Downloader API if present
        if downloader_mod and hasattr(downloader_mod, "Downloader"):
            Downloader = downloader_mod.Downloader
            dd = Downloader(distdir=dest_dir)
            for s in sources:
                url = s.get("url") if isinstance(s, dict) else (s.url if hasattr(s,'url') else str(s))
                try:
                    r = dd.fetch(url, dest_dir=dest_dir, dry_run=dry_run)
                    if r.get("ok"):
                        fetched.append(r)
                    else:
                        errors.append(r)
                except Exception as e:
                    errors.append({"url": url, "error": str(e)})
        else:
            # fallback: simple urllib download for each URL
            import urllib.request
            for s in sources:
                url = s.get("url") if isinstance(s, dict) else (s.url if hasattr(s,'url') else str(s))
                filename = None
                try:
                    if dry_run:
                        fetched.append({"url": url, "dry_run": True})
                        continue
                    # derive filename
                    filename = url.split("/")[-1] or spec.get("name")
                    outp = dest_dir / filename
                    _log("builder", f"Downloading {url} -> {outp}", "info")
                    urllib.request.urlretrieve(url, str(outp))
                    fetched.append({"url": url, "path": str(outp)})
                except Exception as e:
                    errors.append({"url": url, "error": str(e), "dest": str(dest_dir)})
        ok = len(errors) == 0
        return {"ok": ok, "fetched": fetched, "errors": errors}

    def extract_sources(self, spec: Dict[str,Any], distdir: Path, dest_workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        """
        Locates source archives in distdir and extracts them into dest_workdir.
        If spec includes an explicit 'build.directory' that enumerates a subdir, we honor it.
        """
        distdir = Path(distdir)
        dest_workdir = Path(dest_workdir)
        dest_workdir.mkdir(parents=True, exist_ok=True)
        sources = spec.get("sources", []) or []
        extracted = []
        errors = []
        if not sources:
            return {"ok": True, "extracted": [], "warnings": ["no_sources"]}
        # Try to find a primary archive (first source with url)
        for s in sources:
            url = s.get("url") if isinstance(s, dict) else (getattr(s,"url",None) or str(s))
            if not url:
                continue
            fname = (url.split("/")[-1] or url).split("?")[0]
            cand = distdir / fname
            if cand.exists():
                # extract
                try:
                    if dry_run:
                        extracted.append({"archive": str(cand), "dry_run": True})
                        continue
                    # use tarfile or zipfile
                    import tarfile, zipfile
                    if tarfile.is_tarfile(str(cand)):
                        with tarfile.open(str(cand), "r:*") as tf:
                            tf.extractall(path=str(dest_workdir))
                            extracted.append({"archive": str(cand), "ok": True})
                    elif zipfile.is_zipfile(str(cand)):
                        with zipfile.ZipFile(str(cand), "r") as zf:
                            zf.extractall(path=str(dest_workdir))
                            extracted.append({"archive": str(cand), "ok": True})
                    else:
                        # fallback: copy
                        target = dest_workdir / cand.name
                        shutil.copy2(str(cand), str(target))
                        extracted.append({"archive": str(cand), "copied": True})
                    # Only extract first matching candidate by default
                    # but continue to extract remaining items if specified
                except Exception as e:
                    errors.append({"archive": str(cand), "error": str(e)})
            else:
                # not present, skip
                errors.append({"archive": str(cand), "error": "not_found"})
        ok = len(errors) == 0
        return {"ok": ok, "extracted": extracted, "errors": errors}

    def apply_patches(self, spec: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        """
        Apply patches defined in spec['patches'] to sources in workdir.
        Uses zeropkg_patcher.apply_patches if available, else does nothing.
        """
        patches = spec.get("patches", []) or []
        if not patches:
            return {"ok": True, "applied": [], "warnings": ["no_patches"]}
        # use patcher_mod if available
        if patcher_mod and hasattr(patcher_mod, "apply_patches"):
            try:
                res = patcher_mod.apply_patches(spec, workdir, dry_run=dry_run)
                return {"ok": res.get("ok", False), "detail": res}
            except Exception as e:
                return {"ok": False, "error": str(e)}
        # fallback: try 'patch' binary per patch entry
        applied = []
        errors = []
        for p in patches:
            path = p.get("path") if isinstance(p, dict) else getattr(p,"path",str(p))
            strip = int(p.get("strip", 1) if isinstance(p, dict) else getattr(p,"strip",1))
            patch_cmd = ["patch", f"-p{strip}", "-i", str(path)]
            if dry_run:
                applied.append({"patch": path, "dry_run": True})
                continue
            r = _run_shell(patch_cmd, cwd=workdir, dry_run=False)
            if r.get("ok"):
                applied.append({"patch": path, "ok": True})
            else:
                errors.append({"patch": path, "result": r})
        ok = len(errors) == 0
        return {"ok": ok, "applied": applied, "errors": errors}

    def run_build_commands(self, spec: Dict[str,Any], workdir: Path, env: Optional[Dict[str,str]] = None,
                           jobs: Optional[int] = None, dry_run: bool=False, fakeroot: bool=False) -> Dict[str,Any]:
        """
        Execute build commands defined in spec["build"]["commands"] or spec["_raw"]["build"]["commands"].
        Returns dict with ok and logs.
        """
        build = spec.get("build") or {}
        cmds = build.get("commands") or (spec.get("_raw",{}).get("build",{}).get("commands") if isinstance(spec.get("_raw",{}), dict) else [])
        if isinstance(cmds, str):
            cmds = [cmds]
        if not cmds:
            return {"ok": True, "warnings": ["no_build_commands"]}
        results = []
        env_vars = os.environ.copy()
        if env:
            env_vars.update({str(k): str(v) for k,v in env.items()})
        # add JOBS or MAKEFLAGS if jobs specified
        if jobs:
            env_vars["JOBS"] = str(jobs)
            makeflags = env_vars.get("MAKEFLAGS", "")
            if f"-j" not in makeflags:
                env_vars["MAKEFLAGS"] = (makeflags + f" -j{jobs}").strip()
        for c in cmds:
            # if fakeroot is requested, prefix with fakeroot (best-effort)
            cmd_list = c if isinstance(c, list) else (["/bin/sh", "-c", c])
            if fakeroot:
                # try to use fakeroot binary if present
                if shutil.which("fakeroot"):
                    cmd_list = ["fakeroot"] + cmd_list
            # run
            out = _run_shell(cmd_list, cwd=workdir, env=env_vars, dry_run=dry_run)
            results.append({"cmd": c, "result": out})
            if not out.get("ok"):
                return {"ok": False, "results": results}
        return {"ok": True, "results": results}

    def stage_install(self, spec: Dict[str,Any], workdir: Path, staging_dir: Path, dry_run: bool=False, fakeroot: bool=False) -> Dict[str,Any]:
        """
        Run install commands into a staging directory.
        The install commands are expected similar to ["make DESTDIR=/staging install"] or separate commands.
        If none provided, attempt 'make install DESTDIR=staging'.
        """
        install = spec.get("install") or {}
        cmds = install.get("commands") or []
        results = []
        if not cmds:
            # default try make install
            # attempt to detect 'make' build system
            # run: make DESTDIR=staging install
            make_cmd = ["/usr/bin/make" if Path("/usr/bin/make").exists() else "make", f"DESTDIR={str(staging_dir)}", "install"]
            if dry_run:
                return {"ok": True, "results": [{"cmd": "make install", "dry_run": True}]}
            r = _run_shell(make_cmd, cwd=workdir, env=None, dry_run=dry_run)
            return {"ok": r.get("ok", False), "results": [r]}
        else:
            for c in cmds:
                # ensure DESTDIR injected if not present
                cmd_text = c
                if "DESTDIR" not in c:
                    if isinstance(c, str):
                        cmd_text = f"{c} DESTDIR={str(staging_dir)}"
                cmd_list = ["/bin/sh", "-c", cmd_text]
                out = _run_shell(cmd_list, cwd=workdir, dry_run=dry_run)
                results.append({"cmd": cmd_text, "result": out})
                if not out.get("ok"):
                    return {"ok": False, "results": results}
            return {"ok": True, "results": results}

    # --------------------
    # Internal helpers
    # --------------------
    def _installer_install_archive(self, archive_path: str, root: str = "/", fakeroot: bool = False) -> Dict[str,Any]:
        """Install from archive using installer_mod or fallback."""
        if installer_mod:
            try:
                if hasattr(installer_mod, "Installer"):
                    inst = installer_mod.Installer(config=self.config)
                    return inst.install_from_archive(archive_path, root=root, fakeroot=fakeroot)
                if hasattr(installer_mod, "install_from_archive"):
                    return installer_mod.install_from_archive(archive_path, root=root, fakeroot=fakeroot)
            except Exception as e:
                return {"ok": False, "error": str(e)}
        # fallback: extract + copy
        try:
            tmp = Path(tempfile.mkdtemp(prefix="zeropkg-inst-"))
            import tarfile, zipfile
            p = Path(archive_path)
            if tarfile.is_tarfile(str(p)):
                with tarfile.open(str(p),"r:*") as tf:
                    tf.extractall(path=str(tmp))
            elif zipfile.is_zipfile(str(p)):
                with zipfile.ZipFile(str(p),"r") as zf:
                    zf.extractall(path=str(tmp))
            else:
                # copy as-is
                shutil.copy2(str(p), str(tmp / p.name))
            # copy to root
            self._fallback_copy_tree(tmp, Path(root))
            shutil.rmtree(tmp, ignore_errors=True)
            return {"ok": True, "method": "fallback_install"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _fallback_copy_tree(self, src: Path, dest: Path):
        """Recursively copy files preserving metadata where possible (best-effort)."""
        if not src.exists():
            raise FileNotFoundError(str(src))
        for p in src.rglob("*"):
            rel = p.relative_to(src)
            target = dest / rel
            if p.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(p), str(target))

# --------------------
# Module-level helpers
# --------------------
def build_package(*args, **kwargs):
    b = ZeropkgBuilder()
    return b.build_package(*args, **kwargs)

# --------------------
# Quick smoke test when run directly (no side-effects)
# --------------------
if __name__ == "__main__":  # pragma: no cover
    print("ZeropkgBuilder smoke test (dry-run)")
    builder = ZeropkgBuilder()
    # Example: attempt to parse a sample recipe if exists
    sample = None
    if Path("example.toml").exists():
        sample = "example.toml"
    if sample:
        res = builder.build_package(sample, dry_run=True, install_after=False)
        print(json.dumps(res, indent=2))
    else:
        print("No example.toml in cwd; nothing to build (this is expected).")
