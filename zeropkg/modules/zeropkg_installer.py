#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_installer.py — Zeropkg installer (improved)

Features:
 - install_from_build(build_dir, ...) : installs from a pkgroot produced by builder
 - install_from_archive(archive_path, ...) : installs from binpkg archive (.tar.zst/.tar.gz)
 - remove(package_name, manifest=None, ...) : remove package using manifest or DB
 - computes SHA256 of installed files and writes install-manifest.json
 - rollback on partial failure (best-effort)
 - supports fakeroot to preserve UID/GID, and safe sandboxing under given root/chroot
 - support for hooks: global (/etc/zeropkg/hooks.d/*) and recipe-local hooks
 - packages to .tar.zst using zstd -T0 if available
 - records install metadata in DB (if zeropkg_db available)
 - integrates with depclean for automatic cleanup after batch installs
 - dry-run and verbose modes
"""

from __future__ import annotations
import os
import sys
import json
import shutil
import hashlib
import tempfile
import tarfile
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Try to import project modules; provide safe fallbacks
try:
    from zeropkg_logger import get_logger
    logger = get_logger("installer")
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_installer")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)

try:
    from zeropkg_db import ZeroPKGDB, record_install_quick, remove_package_quick
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False
    ZeroPKGDB = None
    record_install_quick = None
    remove_package_quick = None

try:
    from zeropkg_chroot import run_in_chroot, prepare_chroot, cleanup_chroot
    CHROOT_AVAILABLE = True
except Exception:
    CHROOT_AVAILABLE = False
    def run_in_chroot(*args, **kwargs):
        raise RuntimeError("zeropkg_chroot.run_in_chroot not available")

try:
    from zeropkg_depclean import ZeroPKGDepClean
    DEP_CLEAN_AVAILABLE = True
except Exception:
    DEP_CLEAN_AVAILABLE = False
    ZeroPKGDepClean = None

# default paths and safelist
DEFAULT_BINPKG_DIR = Path("/var/cache/zeropkg/binpkgs")
DEFAULT_INSTALL_LOG_DIR = Path("/var/log/zeropkg/installer")
GLOBAL_HOOKS_DIR = Path("/etc/zeropkg/hooks.d")
SAFE_PREFIXES = ["/", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc", "/opt", "/var", "/usr/local"]

# utility helpers ---------------------------------------------------------
def _atomic_write(path: Path, data: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)

def _compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _safe_relpath_check(target_root: Path, dest: Path) -> bool:
    """
    Ensure dest is inside target_root (prevents path traversal).
    """
    try:
        target_root = target_root.resolve()
        dest = dest.resolve()
        return str(dest).startswith(str(target_root))
    except Exception:
        return False

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def _run_cmd(cmd: List[str], cwd: Optional[str] = None, capture=False) -> Tuple[int, str, str]:
    logger.debug(f"CMD: {' '.join(cmd)} (cwd={cwd})")
    try:
        if capture:
            p = subprocess.run(cmd, cwd=cwd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.returncode, p.stdout, p.stderr
        else:
            subprocess.run(cmd, cwd=cwd, check=True)
            return 0, "", ""
    except subprocess.CalledProcessError as e:
        return e.returncode, getattr(e, "stdout", ""), getattr(e, "stderr", str(e))
    except FileNotFoundError as e:
        return 127, "", str(e)

# main class --------------------------------------------------------------
class ZeropkgInstaller:
    def __init__(self,
                 binpkg_dir: Optional[Path] = None,
                 log_dir: Optional[Path] = None,
                 require_sandbox: bool = True):
        self.binpkg_dir = Path(binpkg_dir or DEFAULT_BINPKG_DIR)
        self.log_dir = Path(log_dir or DEFAULT_INSTALL_LOG_DIR)
        _ensure_dir(self.binpkg_dir)
        _ensure_dir(self.log_dir)
        self.require_sandbox = bool(require_sandbox)
        if DB_AVAILABLE:
            try:
                self.db = ZeroPKGDB()
            except Exception:
                self.db = None
        else:
            self.db = None

    # -------------------------
    # Install from builder pkgroot directory
    # -------------------------
    def install_from_build(self,
                           pkg_name: str,
                           build_pkgroot: Path,
                           version: Optional[str] = None,
                           root: str = "/",
                           fakeroot: bool = False,
                           use_chroot: bool = False,
                           hooks: Optional[Dict[str, List[str]]] = None,
                           dry_run: bool = False,
                           create_binpkg: bool = True,
                           global_hooks_dir: Optional[Path] = None) -> Dict[str,Any]:
        """
        Instala arquivos do build_pkgroot (a estrutura que seria copiada para /).
        - pkg_name: name of package (for DB and logs)
        - build_pkgroot: path containing files laid out as /usr /bin etc.
        - root: destination root (e.g. "/" or "/mnt/lfs")
        - fakeroot: use fakeroot when copying to preserve uid/gid
        - use_chroot: if True, expects root to be prepared chroot and will run hooks inside chroot
        - hooks: dict with keys pre_install/post_install commands (strings list) executed in build context
        - dry_run: do not actually copy
        - create_binpkg: create compressed binary package (zstd if available) and store in binpkg_dir
        """
        rootp = Path(root).resolve()
        build_pkgroot = Path(build_pkgroot).resolve()
        log_prefix = f"{pkg_name}-{version or 'unknown'}"
        instal_log = self.log_dir / f"{log_prefix}.log"
        logger.info(f"[installer] Installing {pkg_name} -> root={rootp} (dry_run={dry_run} fakeroot={fakeroot} chroot={use_chroot})")

        # safety: ensure target is allowed or sandboxed
        if self.require_sandbox and str(rootp) == "/":
            logger.warning("Default policy: installing into '/' requires explicit permission. Use root param or set require_sandbox=False.")
            # allow but warn; not blocking to be practical
        if not _safe_relpath_check(rootp, rootp):
            raise RuntimeError("Invalid root path")

        # gather list of files to install and compute sizes/hashes
        files = []
        for p in build_pkgroot.rglob("*"):
            if p.is_file():
                rel = p.relative_to(build_pkgroot)
                dest = rootp / rel
                files.append((p, dest))

        manifest = {"package": pkg_name, "version": version, "files": [], "total_size": 0, "installed_at": int(time.time())}
        # compute hashes and sizes
        for src, dst in files:
            size = src.stat().st_size
            sha256 = _compute_sha256(src)
            manifest["files"].append({"src": str(src), "dst": str(dst), "size": size, "sha256": sha256})
            manifest["total_size"] += size

        if dry_run:
            logger.info(f"[dry-run] Would install {len(files)} files, total {manifest['total_size']} bytes")
            return {"ok": True, "dry_run": True, "manifest": manifest}

        # prepare logging file
        with open(instal_log, "w", encoding="utf-8") as logf:
            logf.write(f"Install log for {pkg_name} at {time.ctime()}\n")

        # run pre-install hooks (global then local)
        self._run_hooks("pre_install", pkg_name, version, hooks, rootp, use_chroot, fakeroot)

        # backup files that would be overwritten and copy with rollback ability
        backups = []
        installed = []
        try:
            for src, dst in files:
                # ensure dst parent exists
                dst_parent = dst.parent
                dst_parent.mkdir(parents=True, exist_ok=True)
                # if dst exists, backup
                if dst.exists():
                    # create backup in tmp dir
                    tmpb = Path(tempfile.mkdtemp(prefix=f"zeropkg-inst-bak-{pkg_name}-"))
                    rel = dst.relative_to(rootp)
                    bak_dest = tmpb / rel
                    bak_dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(dst, bak_dest)
                    backups.append({"dst": str(dst), "backup": str(bak_dest)})
                # copy file (use fakeroot if requested)
                if fakeroot and shutil.which("fakeroot"):
                    # use fakeroot sh -c "cp --preserve=mode,ownership,timestamps src dst"
                    cmd = ["fakeroot", "--", "sh", "-c", f"cp --remove-destination --preserve=mode,ownership,timestamps '{src}' '{dst}'"]
                    rc, out, err = _run_cmd(cmd, capture=True)
                    if rc != 0:
                        raise RuntimeError(f"fakeroot copy failed: {err or out}")
                else:
                    shutil.copy2(src, dst)
                installed.append(str(dst))
            # write manifest to /var/lib/zeropkg or rootp/var/lib/zeropkg installed-manifest
            manifest_path = rootp / "var" / "lib" / "zeropkg" / f"{pkg_name}-{version or 'unknown'}-manifest.json"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write(manifest_path, manifest)
            logger.info(f"Installed {len(installed)} files for {pkg_name}; manifest at {manifest_path}")

            # record in DB
            try:
                if DB_AVAILABLE and record_install_quick:
                    record_install_quick(pkg_name, manifest, deps=[], metadata={"install_path": str(rootp)})
            except Exception as e:
                logger.warning(f"DB registration failed: {e}")

            # create binary package if requested
            pkg_archive = None
            if create_binpkg:
                try:
                    pkg_archive = self._create_binpkg_from_pkgroot(build_pkgroot, pkg_name, version)
                    logger.info(f"Created binpkg: {pkg_archive}")
                except Exception as e:
                    logger.warning(f"Failed to create binpkg: {e}")

            # run post-install hooks
            self._run_hooks("post_install", pkg_name, version, hooks, rootp, use_chroot, fakeroot)

            # optionally run depclean to cleanup orphans
            if DEP_CLEAN_AVAILABLE:
                try:
                    depclean = ZeroPKGDepClean()
                    depclean.auto_clean()
                except Exception as e:
                    logger.debug(f"depclean auto_clean failed: {e}")

            return {"ok": True, "installed": installed, "manifest": manifest, "archive": str(pkg_archive) if pkg_archive else None}
        except Exception as e:
            logger.error(f"Install failed for {pkg_name}: {e}; attempting rollback")
            # rollback: restore backups and remove installed files
            self._rollback_install(installed, backups, rootp)
            # remove manifest if present
            try:
                if manifest_path.exists():
                    manifest_path.unlink()
            except Exception:
                pass
            return {"ok": False, "error": str(e)}

    # -------------------------
    # Install from archive (tar.zst / tar.gz)
    # -------------------------
    def install_from_archive(self,
                             archive: Path,
                             pkg_name: Optional[str] = None,
                             version: Optional[str] = None,
                             root: str = "/",
                             fakeroot: bool = False,
                             use_chroot: bool = False,
                             hooks: Optional[Dict[str, List[str]]] = None,
                             dry_run: bool = False,
                             create_binpkg: bool = False) -> Dict[str,Any]:
        archive = Path(archive)
        if not archive.exists():
            raise FileNotFoundError(archive)
        # unpack archive into temp dir then call install_from_build
        tmpd = Path(tempfile.mkdtemp(prefix="zeropkg-inst-unpack-"))
        try:
            logger.info(f"Extracting archive {archive} to {tmpd}")
            if str(archive).endswith(".zst") or str(archive).endswith(".tar.zst"):
                # require zstd binary
                if shutil.which("zstd"):
                    tmp_tar = tmpd / "pkg.tar"
                    rc, out, err = _run_cmd(["zstd", "-d", str(archive), "-o", str(tmp_tar)], capture=True)
                    if rc != 0:
                        raise RuntimeError(f"zstd decompress failed: {err or out}")
                    with tarfile.open(tmp_tar, "r:") as tar:
                        tar.extractall(path=tmpd)
                else:
                    raise RuntimeError("zstd not found to decompress archive")
            else:
                # handle gz/xz via tarfile auto-detection
                with tarfile.open(archive, "r:*") as tar:
                    tar.extractall(path=tmpd)
            # now tmpd should contain the pkgroot layout (files like usr/ bin/ etc)
            return self.install_from_build(pkg_name or archive.stem, tmpd, version=version, root=root, fakeroot=fakeroot, use_chroot=use_chroot, hooks=hooks, dry_run=dry_run, create_binpkg=create_binpkg)
        finally:
            try:
                shutil.rmtree(tmpd)
            except Exception:
                pass

    # -------------------------
    # Remove package
    # -------------------------
    def remove(self,
               pkg_name: str,
               manifest: Optional[Dict[str,Any]] = None,
               root: str = "/",
               run_hooks: bool = True,
               dry_run: bool = False) -> Dict[str,Any]:
        """
        Remove package using manifest (if provided) or DB lookup.
        Manifest must list installed file paths relative to root or absolute.
        """
        rootp = Path(root).resolve()
        logger.info(f"Removing package {pkg_name} from {rootp} (dry_run={dry_run})")
        # find manifest: check DB or manifest files under /var/lib/zeropkg
        if manifest is None:
            # attempt to find manifest file
            man_glob = list((rootp / "var" / "lib" / "zeropkg").glob(f"{pkg_name}-*-manifest.json")) if (rootp / "var" / "lib" / "zeropkg").exists() else []
            if man_glob:
                try:
                    with open(man_glob[-1], "r", encoding="utf-8") as f:
                        manifest = json.load(f)
                except Exception:
                    manifest = None
        if manifest is None and DB_AVAILABLE:
            try:
                # try to query db for file list (record_install_quick schema assumed)
                # fallback: use remove_package_quick which may return manifest
                manifest = getattr(self, "_manifest_from_db", lambda n: None)(pkg_name)
            except Exception:
                manifest = None

        if manifest is None:
            logger.warning("No manifest found; aborting removal for safety")
            return {"ok": False, "error": "manifest-not-found"}

        files = manifest.get("files", [])
        removed = []
        skipped = []
        errors = []
        if dry_run:
            logger.info(f"[dry-run] would remove {len(files)} files")
            return {"ok": True, "dry_run": True, "files": files}

        # pre-remove hooks
        if run_hooks:
            self._run_hooks("pre_remove", pkg_name, manifest.get("version"), None, rootp, False, False)

        for entry in files:
            dst = Path(entry.get("dst"))
            # ensure dst is inside rootp
            if not _safe_relpath_check(rootp, dst):
                skipped.append(str(dst))
                logger.warning(f"Skipping removal outside root: {dst}")
                continue
            try:
                if dst.exists():
                    dst.unlink()
                    removed.append(str(dst))
                else:
                    skipped.append(str(dst))
            except Exception as e:
                logger.error(f"Failed to remove {dst}: {e}")
                errors.append({"file": str(dst), "error": str(e)})

        # remove manifest file
        try:
            man_path = Path(manifest.get("manifest_path")) if manifest.get("manifest_path") else None
            if man_path and man_path.exists():
                man_path.unlink()
        except Exception:
            pass

        # update DB
        try:
            if DB_AVAILABLE and remove_package_quick:
                remove_package_quick(pkg_name)
        except Exception as e:
            logger.warning(f"DB removal failed: {e}")

        if run_hooks:
            self._run_hooks("post_remove", pkg_name, manifest.get("version"), None, rootp, False, False)

        return {"ok": True, "removed": removed, "skipped": skipped, "errors": errors}

    # -------------------------
    # Helpers: create binpkg from a pkgroot dir
    # -------------------------
    def _create_binpkg_from_pkgroot(self, pkgroot: Path, pkg_name: str, version: Optional[str] = None) -> Path:
        """
        Create binary package archive (.tar.zst if zstd available) in binpkg_dir.
        """
        timestamp = int(time.time())
        ver = version or "0"
        base_name = f"{pkg_name}-{ver}-{timestamp}"
        archive_name = f"{base_name}.tar.zst"
        archive_path = self.binpkg_dir / archive_name
        # create a tar then zstd if available
        tmp_tar = self.binpkg_dir / f"{base_name}.tar"
        # use system tar for speed
        _run_cmd(["tar", "-C", str(pkgroot), "-cf", str(tmp_tar), "."], capture=False)
        if shutil.which("zstd"):
            _run_cmd(["zstd", "-q", "-19", str(tmp_tar), "-o", str(archive_path)], capture=False)
            try:
                tmp_tar.unlink()
            except Exception:
                pass
        else:
            # fallback to gz
            gz_path = self.binpkg_dir / f"{base_name}.tar.gz"
            _run_cmd(["gzip", "-c", str(tmp_tar)], capture=False)
            # simpler fallback: use python tarfile to create gz
            with tarfile.open(gz_path, "w:gz") as tf:
                tf.add(str(pkgroot), arcname=".")
            archive_path = gz_path
            try:
                tmp_tar.unlink()
            except Exception:
                pass
        return archive_path

    # -------------------------
    # Rollback helper
    # -------------------------
    def _rollback_install(self, installed_paths: List[str], backups: List[Dict[str,str]], rootp: Path):
        logger.info("Performing rollback of partial installation")
        # remove files that were installed
        for p in installed_paths:
            try:
                pp = Path(p)
                if pp.exists():
                    pp.unlink()
            except Exception as e:
                logger.warning(f"Rollback: failed to remove {p}: {e}")
        # restore backups
        for b in backups:
            try:
                dst = Path(b["dst"])
                bak = Path(b["backup"])
                if bak.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(bak), str(dst))
            except Exception as e:
                logger.warning(f"Rollback: failed to restore backup {b}: {e}")
        logger.info("Rollback complete")

    # -------------------------
    # Hooks runner
    # -------------------------
    def _run_hooks(self,
                   stage: str,
                   pkg_name: str,
                   pkg_version: Optional[str],
                   recipe_hooks: Optional[Dict[str, List[str]]],
                   rootp: Path,
                   use_chroot: bool,
                   fakeroot: bool):
        """
        Stage: 'pre_install', 'post_install', 'pre_remove', 'post_remove'
        Executes global hooks then recipe_hooks[stage]
        """
        # run global hooks
        env = os.environ.copy()
        env.update({"PKG_NAME": pkg_name, "PKG_VERSION": pkg_version or ""})
        # global hooks
        if GLOBAL_HOOKS_DIR.exists():
            for hook in sorted(GLOBAL_HOOKS_DIR.iterdir()):
                if not hook.is_file() or not os.access(hook, os.X_OK):
                    continue
                cmd = str(hook)
                logger.info(f"Running global hook {stage}: {cmd}")
                if use_chroot and CHROOT_AVAILABLE:
                    try:
                        run_in_chroot(rootp, cmd, env=env, fakeroot=fakeroot)
                    except Exception as e:
                        logger.warning(f"Global hook failed in chroot: {e}")
                else:
                    try:
                        _run_cmd([cmd], capture=False)
                    except Exception as e:
                        logger.warning(f"Global hook failed: {e}")
        # recipe local hooks
        if recipe_hooks and stage in recipe_hooks:
            for cmd in recipe_hooks[stage]:
                # resolve macros maybe present; do simple env interpolation
                command = cmd.format(PKG_NAME=pkg_name, PKG_VERSION=pkg_version or "")
                logger.info(f"Running recipe hook {stage}: {command}")
                if use_chroot and CHROOT_AVAILABLE:
                    try:
                        run_in_chroot(rootp, command, env=env, fakeroot=fakeroot)
                    except Exception as e:
                        logger.warning(f"Recipe hook failed in chroot: {e}")
                else:
                    rc, out, err = _run_cmd(["sh", "-c", command], capture=True)
                    if rc != 0:
                        logger.warning(f"Recipe hook failed: rc={rc} err={err}")

    # -------------------------
    # Helper: attempt to get manifest from DB (fallback)
    # -------------------------
    def _manifest_from_db(self, pkg_name: str) -> Optional[Dict[str,Any]]:
        if not DB_AVAILABLE or not self.db:
            return None
        try:
            rec = self.db.get_package_manifest(pkg_name)
            return rec
        except Exception:
            return None

# CLI ---------------------------------------------------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-installer", description="Zeropkg installer tool")
    sub = parser.add_subparsers(dest="cmd")

    p_install = sub.add_parser("install")
    p_install.add_argument("pkgroot", help="path to package root (layout with usr/ bin/ etc) or archive")
    p_install.add_argument("--name", help="package name")
    p_install.add_argument("--version", help="package version")
    p_install.add_argument("--root", default="/", help="target root (default /)")
    p_install.add_argument("--fakeroot", action="store_true", help="use fakeroot for preserve uid/gid")
    p_install.add_argument("--chroot", action="store_true", help="assume target root is prepared chroot and run hooks inside it")
    p_install.add_argument("--dry-run", action="store_true")
    p_install.add_argument("--no-binpkg", action="store_true", help="do not create binpkg")
    p_install.add_argument("--archive", action="store_true", help="treat pkgroot argument as archive to extract")

    p_remove = sub.add_parser("remove")
    p_remove.add_argument("pkg", help="package name to remove")
    p_remove.add_argument("--root", default="/", help="target root")
    p_remove.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    inst = ZeropkgInstaller()
    if args.cmd == "install":
        path = Path(args.pkgroot)
        if args.archive:
            res = inst.install_from_archive(path, pkg_name=args.name, version=args.version, root=args.root, fakeroot=args.fakeroot, use_chroot=args.chroot, dry_run=args.dry_run, create_binpkg=not args.no_binpkg)
        else:
            res = inst.install_from_build(args.name or path.stem, path, version=args.version, root=args.root, fakeroot=args.fakeroot, use_chroot=args.chroot, dry_run=args.dry_run, create_binpkg=not args.no_binpkg)
        print(json.dumps(res, indent=2))
    elif args.cmd == "remove":
        res = inst.remove(args.pkg, root=args.root, dry_run=args.dry_run)
        print(json.dumps(res, indent=2))
    else:
        parser.print_help()

if __name__ == "__main__":
    _cli()
