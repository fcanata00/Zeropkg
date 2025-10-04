#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_builder.py — Zeropkg build system
Full-featured builder integrated with config, downloader, patcher, chroot, deps, db, installer and vuln.
"""

from __future__ import annotations
import os
import sys
import json
import shutil
import tempfile
import subprocess
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# Try to import project modules; fallback safe shims
try:
    from zeropkg_config import load_config
except Exception:
    def load_config(path=None):
        return {
            "paths": {
                "build_root": "/var/tmp/zeropkg/build",
                "cache_dir": "/var/cache/zeropkg",
                "binpkg_dir": "/var/cache/zeropkg/binpkgs",
                "distfiles": "/usr/ports/distfiles",
                "logs": "/var/log/zeropkg",
                "root": "/"
            },
            "build": {"jobs": 1, "use_chroot": True, "fakeroot_default": False}
        }

try:
    from zeropkg_logger import get_logger
    log = get_logger("builder")
except Exception:
    import logging
    log = logging.getLogger("zeropkg_builder")
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(h)
    log.setLevel(logging.INFO)

try:
    from zeropkg_downloader import Downloader
except Exception:
    Downloader = None

try:
    from zeropkg_patcher import Patcher
except Exception:
    Patcher = None

try:
    from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot
except Exception:
    def prepare_chroot(rootfs: Path):
        return []
    def cleanup_chroot(rootfs: Path, mounts: list):
        return
    def run_in_chroot(cfg, cmd):
        raise RuntimeError("zeropkg_chroot.run_in_chroot not available")

try:
    from zeropkg_db import record_install_quick, get_manifest_quick, list_installed_quick
except Exception:
    record_install_quick = None
    get_manifest_quick = None
    list_installed_quick = None

try:
    from zeropkg_installer import ZeropkgInstaller
except Exception:
    ZeropkgInstaller = None

try:
    from zeropkg_deps import DepsManager
except Exception:
    DepsManager = None

try:
    from zeropkg_vuln import ZeroPKGVulnManager
except Exception:
    ZeroPKGVulnManager = None

try:
    from zeropkg_toml import ZeropkgTOML, resolve_macros
except Exception:
    ZeropkgTOML = None
    def resolve_macros(x, env=None): return x

# Utilities
def sh_check(cmd, cwd=None, env=None, capture=False, shell=False):
    log.debug(f"RUN: {' '.join(cmd) if isinstance(cmd, (list,tuple)) else cmd} (cwd={cwd})")
    if capture:
        res = subprocess.run(cmd, cwd=cwd, env=env, shell=shell,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        return res.returncode, res.stdout, res.stderr
    else:
        subprocess.run(cmd, cwd=cwd, env=env, shell=shell, check=True)
        return 0, "", ""

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

# Main builder
class ZeropkgBuilder:
    def __init__(self, config_path: Optional[str] = None):
        self.cfg = load_config(config_path)
        self.build_root = Path(self.cfg["paths"].get("build_root", "/var/tmp/zeropkg/build")).resolve()
        self.cache_dir = Path(self.cfg["paths"].get("cache_dir", "/var/cache/zeropkg")).resolve()
        self.binpkg_dir = Path(self.cfg["paths"].get("binpkg_dir", str(self.cache_dir / "binpkgs"))).resolve()
        self.distfiles = Path(self.cfg["paths"].get("distfiles", "/usr/ports/distfiles")).resolve()
        self.logs_dir = Path(self.cfg["paths"].get("logs", "/var/log/zeropkg")).resolve()
        self.root_dir = Path(self.cfg["paths"].get("root", "/")).resolve()
        safe_mkdir(self.build_root)
        safe_mkdir(self.cache_dir)
        safe_mkdir(self.binpkg_dir)
        safe_mkdir(self.distfiles)
        safe_mkdir(self.logs_dir)
        # integrations
        self.downloader = Downloader(self.cfg) if Downloader else None
        self.patcher = Patcher(self.build_root, self.cfg, fakeroot=False) if Patcher else None
        self.installer = ZeropkgInstaller() if ZeropkgInstaller else None
        self.deps = DepsManager() if DepsManager else None
        self.vuln = ZeroPKGVulnManager() if ZeroPKGVulnManager else None
        self.toml = ZeropkgTOML() if ZeropkgTOML else None
        self.jobs = int(self.cfg.get("build", {}).get("jobs", 1))
        self.use_chroot_default = bool(self.cfg.get("build", {}).get("use_chroot", True))
        self.fakeroot_default = bool(self.cfg.get("build", {}).get("fakeroot_default", False))

    # ---------------------------
    # High-level build entrypoints
    # ---------------------------
    def build(self, recipe_path: str, *,
              use_chroot: Optional[bool] = None,
              fakeroot: Optional[bool] = None,
              dry_run: bool = False,
              use_cache: bool = True,
              dir_install: Optional[str] = None,
              force_rebuild: bool = False) -> Dict[str,Any]:
        """
        Build a single recipe (path to .toml). Returns dict with status and artifacts.
        """
        recipe_path = Path(recipe_path)
        log.info(f"Starting build for recipe: {recipe_path}")
        if not recipe_path.exists():
            raise FileNotFoundError(recipe_path)

        # read recipe
        recipe = self._load_recipe(recipe_path)

        # check vuln (if integration enabled)
        if self.vuln:
            vulns = self.vuln.vulndb.get_vulns(recipe["package"]["name"])
            critical = [v for v in vulns if v.get("severity") == "critical"]
            if critical:
                log.warning(f"Package {recipe['package']['name']} has critical vulnerabilities: {len(critical)}. Aborting build.")
                return {"ok": False, "error": "vulnerable", "details": critical}

        use_chroot = self.use_chroot_default if use_chroot is None else bool(use_chroot)
        fakeroot = self.fakeroot_default if fakeroot is None else bool(fakeroot)

        # resolve dependencies (unless skip)
        if self.deps and not recipe.get("options", {}).get("skip_deps", False):
            try:
                deps_result = self.deps.resolve([recipe["package"]["name"]])
                if not deps_result["ok"]:
                    log.warning(f"Dependency resolution reported cycles: {deps_result.get('cycles')}")
                # Optionally build deps first if requested
                if recipe.get("options", {}).get("build_deps", True):
                    log.info("Building dependencies first (resolve_and_build)...")
                    self.deps.resolve_and_build([recipe["package"]["name"]], jobs=self.jobs, dry_run=dry_run)
            except Exception as e:
                log.warning(f"Dependency resolver not available or failed: {e}")

        # prepare build workdir
        with tempfile.TemporaryDirectory(prefix=f"zeropkg-build-{recipe['package']['name']}-") as td:
            workdir = Path(td)
            build_dir = workdir / "build"
            safe_mkdir(build_dir)
            src_dir = workdir / "src"
            safe_mkdir(src_dir)
            logs = self.logs_dir / f"{recipe['package']['name']}-{int(time.time())}.log"
            summary = {"package": recipe["package"]["name"], "version": recipe["package"].get("version"), "stages": []}

            try:
                # fetch sources (downloader handles mirrors + cache)
                if dry_run:
                    log.info("[dry-run] fetch sources")
                else:
                    self._fetch_sources(recipe, src_dir, use_cache=use_cache)

                # extract
                if dry_run:
                    log.info("[dry-run] extract sources")
                else:
                    self._extract_sources(recipe, src_dir, build_dir)

                # apply patches
                if dry_run:
                    log.info("[dry-run] apply patches")
                else:
                    self._apply_patches(recipe, build_dir, fakeroot=fakeroot)

                # prepare chroot
                tmp_mounts = []
                if use_chroot:
                    log.info("Preparing chroot for build")
                    tmp_mounts = prepare_chroot(self.root_dir)
                    # copy resolv.conf to chroot for network access
                    try:
                        resolv_src = Path("/etc/resolv.conf")
                        resolv_dst = self.root_dir / "etc" / "resolv.conf"
                        resolv_dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(resolv_src, resolv_dst)
                    except Exception:
                        log.debug("Could not copy resolv.conf into chroot (may not be necessary)")

                # run build commands (configure/make/make install or commands in recipe)
                build_output_dir = build_dir / "pkgroot"
                safe_mkdir(build_output_dir)
                env = os.environ.copy()
                # merge environment from recipe
                for k,v in recipe.get("build", {}).get("environment", {}).items():
                    env[k] = resolve_macros(str(v), env)

                # choose actual build directory (if recipe has subdir)
                src_sub = recipe.get("extract_to") or recipe.get("build", {}).get("source_subdir")
                actual_src = build_dir
                if src_sub:
                    actual_src = build_dir / src_sub

                build_ok = True
                try:
                    if dry_run:
                        log.info("[dry-run] would run build commands")
                    else:
                        self._run_build_steps(recipe, actual_src, build_output_dir, env, logs, fakeroot, use_chroot, dir_install)
                    summary["stages"].append("built")
                except Exception as e:
                    build_ok = False
                    summary["error"] = str(e)
                    log.error(f"Build failed: {e}")
                    raise

                # packaging
                if build_ok and not dry_run:
                    pkg_archive = self._pack_binary(recipe, build_output_dir)
                    summary["pkg_archive"] = str(pkg_archive)
                    summary["ok"] = True
                    # register in DB
                    try:
                        if record_install_quick:
                            manifest = self._build_manifest_from_pkgroot(build_output_dir)
                            record_install_quick(f"{recipe['package']['name']}-{recipe['package'].get('version','unknown')}", manifest, deps=[], metadata={"archive": str(pkg_archive)})
                    except Exception as e:
                        log.warning(f"DB registration failed: {e}")

                    # install from binpkg if requested / installer integration
                    if dir_install:
                        log.info(f"Installing package into dir: {dir_install}")
                        self._install_to_dir(pkg_archive, dir_install, fakeroot=fakeroot)
                    else:
                        # try to perform real installation into system (via installer) if requested and installer available
                        if self.installer and recipe.get("options", {}).get("auto_install", False):
                            log.info("Installing package via ZeropkgInstaller")
                            manifest = self._build_manifest_from_pkgroot(build_output_dir)
                            self.installer.install(recipe["package"]["name"], build_output_dir, manifest, recipe["package"].get("version","unknown"), fakeroot_mode=fakeroot, use_chroot=use_chroot)
                summary["finished_at"] = int(time.time())
                return summary
            finally:
                # cleanup chroot mounts
                if use_chroot:
                    try:
                        cleanup_chroot(self.root_dir, tmp_mounts)
                    except Exception as e:
                        log.warning(f"cleanup_chroot failed: {e}")

    # ---------------------------
    # Toolchain utilities (LFS bootstrapping)
    # ---------------------------
    def build_toolchain(self, world_list: List[str], *, lfs_root: Optional[str] = None, dry_run: bool = False):
        """
        High level helper to build a LFS toolchain sequence.
        world_list: ordered list of recipe paths/names to build for toolchain.
        lfs_root: chroot /mnt/lfs path
        """
        lfs_root = Path(lfs_root or self.cfg["paths"].get("root", "/mnt/lfs"))
        log.info(f"Starting toolchain build into {lfs_root}")
        for item in world_list:
            # item may be recipe path or package name -> try to find recipe
            recipe_path = Path(item) if Path(item).exists() else self._find_recipe_for_pkg(item)
            if not recipe_path:
                log.error(f"Recipe for {item} not found; aborting toolchain build")
                return {"ok": False, "error": f"recipe-not-found:{item}"}
            res = self.build(str(recipe_path), use_chroot=True, fakeroot=True, dry_run=dry_run, dir_install=str(lfs_root))
            if not res.get("ok"):
                log.error(f"Toolchain build failed for {item}: {res.get('error')}")
                return {"ok": False, "failed": item, "details": res}
        return {"ok": True}

    # ---------------------------
    # Build world: sequence build of many packages
    # ---------------------------
    def build_world(self, world_file: Optional[str] = None, *, dry_run: bool = False):
        """
        Build a world file: text file with list of recipe paths/names (one per line).
        """
        if not world_file:
            raise ValueError("world_file required")
        with open(world_file, "r", encoding="utf-8") as f:
            items = [l.strip() for l in f.readlines() if l.strip() and not l.startswith("#")]
        results = {}
        for it in items:
            recipe_path = Path(it) if Path(it).exists() else self._find_recipe_for_pkg(it)
            if not recipe_path:
                results[it] = {"ok": False, "error": "recipe-not-found"}
                continue
            results[it] = self.build(str(recipe_path), dry_run=dry_run)
        return results

    # ---------------------------
    # Internal helpers
    # ---------------------------
    def _load_recipe(self, recipe_path: Path) -> Dict[str,Any]:
        if self.toml:
            try:
                return self.toml.load(recipe_path)
            except Exception as e:
                log.warning(f"TOML parser failed: {e}; falling back to minimal parse")
        # fallback minimal recipe
        data = {"package": {"name": recipe_path.stem, "version": None}, "sources": [], "patches": [], "build": {"commands": []}, "options": {}}
        return data

    def _fetch_sources(self, recipe: Dict[str,Any], src_dir: Path, *, use_cache: bool = True):
        sources = recipe.get("sources", []) or []
        if not sources:
            log.info("No sources defined in recipe")
            return
        if not self.downloader:
            raise RuntimeError("Downloader not available")
        for src in sources:
            # downloader returns path in distfiles/cache
            p = self.downloader.fetch_all([src], jobs=self.jobs, dry_run=False)
            # fetch_all returns list of futures results; ensure returned path or list
        return True

    def _extract_sources(self, recipe: Dict[str,Any], src_dir: Path, build_dir: Path):
        # For simplicity: extract everything in distfiles found with downloader into build_dir
        for src in recipe.get("sources", []) or []:
            file_name = src.get("url") if isinstance(src, dict) else src
            # try resolve name
            if isinstance(file_name, dict):
                file_name = file_name.get("url")
            if not file_name:
                continue
            distname = os.path.basename(file_name.split("@")[0])
            possible = list(self.distfiles.glob(f"*{distname}*"))
            if not possible:
                # maybe downloader put it in cache_dir
                possible = list(self.cache_dir.glob(f"*{distname}*"))
            if not possible:
                log.warning(f"Source {distname} not found in distfiles or cache")
                continue
            # extract first match
            srcpath = possible[0]
            try:
                if srcpath.suffix in (".zip",):
                    import zipfile
                    with zipfile.ZipFile(srcpath, "r") as z:
                        z.extractall(build_dir)
                else:
                    import tarfile
                    with tarfile.open(srcpath, "r:*") as tar:
                        safe_names = [m for m in tar.getmembers() if not (m.name.startswith("/") or ".." in m.name)]
                        tar.extractall(build_dir, members=safe_names)
                log.info(f"Extracted {srcpath} -> {build_dir}")
            except Exception as e:
                log.error(f"Extraction failed for {srcpath}: {e}")

    def _apply_patches(self, recipe: Dict[str,Any], build_dir: Path, *, fakeroot: bool=False):
        patches = recipe.get("patches", []) or []
        if not patches:
            return
        if not self.patcher:
            raise RuntimeError("Patcher not available")
        # Patcher expects work_dir; create adapter
        p = Patcher(build_dir, self.cfg, fakeroot=fakeroot)
        p.apply_all(recipe["package"]["name"], {"patches": patches, "hooks": recipe.get("hooks", {})})

    def _run_build_steps(self, recipe: Dict[str,Any], src_dir: Path, pkgroot_dir: Path, env: Dict[str,str], logs: Path, fakeroot: bool, use_chroot: bool, dir_install: Optional[str]):
        """
        Standard sequence:
         - configure (if present)
         - make (-j)
         - make install into pkgroot_dir (DESTDIR)
        Also respects recipe['build']['commands'] for custom sequences.
        """
        # default make -j
        jobs = self.jobs or 1
        build_commands = recipe.get("build", {}).get("commands") or []
        if not build_commands:
            # try autodetect typical sequence
            if (src_dir / "configure").exists():
                build_commands = [
                    f"./configure --prefix=/usr {' '.join(recipe.get('build', {}).get('configure_args', []))}"
                ]
            if (src_dir / "Makefile").exists() or any(src_dir.glob("**/Makefile")):
                build_commands.append(f"make -j{jobs}")
                if dir_install:
                    build_commands.append(f"make DESTDIR={dir_install} install")
                else:
                    build_commands.append(f"make DESTDIR={pkgroot_dir} install")
        # run commands in order
        for cmd in build_commands:
            # expand macros
            cmd = resolve_macros(cmd, env)
            log.info(f"BUILD CMD: {cmd}")
            if use_chroot:
                # run the command inside chroot (we assume src_dir is available inside chroot path)
                try:
                    run_in_chroot(self.cfg, cmd)
                except Exception as e:
                    # fallback: try running locally in src_dir
                    log.warning(f"run_in_chroot failed: {e}; trying local run")
                    sh_check(cmd, cwd=str(src_dir), shell=True, env=env)
            else:
                if fakeroot:
                    # wrap with fakeroot
                    sh_check(["fakeroot", "sh", "-c", cmd], cwd=str(src_dir), env=env)
                else:
                    sh_check(cmd, cwd=str(src_dir), shell=True, env=env)
        return True

    def _pack_binary(self, recipe: Dict[str,Any], pkgroot_dir: Path) -> Path:
        """
        Create binary package from pkgroot_dir and store in binpkg_dir.
        """
        pkgname = recipe["package"]["name"]
        version = recipe["package"].get("version", "0")
        timestamp = int(time.time())
        archive_name = f"{pkgname}-{version}-{timestamp}.tar.zst"
        archive_path = self.binpkg_dir / archive_name
        # prefer zstd if available
        try:
            import subprocess
            if shutil.which("zstd"):
                # create tar and pipe to zstd
                tar_path = str(self.cache_dir / f"{pkgname}-{version}.tar")
                sh_check(["tar", "-cf", tar_path, "-C", str(pkgroot_dir), "."], cwd=str(pkgroot_dir))
                sh_check(["zstd", "-q", "-19", tar_path, "-o", str(archive_path)])
                Path(tar_path).unlink(missing_ok=True)
            else:
                sh_check(["tar", "-C", str(pkgroot_dir), "-cJf", str(archive_path), "."])
        except Exception:
            # fallback python tar
            import tarfile
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(str(pkgroot_dir), arcname=".")
        log.info(f"Packaged binary to {archive_path}")
        return archive_path

    def _build_manifest_from_pkgroot(self, pkgroot_dir: Path) -> Dict[str, List[str]]:
        """
        Build a simple manifest grouped by category (bin, lib, etc.) from pkgroot.
        """
        manifest = {}
        for p in pkgroot_dir.rglob("*"):
            if p.is_dir():
                continue
            rel = "/" + str(p.relative_to(pkgroot_dir)).lstrip("./")
            cat = "misc"
            if rel.startswith("/usr/bin") or rel.startswith("/bin"):
                cat = "bin"
            elif rel.startswith("/usr/lib") or rel.startswith("/lib"):
                cat = "lib"
            manifest.setdefault(cat, []).append(rel)
        return manifest

    def _install_to_dir(self, archive_path: Path, target_dir: str, *, fakeroot: bool=False):
        """
        Extract binpkg archive into target_dir (used for dir-install option).
        """
        target = Path(target_dir)
        safe_mkdir(target)
        log.info(f"Installing archive {archive_path} into {target}")
        try:
            # prefer zstd
            if str(archive_path).endswith(".zst"):
                tmp = tempfile.TemporaryDirectory()
                tmpf = Path(tmp.name) / "pkg.tar"
                # decompress
                sh_check(["zstd", "-d", str(archive_path), "-o", str(tmpf)])
                sh_check(["tar", "-xf", str(tmpf), "-C", str(target)])
            else:
                sh_check(["tar", "-xzf", str(archive_path), "-C", str(target)])
        except Exception as e:
            log.error(f"Failed to extract archive into {target}: {e}")

    # ---------------------------
    # Utilities for locating recipes
    # ---------------------------
    def _find_recipe_for_pkg(self, pkgname: str) -> Optional[Path]:
        # look for {pkgname}.toml under /usr/ports
        ports = Path(self.cfg["paths"].get("recipes_dir", "/usr/ports"))
        candidates = list(ports.rglob(f"{pkgname}*.toml"))
        return candidates[0] if candidates else None

# ---------------------------
# CLI wrapper
# ---------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-builder", description="Zeropkg builder")
    parser.add_argument("--recipe", "-r", help="Path to recipe TOML or package name", required=False)
    parser.add_argument("--build-world", help="Path to world file (one recipe per line)", required=False)
    parser.add_argument("--toolchain", action="store_true", help="Build LFS toolchain (requires ordered list in --recipe or world)")
    parser.add_argument("--dir-install", help="Install into target directory after build (DESTDIR style)", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-chroot", action="store_true")
    parser.add_argument("--fakeroot", action="store_true")
    parser.add_argument("--use-cache", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--jobs", "-j", type=int, default=None)
    args = parser.parse_args()

    builder = ZeropkgBuilder()
    if args.recipe:
        # recipe may be name or path
        rp = Path(args.recipe)
        if not rp.exists():
            rp = builder._find_recipe_for_pkg(args.recipe)
            if not rp:
                print("Recipe not found")
                sys.exit(2)
        res = builder.build(str(rp), use_chroot=not args.no_chroot, fakeroot=args.fakeroot, dry_run=args.dry_run, dir_install=args.dir_install, use_cache=args.use_cache, force_rebuild=args.rebuild)
        print(json.dumps(res, indent=2))
    elif args.build_world:
        res = builder.build_world(args.build_world, dry_run=args.dry_run)
        print(json.dumps(res, indent=2))
    elif args.toolchain:
        # expects recipe param or world file
        if args.recipe:
            world = [l.strip() for l in open(args.recipe).read().splitlines() if l.strip()]
            res = builder.build_toolchain(world, dry_run=args.dry_run)
            print(json.dumps(res, indent=2))
        else:
            print("Provide ordered list via --recipe (file) for toolchain")
            sys.exit(2)
    else:
        parser.print_help()

if __name__ == "__main__":
    _cli()
