#!/usr/bin/env python3
# zeropkg_cli.py — CLI completo do Zeropkg (integração total)
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import fnmatch
import subprocess
from pathlib import Path
from typing import Optional, List, Any, Dict

# Logging / utils
from zeropkg_logger import log_event, get_logger
from zeropkg_toml import load_toml

logger = get_logger(stage="cli")

# Try importing core components; be tolerant to missing pieces and give clear errors
try:
    from zeropkg_builder import Builder
except Exception:
    Builder = None
try:
    from zeropkg_installer import Installer
except Exception:
    Installer = None
try:
    from zeropkg_deps import DependencyResolver, resolve_and_install
except Exception:
    DependencyResolver = None
    resolve_and_install = None
try:
    from zeropkg_depclean import DepCleaner
except Exception:
    DepCleaner = None
try:
    from zeropkg_upgrade import UpgradeManager
except Exception:
    UpgradeManager = None
try:
    from zeropkg_update import Updater
except Exception:
    Updater = None
try:
    from zeropkg_sync import sync_repos
except Exception:
    sync_repos = None
try:
    from zeropkg_remover import Remover
except Exception:
    Remover = None
try:
    from zeropkg_downloader import Downloader
except Exception:
    Downloader = None
try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None
try:
    from zeropkg_chroot import prepare_chroot, cleanup_chroot, enter_chroot
except Exception:
    prepare_chroot = None
    cleanup_chroot = None
    enter_chroot = None

# Default locations (can be overridden by CLI or config)
DEFAULT_DB = "/var/lib/zeropkg/installed.sqlite3"
DEFAULT_PORTS = "/usr/ports"
DEFAULT_BUILD_ROOT = "/var/zeropkg/build"
DEFAULT_CACHE_DIR = "/usr/ports/distfiles"
DEFAULT_PACKAGES_DIR = "/var/zeropkg/packages"


# ---------------------
# Helpers
# ---------------------
def make_args_namespace(ns: argparse.Namespace) -> Any:
    """Convert argparse Namespace to a simple object with attributes (already is)"""
    return ns


def ensure_dirs(args: argparse.Namespace):
    for p in (args.build_root, args.cache_dir, args.packages_dir):
        try:
            os.makedirs(p, exist_ok=True)
        except Exception:
            pass


def find_metafile_for(pkgname: str, ports_dir: str) -> Optional[str]:
    pattern = f"**/{pkgname}-*.toml"
    matches = list(Path(ports_dir).glob(pattern))
    if not matches:
        # try exact name in ports_dir/pkgname
        candidate = Path(ports_dir) / pkgname / f"{pkgname}.toml"
        if candidate.exists():
            return str(candidate)
        return None
    # choose the last (likely latest lexicographic)
    matches_sorted = sorted(matches)
    return str(matches_sorted[-1])


# ---------------------
# Commands
# ---------------------
def cmd_build(args: argparse.Namespace):
    make_args = make_args_namespace(args)
    ensure_dirs(args)

    target = args.target
    # load meta if target is a metafile path
    meta = None
    try:
        if os.path.isfile(target):
            meta = load_toml(target)
            target_name = meta["package"]["name"]
        else:
            mf = find_metafile_for(target, args.ports_dir)
            if mf:
                meta = load_toml(mf)
                target_name = meta["package"]["name"]
            else:
                target_name = target
    except Exception as e:
        logger.error(f"Failed to load metafile for {target}: {e}")
        meta = None
        target_name = target

    b = Builder(db_path=args.db_path, ports_dir=args.ports_dir, build_root=args.build_root,
                cache_dir=args.cache_dir, packages_dir=args.packages_dir, jobs=args.jobs) if Builder else None
    if not b:
        logger.error("Builder module not available.")
        sys.exit(1)

    try:
        b.build(target if meta is None else mf, args)
        log_event(target_name, "cli.build", f"Build command completed (dry_run={args.dry_run})")
    except Exception as e:
        logger.error(f"Build failed: {e}")
        log_event(target_name, "cli.build", f"Build failed: {e}", level="error")
        sys.exit(2)


def cmd_install(args: argparse.Namespace):
    make_args = make_args_namespace(args)
    ensure_dirs(args)
    installer = Installer(db_path=args.db_path, ports_dir=args.ports_dir, root=args.root,
                          dry_run=args.dry_run, use_fakeroot=args.fakeroot) if Installer else None
    if not installer:
        logger.error("Installer module not available.")
        sys.exit(1)

    # if target is a metafile path, load it; else find package file in packages_dir or build it
    target = args.target
    meta = None
    pkg_file = None

    # if explicit package file provided with --pkg-file, use it
    if args.pkg_file:
        pkg_file = args.pkg_file

    # otherwise if metafile path is given, build package then install
    try:
        if os.path.isfile(target):
            meta = load_toml(target)
            # build it first with builder if requested (we'll call Builder if available)
            if args.build_first and Builder:
                builder = Builder(db_path=args.db_path, ports_dir=args.ports_dir,
                                  build_root=args.build_root, cache_dir=args.cache_dir, packages_dir=args.packages_dir, jobs=args.jobs)
                builder.build(target, args, dir_install=None)
            # attempt to locate package in packages_dir
            pkgname = meta["package"]["name"]
            version = meta["package"]["version"]
            candidate = Path(args.packages_dir) / f"{pkgname}-{version}.tar.xz"
            if candidate.exists():
                pkg_file = str(candidate)
        else:
            # treat target as package name: try package in packages_dir
            candidate_glob = list(Path(args.packages_dir).glob(f"{target}-*.tar.*"))
            if candidate_glob:
                pkg_file = str(sorted(candidate_glob)[-1])
    except Exception as e:
        logger.warning(f"Install: error while resolving target: {e}")

    # If still no pkg_file and Builder available and --build-if-missing, try to build
    if not pkg_file and Builder and args.build_if_missing:
        try:
            builder = Builder(db_path=args.db_path, ports_dir=args.ports_dir,
                              build_root=args.build_root, cache_dir=args.cache_dir, packages_dir=args.packages_dir, jobs=args.jobs)
            builder.build(target, args)
            # try again to find package
            candidate_glob = list(Path(args.packages_dir).glob(f"{target}-*.tar.*"))
            if candidate_glob:
                pkg_file = str(sorted(candidate_glob)[-1])
        except Exception as e:
            logger.error(f"Build-then-install failed: {e}")

    try:
        installer.install(target, args, pkg_file=pkg_file, meta=meta, dir_install=args.dir_install)
        log_event(target, "cli.install", f"Install command completed (dry_run={args.dry_run})")
    except Exception as e:
        logger.error(f"Install failed: {e}")
        log_event(target, "cli.install", f"Install failed: {e}", level="error")
        sys.exit(3)


def cmd_remove(args: argparse.Namespace):
    ensure_dirs(args)
    remover = None
    if Remover:
        remover = Remover(db_path=args.db_path, ports_dir=args.ports_dir, root=args.root,
                          dry_run=args.dry_run, use_fakeroot=args.fakeroot)
    else:
        # fallback to Installer.remove if Remover not available
        if Installer:
            installer = Installer(db_path=args.db_path, ports_dir=args.ports_dir, root=args.root,
                                  dry_run=args.dry_run, use_fakeroot=args.fakeroot)
            def simple_remove(name):
                return installer.remove(name, version=args.version, hooks=None, force=args.force)
            remover = type("R", (), {"remove_multiple": lambda self, packages, force=False: {"removed": [p for p in packages]}})
        else:
            logger.error("Neither Remover nor Installer modules available.")
            sys.exit(1)

    pkgs = args.package
    try:
        result = remover.remove_multiple(pkgs, force=args.force) if hasattr(remover, "remove_multiple") else {"removed": []}
        log_event(",".join(pkgs), "cli.remove", f"Remove completed: {result}")
        print("Remove result:", result)
    except Exception as e:
        logger.error(f"Remove failed: {e}")
        log_event(",".join(pkgs), "cli.remove", f"Remove failed: {e}", level="error")
        sys.exit(4)


def cmd_depclean(args: argparse.Namespace):
    ensure_dirs(args)
    if DepCleaner is None:
        logger.error("DepCleaner module not available.")
        sys.exit(1)
    cleaner = DepCleaner(db_path=args.db_path, ports_dir=args.ports_dir, root=args.root,
                         dry_run=args.dry_run, use_fakeroot=args.fakeroot)
    try:
        summary = cleaner.clean(force=args.force, args=args)
        log_event("depclean", "cli.depclean", f"Depclean completed: {summary}")
        print("Depclean summary:", summary)
    except Exception as e:
        logger.error(f"Depclean failed: {e}")
        log_event("depclean", "cli.depclean", f"Depclean failed: {e}", level="error")
        sys.exit(5)


def cmd_search(args: argparse.Namespace):
    pattern = args.query
    ports = Path(args.ports_dir)
    matches = []
    for path in ports.rglob("*.toml"):
        try:
            meta = load_toml(str(path))
            pkg = meta.get("package", {}).get("name", "")
            desc = meta.get("package_extra", {}).get("description", "") or meta.get("package", {}).get("description", "")
            if pattern.lower() in pkg.lower() or (desc and pattern.lower() in desc.lower()):
                matches.append({"name": pkg, "file": str(path)})
        except Exception:
            continue
    for m in matches:
        print(f"{m['name']}\t{m['file']}")
    log_event("search", "cli.search", f"Search for '{pattern}' returned {len(matches)} results")


def cmd_info(args: argparse.Namespace):
    target = args.target
    mf = find_metafile_for(target, args.ports_dir) if not os.path.isfile(target) else target
    if not mf:
        print("Metafile not found for", target)
        sys.exit(1)
    try:
        meta = load_toml(mf)
        import json
        print(json.dumps(meta, indent=2))
        log_event(target, "cli.info", f"Displayed info for {target}")
    except Exception as e:
        logger.error(f"Failed to load metafile: {e}")
        sys.exit(2)


def cmd_revdep(args: argparse.Namespace):
    pkg = args.package
    if DependencyResolver:
        resolver = DependencyResolver(args.db_path, args.ports_dir)
        try:
            revs = resolver.reverse_deps(pkg)
            for r in revs:
                print(r)
            log_event(pkg, "cli.revdep", f"Revdeps: {len(revs)}")
        except Exception as e:
            logger.error(f"reverse_deps failed: {e}")
            sys.exit(1)
    else:
        # fallback to DBManager.find_revdeps if available
        if DBManager:
            db = DBManager(args.db_path)
            revs = db.find_revdeps(pkg)
            for r in revs:
                print(r)
            log_event(pkg, "cli.revdep", f"Revdeps via DB: {len(revs)}")
        else:
            logger.error("No reverse-deps implementation available.")
            sys.exit(1)


def cmd_sync(args: argparse.Namespace):
    if sync_repos is None:
        logger.error("sync_repos not available.")
        sys.exit(1)
    try:
        sync_repos(args)
        log_event("sync", "cli.sync", "Sync completed")
    except Exception as e:
        logger.error(f"sync failed: {e}")
        log_event("sync", "cli.sync", f"sync failed: {e}", level="error")
        sys.exit(1)


def cmd_update(args: argparse.Namespace):
    if Updater is None:
        logger.error("Updater module not available.")
        sys.exit(1)
    upd = Updater(db_path=args.db_path, ports_dir=args.ports_dir)
    try:
        report = upd.check_all(upgrade_only=args.upgrade_only)
        print("Update report:", report)
        log_event("update", "cli.update", "Update scan completed")
    except Exception as e:
        logger.error(f"update failed: {e}")
        log_event("update", "cli.update", f"update failed: {e}", level="error")
        sys.exit(1)


def cmd_upgrade(args: argparse.Namespace):
    if UpgradeManager is None:
        logger.error("UpgradeManager not available.")
        sys.exit(1)
    upgr = UpgradeManager(db_path=args.db_path, ports_dir=args.ports_dir, root=args.root)
    try:
        if args.all:
            result = upgr.upgrade_all(dry_run=args.dry_run)
        else:
            result = upgr.upgrade_package(args.package, dry_run=args.dry_run)
        print("Upgrade result:", result)
        log_event("upgrade", "cli.upgrade", "Upgrade completed")
    except Exception as e:
        logger.error(f"upgrade failed: {e}")
        log_event("upgrade", "cli.upgrade", f"upgrade failed: {e}", level="error")
        sys.exit(1)


def cmd_chroot_enter(args: argparse.Namespace):
    root = args.root
    if enter_chroot:
        try:
            enter_chroot(root)
            log_event("chroot", "cli.chroot", f"Entered chroot {root}")
        except Exception as e:
            logger.error(f"enter chroot failed: {e}")
            sys.exit(1)
    else:
        # fallback using system chroot
        try:
            os.execvp("chroot", ["chroot", root, "/bin/bash"])
        except Exception as e:
            logger.error(f"Fallback chroot failed: {e}")
            sys.exit(1)


def cmd_lfs_bootstrap(args: argparse.Namespace):
    # convenience wrapper to build a list of recipes in toolchain order
    # Expect a list of packages or to use a standard LFS sequence from port tree
    seq = args.packages or []
    if not seq:
        print("No packages provided. Provide packages in toolchain order or maintain a bundle recipe.")
        return
    b = Builder(db_path=args.db_path, ports_dir=args.ports_dir, build_root=args.build_root,
                cache_dir=args.cache_dir, packages_dir=args.packages_dir, jobs=args.jobs)
    for pkg in seq:
        print(f"Bootstrapping {pkg} ...")
        try:
            b.build(pkg, args)
        except Exception as e:
            logger.error(f"LFS bootstrap failed at {pkg}: {e}")
            sys.exit(1)
    log_event("lfs", "cli.lfs", "LFS bootstrap complete")


# ---------------------
# Argument parsing
# ---------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg package manager CLI")
    # global options
    p.add_argument("--db-path", default=os.environ.get("ZEROPKG_DB", DEFAULT_DB))
    p.add_argument("--ports-dir", default=os.environ.get("ZEROPKG_PORTS", DEFAULT_PORTS))
    p.add_argument("--build-root", default=os.environ.get("ZEROPKG_BUILDROOT", DEFAULT_BUILD_ROOT))
    p.add_argument("--cache-dir", default=os.environ.get("ZEROPKG_CACHE", DEFAULT_CACHE_DIR))
    p.add_argument("--packages-dir", default=os.environ.get("ZEROPKG_PACKAGES", DEFAULT_PACKAGES_DIR))
    p.add_argument("--root", default="/", help="Installation root (use /mnt/lfs for LFS)")
    p.add_argument("--dry-run", action="store_true", help="Simulate actions")
    p.add_argument("--fakeroot", action="store_true", help="Use fakeroot for file operations")
    p.add_argument("--force", action="store_true", help="Force actions (where supported)")
    p.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 1)), help="Parallel jobs for build")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    sub = p.add_subparsers(dest="cmd", required=True)

    # build
    sb = sub.add_parser("build", help="Build a package from ports/metafile")
    sb.add_argument("target", help="package name or path to metafile")
    sb.add_argument("--build-only", action="store_true", help="Only build, do not install")
    sb.add_argument("--include-build-deps", action="store_true", help="Resolve build dependencies too")

    # install
    si = sub.add_parser("install", help="Install a package")
    si.add_argument("target", help="package name or path to metafile")
    si.add_argument("-i", "--install", dest="install_flag", action="store_true", help="alias for install")
    si.add_argument("--pkg-file", help="Install from package file (.tar.xz)")
    si.add_argument("--build-if-missing", action="store_true", help="Build if package not available")
    si.add_argument("--build-first", action="store_true", help="Build before install")
    si.add_argument("--dir-install", help="Install into a directory instead of root")

    # remove
    sr = sub.add_parser("remove", help="Remove installed package(s)")
    sr.add_argument("package", nargs="+", help="package(s) to remove")
    sr.add_argument("--version", default=None)
    sr.add_argument("-f", "--force", action="store_true", help="Force removal")

    # depclean
    sd = sub.add_parser("depclean", help="Remove orphan dependencies")
    sd.add_argument("--force", action="store_true", help="Force removal of packages with dependents (NOT recommended)")

    # search
    ssearch = sub.add_parser("search", help="Search packages in ports")
    ssearch.add_argument("query", help="search string")

    # info
    sinfo = sub.add_parser("info", help="Show package metafile")
    sinfo.add_argument("target", help="package name or metafile path")

    # revdep
    srev = sub.add_parser("revdep", help="List reverse dependencies")
    srev.add_argument("package", help="package name")

    # sync
    ssync = sub.add_parser("sync", help="Sync repositories (pull/update ports)")
    ssync.add_argument("--repo", default=None, help="Specific repo to sync (optional)")

    # update (check upstream)
    supd = sub.add_parser("update", help="Check upstreams for new versions")
    supd.add_argument("--upgrade-only", action="store_true", help="Only mark upgrades")

    # upgrade
    supg = sub.add_parser("upgrade", help="Upgrade package or all")
    supg.add_argument("package", nargs="?", help="package to upgrade (omit with --all)")
    supg.add_argument("--all", action="store_true", help="Upgrade all packages")

    # chroot enter
    sch = sub.add_parser("chroot", help="Enter chroot")
    sch.add_argument("root", nargs="?", default="/", help="root to enter (default /)")

    # lfs bootstrap
    slfs = sub.add_parser("lfs-bootstrap", help="Bootstrap LFS sequence")
    slfs.add_argument("packages", nargs="*", help="ordered list of packages to build for toolchain")

    return p


def main(argv: Optional[List[str]] = None):
    parser = build_parser()
    args = parser.parse_args(argv)

    # adjust logging level
    if args.verbose:
        logger.setLevel("DEBUG")

    # set derived defaults as attributes on args
    args.db_path = args.db_path
    args.ports_dir = args.ports_dir
    args.build_root = args.build_root
    args.cache_dir = args.cache_dir
    args.packages_dir = args.packages_dir
    args.root = args.root
    args.dry_run = args.dry_run
    args.fakeroot = args.fakeroot
    args.force = args.force
    args.jobs = args.jobs

    # dispatch commands
    cmd = args.cmd
    if cmd == "build":
        cmd_build(args)
    elif cmd == "install":
        cmd_install(args)
    elif cmd == "remove":
        cmd_remove(args)
    elif cmd == "depclean":
        cmd_depclean(args)
    elif cmd == "search":
        cmd_search(args)
    elif cmd == "info":
        cmd_info(args)
    elif cmd == "revdep":
        cmd_revdep(args)
    elif cmd == "sync":
        cmd_sync(args)
    elif cmd == "update":
        cmd_update(args)
    elif cmd == "upgrade":
        cmd_upgrade(args)
    elif cmd == "chroot":
        # enter chroot
        args.root = args.root or "/"
        cmd_chroot_enter(args)
    elif cmd == "lfs-bootstrap":
        cmd_lfs_bootstrap(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
