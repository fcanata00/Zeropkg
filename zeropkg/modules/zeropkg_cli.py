#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_cli.py - Entrypoint CLI para Zeropkg (completo, integrado)
Suporta:
 - build, install, remove, upgrade, update, sync, depclean, deps graph, vuln, patch, chroot, downloader, db, logger
 - abreviações e long options
 - --dry-run, --jobs/-j, --root, --use-chroot/--no-chroot, --fakeroot
 - comandos compostos (build --with-deps, build-world, build-toolchain)
"""
from __future__ import annotations
import sys
import os
import argparse
import json
import shutil
import time
from pathlib import Path
from typing import Optional, List, Any, Dict

# --------------------
# Safe import helper
# --------------------
def safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

# Load optional modules (may be present in /usr/lib/zeropkg/modules)
config_mod = safe_import("zeropkg_config")
logger_mod = safe_import("zeropkg_logger")
db_mod = safe_import("zeropkg_db")
builder_mod = safe_import("zeropkg_builder")
installer_mod = safe_import("zeropkg_installer")
downloader_mod = safe_import("zeropkg_downloader")
patcher_mod = safe_import("zeropkg_patcher")
chroot_mod = safe_import("zeropkg_chroot")
deps_mod = safe_import("zeropkg_deps")
depclean_mod = safe_import("zeropkg_depclean")
remover_mod = safe_import("zeropkg_remover")
upgrade_mod = safe_import("zeropkg_upgrade")
update_mod = safe_import("zeropkg_update")
vuln_mod = safe_import("zeropkg_vuln")
sync_mod = safe_import("zeropkg_sync")

# --------------------
# Small logging wrapper (uses zeropkg_logger if available)
# --------------------
def log(evt: str, msg: str, level: str = "INFO", metadata: Optional[Dict[str,Any]] = None):
    if logger_mod and hasattr(logger_mod, "log_event"):
        try:
            logger_mod.log_event(evt, msg, level=level, metadata=metadata)
            return
        except Exception:
            pass
    # fallback
    prefix = f"[{level}] {evt}:"
    if level == "ERROR":
        print(f"{prefix} {msg}", file=sys.stderr)
    else:
        print(f"{prefix} {msg}")

# --------------------
# Helpers to call modules with fallbacks
# --------------------
def get_config():
    if config_mod and hasattr(config_mod, "get_config_manager"):
        try:
            return config_mod.get_config_manager().config
        except Exception:
            pass
    # fallback default config structure
    return {
        "paths": {
            "distfiles_dir": "/usr/ports/distfiles",
            "state_dir": "/var/lib/zeropkg",
            "log_dir": "/var/log/zeropkg"
        },
        "chroot": {"default_profile": "lfs"},
        "build": {"use_fakeroot": False}
    }

CONFIG = get_config()

# builder wrapper
def call_builder_build(recipe: str, **kwargs):
    if not builder_mod:
        log("builder", "builder module not available", "ERROR")
        return {"ok": False, "error": "no_builder"}
    try:
        Builder = builder_mod.ZeropkgBuilder if hasattr(builder_mod, "ZeropkgBuilder") else builder_mod
        builder = Builder(config=CONFIG) if hasattr(builder_mod, "ZeropkgBuilder") else builder_mod
        return builder.build_package(recipe, **kwargs)
    except Exception as e:
        log("builder", f"exception: {e}", "ERROR")
        return {"ok": False, "error": str(e)}

# installer wrapper
def call_installer_install_from_archive(archive: str, root: str = "/", fakeroot: bool = False):
    if installer_mod:
        try:
            if hasattr(installer_mod, "Installer"):
                inst = installer_mod.Installer()
                if hasattr(inst, "install_from_archive"):
                    return inst.install_from_archive(archive, root=root, fakeroot=fakeroot)
            if hasattr(installer_mod, "install_from_archive"):
                return installer_mod.install_from_archive(archive, root=root, fakeroot=fakeroot)
        except Exception as e:
            log("installer", f"install_from_archive failed: {e}", "ERROR")
            return {"ok": False, "error": str(e)}
    # fallback naive
    try:
        tmp = Path("/tmp/zeropkg-install-tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True, exist_ok=True)
        import tarfile
        with tarfile.open(archive, "r:*") as tf:
            tf.extractall(str(tmp))
        # copy to root
        for p in tmp.rglob("*"):
            rel = p.relative_to(tmp)
            dest = Path(root) / rel
            if p.is_dir():
                dest.mkdir(parents=True, exist_ok=True)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(p), str(dest))
        shutil.rmtree(tmp, ignore_errors=True)
        return {"ok": True, "method": "fallback_copy"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# remover wrapper
def call_remove(pkg_name: str, do_it: bool = False, force: bool=False, with_dependents: bool=False, no_backup: bool=False):
    if remover_mod and hasattr(remover_mod, "remove_package_cli"):
        try:
            return remover_mod.remove_package_cli(pkg_name, do_it=do_it, force=force, with_dependents=with_dependents, no_backup=no_backup)
        except Exception as e:
            log("remover", f"remove_package_cli failed: {e}", "ERROR")
            return {"ok": False, "error": str(e)}
    # fallback: use db to remove metadata only
    if db_mod and hasattr(db_mod, "remove_package_quick"):
        try:
            if not do_it:
                return {"ok": True, "dry_run": True, "pkg": pkg_name}
            return db_mod.remove_package_quick(pkg_name)
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no_remover_no_db"}

# depclean wrapper
def call_depclean(apply: bool=False, only: Optional[List[str]]=None, exclude: Optional[List[str]]=None, keep: Optional[List[str]]=None, parallel: bool=True, max_workers: Optional[int]=None, backup: Optional[bool]=None, report_tag: Optional[str]=None):
    if not depclean_mod:
        return {"ok": False, "error": "depclean_not_available"}
    try:
        Depclean = depclean_mod.Depclean if hasattr(depclean_mod, "Depclean") else depclean_mod
        dc = Depclean()
        return dc.execute(apply=apply, only=only, exclude=exclude, keep=keep, parallel=parallel, max_workers=max_workers, backup=backup, report_tag=report_tag)
    except Exception as e:
        return {"ok": False, "error": str(e)}

# deps graph
def call_deps_graph(out: Optional[str] = None, dot: bool=False):
    if not deps_mod:
        return {"ok": False, "error": "deps_not_available"}
    try:
        if hasattr(deps_mod, "build_graph") and hasattr(deps_mod, "export_graph"):
            graph = deps_mod.build_graph()
            if out:
                deps_mod.export_graph(graph, out, dot=dot)
                return {"ok": True, "path": out}
            return {"ok": True, "graph": graph}
        # fallback call
        if hasattr(deps_mod, "build_graph"):
            graph = deps_mod.build_graph()
            return {"ok": True, "graph": graph}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no_graph_funcs"}

# upgrade wrapper
def call_upgrade(targets: Optional[List[str]] = None, dry_run: bool=False, jobs: Optional[int]=None, fakeroot: bool=False, use_chroot: bool=True, force: bool=False, no_backup: bool=False):
    if not upgrade_mod:
        return {"ok": False, "error": "upgrade_not_available"}
    try:
        return upgrade_mod.upgrade_packages(targets=targets, dry_run=dry_run, jobs=jobs, fakeroot=fakeroot, use_chroot=use_chroot, force=force, no_backup=no_backup)
    except Exception as e:
        return {"ok": False, "error": str(e)}

# update wrapper
def call_update(packages: Optional[List[str]] = None, dry_run: bool=False, auto_update: bool=False, notify: bool=False):
    if not update_mod:
        return {"ok": False, "error": "update_module_missing"}
    try:
        return update_mod.run_update(packages=packages, dry_run=dry_run, auto_update=auto_update, notify=notify)
    except Exception as e:
        return {"ok": False, "error": str(e)}

# vuln wrapper
def call_vuln(action: str = "scan", packages: Optional[List[str]] = None, apply_fix: bool=False, fetch_remote: bool=False):
    if not vuln_mod:
        return {"ok": False, "error": "vuln_module_missing"}
    try:
        V = vuln_mod.ZeroPKGVulnManager if hasattr(vuln_mod, "ZeroPKGVulnManager") else vuln_mod
        vm = V()
        if action == "fetch":
            return vm.fetch_remote()
        elif action == "scan":
            return vm.scan(packages=packages)
        elif action == "apply":
            return vm.apply_fix(packages=packages)
        elif action == "report":
            return vm.report(packages=packages)
        else:
            return {"ok": False, "error": "unknown_action"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# sync wrapper
def call_sync(repos: Optional[List[str]] = None, dry_run: bool=False):
    if not sync_mod:
        return {"ok": False, "error": "sync_module_missing"}
    try:
        return sync_mod.sync_repos(repos=repos, dry_run=dry_run)
    except Exception as e:
        return {"ok": False, "error": str(e)}

# search/info wrappers
def call_search(query: str):
    # fallback simple DB search or filesystem search
    if db_mod and hasattr(db_mod, "list_installed_quick"):
        try:
            allp = db_mod.list_installed_quick()
            res = [p for p in allp if query.lower() in p["name"].lower()]
            return {"ok": True, "results": res}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no_db"}

def call_info(pkg: str):
    if db_mod and hasattr(db_mod, "get_package_manifest"):
        try:
            m = db_mod.get_package_manifest(pkg)
            if not m:
                return {"ok": False, "error": "not_found"}
            return {"ok": True, "manifest": m}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no_db"}

# downloader wrapper
def call_fetch(recipe: Optional[str] = None, urls: Optional[List[str]] = None, dry_run: bool=False):
    if not downloader_mod:
        return {"ok": False, "error": "downloader_missing"}
    try:
        Downloader = downloader_mod.Downloader if hasattr(downloader_mod, "Downloader") else None
        dd = Downloader(distdir=Path(CONFIG.get("paths",{}).get("distfiles_dir","/usr/ports/distfiles"))) if Downloader else downloader_mod
        if recipe:
            # load recipe and fetch from sources
            if toml := safe_import("zeropkg_toml"):
                try:
                    rec = toml.load_recipe(recipe)
                    sources = rec.get("source") or rec.get("sources") or []
                    fetched = []
                    errors = []
                    for s in sources:
                        url = s.get("url") or s.get("path")
                        r = dd.fetch(url, dest_dir=Path(CONFIG.get("paths",{}).get("distfiles_dir","/usr/ports/distfiles")), dry_run=dry_run)
                        if r.get("ok"):
                            fetched.append(r)
                        else:
                            errors.append(r)
                    return {"ok": True, "fetched": fetched, "errors": errors}
                except Exception as e:
                    return {"ok": False, "error": str(e)}
            else:
                return {"ok": False, "error": "toml_missing"}
        if urls:
            res = []
            for u in urls:
                r = dd.fetch(u, dest_dir=Path(CONFIG.get("paths",{}).get("distfiles_dir","/usr/ports/distfiles")), dry_run=dry_run)
                res.append(r)
            return {"ok": True, "results": res}
        return {"ok": False, "error": "no_input"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# patch wrapper
def call_patch(recipe: str, dry_run: bool=False):
    if not patcher_mod:
        return {"ok": False, "error": "patcher_missing"}
    try:
        if hasattr(patcher_mod, "apply_patches"):
            rec = toml_mod.load_recipe(recipe) if toml_mod and hasattr(toml_mod, "load_recipe") else None
            workdir = Path("/tmp")
            return patcher_mod.apply_patches(rec, workdir, dry_run=dry_run)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no_patch_func"}

# chroot wrappers
def call_chroot_prepare(profile: Optional[str] = None, root: Optional[str]=None, workdir: Optional[str]=None):
    if not chroot_mod or not hasattr(chroot_mod, "prepare_chroot"):
        return {"ok": False, "error": "chroot_module_missing"}
    try:
        return chroot_mod.prepare_chroot(profile=profile, root=root, workdir=workdir)
    except Exception as e:
        return {"ok": False, "error": str(e)}

def call_chroot_cleanup(profile: Optional[str] = None, root: Optional[str]=None, workdir: Optional[str]=None):
    if not chroot_mod or not hasattr(chroot_mod, "cleanup_chroot"):
        return {"ok": False, "error": "chroot_module_missing"}
    try:
        return chroot_mod.cleanup_chroot(profile=profile, root=root, workdir=workdir)
    except Exception as e:
        return {"ok": False, "error": str(e)}

# db wrappers
def call_db_list():
    if db_mod and hasattr(db_mod, "list_installed_quick"):
        return db_mod.list_installed_quick()
    return []

def call_db_export(dest: Optional[str] = None):
    if db_mod and hasattr(db_mod, "export_db"):
        return db_mod.export_db(dest)
    return {"ok": False, "error": "db_export_missing"}

# --------------------
# CLI wiring (argparse subparsers)
# --------------------
def build_cli():
    parser = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg package manager - build LFS/BLFS and manage packages")
    parser.add_argument("--version", action="version", version="zeropkg 1.0")
    parser.add_argument("--debug", action="store_true", help="Verbose debug output")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    # install (alias -i)
    p_install = subparsers.add_parser("install", aliases=["i"], help="Install package(s) from recipe(s) or binary archive")
    p_install.add_argument("targets", nargs="+", help="Recipe TOML path(s) or binary archive(s)")
    p_install.add_argument("--root", default="/", help="Target root (default /)")
    p_install.add_argument("--fakeroot", action="store_true", help="Use fakeroot for install steps")
    p_install.add_argument("--from-cache", dest="from_cache", help="Install from binary cache archive path")
    p_install.add_argument("--dry-run", action="store_true", help="Simulate only")
    p_install.add_argument("-j", "--jobs", type=int, default=None, help="Parallel jobs (if supported)")

    # build (alias -b)
    p_build = subparsers.add_parser("build", aliases=["b"], help="Build package from recipe")
    p_build.add_argument("recipe", help="Recipe TOML path")
    p_build.add_argument("--with-deps", action="store_true", help="Resolve and build dependencies first")
    p_build.add_argument("--use-chroot", action="store_true", help="Force build in chroot")
    p_build.add_argument("--no-chroot", action="store_true", help="Do not use chroot")
    p_build.add_argument("--dir-install", action="store_true", help="Do dir install (pack staging)")
    p_build.add_argument("--staging", help="Override staging directory")
    p_build.add_argument("--fakeroot", action="store_true", help="Use fakeroot for install steps")
    p_build.add_argument("--dry-run", action="store_true", help="Dry-run build")
    p_build.add_argument("-j", "--jobs", type=int, default=None, help="Parallel jobs")

    # build-world (build many from a world file)
    p_world = subparsers.add_parser("build-world", help="Build a world set (file with list of recipes)")
    p_world.add_argument("worldfile", help="Path to world file (one recipe per line)")
    p_world.add_argument("--use-chroot", action="store_true")
    p_world.add_argument("--dry-run", action="store_true")

    # build-toolchain (lfs bootstrap)
    p_toolchain = subparsers.add_parser("build-toolchain", help="Build LFS toolchain (bootstrap)")
    p_toolchain.add_argument("--root", default="/mnt/lfs", help="LFS root path")
    p_toolchain.add_argument("--dry-run", action="store_true")

    # upgrade (alias -u)
    p_upgrade = subparsers.add_parser("upgrade", aliases=["u"], help="Upgrade package(s)")
    p_upgrade.add_argument("packages", nargs="+", help="Package names or recipe paths")
    p_upgrade.add_argument("--dry-run", action="store_true")
    p_upgrade.add_argument("--force", action="store_true")
    p_upgrade.add_argument("--no-backup", action="store_true")
    p_upgrade.add_argument("-j", "--jobs", type=int, default=None)
    p_upgrade.add_argument("--fakeroot", action="store_true")
    p_upgrade.add_argument("--use-chroot", action="store_true")
    p_upgrade.add_argument("--no-chroot", action="store_true")

    # update (check upstreams)
    p_update = subparsers.add_parser("update", help="Check for new versions upstream")
    p_update.add_argument("packages", nargs="*", help="Optional package names")
    p_update.add_argument("--dry-run", action="store_true")
    p_update.add_argument("--auto-update", action="store_true")
    p_update.add_argument("--notify", action="store_true")
    p_update.add_argument("--force", action="store_true")

    # sync (sync repos to /usr/ports)
    p_sync = subparsers.add_parser("sync", help="Sync repo metadata to /usr/ports")
    p_sync.add_argument("--repos", nargs="*", help="Repo URLs or names")
    p_sync.add_argument("--dry-run", action="store_true")

    # remove
    p_remove = subparsers.add_parser("remove", help="Remove package")
    p_remove.add_argument("packages", nargs="+", help="Package names")
    p_remove.add_argument("--do-it", action="store_true", help="Actually perform removal (default dry-run)")
    p_remove.add_argument("--force", action="store_true", help="Force removal even if protected")
    p_remove.add_argument("--with-dependents", action="store_true", help="Also remove reverse dependencies")
    p_remove.add_argument("--no-backup", action="store_true", help="Do not create backup before removal")

    # depclean
    p_depclean = subparsers.add_parser("depclean", help="Clean orphan dependencies")
    p_depclean.add_argument("--apply", action="store_true", help="Apply removals (default dry-run)")
    p_depclean.add_argument("--only", nargs="*", help="Only these packages")
    p_depclean.add_argument("--exclude", nargs="*", help="Exclude these packages")
    p_depclean.add_argument("--keep", nargs="*", help="Keep these packages")
    p_depclean.add_argument("--parallel", action="store_true", help="Run parallel")
    p_depclean.add_argument("--max-workers", type=int, default=None)
    p_depclean.add_argument("--no-backup", action="store_true")
    p_depclean.add_argument("--report-tag", help="Report filename prefix")

    # deps graph
    p_deps = subparsers.add_parser("graph-deps", help="Generate or show dependencies graph")
    p_deps.add_argument("--out", help="Output file (json/dot)")
    p_deps.add_argument("--dot", action="store_true", help="Emit DOT format")

    # revdep
    p_revdep = subparsers.add_parser("revdep", help="Show reverse dependencies for a package")
    p_revdep.add_argument("package", help="Package name")

    # search/info
    p_search = subparsers.add_parser("search", help="Search packages")
    p_search.add_argument("query", help="Search term")
    p_info = subparsers.add_parser("info", help="Show package info")
    p_info.add_argument("package", help="Package name")

    # downloader fetch
    p_fetch = subparsers.add_parser("fetch", help="Fetch sources by recipe or urls")
    p_fetch.add_argument("--recipe", help="Recipe path")
    p_fetch.add_argument("--url", nargs="*", help="URL(s) to fetch")
    p_fetch.add_argument("--dry-run", action="store_true")

    # patch
    p_patch = subparsers.add_parser("patch", help="Apply patches for a recipe")
    p_patch.add_argument("recipe", help="Recipe path")
    p_patch.add_argument("--dry-run", action="store_true")

    # chroot control
    p_chroot = subparsers.add_parser("chroot", help="Manage chroot environments")
    p_chroot.add_argument("op", choices=["prepare","cleanup","verify","list","force-clean","cleanup-stale"], help="Operation")
    p_chroot.add_argument("--profile", help="Profile name")
    p_chroot.add_argument("--root", help="Chroot root")
    p_chroot.add_argument("--workdir", help="Workdir for chroot operations")

    # vuln
    p_vuln = subparsers.add_parser("vuln", help="Vulnerability scanning/management")
    p_vuln.add_argument("action", choices=["fetch","scan","apply","report"], help="Action")
    p_vuln.add_argument("--package", nargs="*", help="Package(s)")
    p_vuln.add_argument("--apply-fix", action="store_true", help="Apply fixes if possible")
    p_vuln.add_argument("--fetch-remote", action="store_true", help="Fetch remote CVE DB before scanning")

    # db
    p_db = subparsers.add_parser("db", help="DB operations")
    p_db.add_argument("op", choices=["list","export","events"], help="Op")
    p_db.add_argument("--dest", help="Export destination")

    # logger
    p_log = subparsers.add_parser("logger", help="Logger operations")
    p_log.add_argument("--list-sessions", action="store_true")
    p_log.add_argument("--cleanup", action="store_true")
    p_log.add_argument("--upload", action="store_true")

    # update (short alias 'upd')
    p_update_alias = subparsers.add_parser("upd", help=argparse.SUPPRESS)

    return parser

# --------------------
# Command dispatchers
# --------------------
def cmd_install(args):
    results = []
    for t in args.targets:
        if t.endswith((".tar.gz", ".tgz", ".tar.zst", ".tar")):
            # binary archive install
            res = call_installer_install_from_archive(t, root=args.root, fakeroot=args.fakeroot)
            results.append({"target": t, "result": res})
        else:
            # treat as recipe
            bres = call_builder_build(t,
                                     use_chroot=not args.fakeroot and True,
                                     chroot_profile=CONFIG.get("chroot",{}).get("default_profile"),
                                     dir_install=args.from_cache is not None,
                                     staging_dir_override=None,
                                     fakeroot=args.fakeroot,
                                     dry_run=args.dry_run,
                                     install_after=not args.from_cache,
                                     install_from_cache=args.from_cache if args.from_cache else None,
                                     jobs=args.jobs,
                                     root_for_install=args.root)
            results.append({"target": t, "result": bres})
    print(json.dumps(results, indent=2, ensure_ascii=False))

def cmd_build(args):
    use_chroot = False
    if args.use_chroot:
        use_chroot = True
    if args.no_chroot:
        use_chroot = False
    # optionally resolve dependencies
    if args.with_deps and deps_mod and hasattr(deps_mod, "resolve_and_build"):
        try:
            log("deps", f"resolving/building deps for {args.recipe}", "INFO")
            deps_res = deps_mod.resolve_and_build(args.recipe, dry_run=args.dry_run, jobs=args.jobs)
            # deps_mod.resolve_and_build should return list of recipes built or result structure
        except Exception as e:
            log("deps", f"deps resolution/build failed: {e}", "WARNING")
    res = call_builder_build(args.recipe,
                             use_chroot=use_chroot,
                             chroot_profile=args.chroot_profile,
                             dir_install=args.dir_install,
                             staging_dir_override=args.staging,
                             fakeroot=args.fakeroot,
                             dry_run=args.dry_run,
                             install_after=False,
                             install_from_cache=None,
                             jobs=args.jobs,
                             root_for_install="/")
    print(json.dumps(res, indent=2, ensure_ascii=False))

def cmd_build_world(args):
    results = []
    wf = Path(args.worldfile)
    if not wf.exists():
        print({"ok": False, "error": "worldfile_missing"})
        return
    for line in wf.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # each line is recipe path
        r = call_builder_build(line, use_chroot=args.use_chroot, chroot_profile=None, dir_install=False, staging_dir_override=None, fakeroot=False, dry_run=args.dry_run)
        results.append({line: r})
    print(json.dumps(results, indent=2, ensure_ascii=False))

def cmd_build_toolchain(args):
    # For LFS bootstrap assume a predefined list in config or a known sequence
    seq = CONFIG.get("lfs", {}).get("toolchain_order") or []
    if not seq:
        # fallback minimal toolchain recipes (user should provide in config)
        log("builder", "No toolchain sequence in config; provide via config.lfs.toolchain_order", "WARNING")
        print({"ok": False, "error": "no_toolchain_sequence"})
        return
    results = []
    for recipe in seq:
        r = call_builder_build(recipe, use_chroot=True, chroot_profile=CONFIG.get("chroot",{}).get("lfs_profile","lfs"), dir_install=False, fakeroot=False, dry_run=args.dry_run)
        results.append({recipe: r})
    print(json.dumps(results, indent=2, ensure_ascii=False))

def cmd_upgrade(args):
    res = call_upgrade(targets=args.packages, dry_run=args.dry_run, jobs=args.jobs, fakeroot=args.fakeroot, use_chroot=args.use_chroot and not args.no_chroot, force=args.force, no_backup=args.no_backup)
    print(json.dumps(res, indent=2, ensure_ascii=False))

def cmd_update(args):
    res = call_update(packages=args.packages if args.packages else None, dry_run=args.dry_run, auto_update=args.auto_update, notify=args.notify)
    print(json.dumps(res, indent=2, ensure_ascii=False))

def cmd_sync(args):
    res = call_sync(repos=args.repos, dry_run=args.dry_run)
    print(json.dumps(res, indent=2, ensure_ascii=False))

def cmd_remove(args):
    reports = []
    for p in args.packages:
        r = call_remove(p, do_it=args.do_it, force=args.force, with_dependents=args.with_dependents, no_backup=args.no_backup)
        reports.append({p: r})
    print(json.dumps(reports, indent=2, ensure_ascii=False))

def cmd_depclean(args):
    rep = call_depclean(apply=args.apply, only=args.only, exclude=args.exclude, keep=args.keep, parallel=args.parallel, max_workers=args.max_workers, backup=not args.no_backup, report_tag=args.report_tag)
    print(json.dumps(rep, indent=2, ensure_ascii=False))

def cmd_graph_deps(args):
    res = call_deps_graph(out=args.out, dot=args.dot)
    print(json.dumps(res, indent=2, ensure_ascii=False))

def cmd_revdep(args):
    if not db_mod or not hasattr(db_mod, "find_revdeps"):
        print(json.dumps({"ok": False, "error": "deps/db_missing"}, indent=2))
        return
    try:
        rev = db_mod.find_revdeps(args.package)
        print(json.dumps({"package": args.package, "revdeps": rev}, indent=2))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, indent=2))

def cmd_search(args):
    r = call_search(args.query)
    print(json.dumps(r, indent=2, ensure_ascii=False))

def cmd_info(args):
    r = call_info(args.package)
    print(json.dumps(r, indent=2, ensure_ascii=False))

def cmd_fetch(args):
    r = call_fetch(recipe=args.recipe, urls=args.url, dry_run=args.dry_run)
    print(json.dumps(r, indent=2, ensure_ascii=False))

def cmd_patch(args):
    r = call_patch(args.recipe, dry_run=args.dry_run)
    print(json.dumps(r, indent=2, ensure_ascii=False))

def cmd_chroot(args):
    if args.op == "prepare":
        r = call_chroot_prepare(profile=args.profile, root=args.root, workdir=args.workdir)
    elif args.op == "cleanup":
        r = call_chroot_cleanup(profile=args.profile, root=args.root, workdir=args.workdir)
    elif args.op == "verify":
        r = chroot_mod.verify_chroot() if chroot_mod and hasattr(chroot_mod, "verify_chroot") else {"ok": False, "error": "verify_not_available"}
    elif args.op == "list":
        r = chroot_mod.list_chroots() if chroot_mod and hasattr(chroot_mod, "list_chroots") else {"ok": False, "error": "list_not_available"}
    elif args.op == "force-clean":
        r = chroot_mod.force_cleanup_all() if chroot_mod and hasattr(chroot_mod, "force_cleanup_all") else {"ok": False, "error": "force_cleanup_not_available"}
    elif args.op == "cleanup-stale":
        r = chroot_mod.cleanup_stale() if chroot_mod and hasattr(chroot_mod, "cleanup_stale") else {"ok": False, "error": "cleanup_stale_not_available"}
    else:
        r = {"ok": False, "error": "unknown_op"}
    print(json.dumps(r, indent=2, ensure_ascii=False))

def cmd_vuln(args):
    r = call_vuln(action=args.action, packages=args.package, apply_fix=args.apply_fix, fetch_remote=args.fetch_remote)
    print(json.dumps(r, indent=2, ensure_ascii=False))

def cmd_db(args):
    if args.op == "list":
        r = call_db_list()
        print(json.dumps(r, indent=2, ensure_ascii=False))
    elif args.op == "export":
        r = call_db_export(dest=args.dest)
        print(json.dumps(r, indent=2, ensure_ascii=False))
    elif args.op == "events":
        if db_mod and hasattr(db_mod, "query_events"):
            print(json.dumps(db_mod.query_events(), indent=2, ensure_ascii=False))
        else:
            print(json.dumps({"ok": False, "error": "events_not_available"}, indent=2))

def cmd_logger(args):
    if not logger_mod:
        print(json.dumps({"ok": False, "error": "logger_missing"}, indent=2))
        return
    if args.list_sessions:
        logger_mod.main() if hasattr(logger_mod, "main") else print("list not supported")
    elif args.cleanup:
        if hasattr(logger_mod, "_cleanup_old_logs"):
            logger_mod._cleanup_old_logs()
            print(json.dumps({"ok": True, "msg": "cleanup_done"}, indent=2))
        else:
            print(json.dumps({"ok": False, "error": "cleanup_func_missing"}, indent=2))
    elif args.upload:
        if hasattr(logger_mod, "_upload_logs"):
            logger_mod._upload_logs()
            print(json.dumps({"ok": True, "msg": "upload_triggered"}, indent=2))
        else:
            print(json.dumps({"ok": False, "error": "upload_func_missing"}, indent=2))
    else:
        print(json.dumps({"ok": False, "error": "no_action"}, indent=2))

# --------------------
# Main dispatcher
# --------------------
def main():
    parser = build_cli()
    args = parser.parse_args()
    cmd = args.cmd

    # dispatch
    try:
        if cmd in ("install", "i"):
            cmd_install(args)
        elif cmd in ("build", "b"):
            cmd_build(args)
        elif cmd == "build-world":
            cmd_build_world(args)
        elif cmd == "build-toolchain":
            cmd_build_toolchain(args)
        elif cmd in ("upgrade", "u"):
            cmd_upgrade(args)
        elif cmd == "update":
            cmd_update(args)
        elif cmd == "sync":
            cmd_sync(args)
        elif cmd == "remove":
            cmd_remove(args)
        elif cmd == "depclean":
            cmd_depclean(args)
        elif cmd == "graph-deps":
            cmd_graph_deps(args)
        elif cmd == "revdep":
            cmd_revdep(args)
        elif cmd == "search":
            cmd_search(args)
        elif cmd == "info":
            cmd_info(args)
        elif cmd == "fetch":
            cmd_fetch(args)
        elif cmd == "patch":
            cmd_patch(args)
        elif cmd == "chroot":
            cmd_chroot(args)
        elif cmd == "vuln":
            cmd_vuln(args)
        elif cmd == "db":
            cmd_db(args)
        elif cmd == "logger":
            cmd_logger(args)
        else:
            # fallback unknown
            print(json.dumps({"ok": False, "error": f"unknown command {cmd}"}))
    except Exception as e:
        log("cli", f"Unhandled exception in command {cmd}: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        print(json.dumps({"ok": False, "error": str(e)}))

if __name__ == "__main__":
    main()
