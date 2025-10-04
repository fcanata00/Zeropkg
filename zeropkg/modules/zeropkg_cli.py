#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg CLI — ponto único de entrada integrado para todos os módulos Zeropkg.

Comandos principais:
  install (-i, --install)       : instalar pacotes (resolve deps, build, install)
  build (-b, --build)           : construir pacote(s) (não instala por padrão)
  build-world (--build-world)   : construir lista world.base (do config)
  build-toolchain (--toolchain) : construir toolchain LFS (gcc pass1, binutils, etc.)
  remove (-r, --remove)         : remover pacote(s)
  upgrade (--upgrade)           : atualizar pacotes instalados
  update (--update)             : checar upstreams e gerar resumo de updates
  sync (--sync)                 : sincronizar repositórios (ports)
  depclean (--depclean)         : limpar dependências órfãs
  revdep (--revdep)             : mostrar reverse-deps
  search (--search)             : procurar receita por nome
  info (--info)                 : mostrar info de recipe/instalado
  patch (--patch)               : aplicar patches (chama patcher)
  deps (--graph-deps)           : exportar/mostrar grafo de dependências
  chroot (subcommands)          : preparar/cleanup/verify chroots
  db (subcommands)              : DB inspections (list, manifest, export)
  logger (subcommands)          : show sessions, cleanup logs
  help
Usa config via zeropkg_config.load_config()
"""

from __future__ import annotations
import sys
import os
import argparse
import json
import shutil
from pathlib import Path
from typing import List, Optional

# --- Safe imports of Zeropkg modules (fall back to None or simple wrappers) ---
def safe_import(name: str):
    try:
        mod = __import__(name, fromlist=["*"])
        return mod
    except Exception:
        return None

# core modules
cfg_mod = safe_import("zeropkg_config")
logger_mod = safe_import("zeropkg_logger")
db_mod = safe_import("zeropkg_db")
deps_mod = safe_import("zeropkg_deps")
builder_mod = safe_import("zeropkg_builder")
installer_mod = safe_import("zeropkg_installer")
downloader_mod = safe_import("zeropkg_downloader")
patcher_mod = safe_import("zeropkg_patcher")
depclean_mod = safe_import("zeropkg_depclean")
remover_mod = safe_import("zeropkg_remover")
upgrade_mod = safe_import("zeropkg_upgrade")
update_mod = safe_import("zeropkg_update")
sync_mod = safe_import("zeropkg_sync")
chroot_mod = safe_import("zeropkg_chroot")
vuln_mod = safe_import("zeropkg_vuln")

# helpers
def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# config bootstrap
if cfg_mod and hasattr(cfg_mod, "load_config"):
    CONFIG = cfg_mod.load_config()
else:
    CONFIG = {"paths": {"cache_dir": "/var/cache/zeropkg", "log_dir": "/var/log/zeropkg"}, "cli": {"default_jobs": 4}}

# logger bootstrap
if logger_mod and hasattr(logger_mod, "get_logger"):
    log = logger_mod.get_logger("cli")
    log_event = getattr(logger_mod, "log_event", lambda pkg, stage, msg, level="info", extra=None: None)
else:
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("zeropkg.cli")
    def log_event(pkg, stage, msg, level="info", extra=None):
        logging.getLogger("zeropkg.cli").info(f"{pkg}:{stage} - {msg}")

# db convenience
DB = None
if db_mod and hasattr(db_mod, "ZeroPKGDB"):
    try:
        # use default db instance helper if provided
        if hasattr(db_mod, "_get_default_db"):
            DB = db_mod._get_default_db()
        else:
            DB = db_mod.ZeroPKGDB()
    except Exception:
        DB = None

# find modules convenience wrappers
DEPS = deps_mod.DepsManager() if deps_mod and hasattr(deps_mod, "DepsManager") else None
BUILDER = builder_mod.ZeropkgBuilder() if builder_mod and hasattr(builder_mod, "ZeropkgBuilder") else None
INSTALLER = installer_mod if installer_mod else None
DOWNLOADER = downloader_mod.Downloader() if downloader_mod and hasattr(downloader_mod, "Downloader") else None
PATCHER = patcher_mod.ZeropkgPatcher() if patcher_mod and hasattr(patcher_mod, "ZeropkgPatcher") else None
DEPCLEAN = depclean_mod.Depclean() if depclean_mod and hasattr(depclean_mod, "Depclean") else None
REMOVER = remover_mod.ZeropkgRemover() if remover_mod and hasattr(remover_mod, "ZeropkgRemover") else None
UPGRADE = upgrade_mod if upgrade_mod else None
UPDATE = update_mod if update_mod else None
SYNC = sync_mod if sync_mod else None
CHROOT = chroot_mod if chroot_mod else None
VULN = vuln_mod if vuln_mod else None

# Utility: ensure path for recipe/ports
def find_recipe_by_name(name: str) -> Optional[str]:
    """Try to locate recipe by scanning DEPS index or /usr/ports structure."""
    # check DEPS index first
    if DEPS and hasattr(DEPS, "_recipes_index"):
        rp = DEPS._recipes_index.get(name)
        if rp:
            return rp
    # try common locations from config
    ports_roots = []
    try:
        ports_roots = CONFIG.get("repos", {}).get("roots", []) or [CONFIG.get("paths", {}).get("ports_dir", "/usr/ports")]
    except Exception:
        ports_roots = ["/usr/ports"]
    for root in ports_roots:
        p = Path(root)
        if not p.exists():
            continue
        # naive search: name matches a filename prefix
        for rf in p.rglob("*.toml"):
            if rf.stem.startswith(name):
                return str(rf)
    return None

# --- Core command implementations ---
def cmd_sync(args):
    if not SYNC:
        print_err("sync module not available")
        return 2
    try:
        # expect sync_mod.sync_repos() existence
        if hasattr(SYNC, "sync_repos"):
            SYNC.sync_repos()
        elif hasattr(SYNC, "sync"):
            SYNC.sync()
        else:
            # maybe module exposes function
            try:
                sync_mod.sync_repos()
            except Exception as e:
                print_err("sync function not found:", e)
                return 2
        print("Sync completed")
        return 0
    except Exception as e:
        print_err("sync failed:", e)
        return 1

def cmd_search(name: str, args):
    # search recipes and installed packages
    found = {"recipes": [], "installed": []}
    # recipes via deps index
    if DEPS and hasattr(DEPS, "_recipes_index"):
        for pkg, path in DEPS._recipes_index.items():
            if name in pkg:
                found["recipes"].append({"name": pkg, "path": path})
    # fallback scan
    if not found["recipes"]:
        # quick scan in configured ports
        ports_dirs = CONFIG.get("repos", {}).get("roots", [CONFIG.get("paths", {}).get("ports_dir", "/usr/ports")])
        for root in ports_dirs:
            rootp = Path(root)
            if not rootp.exists():
                continue
            for rf in rootp.rglob("*.toml"):
                if name in rf.stem:
                    found["recipes"].append({"name": rf.stem, "path": str(rf)})
    # installed
    try:
        if DB:
            for r in db_mod.list_installed_quick():
                if name in r["name"]:
                    found["installed"].append(r)
    except Exception:
        pass
    print(json.dumps(found, indent=2, ensure_ascii=False))
    return 0

def cmd_info(target: str, args):
    # show recipe info or installed package info
    # first check installed
    if DB:
        m = db_mod.get_package_manifest(target)
        if m:
            print(json.dumps(m, indent=2))
            return 0
    # try recipe
    rp = find_recipe_by_name(target)
    if rp:
        try:
            # load via toml module if available
            toml_mod = safe_import("zeropkg_toml")
            if toml_mod and hasattr(toml_mod, "load_recipe"):
                rec = toml_mod.load_recipe(rp)
                print(json.dumps(rec, indent=2))
                return 0
            else:
                print("Found recipe at:", rp)
                return 0
        except Exception as e:
            print_err("failed to load recipe:", e)
            return 2
    print_err("no info found for", target)
    return 1

def cmd_revdep(args):
    pkg = args if isinstance(args, str) else (args.pkg if hasattr(args, "pkg") else None)
    if not pkg:
        print_err("revdep needs a package name")
        return 2
    if DEPS:
        res = DEPS.find_revdeps(pkg)
        print(json.dumps(res, indent=2))
        return 0
    if DB:
        try:
            print(json.dumps(db_mod.find_revdeps(pkg), indent=2))
            return 0
        except Exception:
            pass
    print_err("revdep information not available (no deps/db)")
    return 2

def cmd_graph_deps(args):
    # export graph to dot/json or print plan
    out = args.out or None
    if not DEPS:
        print_err("deps module not available")
        return 2
    if args.packages:
        plan = DEPS.build_plan(args.packages)
        print(json.dumps(plan, indent=2))
    else:
        if args.format == "dot" or out and out.endswith(".dot"):
            dest = out or "zeropkg_deps.dot"
            DEPS.export_dot(dest)
            print("Wrote", dest)
        else:
            dest = out or "zeropkg_deps.json"
            DEPS.export_json(dest)
            print("Wrote", dest)
    return 0

def cmd_depclean(args):
    if not DEPCLEAN:
        print_err("depclean module not available")
        return 2
    only = set(args.only) if args.only else None
    exclude = set(args.exclude) if args.exclude else None
    keep = set(args.keep) if args.keep else None
    protected = set(args.protected) if args.protected else None
    apply_flag = bool(args.apply)
    dry_run = not apply_flag
    res = DEPCLEAN.execute(only=only, exclude=exclude, keep=keep, apply=apply_flag, dry_run=dry_run,
                           backup_before_remove=args.backup, parallel=args.parallel,
                           protected_extra=protected, report_tag=args.tag)
    print(json.dumps(res, indent=2, ensure_ascii=False))
    return 0

def cmd_build(args):
    # Build packages (resolve deps then build). -i/--install handled elsewhere.
    pkgs = args.packages or []
    if not pkgs:
        print_err("no packages specified to build")
        return 2
    # resolve via DEPS if available to get order
    if DEPS:
        plan = DEPS.resolve(pkgs)
        if not plan["ok"]:
            print_err("dependency resolution failed or cycles found:", plan.get("cycles"))
            # continue if keep_going
        build_sequence = list(reversed(plan["order"])) if plan.get("order") else pkgs
    else:
        build_sequence = pkgs
    results = []
    for pkg in build_sequence:
        try:
            # find recipe path
            rp = find_recipe_by_name(pkg)
            if not rp:
                print_err("recipe not found for", pkg)
                results.append({"pkg": pkg, "status": "missing_recipe"})
                continue
            if not BUILDER:
                results.append({"pkg": pkg, "status": "no_builder"})
                continue
            out = BUILDER.build(rp, dry_run=args.dry_run, dir_install=args.dir_install, jobs=args.jobs)
            results.append({"pkg": pkg, "result": out})
        except Exception as e:
            results.append({"pkg": pkg, "error": str(e)})
            if not args.keep_going:
                break
    print(json.dumps(results, indent=2, ensure_ascii=False))
    return 0

def cmd_install(args):
    # install resolves deps, builds and installs into / or LFS root
    pkgs = args.packages or []
    if not pkgs:
        print_err("no packages specified to install")
        return 2
    # resolve deps
    if DEPS:
        plan = DEPS.resolve(pkgs)
        if not plan["ok"]:
            print_err("dependency resolution failed or cycles found:", plan.get("cycles"))
            if not args.keep_going:
                return 3
        build_sequence = list(reversed(plan["order'])) if plan.get("order") else pkgs
    else:
        build_sequence = pkgs
    final_results = []
    for pkg in build_sequence:
        rp = find_recipe_by_name(pkg)
        if not rp:
            final_results.append({"pkg": pkg, "error": "recipe_not_found"})
            if not args.keep_going:
                break
            else:
                continue
        # build
        if not BUILDER:
            final_results.append({"pkg": pkg, "error": "builder_not_available"})
            continue
        out = BUILDER.build(rp, dry_run=args.dry_run, dir_install=args.dir_install, jobs=args.jobs)
        if args.dry_run:
            final_results.append({"pkg": pkg, "build": "planned", "builder_out": out})
            continue
        # install using installer module (supports fakeroot)
        try:
            if installer_mod and hasattr(installer_mod, "install_pkg"):
                inst_res = installer_mod.install_pkg(rp, root=args.root, fakeroot=args.fakeroot, dir_install=args.dir_install)
            elif INSTALLER and hasattr(INSTALLER, "install"):
                inst_res = INSTALLER.install(pkg, rp, root=args.root, fakeroot=args.fakeroot, dir_install=args.dir_install)
            else:
                # fallback: ask builder to dir-install into a temp and then copy into root (not ideal)
                inst_res = {"status": "no_installer_available"}
            final_results.append({"pkg": pkg, "install": inst_res})
            # record in DB if builder returned manifest
            try:
                if DB and hasattr(db_mod, "record_install_quick"):
                    manifest = out.get("manifest") if isinstance(out, dict) else None
                    db_mod.record_install_quick(pkg, manifest or {"files": []}, deps=out.get("deps") if isinstance(out, dict) else [])
            except Exception:
                pass
        except Exception as e:
            final_results.append({"pkg": pkg, "error": str(e)})
            if not args.keep_going:
                break
    print(json.dumps(final_results, indent=2, ensure_ascii=False))
    return 0

def cmd_remove(args):
    pkgs = args.packages or []
    if not pkgs:
        print_err("no packages specified to remove")
        return 2
    results = []
    for pkg in pkgs:
        try:
            if REMOVER:
                res = REMOVER.remove(pkg, dry_run=args.dry_run)
                results.append({"pkg": pkg, "result": res})
            else:
                # fallback to DB-only removal
                ok = False
                if DB:
                    ok = db_mod.remove_package_quick(pkg)
                results.append({"pkg": pkg, "db_remove": ok})
        except Exception as e:
            results.append({"pkg": pkg, "error": str(e)})
    print(json.dumps(results, indent=2))
    return 0

def cmd_upgrade(args):
    # upgrade specific packages or system-wide
    pkgs = args.packages or []
    if UPGRADE and hasattr(UPGRADE, "upgrade"):
        try:
            res = UPGRADE.upgrade(pkgs, dry_run=args.dry_run)
            print(json.dumps(res, indent=2))
            return 0
        except Exception as e:
            print_err("upgrade failed:", e)
            return 1
    else:
        print_err("upgrade module not available")
        return 2

def cmd_update(args):
    # check upstreams and produce notification summary
    if UPDATE and hasattr(UPDATE, "scan_all"):
        try:
            res = UPDATE.scan_all(dry_run=args.dry_run)
            print(json.dumps(res, indent=2))
            return 0
        except Exception as e:
            print_err("update scan failed:", e)
            return 1
    else:
        print_err("update module not available")
        return 2

def cmd_sync_repos(args):
    # wrapper to sync module
    return cmd_sync(args)

def cmd_patch(args):
    if not PATCHER:
        print_err("patcher module not available")
        return 2
    out = PATCHER.apply_all(args.recipe, target_dir=args.target, dry_run=args.dry_run, use_chroot=not args.no_chroot, fakeroot=args.fakeroot, parallel=args.parallel)
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0

def cmd_chroot(args):
    if not CHROOT:
        print_err("chroot module not available")
        return 2
    sub = args.subcmd
    if sub == "prepare":
        CHROOT.prepare_chroot(args.root, profile=args.profile, mount_proc=True)
        print("chroot prepared:", args.root)
    elif sub == "cleanup":
        CHROOT.cleanup_chroot(args.root, force=args.force)
        print("chroot cleaned:", args.root)
    elif sub == "verify":
        ok = CHROOT.verify_chroot(args.root)
        print("chroot ok:", ok)
    else:
        print_err("Unknown chroot subcommand")
        return 2
    return 0

def cmd_deps(args):
    # expose DepsManager quick actions
    if not DEPS:
        print_err("deps manager not available")
        return 2
    if args.action == "scan":
        DEPS.scan_recipes(force=args.force)
        print("scanned")
    elif args.action == "resolve":
        res = DEPS.resolve(args.packages)
        print(json.dumps(res, indent=2))
    elif args.action == "graph":
        if args.out:
            if args.out.endswith(".dot"):
                DEPS.export_dot(args.out)
            else:
                DEPS.export_json(args.out)
            print("written", args.out)
        else:
            print(DEPS.graph.to_dot())
    else:
        print_err("unknown deps action")
        return 2
    return 0

def cmd_db(args):
    if not DB:
        print_err("DB module not available")
        return 2
    if args.action == "list":
        print(json.dumps(db_mod.list_installed_quick(), indent=2))
    elif args.action == "manifest":
        print(json.dumps(db_mod.get_package_manifest(args.pkg), indent=2))
    elif args.action == "export":
        p = Path(args.dest or "/tmp/zeropkg-db-export")
        try:
            path = db_mod.export_db(p, compress=not args.no_compress)
            print("exported to", path)
        except Exception as e:
            print_err("export failed:", e)
            return 1
    else:
        print_err("unknown db action")
        return 2
    return 0

def cmd_logger(args):
    if not logger_mod:
        print_err("logger module not available")
        return 2
    if args.action == "list":
        for s in logger_mod.LOG_DIR.iterdir():
            if s.is_dir():
                print(s.name)
    elif args.action == "cleanup":
        logger_mod.cleanup_old_logs(args.max_age)
        print("cleanup triggered")
    elif args.action == "upload":
        ok = logger_mod.upload_logs_now()
        print("upload:", ok)
    else:
        print_err("unknown logger action")
        return 2
    return 0

# --- Argparse setup ---
def build_parser():
    parser = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg package manager CLI")
    parser.add_argument("--config", help="Override config file location")
    parser.add_argument("--dry-run", action="store_true", help="Global dry-run")
    parser.add_argument("--jobs", "-j", type=int, default=CONFIG.get("cli", {}).get("default_jobs", 4), help="Parallel jobs")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # install
    p_install = sub.add_parser("install", aliases=["-i","-I"], help="Resolve deps, build and install packages")
    p_install.add_argument("packages", nargs="+")
    p_install.add_argument("--root", default="/", help="Install root (use /mnt/lfs for LFS)")
    p_install.add_argument("--dir-install", action="store_true", help="Use dir-install (install to staging dir first)")
    p_install.add_argument("--fakeroot", action="store_true", help="Use fakeroot for installation")
    p_install.add_argument("--keep-going", action="store_true", help="Continue on failure")
    p_install.add_argument("--dry-run", action="store_true")

    # build
    p_build = sub.add_parser("build", aliases=["-b"], help="Build packages only")
    p_build.add_argument("packages", nargs="+")
    p_build.add_argument("--dir-install", action="store_true", help="Produce dir-install")
    p_build.add_argument("--jobs", "-j", type=int, default=CONFIG.get("cli", {}).get("default_jobs", 4))
    p_build.add_argument("--dry-run", action="store_true")
    p_build.add_argument("--keep-going", action="store_true")

    # remove
    p_remove = sub.add_parser("remove", aliases=["-r"], help="Remove packages")
    p_remove.add_argument("packages", nargs="+")
    p_remove.add_argument("--dry-run", action="store_true")

    # upgrade
    p_upgrade = sub.add_parser("upgrade", help="Upgrade packages")
    p_upgrade.add_argument("packages", nargs="*", help="If empty, upgrade all updatable packages")
    p_upgrade.add_argument("--dry-run", action="store_true")

    # update (scan upstreams)
    p_update = sub.add_parser("update", aliases=["--update"], help="Scan upstreams for new versions")
    p_update.add_argument("--dry-run", action="store_true")

    # sync
    p_sync = sub.add_parser("sync", aliases=["--sync"], help="Sync repositories")
    p_sync.add_argument("--dry-run", action="store_true")

    # depclean
    p_depclean = sub.add_parser("depclean", aliases=["--depclean"], help="Find and remove orphaned packages")
    p_depclean.add_argument("--apply", action="store_true", help="Actually remove")
    p_depclean.add_argument("--only", nargs="+")
    p_depclean.add_argument("--exclude", nargs="+")
    p_depclean.add_argument("--keep", nargs="+")
    p_depclean.add_argument("--protected", nargs="+")
    p_depclean.add_argument("--backup", action="store_true")
    p_depclean.add_argument("--parallel", action="store_true")
    p_depclean.add_argument("--tag", help="Report tag")

    # revdep
    p_revdep = sub.add_parser("revdep", help="List reverse dependencies")
    p_revdep.add_argument("pkg")

    # search
    p_search = sub.add_parser("search", help="Search recipes and installed pkgs")
    p_search.add_argument("term")

    # info
    p_info = sub.add_parser("info", help="Show recipe or installed package info")
    p_info.add_argument("target")

    # patch
    p_patch = sub.add_parser("patch", help="Apply patches defined in recipe")
    p_patch.add_argument("recipe")
    p_patch.add_argument("--target", help="target dir")
    p_patch.add_argument("--no-chroot", dest="no_chroot", action="store_true")
    p_patch.add_argument("--fakeroot", action="store_true")
    p_patch.add_argument("--parallel", action="store_true")
    p_patch.add_argument("--dry-run", action="store_true")

    # deps / graph
    p_deps = sub.add_parser("deps", help="Dependency operations")
    p_deps.add_argument("action", choices=["scan","resolve","graph"])
    p_deps.add_argument("packages", nargs="*", help="packages for resolve")
    p_deps.add_argument("--out")
    p_deps.add_argument("--force", action="store_true")

    # chroot
    p_chroot = sub.add_parser("chroot", help="chroot operations")
    p_chroot_sub = p_chroot.add_subparsers(dest="subcmd", required=True)
    p_chroot_prep = p_chroot_sub.add_parser("prepare")
    p_chroot_prep.add_argument("root")
    p_chroot_prep.add_argument("--profile", default="lfs")
    p_chroot_prep.add_argument("--no-mount-proc", dest="mount_proc", action="store_false")
    p_chroot_cleanup = p_chroot_sub.add_parser("cleanup")
    p_chroot_cleanup.add_argument("root")
    p_chroot_cleanup.add_argument("--force", action="store_true")
    p_chroot_verify = p_chroot_sub.add_parser("verify")
    p_chroot_verify.add_argument("root")

    # deps graph alternative (graph-deps)
    p_gd = sub.add_parser("graph-deps", help="Export or show dependency graph")
    p_gd.add_argument("--out", help="destination (.dot or .json)")
    p_gd.add_argument("--format", choices=["dot","json"], default="json")
    p_gd.add_argument("--packages", nargs="*", help="Show plan for these packages")

    # db
    p_db = sub.add_parser("db", help="DB utilities")
    p_db.add_argument("action", choices=["list","manifest","export"])
    p_db.add_argument("--pkg")
    p_db.add_argument("--dest")
    p_db.add_argument("--no-compress", action="store_true")

    # logger
    p_log = sub.add_parser("logger", help="Logger utilities")
    p_log.add_argument("action", choices=["list","cleanup","upload"])
    p_log.add_argument("--max-age", type=int, default=30)

    # misc: help is default
    return parser

def main(argv: Optional[List[str]] = None):
    parser = build_parser()
    args = parser.parse_args(argv)
    # global dry-run precedence
    if getattr(args, "dry_run", False):
        global_dry = True
    else:
        global_dry = False

    cmd = args.cmd

    try:
        if cmd in ("install", "-i", "I"):
            return cmd_install(args)
        elif cmd in ("build", "-b"):
            return cmd_build(args)
        elif cmd == "remove" or cmd == "-r":
            return cmd_remove(args)
        elif cmd == "upgrade":
            return cmd_upgrade(args)
        elif cmd == "update":
            return cmd_update(args)
        elif cmd == "sync":
            return cmd_sync_repos(args)
        elif cmd == "search":
            return cmd_search(args.term, args)
        elif cmd == "info":
            return cmd_info(args.target, args)
        elif cmd == "revdep":
            return cmd_revdep(args)
        elif cmd == "depclean":
            return cmd_depclean(args)
        elif cmd == "patch":
            return cmd_patch(args)
        elif cmd == "chroot":
            return cmd_chroot(args)
        elif cmd == "deps":
            return cmd_deps(args)
        elif cmd == "graph-deps":
            return cmd_graph_deps(args)
        elif cmd == "db":
            return cmd_db(args)
        elif cmd == "logger":
            return cmd_logger(args)
        else:
            parser.print_help()
            return 1
    except KeyboardInterrupt:
        print_err("aborted by user")
        return 130
    except Exception as e:
        print_err("unexpected error:", e)
        log_event("cli", "error", f"exception: {e}", level="error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
