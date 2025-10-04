#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg CLI final integrado

Subcomandos principais (resumo):
  install, build, build-world, build-toolchain,
  remove, clean, upgrade, update, sync,
  deps, graph-deps, patch, chroot, db, logger, search, info, revdep

Colore mensagens:
  SUCESSO -> verde
  AVISO   -> amarelo
  ERRO    -> vermelho
"""

from __future__ import annotations
import sys
import os
import argparse
import json
import shutil
import logging
from pathlib import Path
from typing import Optional, List, Any, Set

# -------------------------
# Terminal color helpers
# -------------------------
def _supports_color() -> bool:
    return sys.stdout.isatty()

CSI = "\x1b["
def color(text: str, code: str) -> str:
    if not _supports_color():
        return text
    return f"{CSI}{code}m{text}{CSI}0m"

def green(s: str) -> str:
    return color(s, "32")

def yellow(s: str) -> str:
    return color(s, "33")

def red(s: str) -> str:
    return color(s, "31")

def bold(s: str) -> str:
    return color(s, "1")

def info(msg: str):
    print(green("✔") + " " + msg)

def warn(msg: str):
    print(yellow("!")+ " " + msg)

def error(msg: str):
    print(red("✖") + " " + msg, file=sys.stderr)

# -------------------------
# Safe import wrapper
# -------------------------
def safe_import(module_name: str):
    try:
        mod = __import__(module_name, fromlist=["*"])
        return mod
    except Exception:
        return None

# Core modules (may be None if not installed)
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

# Logger fallback
if logger_mod and hasattr(logger_mod, "get_logger"):
    log = logger_mod.get_logger("cli")
    try:
        log_event = logger_mod.log_event
    except Exception:
        def log_event(pkg, stage, msg, level="info", extra=None):
            pass
else:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("zeropkg.cli")
    def log_event(pkg, stage, msg, level="info", extra=None):
        if level == "error":
            log.error(f"{pkg}:{stage} - {msg}")
        elif level == "warning":
            log.warning(f"{pkg}:{stage} - {msg}")
        else:
            log.info(f"{pkg}:{stage} - {msg}")

# Convenience wrappers/instances (if available)
DEPS = deps_mod.DepsManager() if deps_mod and hasattr(deps_mod, "DepsManager") else None
BUILDER = builder_mod.ZeropkgBuilder() if builder_mod and hasattr(builder_mod, "ZeropkgBuilder") else None
DB = db_mod._get_default_db() if db_mod and hasattr(db_mod, "_get_default_db") else (db_mod.ZeroPKGDB() if db_mod and hasattr(db_mod, "ZeroPKGDB") else None)
INSTALLER = installer_mod if installer_mod else None
DOWNLOADER = downloader_mod.Downloader() if downloader_mod and hasattr(downloader_mod, "Downloader") else None
PATCHER = patcher_mod.ZeropkgPatcher() if patcher_mod and hasattr(patcher_mod, "ZeropkgPatcher") else None
DEPCLEAN = depclean_mod.Depclean() if depclean_mod and hasattr(depclean_mod, "Depclean") else None
REMOVER = remover_mod.ZeropkgRemover() if remover_mod and hasattr(remover_mod, "ZeropkgRemover") else None
UPGRADE = upgrade_mod if upgrade_mod else None
UPDATE = update_mod if update_mod else None
SYNC = sync_mod if sync_mod else None
CHROOT = chroot_mod if chroot_mod else None
VULN = vuln_mod.ZeroPKGVulnManager() if vuln_mod and hasattr(vuln_mod, "ZeroPKGVulnManager") else None

# Load configuration (best-effort)
def load_config_safe():
    try:
        if cfg_mod and hasattr(cfg_mod, "load_config"):
            return cfg_mod.load_config()
    except Exception:
        pass
    return {"paths": {"ports_dir": "/usr/ports", "cache_dir": "/var/cache/zeropkg", "log_dir": "/var/log/zeropkg"}, "cli": {"default_jobs": 4}}

CONFIG = load_config_safe()

# -------------------------
# Utility helpers
# -------------------------
def find_recipe_by_name(name: str) -> Optional[str]:
    # Try DEPS index first
    try:
        if DEPS and hasattr(DEPS, "_recipes_index"):
            rp = DEPS._recipes_index.get(name)
            if rp:
                return rp
    except Exception:
        pass
    # Fallback: naive search in configured ports roots
    roots = CONFIG.get("repos", {}).get("roots", []) or [CONFIG.get("paths", {}).get("ports_dir", "/usr/ports")]
    for root in roots:
        p = Path(root)
        if not p.exists():
            continue
        for rf in p.rglob("*.toml"):
            if rf.stem.startswith(name):
                return str(rf)
    return None

def print_json(obj: Any):
    print(json.dumps(obj, indent=2, ensure_ascii=False))

# -------------------------
# Command implementations
# -------------------------
def cmd_sync(args):
    if not SYNC:
        error("sync module not available")
        return 2
    try:
        # Try common function names
        if hasattr(SYNC, "sync_repos"):
            res = SYNC.sync_repos(dry_run=args.dry_run)
        elif hasattr(SYNC, "sync"):
            res = SYNC.sync(dry_run=args.dry_run)
        else:
            # module might export top-level function
            try:
                sync_mod.sync_repos(dry_run=args.dry_run)
                res = {"ok": True}
            except Exception as e:
                return _fail("sync: function not found: " + str(e))
        info("sync finished")
        return 0
    except Exception as e:
        return _fail(f"sync failed: {e}")

def cmd_search(args):
    term = args.term
    found = {"recipes": [], "installed": []}
    if DEPS and hasattr(DEPS, "_recipes_index"):
        for pkg, path in DEPS._recipes_index.items():
            if term in pkg:
                found["recipes"].append({"name": pkg, "path": path})
    else:
        # quick scan
        roots = CONFIG.get("repos", {}).get("roots", [CONFIG.get("paths", {}).get("ports_dir", "/usr/ports")])
        for root in roots:
            p = Path(root)
            if not p.exists():
                continue
            for rf in p.rglob("*.toml"):
                if term in rf.stem:
                    found["recipes"].append({"name": rf.stem, "path": str(rf)})
    # installed
    try:
        if DB and hasattr(DB, "list_installed_quick"):
            for r in DB.list_installed_quick():
                if term in r["name"]:
                    found["installed"].append(r)
    except Exception:
        pass
    print_json(found)
    return 0

def cmd_info(args):
    target = args.target
    # installed?
    try:
        if DB:
            m = DB.get_package_manifest(target)
            if m:
                print_json(m)
                return 0
    except Exception:
        pass
    # recipe?
    rp = find_recipe_by_name(target)
    if rp:
        try:
            toml_mod = safe_import("zeropkg_toml")
            if toml_mod and hasattr(toml_mod, "load_recipe"):
                rec = toml_mod.load_recipe(rp)
                print_json(rec)
                return 0
            else:
                print("Found recipe at:", rp)
                return 0
        except Exception as e:
            return _fail("failed to load recipe: " + str(e))
    return _fail("no info found for " + target)

def cmd_revdep(args):
    pkg = args.pkg
    if DEPS:
        res = DEPS.find_revdeps(pkg)
        print_json(res)
        return 0
    if DB:
        try:
            print_json(db_mod.find_revdeps(pkg))
            return 0
        except Exception:
            pass
    return _fail("revdep info not available")

def cmd_graph_deps(args):
    if not DEPS:
        return _fail("deps module not available")
    if args.packages:
        plan = DEPS.build_plan(args.packages)
        print_json(plan)
        return 0
    out = args.out
    if args.format == "dot" or (out and str(out).endswith(".dot")):
        dest = out or "zeropkg_deps.dot"
        DEPS.export_dot(dest)
        info(f"Wrote {dest}")
    else:
        dest = out or "zeropkg_deps.json"
        DEPS.export_json(dest)
        info(f"Wrote {dest}")
    return 0

def cmd_depclean(args):
    if not DEPCLEAN:
        return _fail("depclean module not available")
    only = set(args.only) if args.only else None
    exclude = set(args.exclude) if args.exclude else None
    keep = set(args.keep) if args.keep else None
    protected = set(args.protected) if args.protected else None
    apply_flag = bool(args.apply)
    res = DEPCLEAN.execute(only=only, exclude=exclude, keep=keep, apply=apply_flag, dry_run=not apply_flag,
                           backup_before_remove=args.backup, parallel=args.parallel, protected_extra=protected, report_tag=args.tag)
    print_json(res)
    if res.get("apply"):
        info("depclean applied")
    else:
        info("depclean preview (dry-run)")
    return 0

def cmd_remove(args):
    pkgs = args.packages
    if not pkgs:
        return _fail("no packages specified to remove")
    results = []
    for pkg in pkgs:
        try:
            if REMOVER:
                res = REMOVER.remove(pkg, dry_run=args.dry_run, force=args.force, with_dependents=args.with_dependents, no_backup=args.no_backup)
                results.append({"pkg": pkg, "result": res})
            else:
                # fallback to DB-only removal
                ok = False
                if DB:
                    ok = DB.remove_package_quick(pkg)
                results.append({"pkg": pkg, "db_remove": ok})
        except Exception as e:
            results.append({"pkg": pkg, "error": str(e)})
            if not args.keep_going:
                break
    print_json(results)
    return 0

def cmd_build(args):
    pkgs = args.packages
    if not pkgs:
        return _fail("no packages specified")
    # resolve
    if DEPS:
        plan = DEPS.resolve(pkgs)
        if not plan["ok"]:
            warn("dependency resolution had issues (cycles may exist)")
            # continue only if keep_going
            if not args.keep_going:
                return _fail("dependency resolution failed; use --keep-going to force")
        build_sequence = list(reversed(plan["order"])) if plan.get("order") else pkgs
    else:
        build_sequence = pkgs
    results = []
    for pkg in build_sequence:
        rp = find_recipe_by_name(pkg)
        if not rp:
            results.append({"pkg": pkg, "error": "recipe_not_found"})
            if not args.keep_going:
                break
            else:
                continue
        if not BUILDER:
            results.append({"pkg": pkg, "error": "builder_not_available"})
            continue
        try:
            out = BUILDER.build(rp, dry_run=args.dry_run, dir_install=args.dir_install, jobs=args.jobs)
            results.append({"pkg": pkg, "result": out})
            info(f"build finished for {pkg}")
        except Exception as e:
            results.append({"pkg": pkg, "error": str(e)})
            error(f"build failed for {pkg}: {e}")
            if not args.keep_going:
                break
    print_json(results)
    return 0

def cmd_install(args):
    pkgs = args.packages
    if not pkgs:
        return _fail("no packages specified to install")
    # resolve
    if DEPS:
        plan = DEPS.resolve(pkgs)
        if not plan["ok"]:
            warn("dependency resolution had issues (cycles may exist)")
            if not args.keep_going:
                return _fail("dependency resolution failed; use --keep-going to force")
        build_sequence = list(reversed(plan["order"])) if plan.get("order") else pkgs
    else:
        build_sequence = pkgs
    results = []
    for pkg in build_sequence:
        rp = find_recipe_by_name(pkg)
        if not rp:
            results.append({"pkg": pkg, "error": "recipe_not_found"})
            if not args.keep_going:
                break
            else:
                continue
        if not BUILDER:
            results.append({"pkg": pkg, "error": "builder_not_available"})
            continue
        try:
            out = BUILDER.build(rp, dry_run=args.dry_run, dir_install=args.dir_install, jobs=args.jobs)
            if args.dry_run:
                results.append({"pkg": pkg, "build": "planned", "builder_out": out})
                info(f"[dry-run] planned build for {pkg}")
                continue
            # install via installer module
            if INSTALLER and hasattr(INSTALLER, "install"):
                inst_res = INSTALLER.install(pkg, rp, root=args.root, fakeroot=args.fakeroot, dir_install=args.dir_install)
            elif installer_mod and hasattr(installer_mod, "install_pkg"):
                inst_res = installer_mod.install_pkg(rp, root=args.root, fakeroot=args.fakeroot, dir_install=args.dir_install)
            else:
                inst_res = {"status": "no_installer_available"}
            results.append({"pkg": pkg, "install": inst_res})
            info(f"installed {pkg}")
            # record to DB if manifest present
            try:
                manifest = out.get("manifest") if isinstance(out, dict) else None
                if DB and manifest:
                    DB.record_install_quick(pkg, manifest, deps=out.get("deps", []), metadata={"installed_by": "zeropkg_cli"})
            except Exception:
                pass
        except Exception as e:
            results.append({"pkg": pkg, "error": str(e)})
            error(f"install failed for {pkg}: {e}")
            if not args.keep_going:
                break
    print_json(results)
    return 0

def cmd_upgrade(args):
    if not UPGRADE or not hasattr(UPGRADE, "upgrade"):
        return _fail("upgrade module not available")
    try:
        res = UPGRADE.upgrade(args.packages or [], dry_run=args.dry_run)
        print_json(res)
        info("upgrade completed (dry-run)" if args.dry_run else "upgrade completed")
        return 0
    except Exception as e:
        return _fail(f"upgrade failed: {e}")

def cmd_update(args):
    if not UPDATE or not hasattr(UPDATE, "scan_all"):
        return _fail("update module not available")
    try:
        res = UPDATE.scan_all(dry_run=args.dry_run)
        print_json(res)
        info("update scan completed")
        return 0
    except Exception as e:
        return _fail(f"update scan failed: {e}")

def cmd_patch(args):
    if not PATCHER:
        return _fail("patcher module not available")
    try:
        out = PATCHER.apply_all(args.recipe, target_dir=args.target, dry_run=args.dry_run, use_chroot=not args.no_chroot, fakeroot=args.fakeroot, parallel=args.parallel)
        print_json(out)
        if out.get("ok"):
            info("patches applied")
        else:
            warn("patches applied with issues" if out.get("results") else "no patches applied")
        return 0
    except Exception as e:
        return _fail(f"patch failed: {e}")

def cmd_chroot(args):
    if not CHROOT:
        return _fail("chroot module not available")
    sub = args.subcmd
    try:
        if sub == "prepare":
            CHROOT.prepare_chroot(args.root, profile=args.profile, mount_proc=args.mount_proc)
            info(f"chroot prepared: {args.root}")
        elif sub == "cleanup":
            CHROOT.cleanup_chroot(args.root, force=args.force)
            info(f"chroot cleaned: {args.root}")
        elif sub == "verify":
            ok = CHROOT.verify_chroot(args.root)
            info(f"chroot verify: {ok}")
        else:
            return _fail("unknown chroot subcommand")
        return 0
    except Exception as e:
        return _fail(f"chroot op failed: {e}")

def cmd_deps(args):
    if not DEPS:
        return _fail("deps module not available")
    if args.action == "scan":
        DEPS.scan_recipes(force=args.force)
        info("deps scanned")
    elif args.action == "resolve":
        res = DEPS.resolve(args.packages)
        print_json(res)
    elif args.action == "graph":
        if args.out:
            if args.out.endswith(".dot"):
                DEPS.export_dot(args.out)
            else:
                DEPS.export_json(args.out)
            info(f"graph written to {args.out}")
        else:
            print(DEPS.graph.to_dot())
    return 0

def cmd_db(args):
    if not DB:
        return _fail("DB module not available")
    if args.action == "list":
        print_json(DB.list_installed_quick())
    elif args.action == "manifest":
        print_json(DB.get_package_manifest(args.pkg))
    elif args.action == "export":
        try:
            path = DB.export_db(Path(args.dest or "/tmp/zeropkg-db-export"), compress=not args.no_compress)
            info(f"DB exported to {path}")
        except Exception as e:
            return _fail(f"db export failed: {e}")
    return 0

def cmd_logger(args):
    if not logger_mod:
        return _fail("logger module not available")
    try:
        if args.action == "list":
            if hasattr(logger_mod, "list_sessions"):
                print_json(logger_mod.list_sessions())
            else:
                info("logger module has no list_sessions")
        elif args.action == "cleanup":
            if hasattr(logger_mod, "cleanup_old_logs"):
                logger_mod.cleanup_old_logs(args.max_age)
                info("logger cleanup triggered")
            else:
                warn("logger.cleanup not available")
        elif args.action == "upload":
            if hasattr(logger_mod, "upload_logs_now"):
                ok = logger_mod.upload_logs_now()
                info("upload result: " + str(ok))
            else:
                warn("upload function not available")
    except Exception as e:
        return _fail(f"logger command failed: {e}")
    return 0

# -------------------------
# Helpers & failures
# -------------------------
def _fail(msg: str, code: int = 2):
    error(msg)
    log_event("cli", "error", msg, level="error")
    return code

# -------------------------
# Argparse configuration
# -------------------------
def build_parser():
    parser = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg package manager CLI (integrated)")
    parser.add_argument("--config", help="Path to alternative config TOML")
    parser.add_argument("--dry-run", action="store_true", help="Global dry-run toggle")
    parser.add_argument("--jobs", "-j", type=int, default=CONFIG.get("cli", {}).get("default_jobs", 4), help="Parallel jobs")

    sub = parser.add_subparsers(dest="cmd", required=True)

    # install
    p = sub.add_parser("install", aliases=["-i"], help="Resolve deps, build and install packages")
    p.add_argument("packages", nargs="+")
    p.add_argument("--root", default="/", help="Install root (use /mnt/lfs for LFS)")
    p.add_argument("--dir-install", action="store_true")
    p.add_argument("--fakeroot", action="store_true")
    p.add_argument("--keep-going", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    # build
    p = sub.add_parser("build", aliases=["-b"], help="Build packages only")
    p.add_argument("packages", nargs="+")
    p.add_argument("--dir-install", action="store_true")
    p.add_argument("--jobs", "-j", type=int, default=CONFIG.get("cli", {}).get("default_jobs", 4))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--keep-going", action="store_true")

    # build-world
    p = sub.add_parser("build-world", help="Build world from config world.base")
    p.add_argument("--dry-run", action="store_true")

    # build-toolchain
    p = sub.add_parser("build-toolchain", help="Build LFS bootstrap toolchain (pass1/etc)")
    p.add_argument("--dry-run", action="store_true")

    # remove
    p = sub.add_parser("remove", aliases=["-r"], help="Remove packages")
    p.add_argument("packages", nargs="+")
    p.add_argument("--do-it", dest="apply", action="store_true", help="Actually remove (default: dry-run)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--no-backup", action="store_true")
    p.add_argument("--with-dependents", action="store_true")
    p.add_argument("--keep-going", action="store_true")

    # clean (depclean)
    p = sub.add_parser("clean", help="Depclean - find and remove orphaned packages")
    p.add_argument("--apply", action="store_true", help="Actually remove orphans (default dry-run)")
    p.add_argument("--only", nargs="+")
    p.add_argument("--exclude", nargs="+")
    p.add_argument("--keep", nargs="+")
    p.add_argument("--protected", nargs="+")
    p.add_argument("--backup", action="store_true")
    p.add_argument("--parallel", action="store_true")
    p.add_argument("--tag", help="Report tag")

    # upgrade
    p = sub.add_parser("upgrade", help="Upgrade packages")
    p.add_argument("packages", nargs="*", help="If empty, upgrade all updatable packages")
    p.add_argument("--dry-run", action="store_true")

    # update
    p = sub.add_parser("update", help="Scan upstreams for updates")
    p.add_argument("--dry-run", action="store_true")

    # sync
    p = sub.add_parser("sync", help="Sync repositories")
    p.add_argument("--dry-run", action="store_true")

    # patch
    p = sub.add_parser("patch", help="Apply patches from recipe")
    p.add_argument("recipe")
    p.add_argument("--target")
    p.add_argument("--no-chroot", action="store_true")
    p.add_argument("--fakeroot", action="store_true")
    p.add_argument("--parallel", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    # deps
    p = sub.add_parser("deps", help="Dependency manager actions")
    p.add_argument("action", choices=["scan", "resolve", "graph"])
    p.add_argument("packages", nargs="*", help="Packages for resolve")
    p.add_argument("--out")
    p.add_argument("--force", action="store_true")

    # graph-deps (alias)
    p = sub.add_parser("graph-deps", help="Export/show dependency graph")
    p.add_argument("--out")
    p.add_argument("--format", choices=["dot","json"], default="json")
    p.add_argument("--packages", nargs="*")

    # chroot
    p = sub.add_parser("chroot", help="Chroot operations")
    chsub = p.add_subparsers(dest="subcmd", required=True)
    ch_prep = chsub.add_parser("prepare")
    ch_prep.add_argument("root")
    ch_prep.add_argument("--profile", default="lfs")
    ch_prep.add_argument("--no-mount-proc", dest="mount_proc", action="store_false")
    ch_cleanup = chsub.add_parser("cleanup")
    ch_cleanup.add_argument("root")
    ch_cleanup.add_argument("--force", action="store_true")
    ch_verify = chsub.add_parser("verify")
    ch_verify.add_argument("root")

    # db
    p = sub.add_parser("db", help="DB utilities")
    p.add_argument("action", choices=["list","manifest","export"])
    p.add_argument("--pkg")
    p.add_argument("--dest")
    p.add_argument("--no-compress", action="store_true")

    # logger
    p = sub.add_parser("logger", help="Logger utilities")
    p.add_argument("action", choices=["list","cleanup","upload"])
    p.add_argument("--max-age", type=int, default=30)

    # misc
    p = sub.add_parser("search", help="Search recipes and installed packages")
    p.add_argument("term")
    p_info = sub.add_parser("info", help="Show recipe or package info")
    p_info.add_argument("target")
    p_rev = sub.add_parser("revdep", help="Show reverse deps")
    p_rev.add_argument("pkg")

    return parser

# -------------------------
# Main entry
# -------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Per-command dry-run precedence
    if getattr(args, "dry_run", False):
        args_global_dry = True
    else:
        args_global_dry = False

    cmd = args.cmd

    try:
        if cmd in ("install", "-i"):
            return cmd_install(args)
        if cmd in ("build", "-b"):
            return cmd_build(args)
        if cmd == "build-world":
            # convenience: read world.base from config
            world = CONFIG.get("world", {}).get("base", [])
            if not world:
                warn("world.base not defined in config")
                return 0
            # call resolve_and_build or build sequence
            if DEPS:
                res = DEPS.resolve_and_build(world, dry_run=args.dry_run)
                print_json(res)
                return 0
            else:
                # fallback: build sequentially
                for p in world:
                    ret = cmd_build(argparse.Namespace(packages=[p], dir_install=False, jobs=args.jobs, dry_run=args.dry_run, keep_going=False))
                return 0
        if cmd == "build-toolchain":
            if not BUILDER or not hasattr(BUILDER, "build_toolchain"):
                return _fail("builder.toolchain not available")
            out = BUILDER.build_toolchain(dry_run=args.dry_run)
            print_json(out)
            return 0
        if cmd == "remove":
            return cmd_remove(args)
        if cmd == "clean":
            return cmd_depclean(args)
        if cmd == "upgrade":
            return cmd_upgrade(args)
        if cmd == "update":
            return cmd_update(args)
        if cmd == "sync":
            return cmd_sync(args)
        if cmd == "patch":
            return cmd_patch(args)
        if cmd == "chroot":
            return cmd_chroot(args)
        if cmd == "deps":
            return cmd_deps(args)
        if cmd == "graph-deps":
            return cmd_graph_deps(args)
        if cmd == "db":
            return cmd_db(args)
        if cmd == "logger":
            return cmd_logger(args)
        if cmd == "search":
            return cmd_search(args)
        if cmd == "info":
            return cmd_info(args)
        if cmd == "revdep":
            return cmd_revdep(args)
        # unknown
        parser.print_help()
        return 0
    except KeyboardInterrupt:
        error("aborted by user")
        return 130
    except Exception as e:
        error(f"unexpected error: {e}")
        log_event("cli", "error", f"exception: {e}", level="error")
        return 1

# -------------------------
# Entrypoint fallback to the per-command functions defined above
# (they must be visible by name)
# -------------------------
# map function names to actual function objects used above
# (these are local defs - so ensure names exist)
globals().setdefault("cmd_sync", cmd_sync)
globals().setdefault("cmd_search", cmd_search)
globals().setdefault("cmd_info", cmd_info)
globals().setdefault("cmd_revdep", cmd_revdep)
globals().setdefault("cmd_graph_deps", cmd_graph_deps)
globals().setdefault("cmd_depclean", cmd_depclean)
globals().setdefault("cmd_remove", cmd_remove)
globals().setdefault("cmd_build", cmd_build)
globals().setdefault("cmd_install", cmd_install)
globals().setdefault("cmd_upgrade", cmd_upgrade)
globals().setdefault("cmd_update", cmd_update)
globals().setdefault("cmd_patch", cmd_patch)
globals().setdefault("cmd_chroot", cmd_chroot)
globals().setdefault("cmd_deps", cmd_deps)
globals().setdefault("cmd_db", cmd_db)
globals().setdefault("cmd_logger", cmd_logger)

if __name__ == "__main__":
    sys.exit(main())
