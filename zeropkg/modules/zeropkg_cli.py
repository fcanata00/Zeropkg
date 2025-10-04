#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_cli.py — Zeropkg Unified Command Interface

CLI central para o gerenciador Zeropkg.
Suporta:
  - Construção completa de pacotes (com dependências)
  - Instalação, remoção e atualização
  - Grafo de dependências e sincronização
  - Hooks, chroot, fakeroot e rollback automáticos
  - Logs unificados e modo dry-run
"""

import argparse
import sys
import os
import traceback
from pathlib import Path

# --- Módulos internos do Zeropkg
from zeropkg_logger import log_global, log_event, get_logger
from zeropkg_config import load_config
from zeropkg_builder import Builder
from zeropkg_installer import Installer
from zeropkg_deps import DependencyResolver
from zeropkg_upgrade import UpgradeManager
from zeropkg_update import UpdateManager
from zeropkg_depclean import DepCleaner
from zeropkg_sync import sync_repos
from zeropkg_remover import Remover
from zeropkg_db import DBManager

_logger = get_logger("cli")


# ================================
# 🧱 Funções principais
# ================================

def cmd_build(args, cfg):
    builder = Builder(cfg)
    if args.pkg:
        log_event(args.pkg, "build", "Building package")
        builder.build(args.pkg, dry_run=args.dry_run)
    elif args.full_build:
        cmd_full_build(args, cfg)
    else:
        print("You must specify a package or use --full-build")
        sys.exit(1)

def cmd_install(args, cfg):
    inst = Installer(cfg)
    for pkg in args.pkgs:
        inst.install(pkg, Path(f"/usr/ports/distfiles/{pkg}.tar.xz"),
                     dry_run=args.dry_run, parallel=args.parallel)

def cmd_remove(args, cfg):
    rm = Remover(cfg)
    for pkg in args.pkgs:
        rm.remove(pkg, dry_run=args.dry_run)

def cmd_upgrade(args, cfg):
    um = UpgradeManager(cfg)
    um.upgrade_all(dry_run=args.dry_run)

def cmd_update(args, cfg):
    um = UpdateManager(cfg)
    um.check_for_updates()
    log_global("Update check complete.")

def cmd_depclean(args, cfg):
    cleaner = DepCleaner(cfg)
    cleaner.clean_orphans(dry_run=args.dry_run)

def cmd_sync(args, cfg):
    sync_repos(cfg)
    log_global("Repository sync complete.")

def cmd_revdep(args, cfg):
    deps = DependencyResolver(cfg)
    for pkg in args.pkgs:
        rev = deps.reverse_dependencies(pkg)
        print(f"{pkg} reverse dependencies: {rev}")

def cmd_info(args, cfg):
    db = DBManager(cfg["paths"]["db_path"])
    with db:
        info = db.get_package_info(args.pkg)
        if info:
            print(f"Package: {args.pkg}")
            for k, v in info.items():
                print(f"  {k}: {v}")
        else:
            print(f"No info found for {args.pkg}")

def cmd_graph_deps(args, cfg):
    deps = DependencyResolver(cfg)
    graph = deps.build_graph()
    output = args.output or "/var/lib/zeropkg/dependency-graph.dot"
    deps.export_graphviz(graph, output)
    log_global(f"Dependency graph written to {output}")

def cmd_full_build(args, cfg):
    deps = DependencyResolver(cfg)
    builder = Builder(cfg)
    inst = Installer(cfg)

    target = args.pkg or "world"
    dep_order = deps.resolve_dependencies(target)
    log_global(f"Full build order: {dep_order}")

    for pkg in dep_order:
        try:
            builder.build(pkg, dry_run=args.dry_run)
            inst.install(pkg, Path(f"/usr/ports/distfiles/{pkg}.tar.xz"), dry_run=args.dry_run)
        except Exception as e:
            log_event(pkg, "full_build", f"Error building {pkg}: {e}", "error")
            traceback.print_exc()
            break


# ================================
# 🧠 CLI principal
# ================================

def main():
    cfg = load_config()
    parser = argparse.ArgumentParser(
        description="Zeropkg - Source-based Linux package manager"
    )

    sub = parser.add_subparsers(dest="command")

    # Build
    p_build = sub.add_parser("build", help="Build a package")
    p_build.add_argument("pkg", nargs="?", help="Package name to build")
    p_build.add_argument("--dry-run", action="store_true", help="Simulate build")
    p_build.add_argument("--full-build", action="store_true", help="Build all dependencies recursively")
    p_build.set_defaults(func=lambda a: cmd_build(a, cfg))

    # Install
    p_install = sub.add_parser("install", help="Install one or more packages")
    p_install.add_argument("pkgs", nargs="+", help="Package(s) to install")
    p_install.add_argument("--dry-run", action="store_true")
    p_install.add_argument("--parallel", action="store_true")
    p_install.set_defaults(func=lambda a: cmd_install(a, cfg))

    # Remove
    p_remove = sub.add_parser("remove", help="Remove packages")
    p_remove.add_argument("pkgs", nargs="+", help="Packages to remove")
    p_remove.add_argument("--dry-run", action="store_true")
    p_remove.set_defaults(func=lambda a: cmd_remove(a, cfg))

    # Upgrade
    p_upgrade = sub.add_parser("upgrade", help="Upgrade installed packages")
    p_upgrade.add_argument("--dry-run", action="store_true")
    p_upgrade.set_defaults(func=lambda a: cmd_upgrade(a, cfg))

    # Update
    p_update = sub.add_parser("update", help="Check for upstream updates")
    p_update.set_defaults(func=lambda a: cmd_update(a, cfg))

    # Depclean
    p_depclean = sub.add_parser("depclean", help="Remove orphan dependencies")
    p_depclean.add_argument("--dry-run", action="store_true")
    p_depclean.set_defaults(func=lambda a: cmd_depclean(a, cfg))

    # Sync
    p_sync = sub.add_parser("sync", help="Sync repositories")
    p_sync.set_defaults(func=lambda a: cmd_sync(a, cfg))

    # Reverse dependencies
    p_revdep = sub.add_parser("revdep", help="Show reverse dependencies")
    p_revdep.add_argument("pkgs", nargs="+", help="Package(s)")
    p_revdep.set_defaults(func=lambda a: cmd_revdep(a, cfg))

    # Info
    p_info = sub.add_parser("info", help="Show package info")
    p_info.add_argument("pkg", help="Package name")
    p_info.set_defaults(func=lambda a: cmd_info(a, cfg))

    # Graph dependencies
    p_graph = sub.add_parser("graph-deps", help="Generate dependency graph (Graphviz)")
    p_graph.add_argument("--output", help="Output file path (.dot)")
    p_graph.set_defaults(func=lambda a: cmd_graph_deps(a, cfg))

    # Full system build
    p_full = sub.add_parser("full-build", help="Build and install all dependencies recursively")
    p_full.add_argument("pkg", nargs="?", help="Target package (default=world)")
    p_full.add_argument("--dry-run", action="store_true")
    p_full.set_defaults(func=lambda a: cmd_full_build(a, cfg))

    # Parse args
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    try:
        args.func(args)
    except KeyboardInterrupt:
        log_global("Operation cancelled by user", "warning")
        sys.exit(1)
    except Exception as e:
        log_global(f"Command failed: {e}", "error")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
