#!/usr/bin/env python3
"""
zeropkg_cli.py - CLI integrado para Zeropkg
"""

import os
import sys
import argparse
import traceback

MODULES_PATH = "/usr/lib/zeropkg/modules"
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

# imports dos módulos
from zeropkg_toml import parse_toml
from zeropkg_builder import Builder
from zeropkg_installer import Installer
from zeropkg_sync import sync_repos
from zeropkg_update import run_update_scan
from zeropkg_depclean import depclean, revdep
from zeropkg_upgrade import upgrade_package, upgrade_all
from zeropkg_remover import remove_package
from zeropkg_db import get_package
from zeropkg_logger import log_event
from zeropkg_deps import check_missing
from zeropkg_config import load_config, get_paths


# --- utils ---
def find_metafile(pkgname: str, ports_dir: str) -> str:
    for root, _, files in os.walk(ports_dir):
        for f in files:
            if f.endswith(".toml") and f.startswith(pkgname + "-"):
                return os.path.join(root, f)
    candidate = os.path.join(ports_dir, pkgname, f"{pkgname}.toml")
    if os.path.exists(candidate):
        return candidate
    raise FileNotFoundError(f"Metafile para {pkgname} não encontrado em {ports_dir}")


def load_meta_from_name(pkgname: str, ports_dir: str):
    return parse_toml(find_metafile(pkgname, ports_dir))


def safe_print_exc(e: Exception):
    print(f"Erro: {e}")
    traceback.print_exc()


# --- commands ---
def cmd_install(pkgname: str, args):
    meta = load_meta_from_name(pkgname, args.ports_dir)
    log_event(pkgname, "cli", f"install {pkgname} (dry_run={args.dry_run})")

    missing = check_missing(meta, db_path=args.db_path)
    if missing:
        print("Dependências faltantes:", missing)
        for dep in missing:
            dep_meta = load_meta_from_name(dep, args.ports_dir)
            print(f"[+] Construindo dependência {dep}")
            builder = Builder(dep_meta,
                              cache_dir=args.cache_dir,
                              pkg_cache=args.cache_dir,
                              build_root=args.build_root,
                              dry_run=args.dry_run,
                              use_fakeroot=args.fakeroot,
                              chroot=args.chroot_root,
                              db_path=args.db_path)
            pkgfile_dep = builder.build(dir_install=None)
            installer_dep = Installer(db_path=args.db_path,
                                      dry_run=args.dry_run,
                                      root=args.root,
                                      use_fakeroot=args.fakeroot)
            installer_dep.install(pkgfile_dep, dep_meta, compute_hash=True, run_hooks=True)

    builder = Builder(meta,
                      cache_dir=args.cache_dir,
                      pkg_cache=args.cache_dir,
                      build_root=args.build_root,
                      dry_run=args.dry_run,
                      use_fakeroot=args.fakeroot,
                      chroot=args.chroot_root,
                      db_path=args.db_path)
    pkgfile = builder.build(dir_install=None)

    installer = Installer(db_path=args.db_path,
                          dry_run=args.dry_run,
                          root=args.root,
                          use_fakeroot=args.fakeroot)
    installed = installer.install(pkgfile, meta, compute_hash=True, run_hooks=True)
    print(f"Instalados {len(installed)} arquivos.")


def cmd_remove(target: str, args):
    name, _, version = target.partition(":")
    res = remove_package(name,
                         version=version or None,
                         db_path=args.db_path,
                         ports_dir=args.ports_dir,
                         root=args.root,
                         dry_run=args.dry_run,
                         use_fakeroot=args.fakeroot,
                         force=args.force)
    print(res["message"])


def cmd_build(pkgname: str, args):
    meta = load_meta_from_name(pkgname, args.ports_dir)
    builder = Builder(meta,
                      cache_dir=args.cache_dir,
                      pkg_cache=args.cache_dir,
                      build_root=args.build_root,
                      dry_run=args.dry_run,
                      use_fakeroot=args.fakeroot,
                      chroot=args.chroot_root,
                      db_path=args.db_path)
    pkgfile = builder.build(dir_install=args.dir_install)
    print(f"Pacote gerado: {pkgfile}")


def cmd_upgrade(pkgname: str, args):
    ok = upgrade_package(pkgname,
                         db_path=args.db_path,
                         ports_dir=args.ports_dir,
                         pkg_cache=args.cache_dir,
                         dry_run=args.dry_run,
                         root=args.root,
                         backup=True,
                         verbose=True)
    print("Upgrade concluído." if ok else "Upgrade falhou.")


def cmd_upgrade_all(args):
    res = upgrade_all(db_path=args.db_path,
                      ports_dir=args.ports_dir,
                      pkg_cache=args.cache_dir,
                      dry_run=args.dry_run,
                      root=args.root,
                      verbose=True)
    ok = sum(1 for _, s in res if s)
    fail = len(res) - ok
    print(f"Upgrade all: {ok} succeeded, {fail} failed")


def cmd_update(args):
    res = run_update_scan(dry_run=args.dry_run)
    print(res)


def cmd_sync(args):
    print("Sincronizando repositório...")
    sync_repos()
    print("Sync concluído.")


def cmd_depclean(args):
    orphans = depclean(args.db_path)
    print("Órfãos:" if orphans else "Nenhum órfão encontrado.")
    for o in orphans:
        print("  ", o)


def cmd_revdep(args):
    broken = revdep(args.db_path)
    print("Dependências quebradas:" if broken else "Nenhuma dependência quebrada.")
    for pkg, mis in broken.items():
        print(f"{pkg} depende de: {mis}")


def cmd_search(query: str, args):
    results = []
    for root, _, files in os.walk(args.ports_dir):
        for f in files:
            if f.endswith(".toml") and query in f:
                results.append(os.path.join(root, f))
    print("\n".join(results) if results else "Nenhum resultado.")


def cmd_info(pkgname: str, args):
    meta = load_meta_from_name(pkgname, args.ports_dir)
    print(f"Name: {meta.name}")
    print(f"Version: {meta.version}")
    print(f"Category: {getattr(meta, 'category', '')}")
    print(f"Description: {getattr(meta, 'description', '')}")
    print(f"Homepage: {getattr(meta, 'homepage', '')}")
    print(f"Dependencies: {getattr(meta, 'dependencies', {})}")
    print(f"Build directives: {getattr(meta, 'build', {})}")


# --- CLI entrypoint ---
def build_parser():
    paths = get_paths()
    p = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg - source-based package manager")
    p.add_argument("-i", "--install", metavar="PKG")
    p.add_argument("-r", "--remove", metavar="PKG[:VER]")
    p.add_argument("-b", "--build", metavar="PKG")
    p.add_argument("-u", "--upgrade", metavar="PKG")
    p.add_argument("--upgrade-all", action="store_true")
    p.add_argument("-U", "--update", action="store_true")
    p.add_argument("--sync", action="store_true")
    p.add_argument("--depclean", action="store_true")
    p.add_argument("--revdep", action="store_true")
    p.add_argument("-s", "--search", metavar="QUERY")
    p.add_argument("--info", metavar="PKG")
    # flags
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--root", default=paths["root"])
    p.add_argument("--fakeroot", action="store_true")
    p.add_argument("--force", action="store_true", help="força remoção mesmo se houver revdeps")
    p.add_argument("--chroot-root", default=None)
    p.add_argument("--dir-install", default=None)
    p.add_argument("--cache-dir", default=paths["cache_dir"])
    p.add_argument("--build-root", default=paths["build_root"])
    p.add_argument("--ports-dir", default=paths["ports_dir"])
    p.add_argument("--db-path", default=paths["db_path"])
    return p


def main():
    p = build_parser()
    args = p.parse_args()

    cmds = [args.install, args.remove, args.build, args.upgrade,
            args.update, args.sync, args.upgrade_all, args.depclean,
            args.revdep, args.search, args.info]

    if sum(bool(c) for c in cmds) != 1:
        p.print_help()
        sys.exit(1)

    if args.install:
        cmd_install(args.install, args)
    elif args.remove:
        cmd_remove(args.remove, args)
    elif args.build:
        cmd_build(args.build, args)
    elif args.upgrade:
        cmd_upgrade(args.upgrade, args)
    elif args.upgrade_all:
        cmd_upgrade_all(args)
    elif args.update:
        cmd_update(args)
    elif args.sync:
        cmd_sync(args)
    elif args.depclean:
        cmd_depclean(args)
    elif args.revdep:
        cmd_revdep(args)
    elif args.search:
        cmd_search(args.search, args)
    elif args.info:
        cmd_info(args.info, args)


if __name__ == "__main__":
    main()
