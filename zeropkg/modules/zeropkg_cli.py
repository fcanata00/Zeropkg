#!/usr/bin/env python3
"""
zeropkg_cli.py

Interface de linha de comando para Zeropkg, integrando todos os módulos:
- install / remove / build / upgrade / update / depclean / revdep / search / info
- Suporte para abreviações (-i, -r, -b, -u, -U, -s) e versões longas (--install, etc.)
- Flags --dry-run e --dir-install
"""

import argparse
import sys
import os

from zeropkg_toml import parse_package_file
from zeropkg_builder import Builder
from zeropkg_installer import Installer
from zeropkg_deps import check_missing
from zeropkg_depclean import depclean, revdep
from zeropkg_upgrade import upgrade_package, upgrade_all
from zeropkg_update import run_update_scan
from zeropkg_logger import log_event
from zeropkg_db import connect, get_package

# configurações padrão
PORTS_DIR = "/usr/ports"
PKG_CACHE = "/var/zeropkg/packages"
DB_PATH = "/var/lib/zeropkg/installed.sqlite3"

def find_metafile(pkgname: str):
    """Procura o arquivo .toml correspondente a pkgname sob PORTS_DIR."""
    for root, dirs, files in os.walk(PORTS_DIR):
        for f in files:
            if f.endswith(".toml") and f.startswith(pkgname + "-"):
                return os.path.join(root, f)
    return None

def cmd_install(pkgname: str, args):
    mf = find_metafile(pkgname)
    if not mf:
        print(f"Pacote {pkgname} não encontrado em {PORTS_DIR}")
        sys.exit(1)
    meta = parse_package_file(mf)
    log_event(meta.name, "cli", f"Comando install para {meta.name}-{meta.version}")

    missing = check_missing(meta, db_path=DB_PATH)
    if missing:
        print("Dependências faltantes:", missing)
        for dep in missing:
            depname = dep.split(":")[0]
            mf_dep = find_metafile(depname)
            if not mf_dep:
                print(f"Metafile da dependência {dep} não encontrado.")
                sys.exit(1)
            dep_meta = parse_package_file(mf_dep)
            b = Builder(dep_meta, cache_dir=PKG_CACHE, pkg_cache=PKG_CACHE,
                        dry_run=args.dry_run, dir_install=args.dir_install)
            b.fetch_sources()
            b.extract_sources()
            b.build()
            pkgfile_dep = b.package()
            inst_dep = Installer(DB_PATH, dry_run=args.dry_run, dir_install=args.dir_install)
            inst_dep.install(pkgfile_dep, dep_meta)

    b = Builder(meta, cache_dir=PKG_CACHE, pkg_cache=PKG_CACHE,
                dry_run=args.dry_run, dir_install=args.dir_install)
    b.fetch_sources()
    b.extract_sources()
    b.build()
    pkgfile = b.package()

    inst = Installer(DB_PATH, dry_run=args.dry_run, dir_install=args.dir_install)
    inst.install(pkgfile, meta)

def cmd_remove(target: str, args):
    # target pode ser "pkgname:version" ou só "pkgname"
    name, _, version = target.partition(":")
    inst = Installer(DB_PATH, dry_run=args.dry_run, dir_install=args.dir_install)
    inst.remove(name, version)

def cmd_build(pkgname: str, args):
    mf = find_metafile(pkgname)
    if not mf:
        print(f"Pacote {pkgname} não encontrado")
        sys.exit(1)
    meta = parse_package_file(mf)
    b = Builder(meta, cache_dir=PKG_CACHE, pkg_cache=PKG_CACHE,
                dry_run=args.dry_run, dir_install=args.dir_install)
    b.fetch_sources()
    b.extract_sources()
    b.build()
    pkgfile = b.package()
    print(f"Pacote gerado: {pkgfile}")

def cmd_upgrade(pkgname: str, args):
    ok = upgrade_package(pkgname,
                         db_path=DB_PATH,
                         ports_dir=PORTS_DIR,
                         pkg_cache=PKG_CACHE,
                         dry_run=args.dry_run,
                         dir_install=args.dir_install,
                         verbose=True)
    if not ok:
        sys.exit(1)

def cmd_upgrade_all(args):
    results = upgrade_all(db_path=DB_PATH,
                          ports_dir=PORTS_DIR,
                          pkg_cache=PKG_CACHE,
                          dry_run=args.dry_run,
                          dir_install=args.dir_install,
                          verbose=True)
    for name, ok in results:
        status = "OK" if ok else "FAIL"
        print(f"{name}: {status}")

def cmd_update(args):
    """Executa o scan upstream de novas versões e gera os relatórios."""
    log_event("cli", "update", "Executando scan de updates")
    run_update_scan(ports_dir=PORTS_DIR)
    print("Scan de updates concluído. Verifique os arquivos de notificação e reports.")

def cmd_depclean(args):
    orphans = depclean(DB_PATH)
    if not orphans:
        print("Nenhum órfão encontrado.")
    else:
        print("Pacotes órfãos:", orphans)

def cmd_revdep(args):
    broken = revdep(DB_PATH)
    if not broken:
        print("Nenhuma dependência quebrada encontrada.")
    else:
        for pkg, mis in broken.items():
            print(f"{pkg} depende de ausentes: {mis}")

def cmd_search(query: str, args):
    results = []
    for root, dirs, files in os.walk(PORTS_DIR):
        for f in files:
            if f.endswith(".toml") and query in f:
                results.append(f)
    if not results:
        print("Nenhum pacote encontrado.")
    else:
        print("Encontrados:", results)

def cmd_info(pkgname: str, args):
    mf = find_metafile(pkgname)
    if not mf:
        print("Pacote não encontrado.")
        return
    meta = parse_package_file(mf)
    print(f"Nome: {meta.name}")
    print(f"Versão: {meta.version}")
    print(f"Dependências: {meta.dependencies}")

def main():
    parser = argparse.ArgumentParser(prog="zeropkg", description="Gerenciador ZeroPkg")
    parser.add_argument("-i", "--install", help="Instalar pacote")
    parser.add_argument("-r", "--remove", help="Remover pacote (nome:versão)")
    parser.add_argument("-b", "--build", help="Compilar apenas")
    parser.add_argument("-u", "--upgrade", help="Atualizar pacote")
    parser.add_argument("-U", "--update", action="store_true", help="Scan upstream de novas versões")
    parser.add_argument("--upgrade-all", action="store_true", help="Atualizar todos os pacotes instalados")
    parser.add_argument("--depclean", action="store_true", help="Remover dependências órfãs")
    parser.add_argument("--revdep", action="store_true", help="Checar dependências quebradas")
    parser.add_argument("-s", "--search", help="Pesquisar pacote")
    parser.add_argument("--info", help="Mostrar informações de pacote")

    parser.add_argument("--dry-run", action="store_true", help="Simular sem aplicar mudanças")
    parser.add_argument("--dir-install", help="Instalar em diretório alternativo (ex: chroot)")

    args = parser.parse_args()

    # garantir que apenas um comando principal seja usado
    cmd_count = sum(bool(x) for x in [
        args.install, args.remove, args.build,
        args.upgrade, args.update, args.upgrade_all,
        args.depclean, args.revdep, args.search, args.info
    ])
    if cmd_count != 1:
        parser.print_help()
        sys.exit(1)

    if args.install:
        cmd_install(args.install, args)
    elif args.remove:
        cmd_remove(args.remove, args)
    elif args.build:
        cmd_build(args.build, args)
    elif args.upgrade:
        cmd_upgrade(args.upgrade, args)
    elif args.update:
        cmd_update(args)
    elif args.upgrade_all:
        cmd_upgrade_all(args)
    elif args.depclean:
        cmd_depclean(args)
    elif args.revdep:
        cmd_revdep(args)
    elif args.search:
        cmd_search(args.search, args)
    elif args.info:
        cmd_info(args.info, args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
