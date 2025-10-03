import argparse
import sys
import os
from zeropkg_toml import parse_package_file
from zeropkg_builder import Builder
from zeropkg_installer import Installer
from zeropkg_deps import check_missing
from zeropkg_depclean import depclean, revdep
from zeropkg_logger import log_event
from zeropkg_upgrade import upgrade_package, upgrade_all

PORTS_DIR = "/usr/ports"
PKG_CACHE = "/var/zeropkg/packages"
DB_PATH = "/var/lib/zeropkg/installed.json"

def find_metafile(pkgname: str):
    for root, dirs, files in os.walk(PORTS_DIR):
        for f in files:
            if f.startswith(pkgname) and f.endswith(".toml"):
                return os.path.join(root, f)
    return None

def main():
    parser = argparse.ArgumentParser(prog="zeropkg", description="ZeroPkg package manager")

    parser.add_argument("-i", "--install", help="Instalar pacote")
    parser.add_argument("-r", "--remove", help="Remover pacote (nome:versão)")
    parser.add_argument("-b", "--build", help="Compilar pacote sem instalar")
    parser.add_argument("-s", "--search", help="Procurar pacote por nome")
    parser.add_argument("--info", help="Mostrar informações de um pacote")
    parser.add_argument("--depclean", action="store_true", help="Remover dependências órfãs")
    parser.add_argument("--revdep", action="store_true", help="Checar dependências quebradas")
    parser.add_argument("-u", "--upgrade", help="Atualizar pacote para a última versão disponível")
    parser.add_argument("--upgrade-all", action="store_true", help="Atualizar todos os pacotes")

    parser.add_argument("--dry-run", action="store_true", help="Simular execução sem aplicar")
    parser.add_argument("--dir-install", help="Instalar em diretório alternativo (chroot)")

    args = parser.parse_args()

    if args.install:
        pkgname = args.install
        metafile = find_metafile(pkgname)
        if not metafile:
            print(f"Pacote {pkgname} não encontrado em {PORTS_DIR}")
            sys.exit(1)

        meta = parse_package_file(metafile)
        log_event(meta.name, "install", f"Iniciando instalação {meta.name}-{meta.version}")

        missing = check_missing(meta, db_path=DB_PATH)
        if missing:
            print("Resolvendo dependências:", missing)
            for dep in missing:
                dep_meta_file = find_metafile(dep.split(":")[0])
                if not dep_meta_file:
                    print(f"Dependência {dep} não encontrada nos ports.")
                    sys.exit(1)
                dep_meta = parse_package_file(dep_meta_file)
                builder = Builder(dep_meta, pkg_cache=PKG_CACHE, dry_run=args.dry_run)
                builder.fetch_sources()
                builder.extract_sources()
                builder.build()
                pkgfile = builder.package()
                installer = Installer(DB_PATH, dry_run=args.dry_run, dir_install=args.dir_install)
                installer.install(pkgfile, dep_meta)

        builder = Builder(meta, pkg_cache=PKG_CACHE, dry_run=args.dry_run, dir_install=args.dir_install)
        builder.fetch_sources()
        builder.extract_sources()
        builder.build()
        pkgfile = builder.package()

        installer = Installer(DB_PATH, dry_run=args.dry_run, dir_install=args.dir_install)
        installer.install(pkgfile, meta)

    elif args.remove:
        name, _, version = args.remove.partition(":")
        installer = Installer(DB_PATH, dry_run=args.dry_run)
        installer.remove(name, version)

    elif args.build:
        pkgname = args.build
        metafile = find_metafile(pkgname)
        if not metafile:
            print(f"Pacote {pkgname} não encontrado.")
            sys.exit(1)
        meta = parse_package_file(metafile)
        builder = Builder(meta, pkg_cache=PKG_CACHE, dry_run=args.dry_run, dir_install=args.dir_install)
        builder.fetch_sources()
        builder.extract_sources()
        builder.build()
        pkgfile = builder.package()
        print(f"Pacote gerado em {pkgfile}")

    elif args.depclean:
        orphans = depclean(DB_PATH)
        if not orphans:
            print("Nenhum órfão encontrado.")
        else:
            print("Pacotes órfãos:", orphans)

    elif args.revdep:
        broken = revdep(DB_PATH)
        if not broken:
            print("Nenhuma dependência quebrada encontrada.")
        else:
            for pkg, missing in broken.items():
                print(f"{pkg} depende de pacotes ausentes: {missing}")

    elif args.search:
        results = []
        for root, dirs, files in os.walk(PORTS_DIR):
            for f in files:
                if f.endswith(".toml") and args.search in f:
                    results.append(f)
        if not results:
            print("Nenhum pacote encontrado.")
        else:
            print("Resultados:", results)

    elif args.info:
        metafile = find_metafile(args.info)
        if not metafile:
            print("Pacote não encontrado.")
            return
        meta = parse_package_file(metafile)
        print(f"Nome: {meta.name}")
        print(f"Versão: {meta.version}")
        print(f"Descrição: {getattr(meta, 'description', 'N/A')}")
        print(f"Dependências: {getattr(meta, 'dependencies', [])}")

    elif args.upgrade:
        pkgname = args.upgrade
        ok = upgrade_package(pkgname, db_path=DB_PATH, ports_dir=PORTS_DIR,
                             pkg_cache=PKG_CACHE, dry_run=args.dry_run,
                             dir_install=args.dir_install, verbose=True)
        if not ok:
            sys.exit(1)

    elif args.upgrade_all:
        results = upgrade_all(db_path=DB_PATH, ports_dir=PORTS_DIR,
                              pkg_cache=PKG_CACHE, dry_run=args.dry_run,
                              dir_install=args.dir_install, verbose=True)
        for name, ok in results:
            status = "OK" if ok else "FALHA"
            print(f"{name}: {status}")

    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
