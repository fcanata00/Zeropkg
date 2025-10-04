#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zeropkg CLI — interface de linha de comando principal.
Mantém todos os comandos existentes e adiciona:
- graph-deps
- full-build
"""

import os
import sys
import argparse
from zeropkg_config import load_config
from zeropkg_logger import log_event, get_logger

logger = get_logger("cli")

# ============================================================
# Comandos existentes
# ============================================================

def cmd_build(args):
    from zeropkg_builder import Builder
    b = Builder(config_path=args.config)
    b.build(args.target, args=args, dry_run=args.dry_run, rebuild=args.rebuild)

def cmd_install(args):
    from zeropkg_installer import Installer
    i = Installer(args.config)
    meta = {}
    i.install(args.target, {"root": args.root, "dry_run": args.dry_run}, meta)

def cmd_remove(args):
    from zeropkg_remover import Remover
    r = Remover(args.config)
    r.remove(args.target, force=args.force)

def cmd_depclean(args):
    from zeropkg_depclean import DepCleaner
    cleaner = DepCleaner(args.config)
    cleaner.clean(dry_run=args.dry_run, force=args.force)

def cmd_sync(args):
    from zeropkg_sync import sync_repos
    sync_repos()

def cmd_upgrade(args):
    from zeropkg_upgrade import UpgradeManager
    upgr = UpgradeManager(args.config)
    upgr.upgrade(args.target, dry_run=args.dry_run, force=args.force)

def cmd_update(args):
    from zeropkg_update import Updater
    updater = Updater(args.config)
    updater.check_updates(report=True)

def cmd_revdep(args):
    from zeropkg_deps import DependencyResolver
    cfg = load_config(args.config)
    resolver = DependencyResolver(cfg["paths"]["db_path"], cfg["paths"]["ports_dir"])
    print(f"Dependências reversas de {args.package}:")
    for pkg in resolver.reverse_deps(args.package):
        print("  -", pkg)

def cmd_info(args):
    import toml
    cfg = load_config(args.config)
    ports_dir = cfg["paths"]["ports_dir"]
    matches = list(os.popen(f"find {ports_dir} -type f -name '{args.package}-*.toml'").read().splitlines())
    if not matches:
        print(f"Pacote {args.package} não encontrado.")
        return
    data = toml.load(matches[-1])
    print(f"\nInformações sobre {args.package}:")
    for sec, val in data.items():
        print(f"[{sec}]")
        if isinstance(val, dict):
            for k, v in val.items():
                print(f"  {k} = {v}")
        print()

# ============================================================
# NOVOS COMANDOS
# ============================================================

def cmd_graph_deps(args):
    from zeropkg_deps import DependencyResolver
    cfg = load_config(args.config)
    resolver = DependencyResolver(cfg["paths"]["db_path"], cfg["paths"]["ports_dir"])

    if args.all:
        graph = resolver.build_graph_all()
    else:
        graph = resolver.build_graph_for(args.package)

    print("\n=== GRAFO DE DEPENDÊNCIAS ===")
    for pkg, deps in graph.items():
        print(f"{pkg} -> {', '.join(deps) if deps else '(sem deps)'}")

    if args.export:
        with open(args.export, "w") as f:
            f.write("digraph deps {\n")
            for pkg, deps in graph.items():
                for dep in deps:
                    f.write(f'  "{pkg}" -> "{dep}";\n')
            f.write("}\n")
        print(f"Grafo exportado para {args.export}")

def cmd_full_build(args):
    from zeropkg_builder import Builder
    from zeropkg_installer import Installer
    from zeropkg_deps import DependencyResolver

    cfg = load_config(args.config)
    builder = Builder(args.config)
    installer = Installer(args.config)
    resolver = DependencyResolver(cfg["paths"]["db_path"], cfg["paths"]["ports_dir"])

    if args.all:
        pkgs = resolver.all_packages()
    else:
        pkgs = resolver.resolve_tree(args.package)

    print(f"Iniciando build completo de {len(pkgs)} pacotes...")
    for pkg in pkgs:
        print(f"\n==> Construindo {pkg}")
        try:
            builder.build(pkg, args=args, dry_run=args.dry_run)
            installer.install(pkg, {"root": "/", "dry_run": args.dry_run}, {})
        except Exception as e:
            print(f"[ERRO] Falha ao construir {pkg}: {e}")
            if not args.force:
                sys.exit(1)
    print("\n✅ Build completo finalizado com sucesso!")

# ============================================================
# CLI principal
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        prog="zeropkg",
        description="Zeropkg - Gerenciador Source-based e LFS Builder"
    )
    parser.add_argument("--config", default="/etc/zeropkg/config.toml", help="Caminho para o arquivo de configuração")

    subparsers = parser.add_subparsers(dest="command")

    # Comandos existentes
    sp = subparsers.add_parser("build", help="Constrói um pacote")
    sp.add_argument("target", help="Pacote ou caminho da receita TOML")
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--rebuild", action="store_true")
    sp.set_defaults(func=cmd_build)

    sp = subparsers.add_parser("install", help="Instala um pacote já construído")
    sp.add_argument("target", help="Pacote ou arquivo .tar.xz")
    sp.add_argument("--root", default="/")
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_install)

    sp = subparsers.add_parser("remove", help="Remove um pacote")
    sp.add_argument("target")
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(func=cmd_remove)

    sp = subparsers.add_parser("depclean", help="Remove dependências órfãs")
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(func=cmd_depclean)

    sp = subparsers.add_parser("sync", help="Sincroniza repositórios")
    sp.set_defaults(func=cmd_sync)

    sp = subparsers.add_parser("upgrade", help="Atualiza pacotes instalados")
    sp.add_argument("target", nargs="?", default=None)
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(func=cmd_upgrade)

    sp = subparsers.add_parser("update", help="Verifica novas versões upstream")
    sp.set_defaults(func=cmd_update)

    sp = subparsers.add_parser("revdep", help="Lista dependências reversas")
    sp.add_argument("package")
    sp.set_defaults(func=cmd_revdep)

    sp = subparsers.add_parser("info", help="Mostra informações sobre um pacote")
    sp.add_argument("package")
    sp.set_defaults(func=cmd_info)

    # =======================================================
    # Novos subcomandos adicionados
    # =======================================================

    sp = subparsers.add_parser("graph-deps", help="Mostra o grafo de dependências")
    sp.add_argument("package", nargs="?", help="Pacote alvo (opcional)")
    sp.add_argument("--all", action="store_true", help="Mostrar todas as dependências do sistema")
    sp.add_argument("--export", help="Exporta o grafo em formato DOT")
    sp.set_defaults(func=cmd_graph_deps)

    sp = subparsers.add_parser("full-build", help="Constrói e instala todos os pacotes com resolução de dependências")
    sp.add_argument("package", nargs="?", help="Pacote inicial (ou use --all)")
    sp.add_argument("--all", action="store_true", help="Construir todos os pacotes do repositório")
    sp.add_argument("--force", action="store_true", help="Ignorar erros de build e continuar")
    sp.add_argument("--dry-run", action="store_true", help="Simula sem executar os comandos")
    sp.set_defaults(func=cmd_full_build)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)

if __name__ == "__main__":
    main()
