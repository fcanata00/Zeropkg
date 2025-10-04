#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zeropkg CLI — interface principal para gerenciamento e construção do sistema
Padrão B: Integrado, enxuto e funcional.
"""

import sys
import argparse
from zeropkg_config import load_config
from zeropkg_logger import log_event, get_logger

logger = get_logger("zeropkg-cli")


# ============================================================
# Execução de comandos principais
# ============================================================

def cmd_build(cfg, args):
    from zeropkg_builder import Builder
    builder = Builder(args.config)
    log_event("CLI", f"Iniciando build de {args.target}")
    builder.build(args.target, args=args, dry_run=args.dry_run, rebuild=args.rebuild)
    log_event("CLI", f"Build finalizado para {args.target}")

def cmd_install(cfg, args):
    from zeropkg_installer import Installer
    installer = Installer(args.config)
    meta = {}
    log_event("CLI", f"Instalando {args.target}")
    installer.install(args.target, {"root": args.root, "dry_run": args.dry_run}, meta)

def cmd_remove(cfg, args):
    from zeropkg_remover import Remover
    remover = Remover(args.config)
    log_event("CLI", f"Removendo {args.target}")
    remover.remove(args.target, force=args.force)

def cmd_depclean(cfg, args):
    from zeropkg_depclean import DepCleaner
    cleaner = DepCleaner(args.config)
    log_event("CLI", "Executando depclean")
    cleaner.clean(dry_run=args.dry_run, force=args.force)

def cmd_sync(cfg, args):
    from zeropkg_sync import sync_repos
    log_event("CLI", "Sincronizando repositórios")
    sync_repos()

def cmd_upgrade(cfg, args):
    from zeropkg_upgrade import UpgradeManager
    upgr = UpgradeManager(args.config)
    log_event("CLI", f"Atualizando {args.target or 'todos os pacotes'}")
    upgr.upgrade(args.target, dry_run=args.dry_run, force=args.force)

def cmd_update(cfg, args):
    from zeropkg_update import Updater
    updater = Updater(args.config)
    log_event("CLI", "Verificando novas versões upstream")
    updater.check_updates(report=True)

def cmd_revdep(cfg, args):
    from zeropkg_deps import DependencyResolver
    resolver = DependencyResolver(cfg["paths"]["db_path"], cfg["paths"]["ports_dir"])
    print(f"Dependências reversas de {args.package}:")
    for pkg in resolver.reverse_deps(args.package):
        print("  -", pkg)

def cmd_info(cfg, args):
    import toml, os
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
# Novos comandos integrados
# ============================================================

def cmd_graph_deps(cfg, args):
    from zeropkg_deps import DependencyResolver
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

def cmd_full_build(cfg, args):
    from zeropkg_builder import Builder
    from zeropkg_installer import Installer
    from zeropkg_deps import DependencyResolver

    builder = Builder(args.config)
    installer = Installer(args.config)
    resolver = DependencyResolver(cfg["paths"]["db_path"], cfg["paths"]["ports_dir"])

    pkgs = resolver.all_packages() if args.all else resolver.resolve_tree(args.package)
    print(f"Iniciando build completo de {len(pkgs)} pacotes...")
    for pkg in pkgs:
        try:
            log_event("CLI", f"Build iniciado para {pkg}")
            builder.build(pkg, args=args, dry_run=args.dry_run)
            installer.install(pkg, {"root": "/", "dry_run": args.dry_run}, {})
            log_event("CLI", f"Build e instalação finalizados para {pkg}")
        except Exception as e:
            log_event("CLI", f"Erro ao construir {pkg}: {e}")
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
    parser.add_argument("--config", default="/etc/zeropkg/config.toml",
                        help="Caminho para o arquivo de configuração")
    subparsers = parser.add_subparsers(dest="command")

    # --- Comandos principais ---
    def add_cmd(name, help_text, func, args_def):
        sp = subparsers.add_parser(name, help=help_text)
        args_def(sp)
        sp.set_defaults(func=func)

    add_cmd("build", "Constrói um pacote", cmd_build,
            lambda sp: [sp.add_argument("target"), sp.add_argument("--dry-run", action="store_true"),
                        sp.add_argument("--rebuild", action="store_true")])

    add_cmd("install", "Instala um pacote", cmd_install,
            lambda sp: [sp.add_argument("target"), sp.add_argument("--root", default="/"),
                        sp.add_argument("--dry-run", action="store_true")])

    add_cmd("remove", "Remove um pacote", cmd_remove,
            lambda sp: [sp.add_argument("target"), sp.add_argument("--force", action="store_true")])

    add_cmd("depclean", "Remove dependências órfãs", cmd_depclean,
            lambda sp: [sp.add_argument("--dry-run", action="store_true"),
                        sp.add_argument("--force", action="store_true")])

    add_cmd("sync", "Sincroniza repositórios", cmd_sync, lambda sp: None)

    add_cmd("upgrade", "Atualiza pacotes instalados", cmd_upgrade,
            lambda sp: [sp.add_argument("target", nargs="?"),
                        sp.add_argument("--dry-run", action="store_true"),
                        sp.add_argument("--force", action="store_true")])

    add_cmd("update", "Verifica novas versões upstream", cmd_update, lambda sp: None)

    add_cmd("revdep", "Lista dependências reversas", cmd_revdep,
            lambda sp: [sp.add_argument("package")])

    add_cmd("info", "Mostra informações sobre um pacote", cmd_info,
            lambda sp: [sp.add_argument("package")])

    # --- Novos comandos ---
    add_cmd("graph-deps", "Mostra o grafo de dependências", cmd_graph_deps,
            lambda sp: [sp.add_argument("package", nargs="?"),
                        sp.add_argument("--all", action="store_true"),
                        sp.add_argument("--export")])

    add_cmd("full-build", "Constrói e instala todos os pacotes com resolução de dependências", cmd_full_build,
            lambda sp: [sp.add_argument("package", nargs="?"),
                        sp.add_argument("--all", action="store_true"),
                        sp.add_argument("--force", action="store_true"),
                        sp.add_argument("--dry-run", action="store_true")])

    # --- Execução ---
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    cfg = load_config(args.config)
    log_event("CLI", f"Executando comando {args.command}")
    args.func(cfg, args)
    sys.exit(0)


if __name__ == "__main__":
    main()
