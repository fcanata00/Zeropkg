#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_cli.py — CLI com abreviações e opções longas
Suporta tanto subcomandos quanto flags curtas (ex.: -i / --install).
"""

import argparse
import sys
import traceback
from pathlib import Path

# Import lento (lazy) dentro das funções para evitar ImportError na importação do módulo
# Mapear ações para funções internas que fazem import das implementações reais.

def _run_builder(pkgs, dry_run=False, full_build=False, cfg=None):
    try:
        from zeropkg_builder import Builder
    except Exception as e:
        print("Módulo zeropkg_builder não disponível:", e)
        return 2
    b = Builder(cfg)
    for pkg in pkgs:
        b.build(pkg, dry_run=dry_run)
    return 0

def _run_install(pkgs, dry_run=False, parallel=False, cfg=None):
    try:
        from zeropkg_installer import Installer
    except Exception as e:
        print("Módulo zeropkg_installer não disponível:", e)
        return 2
    inst = Installer(cfg)
    if parallel and len(pkgs) > 1:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(inst.install, pkg, Path(f"/usr/ports/distfiles/{pkg}.tar.xz"), None, None, dry_run): pkg for pkg in pkgs}
            for fut in futs:
                try:
                    fut.result()
                except Exception as e:
                    print("Erro na instalação paralela:", futs[fut], e)
    else:
        for pkg in pkgs:
            inst.install(pkg, Path(f"/usr/ports/distfiles/{pkg}.tar.xz"), None, None, dry_run)
    return 0

def _run_remove(pkgs, dry_run=False, cfg=None):
    try:
        from zeropkg_remover import Remover
    except Exception as e:
        print("Módulo zeropkg_remover não disponível:", e)
        return 2
    rm = Remover(cfg)
    for pkg in pkgs:
        rm.remove(pkg, dry_run=dry_run)
    return 0

def _run_upgrade(pkgs=None, dry_run=False, force=False, cfg=None):
    try:
        from zeropkg_upgrade import UpgradeManager
    except Exception as e:
        print("Módulo zeropkg_upgrade não disponível:", e)
        return 2
    um = UpgradeManager(cfg)
    if pkgs:
        res = {}
        for p in pkgs:
            res[p] = um.upgrade_package(p, dry_run=dry_run, force=force)
        print(res)
    else:
        print(um.upgrade_all(dry_run=dry_run, force=force))
    return 0

def _run_update(dry_run=False, cfg=None):
    # zeropkg_update earlier used check_updates; try both APIs
    try:
        from zeropkg_update import check_updates
        res = check_updates(report=True, cfg=cfg, dry_run=dry_run)
        print("Update summary:", res.get("summary") if isinstance(res, dict) else res)
        return 0
    except Exception:
        try:
            from zeropkg_update import UpdateManager
            um = UpdateManager(cfg)
            um.check_for_updates()
            return 0
        except Exception as e:
            print("Módulo zeropkg_update não disponível:", e)
            return 2

def _run_sync(cfg=None):
    try:
        from zeropkg_sync import sync_repos
        sync_repos(cfg)
        return 0
    except Exception as e:
        print("Módulo zeropkg_sync não disponível:", e)
        return 2

def _run_depclean(dry_run=False, parallel=False, cfg=None):
    try:
        from zeropkg_depclean import Depcleaner
        dc = Depcleaner(cfg)
        return dc.depclean(dry_run=dry_run, parallel=parallel)
    except Exception as e:
        print("Módulo zeropkg_depclean não disponível:", e)
        return 2

def _run_revdep(pkgs, cfg=None):
    try:
        from zeropkg_deps import DependencyResolver
        dr = DependencyResolver(cfg)
        for p in pkgs:
            print(p, "revdeps:", dr.reverse_dependencies(p))
        return 0
    except Exception as e:
        print("Módulo zeropkg_deps não disponível:", e)
        return 2

def _run_info(pkg, cfg=None):
    try:
        from zeropkg_db import DBManager
        db = DBManager(cfg["paths"]["db_path"]) if cfg else DBManager()
        with db:
            info = db.get_package_info(pkg)
            if not info:
                print("Nenhuma informação encontrada para", pkg)
            else:
                for k, v in info.items():
                    print(f"{k}: {v}")
        return 0
    except Exception as e:
        print("Módulo zeropkg_db não disponível:", e)
        return 2

def _run_graph(output, cfg=None):
    try:
        from zeropkg_deps import ensure_graph_loaded, export_to_dot
        graph = ensure_graph_loaded(cfg)
        out = output or "/var/lib/zeropkg/deps.dot"
        export_to_dot(graph, out)
        print("Grafo exportado para", out)
        return 0
    except Exception as e:
        print("Geração de grafo falhou:", e)
        return 2

def _run_full_build(target, dry_run=False, cfg=None):
    try:
        from zeropkg_deps import DependencyResolver
        from zeropkg_builder import Builder
        from zeropkg_installer import Installer
        deps = DependencyResolver(cfg)
        order = deps.resolve_install_order([target])["order"]
        if not order:
            order = [target]
        builder = Builder(cfg)
        installer = Installer(cfg)
        for pkg in order:
            builder.build(pkg, dry_run=dry_run)
            installer.install(pkg, Path(f"/usr/ports/distfiles/{pkg}.tar.xz"), dry_run=dry_run)
        return 0
    except Exception as e:
        print("Full-build falhou:", e)
        return 2

# -----------------------
# Parser principal: aceita tanto subcomandos quanto flags curtas
# -----------------------
def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg - source-based package manager")

    # opções globais curtas/longas (abrev)
    parser.add_argument("-n", "--dry-run", action="store_true", help="Simula a operação (abreviação -n)")
    parser.add_argument("-F", "--force", action="store_true", help="Forçar operação (abreviação -F)")
    parser.add_argument("-p", "--parallel", action="store_true", help="Executar em paralelo quando aplicável (-p)")

    # flags curtas que disparam ações (aceitam argumentos quando necessário)
    parser.add_argument("-b", "--build", nargs="+", metavar="PKG", help="Build pacote(s) (abreviação -b)")
    parser.add_argument("-i", "--install", nargs="+", metavar="PKG", help="Instala pacote(s) (abreviação -i)")
    parser.add_argument("-r", "--remove", nargs="+", metavar="PKG", help="Remove pacote(s) (abreviação -r)")
    parser.add_argument("-u", "--upgrade", nargs="*", metavar="PKG", help="Upgrade pacote(s) ou todos (abreviação -u). Sem args -> todos")
    parser.add_argument("-U", "--update", action="store_true", help="Checa novas versões upstream (abreviação -U)")
    parser.add_argument("-s", "--sync", action="store_true", help="Sincroniza repositórios (abreviação -s)")
    parser.add_argument("-c", "--depclean", action="store_true", help="Remove órfãos (abreviação -c)")
    parser.add_argument("-R", "--revdep", nargs="+", metavar="PKG", help="Mostra revdeps (abreviação -R)")
    parser.add_argument("-I", "--info", metavar="PKG", help="Mostra info do pacote (abreviação -I)")
    parser.add_argument("-g", "--graph-deps", nargs="?", const="/var/lib/zeropkg/deps.dot", metavar="OUT", help="Exporta grafo (abreviação -g)")
    parser.add_argument("-f", "--full-build", nargs="?", const="world", metavar="TARGET", help="Full build (abreviação -f)")

    # Subcomandos (compatibilidade com uso estilo 'zeropkg install ...')
    sub = parser.add_subparsers(dest="command", help="Subcomando (opcional)")

    # build
    p_build = sub.add_parser("build", help="Construir pacote")
    p_build.add_argument("pkg", nargs="+")
    p_build.add_argument("--dry-run", action="store_true", dest="dry_run")

    # install
    p_install = sub.add_parser("install", help="Instalar pacote(s)")
    p_install.add_argument("pkgs", nargs="+")
    p_install.add_argument("--dry-run", action="store_true")
    p_install.add_argument("--parallel", action="store_true")

    # remove
    p_remove = sub.add_parser("remove", help="Remover pacote(s)")
    p_remove.add_argument("pkgs", nargs="+")
    p_remove.add_argument("--dry-run", action="store_true")

    # upgrade
    p_upgrade = sub.add_parser("upgrade", help="Atualizar pacotes")
    p_upgrade.add_argument("pkgs", nargs="*", help="(opcional) lista de pacotes; vazio = todos")
    p_upgrade.add_argument("--dry-run", action="store_true")
    p_upgrade.add_argument("--force", action="store_true")

    # update
    p_update = sub.add_parser("update", help="Checar upstream")
    p_update.add_argument("--dry-run", action="store_true")

    # sync
    sub.add_parser("sync", help="Sincronizar repositórios")

    # depclean
    p_depclean = sub.add_parser("depclean", help="Remover órfãos")
    p_depclean.add_argument("--dry-run", action="store_true")
    p_depclean.add_argument("--parallel", action="store_true")

    # revdep
    p_revdep = sub.add_parser("revdep", help="Mostrar revdeps")
    p_revdep.add_argument("pkgs", nargs="+", help="Pacote(s)")

    # info
    p_info = sub.add_parser("info", help="Mostrar info do pacote")
    p_info.add_argument("pkg", help="Pacote")

    # graph-deps
    p_g = sub.add_parser("graph-deps", help="Exportar grafo")
    p_g.add_argument("--output", "-o", help="Arquivo de saída (.dot)")

    # full-build
    p_fb = sub.add_parser("full-build", help="Full build")
    p_fb.add_argument("target", nargs="?", default="world")
    p_fb.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)

    # PRIORIDADE: subcomando > flags curtas
    try:
        cfg = None
        # Se subcomando foi usado, delega aos handlers subcomando
        if args.command:
            cmd = args.command
            if cmd == "build":
                return _run_builder(args.pkg, dry_run=getattr(args, "dry_run", False), full_build=False, cfg=cfg)
            if cmd == "install":
                return _run_install(args.pkgs, dry_run=getattr(args, "dry_run", False), parallel=getattr(args, "parallel", False), cfg=cfg)
            if cmd == "remove":
                return _run_remove(args.pkgs, dry_run=getattr(args, "dry_run", False), cfg=cfg)
            if cmd == "upgrade":
                return _run_upgrade(args.pkgs, dry_run=getattr(args, "dry_run", False), force=getattr(args, "force", False), cfg=cfg)
            if cmd == "update":
                return _run_update(dry_run=getattr(args, "dry_run", False), cfg=cfg)
            if cmd == "sync":
                return _run_sync(cfg=cfg)
            if cmd == "depclean":
                return _run_depclean(dry_run=getattr(args, "dry_run", False), parallel=getattr(args, "parallel", False), cfg=cfg)
            if cmd == "revdep":
                return _run_revdep(args.pkgs, cfg=cfg)
            if cmd == "info":
                return _run_info(args.pkg, cfg=cfg)
            if cmd == "graph-deps":
                return _run_graph(getattr(args, "output", None), cfg=cfg)
            if cmd == "full-build":
                return _run_full_build(getattr(args, "target", "world"), dry_run=getattr(args, "dry_run", False), cfg=cfg)

        # Caso sem subcomando, verificar flags curtas/longas
        # build
        if args.build:
            return _run_builder(args.build, dry_run=args.dry_run, full_build=False, cfg=cfg)

        # install
        if args.install:
            return _run_install(args.install, dry_run=args.dry_run, parallel=args.parallel, cfg=cfg)

        # remove
        if args.remove:
            return _run_remove(args.remove, dry_run=args.dry_run, cfg=cfg)

        # upgrade
        if args.upgrade is not None:
            pkgs = args.upgrade if len(args.upgrade) > 0 else None
            return _run_upgrade(pkgs, dry_run=args.dry_run, force=args.force, cfg=cfg)

        # update
        if args.update:
            return _run_update(dry_run=args.dry_run, cfg=cfg)

        # sync
        if args.sync:
            return _run_sync(cfg=cfg)

        # depclean
        if args.depclean:
            return _run_depclean(dry_run=args.dry_run, parallel=args.parallel, cfg=cfg)

        # revdep
        if args.revdep:
            return _run_revdep(args.revdep, cfg=cfg)

        # info
        if args.info:
            return _run_info(args.info, cfg=cfg)

        # graph-deps
        if args.graph_deps is not None:
            return _run_graph(args.graph_deps, cfg=cfg)

        # full-build
        if args.full_build is not None:
            target = args.full_build or "world"
            return _run_full_build(target, dry_run=args.dry_run, cfg=cfg)

        # se nada foi pedido, printa ajuda
        parser.print_help()
        return 0

    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
        return 130
    except Exception as e:
        print("Erro ao executar comando:", e)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
