#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg CLI final — inclui build-world (-B)
- Usa zeropkg_deps para resolver dependências
- Sempre constrói em chroot (zeropkg_chroot)
- --parallel (-p) paraleliza apenas a instalação
- Suporta abreviações curtas (-b, -i, -r, -u, -B, etc.) e opções longas
"""

from __future__ import annotations
import argparse
import sys
import traceback
from pathlib import Path
from typing import List, Optional

# -------------------------
# Helpers com import lazy
# -------------------------
def _load_config():
    try:
        from zeropkg_config import load_config
        return load_config()
    except Exception as e:
        print("Aviso: zeropkg_config não disponível; usando defaults:", e)
        return {
            "paths": {
                "state_dir": "/var/lib/zeropkg",
                "build_dir": "/var/zeropkg/build",
                "db_path": "/var/lib/zeropkg/installed.sqlite3",
                "ports_dir": "/usr/ports",
            },
            "build": {"fakeroot": True, "use_chroot": True},
            "world": {"base": []}
        }

def _get_logger():
    try:
        from zeropkg_logger import get_logger
        return get_logger("cli")
    except Exception:
        import logging
        l = logging.getLogger("zeropkg_cli")
        if not l.handlers:
            l.addHandler(logging.StreamHandler(sys.stdout))
        return l

# -------------------------
# Operações básicas (wrappers)
# -------------------------
def _cmd_build(pkgs: List[str], dry_run: bool, cfg: dict):
    try:
        from zeropkg_builder import Builder
    except Exception as e:
        print("Módulo zeropkg_builder não disponível:", e)
        return 2
    builder = Builder(cfg)
    rc = 0
    for pkg in pkgs:
        print(f"[build] {pkg} (dry_run={dry_run})")
        res = builder.build(pkg, dry_run=dry_run)
        if res is None and not dry_run:
            print(f"[build] falha em {pkg}")
            rc = 3
    return rc

def _cmd_install(pkgs: List[str], dry_run: bool, parallel: bool, cfg: dict, root: Optional[str] = None):
    try:
        from zeropkg_installer import Installer
    except Exception as e:
        print("Módulo zeropkg_installer não disponível:", e)
        return 2
    installer = Installer(cfg)
    # install_from_cache path convention: /usr/ports/distfiles/<pkg>-<ver>.tar.xz
    def _install_one(pkg):
        pkgfile = Path(cfg["paths"].get("ports_dir", "/usr/ports")) / "distfiles" / f"{pkg}.tar.xz"
        print(f"[install] {pkg} from {pkgfile} (dry_run={dry_run})")
        try:
            ok = installer.install(pkg, pkgfile, dry_run=dry_run)
            return (pkg, ok)
        except Exception as e:
            return (pkg, False)

    results = {}
    if parallel and len(pkgs) > 1:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_install_one, p): p for p in pkgs}
            for fut in futs:
                try:
                    pkg, ok = fut.result()
                    results[pkg] = ok
                    if not ok:
                        print(f"[install] falha {pkg}")
                except Exception as e:
                    pkg = futs[fut]
                    results[pkg] = False
                    print(f"[install] exceção instalando {pkg}: {e}")
    else:
        for p in pkgs:
            pkg, ok = _install_one(p)
            results[pkg] = ok
    # exit code: 0 if all true
    return 0 if all(results.values()) else 4

def _cmd_remove(pkgs: List[str], dry_run: bool, cfg: dict):
    try:
        from zeropkg_remover import Remover
    except Exception as e:
        print("Módulo zeropkg_remover não disponível:", e)
        return 2
    remover = Remover(cfg)
    ok_all = True
    for pkg in pkgs:
        rep = remover.remove(pkg, dry_run=dry_run)
        if not rep.get("ok", False):
            ok_all = False
            print(f"[remove] falha em {pkg}: {rep.get('errors')}")
    return 0 if ok_all else 5

def _cmd_upgrade(pkgs: Optional[List[str]], dry_run: bool, force: bool, cfg: dict):
    try:
        from zeropkg_upgrade import UpgradeManager
    except Exception as e:
        print("Módulo zeropkg_upgrade não disponível:", e)
        return 2
    um = UpgradeManager(cfg)
    if pkgs:
        ok_all = True
        for p in pkgs:
            r = um.upgrade_package(p, dry_run=dry_run, force=force)
            if not r.get("ok"):
                ok_all = False
                print(f"[upgrade] falha {p}: {r.get('error')}")
        return 0 if ok_all else 6
    else:
        res = um.upgrade_all(dry_run=dry_run, force=force)
        return 0 if res.get("ok") else 6

def _cmd_update(dry_run: bool, cfg: dict):
    try:
        from zeropkg_update import UpdateManager
        um = UpdateManager(cfg)
        um.check_for_updates(dry_run=dry_run)
        return 0
    except Exception as e:
        print("Módulo zeropkg_update não disponível:", e)
        return 2

def _cmd_sync(cfg: dict):
    try:
        from zeropkg_sync import sync_repos
        sync_repos(cfg)
        return 0
    except Exception as e:
        print("Módulo zeropkg_sync não disponível:", e)
        return 2

def _cmd_depclean(dry_run: bool, parallel: bool, cfg: dict):
    try:
        from zeropkg_depclean import Depcleaner
        dc = Depcleaner(cfg)
        rep = dc.depclean(dry_run=dry_run, parallel=parallel)
        print("Depclean report:", rep)
        return 0
    except Exception as e:
        print("Módulo zeropkg_depclean não disponível:", e)
        return 2

def _cmd_revdep(pkgs: List[str], cfg: dict):
    try:
        from zeropkg_deps import DependencyResolver
        dr = DependencyResolver(cfg)
        for p in pkgs:
            rev = dr.reverse_dependencies(p)
            print(p, "revdeps:", rev)
        return 0
    except Exception as e:
        print("Módulo zeropkg_deps não disponível:", e)
        return 2

def _cmd_info(pkg: str, cfg: dict):
    try:
        from zeropkg_db import DBManager
        db = DBManager(cfg["paths"]["db_path"])
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

def _cmd_graph(output: Optional[str], cfg: dict):
    try:
        from zeropkg_deps import ensure_graph_loaded, export_graphviz
        graph = ensure_graph_loaded(cfg)
        out = output or "/var/lib/zeropkg/dependency-graph.dot"
        export_graphviz(graph, out)
        print("Grafo escrito em", out)
        return 0
    except Exception as e:
        print("Geração de grafo falhou:", e)
        return 2

# -------------------------
# build-world: implementa B (usar lista [world.base] em config)
# sempre construir em chroot; parallel só para instalação
# -------------------------
def _cmd_build_world(dry_run: bool, parallel_install: bool, cfg: dict, root: Optional[str]):
    # carrega lista base do config
    world_list = []
    # config may contain under cfg["world"]["base"] or cfg["paths"]["world_base"]
    if cfg.get("world") and isinstance(cfg["world"].get("base"), list):
        world_list = cfg["world"]["base"]
    elif cfg.get("paths") and isinstance(cfg["paths"].get("world_base"), list):
        world_list = cfg["paths"]["world_base"]
    else:
        print("Nenhuma lista 'world.base' encontrada no config. Defina em /etc/zeropkg/config.toml -> [world] base = [...]")
        return 7

    if not world_list:
        print("Lista world.base vazia; nada a construir.")
        return 0

    # resolve dependências usando zeropkg_deps
    try:
        from zeropkg_deps import DependencyResolver
    except Exception as e:
        print("Módulo zeropkg_deps não disponível:", e)
        return 2

    try:
        dr = DependencyResolver(cfg)
        # resolve_install_order deve retornar algo como {"order": [...]} ou list diretamente
        try:
            res = dr.resolve_install_order(world_list)
            if isinstance(res, dict) and "order" in res:
                build_order = res["order"]
            elif isinstance(res, list):
                build_order = res
            else:
                # fallback: use resolver to expand each and dedupe
                build_order = dr.resolve_dependencies(world_list) if hasattr(dr, "resolve_dependencies") else world_list
        except Exception:
            # fallback alternative
            build_order = dr.resolve_dependencies(world_list) if hasattr(dr, "resolve_dependencies") else world_list

    except Exception as e:
        print("Erro inicializando DependencyResolver:", e)
        return 2

    if not build_order:
        print("Nenhum pacote para construir após resolução de dependências.")
        return 0

    print(f"[build-world] ordem de build: {build_order} (dry_run={dry_run})")
    # preparar chroot functions
    try:
        from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot
    except Exception as e:
        print("Módulo zeropkg_chroot não disponível; abortando build-world:", e)
        return 2

    # builders/installer lazy
    try:
        from zeropkg_builder import Builder
    except Exception as e:
        print("Módulo zeropkg_builder não disponível:", e)
        return 2
    try:
        from zeropkg_installer import Installer
    except Exception as e:
        print("Módulo zeropkg_installer não disponível:", e)
        return 2

    builder = Builder(cfg)
    installer = Installer(cfg)

    # Build each package in order — builder already uses chroot (per suas configurações)
    for pkg in build_order:
        print(f"[build-world] construindo {pkg} ...")
        # builder.build will use chroot if configured in its config
        res = builder.build(pkg, dry_run=dry_run)
        if res is None and not dry_run:
            print(f"[build-world] build falhou para {pkg}; abortando.")
            return 8

    # Após construir todos, instalar — parallel_install controla somente instalação
    print("[build-world] construção concluída, iniciando instalação (parallel={})".format(parallel_install))
    # Prepare list of package "artifacts" — we assume installer.install reads from /usr/ports/distfiles/<pkg>.tar.xz
    pkgfiles = [(pkg, Path(cfg["paths"].get("ports_dir", "/usr/ports")) / "distfiles" / f"{pkg}.tar.xz") for pkg in build_order]

    def _install_pkg_tuple(t):
        pkg, pkgfile = t
        print(f"[build-world] instalando {pkg} from {pkgfile}")
        try:
            return (pkg, installer.install(pkg, pkgfile, dry_run=dry_run))
        except Exception as e:
            print(f"[build-world] exceção instalando {pkg}: {e}")
            return (pkg, False)

    install_results = {}
    if parallel_install and len(pkgfiles) > 1:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_install_pkg_tuple, t): t for t in pkgfiles}
            for fut in futs:
                try:
                    pkg, ok = fut.result()
                    install_results[pkg] = ok
                    if not ok:
                        print(f"[build-world] falha instalando {pkg}")
                except Exception as e:
                    t = futs[fut]
                    install_results[t[0]] = False
                    print(f"[build-world] exceção na instalação paralela {t[0]}: {e}")
    else:
        for t in pkgfiles:
            pkg, ok = _install_pkg_tuple(t)
            install_results[pkg] = ok

    failed = [p for p, ok in install_results.items() if not ok]
    if failed:
        print(f"[build-world] Algumas instalações falharam: {failed}")
        return 9

    print("[build-world] Concluído com sucesso.")
    return 0

# -------------------------
# CLI: parser com abreviações e subcomandos
# -------------------------
def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(prog="zeropkg", description="Zeropkg - source-based package manager")

    # opções globais
    parser.add_argument("-n", "--dry-run", action="store_true", help="Simula a operação (abreviação -n)")
    parser.add_argument("-F", "--force", action="store_true", help="Forçar operação (abreviação -F)")
    parser.add_argument("-p", "--parallel", action="store_true", help="Executar em paralelo quando aplicável (-p)")

    # ações via flags curtas
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
    parser.add_argument("-B", "--build-world", action="store_true", help="Build-world: resolve world.base e constrói/instala (-B)")

    # subcomandos (compatibilidade)
    sub = parser.add_subparsers(dest="command")

    p_build = sub.add_parser("build", help="Construir pacote")
    p_build.add_argument("pkg", nargs="+")
    p_build.add_argument("--dry-run", action="store_true", dest="dry_run")

    p_install = sub.add_parser("install", help="Instalar pacote(s)")
    p_install.add_argument("pkgs", nargs="+")
    p_install.add_argument("--dry-run", action="store_true")
    p_install.add_argument("--parallel", action="store_true")

    p_remove = sub.add_parser("remove", help="Remover pacote(s)")
    p_remove.add_argument("pkgs", nargs="+")
    p_remove.add_argument("--dry-run", action="store_true")

    p_upgrade = sub.add_parser("upgrade", help="Atualizar pacotes")
    p_upgrade.add_argument("pkgs", nargs="*", help="(opcional) lista de pacotes; vazio = todos")
    p_upgrade.add_argument("--dry-run", action="store_true")
    p_upgrade.add_argument("--force", action="store_true")

    p_update = sub.add_parser("update", help="Checar upstream")
    p_update.add_argument("--dry-run", action="store_true")

    sub.add_parser("sync", help="Sincronizar repositórios")

    p_depclean = sub.add_parser("depclean", help="Remover órfãos")
    p_depclean.add_argument("--dry-run", action="store_true")
    p_depclean.add_argument("--parallel", action="store_true")

    p_revdep = sub.add_parser("revdep", help="Mostrar revdeps")
    p_revdep.add_argument("pkgs", nargs="+", help="Pacote(s)")

    p_info = sub.add_parser("info", help="Mostrar info do pacote")
    p_info.add_argument("pkg", help="Pacote")

    p_graph = sub.add_parser("graph-deps", help="Exportar grafo")
    p_graph.add_argument("--output", "-o", help="Arquivo de saída (.dot)")

    p_fb = sub.add_parser("full-build", help="Full build")
    p_fb.add_argument("target", nargs="?", default="world")
    p_fb.add_argument("--dry-run", action="store_true")

    p_bw = sub.add_parser("build-world", help="Construir e instalar world.base")
    p_bw.add_argument("--dry-run", action="store_true")
    p_bw.add_argument("--parallel-install", action="store_true", help="Paralelizar apenas a instalação")

    args = parser.parse_args(argv)

    cfg = _load_config()
    logger = _get_logger()

    # Prioridade: subcomando > flags curtas
    try:
        # Subcomandos primeiro
        if args.command:
            if args.command == "build":
                return _cmd_build(args.pkg, dry_run=getattr(args, "dry_run", False), cfg=cfg)
            if args.command == "install":
                return _cmd_install(args.pkgs, dry_run=getattr(args, "dry_run", False),
                                    parallel=getattr(args, "parallel", False), cfg=cfg)
            if args.command == "remove":
                return _cmd_remove(args.pkgs, dry_run=getattr(args, "dry_run", False), cfg=cfg)
            if args.command == "upgrade":
                return _cmd_upgrade(getattr(args, "pkgs", None), dry_run=getattr(args, "dry_run", False),
                                    force=getattr(args, "force", False), cfg=cfg)
            if args.command == "update":
                return _cmd_update(dry_run=getattr(args, "dry_run", False), cfg=cfg)
            if args.command == "sync":
                return _cmd_sync(cfg=cfg)
            if args.command == "depclean":
                return _cmd_depclean(dry_run=getattr(args, "dry_run", False),
                                     parallel=getattr(args, "parallel", False), cfg=cfg)
            if args.command == "revdep":
                return _cmd_revdep(args.pkgs, cfg=cfg)
            if args.command == "info":
                return _cmd_info(args.pkg, cfg=cfg)
            if args.command == "graph-deps":
                return _cmd_graph(getattr(args, "output", None), cfg=cfg)
            if args.command == "full-build":
                return _cmd_build_world(dry_run=getattr(args, "dry_run", False),
                                        parallel_install=False, cfg=cfg, root=None)

        # Flags curtas / longas (sem subcomando)
        if args.build:
            return _cmd_build(args.build, dry_run=args.dry_run, cfg=cfg)
        if args.install:
            return _cmd_install(args.install, dry_run=args.dry_run, parallel=args.parallel, cfg=cfg)
        if args.remove:
            return _cmd_remove(args.remove, dry_run=args.dry_run, cfg=cfg)
        if args.upgrade is not None:
            pkgs = args.upgrade if len(args.upgrade) > 0 else None
            return _cmd_upgrade(pkgs, dry_run=args.dry_run, force=args.force, cfg=cfg)
        if args.update:
            return _cmd_update(dry_run=args.dry_run, cfg=cfg)
        if args.sync:
            return _cmd_sync(cfg=cfg)
        if args.depclean:
            return _cmd_depclean(dry_run=args.dry_run, parallel=args.parallel, cfg=cfg)
        if args.revdep:
            return _cmd_revdep(args.revdep, cfg=cfg)
        if args.info:
            return _cmd_info(args.info, cfg=cfg)
        if args.graph_deps is not None:
            return _cmd_graph(args.graph_deps, cfg=cfg)
        if args.full_build is not None:
            target = args.full_build or "world"
            return _cmd_build_world(dry_run=args.dry_run, parallel_install=False, cfg=cfg, root=None)

        # Novo: build-world flag curta -B
        if args.build_world:
            return _cmd_build_world(dry_run=args.dry_run, parallel_install=getattr(args, "parallel", False), cfg=cfg, root=None)

        # Nada especificado
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
