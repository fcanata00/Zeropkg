#!/usr/bin/env python3
"""
zeropkg_upgrade.py — versão definitiva e integrada

Funcionalidades:
- upgrade_package(pkgname, ...) : atualiza um pacote (resolve deps, faz backup, remove antigo, build+install novo, hooks, rollback)
- upgrade_all(...) : encontra pacotes que têm versão nova nas ports e atualiza em ordem topológica
- Compatível com as assinaturas dos seus módulos (Builder, Installer, DependencyResolver, DBManager, load_toml)
- Robustez extra para carregar metafiles e executar hooks dentro/fora de chroot (dependendo da receita)
"""

from __future__ import annotations

import os
import re
import glob
import shutil
import logging
from typing import Optional, List, Tuple, Dict, Set

# Import dos módulos do Zeropkg (devem existir no seu projeto)
from zeropkg_toml import load_toml  # função tenta ler .toml (seremos tolerantes à assinatura)
from zeropkg_builder import Builder, BuildError
from zeropkg_installer import Installer, InstallError
from zeropkg_deps import DependencyResolver, resolve_and_install
from zeropkg_db import DBManager
from zeropkg_logger import log_event

logger = logging.getLogger("zeropkg.upgrade")

# Defaults — podem ser sobrescritos por args passados pelo CLI
PORTS_DIR_DEFAULT = "/usr/ports"
PKG_CACHE_DEFAULT = "/var/zeropkg/packages"
DB_PATH_DEFAULT = "/var/lib/zeropkg/installed.sqlite3"
BACKUP_DIR = "/var/zeropkg/backups"


# ----------------------------
# utilitários de versão / busca metafile
# ----------------------------
def _numeric_prefix(version: Optional[str]) -> str:
    if not version:
        return "0"
    m = re.match(r"(\d+(?:\.\d+)*)", version)
    return m.group(1) if m else version


def compare_versions(v1: Optional[str], v2: Optional[str]) -> int:
    """
    Compara versões simples (apenas prefixo numérico).
    Retorna 1 se v1 > v2, 0 se iguais, -1 se v1 < v2.
    """
    if v1 is None and v2 is None:
        return 0
    if v1 is None:
        return -1
    if v2 is None:
        return 1
    a = _numeric_prefix(v1).split(".")
    b = _numeric_prefix(v2).split(".")
    ai = [int(x) if x.isdigit() else 0 for x in a]
    bi = [int(x) if x.isdigit() else 0 for x in b]
    for x, y in zip(ai, bi):
        if x > y:
            return 1
        if x < y:
            return -1
    if len(ai) > len(bi) and any(x > 0 for x in ai[len(bi):]):
        return 1
    if len(bi) > len(ai) and any(y > 0 for y in bi[len(ai):]):
        return -1
    return 0


def _all_metafiles_for(pkgname: str, ports_dir: str) -> List[Tuple[str, str]]:
    """
    Retorna lista de (versão, caminho) para arquivos pkgname-*.toml sob ports_dir.
    """
    pattern = os.path.join(ports_dir, "**", f"{pkgname}-*.toml")
    files = glob.glob(pattern, recursive=True)
    res: List[Tuple[str, str]] = []
    for f in files:
        fname = os.path.basename(f)
        if fname.startswith(pkgname + "-") and fname.endswith(".toml"):
            ver = fname[len(pkgname) + 1 : -len(".toml")]
            res.append((ver, f))
    return res


def find_latest_metafile(pkgname: str, ports_dir: str = PORTS_DIR_DEFAULT) -> Optional[str]:
    """
    Encontra o metafile com a maior versão para pkgname.
    """
    candidates = _all_metafiles_for(pkgname, ports_dir)
    if not candidates:
        return None
    best = None
    best_ver = None
    for ver, path in candidates:
        if best is None or compare_versions(ver, best_ver) == 1:
            best, best_ver = path, ver
    return best


def _load_meta_flexible(metafile_path: Optional[str], ports_dir: str, pkgname: str) -> dict:
    """
    Tenta carregar o TOML aceitando diferentes assinaturas de load_toml:
      - se metafile_path fornecido, tenta load_toml(metafile_path)
      - senão tenta load_toml(ports_dir, pkgname) ou load_toml(os.path.join(ports_dir,...))
    """
    if metafile_path:
        try:
            return load_toml(metafile_path)
        except TypeError:
            # talvez load_toml tenha assinatura (ports_dir, pkgname)
            try:
                return load_toml(ports_dir, pkgname)
            except Exception:
                raise
    else:
        # tenta localizar com find_latest_metafile
        path = find_latest_metafile(pkgname, ports_dir)
        if not path:
            raise FileNotFoundError(f"No metafile for {pkgname} in {ports_dir}")
        try:
            return load_toml(path)
        except TypeError:
            return load_toml(ports_dir, pkgname)


# ----------------------------
# Hooks helpers
# ----------------------------
def _run_hooks(meta: dict, hook_key: str, args) -> None:
    """
    Executa hooks listados em meta['hooks'][hook_key].
    Respeita meta['options'].get('chroot', False) quando executar (assume que Installer/Builder preparem chroot).
    Observe: hooks são strings de comando de shell ou listas de strings.
    """
    if not meta:
        return
    hooks = meta.get("hooks", {}) or {}
    entry = hooks.get(hook_key)
    if not entry:
        return
    if isinstance(entry, str):
        commands = [entry]
    else:
        commands = list(entry)

    # Se a receita pede chroot, as ferramentas externas (installer/builder) já preparam chroot; aqui
    # nós apenas executamos os hook commands no contexto "host" (assume-se que hook scripts se adaptam),
    # mas também podemos suportar execução via chroot se args.root for passado e options.chroot True.
    use_chroot = meta.get("options", {}).get("chroot", False)
    root = getattr(args, "root", "/") if args else "/"
    for cmd in commands:
        try:
            log_event(meta["package"]["name"], "hook", f"Running {hook_key}: {cmd}")
            if use_chroot and root and root != "/":
                # executar o hook dentro do chroot usando a forma simples de chroot+sh -c
                full = f"chroot {root} /usr/bin/env -i /bin/bash -lc \"{cmd}\""
                if getattr(args, "dry_run", False):
                    log_event(meta["package"]["name"], "hook", f"[dry-run] {full}")
                else:
                    rc = os.system(full)
                    if rc != 0:
                        log_event(meta["package"]["name"], "hook", f"Hook {hook_key} failed: {cmd}", level="warning")
            else:
                if getattr(args, "dry_run", False):
                    log_event(meta["package"]["name"], "hook", f"[dry-run] {cmd}")
                else:
                    rc = os.system(cmd)
                    if rc != 0:
                        log_event(meta["package"]["name"], "hook", f"Hook {hook_key} failed: {cmd}", level="warning")
        except Exception as e:
            log_event(meta["package"]["name"], "hook", f"Exception running hook {hook_key}: {e}", level="error")


# ----------------------------
# upgrade_package
# ----------------------------
def upgrade_package(
    pkgname: str,
    db_path: str = DB_PATH_DEFAULT,
    ports_dir: str = PORTS_DIR_DEFAULT,
    pkg_cache: str = PKG_CACHE_DEFAULT,
    dry_run: bool = False,
    root: Optional[str] = None,
    backup: bool = True,
    verbose: bool = False,
    args=None
) -> bool:
    """
    Atualiza um pacote:
      - encontra metafile mais recente
      - resolve dependências transitivas (DependencyResolver + resolve_and_install)
      - executa hooks pre_upgrade (se houver)
      - remove versão antiga (Installer.remove) — respeita revdeps / --force
      - build+install nova versão (Builder.build -> Installer.install via builder)
      - em falha, tenta rollback para backup (Installer.install from backup pkg)
      - executa hooks post_upgrade
    """
    log_event(pkgname, "upgrade", f"Starting upgrade for {pkgname}")

    db = DBManager(db_path)
    installed = db.get_package(pkgname)
    current_version = installed["version"] if installed else None

    latest_metapath = find_latest_metafile(pkgname, ports_dir)
    if not latest_metapath:
        log_event(pkgname, "upgrade", "No metafile found for package", level="error")
        return False

    # carregar meta (tolerante)
    try:
        latest_meta = _load_meta_flexible(latest_metapath, ports_dir, pkgname)
    except Exception as e:
        log_event(pkgname, "upgrade", f"Failed to load metafile: {e}", level="error")
        return False

    latest_version = latest_meta["package"]["version"]

    if verbose:
        log_event(pkgname, "upgrade", f"Installed: {current_version}, Latest: {latest_version}")

    if compare_versions(latest_version, current_version) <= 0:
        log_event(pkgname, "upgrade", f"No upgrade necessary ({current_version} >= {latest_version})")
        return True

    # dry-run: report missing dependencies and exit success
    if dry_run:
        try:
            resolver = DependencyResolver(db_path, ports_dir)
            missing = resolver.missing_deps(pkgname)
            log_event(pkgname, "upgrade", f"[dry-run] Missing dependencies: {missing}")
            return True
        except Exception as e:
            log_event(pkgname, "upgrade", f"[dry-run] resolver error: {e}", level="error")
            return False

    # 1) resolve and install dependencies (transitively) BEFORE upgrade
    try:
        resolver = DependencyResolver(db_path, ports_dir)
        resolve_and_install(resolver, pkgname, Builder, Installer, args)
    except Exception as e:
        log_event(pkgname, "upgrade", f"Dependency resolution/install failed: {e}", level="error")
        return False

    # 2) prepare backup of existing binary package (if any)
    backup_pkg_path = None
    if backup and installed and installed.get("pkgfile"):
        old_pkgfile = installed.get("pkgfile")
        if old_pkgfile and os.path.exists(old_pkgfile):
            os.makedirs(BACKUP_DIR, exist_ok=True)
            backup_pkg_path = os.path.join(BACKUP_DIR, os.path.basename(old_pkgfile))
            try:
                shutil.copy2(old_pkgfile, backup_pkg_path)
                log_event(pkgname, "upgrade", f"Backup of old package created: {backup_pkg_path}")
            except Exception as e:
                log_event(pkgname, "upgrade", f"Failed to backup old package: {e}", level="warning")
                backup_pkg_path = None

    # 3) run pre-upgrade hooks (if any)
    try:
        _run_hooks(latest_meta, "pre_upgrade", args)
    except Exception as e:
        log_event(pkgname, "upgrade", f"pre_upgrade hooks error: {e}", level="warning")

    # 4) remove old package (if installed)
    if installed:
        try:
            installer = Installer(db_path=db_path, ports_dir=ports_dir, root=root or "/", dry_run=dry_run,
                                  use_fakeroot=getattr(args, "fakeroot", True) if args else True)
            force_flag = getattr(args, "force", False) if args else False
            removed = installer.remove(pkgname, version=installed.get("version"), hooks=None, force=force_flag)
            if not removed:
                log_event(pkgname, "upgrade", "Removal of old package aborted (revdeps?) — upgrade stopped", level="error")
                return False
            log_event(pkgname, "upgrade", f"Old package {pkgname}-{installed.get('version')} removed")
        except Exception as e:
            log_event(pkgname, "upgrade", f"Error removing old package: {e}", level="error")
            # try rollback with backup if present
            if backup_pkg_path:
                try:
                    installer.install(pkgname, args, pkg_file=backup_pkg_path, meta=None, dir_install=root)
                    log_event(pkgname, "upgrade", "Rollback: restored backup after failed remove")
                except Exception as re:
                    log_event(pkgname, "upgrade", f"Rollback failed: {re}", level="error")
            return False

    # 5) build & install new version
    try:
        builder = Builder(db_path=db_path, ports_dir=ports_dir,
                          build_root=getattr(args, "build_root", "/var/zeropkg/build") if args else "/var/zeropkg/build",
                          cache_dir=getattr(args, "cache_dir", "/usr/ports/distfiles") if args else "/usr/ports/distfiles",
                          packages_dir=pkg_cache)
        builder.build(pkgname, args, dir_install=root)
        log_event(pkgname, "upgrade", f"Built and installed new version {latest_version}")
    except Exception as e:
        log_event(pkgname, "upgrade", f"Build/install failed: {e}", level="error")
        # attempt rollback with backup package
        if backup_pkg_path:
            try:
                installer = Installer(db_path=db_path, ports_dir=ports_dir, root=root or "/", dry_run=dry_run,
                                      use_fakeroot=getattr(args, "fakeroot", True) if args else True)
                installer.install(pkgname, args, pkg_file=backup_pkg_path, meta=None, dir_install=root)
                log_event(pkgname, "upgrade", "Rollback: restored backup after build failure")
            except Exception as re:
                log_event(pkgname, "upgrade", f"Rollback failed: {re}", level="error")
        return False

    # 6) run post-upgrade hooks
    try:
        _run_hooks(latest_meta, "post_upgrade", args)
    except Exception as e:
        log_event(pkgname, "upgrade", f"post_upgrade hooks error: {e}", level="warning")

    log_event(pkgname, "upgrade", f"Upgrade finished successfully: {pkgname} -> {latest_version}")
    return True


# ----------------------------
# upgrade_all com topological order
# ----------------------------
def upgrade_all(
    db_path: str = DB_PATH_DEFAULT,
    ports_dir: str = PORTS_DIR_DEFAULT,
    pkg_cache: str = PKG_CACHE_DEFAULT,
    dry_run: bool = False,
    root: Optional[str] = None,
    verbose: bool = False,
    args=None
) -> List[Tuple[str, bool]]:
    """
    Atualiza todos pacotes instalados que têm versão mais recente no ports_dir.
    Calcula uma ordem segura (topological) entre os pacotes que precisam de upgrade,
    considerando apenas dependências entre os pacotes que serão atualizados.
    """
    db = DBManager(db_path)
    installed = db.list_installed()  # espera-se lista de dicts com keys: name, version, pkgfile, etc.

    # descobrir quais pacotes têm atualização disponível
    to_upgrade: Dict[str, str] = {}
    metas_cache: Dict[str, dict] = {}
    for pkg in installed:
        name = pkg["name"]
        meta_path = find_latest_metafile(name, ports_dir)
        if not meta_path:
            continue
        try:
            meta = _load_meta_flexible(meta_path, ports_dir, name)
        except Exception:
            continue
        latest_version = meta["package"]["version"]
        if compare_versions(latest_version, pkg["version"]) > 0:
            to_upgrade[name] = latest_version
            metas_cache[name] = meta

    if not to_upgrade:
        log_event("upgrade_all", "info", "No packages to upgrade")
        return []

    # construir grafo entre pacotes que serão atualizados: edge A->B se A depende de B and B in to_upgrade
    graph: Dict[str, Set[str]] = {}
    resolver = DependencyResolver(db_path, ports_dir)
    for name in to_upgrade:
        deps = resolver._load_deps_from_toml(name).get("runtime", [])  # runtime deps relevantes
        graph[name] = set(d for d in deps if d in to_upgrade)

    # topological sort (detecta ciclos)
    order: List[str] = []
    visited: Set[str] = set()
    temp: Set[str] = set()

    def visit(n: str):
        if n in visited:
            return
        if n in temp:
            raise RuntimeError(f"Cyclic dependency detected among upgrades at {n}")
        temp.add(n)
        for d in graph.get(n, []):
            visit(d)
        temp.remove(n)
        visited.add(n)
        order.append(n)

    for pkg in list(to_upgrade.keys()):
        visit(pkg)

    # order agora tem dependências primeiro (ordenado)
    results: List[Tuple[str, bool]] = []
    for pkg in order:
        try:
            ok = upgrade_package(pkg, db_path=db_path, ports_dir=ports_dir,
                                 pkg_cache=pkg_cache, dry_run=dry_run, root=root,
                                 backup=True, verbose=verbose, args=args)
            results.append((pkg, ok))
        except Exception as e:
            log_event(pkg, "upgrade_all", f"Error upgrading {pkg}: {e}", level="error")
            results.append((pkg, False))
    return results
