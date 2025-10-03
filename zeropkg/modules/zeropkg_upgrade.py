#!/usr/bin/env python3
"""
zeropkg_upgrade.py
Módulo de upgrade para Zeropkg — versão revisada com DependencyResolver:
- Resolve dependências via grafo completo
- Remove versão antiga antes de instalar a nova
- Executa hooks de pre_remove/post_remove
- Mantém backup e rollback em caso de falha
- Suporta upgrade individual e global (ordem topológica)
"""

import os
import re
import glob
import shutil
import logging
from typing import Optional, List, Tuple, Dict, Set

from zeropkg_toml import load_toml
from zeropkg_builder import Builder, BuildError
from zeropkg_installer import Installer, InstallError
from zeropkg_deps import DependencyResolver, resolve_and_install
from zeropkg_db import DBManager
from zeropkg_logger import log_event

logger = logging.getLogger("zeropkg.upgrade")

# Constantes
PORTS_DIR_DEFAULT = "/usr/ports"
PKG_CACHE_DEFAULT = "/var/zeropkg/packages"
DB_PATH_DEFAULT = "/var/lib/zeropkg/installed.sqlite3"
BACKUP_DIR = "/var/zeropkg/backups"


# ----------------------------
# helpers
# ----------------------------
def _numeric_prefix(version: str) -> str:
    m = re.match(r"(\d+(?:\.\d+)*)", version)
    return m.group(1) if m else version

def compare_versions(v1: str, v2: str) -> int:
    a = _numeric_prefix(v1).split(".")
    b = _numeric_prefix(v2).split(".")
    ai = [int(x) if x.isdigit() else 0 for x in a]
    bi = [int(x) if x.isdigit() else 0 for x in b]
    for x, y in zip(ai, bi):
        if x > y: return 1
        if x < y: return -1
    if len(ai) > len(bi) and any(x > 0 for x in ai[len(bi):]):
        return 1
    if len(bi) > len(ai) and any(y > 0 for y in bi[len(ai):]):
        return -1
    return 0

def _all_metafiles_for(pkgname: str, ports_dir: str) -> List[Tuple[str, str]]:
    pattern = os.path.join(ports_dir, "**", f"{pkgname}-*.toml")
    files = glob.glob(pattern, recursive=True)
    res = []
    for f in files:
        fname = os.path.basename(f)
        if fname.startswith(pkgname + "-") and fname.endswith(".toml"):
            ver = fname[len(pkgname) + 1 : -len(".toml")]
            res.append((ver, f))
    return res

def find_latest_metafile(pkgname: str, ports_dir: str = PORTS_DIR_DEFAULT) -> Optional[str]:
    cand = _all_metafiles_for(pkgname, ports_dir)
    if not cand:
        return None
    best = None
    best_ver = None
    for ver, path in cand:
        if best is None or compare_versions(ver, best_ver) == 1:
            best, best_ver = path, ver
    return best


# ----------------------------
# upgrade de um único pacote
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
    Atualiza um pacote para a versão mais nova encontrada.
    Fluxo:
      1. Resolve dependências com DependencyResolver
      2. Remove versão antiga (se instalada)
      3. Constrói e instala nova versão
      4. Rollback em caso de falha
    """
    db = DBManager(db_path)
    installed = db.get_package(pkgname)
    current_version = installed["version"] if installed else None

    if verbose:
        log_event(pkgname, "upgrade", f"Versão instalada: {current_version}")

    latest_path = find_latest_metafile(pkgname, ports_dir)
    if not latest_path:
        log_event(pkgname, "upgrade", "Nenhum metafile encontrado", level="error")
        return False

    latest_meta = load_toml(ports_dir, pkgname)
    latest_version = latest_meta["package"]["version"]

    cmp = compare_versions(latest_version, current_version) if current_version else 1
    if cmp <= 0:
        log_event(pkgname, "upgrade", f"Já atualizado ({current_version} >= {latest_version})")
        return True

    log_event(pkgname, "upgrade", f"Upgrade: {current_version} → {latest_version}")

    if dry_run:
        resolver = DependencyResolver(db_path, ports_dir)
        missing = resolver.missing_deps(pkgname)
        log_event(pkgname, "upgrade", f"[dry-run] Dependências faltantes: {missing}")
        return True

    # Resolver dependências e instalá-las
    try:
        resolver = DependencyResolver(db_path, ports_dir)
        resolve_and_install(resolver, pkgname, Builder, Installer, args)
    except Exception as e:
        log_event(pkgname, "upgrade", f"Erro ao resolver dependências: {e}", level="error")
        return False

    # Backup opcional
    backup_pkg = None
    if backup and installed and installed.get("pkgfile"):
        old_pkgfile = installed.get("pkgfile")
        if os.path.exists(old_pkgfile):
            os.makedirs(BACKUP_DIR, exist_ok=True)
            backup_pkg = os.path.join(BACKUP_DIR, os.path.basename(old_pkgfile))
            shutil.copy2(old_pkgfile, backup_pkg)
            log_event(pkgname, "upgrade", f"Backup criado: {backup_pkg}")

    # Remover versão antiga antes de instalar
    if installed:
        try:
            inst = Installer(db_path=db_path)
            inst.remove(pkgname, current_version)
            log_event(pkgname, "upgrade", f"Versão antiga {current_version} removida com sucesso")
        except Exception as e:
            log_event(pkgname, "upgrade", f"Falha ao remover versão antiga: {e}", level="warning")

    # Construir e instalar nova versão
    try:
        builder = Builder(db_path=db_path, ports_dir=ports_dir)
        builder.build(pkgname, args, dir_install=root)
        log_event(pkgname, "upgrade", f"Build concluído de {pkgname}-{latest_version}")
        return True
    except (BuildError, InstallError) as e:
        log_event(pkgname, "upgrade", f"Erro durante upgrade: {e}", level="error")
        # rollback
        if backup_pkg and os.path.exists(backup_pkg):
            log_event(pkgname, "upgrade", "Tentando rollback...")
            try:
                inst = Installer(db_path=db_path)
                inst.install(pkgname, args, pkg_file=backup_pkg, dir_install=root)
                log_event(pkgname, "upgrade", "Rollback concluído")
            except Exception as re:
                log_event(pkgname, "upgrade", f"Rollback falhou: {re}", level="error")
        return False


# ----------------------------
# upgrade global com grafo
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
    Atualiza todos os pacotes instalados.
    Constrói grafo global e aplica upgrade em ordem topológica.
    """
    db = DBManager(db_path)
    installed = db.list_installed()

    graph: Dict[str, Set[str]] = {}
    to_upgrade: Dict[str, str] = {}

    for pkg in installed:
        name = pkg["name"]
        latest_path = find_latest_metafile(name, ports_dir)
        if not latest_path:
            continue
        latest_meta = load_toml(ports_dir, name)
        if compare_versions(latest_meta["package"]["version"], pkg["version"]) > 0:
            to_upgrade[name] = latest_meta["package"]["version"]
            resolver = DependencyResolver(db_path, ports_dir)
            deps = resolver._load_deps_from_toml(name)["runtime"]
            graph[name] = set(deps)

    # ordenação topológica
    order: List[str] = []
    visited: Set[str] = set()
    temp: Set[str] = set()

    def visit(n: str):
        if n in visited:
            return
        if n in temp:
            raise RuntimeError(f"Dependência cíclica detectada em {n}")
        temp.add(n)
        for d in graph.get(n, []):
            if d in to_upgrade:
                visit(d)
        temp.remove(n)
        visited.add(n)
        order.append(n)

    for pkg in to_upgrade:
        visit(pkg)

    results: List[Tuple[str, bool]] = []
    for pkg in order:
        try:
            ok = upgrade_package(pkg, db_path=db_path, ports_dir=ports_dir,
                                 pkg_cache=pkg_cache, dry_run=dry_run,
                                 root=root, verbose=verbose, args=args)
            results.append((pkg, ok))
        except Exception as e:
            log_event(pkg, "upgrade", f"Erro no upgrade_all: {e}", level="error")
            results.append((pkg, False))
    return results
