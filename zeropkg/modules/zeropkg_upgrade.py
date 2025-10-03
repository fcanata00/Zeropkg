"""
zeropkg_upgrade.py

Módulo de upgrade para Zeropkg — versão revisada e integrada ao Builder/Installer atuais.
"""

import os
import re
import glob
import shutil
import logging
from typing import Optional, List, Tuple

from zeropkg_toml import parse_toml, PackageMeta
from zeropkg_builder import Builder, BuildError
from zeropkg_installer import Installer, InstallError
from zeropkg_deps import check_missing
from zeropkg_db import connect, get_package, record_event
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
# upgrade principal
# ----------------------------
def upgrade_package(
    pkgname: str,
    db_path: str = DB_PATH_DEFAULT,
    ports_dir: str = PORTS_DIR_DEFAULT,
    pkg_cache: str = PKG_CACHE_DEFAULT,
    dry_run: bool = False,
    root: Optional[str] = None,
    backup: bool = True,
    verbose: bool = False
) -> bool:
    """
    Atualiza um pacote para a versão mais nova encontrada.
    """
    conn = connect(db_path)
    installed = get_package(conn, pkgname)
    current_version = installed["version"] if installed else None

    if verbose:
        log_event(pkgname, "upgrade", f"Versão instalada: {current_version}")

    latest_path = find_latest_metafile(pkgname, ports_dir)
    if not latest_path:
        log_event(pkgname, "upgrade", "Nenhum metafile encontrado", level="error")
        return False

    latest_meta = parse_toml(latest_path)
    latest_version = latest_meta.version

    cmp = compare_versions(latest_version, current_version) if current_version else 1
    if cmp <= 0:
        log_event(pkgname, "upgrade", f"Já atualizado ({current_version} >= {latest_version})")
        return True

    log_event(pkgname, "upgrade", f"Upgrade: {current_version} → {latest_version}")

    if dry_run:
        missing = check_missing(latest_meta, db_path=db_path)
        log_event(pkgname, "upgrade", f"[dry-run] Dependências faltantes: {missing}")
        log_event(pkgname, "upgrade", "[dry-run] Plano: build + install")
        return True

    # Dependências faltantes
    missing = check_missing(latest_meta, db_path=db_path)
    if missing:
        log_event(pkgname, "upgrade", f"Dependências faltantes: {missing}", level="warning")

    # Backup opcional
    backup_pkg = None
    if backup and installed and installed.get("pkgfile"):
        old_pkgfile = installed.get("pkgfile")
        if os.path.exists(old_pkgfile):
            os.makedirs(BACKUP_DIR, exist_ok=True)
            backup_pkg = os.path.join(BACKUP_DIR, os.path.basename(old_pkgfile))
            shutil.copy2(old_pkgfile, backup_pkg)
            log_event(pkgname, "upgrade", f"Backup criado: {backup_pkg}")

    # Construir e instalar nova versão
    try:
        builder = Builder(latest_meta, cache_dir=pkg_cache, pkg_cache=pkg_cache,
                          dry_run=dry_run, db_path=db_path)
        result = builder.build(dir_install=root)
        log_event(pkgname, "upgrade", f"Build concluído → {result['pkgfile']}")
        return True
    except (BuildError, InstallError) as e:
        log_event(pkgname, "upgrade", f"Erro durante upgrade: {e}", level="error")
        # rollback
        if backup_pkg and os.path.exists(backup_pkg):
            log_event(pkgname, "upgrade", "Tentando rollback...")
            try:
                inst = Installer(db_path=db_path, dry_run=False, root=root)
                inst.install(backup_pkg, latest_meta)
                log_event(pkgname, "upgrade", "Rollback concluído")
            except Exception as re:
                log_event(pkgname, "upgrade", f"Rollback falhou: {re}", level="error")
        return False


def upgrade_all(
    db_path: str = DB_PATH_DEFAULT,
    ports_dir: str = PORTS_DIR_DEFAULT,
    pkg_cache: str = PKG_CACHE_DEFAULT,
    dry_run: bool = False,
    root: Optional[str] = None,
    verbose: bool = False
) -> List[Tuple[str, bool]]:
    """
    Atualiza todos os pacotes instalados.
    """
    conn = connect(db_path)
    installed = conn.execute("SELECT name, version FROM packages").fetchall()
    results: List[Tuple[str, bool]] = []
    for row in installed:
        name = row["name"]
        try:
            ok = upgrade_package(name, db_path=db_path, ports_dir=ports_dir,
                                 pkg_cache=pkg_cache, dry_run=dry_run,
                                 root=root, verbose=verbose)
            results.append((name, ok))
        except Exception as e:
            log_event(name, "upgrade", f"Erro no upgrade_all: {e}", level="error")
            results.append((name, False))
    return results
