"""
zeropkg_upgrade.py

Módulo para atualizar (upgrade) pacotes:
- encontra a última versão disponível nos ports
- compara com o que está instalado no DB
- resolve dependências faltantes
- compila (Builder) e instala (Installer)
- suporte a dry-run, backup simples e rollback

Integra com:
- zeropkg_toml.parse_toml
- zeropkg_downloader (indiretamente via Builder)
- zeropkg_builder.Builder
- zeropkg_installer.Installer
- zeropkg_db.connect/get_package
- zeropkg_deps.check_missing
"""

from __future__ import annotations
import os
import re
import glob
import shutil
import logging
from typing import Optional, List, Tuple

from zeropkg_toml import parse_toml, PackageMeta
from zeropkg_builder import Builder
from zeropkg_installer import Installer
from zeropkg_deps import check_missing
from zeropkg_db import connect, get_package

logger = logging.getLogger("zeropkg.upgrade")
logging.basicConfig(level=logging.INFO)

PORTS_DIR_DEFAULT = "/usr/ports"
PKG_CACHE_DEFAULT = "/var/zeropkg/packages"
DB_PATH_DEFAULT = "/var/lib/zeropkg/installed.sqlite3"  # adaptado ao schema que você usa

# -----------------------
# utilitários de versão
# -----------------------
def _numeric_prefix(version: str) -> str:
    """
    Extrai o prefixo numérico da versão, ex: "13.2.0-pass1" -> "13.2.0"
    """
    m = re.match(r"(\d+(?:\.\d+)*)", version)
    return m.group(1) if m else version

def compare_versions(v1: str, v2: str) -> int:
    """
    Compara versões simples com ponto (sem suporte completo semver):
    - retorna 1 se v1 > v2
    - 0 se igual (considerando prefixo numérico)
    - -1 se v1 < v2
    """
    a = _numeric_prefix(v1).split(".")
    b = _numeric_prefix(v2).split(".")
    # converter para inteiros quando possível
    def to_ints(lst):
        res = []
        for p in lst:
            try:
                res.append(int(p))
            except Exception:
                res.append(0)
        return res
    ai = to_ints(a)
    bi = to_ints(b)
    # comparar elemento a elemento
    for x, y in zip(ai, bi):
        if x > y:
            return 1
        if x < y:
            return -1
    # se um tem componentes extras
    if len(ai) > len(bi):
        if any(x > 0 for x in ai[len(bi):]):
            return 1
    elif len(bi) > len(ai):
        if any(x > 0 for x in bi[len(ai):]):
            return -1
    return 0

# -----------------------
# localizar metafiles
# -----------------------
def _all_metafiles_for(pkgname: str, ports_dir: str) -> List[Tuple[str, str]]:
    """
    Retorna lista de (version_string, path) encontrados em ports
    procura por arquivos que comecem com 'pkgname-' e terminem em '.toml'
    """
    pattern = os.path.join(ports_dir, "**", f"{pkgname}-*.toml")
    files = glob.glob(pattern, recursive=True)
    res = []
    for f in files:
        fname = os.path.basename(f)
        # fname = pkgname-VERSION[-variant].toml
        if not fname.startswith(pkgname + "-") or not fname.endswith(".toml"):
            continue
        ver = fname[len(pkgname) + 1 : -len(".toml")]
        res.append((ver, f))
    return res

def find_latest_metafile(pkgname: str, ports_dir: str = PORTS_DIR_DEFAULT) -> Optional[str]:
    """
    Retorna o caminho para o metafile da versão mais recente, ou None se não encontrado.
    """
    cand = _all_metafiles_for(pkgname, ports_dir)
    if not cand:
        return None
    # escolher a maior versão numérica (ignorando sufixos variantes)
    best = None
    best_ver = None
    for ver, path in cand:
        if best is None:
            best, best_ver = path, ver
            continue
        cmp = compare_versions(ver, best_ver)
        if cmp == 1:
            best, best_ver = path, ver
    return best

# -----------------------
# upgrade de um pacote
# -----------------------
def upgrade_package(
    pkgname: str,
    db_path: str = DB_PATH_DEFAULT,
    ports_dir: str = PORTS_DIR_DEFAULT,
    pkg_cache: str = PKG_CACHE_DEFAULT,
    dry_run: bool = False,
    dir_install: Optional[str] = None,
    backup: bool = True,
    verbose: bool = False
) -> bool:
    """
    Faz upgrade de 'pkgname' para a versão mais recente encontrada em ports.
    Retorna True se atualizou com sucesso (ou se estava atualizado), False em caso de erro.
    Em dry_run True apenas mostra o plano.
    """
    conn = None
    try:
        conn = connect(db_path)
    except Exception as e:
        logger.warning(f"Não foi possível conectar ao DB ({db_path}): {e}. Continuando sem DB.")
    installed = None
    if conn:
        try:
            installed = get_package(conn, pkgname)
        except Exception:
            installed = None

    current_version = installed["version"] if installed else None
    if verbose:
        logger.info(f"Versão instalada de {pkgname}: {current_version}")

    latest_path = find_latest_metafile(pkgname, ports_dir=ports_dir)
    if not latest_path:
        logger.error(f"Nenhum metafile encontrado para {pkgname} em {ports_dir}")
        return False

    latest_meta = parse_toml(latest_path)
    latest_version = latest_meta.version

    cmp = 0
    if current_version is not None:
        cmp = compare_versions(latest_version, current_version)

    if cmp <= 0:
        logger.info(f"{pkgname} já está atualizado (instalado: {current_version}, disponível: {latest_version})")
        return True

    # plano de atualização
    logger.info(f"Upgrade disponível: {pkgname} {current_version or '(não instalado)'} -> {latest_version}")
    if dry_run:
        # mostrar dependências faltantes e passos
        missing = check_missing(latest_meta, db_path=db_path)
        logger.info(f"[dry-run] Dependências faltantes: {missing}")
        logger.info("[dry-run] Plano: resolver deps -> build -> package -> install")
        return True

    # 1) resolver dependências faltantes
    missing = check_missing(latest_meta, db_path=db_path)
    if missing:
        logger.info(f"Dependências faltantes para {pkgname}: {missing}")
        for dep in missing:
            dep_name = dep.split(":", 1)[0]
            # encontrar metafile do dep
            dep_meta_path = find_latest_metafile(dep_name, ports_dir=ports_dir)
            if not dep_meta_path:
                logger.error(f"Dependência {dep} não encontrada em {ports_dir}. Abortando upgrade.")
                return False
            dep_meta = parse_toml(dep_meta_path)
            # compilar e instalar dependência
            logger.info(f"Compilando dependência {dep_name} -> {dep_meta.version}")
            b = Builder(dep_meta, cache_dir=pkg_cache, pkg_cache=pkg_cache, dry_run=dry_run)
            b.fetch_sources()
            b.extract_sources()
            b.build()
            pkgfile = b.package()
            inst = Installer(db_path, dry_run=dry_run, dir_install=dir_install)
            inst.install(pkgfile, dep_meta)

    # 2) backup do pacote atual (se houver) - tenta usar pkgfile do DB se presente
    backup_pkg = None
    if backup and installed and installed.get("pkgfile"):
        try:
            srcpkg = installed.get("pkgfile")
            if os.path.exists(srcpkg):
                bakdir = os.path.join("/var/zeropkg/backups")
                os.makedirs(bakdir, exist_ok=True)
                backup_pkg = os.path.join(bakdir, os.path.basename(srcpkg))
                shutil.copy2(srcpkg, backup_pkg)
                logger.info(f"Backup do pacote anterior criado em {backup_pkg}")
        except Exception as e:
            logger.warning(f"Falha ao criar backup: {e}")

    # 3) build da nova versão
    try:
        builder = Builder(latest_meta, cache_dir=pkg_cache, pkg_cache=pkg_cache, dry_run=dry_run, dir_install=dir_install)
        builder.fetch_sources()
        builder.extract_sources()
        builder.build()
        new_pkgfile = builder.package()
    except Exception as e:
        logger.error(f"Falha ao compilar nova versão: {e}")
        return False

    # 4) instalar nova versão
    try:
        installer = Installer(db_path, dry_run=dry_run, dir_install=dir_install)
        installer.install(new_pkgfile, latest_meta)
        logger.info(f"{pkgname} atualizado para {latest_version} com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Falha ao instalar nova versão: {e}")
        # tentar rollback a partir do backup se possível
        if backup_pkg and os.path.exists(backup_pkg):
            logger.info("Tentando rollback a partir do backup...")
            try:
                rb_inst = Installer(db_path, dry_run=False, dir_install=dir_install)
                rb_inst.install(backup_pkg, latest_meta)  # obs: meta do backup pode não estar disponível; tentativa best-effort
                logger.info("Rollback realizado (best-effort).")
            except Exception as re:
                logger.error(f"Rollback falhou: {re}")
        return False

# -----------------------
# upgrade all
# -----------------------
def upgrade_all(db_path: str = DB_PATH_DEFAULT,
                ports_dir: str = PORTS_DIR_DEFAULT,
                pkg_cache: str = PKG_CACHE_DEFAULT,
                dry_run: bool = False,
                dir_install: Optional[str] = None,
                verbose: bool = False) -> List[Tuple[str, bool]]:
    """
    Itera por todos os pacotes instalados e tenta atualizá-los.
    Retorna lista de tuples (pkgname, success_bool)
    """
    results = []
    try:
        conn = connect(db_path)
    except Exception as e:
        logger.error(f"Não foi possível conectar ao DB: {e}")
        return results

    cur = conn.cursor()
    cur.execute("SELECT name, version FROM packages")
    rows = cur.fetchall()
    for r in rows:
        name = r["name"]
        try:
            ok = upgrade_package(name, db_path=db_path, ports_dir=ports_dir, pkg_cache=pkg_cache, dry_run=dry_run, dir_install=dir_install, verbose=verbose)
            results.append((name, ok))
        except Exception as e:
            logger.error(f"Erro ao atualizar {name}: {e}")
            results.append((name, False))
    return results
