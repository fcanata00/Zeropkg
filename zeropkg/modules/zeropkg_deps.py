import logging
from typing import List, Dict, Set
from zeropkg_db import is_installed

logger = logging.getLogger("zeropkg.deps")

class DependencyError(Exception):
    pass

def resolve_dependencies(meta, db_path="/var/lib/zeropkg/installed.json") -> List[str]:
    """
    Resolve as dependências de um pacote com base no banco.
    Retorna lista de pacotes que precisam ser instalados na ordem correta.
    """
    deps = getattr(meta, "dependencies", [])
    resolved: List[str] = []
    visited: Set[str] = set()

    def visit(dep):
        if dep in visited:
            return
        visited.add(dep)
        name, _, version = dep.partition(":")
        version = version or None

        if is_installed(db_path, name, version):
            logger.info(f"Dependência já instalada: {dep}")
            return

        # TODO: Aqui poderia carregar o metafile do dep e resolver recursivamente
        resolved.append(dep)

    for dep in deps:
        visit(dep)

    return resolved

def check_missing(meta, db_path="/var/lib/zeropkg/installed.json") -> List[str]:
    """Retorna lista de dependências que ainda não estão instaladas"""
    missing = []
    for dep in getattr(meta, "dependencies", []):
        name, _, version = dep.partition(":")
        version = version or None
        if not is_installed(db_path, name, version):
            missing.append(dep)
    return missing
