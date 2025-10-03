"""
zeropkg_deps.py

Gerenciamento avançado de dependências no Zeropkg.
Suporta:
- Tipos de dependência (build/runtime/optional)
- Resolução recursiva com detecção de ciclos
- Comparação de versões básica (>=, <=, ==)
- Explicação do grafo de dependências
"""

import os
import logging
from typing import List, Dict, Set, Optional, Tuple, Any
from zeropkg_db import connect, get_package
from zeropkg_toml import parse_toml, PackageMeta

logger = logging.getLogger("zeropkg.deps")

class DependencyError(Exception):
    pass

# --- helpers de versão ---
def _parse_version_constraint(dep: str) -> Tuple[str, str, Optional[str]]:
    """
    Retorna (nome, operador, versão)
    Ex: "glibc>=2.38" -> ("glibc", ">=", "2.38")
    """
    for op in [">=", "<=", "==", ">", "<", "~"]:
        if op in dep:
            name, ver = dep.split(op, 1)
            return name.strip(), op, ver.strip()
    if ":" in dep:
        name, ver = dep.split(":", 1)
        return name.strip(), "==", ver.strip()
    return dep.strip(), "any", None

def _version_satisfies(installed: str, op: str, required: Optional[str]) -> bool:
    if not required or op == "any":
        return True
    try:
        def to_tuple(v): return tuple(map(int, v.split(".")))
        iv, rv = to_tuple(installed), to_tuple(required)
    except Exception:
        return True  # fallback: não falhar se versão for estranha
    if op == "==": return iv == rv
    if op == ">=": return iv >= rv
    if op == "<=": return iv <= rv
    if op == ">":  return iv > rv
    if op == "<":  return iv < rv
    if op == "~":  return iv[0] == rv[0]  # mesma major
    return True

# --- resolução ---
def resolve_dependencies(meta: PackageMeta,
                         ports_dir="/usr/ports",
                         db_path="/var/lib/zeropkg/installed.sqlite3",
                         include_optional=False) -> List[str]:
    """
    Resolve dependências de um pacote (recursivamente).
    Retorna lista ordenada de pacotes a instalar.
    """

    deps = getattr(meta, "dependencies", [])
    resolved: List[str] = []
    visiting: Set[str] = set()
    visited: Set[str] = set()

    def visit(dep: Any):
        if isinstance(dep, dict):
            name = dep.get("name")
            dtype = dep.get("type", "runtime")
            constraint = dep.get("version")
        else:
            name, op, constraint = _parse_version_constraint(str(dep))
            dtype, dep = "runtime", dep

        if dtype == "optional" and not include_optional:
            logger.info(f"Ignorando dependência opcional: {name}")
            return

        key = f"{name}:{constraint or ''}"
        if key in visited:
            return
        if key in visiting:
            raise DependencyError(f"Ciclo detectado em dependência: {key}")
        visiting.add(key)

        # 1. verificar se já instalado
        conn = connect(db_path)
        pkg = get_package(conn, name)
        conn.close()
        if pkg:
            if _version_satisfies(pkg["version"], op if 'op' in locals() else "any", constraint):
                logger.info(f"Dependência já instalada: {name} {pkg['version']}")
                visiting.remove(key)
                visited.add(key)
                return
            else:
                raise DependencyError(f"Versão instalada de {name} não satisfaz {op}{constraint}")

        # 2. carregar receita do repo
        metafile = os.path.join(ports_dir, name, f"{name}.toml")
        if not os.path.isfile(metafile):
            raise DependencyError(f"Metafile não encontrado para dependência: {dep}")
        dep_meta = parse_toml(metafile)

        # 3. resolver dependências dela primeiro
        for subdep in getattr(dep_meta, "dependencies", []):
            visit(subdep)

        # 4. adicionar dependência atual
        resolved.append(name)
        visiting.remove(key)
        visited.add(key)

    for dep in deps:
        visit(dep)

    return resolved

def explain_dependencies(meta: PackageMeta,
                         ports_dir="/usr/ports",
                         db_path="/var/lib/zeropkg/installed.sqlite3",
                         depth=0,
                         include_optional=False):
    """Imprime árvore de dependências com indentação."""
    deps = getattr(meta, "dependencies", [])
    prefix = "  " * depth
    for dep in deps:
        if isinstance(dep, dict):
            name = dep.get("name")
            dtype = dep.get("type", "runtime")
            constraint = dep.get("version")
            dep_str = f"{name} ({dtype}{' '+constraint if constraint else ''})"
        else:
            dep_str = str(dep)
        print(f"{prefix}└─ {dep_str}")

        name = dep["name"] if isinstance(dep, dict) else str(dep).split(":")[0]
        metafile = os.path.join(ports_dir, name, f"{name}.toml")
        if os.path.isfile(metafile):
            dep_meta = parse_toml(metafile)
            explain_dependencies(dep_meta, ports_dir, db_path, depth+1, include_optional)
