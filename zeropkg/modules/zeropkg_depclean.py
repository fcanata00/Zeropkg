"""
zeropkg_depclean.py

Implementação de revdep e depclean para Zeropkg, utilizando zeropkg_db.
"""

from typing import List, Dict, Any
from zeropkg_db import connect, get_package, list_installed
from zeropkg_logger import log_event

def revdep(db_path: str = "/var/lib/zeropkg/installed.sqlite3") -> Dict[str, List[str]]:
    """
    Retorna um dicionário {pacote: [dependências ausentes]} para pacotes que têm dependências quebradas.
    """
    broken: Dict[str, List[str]] = {}
    conn = connect(db_path)
    installed = list_installed(conn)

    # montar index de pacotes instalados por nome e versão
    installed_map = {}
    for p in installed:
        key = (p["name"], p["version"])
        installed_map[key] = p

    for pkg in installed:
        name = pkg["name"]
        version = pkg["version"]
        deps_json = pkg.get("dependencies_json") or pkg.get("dependencies") or []
        # se dependencies_json for JSON string, converter
        deps = []
        if isinstance(deps_json, str):
            import json
            try:
                deps = json.loads(deps_json)
            except Exception:
                deps = []
        elif isinstance(deps_json, list):
            deps = deps_json
        else:
            # dependências num formato inesperado
            deps = []

        missing = []
        for d in deps:
            dep_name = None
            dep_version = None
            if isinstance(d, dict):
                dep_name = d.get("name")
                dep_version = d.get("version")
            elif isinstance(d, str):
                # se estiver no formato "name:version"
                parts = d.split(":", 1)
                dep_name = parts[0]
                dep_version = parts[1] if len(parts) > 1 else None
            if not dep_name:
                continue
            # verificar se existe instalado
            found = False
            for inst in installed:
                if inst["name"] == dep_name:
                    if dep_version:
                        if inst["version"] == dep_version:
                            found = True
                            break
                    else:
                        found = True
                        break
            if not found:
                missing.append(d)

        if missing:
            broken[f"{name}:{version}"] = missing
            log_event(name, "depclean", f"revdep achou dependências ausentes para {name}:{version}: {missing}")

    return broken

def depclean(db_path: str = "/var/lib/zeropkg/installed.sqlite3") -> List[str]:
    """
    Retorna lista de pacotes órfãos: instalados mas não dependidos por nenhum outro e não explícitos.
    Supõe que o campo 'explicit' pode existir no JSON manifest ou como coluna (se for adotado).
    """
    orphans: List[str] = []
    conn = connect(db_path)
    installed = list_installed(conn)

    all_deps = set()
    for pkg in installed:
        deps_json = pkg.get("dependencies_json") or pkg.get("dependencies") or []
        if isinstance(deps_json, str):
            import json
            try:
                deps = json.loads(deps_json)
            except Exception:
                deps = []
        else:
            deps = deps_json if isinstance(deps_json, list) else []
        for d in deps:
            if isinstance(d, dict):
                name = d.get("name")
                version = d.get("version")
                if version:
                    all_deps.add(f"{name}:{version}")
                else:
                    # generic
                    all_deps.add(f"{name}")
            elif isinstance(d, str):
                all_deps.add(d)

    for pkg in installed:
        name = pkg["name"]
        version = pkg["version"]
        # construir referência
        ref = f"{name}:{version}"
        # verificar explicit flag se existir
        explicit = False
        # se houver chave explicit no registro, considerar
        if "explicit" in pkg and pkg["explicit"]:
            explicit = True
        # se não estiver em all_deps e não for explícito
        if ref not in all_deps and not explicit:
            orphans.append(ref)
            log_event(name, "depclean", f"depclean considerou órfão: {ref}")

    return orphans
