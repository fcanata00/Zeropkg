"""
zeropkg_depclean.py

Revdep e Depclean avançado para Zeropkg
- Detecta dependências quebradas
- Lista pacotes órfãos
- Suporta flag 'explicit'
- Integra com zeropkg_db e logger
- Pode remover em cascata (auto depclean)
"""

import json
from typing import List, Dict, Any, Set
from zeropkg_db import connect, list_installed, record_event
from zeropkg_logger import log_event

class DepcleanError(Exception):
    pass


def revdep(db_path: str = "/var/lib/zeropkg/installed.sqlite3") -> Dict[str, List[str]]:
    """
    Retorna pacotes com dependências quebradas no formato:
    { "pkg:versão": [deps ausentes] }
    """
    broken: Dict[str, List[str]] = {}
    conn = connect(db_path)
    installed = list_installed(conn)

    # index de pacotes instalados
    installed_names = {p["name"]: p["version"] for p in installed}

    for pkg in installed:
        deps_json = pkg.get("dependencies_json") or []
        deps = []
        if isinstance(deps_json, str):
            try:
                deps = json.loads(deps_json)
            except Exception:
                deps = []
        elif isinstance(deps_json, list):
            deps = deps_json

        missing = []
        for d in deps:
            if isinstance(d, dict):
                dname = d.get("name")
                dver = d.get("version")
            else:
                parts = str(d).split(":", 1)
                dname = parts[0]
                dver = parts[1] if len(parts) > 1 else None

            if dname not in installed_names:
                missing.append(d)
            elif dver and installed_names[dname] != dver:
                missing.append(d)

        if missing:
            key = f"{pkg['name']}:{pkg['version']}"
            broken[key] = missing
            log_event(pkg["name"], "revdep",
                      f"Dependências ausentes: {missing}", level="warning")
            record_event(conn, "warning", "revdep",
                         f"{pkg['name']} tem dependências quebradas", {"missing": missing})

    conn.close()
    return broken


def depclean(db_path: str = "/var/lib/zeropkg/installed.sqlite3",
             auto: bool = False) -> List[str]:
    """
    Retorna lista de pacotes órfãos.
    Se auto=True, resolve em cascata (retorna ordem segura de remoção).
    """
    conn = connect(db_path)
    installed = list_installed(conn)

    all_deps: Set[str] = set()
    explicit_pkgs: Set[str] = set()

    for pkg in installed:
        ref = f"{pkg['name']}:{pkg['version']}"
        # explicit flag
        if pkg.get("explicit"):
            explicit_pkgs.add(ref)

        deps_json = pkg.get("dependencies_json") or []
        deps = []
        if isinstance(deps_json, str):
            try:
                deps = json.loads(deps_json)
            except Exception:
                deps = []
        elif isinstance(deps_json, list):
            deps = deps_json

        for d in deps:
            if isinstance(d, dict):
                dname = d.get("name")
                dver = d.get("version")
                depref = f"{dname}:{dver}" if dver else dname
                all_deps.add(depref)
            else:
                all_deps.add(str(d))

    # órfãos = instalados não referenciados e não explícitos
    orphans = []
    for pkg in installed:
        ref = f"{pkg['name']}:{pkg['version']}"
        if ref not in all_deps and ref not in explicit_pkgs:
            orphans.append(ref)
            log_event(pkg["name"], "depclean",
                      f"Marcado como órfão: {ref}", level="info")
            record_event(conn, "info", "depclean",
                         f"{ref} marcado como órfão", {})

    if not auto:
        conn.close()
        return orphans

    # --- auto-clean: calcular ordem segura ---
    removed: List[str] = []
    while True:
        new_orphans = []
        installed_map = {f"{p['name']}:{p['version']}": p for p in installed}

        for ref in list(installed_map.keys()):
            if ref not in all_deps and ref not in explicit_pkgs and ref not in removed:
                new_orphans.append(ref)

        if not new_orphans:
            break
        removed.extend(new_orphans)

        # recalcular dependências sem os removidos
        all_deps = set()
        for ref, pkg in installed_map.items():
            if ref in removed:
                continue
            deps_json = pkg.get("dependencies_json") or []
            deps = []
            if isinstance(deps_json, str):
                try:
                    deps = json.loads(deps_json)
                except Exception:
                    deps = []
            elif isinstance(deps_json, list):
                deps = deps_json
            for d in deps:
                if isinstance(d, dict):
                    dname = d.get("name")
                    dver = d.get("version")
                    depref = f"{dname}:{dver}" if dver else dname
                    all_deps.add(depref)
                else:
                    all_deps.add(str(d))

    conn.close()
    return removed
