import logging
from typing import Dict, List
from zeropkg_db import load_db

logger = logging.getLogger("zeropkg.depclean")

def revdep(db_path="/var/lib/zeropkg/installed.json") -> Dict[str, List[str]]:
    """
    Verifica dependências quebradas.
    Retorna dict {pacote: [dependências ausentes]}.
    """
    db = load_db(db_path)
    broken = {}

    for pkg in db.get("installed", []):
        name = pkg["name"]
        deps = pkg.get("dependencies", [])
        missing = []
        for dep in deps:
            dep_name, _, dep_version = dep.partition(":")
            found = any(
                p["name"] == dep_name and (not dep_version or p["version"] == dep_version)
                for p in db.get("installed", [])
            )
            if not found:
                missing.append(dep)
        if missing:
            broken[name] = missing

    return broken

def depclean(db_path="/var/lib/zeropkg/installed.json") -> List[str]:
    """
    Retorna pacotes órfãos (não dependidos por ninguém e não explícitos).
    """
    db = load_db(db_path)
    installed = db.get("installed", [])

    all_deps = set()
    for pkg in installed:
        all_deps.update(pkg.get("dependencies", []))

    orphans = []
    for pkg in installed:
        ref = f"{pkg['name']}:{pkg['version']}"
        if ref not in all_deps and not pkg.get("explicit", True):
            orphans.append(ref)

    return orphans
