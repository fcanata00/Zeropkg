#!/usr/bin/env python3
"""
zeropkg_config.py

Carrega configuração global do Zeropkg (TOML). Procura em várias localizações e
normaliza o resultado para uso pelo CLI e módulos.
"""

from __future__ import annotations
import os

# compatibilidade toml
try:
    import tomllib  # Python 3.11+
except Exception:
    try:
        import tomli as tomllib  # type: ignore
    except Exception:
        tomllib = None  # will raise later if used

# Locais onde o config pode existir (ordem de preferência)
CANDIDATE_PATHS = [
    "/etc/zeropkg.conf",                 # recommended system config
    "/usr/lib/zeropkg/config.toml",      # installed package config
    os.path.join(os.path.dirname(__file__), "..", "config.toml"),  # repo-local (relative)
    "./config.toml",                     # working dir (dev)
]

# defaults
DEFAULT = {
    "paths": {
        "root": "/",
        "build_root": "/var/zeropkg/build",
        "cache_dir": "/var/zeropkg/packages",
        "ports_dir": "/usr/ports",
        "db_path": "/var/lib/zeropkg/installed.sqlite3",
    },
    "repo": {
        "local": "/usr/ports",
        "remote": None,
        "branch": "main",
    },
    "sync": {
        "force": False
    }
}


def _load_toml_file(path: str) -> dict:
    if tomllib is None:
        raise RuntimeError("Nenhuma biblioteca TOML disponível (instale tomli para Python <3.11).")
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_config() -> dict:
    """
    Carrega o primeiro arquivo TOML encontrado entre os candidatos e
    retorna um dict normalizado combinando com DEFAULT.
    """
    cfg = {
        "paths": dict(DEFAULT["paths"]),
        "repo": dict(DEFAULT["repo"]),
        "sync": dict(DEFAULT["sync"]),
    }

    found = None
    for p in CANDIDATE_PATHS:
        if not p:
            continue
        try:
            # normalize relative path
            p_exp = os.path.abspath(p)
            if os.path.exists(p_exp):
                parsed = _load_toml_file(p_exp)
                found = p_exp
                # merge paths
                if "paths" in parsed and isinstance(parsed["paths"], dict):
                    cfg["paths"].update(parsed["paths"])
                # support older config that uses [repo] (your repo file)
                if "repo" in parsed and isinstance(parsed["repo"], dict):
                    cfg["repo"].update(parsed["repo"])
                    # ensure ports_dir mirrors repo.local if not set
                    if "local" in parsed["repo"] and not cfg["paths"].get("ports_dir"):
                        cfg["paths"]["ports_dir"] = parsed["repo"]["local"]
                if "sync" in parsed and isinstance(parsed["sync"], dict):
                    cfg["sync"].update(parsed["sync"])
                # some repos use top-level keys directly (backward compatibility)
                for k in ("ports_dir", "cache_dir", "build_root", "root", "db_path"):
                    if k in parsed and k not in cfg["paths"]:
                        cfg["paths"][k] = parsed[k]
                break
        except Exception:
            # ignore parse errors and try next candidate
            continue

    # If not found, cfg stays defaults
    return cfg


# convenience getters
def get_paths() -> dict:
    return load_config()["paths"]


def get_repo() -> dict:
    return load_config()["repo"]


def get_sync() -> dict:
    return load_config()["sync"]
