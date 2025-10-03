#!/usr/bin/env python3
"""
zeropkg_config.py

Leitor de configuração global do Zeropkg.
Agora com cache, logging de erros e validação de diretórios.
"""

import os
import sys

try:
    import tomllib  # Python 3.11+
except ImportError:
    import tomli as tomllib  # fallback

from zeropkg_logger import log_event

# ------------------------
# Defaults
# ------------------------
CONFIG_LOCATIONS = [
    "/etc/zeropkg/config.toml",
    "/usr/lib/zeropkg/config.toml",
    os.path.expanduser("~/.config/zeropkg/config.toml"),
    "./config.toml",
]

DEFAULT_CONFIG = {
    "paths": {
        "root": "/",
        "build_root": "/var/zeropkg/build",
        "cache_dir": "/var/zeropkg/cache",
        "ports_dir": "/usr/ports",
        "db_path": "/var/lib/zeropkg/installed.sqlite3",
    },
    "repo": {
        "local": "/usr/ports",
        "remote": "https://example.com/zeropkg-ports.git",
        "branch": "main",
    },
    "sync": {
        "force": False,
    }
}

_cached_config = None  # cache em memória


# ------------------------
# Funções
# ------------------------
def load_config(force_reload: bool = False) -> dict:
    """
    Lê o config.toml. Usa cache, a menos que force_reload=True.
    """
    global _cached_config
    if _cached_config is not None and not force_reload:
        return _cached_config

    cfg = DEFAULT_CONFIG.copy()
    for path in CONFIG_LOCATIONS:
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    data = tomllib.load(f)
                    # Merge sections
                    if "paths" in data:
                        cfg["paths"].update(data["paths"])
                    if "repo" in data:
                        cfg["repo"].update(data["repo"])
                    if "sync" in data:
                        cfg["sync"].update(data["sync"])
                    # compatibilidade com configs antigos (flat)
                    for k, v in data.items():
                        if k not in ("paths", "repo", "sync"):
                            cfg[k] = v
                    log_event("config", "load", f"Config carregado de {path}")
            except Exception as e:
                log_event("config", "error", f"Falha ao ler {path}: {e}", level="error")
                continue

    # Garantir consistência de paths
    if "ports_dir" not in cfg["paths"] or not cfg["paths"]["ports_dir"]:
        cfg["paths"]["ports_dir"] = cfg["repo"]["local"]

    # Validação de diretórios críticos
    for key in ("build_root", "cache_dir", "ports_dir"):
        path = cfg["paths"].get(key)
        if path and not os.path.exists(path):
            try:
                os.makedirs(path, exist_ok=True)
                log_event("config", "paths", f"Criado diretório: {path}")
            except Exception as e:
                log_event("config", "error", f"Não foi possível criar {key} em {path}: {e}", level="error")

    _cached_config = cfg
    return cfg


def get_paths() -> dict:
    return load_config()["paths"]

def get_repo() -> dict:
    return load_config()["repo"]

def get_sync() -> dict:
    return load_config()["sync"]

def get(key: str, default=None):
    return load_config().get(key, default)


# ------------------------
# CLI rápido para debug
# ------------------------
if __name__ == "__main__":
    import argparse, json
    ap = argparse.ArgumentParser(description="Debug zeropkg config")
    ap.add_argument("--reload", action="store_true", help="Força releitura do arquivo")
    args = ap.parse_args()

    cfg = load_config(force_reload=args.reload)
    print(json.dumps(cfg, indent=2))
