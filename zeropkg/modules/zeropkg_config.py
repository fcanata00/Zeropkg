#!/usr/bin/env python3
"""
zeropkg_config.py - Leitura da configuração global do Zeropkg

Lê /etc/zeropkg.conf (TOML) e retorna um dicionário com as configurações.
Se o arquivo não existir, retorna valores padrão.
"""

import os
import tomllib  # Python 3.11+; se estiver em 3.10, use `import tomli as tomllib`

DEFAULT_CONFIG = {
    "paths": {
        "root": "/",
        "build_root": "/var/zeropkg/build",
        "cache_dir": "/var/zeropkg/packages",
        "ports_dir": "/usr/ports",
        "db_path": "/var/lib/zeropkg/installed.sqlite3",
    }
}

CONFIG_FILE = "/etc/zeropkg.conf"


def load_config(config_file: str = CONFIG_FILE) -> dict:
    """
    Lê o arquivo de configuração TOML e retorna como dict.
    Se não existir, retorna DEFAULT_CONFIG.
    """
    cfg = DEFAULT_CONFIG.copy()
    if os.path.exists(config_file):
        try:
            with open(config_file, "rb") as f:
                parsed = tomllib.load(f)
            # sobrepor defaults com valores do arquivo
            for section, values in parsed.items():
                if section not in cfg:
                    cfg[section] = {}
                for k, v in values.items():
                    cfg[section][k] = v
        except Exception as e:
            print(f"[!] Erro ao ler {config_file}: {e}")
    return cfg


# Helper para pegar paths já resolvidos
def get_paths(config_file: str = CONFIG_FILE) -> dict:
    cfg = load_config(config_file)
    return cfg.get("paths", {})
