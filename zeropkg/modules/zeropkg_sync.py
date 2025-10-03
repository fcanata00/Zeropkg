#!/usr/bin/env python3
import os
import subprocess
import tomllib
import logging

CONFIG_FILE = "/etc/zeropkg/config.toml"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Configuração não encontrada: {CONFIG_FILE}")
    with open(CONFIG_FILE, "rb") as f:
        return tomllib.load(f)

def run_cmd(cmd, cwd=None):
    logging.info(f"Executando: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError(f"Erro ao rodar comando: {' '.join(cmd)}")
    return result.stdout.strip()

def sync_repos():
    cfg = load_config()
    repo_local = cfg["repo"]["local"]
    repo_remote = cfg["repo"]["remote"]
    branch = cfg["repo"].get("branch", "main")
    force = cfg.get("sync", {}).get("force", False)

    os.makedirs(repo_local, exist_ok=True)

    if not os.path.exists(os.path.join(repo_local, ".git")):
        print(f"[*] Clonando {repo_remote} em {repo_local}")
        run_cmd(["git", "clone", "-b", branch, repo_remote, repo_local])
    else:
        print(f"[*] Atualizando repositório em {repo_local}")
        if force:
            run_cmd(["git", "fetch", "--all"], cwd=repo_local)
            run_cmd(["git", "reset", "--hard", f"origin/{branch}"], cwd=repo_local)
        else:
            run_cmd(["git", "pull", "origin", branch], cwd=repo_local)

    print("[+] Sincronização concluída com sucesso!")
