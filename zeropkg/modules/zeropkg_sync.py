#!/usr/bin/env python3
import os
import subprocess
import logging

# usa o módulo de config unificado
from zeropkg_config import get_repo, get_sync


def run_cmd(cmd, cwd=None):
    logging.info(f"Executando: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(result.stderr.strip())
        raise RuntimeError(f"Erro ao rodar comando: {' '.join(cmd)}")
    return result.stdout.strip()


def sync_repos(local=None, remote=None, branch=None, force=None):
    """
    Sincroniza o repositório de ports.
    Se parâmetros não forem passados, lê do config.
    Retorna dict com status.
    """
    repo_cfg = get_repo()
    sync_cfg = get_sync()

    repo_local = local or repo_cfg.get("local", "/usr/ports")
    repo_remote = remote or repo_cfg.get("remote")
    branch = branch or repo_cfg.get("branch", "main")
    force = force if force is not None else sync_cfg.get("force", False)

    os.makedirs(repo_local, exist_ok=True)

    status = {"action": None, "branch": branch, "path": repo_local}

    if not os.path.exists(os.path.join(repo_local, ".git")):
        print(f"[*] Clonando {repo_remote} em {repo_local}")
        run_cmd(["git", "clone", "-b", branch, repo_remote, repo_local])
        status["action"] = "cloned"
    else:
        print(f"[*] Atualizando repositório em {repo_local}")
        if force:
            run_cmd(["git", "fetch", "--all"], cwd=repo_local)
            run_cmd(["git", "reset", "--hard", f"origin/{branch}"], cwd=repo_local)
            status["action"] = "forced-update"
        else:
            run_cmd(["git", "pull", "origin", branch], cwd=repo_local)
            status["action"] = "updated"

    print("[+] Sincronização concluída com sucesso!")
    return status
