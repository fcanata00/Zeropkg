#!/usr/bin/env python3
"""
zeropkg_sync.py - Sincronização de repositório do Zeropkg
"""

import os
import subprocess
import logging

from zeropkg_config import get_repo, get_sync
from zeropkg_logger import log_event


def run_cmd(cmd, cwd=None) -> str:
    logging.info(f"Executando: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(result.stderr.strip())
        raise RuntimeError(f"Erro ao rodar comando: {' '.join(cmd)}")
    return result.stdout.strip()


def sync_repos(local=None, remote=None, branch=None, force=None):
    """
    Sincroniza o(s) repositório(s) de ports.
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

    status = {"action": None, "branch": branch, "path": repo_local, "commit": None}

    if not os.path.exists(os.path.join(repo_local, ".git")):
        log_event("sync", "repo", f"Clonando {repo_remote} em {repo_local}")
        run_cmd(["git", "clone", "-b", branch, repo_remote, repo_local])
        status["action"] = "cloned"
    else:
        log_event("sync", "repo", f"Atualizando repositório em {repo_local}")
        if force:
            run_cmd(["git", "fetch", "--all"], cwd=repo_local)
            run_cmd(["git", "reset", "--hard", f"origin/{branch}"], cwd=repo_local)
            status["action"] = "forced-update"
        else:
            run_cmd(["git", "pull", "origin", branch], cwd=repo_local)
            status["action"] = "updated"

    # pegar commit atual
    try:
        commit = run_cmd(["git", "rev-parse", "HEAD"], cwd=repo_local)
        status["commit"] = commit.strip()
        log_event("sync", "repo", f"HEAD atualizado para {commit}")
    except Exception:
        status["commit"] = None

    print(f"[+] Sincronização concluída: {status['action']} @ {status['commit'] or 'desconhecido'}")
    return status
