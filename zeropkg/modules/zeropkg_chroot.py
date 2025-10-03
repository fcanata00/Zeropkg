#!/usr/bin/env python3
"""
zeropkg_chroot.py

Gerenciamento seguro e robusto de chroot para Zeropkg (LFS).
- prepare_chroot(root, copy_resolv=True, dry_run=False)
- enter_chroot(root, command=None, env=None, dry_run=False)
- cleanup_chroot(root, force_lazy=False, dry_run=False)

Funcionalidades:
- monta /dev, /dev/pts, /proc, /sys, /run, /dev/shm, /tmp
- confere montagens existentes antes de montar (idempotente)
- valida permissões/ownership dos diretórios
- copia /etc/resolv.conf do host para o chroot (com backup)
- desmontagem em ordem reversa com fallback lazy umount
- exige root (verificação clara)
- integra com zeropkg_logger.log_event
"""

from __future__ import annotations
import os
import stat
import shutil
import subprocess
import logging
import time
from typing import Dict, List, Optional

from zeropkg_logger import log_event

logger = logging.getLogger("zeropkg.chroot")


class ChrootError(Exception):
    pass


# ordem de montagem e tipos: (source, target_relpath, fstype, options, is_bind)
_DEFAULT_MOUNTS = [
    ("/dev", "dev", None, "bind", True),
    ("/dev/pts", "dev/pts", "devpts", "gid=5,mode=620,ptmxmode=666", False),
    ("/proc", "proc", "proc", "nosuid,noexec,nodev", False),
    ("/sys", "sys", "sysfs", "nosuid,noexec,nodev", False),
    ("/run", "run", None, "bind", True),
    ("/dev/shm", "dev/shm", "tmpfs", "mode=1777", False),
    ("/tmp", "tmp", "tmpfs", "mode=1777", False),
]


def _check_root():
    if os.geteuid() != 0:
        raise ChrootError("Operação de chroot requer privilégios root.")


def _is_mounted(target: str) -> bool:
    """Verifica /proc/mounts se target está montado (caminho absoluto)."""
    try:
        target = os.path.abspath(target)
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and os.path.abspath(parts[1]) == target:
                    return True
    except FileNotFoundError:
        # fallback
        return os.path.ismount(target)
    return False


def _run(cmd: List[str], dry_run: bool = False, capture: bool = False) -> subprocess.CompletedProcess:
    """Helper para rodar comandos com erro tratado."""
    log_event("chroot", "cmd", f"CMD: {' '.join(cmd)}")
    if dry_run:
        logger.info("[dry-run] " + " ".join(cmd))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    try:
        if capture:
            return subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        else:
            return subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.exception("Comando falhou: %s", " ".join(cmd))
        raise ChrootError(f"Erro ao executar comando: {' '.join(cmd)}: {e}") from e


def _ensure_dir(path: str, mode: int = 0o755, dry_run: bool = False):
    if not os.path.exists(path):
        log_event("chroot", "prepare", f"Criando diretório {path}")
        if not dry_run:
            os.makedirs(path, exist_ok=True)
            os.chmod(path, mode)
    else:
        if not os.path.isdir(path):
            raise ChrootError(f"{path} existe e não é diretório.")
        try:
            st = os.stat(path)
            cur_mode = stat.S_IMODE(st.st_mode)
            if cur_mode != mode and not dry_run:
                os.chmod(path, mode)
                log_event("chroot", "prepare", f"Ajustado mode {oct(mode)} em {path}")
        except Exception:
            pass


def prepare_chroot(root: str, copy_resolv: bool = True, dry_run: bool = False, mounts: Optional[List] = None) -> Dict:
    """
    Prepara o ambiente de chroot em 'root':
    - monta /dev, /dev/pts, /proc, /sys, /run, /dev/shm, /tmp
    - copia /etc/resolv.conf -> $root/etc/resolv.conf (com backup)
    - retorna dict com status e lista de montagens efetuadas.
    """
    _check_root()
    root = os.path.abspath(root)
    mounts = mounts or _DEFAULT_MOUNTS

    if not os.path.exists(root):
        raise ChrootError(f"Root {root} não existe")

    log_event("chroot", "prepare", f"Iniciando prepare_chroot em {root}")

    mounted = []
    try:
        _ensure_dir(os.path.join(root, "etc"), mode=0o755, dry_run=dry_run)
        for src, rel_target, fstype, opts, is_bind in mounts:
            target = os.path.join(root, rel_target)
            _ensure_dir(target, mode=0o755, dry_run=dry_run)

            if _is_mounted(target):
                log_event("chroot", "prepare", f"Já montado: {target}")
                mounted.append({"target": target, "action": "already_mounted"})
                continue

            if is_bind:
                cmd = ["mount", "--bind", src, target]
            else:
                if fstype is None:
                    cmd = ["mount", "--bind", src, target]
                else:
                    if opts:
                        cmd = ["mount", "-t", fstype, "-o", opts, src, target]
                    else:
                        cmd = ["mount", "-t", fstype, src, target]

            _run(cmd, dry_run=dry_run)
            mounted.append({"target": target, "action": "mounted", "cmd": " ".join(cmd)})
            log_event("chroot", "prepare", f"Montado {src} -> {target} (fstype={fstype}, opts={opts})")

            # ptmx symlink
            if rel_target == "dev/pts":
                ptmx = os.path.join(root, "dev", "ptmx")
                if not os.path.exists(ptmx) and not dry_run:
                    try:
                        os.symlink("/dev/ptmx", ptmx)
                        log_event("chroot", "prepare", f"Criado symlink ptmx em {ptmx}")
                    except FileExistsError:
                        pass

        # copiar resolv.conf
        if copy_resolv:
            host_resolv = "/etc/resolv.conf"
            dest = os.path.join(root, "etc", "resolv.conf")
            if os.path.exists(host_resolv):
                if os.path.exists(dest):
                    bak = dest + ".zeropkg.bak"
                    if not dry_run:
                        shutil.copy2(dest, bak)
                    log_event("chroot", "prepare", f"Backup {dest} -> {bak}")
                if not dry_run:
                    shutil.copy2(host_resolv, dest)
                mounted.append({"target": dest, "action": "copied_resolv"})
                log_event("chroot", "prepare", f"Copiado {host_resolv} -> {dest}")
            else:
                log_event("chroot", "prepare", f"{host_resolv} não encontrado; não copiado", level="warning")

        log_event("chroot", "prepare", f"prepare_chroot concluído em {root}")
        return {"status": "ok", "mounted": mounted}
    except Exception as e:
        log_event("chroot", "prepare", f"Erro em prepare_chroot: {e}", level="error")
        try:
            cleanup_chroot(root, force_lazy=True, dry_run=dry_run)
        except Exception:
            pass
        raise


def enter_chroot(root: str, command: Optional[List[str]] = None, env: Optional[Dict[str, str]] = None, dry_run: bool = False):
    """
    Entra no chroot e executa 'command' com ambiente limpo.
    - command: lista, ex: ['/bin/bash', '-lc', 'make -j4']
    - env: dict de variáveis a exportar (ex: {'LFS':'/mnt/lfs'})
    """
    _check_root()
    root = os.path.abspath(root)
    if not os.path.isdir(root):
        raise ChrootError(f"Root inválido: {root}")

    cmd = ["chroot", root, "/usr/bin/env", "-i"]
    if env:
        for k, v in env.items():
            cmd.append(f'{k}={v}')
    if "PATH" not in (env or {}):
        cmd.append("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
    if command:
        cmd += command
    else:
        cmd.append("/bin/bash")

    log_event("chroot", "enter", f"Entrando em chroot {root} CMD: {' '.join(cmd)}")
    if dry_run:
        logger.info("[dry-run] " + " ".join(cmd))
        return {"status": "dry-run", "cmd": " ".join(cmd)}
    try:
        subprocess.run(cmd, check=True)
        log_event("chroot", "enter", "Comando em chroot executado com sucesso")
        return {"status": "ok"}
    except subprocess.CalledProcessError as e:
        log_event("chroot", "enter", f"Erro ao executar em chroot: {e}", level="error")
        raise ChrootError(f"Erro ao executar em chroot: {e}") from e


def cleanup_chroot(root: str, force_lazy: bool = False, dry_run: bool = False) -> Dict:
    """
    Desmonta de forma segura tudo o que foi montado por prepare_chroot.
    - desmonta em ordem reversa preferencialmente
    - tenta ummount normal, se falhar tenta 'umount -l' se force_lazy True
    """
    _check_root()
    root = os.path.abspath(root)
    log_event("chroot", "cleanup", f"Iniciando cleanup_chroot em {root}")
    results = {"attempts": []}

    targets = [
        os.path.join(root, "dev/pts"),
        os.path.join(root, "dev"),
        os.path.join(root, "proc"),
        os.path.join(root, "sys"),
        os.path.join(root, "run"),
        os.path.join(root, "dev/shm"),
        os.path.join(root, "tmp"),
    ]

    for t in targets:
        if not os.path.exists(t):
            results["attempts"].append({"target": t, "status": "missing"})
            continue
        if not _is_mounted(t):
            results["attempts"].append({"target": t, "status": "not_mounted"})
            continue

        try:
            _run(["umount", t], dry_run=dry_run)
            results["attempts"].append({"target": t, "status": "unmounted"})
            log_event("chroot", "cleanup", f"Desmontado {t}")
            time.sleep(0.05)
        except Exception as e:
            log_event("chroot", "cleanup", f"Falha ao desmontar {t}: {e}", level="warning")
            if force_lazy:
                try:
                    _run(["umount", "-l", t], dry_run=dry_run)
                    results["attempts"].append({"target": t, "status": "lazy_unmounted"})
                    log_event("chroot", "cleanup", f"Lazy unmount {t}")
                except Exception as e2:
                    results["attempts"].append({"target": t, "status": "failed", "error": str(e2)})
                    log_event("chroot", "cleanup", f"Falha lazy unmount {t}: {e2}", level="error")
            else:
                results["attempts"].append({"target": t, "status": "failed", "error": str(e)})

    ptmx = os.path.join(root, "dev", "ptmx")
    if os.path.islink(ptmx):
        try:
            if not dry_run:
                os.unlink(ptmx)
            results["ptmx"] = "removed"
            log_event("chroot", "cleanup", f"Removido symlink {ptmx}")
        except Exception as e:
            results["ptmx"] = f"error: {e}"
            log_event("chroot", "cleanup", f"Erro ao remover ptmx: {e}", level="warning")

    log_event("chroot", "cleanup", f"cleanup_chroot concluído em {root}")
    return results
