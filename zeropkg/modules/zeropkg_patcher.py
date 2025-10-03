#!/usr/bin/env python3
# zeropkg_patcher.py - Sistema de aplicação de patches e execução de hooks
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import subprocess
import shlex
from typing import Dict, List, Optional, Union

from zeropkg_logger import log_event, get_logger
from zeropkg_toml import PatchEntry

logger = get_logger(stage="patcher")


class PatchError(Exception):
    """Falha na aplicação de patch."""
    pass


class HookError(Exception):
    """Falha na execução de hook."""
    pass


class Patcher:
    def __init__(
        self,
        workdir: str,
        ports_dir: Optional[str] = "/usr/ports",
        env: Optional[Dict[str, str]] = None,
        pkg_name: Optional[str] = None,
        hook_fatal: bool = True,
    ):
        """
        workdir: diretório de trabalho do pacote
        ports_dir: raiz do repositório de receitas (/usr/ports)
        env: variáveis de ambiente adicionais
        pkg_name: nome do pacote (para logging)
        hook_fatal: se True, falha em hook aborta o processo
        """
        self.workdir = os.path.abspath(workdir)
        self.ports_dir = os.path.abspath(ports_dir or "/usr/ports")
        self.env = os.environ.copy()
        if env:
            self.env.update(env)
        self.pkg = pkg_name or "unknown"
        self.hook_fatal = hook_fatal

    # ---------------------------------------------
    # Utilitários
    # ---------------------------------------------
    def _resolve_patch_path(self, patch_file: str) -> Optional[str]:
        """Procura patch em múltiplos locais padrão."""
        search_paths = [
            patch_file,
            os.path.join(self.workdir, "patches", patch_file),
            os.path.join(self.ports_dir, self.pkg, "patches", patch_file),
            os.path.join(self.ports_dir, "distfiles", patch_file),
        ]
        for p in search_paths:
            if os.path.exists(p):
                return os.path.abspath(p)
        return None

    def _run_command(self, cmd: Union[str, List[str]], cwd: Optional[str] = None, stage: str = "cmd"):
        """Executa comando genérico e retorna stdout/stderr."""
        if isinstance(cmd, str):
            shell = True
            display = cmd
        else:
            shell = False
            display = " ".join(shlex.quote(c) for c in cmd)
        log_event(self.pkg, stage, f"Executando: {display}")
        try:
            result = subprocess.run(
                cmd, cwd=cwd or self.workdir, env=self.env, shell=shell,
                capture_output=True, text=True, check=True
            )
            if result.stdout.strip():
                log_event(self.pkg, stage, f"[stdout]\n{result.stdout.strip()}")
            if result.stderr.strip():
                log_event(self.pkg, stage, f"[stderr]\n{result.stderr.strip()}")
            return result
        except subprocess.CalledProcessError as e:
            log_event(self.pkg, stage, f"Erro: {e.stderr}", level="error")
            raise

    # ---------------------------------------------
    # Aplicação de patches
    # ---------------------------------------------
    def apply_patch(self, patch: Union[str, PatchEntry]):
        """Aplica patch no workdir. Aceita path (str) ou PatchEntry."""
        if isinstance(patch, PatchEntry):
            patch_file = patch.path
            strip = patch.strip or 1
        else:
            patch_file = str(patch)
            strip = 1

        resolved = self._resolve_patch_path(patch_file)
        if not resolved:
            raise PatchError(f"Patch não encontrado: {patch_file}")

        log_event(self.pkg, "patch", f"Aplicando patch {os.path.basename(resolved)} (strip={strip})")
        cmd = ["patch", f"-p{strip}", "-i", resolved]
        try:
            self._run_command(cmd, stage="patch")
            log_event(self.pkg, "patch", f"Patch aplicado: {resolved}")
        except subprocess.CalledProcessError:
            # fallback git apply
            log_event(self.pkg, "patch", f"Tentando git apply: {resolved}")
            try:
                self._run_command(["git", "apply", resolved], stage="patch")
                log_event(self.pkg, "patch", f"Patch aplicado via git: {resolved}")
            except subprocess.CalledProcessError as ge:
                raise PatchError(f"Falha ao aplicar patch {resolved}") from ge

    # ---------------------------------------------
    # Execução de hooks
    # ---------------------------------------------
    def run_hook(self, script: str, stage: Optional[str] = None, timeout: int = 600):
        """Executa hook de shell com timeout."""
        stage_name = stage or "hook"
        log_event(self.pkg, stage_name, f"Executando hook: {script}")
        try:
            subprocess.run(
                script, cwd=self.workdir, env=self.env,
                shell=True, check=True, timeout=timeout,
                capture_output=True, text=True
            )
            log_event(self.pkg, stage_name, f"Hook concluído: {script}")
        except subprocess.TimeoutExpired:
            msg = f"Hook expirou após {timeout}s: {script}"
            log_event(self.pkg, stage_name, msg, level="error")
            if self.hook_fatal:
                raise HookError(msg)
        except subprocess.CalledProcessError as e:
            msg = f"Erro no hook {script}:\n{e.stderr}"
            log_event(self.pkg, stage_name, msg, level="error")
            if self.hook_fatal:
                raise HookError(msg)

    def apply_stage(
        self,
        stage: str,
        patches: Optional[List[Union[str, PatchEntry]]] = None,
        hooks: Optional[Dict[str, List[str]]] = None,
    ):
        """
        Aplica patches e hooks de um estágio específico (pre_configure, post_install, etc).
        """
        log_event(self.pkg, stage, f"Iniciando estágio: {stage}")

        # aplicar patches
        if patches:
            for patch in patches:
                if isinstance(patch, PatchEntry) and patch.stage and patch.stage != stage:
                    continue
                try:
                    self.apply_patch(patch)
                except PatchError as e:
                    log_event(self.pkg, stage, str(e), level="error")
                    if self.hook_fatal:
                        raise

        # executar hooks
        if hooks and stage in hooks:
            for script in hooks[stage]:
                try:
                    self.run_hook(script, stage=stage)
                except HookError as e:
                    log_event(self.pkg, stage, str(e), level="error")
                    if self.hook_fatal:
                        raise

        log_event(self.pkg, stage, f"Estágio concluído: {stage}")
