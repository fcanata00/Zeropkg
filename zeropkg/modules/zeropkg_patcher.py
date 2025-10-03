"""
zeropkg_patcher.py

Módulo Patcher / Hooks para Zeropkg — versão revisada.

Funcionalidades:
- Aplica patches declarados nos metadados (pre / post stages) com fallback (patch / git apply).
- Executa hooks de shell nos estágios.
- Integra logging usando zeropkg_logger.
- Propaga erros claros (PatchError, HookError).
"""

from __future__ import annotations
import os
import subprocess
import logging
from typing import Dict, List, Optional
from zeropkg_logger import log_event, setup_logger

logger = setup_logger(pkg_name=None, stage="patcher")

class PatchError(Exception):
    """Falha na aplicação de patch."""
    pass

class HookError(Exception):
    """Falha na execução de hook."""
    pass

class Patcher:
    def __init__(self, workdir: str, env: Optional[Dict[str, str]] = None, pkg_name: Optional[str] = None):
        self.workdir = workdir
        self.env = os.environ.copy()
        if env:
            self.env.update(env)
        # para logar eventos sob o pacote correto
        self.pkg = pkg_name or "unknown"

    def apply_patch(self, patch_file: str, strip: int = 1):
        """Aplica patch (patch -pN) no workdir. Tenta fallback com git apply se falhar."""
        abs_patch = os.path.abspath(patch_file)
        log_event(self.pkg, "patch", f"Aplicando patch {patch_file} (strip={strip})")
        # comando padrão patch
        cmd = ["patch", f"-p{strip}", "-i", abs_patch]
        try:
            subprocess.run(cmd, cwd=self.workdir, env=self.env, check=True, capture_output=True, text=True)
            log_event(self.pkg, "patch", f"Patch aplicado com sucesso: {patch_file}")
        except subprocess.CalledProcessError as e:
            # tentar fallback git apply
            log_event(self.pkg, "patch", f"Falha com patch; tentando git apply: {patch_file}")
            try:
                # `git apply --directory=...`
                subprocess.run(["git", "apply", abs_patch], cwd=self.workdir, env=self.env,
                               check=True, capture_output=True, text=True)
                log_event(self.pkg, "patch", f"Patch aplicado via git apply: {patch_file}")
            except subprocess.CalledProcessError as ge:
                msg = e.stderr or "" + "\n" + (ge.stderr or "")
                raise PatchError(f"Falha ao aplicar patch {patch_file}:\n{msg}") from ge

    def run_hook(self, script: str):
        """Executa hook shell no workdir."""
        log_event(self.pkg, "hook", f"Executando hook: {script}")
        # script pode ser caminho ou comando
        try:
            subprocess.run(script.split(), cwd=self.workdir, env=self.env, check=True, capture_output=True, text=True)
            log_event(self.pkg, "hook", f"Hook executado: {script}")
        except subprocess.CalledProcessError as e:
            raise HookError(f"Falha no hook {script}:\n{e.stderr}") from e

    def apply_stage(self, stage: str,
                    patches: Optional[Dict[str, List[str]]] = None,
                    hooks: Optional[Dict[str, List[str]]] = None):
        """
        Aplica todos os patches e hooks definidos para o estágio dado (ex: 'pre_configure').
        patches: dict com stage -> lista de patch paths
        hooks: dict com stage -> lista de hook comandos
        """
        if patches:
            for patch in patches.get(stage, []):
                self.apply_patch(patch)

        if hooks:
            for hook in hooks.get(stage, []):
                self.run_hook(hook)
