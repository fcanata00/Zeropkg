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
from typing import Dict, List, Optional, Union
from zeropkg_logger import log_event, setup_logger
from zeropkg_toml import PatchEntry

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
        self.pkg = pkg_name or "unknown"

    def apply_patch(self, patch: Union[str, PatchEntry]):
        """Aplica patch no workdir. Aceita path (str) ou PatchEntry."""
        if isinstance(patch, PatchEntry):
            patch_file = patch.path
            strip = patch.strip
        else:
            patch_file = str(patch)
            strip = 1

        abs_patch = os.path.abspath(patch_file)
        log_event(self.pkg, "patch", f"Aplicando patch {patch_file} (strip={strip})")
        cmd = ["patch", f"-p{strip}", "-i", abs_patch]
        try:
            subprocess.run(cmd, cwd=self.workdir, env=self.env, check=True,
                           capture_output=True, text=True)
            log_event(self.pkg, "patch", f"Patch aplicado com sucesso: {patch_file}")
        except subprocess.CalledProcessError as e:
            # fallback git apply
            log_event(self.pkg, "patch", f"Falha com patch; tentando git apply: {patch_file}")
            try:
                subprocess.run(["git", "apply", abs_patch], cwd=self.workdir,
                               env=self.env, check=True, capture_output=True, text=True)
                log_event(self.pkg, "patch", f"Patch aplicado via git apply: {patch_file}")
            except subprocess.CalledProcessError as ge:
                msg = (e.stderr or "") + "\n" + (ge.stderr or "")
                raise PatchError(f"Falha ao aplicar patch {patch_file}:\n{msg}") from ge

    def run_hook(self, script: str, stage: Optional[str] = None):
        """Executa hook de shell (com suporte a comandos complexos)."""
        log_event(self.pkg, "hook", f"Executando hook [{stage or 'unknown'}]: {script}")
        try:
            subprocess.run(script, cwd=self.workdir, env=self.env,
                           shell=True, check=True, capture_output=True, text=True)
            log_event(self.pkg, "hook", f"Hook concluído: {script}")
        except subprocess.CalledProcessError as e:
            raise HookError(f"Falha no hook {script}:\n{e.stderr}") from e

    def apply_stage(self, stage: str,
                    patches: Optional[List[Union[str, PatchEntry]]] = None,
                    hooks: Optional[Dict[str, List[str]]] = None):
        """
        Aplica todos os patches e hooks definidos para o estágio dado (ex: 'pre_configure').
        - patches: lista de PatchEntry ou paths (str)
        - hooks: dict com stage -> lista de comandos
        """
        if patches:
            for patch in patches:
                if isinstance(patch, PatchEntry) and patch.stage != stage:
                    continue
                self.apply_patch(patch)

        if hooks:
            for hook in hooks.get(stage, []):
                self.run_hook(hook, stage=stage)
