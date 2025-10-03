import os
import subprocess
import logging
from typing import Dict, List

logger = logging.getLogger("zeropkg.patcher")

class PatchError(Exception):
    pass

class HookError(Exception):
    pass

class Patcher:
    def __init__(self, workdir: str, env: Dict[str, str] = None):
        self.workdir = workdir
        self.env = os.environ.copy()
        if env:
            self.env.update(env)

    def apply_patch(self, patch_file: str, strip: int = 1):
        """Aplica um patch dentro do workdir"""
        logger.info(f"Aplicando patch {patch_file} em {self.workdir}")
        patch_path = os.path.abspath(patch_file)
        try:
            subprocess.run(
                ["patch", f"-p{strip}", "-i", patch_path],
                cwd=self.workdir,
                env=self.env,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise PatchError(f"Falha ao aplicar patch {patch_file}:\n{e.stderr}") from e

    def run_hook(self, script: str):
        """Executa um hook dentro do workdir"""
        logger.info(f"Rodando hook {script} em {self.workdir}")
        try:
            subprocess.run(
                ["/bin/sh", script],
                cwd=self.workdir,
                env=self.env,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise HookError(f"Falha no hook {script}:\n{e.stderr}") from e

    def apply_stage(self, stage: str, patches: Dict[str, List[str]], hooks: Dict[str, List[str]]):
        """Executa patches e hooks de um estágio (ex: pre_configure)"""
        # Patches
        for patch in patches.get(stage, []):
            self.apply_patch(patch)

        # Hooks
        for hook in hooks.get(stage, []):
            self.run_hook(hook)
