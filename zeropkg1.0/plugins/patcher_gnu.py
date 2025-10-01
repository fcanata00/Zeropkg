# zeropkg1.0/plugins/patcher_gnu.py
import os
import subprocess
from . import register_plugin

class GNUPatcher:
    def apply(self, src_dir, patches, strip_level=1, log_file=None):
        """
        Aplica múltiplos patches no diretório de fontes.
        :param src_dir: diretório do código fonte
        :param patches: lista de arquivos .patch/.diff
        :param strip_level: nível de -p (0,1,2)
        :param log_file: caminho do arquivo de log
        """
        if not patches:
            return True

        for patch in patches:
            if not os.path.exists(patch):
                raise FileNotFoundError(f"Patch não encontrado: {patch}")

            cmd = ["patch", f"-p{strip_level}", "-i", patch, "-d", src_dir, "-s"]
            with open(log_file, "a") if log_file else subprocess.DEVNULL as logf:
                proc = subprocess.run(cmd, stdout=logf, stderr=logf)
                if proc.returncode != 0:
                    raise RuntimeError(f"Falha ao aplicar patch: {patch}")

        return True

# auto-registro
register_plugin("patcher", "gnu", GNUPatcher())
