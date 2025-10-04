# zeropkg_patcher.py
# Módulo responsável por aplicar patches, executar hooks e validar segurança no Zeropkg

import os
import subprocess
import hashlib
import shutil
from pathlib import Path
from zeropkg_logger import log
from zeropkg_toml import resolve_macros
from zeropkg_downloader import Downloader
from zeropkg_chroot import run_in_chroot
from zeropkg_db import Database
from zeropkg_config import ZeropkgConfig

class Patcher:
    def __init__(self, work_dir, config: ZeropkgConfig, fakeroot=False):
        self.work_dir = Path(work_dir)
        self.config = config
        self.fakeroot = fakeroot
        self.db = Database(config)
        self.downloader = Downloader(config)
        self.patch_cache_dir = Path(config.get("paths", "patch_cache", fallback="/var/cache/zeropkg/patches"))
        self.patch_cache_dir.mkdir(parents=True, exist_ok=True)

    # ===============================================================
    # Função principal: aplica todos os patches e hooks definidos
    # ===============================================================
    def apply_all(self, pkg_name, recipe):
        log(f"🔧 Iniciando aplicação de patches e hooks para {pkg_name}")

        self._apply_patches(pkg_name, recipe.get("patches", []))
        self._run_hooks(pkg_name, recipe.get("hooks", {}))
        log(f"✅ Patches e hooks aplicados para {pkg_name}")

    # ===============================================================
    # Aplicar patches — suporta locais, remotos e checagem de hash
    # ===============================================================
    def _apply_patches(self, pkg_name, patches):
        if not patches:
            log(f"ℹ️ Nenhum patch a aplicar para {pkg_name}")
            return

        for patch in patches:
            patch_url = resolve_macros(patch.get("url") or patch.get("path"))
            sha256 = patch.get("sha256")
            patch_file = self._get_patch_file(patch_url)

            if sha256 and not self._verify_sha256(patch_file, sha256):
                raise ValueError(f"❌ SHA256 incorreto para {patch_file}")

            log(f"📦 Aplicando patch: {patch_file}")
            try:
                self._apply_patch_file(patch_file)
                self.db.mark_patch_applied(pkg_name, patch_file.name)
            except Exception as e:
                log(f"⚠️ Falha ao aplicar patch {patch_file}: {e}")
                self._save_patch_failure(pkg_name, patch_file, e)
                raise

    def _get_patch_file(self, patch_url):
        if patch_url.startswith(("http://", "https://", "ftp://", "git://")):
            return self.downloader.download_to_cache(patch_url, self.patch_cache_dir)
        else:
            local_path = Path(patch_url).expanduser()
            if not local_path.exists():
                raise FileNotFoundError(f"Patch não encontrado: {patch_url}")
            return local_path

    def _verify_sha256(self, file_path, expected_hash):
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        result = sha256.hexdigest()
        log(f"Verificando SHA256: {result}")
        return result == expected_hash

    def _apply_patch_file(self, patch_file):
        # Tenta patch, depois git apply, detecta automaticamente -p nível
        try:
            subprocess.run(["patch", "-p1", "-i", str(patch_file)], cwd=self.work_dir, check=True)
        except subprocess.CalledProcessError:
            for p_level in range(0, 3):
                try:
                    subprocess.run(["patch", f"-p{p_level}", "-i", str(patch_file)],
                                   cwd=self.work_dir, check=True)
                    return
                except subprocess.CalledProcessError:
                    continue
            try:
                subprocess.run(["git", "apply", str(patch_file)], cwd=self.work_dir, check=True)
            except Exception as e:
                raise RuntimeError(f"Falha ao aplicar patch {patch_file}: {e}")

    def _save_patch_failure(self, pkg_name, patch_file, error):
        fail_dir = Path("/var/log/zeropkg/patch_failures")
        fail_dir.mkdir(parents=True, exist_ok=True)
        fail_log = fail_dir / f"{pkg_name}_{patch_file.name}.log"
        with open(fail_log, "w") as f:
            f.write(str(error))
        log(f"⚠️ Log de falha salvo em {fail_log}")

    # ===============================================================
    # Hooks pré/pós
    # ===============================================================
    def _run_hooks(self, pkg_name, hooks):
        for stage, cmds in hooks.items():
            if not cmds:
                continue
            log(f"⚙️ Executando hook {stage} para {pkg_name}")
            for cmd in cmds:
                resolved_cmd = resolve_macros(cmd)
                self._run_command(resolved_cmd, stage)

    def _run_command(self, command, stage):
        if self.fakeroot:
            command = f"fakeroot sh -c '{command}'"
        elif self.config.get_bool("build", "use_chroot", fallback=False):
            run_in_chroot(self.config, command)
            return
        subprocess.run(command, shell=True, cwd=self.work_dir, check=True)

    # ===============================================================
    # Rollback de patches aplicados (melhor esforço)
    # ===============================================================
    def rollback(self, pkg_name):
        patches = self.db.get_applied_patches(pkg_name)
        for patch in reversed(patches):
            try:
                subprocess.run(["patch", "-R", "-p1", "-i", str(patch)], cwd=self.work_dir, check=True)
                log(f"🔁 Patch revertido: {patch}")
            except subprocess.CalledProcessError:
                log(f"⚠️ Falha ao reverter patch {patch}")

    # ===============================================================
    # Integração futura com verificação de vulnerabilidades
    # ===============================================================
    def check_vulnerabilities(self, pkg_name):
        """Placeholder — será integrado com zeropkg_security futuramente."""
        vulns = self.db.get_known_vulnerabilities(pkg_name)
        if vulns:
            log(f"⚠️ {pkg_name} contém {len(vulns)} vulnerabilidades conhecidas!")
        else:
            log(f"✅ Nenhuma vulnerabilidade conhecida em {pkg_name}")

# ===============================================================
# CLI direto: aplicar patches manualmente
# ===============================================================
if __name__ == "__main__":
    import sys
    from zeropkg_config import ZeropkgConfig
    config = ZeropkgConfig()
    pkg = sys.argv[1] if len(sys.argv) > 1 else "teste"
    recipe = {"patches": [{"url": "example.patch", "sha256": "abcd"}]}
    patcher = Patcher("/tmp/build", config)
    patcher.apply_all(pkg, recipe)
