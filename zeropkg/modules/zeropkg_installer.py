import os
import tarfile
import shutil
import logging
from typing import Dict, Optional, List
from zeropkg_db import register_package, remove_package, get_installed_files
from zeropkg_patcher import Patcher

logger = logging.getLogger("zeropkg.installer")

class InstallerError(Exception):
    pass

class Installer:
    def __init__(self, db_path="/var/lib/zeropkg/installed.json",
                 dry_run=False, dir_install=None):
        self.db_path = db_path
        self.dry_run = dry_run
        self.dir_install = dir_install or "/"

    def install(self, pkgfile: str, meta, hooks: Optional[Dict[str, List[str]]] = None):
        """Instala um pacote binário e registra no DB"""
        stagingdir = "/tmp/zeropkg-install-staging"
        os.makedirs(stagingdir, exist_ok=True)

        # 1. Extrair pacote no staging
        logger.info(f"Extraindo {pkgfile} -> {stagingdir}")
        if not self.dry_run:
            with tarfile.open(pkgfile, "r:*") as tar:
                tar.extractall(stagingdir)

        # 2. Copiar arquivos para o destino
        dest = self.dir_install
        logger.info(f"Instalando arquivos em {dest}")
        if not self.dry_run:
            for root, dirs, files in os.walk(stagingdir):
                rel = os.path.relpath(root, stagingdir)
                target_dir = os.path.join(dest, rel) if rel != "." else dest
                os.makedirs(target_dir, exist_ok=True)
                for f in files:
                    src = os.path.join(root, f)
                    dst = os.path.join(target_dir, f)
                    shutil.copy2(src, dst)

        # 3. Registrar no DB
        if not self.dry_run:
            register_package(self.db_path, meta, stagingdir)

        # 4. Executar hooks pós-install
        if hooks:
            patcher = Patcher(workdir=dest, env=getattr(meta, "environment", {}))
            patcher.apply_stage("post_install", {}, hooks)

        logger.info(f"Pacote {meta.name}-{meta.version} instalado com sucesso.")

    def remove(self, name: str, version: str, hooks: Optional[Dict[str, List[str]]] = None):
        """Remove um pacote com base no DB"""
        files = get_installed_files(self.db_path, name, version)
        if not files:
            raise InstallerError(f"Pacote {name}-{version} não encontrado no banco.")

        # 1. Remover arquivos
        logger.info(f"Removendo pacote {name}-{version}")
        if not self.dry_run:
            for f in files:
                try:
                    os.remove(f)
                except FileNotFoundError:
                    pass  # já removido
                except Exception as e:
                    logger.warning(f"Erro ao remover {f}: {e}")

        # 2. Remover do DB
        if not self.dry_run:
            remove_package(self.db_path, name, version)

        # 3. Executar hooks pós-remove
        if hooks:
            patcher = Patcher(workdir="/", env=os.environ.copy())
            patcher.apply_stage("post_remove", {}, hooks)

        logger.info(f"Pacote {name}-{version} removido com sucesso.")
