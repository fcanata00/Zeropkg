"""
zeropkg_installer.py

Módulo Installer para Zeropkg — versão revisada.

Funções:
- instalar pacotes binários (tarball) no sistema ou em dir alternativo
- desinstalar pacotes baseando-se no banco
- registrar no DB (chamando zeropkg_db.register_package / remove_package)
- rodar hooks pós-install / pós-remove
- registrar eventos com logger e no DB
"""

import os
import tarfile
import shutil
from typing import Optional, List, Tuple
from zeropkg_db import connect, register_package, remove_package, get_package, query_events, record_event
from zeropkg_logger import log_event, setup_logger
from zeropkg_patcher import Patcher, HookError

logger = setup_logger(pkg_name=None, stage="installer")

class InstallerError(Exception):
    pass

class Installer:
    def __init__(self, db_path: str = "/var/lib/zeropkg/installed.sqlite3",
                 dry_run: bool = False, dir_install: Optional[str] = None):
        self.db_path = db_path
        self.dry_run = dry_run
        # destino de instalação real (prefix root). Se dir_install fornecido, instala nele.
        self.dir_install = dir_install or "/"

    def install(self, pkgfile: str, meta, hooks: Optional[dict] = None) -> List[str]:
        """
        Instala um pacote binário tarball `pkgfile` para meta (PackageMeta).
        Retorna lista dos caminhos dos arquivos instalados.
        """
        pkgname = meta.name
        staging = "/tmp/zeropkg_install_staging"
        if os.path.exists(staging):
            shutil.rmtree(staging, ignore_errors=True)
        os.makedirs(staging, exist_ok=True)

        log_event(pkgname, "installer", f"Extraindo {pkgfile} em staging")
        if not self.dry_run:
            with tarfile.open(pkgfile, "r:*") as tar:
                tar.extractall(staging)

        log_event(pkgname, "installer", f"Copiando arquivos para {self.dir_install}")
        installed_files: List[str] = []
        if not self.dry_run:
            for root, dirs, files in os.walk(staging):
                rel = os.path.relpath(root, staging)
                target_dir = os.path.join(self.dir_install, rel) if rel != "." else self.dir_install
                os.makedirs(target_dir, exist_ok=True)
                for f in files:
                    src = os.path.join(root, f)
                    dst = os.path.join(target_dir, f)
                    shutil.copy2(src, dst)
                    installed_files.append(dst)

        # registrar no DB
        conn = connect(self.db_path)
        # produzir lista (path, None) porque não calculamos hash aqui
        file_list = [(p, None) for p in installed_files]
        pkg_id = None
        if not self.dry_run:
            pkg_id = register_package(conn, meta, pkgfile, file_list)
            record_event(conn, "INFO", "installer", f"Pacote instalado: {meta.name}-{meta.version}", {"pkg_id": pkg_id})

        # hooks pós-install
        if hooks:
            p = Patcher(workdir=self.dir_install, env=meta.environment if hasattr(meta, "environment") else {}, pkg_name=pkgname)
            try:
                p.apply_stage("post_install", {}, hooks)
            except HookError as he:
                log_event(pkgname, "installer", f"Erro em hook pós-install: {he}", level="error")
                raise

        log_event(pkgname, "installer", f"Instalação concluída: {meta.name}-{meta.version}")
        return installed_files

    def remove(self, name: str, version: Optional[str] = None, hooks: Optional[dict] = None) -> List[str]:
        """
        Remove um pacote nome:versão. Retorna lista de arquivos que tentou apagar.
        """
        conn = connect(self.db_path)
        pkg = get_package(conn, name, version)
        if not pkg:
            raise InstallerError(f"Pacote não instalado: {name}:{version}")

        # obter arquivos antes de remover do DB
        # usamos remove_package que retorna paths
        files_to_remove = remove_package(conn, name, pkg.get("version"))

        log_event(name, "installer", f"Removendo pacote {name}:{pkg.get('version')}")
        removed = []
        if not self.dry_run:
            for fpath in files_to_remove:
                try:
                    os.remove(fpath)
                    removed.append(fpath)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    log_event(name, "installer", f"Erro ao remover {fpath}: {e}", level="error")

        # hooks pós-remove
        if hooks:
            p = Patcher(workdir=self.dir_install, env=os.environ.copy(), pkg_name=name)
            try:
                p.apply_stage("post_remove", {}, hooks)
            except HookError as he:
                log_event(name, "installer", f"Erro em hook pós-remove: {he}", level="error")
                raise

        log_event(name, "installer", f"Remoção concluída: {name}:{pkg.get('version')}")
        return removed
