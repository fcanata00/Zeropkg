"""
zeropkg_builder.py

Builder principal do Zeropkg — versão revisada para LFS:
- Resolve dependências
- Suporte a fakeroot/chroot
- Logs por fase
- Controle configurável de jobs
- Retorno estruturado
"""

import os
import tarfile
import shutil
import subprocess
import tempfile
import logging
from pathlib import Path
from typing import Optional, Dict, Any

from zeropkg_downloader import Downloader
from zeropkg_patcher import Patcher
from zeropkg_logger import log_event
from zeropkg_db import connect, record_build_start, record_build_finish
from zeropkg_installer import Installer
from zeropkg_deps import resolve_dependencies
from zeropkg_toml import PackageMeta, package_id

logger = logging.getLogger("zeropkg.builder")

class BuildError(Exception):
    pass


class Builder:
    def __init__(self, meta: PackageMeta,
                 cache_dir="/usr/ports/distfiles",
                 pkg_cache="/var/zeropkg/packages",
                 build_root="/var/zeropkg/build",
                 dry_run=False,
                 use_fakeroot=True,
                 chroot: Optional[str] = None,
                 db_path="/var/lib/zeropkg/installed.sqlite3"):

        self.meta = meta
        self.cache_dir = cache_dir
        self.pkg_cache = pkg_cache
        self.build_root = build_root
        self.dry_run = dry_run
        self.use_fakeroot = use_fakeroot
        self.chroot = chroot
        self.db_path = db_path

    def _run(self, cmd, cwd=None, env=None, stage="build"):
        log_event(self.meta.name, stage, f"Executando: {cmd}")
        if self.dry_run:
            return
        try:
            subprocess.run(cmd, cwd=cwd, env=env or os.environ,
                           shell=isinstance(cmd, str),
                           check=True, text=True)
        except subprocess.CalledProcessError as e:
            raise BuildError(f"Falha no estágio {stage}: {cmd}\n{e.stderr or e}") from e

    def _extract(self, src: str, dest: str):
        if self.dry_run:
            return
        if src.endswith(".tar.gz") or src.endswith(".tgz"):
            mode = "r:gz"
        elif src.endswith(".tar.xz"):
            mode = "r:xz"
        elif src.endswith(".tar.bz2"):
            mode = "r:bz2"
        else:
            raise BuildError(f"Formato não suportado: {src}")
        with tarfile.open(src, mode) as tf:
            def is_within_directory(directory, target):
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
                return os.path.commonprefix([abs_directory, abs_target]) == abs_directory
            for m in tf.getmembers():
                if not is_within_directory(dest, os.path.join(dest, m.name)):
                    raise BuildError("Tentativa de path traversal no tar")
            tf.extractall(path=dest)

    def build(self, dir_install: Optional[str] = None) -> Dict[str, Any]:
        """Executa o build completo do pacote. Retorna dict com status e caminho do pacote."""
        os.makedirs(self.build_root, exist_ok=True)
        os.makedirs(self.pkg_cache, exist_ok=True)

        # --- registrar build ---
        conn = connect(self.db_path)
        build_id = record_build_start(conn, self.meta)
        conn.close()

        pkgid = package_id(self.meta)
        staging = os.path.join(self.build_root, f"{self.meta.name}-{pkgid}")
        if os.path.exists(staging):
            shutil.rmtree(staging)
        os.makedirs(staging, exist_ok=True)

        env = os.environ.copy()
        env.update(self.meta.environment or {})
        if self.use_fakeroot:
            env["FAKEROOT"] = "1"

        try:
            # 1. resolver dependências
            deps = resolve_dependencies(self.meta, ports_dir="/usr/ports", db_path=self.db_path)
            for dep in deps:
                log_event(self.meta.name, "deps", f"Dependência requerida: {dep}")

            # 2. baixar fontes
            dl = Downloader("/usr/ports", self.cache_dir, dry_run=self.dry_run)
            sources = []
            for s in self.meta.sources:
                sources.append(dl.fetch(s))

            # 3. extrair fonte
            srcdir = os.path.join(self.build_root, f"{self.meta.name}-{self.meta.version}")
            if os.path.exists(srcdir):
                shutil.rmtree(srcdir)
            os.makedirs(srcdir, exist_ok=True)
            for src in sources:
                self._extract(src, srcdir)

            # 4. aplicar patches/hooks pré-configure
            patcher = Patcher(srcdir, env=env, pkg_name=self.meta.name)
            patcher.apply_stage("pre_configure", patches=self.meta.patches, hooks=self.meta.hooks)

            # 5. configure
            if "configure" in self.meta.build:
                self._run(self.meta.build["configure"], cwd=srcdir, env=env, stage="configure")

            # 6. build
            jobs = env.get("MAKEJOBS", "4")
            build_cmd = self.meta.build.get("make", f"make -j{jobs}")
            self._run(build_cmd, cwd=srcdir, env=env, stage="build")

            # 7. hooks pós-build
            patcher.apply_stage("post_build", hooks=self.meta.hooks)

            # 8. instalar em staging
            install_cmd = self.meta.build.get("install", f"make install DESTDIR={staging}")
            self._run(install_cmd, cwd=srcdir, env=env, stage="install")

            # 9. hooks pós-install
            patcher.apply_stage("post_install", hooks=self.meta.hooks)

            # 10. empacotar
            pkgfile = os.path.join(self.pkg_cache, f"{self.meta.name}-{self.meta.version}.tar.xz")
            if not self.dry_run:
                with tarfile.open(pkgfile, "w:xz") as tf:
                    tf.add(staging, arcname="/")

            # 11. instalar no root, se solicitado
            if dir_install:
                installer = Installer(db_path=self.db_path, dry_run=self.dry_run, root=dir_install)
                installer.install(pkgfile, self.meta)

            # finalizar build
            conn = connect(self.db_path)
            record_build_finish(conn, build_id, "success")
            conn.close()
            return {"status": "success", "pkgfile": pkgfile, "build_id": build_id}

        except Exception as e:
            conn = connect(self.db_path)
            record_build_finish(conn, build_id, f"failed: {e}")
            conn.close()
            raise
        finally:
            if os.path.exists(staging):
                shutil.rmtree(staging)
