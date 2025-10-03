#!/usr/bin/env python3
"""
zeropkg_builder.py - Builder do Zeropkg (integrado com zeropkg_chroot)

Alterações:
- Se self.chroot definido, chama prepare_chroot(self.chroot) antes do build e cleanup depois.
- Se dir_install for fornecido e não for "/", prepara chroot no dir_install antes da instalação.
- Garante cleanup seguro em finally.
"""

import os
import tarfile
import shutil
import subprocess
import logging
from typing import Optional, Dict, Any, List

from zeropkg_downloader import download_package
from zeropkg_patcher import Patcher
from zeropkg_logger import log_event
from zeropkg_db import connect, record_build_start, record_build_finish
from zeropkg_installer import Installer
from zeropkg_deps import resolve_dependencies
from zeropkg_toml import PackageMeta, package_id
from zeropkg_chroot import prepare_chroot, cleanup_chroot, ChrootError

logger = logging.getLogger("zeropkg.builder")


class BuildError(Exception):
    pass


class Builder:
    def __init__(self,
                 meta: PackageMeta,
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

    # -------------------------------------
    # helpers
    # -------------------------------------
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

    # -------------------------------------
    # build principal
    # -------------------------------------
    def build(self, dir_install: Optional[str] = None, parallel_download: bool = False) -> Dict[str, Any]:
        """
        Executa o build completo do pacote.
        Retorna dict com status, caminho do pacote e build_id.
        """
        os.makedirs(self.build_root, exist_ok=True)
        os.makedirs(self.pkg_cache, exist_ok=True)

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

        chroot_prepared = False
        install_chroot_prepared = False

        try:
            # If a chroot is requested for build-time, prepare it
            if self.chroot:
                log_event(self.meta.name, "chroot", f"Preparando chroot de build em {self.chroot}")
                prepare_chroot(self.chroot, copy_resolv=True, dry_run=self.dry_run)
                chroot_prepared = True

            # 1. resolver dependências
            deps = resolve_dependencies(self.meta, ports_dir="/usr/ports", db_path=self.db_path)
            for dep in deps:
                log_event(self.meta.name, "deps", f"Dependência requerida: {dep}")

            # 2. baixar fontes
            sources_info: List[Dict[str, Any]] = download_package(
                self.meta,
                cache_dir=self.cache_dir,
                prefer_existing=True,
                verbose=True,
                parallel=parallel_download
            )

            # 3. extrair fontes
            srcdir = os.path.join(self.build_root, f"{self.meta.name}-{self.meta.version}")
            if os.path.exists(srcdir):
                shutil.rmtree(srcdir)
            os.makedirs(srcdir, exist_ok=True)

            for entry in sources_info:
                src_path = entry["path"]
                log_event(self.meta.name, "fetch", f"Fonte obtida: {src_path} (cache={entry['from_cache']})")
                self._extract(src_path, srcdir)

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

            # 11. instalar no root se solicitado
            if dir_install:
                # if installing into a chroot path, prepare it
                if os.path.abspath(dir_install) != "/":
                    try:
                        prepare_chroot(dir_install, copy_resolv=True, dry_run=self.dry_run)
                        install_chroot_prepared = True
                    except ChrootError as ce:
                        log_event(self.meta.name, "chroot", f"Falha ao preparar chroot para install: {ce}", level="warning")

                installer = Installer(db_path=self.db_path,
                                      dry_run=self.dry_run,
                                      root=dir_install,
                                      use_fakeroot=self.use_fakeroot)
                installer.install(pkgfile, self.meta, hooks=self.meta.hooks, build_id=build_id)

            conn = connect(self.db_path)
            record_build_finish(conn, build_id, "success", log_path=None)
            conn.close()
            return {"status": "success", "pkgfile": pkgfile, "build_id": build_id}

        except Exception as e:
            conn = connect(self.db_path)
            record_build_finish(conn, build_id, f"failed: {e}")
            conn.close()
            raise
        finally:
            # limpar staging
            if os.path.exists(staging):
                shutil.rmtree(staging, ignore_errors=True)

            # limpar chroots preparados
            try:
                if install_chroot_prepared:
                    cleanup_chroot(dir_install, force_lazy=True, dry_run=self.dry_run)
                if chroot_prepared:
                    cleanup_chroot(self.chroot, force_lazy=True, dry_run=self.dry_run)
            except Exception as e:
                log_event(self.meta.name, "chroot", f"Erro cleanup chroot: {e}", level="warning")
