#!/usr/bin/env python3
"""
zeropkg_installer.py

Installer completo para Zeropkg.

Funcionalidades:
- instalação de pacotes binários (tarballs) em um prefixo (--root)
- suporte a fakeroot para preservar UID/GID (use_fakeroot=True)
- integração com Builder via build_id (gravado no DB)
- registro no banco SQLite (register_package)
- execução de hooks (post_install / post_remove) via Patcher
- dry-run para simulação
- extração segura e cálculo opcional de SHA256 por arquivo
"""

from __future__ import annotations
import os
import tarfile
import tempfile
import shutil
import hashlib
import time
import subprocess
import json
import shlex
from typing import Optional, List, Tuple, Dict

from zeropkg_db import connect, register_package, remove_package, get_package, record_event
from zeropkg_logger import log_event
from zeropkg_patcher import Patcher, HookError

# defaults (ajuste conforme sua instalação)
DEFAULT_DB = "/var/lib/zeropkg/installed.sqlite3"
DEFAULT_STAGING_BASE = "/var/zeropkg/staging"
DEFAULT_PKG_CACHE = "/var/zeropkg/packages"
DEFAULT_FAKEROOT = "fakeroot"

class InstallerError(Exception):
    pass

def _now_tag() -> str:
    return time.strftime("%Y%m%d%H%M%S")

def _safe_extract(tar: tarfile.TarFile, dest: str) -> None:
    """Extrai um tarfile prevenindo path traversal."""
    for member in tar.getmembers():
        member_path = os.path.join(dest, member.name)
        abs_dest = os.path.abspath(dest)
        abs_target = os.path.abspath(member_path)
        if not (abs_target == abs_dest or abs_target.startswith(abs_dest + os.sep)):
            raise InstallerError(f"Arquivo inseguro no tar: {member.name}")
    tar.extractall(dest)

def _calc_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

class Installer:
    def __init__(self,
                 db_path: str = DEFAULT_DB,
                 dry_run: bool = False,
                 root: str = "/",
                 use_fakeroot: bool = False,
                 fakeroot_bin: str = DEFAULT_FAKEROOT):
        """
        db_path: caminho do sqlite DB
        dry_run: se True, simula sem aplicar
        root: prefixo de instalação (ex: "/", "/mnt/lfs")
        use_fakeroot: se True, usa fakeroot para preservar UID/GID
        fakeroot_bin: caminho do binário fakeroot (por padrão "fakeroot")
        """
        self.db_path = db_path
        self.dry_run = dry_run
        # normalizar root (sem trailing slash exceto se for apenas "/")
        self.root = os.path.abspath(root)
        self.use_fakeroot = use_fakeroot
        self.fakeroot_bin = fakeroot_bin

        os.makedirs(DEFAULT_STAGING_BASE, exist_ok=True)
        os.makedirs(DEFAULT_PKG_CACHE, exist_ok=True)

    # -------------------------
    # Helpers para fakeroot
    # -------------------------
    def _run_fakeroot_sh(self, sh_command: str, cwd: Optional[str] = None) -> None:
        """
        Executa uma string shell via fakeroot: fakeroot sh -c '...'
        Levanta InstallerError em caso de falha.
        """
        cmd = [self.fakeroot_bin, "sh", "-c", sh_command]
        try:
            subprocess.run(cmd, cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as e:
            raise InstallerError(f"fakeroot comando falhou: {sh_command}\n{e.stderr}") from e

    def _fakeroot_copy(self, src: str, dst: str) -> None:
        src_q = shlex.quote(src)
        dst_q = shlex.quote(dst)
        self._run_fakeroot_sh(f"cp -a {src_q} {dst_q}")

    def _fakeroot_mkdir(self, path: str) -> None:
        path_q = shlex.quote(path)
        self._run_fakeroot_sh(f"mkdir -p {path_q}")

    def _fakeroot_remove(self, path: str) -> None:
        path_q = shlex.quote(path)
        self._run_fakeroot_sh(f"rm -f {path_q}")

    def _fakeroot_extract(self, tarfile_path: str, dest: str) -> None:
        tar_q = shlex.quote(os.path.abspath(tarfile_path))
        dest_q = shlex.quote(dest)
        self._run_fakeroot_sh(f"mkdir -p {dest_q} && tar -xf {tar_q} -C {dest_q}")

    # -------------------------
    # Instalação
    # -------------------------
    def install(self,
                pkgfile: str,
                meta,
                compute_hash: bool = True,
                run_hooks: bool = True,
                build_id: Optional[int] = None) -> List[str]:
        """
        Instala um tarball pkgfile segundo PackageMeta meta.

        Retorna: lista de caminhos instalados (absolutos relativos ao prefix root).
        """
        pkg_name = getattr(meta, "name", "unknown")
        pkg_ver = getattr(meta, "version", "unknown")
        staging_dir = os.path.join(DEFAULT_STAGING_BASE, f"{pkg_name}-{pkg_ver}-{_now_tag()}")

        log_event(pkg_name, "installer", f"Iniciando instalação {pkg_name}-{pkg_ver} (root={self.root}, fakeroot={self.use_fakeroot})")

        # criar staging
        if not self.dry_run:
            os.makedirs(staging_dir, exist_ok=True)
        else:
            print(f"[dry-run] Criar staging: {staging_dir}")

        # extrair
        try:
            if self.dry_run:
                log_event(pkg_name, "installer", f"[dry-run] extrair {pkgfile} -> {staging_dir}")
            else:
                if self.use_fakeroot:
                    # usar fakeroot para extrair de forma que ownerships simulados sejam aplicados
                    self._fakeroot_extract(pkgfile, staging_dir)
                else:
                    with tarfile.open(pkgfile, "r:*") as tar:
                        _safe_extract(tar, staging_dir)
                log_event(pkg_name, "installer", f"Extração concluída: {staging_dir}")
        except Exception as e:
            raise InstallerError(f"Falha na extração do pacote {pkgfile}: {e}") from e

        # copiar arquivos do staging para o prefix root
        installed_files: List[str] = []
        try:
            for root_dir, dirs, files in os.walk(staging_dir):
                rel_root = os.path.relpath(root_dir, staging_dir)
                if rel_root == ".":
                    rel_root = ""
                target_root = os.path.join(self.root, rel_root) if rel_root else self.root

                if self.use_fakeroot:
                    # criar diretório alvo via fakeroot
                    if self.dry_run:
                        print(f"[dry-run] fakeroot mkdir -p {target_root}")
                    else:
                        self._fakeroot_mkdir(target_root)
                else:
                    if not self.dry_run:
                        os.makedirs(target_root, exist_ok=True)

                for fname in files:
                    src_path = os.path.join(root_dir, fname)
                    dst_path = os.path.join(target_root, fname)

                    if self.dry_run:
                        print(f"[dry-run] copy {src_path} -> {dst_path}")
                    else:
                        if self.use_fakeroot:
                            # cp -a via fakeroot (shell quoting)
                            try:
                                self._fakeroot_copy(src_path, dst_path)
                            except InstallerError:
                                # fallback para operação local se cp falhar
                                shutil.copy2(src_path, dst_path)
                        else:
                            shutil.copy2(src_path, dst_path)
                        installed_files.append(dst_path)
        except Exception as e:
            raise InstallerError(f"Falha ao copiar arquivos para root: {e}") from e

        # calcular hashes
        files_with_hash: List[Tuple[str, Optional[str]]] = []
        for p in installed_files:
            if compute_hash and not self.dry_run:
                try:
                    h = _calc_sha256(p)
                except Exception:
                    h = None
            else:
                h = None
            files_with_hash.append((p, h))

        # registrar no DB
        if not self.dry_run:
            try:
                conn = connect(self.db_path)
                pkgfile_abs = os.path.abspath(pkgfile)
                pkg_id = register_package(conn, meta, pkgfile_abs, files_with_hash, build_id=build_id)
                record_event(conn, "INFO", "installer", f"Pacote instalado: {pkg_name}-{pkg_ver}", {"pkg_id": pkg_id, "build_id": build_id})
                log_event(pkg_name, "installer", f"Registrado no DB (pkg_id={pkg_id}, build_id={build_id})")
            except Exception as e:
                raise InstallerError(f"Erro ao registrar pacote no DB: {e}") from e
        else:
            log_event(pkg_name, "installer", "[dry-run] registro no DB simulado")

        # executar hooks post_install
        if run_hooks:
            hooks = getattr(meta, "hooks", {}) or {}
            if "post_install" in hooks:
                try:
                    patcher = Patcher(workdir=self.root, env=getattr(meta, "environment", {}), pkg_name=pkg_name)
                    if self.dry_run:
                        print(f"[dry-run] executar post_install hooks: {hooks['post_install']}")
                    else:
                        patcher.apply_stage("post_install", {}, {"post_install": hooks["post_install"]})
                        log_event(pkg_name, "installer", "post_install hooks executados")
                except HookError as he:
                    # registrar evento e continuar (política: não reverter automaticamente)
                    log_event(pkg_name, "installer", f"Erro em hook post_install: {he}")

        # limpeza do staging
        if not self.dry_run:
            try:
                shutil.rmtree(staging_dir, ignore_errors=True)
            except Exception:
                pass
        else:
            print(f"[dry-run] staging {staging_dir} mantido para inspeção")

        log_event(pkg_name, "installer", f"Instalação concluída: {pkg_name}-{pkg_ver} ({len(installed_files)} arquivos)")
        return installed_files

    # -------------------------
    # Remoção
    # -------------------------
    def remove(self,
               name: str,
               version: Optional[str] = None,
               run_hooks: bool = True) -> List[str]:
        """
        Remove pacote com base no DB.
        Retorna lista de arquivos que foram (ou seriam) removidos.
        """
        conn = connect(self.db_path)
        pkg = get_package(conn, name, version)
        if not pkg:
            raise InstallerError(f"Pacote não encontrado no DB: {name} {version or ''}")

        pkg_version = pkg.get("version")
        manifest_json = pkg.get("manifest_json")
        hooks = {}
        if manifest_json:
            try:
                mj = json.loads(manifest_json)
                hooks = mj.get("hooks", {}) or {}
            except Exception:
                hooks = {}

        # excluir do DB (remove_package retorna paths registrados)
        try:
            files_to_remove = remove_package(conn, name, pkg_version)
        except Exception as e:
            raise InstallerError(f"Erro ao remover registro do DB: {e}") from e

        removed_files: List[str] = []
        for fpath in files_to_remove:
            if self.dry_run:
                print(f"[dry-run] remover arquivo {fpath}")
            else:
                try:
                    if self.use_fakeroot:
                        # usar fakeroot para remover
                        try:
                            self._fakeroot_remove(fpath)
                        except InstallerError:
                            # fallback: tentar remover localmente
                            if os.path.exists(fpath):
                                os.remove(fpath)
                        else:
                            # assume removido
                            removed_files.append(fpath)
                    else:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                            removed_files.append(fpath)
                except Exception as e:
                    log_event(name, "installer", f"Erro ao remover {fpath}: {e}")

        # tentar remover diretórios vazios ascendentes (respeitando prefix self.root)
        if not self.dry_run:
            for fpath in removed_files:
                d = os.path.dirname(fpath)
                while True:
                    if not d.startswith(self.root):
                        break
                    if d == self.root or d == os.path.sep:
                        break
                    try:
                        if os.listdir(d):
                            break
                        # se fakeroot, tentar com fakeroot
                        if self.use_fakeroot:
                            try:
                                self._run_fakeroot_sh(f"rmdir {shlex.quote(d)}")
                            except InstallerError:
                                break
                        else:
                            os.rmdir(d)
                        d = os.path.dirname(d)
                    except Exception:
                        break

        # executar hooks post_remove
        if run_hooks and "post_remove" in hooks:
            try:
                patcher = Patcher(workdir=self.root, env=os.environ.copy(), pkg_name=name)
                if self.dry_run:
                    print(f"[dry-run] executar post_remove hooks: {hooks['post_remove']}")
                else:
                    patcher.apply_stage("post_remove", {}, {"post_remove": hooks["post_remove"]})
                    log_event(name, "installer", "post_remove hooks executados")
            except HookError as he:
                log_event(name, "installer", f"Erro em hook post_remove: {he}")

        log_event(name, "installer", f"Remoção concluída: {name}:{pkg_version} (arquivos removidos: {len(removed_files)})")
        return removed_files
