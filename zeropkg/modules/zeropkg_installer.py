#!/usr/bin/env python3
"""
zeropkg_installer.py

Installer completo para Zeropkg — funcionalidade LFS-ready.

Principais features:
- instalar .tar.xz/.tar.gz/.zip em um prefixo (--root)
- usar fakeroot para preservar UID/GID (use_fakeroot=True)
- preparar/desmontar chroot (prepare_chroot / cleanup_chroot)
- integração com DB: register_package / remove_package / get_package / record_event
- executar hooks via Patcher (pre_install, post_install, pre_remove, post_remove)
- extração segura (prevenção path traversal)
- dry_run que simula operações
- calcular sha256 para arquivos instalados (opcional)
"""

from __future__ import annotations
import os
import shutil
import tarfile
import zipfile
import tempfile
import subprocess
import hashlib
import time
import json
import logging
import shlex
from typing import Optional, List, Tuple, Dict

# Importar API do projeto — adapte se o seu módulo tiver outro nome/assinatura
from zeropkg_db import connect, register_package, remove_package, get_package, record_event
from zeropkg_patcher import Patcher, HookError
from zeropkg_logger import log_event

# Defaults (ajuste se quiser)
DEFAULT_DB = "/var/lib/zeropkg/installed.sqlite3"
DEFAULT_STAGING_BASE = "/var/zeropkg/staging"
DEFAULT_PKG_CACHE = "/var/zeropkg/packages"

# configure simple logging fallback
logger = logging.getLogger("zeropkg.installer")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


class InstallerError(Exception):
    pass


def _now_tag() -> str:
    return time.strftime("%Y%m%d%H%M%S")


def _safe_extract_tar(tar_path: str, dest: str) -> None:
    """
    Extrai tar de forma segura prevenindo path traversal.
    Suporta tar.* (gzip/xz) via tarfile.
    """
    with tarfile.open(tar_path, "r:*") as tar:
        for member in tar.getmembers():
            member_path = os.path.join(dest, member.name)
            abs_dest = os.path.abspath(dest)
            abs_target = os.path.abspath(member_path)
            if not (abs_target == abs_dest or abs_target.startswith(abs_dest + os.sep)):
                raise InstallerError(f"Arquivo inseguro no tar: {member.name}")
        tar.extractall(dest)


def _extract_archive_any(path: str, dest: str) -> None:
    """
    Detecta tar/zip/single-file e extrai/copia para dest.
    """
    os.makedirs(dest, exist_ok=True)
    if tarfile.is_tarfile(path):
        _safe_extract_tar(path, dest)
    elif zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(dest)
    else:
        # fallback: copiar arquivo simples
        shutil.copy2(path, os.path.join(dest, os.path.basename(path)))


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
                 fakeroot_bin: str = "fakeroot"):
        """
        db_path: caminho para sqlite DB
        dry_run: se True, apenas simula
        root: prefixo de instalação (ex: '/', '/mnt/lfs')
        use_fakeroot: se True, usa fakeroot para operações que preservam UID/GID
        fakeroot_bin: binário fakeroot (padrão 'fakeroot')
        """
        self.db_path = db_path
        self.dry_run = dry_run
        self.root = os.path.abspath(root)
        self.use_fakeroot = use_fakeroot
        self.fakeroot_bin = fakeroot_bin

        os.makedirs(DEFAULT_STAGING_BASE, exist_ok=True)
        os.makedirs(DEFAULT_PKG_CACHE, exist_ok=True)

    # -------------------------
    # chroot helpers (mount /dev, /proc, /sys, /dev/pts)
    # -------------------------
    def prepare_chroot(self) -> None:
        """
        Prepara mounts dentro do self.root (bind /dev, mount proc/sys/devpts).
        Necessita privilégios (root). Em dry_run apenas loga.
        """
        chroot_path = self.root
        log_event("installer", "installer", f"prepare_chroot: {chroot_path}")
        if self.dry_run:
            log_event("installer", "installer", f"[dry-run] mount --bind /dev -> {chroot_path}/dev")
            log_event("installer", "installer", f"[dry-run] mount -t proc proc -> {chroot_path}/proc")
            log_event("installer", "installer", f"[dry-run] mount -t sysfs sys -> {chroot_path}/sys")
            log_event("installer", "installer", f"[dry-run] mount -t devpts devpts -> {chroot_path}/dev/pts")
            return

        try:
            for d in ("dev", "proc", "sys", "dev/pts"):
                path = os.path.join(chroot_path, d)
                os.makedirs(path, exist_ok=True)

            subprocess.run(["mount", "--bind", "/dev", os.path.join(chroot_path, "dev")], check=True)
            subprocess.run(["mount", "-t", "proc", "proc", os.path.join(chroot_path, "proc")], check=True)
            subprocess.run(["mount", "-t", "sysfs", "sys", os.path.join(chroot_path, "sys")], check=True)
            subprocess.run(["mount", "-t", "devpts", "devpts", os.path.join(chroot_path, "dev/pts")], check=True)
            log_event("installer", "installer", "prepare_chroot: mounts prontos")
        except subprocess.CalledProcessError as e:
            log_event("installer", "installer", f"prepare_chroot erro: {e}")
            raise InstallerError(f"prepare_chroot failed: {e}") from e

    def cleanup_chroot(self) -> None:
        """
        Limpa os mounts criados por prepare_chroot. Best-effort, ignora erros.
        """
        chroot_path = self.root
        log_event("installer", "installer", f"cleanup_chroot: {chroot_path}")
        if self.dry_run:
            log_event("installer", "installer", f"[dry-run] umount {chroot_path}/dev/pts")
            log_event("installer", "installer", f"[dry-run] umount {chroot_path}/sys")
            log_event("installer", "installer", f"[dry-run] umount {chroot_path}/proc")
            log_event("installer", "installer", f"[dry-run] umount {chroot_path}/dev")
            return

        mpoints = [
            os.path.join(chroot_path, "dev/pts"),
            os.path.join(chroot_path, "sys"),
            os.path.join(chroot_path, "proc"),
            os.path.join(chroot_path, "dev"),
        ]
        for mp in mpoints:
            try:
                # lazy umount
                subprocess.run(["umount", "-l", mp], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass
        log_event("installer", "installer", "cleanup_chroot: mounts limpos (tentativa)")

    # -------------------------
    # fakeroot helpers
    # -------------------------
    def _run_fakeroot_sh(self, sh_command: str, cwd: Optional[str] = None) -> None:
        cmd = [self.fakeroot_bin, "sh", "-c", sh_command]
        try:
            subprocess.run(cmd, cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as e:
            raise InstallerError(f"fakeroot command failed: {sh_command}\n{e.stderr}") from e

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

    def _fakeroot_extract(self, archive: str, dest: str) -> None:
        arch_q = shlex.quote(os.path.abspath(archive))
        dest_q = shlex.quote(dest)
        self._run_fakeroot_sh(f"mkdir -p {dest_q} && tar -xf {arch_q} -C {dest_q}")

    # -------------------------
    # Main operations
    # -------------------------
    def install(self,
                pkgfile: str,
                meta,
                compute_hash: bool = True,
                run_hooks: bool = True,
                build_id: Optional[int] = None) -> List[str]:
        """
        Instala um pacote binário (.tar.* ou .zip) no prefixo self.root.
        Retorna lista de arquivos instalados (caminhos absolutos).
        """
        pkg_name = getattr(meta, "name", "unknown")
        pkg_ver = getattr(meta, "version", "unknown")
        staging = os.path.join(DEFAULT_STAGING_BASE, f"{pkg_name}-{pkg_ver}-{_now_tag()}")

        log_event(pkg_name, "installer", f"install: iniciando {pkg_name}-{pkg_ver} root={self.root} fakeroot={self.use_fakeroot} dry_run={self.dry_run}")

        # preparar chroot se necessário
        chroot_was_prepared = False
        if getattr(meta, "hooks", None) or getattr(meta, "build", None):
            # Preferência: se meta indica que precisamos de chroot, deixe o usuário passar chroot via root; aqui só usamos flag root.
            pass

        if self.dry_run:
            log_event(pkg_name, "installer", f"[dry-run] criar staging em {staging}")
        else:
            os.makedirs(staging, exist_ok=True)

        # Se precisamos preparar chroot (usuário quer instalar dentro de um chroot root), faça
        # (decisão: se root != '/', consideramos que é chroot target)
        try:
            if self.root != "/":
                self.prepare_chroot()
                chroot_was_prepared = True

            # 1) extrair para staging
            if self.dry_run:
                log_event(pkg_name, "installer", f"[dry-run] extrair {pkgfile} -> {staging}")
            else:
                if self.use_fakeroot:
                    # usar fakeroot para extrair — preserva owners simulados
                    self._fakeroot_extract(pkgfile, staging)
                else:
                    _extract_archive_any(pkgfile, staging)
                log_event(pkg_name, "installer", f"extraído para {staging}")

            # 2) executar pre_install hooks (no root)
            hooks = getattr(meta, "hooks", {}) or {}
            if run_hooks and "pre_install" in hooks:
                try:
                    if self.dry_run:
                        log_event(pkg_name, "installer", f"[dry-run] pre_install hooks: {hooks['pre_install']}")
                    else:
                        patcher = Patcher(workdir=self.root, env=getattr(meta, "environment", {}), pkg_name=pkg_name)
                        patcher.apply_stage("pre_install", {}, {"pre_install": hooks["pre_install"]})
                        log_event(pkg_name, "installer", "pre_install hooks executados")
                except HookError as he:
                    log_event(pkg_name, "installer", f"Erro em pre_install hook: {he}")

            # 3) copiar arquivos do staging para o prefix root
            installed_files: List[str] = []
            for src_root, dirs, files in os.walk(staging):
                rel_root = os.path.relpath(src_root, staging)
                if rel_root == ".":
                    rel_root = ""
                target_root = os.path.join(self.root, rel_root) if rel_root else self.root

                # criar target_root
                if self.dry_run:
                    log_event(pkg_name, "installer", f"[dry-run] criar dir {target_root}")
                else:
                    if self.use_fakeroot:
                        try:
                            self._fakeroot_mkdir(target_root)
                        except InstallerError:
                            os.makedirs(target_root, exist_ok=True)
                    else:
                        os.makedirs(target_root, exist_ok=True)

                for fname in files:
                    src_path = os.path.join(src_root, fname)
                    dst_path = os.path.join(target_root, fname)
                    if self.dry_run:
                        log_event(pkg_name, "installer", f"[dry-run] copy {src_path} -> {dst_path}")
                    else:
                        try:
                            if self.use_fakeroot:
                                try:
                                    self._fakeroot_copy(src_path, dst_path)
                                except InstallerError:
                                    # fallback para copy local
                                    shutil.copy2(src_path, dst_path)
                            else:
                                shutil.copy2(src_path, dst_path)
                            installed_files.append(dst_path)
                        except PermissionError as pe:
                            raise InstallerError(f"Permissão negada ao copiar {src_path} -> {dst_path}: {pe}") from pe
                        except Exception as e:
                            raise InstallerError(f"Erro ao copiar {src_path} -> {dst_path}: {e}") from e

            # 4) calcular hashes (opcional)
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

            # 5) registrar no DB
            if not self.dry_run:
                conn = connect(self.db_path)
                pkgfile_abs = os.path.abspath(pkgfile)
                pkg_id = register_package(conn, meta, pkgfile_abs, files_with_hash, build_id=build_id)
                try:
                    record_event(conn, "INFO", "installer", f"Pacote instalado: {pkg_name}-{pkg_ver}", {"pkg_id": pkg_id, "build_id": build_id})
                except Exception:
                    # Se record_event não existir na sua versão do DB, ignore
                    pass
                log_event(pkg_name, "installer", f"registrado no DB pkg_id={pkg_id}")
            else:
                log_event(pkg_name, "installer", "[dry-run] registro no DB simulado")

            # 6) executar post_install hooks
            if run_hooks and "post_install" in hooks:
                try:
                    if self.dry_run:
                        log_event(pkg_name, "installer", f"[dry-run] post_install hooks: {hooks['post_install']}")
                    else:
                        patcher = Patcher(workdir=self.root, env=getattr(meta, "environment", {}), pkg_name=pkg_name)
                        patcher.apply_stage("post_install", {}, {"post_install": hooks["post_install"]})
                        log_event(pkg_name, "installer", "post_install hooks executados")
                except HookError as he:
                    log_event(pkg_name, "installer", f"Erro em post_install hook: {he}")

            # 7) limpar staging
            if not self.dry_run:
                try:
                    shutil.rmtree(staging, ignore_errors=True)
                except Exception:
                    pass

            log_event(pkg_name, "installer", f"Instalação finalizada: {pkg_name}-{pkg_ver} (arquivos: {len(installed_files)})")
            return installed_files

        finally:
            # cleanup chroot se preparado
            if chroot_was_prepared:
                try:
                    self.cleanup_chroot()
                except Exception:
                    log_event(pkg_name, "installer", "Erro durante cleanup_chroot (ignorado)")

    # -------------------------
    # Remoção
    # -------------------------
    def remove(self,
               name: str,
               version: Optional[str] = None,
               run_hooks: bool = True) -> List[str]:
        """
        Remove pacote com base no registro no DB. Retorna lista de arquivos removidos.
        """
        pkg = get_package(connect(self.db_path), name, version)
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

        pkg_name = name
        log_event(pkg_name, "installer", f"remove: iniciando remoção {pkg_name}:{pkg_version}")

        # prepare chroot if root != '/'
        chroot_was_prepared = False
        if self.root != "/":
            self.prepare_chroot()
            chroot_was_prepared = True

        try:
            # run pre_remove hooks
            if run_hooks and "pre_remove" in hooks:
                try:
                    if self.dry_run:
                        log_event(pkg_name, "installer", f"[dry-run] pre_remove hooks: {hooks['pre_remove']}")
                    else:
                        patcher = Patcher(workdir=self.root, env=os.environ.copy(), pkg_name=pkg_name)
                        patcher.apply_stage("pre_remove", {}, {"pre_remove": hooks["pre_remove"]})
                except HookError as he:
                    log_event(pkg_name, "installer", f"Erro em pre_remove hook: {he}")

            # remove registro do DB e obter lista de caminhos (remove_package deve implementar isso)
            conn = connect(self.db_path)
            try:
                paths = remove_package(conn, name, pkg_version)
            except Exception as e:
                raise InstallerError(f"Erro ao remover pacote do DB: {e}") from e

            removed: List[str] = []
            for p in paths:
                if self.dry_run:
                    log_event(pkg_name, "installer", f"[dry-run] remover arquivo {p}")
                else:
                    try:
                        if self.use_fakeroot:
                            try:
                                self._fakeroot_remove(p)
                                removed.append(p)
                            except InstallerError:
                                if os.path.exists(p):
                                    os.remove(p)
                                    removed.append(p)
                        else:
                            if os.path.exists(p):
                                os.remove(p)
                                removed.append(p)
                    except Exception as e:
                        log_event(pkg_name, "installer", f"Erro ao remover {p}: {e}")

            # remover diretórios vazios ascendentes (respeitando self.root)
            if not self.dry_run:
                for p in removed:
                    d = os.path.dirname(p)
                    while True:
                        if not d.startswith(self.root):
                            break
                        if d == self.root or d == os.path.sep:
                            break
                        try:
                            if os.listdir(d):
                                break
                            # tentar remover via fakeroot se solicitado
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

            # post_remove hooks
            if run_hooks and "post_remove" in hooks:
                try:
                    if self.dry_run:
                        log_event(pkg_name, "installer", f"[dry-run] post_remove hooks: {hooks['post_remove']}")
                    else:
                        patcher = Patcher(workdir=self.root, env=os.environ.copy(), pkg_name=pkg_name)
                        patcher.apply_stage("post_remove", {}, {"post_remove": hooks["post_remove"]})
                        log_event(pkg_name, "installer", "post_remove hooks executados")
                except HookError as he:
                    log_event(pkg_name, "installer", f"Erro em post_remove hook: {he}")

            log_event(pkg_name, "installer", f"Remoção concluída: {pkg_name}:{pkg_version} (arquivos removidos: {len(removed)})")
            return removed

        finally:
            if chroot_was_prepared:
                try:
                    self.cleanup_chroot()
                except Exception:
                    log_event(pkg_name, "installer", "Erro ao cleanup_chroot (ignorado)")
