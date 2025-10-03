#!/usr/bin/env python3
"""
zeropkg_builder.py

Builder completo para Zeropkg — integrado com downloader, patcher, installer e DB.

Adicionado: helpers para montar/desmontar chroot (/proc, /sys, /dev, /dev/pts)
quando chroot for usado, garantindo ambiente consistente para builds que dependem
de mounts do kernel.

Funcionalidades:
- fetch_sources(meta) -> caminho do arquivo baixado (usa zeropkg_downloader.download_package)
- extract_sources(src_path) -> extrai para workdir de build
- apply_patches_and_hooks(stage) -> usa zeropkg_patcher.Patcher
- configure/build/install -> executa comandos do meta.build (ou defaults)
- faz instalação em staging (DESTDIR) usando fakeroot se solicitado
- empacota staging em .tar.xz no pkg_cache
- registra build no DB (record_build_start/finish) e retorna build_id
- suporta chroot: prepara mountpoints e limpa no final
- supports dry_run and logging via log_event
"""

from __future__ import annotations
import os
import shutil
import tarfile
import zipfile
import subprocess
import tempfile
import time
import logging
import json
from typing import List, Optional, Tuple, Dict

from zeropkg_downloader import download_package
from zeropkg_patcher import Patcher, PatchError, HookError
from zeropkg_installer import Installer
from zeropkg_logger import log_event
from zeropkg_db import connect, record_build_start, record_build_finish

logger = logging.getLogger("zeropkg.builder")

# defaults
DEFAULT_CACHE_DIR = "/usr/ports/distfiles"
DEFAULT_PKG_CACHE = "/var/zeropkg/packages"
DEFAULT_BUILD_ROOT = "/var/zeropkg/build"
DEFAULT_STAGING_BASE = "/var/zeropkg/staging"
DEFAULT_LOG_DIR = "/var/log/zeropkg"

# small helpers
def _now_iso():
    return time.strftime("%Y%m%d%H%M%S")

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _is_tar(path: str) -> bool:
    try:
        return tarfile.is_tarfile(path)
    except Exception:
        return False

def _safe_extract_tar(path: str, dest: str) -> None:
    with tarfile.open(path, "r:*") as tar:
        for member in tar.getmembers():
            member_path = os.path.join(dest, member.name)
            abs_dest = os.path.abspath(dest)
            abs_target = os.path.abspath(member_path)
            if not (abs_target == abs_dest or abs_target.startswith(abs_dest + os.sep)):
                raise RuntimeError(f"Arquivo inseguro no tar: {member.name}")
        tar.extractall(dest)

def _extract_archive(path: str, dest: str) -> None:
    if _is_tar(path):
        _safe_extract_tar(path, dest)
    elif path.lower().endswith(".zip"):
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(dest)
    else:
        # fallback: copy single file into dest
        _ensure_dir(dest)
        shutil.copy2(path, os.path.join(dest, os.path.basename(path)))

def shlex_quote(s: str) -> str:
    import shlex
    return shlex.quote(s)

def _run(cmd: List[str], cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None,
         dry_run: bool = False, use_fakeroot: bool = False, chroot: Optional[str] = None) -> Tuple[int,str,str]:
    """
    Executa um comando e retorna (returncode, stdout, stderr).
    - if dry_run: não executa, retorna (0, "", "")
    - if use_fakeroot: prefixa com fakeroot sh -c '...'
    - if chroot provided: executa via chroot <path> /bin/sh -c '...'
    """
    if dry_run:
        logger.debug("[dry-run] " + " ".join(cmd))
        return 0, "", ""

    # build shell command string
    shell = " ".join([shlex_quote(x) for x in cmd])
    if use_fakeroot:
        full_cmd = ["fakeroot", "sh", "-c", shell]
    elif chroot:
        # execute in chroot using shell
        full_cmd = ["chroot", chroot, "sh", "-c", shell]
    else:
        full_cmd = cmd

    try:
        proc = subprocess.run(full_cmd, cwd=cwd, env=env, check=False,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as e:
        return 1, "", str(e)

class BuildError(Exception):
    pass

class Builder:
    def __init__(self,
                 meta,
                 cache_dir: str = DEFAULT_CACHE_DIR,
                 pkg_cache: str = DEFAULT_PKG_CACHE,
                 build_root: str = DEFAULT_BUILD_ROOT,
                 staging_base: str = DEFAULT_STAGING_BASE,
                 log_dir: str = DEFAULT_LOG_DIR,
                 dry_run: bool = False,
                 use_fakeroot: bool = False,
                 chroot: Optional[str] = None,
                 db_path: str = "/var/lib/zeropkg/installed.sqlite3"):
        """
        meta: PackageMeta
        cache_dir: where distfiles are cached (downloader)
        pkg_cache: where pkg tarballs are stored
        build_root: where to create workdir
        staging_base: base for DESTDIR staging
        log_dir: where to store build logs
        dry_run: simulate only
        use_fakeroot: run install step inside fakeroot to preserve uids
        chroot: optional chroot path to run commands inside
        db_path: path to sqlite DB for recording builds
        """
        self.meta = meta
        self.cache_dir = cache_dir
        self.pkg_cache = pkg_cache
        self.build_root = build_root
        self.staging_base = staging_base
        self.log_dir = log_dir
        self.dry_run = dry_run
        self.use_fakeroot = use_fakeroot
        self.chroot = chroot
        self.db_path = db_path

        # normalize
        _ensure_dir(self.cache_dir)
        _ensure_dir(self.pkg_cache)
        _ensure_dir(self.build_root)
        _ensure_dir(self.staging_base)
        _ensure_dir(self.log_dir)

        # work dirs
        base = f"{self.meta.name}-{self.meta.version}-{_now_iso()}"
        self.workdir = os.path.join(self.build_root, base)
        self.staging = os.path.join(self.staging_base, base)

        # prepare environment for commands
        self.env = os.environ.copy()
        env_meta = getattr(self.meta, "environment", {}) or {}
        for k,v in env_meta.items():
            self.env[str(k)] = str(v)

        # logging file per build
        self.build_log = os.path.join(self.log_dir, f"{self.meta.name}-{self.meta.version}.log")

    # -------------------------
    # chroot helpers
    # -------------------------
    def prepare_chroot(self, chroot_path: str) -> None:
        """
        Prepara montagens necessárias no chroot:
        - bind /dev -> chroot/dev
        - mount -t proc proc -> chroot/proc
        - mount -t sysfs sys -> chroot/sys
        - mount -t devpts devpts -> chroot/dev/pts
        Se dry_run: apenas loga.
        """
        if not chroot_path:
            return
        log_event(self.meta.name, "builder", f"Preparando chroot mounts em {chroot_path}")
        if self.dry_run:
            log_event(self.meta.name, "builder", f"[dry-run] mount --bind /dev -> {chroot_path}/dev")
            log_event(self.meta.name, "builder", f"[dry-run] mount -t proc proc -> {chroot_path}/proc")
            log_event(self.meta.name, "builder", f"[dry-run] mount -t sysfs sys -> {chroot_path}/sys")
            log_event(self.meta.name, "builder", f"[dry-run] mount -t devpts devpts -> {chroot_path}/dev/pts")
            return

        try:
            # ensure directories exist
            for d in ("dev", "proc", "sys", "dev/pts"):
                path = os.path.join(chroot_path, d)
                os.makedirs(path, exist_ok=True)

            # bind /dev
            subprocess.run(["mount", "--bind", "/dev", os.path.join(chroot_path, "dev")], check=True)
            # mount proc
            subprocess.run(["mount", "-t", "proc", "proc", os.path.join(chroot_path, "proc")], check=True)
            # mount sysfs
            subprocess.run(["mount", "-t", "sysfs", "sys", os.path.join(chroot_path, "sys")], check=True)
            # mount devpts
            subprocess.run(["mount", "-t", "devpts", "devpts", os.path.join(chroot_path, "dev/pts")], check=True)
            log_event(self.meta.name, "builder", "Chroot mounts prontos")
        except subprocess.CalledProcessError as e:
            log_event(self.meta.name, "builder", f"Erro ao preparar chroot mounts: {e}",)
            raise BuildError(f"Erro ao preparar chroot mounts: {e}") from e

    def cleanup_chroot(self, chroot_path: str) -> None:
        """
        Desmonta os mountpoints adicionados por prepare_chroot na ordem reversa.
        Ignora erros (best-effort).
        """
        if not chroot_path:
            return
        log_event(self.meta.name, "builder", f"Limpando chroot mounts em {chroot_path}")
        if self.dry_run:
            log_event(self.meta.name, "builder", f"[dry-run] umount {chroot_path}/dev/pts")
            log_event(self.meta.name, "builder", f"[dry-run] umount {chroot_path}/sys")
            log_event(self.meta.name, "builder", f"[dry-run] umount {chroot_path}/proc")
            log_event(self.meta.name, "builder", f"[dry-run] umount {chroot_path}/dev")
            return

        # attempt to unmount in reverse order; ignore failures
        mpoints = [
            os.path.join(chroot_path, "dev/pts"),
            os.path.join(chroot_path, "sys"),
            os.path.join(chroot_path, "proc"),
            os.path.join(chroot_path, "dev"),
        ]
        for mp in mpoints:
            try:
                subprocess.run(["umount", "-l", mp], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                # ignore
                pass
        log_event(self.meta.name, "builder", "Chroot mounts limpos (ou tentativa feita)")

    # -------------
    # high level
    # -------------
    def fetch_sources(self) -> str:
        """Usa zeropkg_downloader para obter o arquivo principal (prioritário)."""
        log_event(self.meta.name, "builder", "fetch_sources: iniciando download das fontes")
        path = download_package(self.meta, cache_dir=self.cache_dir, prefer_existing=True, verbose=False)
        log_event(self.meta.name, "builder", f"fetch_sources: fonte em {path}")
        return path

    def extract_sources(self, src_path: Optional[str] = None) -> str:
        """Extrai src_path para workdir e retorna path do diretório de origem (src_dir)."""
        if src_path is None:
            raise BuildError("extract_sources: src_path obrigatório")
        log_event(self.meta.name, "builder", f"extract_sources: {src_path} -> {self.workdir}")
        if self.dry_run:
            return self.workdir
        # ensure clean workdir
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir, ignore_errors=True)
        os.makedirs(self.workdir, exist_ok=True)
        try:
            _extract_archive(src_path, self.workdir)
        except Exception as e:
            raise BuildError(f"Falha ao extrair fontes: {e}") from e
        # if extraction created a single top-level folder, set src_dir
        entries = [e for e in os.listdir(self.workdir) if not e.startswith('.')]
        if len(entries) == 1 and os.path.isdir(os.path.join(self.workdir, entries[0])):
            src_dir = os.path.join(self.workdir, entries[0])
        else:
            src_dir = self.workdir
        log_event(self.meta.name, "builder", f"extract_sources: origem = {src_dir}")
        return src_dir

    def _apply_patches_and_hooks(self, stage: str, patches: Optional[dict], hooks: Optional[dict]):
        """Aplica patches e hooks para um estágio específico."""
        log_event(self.meta.name, "builder", f"apply_stage: {stage}")
        patcher = Patcher(workdir=self.workdir, env=self.env, pkg_name=self.meta.name)
        try:
            patcher.apply_stage(stage, patches or {}, hooks or {})
        except (PatchError, HookError) as e:
            raise BuildError(f"Erro em stage {stage}: {e}") from e

    def _run_cmds(self, cmds: List[str], cwd: Optional[str] = None,
                  use_fakeroot: Optional[bool] = None) -> None:
        """Executa uma lista de comandos (cada item é string, executado via shell)."""
        if use_fakeroot is None:
            use_fakeroot = self.use_fakeroot
        for cmd in cmds:
            log_event(self.meta.name, "builder", f"run_cmd: {cmd} (cwd={cwd})")
            rc, out, err = _run(["/bin/sh", "-c", cmd], cwd=cwd or self.workdir, env=self.env,
                                dry_run=self.dry_run, use_fakeroot=use_fakeroot, chroot=self.chroot)
            # append output to build log
            try:
                with open(self.build_log, "a") as lf:
                    lf.write(f"\n--- CMD: {cmd} ---\n")
                    lf.write(out or "")
                    lf.write(err or "")
            except Exception:
                pass
            if rc != 0:
                raise BuildError(f"Comando falhou: {cmd}\nstderr: {err}")

    def build(self, dir_install: Optional[str] = None) -> str:
        """
        Processo completo: fetch -> extract -> configure -> build -> install (staging) -> package.
        Retorna caminho do pacote gerado (.tar.xz).

        dir_install: se fornecido, além de gerar o pacote, tentará também
                    instalar o resultado diretamente em dir_install (usado para chroot final).
        """
        conn = None
        build_id = None
        try:
            conn = connect(self.db_path)
            build_id = record_build_start(conn, self.meta)
            log_event(self.meta.name, "builder", f"build_id={build_id} registrado")
        except Exception:
            log_event(self.meta.name, "builder", "Não foi possível registrar build no DB")

        try:
            # If chroot is requested, prepare mounts
            if self.chroot:
                self.prepare_chroot(self.chroot)

            # 1) fetch
            srcpath = self.fetch_sources()

            # 2) extract
            src_dir = self.extract_sources(srcpath)

            # 3) prepare staging
            if not self.dry_run:
                if os.path.exists(self.staging):
                    shutil.rmtree(self.staging, ignore_errors=True)
                os.makedirs(self.staging, exist_ok=True)
            else:
                log_event(self.meta.name, "builder", "[dry-run] preparar staging simulado")

            # 4) patches & pre-configure hooks
            patches = getattr(self.meta, "patches", {}) or {}
            hooks = getattr(self.meta, "hooks", {}) or {}
            self._apply_patches_and_hooks("pre_configure", patches, hooks)

            # 5) configure
            configure_cmds = []
            if getattr(self.meta, "build", None):
                # allow different key names
                if isinstance(self.meta.build.get("configure", None), list):
                    configure_cmds = self.meta.build.get("configure", [])
                else:
                    configure_cmds = self.meta.build.get("configure_cmds", []) or self.meta.build.get("configure", [])
            if configure_cmds:
                self._run_cmds(configure_cmds, cwd=src_dir)

            # 6) pre_build hooks
            self._apply_patches_and_hooks("pre_build", patches, hooks)

            # 7) build (make)
            build_cmds = []
            if getattr(self.meta, "build", None):
                if self.meta.build.get("build_cmds"):
                    build_cmds = self.meta.build.get("build_cmds")
                elif self.meta.build.get("make"):
                    build_cmds = [self.meta.build.get("make")]
            if not build_cmds:
                build_cmds = ["make -j4"]
            self._run_cmds(build_cmds, cwd=src_dir)

            # 8) post_build hooks
            self._apply_patches_and_hooks("post_build", patches, hooks)

            # 9) install into staging (DESTDIR)
            install_cmds = []
            if getattr(self.meta, "build", None):
                if self.meta.build.get("install_cmds"):
                    install_cmds = self.meta.build.get("install_cmds")
                elif self.meta.build.get("install"):
                    install_cmds = [self.meta.build.get("install")]
            if not install_cmds:
                install_cmds = [f"make DESTDIR={self.staging} install"]

            # determine whether install should use fakeroot: prefer meta.build.fakeroot if present
            install_use_fakeroot = self.use_fakeroot
            if getattr(self.meta, "build", None):
                install_use_fakeroot = bool(self.meta.build.get("fakeroot", install_use_fakeroot))

            # pre_install hooks
            self._apply_patches_and_hooks("pre_install", patches, hooks)

            # run install commands (may use fakeroot or chroot)
            self._run_cmds(install_cmds, cwd=src_dir, use_fakeroot=install_use_fakeroot)

            # post_install hooks
            self._apply_patches_and_hooks("post_install", patches, hooks)

            # 10) package staging into tar.xz
            timestamp = _now_iso()
            pkgname = f"{self.meta.name}-{self.meta.version}.tar.xz"
            pkgpath = os.path.join(self.pkg_cache, pkgname)
            if self.dry_run:
                log_event(self.meta.name, "builder", f"[dry-run] empacotar staging -> {pkgpath}")
            else:
                with tarfile.open(pkgpath, "w:xz") as tar:
                    # add files inside staging with arcname = .
                    tar.add(self.staging, arcname=".")
                log_event(self.meta.name, "builder", f"Pacote gerado: {pkgpath}")

            # 11) optionally install into dir_install (after packaging)
            if dir_install:
                inst = Installer(db_path=self.db_path, dry_run=self.dry_run, root=dir_install, use_fakeroot=install_use_fakeroot)
                if self.dry_run:
                    log_event(self.meta.name, "builder", f"[dry-run] instalar pacote {pkgpath} -> {dir_install}")
                else:
                    inst.install(pkgpath, self.meta, compute_hash=False, run_hooks=True, build_id=build_id)
                    log_event(self.meta.name, "builder", f"Instalado pacote em {dir_install}")

            # success record
            if conn and build_id:
                try:
                    record_build_finish(conn, build_id, status="success", log_path=self.build_log)
                except Exception:
                    pass

            return pkgpath

        except Exception as e:
            if conn and build_id:
                try:
                    record_build_finish(conn, build_id, status="failed", log_path=self.build_log)
                except Exception:
                    pass
            log_event(self.meta.name, "builder", f"Build falhou: {e}")
            raise BuildError(f"Build falhou: {e}") from e

        finally:
            # cleanup staging unless dry_run
            if not self.dry_run:
                try:
                    shutil.rmtree(self.staging, ignore_errors=True)
                except Exception:
                    pass
            else:
                log_event(self.meta.name, "builder", f"[dry-run] staging {self.staging} mantido")
            # always attempt to cleanup chroot mounts if we prepared them
            if self.chroot:
                try:
                    self.cleanup_chroot(self.chroot)
                except Exception:
                    # ignore cleanup errors but log
                    log_event(self.meta.name, "builder", "Erro ao limpar chroot (ignorado)")
