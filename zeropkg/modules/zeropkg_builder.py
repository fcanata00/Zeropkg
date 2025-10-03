#!/usr/bin/env python3
"""
zeropkg_builder.py — Builder completo e integrado

Responsabilidades:
- Ler metafile TOML (receita)
- Resolver dependências (via DependencyResolver)
- Baixar fontes / clonar git (via downloader)
- Aplicar patches e hooks (via Patcher)
- Construir (configure/make/check/install)
- Suportar chroot quando recipe.options.chroot == True
- Instalar em staging (DESTDIR) e empacotar (.tar.xz)
- Chamar Installer.install quando solicitado (dir_install ou instalação padrão)
- Registrar início/fim do build no DB
- Suportar fakeroot, dry-run, dir-install, --build-only
"""

import os
import shutil
import tarfile
import subprocess
import logging
from pathlib import Path
from typing import Optional, Dict, Any

from zeropkg_logger import log_event
from zeropkg_toml import load_toml
from zeropkg_patcher import Patcher
from zeropkg_installer import Installer
from zeropkg_deps import DependencyResolver, resolve_and_install
from zeropkg_db import DBManager
from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot

# downloader: support function or class name variations
try:
    # preferred: function download_package(meta, cache_dir)
    from zeropkg_downloader import download_package as _download_package
    def _downloader_fetch(meta, cache_dir, dest=None):
        return _download_package(meta, cache_dir)
except Exception:
    try:
        # alternative: Downloader class with fetch(meta, dest)
        from zeropkg_downloader import Downloader as _Downloader
        def _downloader_fetch(meta, cache_dir, dest=None):
            dl = _Downloader(cache_dir)
            return dl.fetch(meta, dest)
    except Exception:
        # fallback - raise at runtime if used
        def _downloader_fetch(meta, cache_dir, dest=None):
            raise RuntimeError("Nenhum downloader disponível (download_package ou Downloader)")

logger = logging.getLogger("zeropkg.builder")


class BuildError(Exception):
    pass


class Builder:
    def __init__(
        self,
        db_path: str,
        ports_dir: str = "/usr/ports",
        build_root: str = "/var/zeropkg/build",
        cache_dir: str = "/usr/ports/distfiles",
        packages_dir: str = "/var/zeropkg/packages",
    ):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.build_root = Path(build_root)
        self.cache_dir = Path(cache_dir)
        self.packages_dir = Path(packages_dir)
        self.db = DBManager(db_path)

        # ensure directories
        self.build_root.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.packages_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # helpers
    # -------------------------
    def _load_meta(self, target: str) -> Dict[str, Any]:
        """
        Carrega a receita TOML. target pode ser 'pkgname' (procura no ports) ou um path.
        """
        # Se target for caminho de arquivo
        if os.path.isfile(target):
            try:
                return load_toml(target)
            except TypeError:
                # some load_toml implementations accept (ports_dir, pkgname)
                raise
        # tentativa flexível: load_toml(ports_dir, pkgname) ou load_toml(path_found)
        try:
            return load_toml(self.ports_dir, target)
        except TypeError:
            # fallback: procurar metafile no ports_dir
            path_pattern = os.path.join(self.ports_dir, "**", f"{target}-*.toml")
            import glob
            matches = glob.glob(path_pattern, recursive=True)
            if not matches:
                raise FileNotFoundError(f"Metafile for {target} not found under {self.ports_dir}")
            return load_toml(matches[0])

    def _run(self, cmd: str, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None,
             chroot_root: Optional[str] = None, dry_run: bool = False):
        """
        Executa um comando no host ou dentro do chroot (usando run_in_chroot).
        Usa subprocess.run para capturar erros (raise on non-zero).
        """
        if not cmd:
            return 0
        log_event("builder", "cmd", f"CMD: {cmd} (cwd={cwd}, chroot={chroot_root}, dry_run={dry_run})")
        if dry_run:
            return 0

        if chroot_root:
            # run_in_chroot deve executar comando no chroot e retornar exit code
            return run_in_chroot(chroot_root, cmd, env=env, cwd=cwd)
        else:
            res = subprocess.run(cmd, shell=True, cwd=cwd, env=env)
            if res.returncode != 0:
                raise BuildError(f"Command failed ({res.returncode}): {cmd}")
            return res.returncode

    def _extract_archive_to(self, archive_path: str, dest: str):
        """
        Extrai tarballs e outros formatos suportados para 'dest'.
        """
        archive_path = str(archive_path)
        if archive_path.endswith((".tar.gz", ".tgz", ".tar.xz", ".tar.bz2", ".tar")):
            with tarfile.open(archive_path, "r:*") as tf:
                tf.extractall(dest)
        else:
            # Se for diretório (p.ex. git clone retornou pasta) ou outro tipo, copie
            if os.path.isdir(archive_path):
                shutil.copytree(archive_path, dest, dirs_exist_ok=True)
            else:
                # fallback: copiar
                shutil.copy2(archive_path, dest)

    # -------------------------
    # build principal
    # -------------------------
    def build(self, target: str, args, dir_install: Optional[str] = None):
        """
        Build público:
          - target: nome do pacote (ex: "gcc-13.2.0" ou "binutils-2.41-pass1")
          - args: objeto de args do CLI (deve conter: dry_run, root, fakeroot, build_root, cache_dir, packages_dir, etc)
          - dir_install: se passado, indica instalação alternativa (equivalente a --dir-install)
        """
        # carregar meta
        meta = self._load_meta(target)
        pkgname = meta["package"]["name"]
        pkgversion = meta["package"]["version"]
        pkg_full = f"{pkgname}-{pkgversion}"

        log_event(pkgname, "build.start", f"Starting build for {pkg_full}")

        # registrar início no DB (tentar, sem quebrar se não existir função)
        try:
            if hasattr(self.db, "record_build_start"):
                self.db.record_build_start(pkgname, pkgversion)
        except Exception as e:
            log_event(pkgname, "db", f"record_build_start failed: {e}", level="warning")

        # preparar dirs
        work_dir = self.build_root / pkg_full
        src_dir = work_dir / "src"
        staging_dir = work_dir / "staging"
        work_dir.mkdir(parents=True, exist_ok=True)
        src_dir.mkdir(parents=True, exist_ok=True)
        staging_dir.mkdir(parents=True, exist_ok=True)

        # resolver dependências (instala automaticamente dependências faltantes)
        try:
            resolver = DependencyResolver(self.db_path, self.ports_dir)
            # include build deps? Usamos args para decidir, default True para builds
            resolve_and_install(resolver, pkgname, Builder, Installer, args)
        except Exception as e:
            log_event(pkgname, "deps", f"Dependency resolution/install failed: {e}", level="error")
            raise BuildError(f"Dependency resolution failed: {e}")

        # fazer download / fetch dos sources
        try:
            # downloader pode retornar caminho para tarball ou para diretório (se git clonou)
            tarball_or_dir = _downloader_fetch(meta, str(self.cache_dir), dest=str(src_dir))
            log_event(pkgname, "download", f"Downloaded: {tarball_or_dir}")
        except Exception as e:
            log_event(pkgname, "download", f"Download failed: {e}", level="error")
            raise BuildError(f"Download failed: {e}")

        # Se o downloader colocou um tarball no cache, precisamos extrair em src_dir
        # Se o retorno for um arquivo dentro cache_dir, extrair nele
        try:
            # se tarball_or_dir aponta para um arquivo tarball, extraia para work_dir/build (um dir de build)
            if isinstance(tarball_or_dir, str) and os.path.isfile(tarball_or_dir):
                # extrair num build subdir
                self._extract_archive_to(tarball_or_dir, str(src_dir))
            # se for diretório, assume o código já está lá
        except Exception as e:
            log_event(pkgname, "extract", f"Extraction failed: {e}", level="error")
            raise BuildError(f"Extract failed: {e}")

        # tentar descobrir diretório do source (primeiro e único subdir)
        children = [p for p in src_dir.iterdir() if p.is_dir()]
        if len(children) == 1:
            src_root = str(children[0])
        else:
            # se múltiplos ou nenhum, usar src_dir direto (receitas que usam in-tree build podem esperar isso)
            src_root = str(src_dir)

        # aplicar patches e hooks pre_configure
        try:
            patcher = Patcher()
            # executar pre_configure hooks via patcher.apply_stage se disponível
            try:
                patcher.apply_stage("pre_configure", src_root, meta)
            except TypeError:
                # assinatura alternativa
                patcher.apply_stage("pre_configure", src_root)
            # aplicar patches listados
            for p in meta.get("patches", {}).get("files", []):
                try:
                    patcher.apply_patch(src_root, p)
                except Exception as e:
                    log_event(pkgname, "patch", f"Applying patch {p} failed: {e}", level="warning")
            try:
                patcher.apply_stage("post_configure", src_root, meta)
            except TypeError:
                patcher.apply_stage("post_configure", src_root)
        except Exception as e:
            log_event(pkgname, "patch", f"Patcher stage failed: {e}", level="error")
            raise BuildError(f"Patcher failed: {e}")

        # construir ambiente (env) a partir do meta['build.env']
        env = os.environ.copy()
        for k, v in (meta.get("build.env") or {}).items():
            env[str(k)] = str(v)

        # decidir se usar chroot com base na receita
        use_chroot = meta.get("options", {}).get("chroot", False)
        chroot_root = getattr(args, "root", None) if use_chroot else None

        # executar configure/make/check/install
        try:
            cfg = meta.get("build", {}).get("configure")
            mk = meta.get("build", {}).get("make")
            chk = meta.get("build", {}).get("check")
            inst_cmd = meta.get("build", {}).get("install")

            # configure
            if cfg:
                self._run(cfg, cwd=src_root, env=env, chroot_root=chroot_root, dry_run=getattr(args, "dry_run", False))

            # make
            if mk:
                self._run(mk, cwd=src_root, env=env, chroot_root=chroot_root, dry_run=getattr(args, "dry_run", False))

            # check
            if chk and meta.get("options", {}).get("run_tests", False):
                try:
                    self._run(chk, cwd=src_root, env=env, chroot_root=chroot_root, dry_run=getattr(args, "dry_run", False))
                except Exception as e:
                    # se check falhar, log e continuar se receita permitir (opção)
                    log_event(pkgname, "check", f"Tests failed or errored: {e}", level="warning")
                    if meta.get("options", {}).get("fail_on_test", False):
                        raise

            # install -> para staging via DESTDIR
            if inst_cmd:
                # alguns Makefiles esperam DESTDIR na env, outros como argumento. Fornecer ambos.
                dest_for_cmd = f" DESTDIR={staging_dir}"
                full_inst = inst_cmd + dest_for_cmd
                self._run(full_inst, cwd=src_root, env=env, chroot_root=chroot_root, dry_run=getattr(args, "dry_run", False))
        except BuildError:
            raise
        except Exception as e:
            log_event(pkgname, "build", f"Build steps failed: {e}", level="error")
            raise BuildError(f"Build steps failed: {e}")

        # post-install hooks via patcher
        try:
            try:
                patcher.apply_stage("post_install", staging_dir, meta)
            except TypeError:
                patcher.apply_stage("post_install", staging_dir)
        except Exception as e:
            log_event(pkgname, "hooks", f"post_install hooks error: {e}", level="warning")

        # empacotar staging em tar.xz
        package_filename = self.packages_dir / f"{pkg_full}.tar.xz"
        try:
            if not getattr(args, "dry_run", False):
                with tarfile.open(str(package_filename), "w:xz") as tf:
                    # empacotar conteúdo de staging como root '/'
                    tf.add(str(staging_dir), arcname="/")
            log_event(pkgname, "package", f"Package created: {package_filename}")
        except Exception as e:
            log_event(pkgname, "package", f"Packaging failed: {e}", level="error")
            raise BuildError(f"Packaging failed: {e}")

        # registrar build finish no DB (tentar)
        try:
            if hasattr(self.db, "record_build_finish"):
                self.db.record_build_finish(pkgname, pkgversion, str(package_filename))
        except Exception as e:
            log_event(pkgname, "db", f"record_build_finish failed: {e}", level="warning")

        # se dir_install solicitado, chamar installer.install para instalar do pacote gerado
        try:
            do_install = False
            if dir_install:
                do_install = True
            elif not getattr(args, "build_only", False):
                do_install = True

            if do_install:
                installer = Installer(db_path=self.db_path, ports_dir=self.ports_dir, root=getattr(args, "root", "/"),
                                      dry_run=getattr(args, "dry_run", False), use_fakeroot=getattr(args, "fakeroot", True))
                # prefer passar meta completo para hooks/registro
                installer.install(pkgname, args, pkg_file=str(package_filename), meta=meta, dir_install=dir_install)
        except Exception as e:
            log_event(pkgname, "installer", f"Installer failed after build: {e}", level="error")
            raise BuildError(f"Installer failed: {e}")

        # cleanup: remover staging e workdir (opcional config could control)
        try:
            shutil.rmtree(staging_dir, ignore_errors=True)
            # preserve sources if requested? If meta.options.cleanup_sources == True remove src
            if meta.get("options", {}).get("cleanup_sources", True):
                shutil.rmtree(src_dir, ignore_errors=True)
        except Exception as e:
            log_event(pkgname, "cleanup", f"Cleanup warning: {e}", level="warning")

        # if build used chroot, ensure cleanup_chroot if prepare_chroot was used earlier.
        # Note: installer.install/remove and builder._run use prepare_chroot/run_in_chroot as necessary.
        log_event(pkgname, "build.finish", f"Build finished for {pkg_full}")
        return True
