#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_installer.py — Installer completo e integrado

Responsabilidades:
- Instalar pacotes empacotados (.tar.xz) no root (ou chroot) apropriado
- Remover pacotes instalados consultando o DB e respeitando revdeps
- Executar hooks (pre_install, post_install, pre_remove, post_remove)
- Suportar fakeroot, dry-run, chroot, registros no DB
- Funciona com as assinaturas que você já usa:
    Installer.install(name, args, pkg_file=None, meta=None, dir_install=None)
    Installer.remove(name, version=None, hooks=None, force=False)
"""

import os
import shutil
import tarfile
import tempfile
import logging
from pathlib import Path
from typing import Optional, Dict, List

from zeropkg_logger import log_event
from zeropkg_patcher import Patcher
from zeropkg_db import connect, record_install, remove_package, get_package_files  # assume helpers
from zeropkg_chroot import prepare_chroot, cleanup_chroot, ChrootError
from zeropkg_deps import DependencyResolver

logger = logging.getLogger("zeropkg.installer")


class InstallError(Exception):
    pass


class Installer:
    def __init__(
        self,
        db_path: str,
        ports_dir: str = "/usr/ports",
        root: str = "/",
        dry_run: bool = False,
        use_fakeroot: bool = True,
    ):
        """
        db_path: caminho para o sqlite DB do zeropkg
        ports_dir: diretório com metafiles
        root: destino da instalação ("/" ou "/mnt/lfs")
        dry_run: se True, apenas simula ações
        use_fakeroot: se True, usa fakeroot para preservar UID/GID nas cópias
        """
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.root = os.path.abspath(root or "/")
        self.dry_run = bool(dry_run)
        self.use_fakeroot = bool(use_fakeroot)

    # -------------------------
    # Helpers internos
    # -------------------------
    def _is_chroot_needed(self, meta: Optional[Dict]) -> bool:
        """Decide se devemos preparar chroot com base na receita (meta)"""
        if not meta:
            # se não há meta, manter o comportamento conservador:
            # se root != "/" e o usuário indicou instalação em outro root, usamos chroot
            return self.root != "/"
        return bool(meta.get("options", {}).get("chroot", False))

    def _run_hook(self, cmd: str, meta: Optional[Dict], args) -> None:
        """
        Executa um comando de hook.
        Se meta indica chroot e root != "/", executa dentro do chroot.
        Usa dry_run para simular.
        """
        pkgname = meta["package"]["name"] if meta and "package" in meta else "unknown"
        use_chroot = self._is_chroot_needed(meta)
        try:
            if self.dry_run or getattr(args, "dry_run", False):
                log_event(pkgname, "hook", f"[dry-run] {cmd}")
                return

            if use_chroot and self.root != "/":
                # Executa dentro do chroot usando chroot + sh -c
                full = f"chroot {self.root} /usr/bin/env -i /bin/bash -lc \"{cmd}\""
                ret = os.system(full)
                if ret != 0:
                    log_event(pkgname, "hook", f"Hook failed ({cmd}) rc={ret}", level="warning")
            else:
                ret = os.system(cmd)
                if ret != 0:
                    log_event(pkgname, "hook", f"Hook failed ({cmd}) rc={ret}", level="warning")
        except Exception as e:
            log_event(pkgname, "hook", f"Exception executing hook {cmd}: {e}", level="error")

    def _apply_stage_hooks(self, stage: str, meta: Optional[Dict], args) -> None:
        """
        Aplica os hooks definidos no meta para a chave stage (pre_install, post_install, pre_remove, post_remove, ...)
        """
        if not meta:
            return
        hooks = meta.get("hooks", {}) or {}
        entry = hooks.get(stage)
        if not entry:
            return
        if isinstance(entry, str):
            cmds = [entry]
        else:
            cmds = list(entry)
        for cmd in cmds:
            self._run_hook(cmd, meta, args)

    def _copy_from_staging(self, staging_dir: str, args) -> None:
        """
        Copia os arquivos do staging (estrutura de package) para self.root.
        Usa fakeroot se habilitado; em dry-run apenas loga.
        Preserva permissões e timestamps (shutil.copy2).
        """
        pkgname = "installer"
        if self.dry_run or getattr(args, "dry_run", False):
            log_event(pkgname, "install", f"[dry-run] Copiar de {staging_dir} para {self.root}")
            return

        # Andar recursivamente pela árvore de staging e copiar para root
        for root_dir, dirs, files in os.walk(staging_dir):
            rel = os.path.relpath(root_dir, staging_dir)
            if rel == ".":
                rel = ""
            dest_dir = os.path.join(self.root, rel.lstrip("/"))
            os.makedirs(dest_dir, exist_ok=True)
            # copiar arquivos
            for fname in files:
                src = os.path.join(root_dir, fname)
                dst = os.path.join(dest_dir, fname)
                # se fakeroot ativo, usar comando externo que preserva owners (fallback)
                if self.use_fakeroot:
                    # tentativa simples: usar cp -a via fakeroot para preservar UID/GID
                    # Observação: chama comando externo; assume fakeroot instalado
                    cmd = f"fakeroot sh -c 'cp -a --preserve=all \"{src}\" \"{dst}\"' || cp --preserve=mode,timestamps \"{src}\" \"{dst}\""
                    ret = os.system(cmd)
                    if ret != 0:
                        # fallback a shutil
                        shutil.copy2(src, dst)
                else:
                    shutil.copy2(src, dst)
                log_event(fname, "install", f"Installed {dst}")

            # criar diretórios vazios, já criados acima

    # -------------------------
    # Public API: install
    # -------------------------
    def install(
        self,
        name: str,
        args,
        pkg_file: Optional[str] = None,
        meta: Optional[Dict] = None,
        dir_install: Optional[str] = None,
    ) -> bool:
        """
        Instala um pacote no root configurado.
        - name: nome do pacote lógico (p.ex. "gcc" ou "binutils-2.41")
        - args: objeto de args do CLI (para flags dry_run, fakeroot, etc.)
        - pkg_file: caminho para o .tar.xz do pacote. Se None e meta fornecido, procura em /var/zeropkg/packages
        - meta: receita TOML (usada para hooks e registro)
        - dir_install: se fornecido, instala para esse diretório em vez de self.root (modo 'dir-install')
        """
        target_root = dir_install if dir_install else self.root
        target_root = os.path.abspath(target_root)

        pkg_display = f"{name}"
        log_event(pkg_display, "install.start", f"Beginning install of {pkg_display} to {target_root}")

        # decidir se usamos chroot (com base na meta); se dir_install for passado, tratamos como install local (não chroot)
        use_chroot = False
        if dir_install:
            use_chroot = False
        else:
            use_chroot = self._is_chroot_needed(meta)

        chroot_prepared = False
        staging_dir = None

        try:
            # preparar chroot se necessário
            if use_chroot and target_root != "/":
                try:
                    prepare_chroot(target_root, copy_resolv=True, dry_run=self.dry_run or getattr(args, "dry_run", False))
                    chroot_prepared = True
                except ChrootError as ce:
                    log_event(pkg_display, "install", f"prepare_chroot failed: {ce}", level="warning")
                    # se não conseguir preparar chroot, abortar a instalação
                    raise InstallError(f"Failed to prepare chroot: {ce}")

            # executar hooks pre_install
            try:
                self._apply_stage_hooks("pre_install", meta, args)
            except Exception as e:
                log_event(pkg_display, "install", f"pre_install hooks error: {e}", level="warning")

            # localizar pacote se não informado
            if not pkg_file and meta:
                pkg_fullname = f"{meta['package']['name']}-{meta['package']['version']}"
                pkg_file = os.path.join("/var/zeropkg/packages", f"{pkg_fullname}.tar.xz")

            if not pkg_file or not os.path.exists(pkg_file):
                raise InstallError(f"Package file not found: {pkg_file}")

            # criar staging temporário e extrair para lá
            staging_dir = tempfile.mkdtemp(prefix=f"zeropkg-staging-{name}-")
            if self.dry_run or getattr(args, "dry_run", False):
                log_event(pkg_display, "install", f"[dry-run] would extract {pkg_file} to {staging_dir}")
            else:
                try:
                    with tarfile.open(pkg_file, "r:*") as tf:
                        tf.extractall(path=staging_dir)
                except Exception as e:
                    raise InstallError(f"Failed to extract package {pkg_file}: {e}")

            # executar patcher pre/post install no staging (se existir lógica)
            try:
                patcher = Patcher(target_root, pkg_name=name)
                # Alguns patchers podem esperar apply_stage(stage, hooks=..., meta=...), vamos ser permissivos
                try:
                    patcher.apply_stage("pre_install", hooks=meta.get("hooks", {}) if meta else {})
                except TypeError:
                    patcher.apply_stage("pre_install")
            except Exception as e:
                log_event(pkg_display, "install", f"Patcher pre_install warning: {e}", level="warning")

            # copiar arquivos do staging para target_root
            # se dir_install foi solicitado, target_root é dir_install (sem chroot)
            self._copy_from_staging(staging_dir, args)

            # registrar no DB (se meta disponível)
            if not (self.dry_run or getattr(args, "dry_run", False)) and meta:
                try:
                    conn = connect(self.db_path)
                    record_install(conn, meta, pkg_file)
                    conn.close()
                except Exception as e:
                    log_event(pkg_display, "db", f"record_install failed: {e}", level="warning")

            # executar hooks post_install
            try:
                self._apply_stage_hooks("post_install", meta, args)
            except Exception as e:
                log_event(pkg_display, "install", f"post_install hooks error: {e}", level="warning")

            log_event(pkg_display, "install.finish", f"Installation completed: {pkg_display}")
            return True

        except InstallError as ie:
            log_event(pkg_display, "install", f"InstallError: {ie}", level="error")
            raise
        except Exception as e:
            log_event(pkg_display, "install", f"Unhandled exception during install: {e}", level="error")
            raise
        finally:
            # cleanup staging
            if staging_dir:
                try:
                    shutil.rmtree(staging_dir, ignore_errors=True)
                except Exception as e:
                    log_event(pkg_display, "install", f"Failed to cleanup staging: {e}", level="warning")
            # cleanup chroot
            if chroot_prepared:
                try:
                    cleanup_chroot(target_root, force_lazy=True, dry_run=self.dry_run or getattr(args, "dry_run", False))
                except Exception as e:
                    log_event(pkg_display, "install", f"cleanup_chroot failed: {e}", level="warning")

    # -------------------------
    # Public API: remove
    # -------------------------
    def remove(
        self,
        name: str,
        version: Optional[str] = None,
        hooks: Optional[Dict[str, List[str]]] = None,
        force: bool = False,
    ) -> bool:
        """
        Remove um pacote instalado:
        - Consulta o DB para obter a lista de caminhos (remove_package)
        - Verifica dependências reversas via DependencyResolver.reverse_deps
        - Executa hooks pre_remove/post_remove (se houver)
        - Suporta flag 'force' para ignorar revdeps
        - Sempre tenta preparar chroot quando root != "/"
        """
        pkg_display = f"{name}:{version}" if version else name
        log_event(pkg_display, "remove.start", f"Beginning removal of {pkg_display} from {self.root}")

        chroot_prepared = False
        try:
            # preparar chroot sempre que remoção for no root != "/"
            if self.root != "/":
                try:
                    prepare_chroot(self.root, copy_resolv=True, dry_run=self.dry_run)
                    chroot_prepared = True
                except ChrootError as ce:
                    log_event(pkg_display, "remove", f"prepare_chroot failed: {ce}", level="warning")
                    # continuar mesmo sem chroot? aqui preferimos abortar para segurança
                    raise

            # verificar revdeps
            try:
                resolver = DependencyResolver(self.db_path, ports_dir=self.ports_dir)
                revdeps = resolver.reverse_deps(name)
                if revdeps and not force:
                    log_event(pkg_display, "remove", f"Abort: {name} is required by: {', '.join(revdeps)}", level="error")
                    return False
            except Exception as e:
                log_event(pkg_display, "remove", f"reverse_deps check failed: {e}", level="warning")
                # se check revdeps falhar, optar por abortar para segurança
                raise

            # executar hooks pre_remove (prefer hooks param, senão meta hooks não acessível aqui)
            if hooks:
                for cmd in hooks.get("pre_remove", []):
                    self._run_hook(cmd, None, args=type("x", (), {"dry_run": self.dry_run}))

            # pegar caminhos a remover via DB helper remove_package
            try:
                conn = connect(self.db_path)
                # remove_package deve retornar lista de paths a remover (ou registrar e retornar)
                paths = remove_package(conn, name, version)
                conn.close()
            except Exception as e:
                log_event(pkg_display, "remove", f"DB remove_package failed: {e}", level="error")
                raise

            # executar remoção dos paths
            if self.dry_run:
                log_event(pkg_display, "remove", f"[dry-run] would remove paths: {paths}")
            else:
                for relpath in paths:
                    # relpath pode começar com '/', garantir comportamento
                    p = os.path.join(self.root, relpath.lstrip("/"))
                    if os.path.exists(p):
                        try:
                            if os.path.islink(p) or os.path.isfile(p):
                                os.unlink(p)
                            elif os.path.isdir(p):
                                shutil.rmtree(p)
                            log_event(pkg_display, "remove", f"Removed {p}")
                        except Exception as e:
                            log_event(pkg_display, "remove", f"Failed to remove {p}: {e}", level="warning")

                # limpeza de diretórios vazios ascendentes
                for relpath in paths:
                    d = os.path.dirname(os.path.join(self.root, relpath.lstrip("/")))
                    while d and d != self.root and os.path.isdir(d):
                        try:
                            if not os.listdir(d):
                                os.rmdir(d)
                                d = os.path.dirname(d)
                            else:
                                break
                        except Exception:
                            break

            # executar hooks post_remove
            if hooks:
                for cmd in hooks.get("post_remove", []):
                    self._run_hook(cmd, None, args=type("x", (), {"dry_run": self.dry_run}))

            log_event(pkg_display, "remove.finish", f"Removal finished: {pkg_display}")
            return True

        except Exception as e:
            log_event(pkg_display, "remove", f"Exception during remove: {e}", level="error")
            raise
        finally:
            if chroot_prepared:
                try:
                    cleanup_chroot(self.root, force_lazy=True, dry_run=self.dry_run)
                except Exception as e:
                    log_event(pkg_display, "remove", f"cleanup_chroot failed: {e}", level="warning")
