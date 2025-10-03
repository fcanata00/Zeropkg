import os
import subprocess
import shutil
from pathlib import Path
from zeropkg_logger import log_event
from zeropkg_downloader import Downloader
from zeropkg_patcher import Patcher
from zeropkg_installer import Installer
from zeropkg_toml import load_toml
from zeropkg_chroot import prepare_chroot, cleanup_chroot, run_in_chroot


class Builder:
    def __init__(self, db_path, ports_dir, build_root, cache_dir, packages_dir):
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.build_root = Path(build_root)
        self.cache_dir = Path(cache_dir)
        self.packages_dir = Path(packages_dir)

    def _run_cmd(self, cmd, env=None, cwd=None, chroot_root=None, dry_run=False):
        """Executa comandos de forma segura, com ou sem chroot"""
        if dry_run:
            log_event("builder", "run_cmd", f"[dry-run] {cmd}")
            return 0

        if chroot_root:
            return run_in_chroot(chroot_root, cmd, env=env, cwd=cwd)
        else:
            return subprocess.call(cmd, shell=True, env=env, cwd=cwd)

    def build(self, target, args, dir_install=None):
        meta = load_toml(self.ports_dir, target)
        pkgname = f"{meta['package']['name']}-{meta['package']['version']}"
        log_event(pkgname, "build", f"Iniciando build de {pkgname}")

        # detectar se precisa chroot
        use_chroot = meta.get("options", {}).get("chroot", False)
        chroot_root = args.root if use_chroot else None

        # diretórios de trabalho
        build_dir = self.build_root / pkgname
        src_dir = build_dir / "src"
        os.makedirs(src_dir, exist_ok=True)

        # preparar sources
        downloader = Downloader(self.cache_dir)
        sources = downloader.fetch(meta["source"], dest=src_dir)

        # aplicar patches se houver
        patcher = Patcher(build_root=build_dir, pkg_name=pkgname)
        patcher.apply_stage("pre_configure", meta.get("hooks", {}))

        # preparar env
        env = os.environ.copy()
        for k, v in meta.get("build.env", {}).items():
            env[k] = v

        # rodar configure
        if "configure" in meta["build"]:
            self._run_cmd(meta["build"]["configure"], env=env, cwd=str(build_dir),
                          chroot_root=chroot_root, dry_run=args.dry_run)

        # rodar make
        if "make" in meta["build"]:
            self._run_cmd(meta["build"]["make"], env=env, cwd=str(build_dir),
                          chroot_root=chroot_root, dry_run=args.dry_run)

        # rodar testes
        if meta.get("options", {}).get("run_tests", False) and "check" in meta["build"]:
            self._run_cmd(meta["build"]["check"], env=env, cwd=str(build_dir),
                          chroot_root=chroot_root, dry_run=args.dry_run)

        # instalar (staging)
        staging_dir = build_dir / "staging"
        os.makedirs(staging_dir, exist_ok=True)
        if "install" in meta["build"]:
            self._run_cmd(meta["build"]["install"] + f" DESTDIR={staging_dir}",
                          env=env, cwd=str(build_dir),
                          chroot_root=chroot_root, dry_run=args.dry_run)

        # hooks pós instalação
        patcher.apply_stage("post_install", meta.get("hooks", {}))

        # integrar com installer
        installer = Installer(db_path=self.db_path,
                              ports_dir=self.ports_dir,
                              root=args.root,
                              dry_run=args.dry_run,
                              use_fakeroot=args.fakeroot)
        installer.install_from_staging(pkgname, staging_dir, meta)

        log_event(pkgname, "build", f"Build concluído: {pkgname}")

        # limpar chroot se usado
        if use_chroot:
            cleanup_chroot(args.root)
