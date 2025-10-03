"""
builder.py

Módulo responsável por orquestrar o processo de construção de pacotes a partir de metafiles.

Funcionalidades:
- Executar pipeline de fases: fetch, extract, build, install, package, clean.
- Suporte a hooks definidos no metafile (pre/post para cada fase).
- Uso de SandboxSession para segurança.
- Integração com downloader para buscar fontes.
- Registro detalhado e colorido de logs.
"""
from __future__ import annotations
import os
import tarfile
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List

from ports_manager_initial_modules import setup_logging, MetaFile
from downloader import download_sources_from_metafile
from sandbox import SandboxSession, SandboxError

logger = setup_logging('pmgr_builder', log_dir=Path('./logs'))


class BuildError(Exception):
    pass


class Builder:
    def __init__(self, metafile: MetaFile, work_root: Optional[Path] = None):
        self.metafile = metafile
        self.work_root = Path(work_root) if work_root else Path('/tmp/pmgr_build') / metafile.name
        self.src_cache = Path('./cache/sources')
        self.build_dir = self.work_root / 'build'
        self.pkg_dir = self.work_root / 'pkg'
        self.destdir = self.work_root / 'dest'
        for d in [self.build_dir, self.pkg_dir, self.destdir]:
            d.mkdir(parents=True, exist_ok=True)

    def run_hook(self, phase: str, when: str, sandbox: SandboxSession):
        """Executa hooks definidos no metafile."""
        hooks = self.metafile.hooks.get(f'{when}_{phase}', [])
        for h in hooks:
            logger.info('Executando hook %s_%s: %s', when, phase, h)
            sandbox.run(h.split())

    def fetch(self):
        logger.info('==> Fetching sources')
        download_sources_from_metafile(self.metafile, dest_dir=self.src_cache)

    def extract(self):
        logger.info('==> Extracting sources')
        for src in self.metafile.sources:
            fname = Path(src["url"]).name
            fpath = self.src_cache / fname
            if tarfile.is_tarfile(fpath):
                with tarfile.open(fpath, 'r:*') as tf:
                    tf.extractall(self.build_dir)
                    logger.info('Extraído %s para %s', fpath, self.build_dir)
            else:
                shutil.copy(fpath, self.build_dir)

    def build(self, sandbox: SandboxSession):
        logger.info('==> Build phase')
        for cmd in self.metafile.build:
            logger.info('Build step: %s', cmd)
            sandbox.run(cmd.split(), env=self.metafile.environment)

    def install(self, sandbox: SandboxSession):
        logger.info('==> Install phase')
        for cmd in self.metafile.install:
            logger.info('Install step: %s', cmd)
            sandbox.install_with_fakeroot(cmd.split(), env=self.metafile.environment)

    def package(self):
        logger.info('==> Package phase')
        pkgfile = self.pkg_dir / f"{self.metafile.name}-{self.metafile.version}.tar.gz"
        with tarfile.open(pkgfile, 'w:gz') as tf:
            tf.add(self.destdir, arcname="/")
        logger.info('Pacote criado: %s', pkgfile)
        return pkgfile

    def clean(self):
        logger.info('==> Clean phase')
        if self.work_root.exists():
            shutil.rmtree(self.work_root)

    def full_build(self):
        logger.info('==== Iniciando build completo de %s ====', self.metafile.name)
        try:
            self.fetch()
            self.extract()
            with SandboxSession(work_dir=self.work_root) as sandbox:
                self.run_hook('build', 'pre', sandbox)
                self.build(sandbox)
                self.run_hook('build', 'post', sandbox)

                self.run_hook('install', 'pre', sandbox)
                self.install(sandbox)
                self.run_hook('install', 'post', sandbox)

            pkgfile = self.package()
            return pkgfile
        except (SandboxError, Exception) as e:
            logger.error('Erro na construção: %s', e)
            raise BuildError(str(e))
        finally:
            self.clean()


# ---------------- CLI -------------------
if __name__ == '__main__':
    import argparse, tomllib

    p = argparse.ArgumentParser(prog='pmgr_builder', description='Construtor de pacotes pmgr')
    p.add_argument('metafile', type=Path, help='Arquivo TOML do pacote')
    args = p.parse_args()

    with open(args.metafile, 'rb') as f:
        meta = MetaFile.from_dict(tomllib.load(f))
    b = Builder(meta)
    b.full_build()
