# builder.py
"""
Builder compatível com os módulos do projeto pmgr.

Responsabilidades:
 - baixar fontes com downloader.download_sources_from_metafile()
 - extrair/organizar fontes no build_dir
 - executar hooks (pre_build, post_build, pre_install, post_install)
 - executar comandos de build dentro do SandboxSession
 - executar instalação para DESTDIR via fakeroot
 - aplicar strip e empacotar com packaging.create_package
 - retornar path do pacote gerado
"""
from __future__ import annotations
import os
import tarfile
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

from ports_manager_initial_modules import setup_logging, MetaFile, CACHE_DIR_DEFAULT
from downloader import download_sources_from_metafile
from sandbox import SandboxSession, SandboxError
from packaging import strip_binaries, create_package

logger = setup_logging('pmgr_builder', log_dir=Path('./logs'))


class BuildError(Exception):
    pass


class Builder:
    def __init__(self,
                 metafile: MetaFile,
                 work_root: Optional[Path] = None,
                 cache_dir: Optional[Path] = None):
        self.metafile = metafile
        # work_root default is a unique tmp dir per package under /tmp
        if work_root:
            self.work_root = Path(work_root)
        else:
            self.work_root = Path(tempfile.mkdtemp(prefix=f'pmgr_build_{metafile.name}_'))
        self.cache_dir = Path(cache_dir) if cache_dir else Path(CACHE_DIR_DEFAULT)
        self.src_cache = self.cache_dir / 'sources'
        self.build_dir = self.work_root / 'build'
        self.destdir = self.work_root / 'dest'
        self.pkg_dir = self.work_root / 'pkg'
        for d in [self.src_cache, self.build_dir, self.destdir, self.pkg_dir]:
            d.mkdir(parents=True, exist_ok=True)

    # ---------- hooks ----------
    def _get_hooks_for(self, phase: str, when: str) -> List[str]:
        """
        Retorna lista de hooks para names como 'pre_build' ou 'post_install'.
        Hooks no metafile podem ser string ou lista.
        """
        key = f"{when}_{phase}"
        raw = self.metafile.hooks.get(key)
        if not raw:
            return []
        if isinstance(raw, str):
            return [raw]
        if isinstance(raw, list):
            return [r for r in raw if r]
        return []

    def run_hooks(self, phase: str, when: str, sandbox: SandboxSession) -> None:
        hooks = self._get_hooks_for(phase, when)
        for h in hooks:
            logger.info("Executando hook %s_%s: %s", when, phase, h)
            # hooks podem ser comandos shell; aqui executamos tokenizados por split
            sandbox.run(h.split(), env=None)

    # ---------- fetch ----------
    def fetch(self, parallel: int = 4) -> List[Dict[str, Any]]:
        """
        Baixa as fontes declaradas no metafile para o cache.
        Retorna a lista de resultados (cada dict do downloader).
        """
        logger.info("Fetching sources for %s", self.metafile.name)
        ensure_dir = self.src_cache  # already created in __init__
        res = download_sources_from_metafile(self.metafile, cache_dir=self.src_cache, parallel=parallel)
        ok_count = sum(1 for r in res if r.get('ok'))
        logger.info("Fetch concluído: %d/%d fontes OK", ok_count, len(res))
        return res

    # ---------- extract ----------
    def extract(self, download_results: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        Extrai tudo para self.build_dir.
        Trata tarballs, diretórios (ex: git clone path) e arquivos simples.
        """
        logger.info("Extracting sources into %s", self.build_dir)
        self.build_dir.mkdir(parents=True, exist_ok=True)
        if download_results is None:
            # fallback: tentar extrair arquivos na cache (pode ser útil em runs manuais)
            candidates = list(self.src_cache.iterdir()) if self.src_cache.exists() else []
            download_results = [{'ok': True, 'path': str(p)} for p in candidates]

        for r in download_results:
            if not r.get('ok'):
                logger.warning("Pulando source com erro: %s", r.get('error'))
                continue
            pstr = r.get('path')
            if not pstr:
                continue
            p = Path(pstr)
            # se p for diretório (ex: git clone), copiamos conteúdos para build_dir
            if p.is_dir():
                # copiar conteúdo (mantendo subdirs)
                for item in sorted(p.iterdir()):
                    dst = self.build_dir / item.name
                    if item.is_dir():
                        shutil.copytree(item, dst, dirs_exist_ok=True)
                    else:
                        shutil.copy2(item, dst)
                logger.info("Conteúdo de repo/cloned dir %s copiado para %s", p, self.build_dir)
                continue
            # se for arquivo e tar detectado
            try:
                if tarfile.is_tarfile(p):
                    with tarfile.open(p, 'r:*') as tf:
                        # extrair preservando estrutura; alguns tarballs têm um top-level dir
                        tf.extractall(path=self.build_dir)
                    logger.info("Extraído %s para %s", p, self.build_dir)
                    continue
            except Exception as e:
                logger.warning("Falha ao extrair tar %s: %s", p, e)
            # caso contrário, copiar o arquivo simples para build_dir
            try:
                dst = self.build_dir / p.name
                shutil.copy2(p, dst)
                logger.info("Arquivo %s copiado para %s", p, dst)
            except Exception as e:
                logger.warning("Falha ao copiar %s para build: %s", p, e)

    # ---------- build steps ----------
    def _gather_commands(self, key_names: List[str]) -> List[str]:
        """
        Retorna lista de comandos procurando por várias chaves no metafile.raw.
        Ex: key_names = ['build', 'build_commands']
        """
        for k in key_names:
            v = self.metafile.raw.get(k)
            if v:
                if isinstance(v, list):
                    return v
                if isinstance(v, str):
                    return [v]
        # fallback: atributo directo se existir
        attr = getattr(self.metafile, 'build', None)
        if isinstance(attr, list):
            return attr
        return []

    def _env_for_build(self) -> Dict[str, str]:
        env = os.environ.copy()
        # metafile.variables expected dict of strings
        if getattr(self.metafile, 'variables', None):
            for k, val in self.metafile.variables.items():
                env[str(k)] = str(val)
        return env

    def build(self, sandbox: SandboxSession) -> None:
        build_cmds = self._gather_commands(['build', 'build_commands'])
        if not build_cmds:
            logger.info("Nenhum comando de build declarado para %s — pulando fase build", self.metafile.name)
            return
        env = self._env_for_build()
        # executar comandos na ordem
        for cmd in build_cmds:
            if not cmd:
                continue
            logger.info("Build cmd: %s", cmd)
            parts = cmd if isinstance(cmd, list) else cmd.split()
            sandbox.run(parts, env=env, check=True)

    def install(self, sandbox: SandboxSession) -> None:
        install_cmds = self._gather_commands(['install', 'install_commands'])
        if not install_cmds:
            logger.info("Nenhum comando de install declarado para %s — pulando fase install", self.metafile.name)
            return
        env = self._env_for_build()
        # instalar para DESTDIR (o SandboxSession já tem self.destdir bindado como /dest)
        for cmd in install_cmds:
            if not cmd:
                continue
            logger.info("Install cmd: %s", cmd)
            parts = cmd if isinstance(cmd, list) else cmd.split()
            # usar fakeroot install
            sandbox.install_with_fakeroot(parts, env=env)

    # ---------- package ----------
    def package(self, strip_patterns: Optional[List[str]] = None, out_format: str = 'tar.xz') -> Path:
        """
        Aplica strip e empacota o conteúdo de destdir.
        Retorna Path para pacote criado.
        """
        # strip binários
        try:
            strip_binaries(self.destdir, patterns=strip_patterns)
        except Exception as e:
            logger.warning("Strip falhou: %s", e)
        # cria pacote no pkg_dir
        out_name = f"{self.metafile.name}-{self.metafile.version}.{ 'tar.xz' if out_format=='tar.xz' else 'tar.gz'}"
        out_path = self.pkg_dir / out_name
        create_package(self.destdir, out_path, format=out_format, metadata={
            'name': self.metafile.name,
            'version': self.metafile.version,
        })
        return out_path

    # ---------- clean ----------
    def clean(self, keep_workdir: bool = False) -> None:
        if keep_workdir:
            logger.info("Mantendo workdir %s (keep_workdir=True)", self.work_root)
            return
        try:
            if self.work_root.exists():
                shutil.rmtree(self.work_root)
                logger.debug("Workdir %s removido", self.work_root)
        except Exception as e:
            logger.warning("Falha ao remover workroot %s: %s", self.work_root, e)

    # ---------- full build flow ----------
    def full_build(self,
                   parallel_fetch: int = 4,
                   strip_patterns: Optional[List[str]] = None,
                   out_format: str = 'tar.xz',
                   keep_workdir: bool = False) -> Path:
        """
        Orquestra build completo:
          fetch -> extract -> sandbox(build/install) -> package -> clean
        Retorna path do pacote criado.
        """
        logger.info("==== Iniciando build completo de %s ====", self.metafile.name)
        download_results = []
        try:
            # 1) fetch
            download_results = self.fetch(parallel=parallel_fetch)
            # 2) extract
            self.extract(download_results)
            # 3) sandboxed build + install -> destdir
            with SandboxSession(work_dir=self.work_root) as sb:
                # executar pre_build hooks
                self.run_hooks('build', 'pre', sb)
                self.build(sb)
                self.run_hooks('build', 'post', sb)

                # pre_install hooks
                self.run_hooks('install', 'pre', sb)
                self.install(sb)
                self.run_hooks('install', 'post', sb)

            # 4) package (strip + tar)
            pkg = self.package(strip_patterns, out_format)
            logger.info("Build finalizado — pacote: %s", pkg)
            return pkg
        except (SandboxError, Exception) as e:
            logger.error("Erro no build de %s: %s", self.metafile.name, e)
            raise BuildError(str(e))
        finally:
            # limpar diretórios temporários, a menos que keep_workdir True
            self.clean(keep_workdir)


# Se executado como script para debug/teste rápido
if __name__ == '__main__':
    import argparse
    import tomllib
    p = argparse.ArgumentParser(prog='pmgr_builder', description='Construtor de pacotes pmgr (compatível)')
    p.add_argument('metafile', type=Path, help='Arquivo TOML do pacote (path ou index key via RepoManager não suportado aqui)')
    p.add_argument('--keep', action='store_true', help='Manter workdir após build')
    args = p.parse_args()

    with open(args.metafile, 'rb') as f:
        mf = MetaFile.from_toml_bytes(f.read())
    b = Builder(mf)
    pkg = b.full_build(keep_workdir=args.keep)
    print("Pacote gerado:", pkg)
