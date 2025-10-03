"""
sandbox.py

Módulo de sandboxing e isolamento para o gerenciador "pmgr".

Funcionalidades:
- Execução de comandos dentro de um sandbox seguro baseado em bubblewrap (bwrap) ou unshare.
- Suporte a fakeroot para instalação temporária em DESTDIR.
- Criação de um ambiente temporário de build com diretórios bind mount (src, build, dest).
- Logging detalhado de cada comando executado.
- API simples: SandboxSession.start(), run(), finish().

Requisitos: precisa que `bubblewrap` (bwrap) e `fakeroot` estejam instalados no sistema.
"""
from __future__ import annotations
import subprocess
import tempfile
import shutil
import os
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    from ports_manager_initial_modules import (
        setup_logging,
        ensure_dirs,
        CACHE_DIR_DEFAULT,
    )
except Exception:
    raise

logger = setup_logging('pmgr_sandbox', log_dir=Path('./logs'))


class SandboxError(Exception):
    pass


class SandboxSession:
    """
    Representa uma sessão de build dentro de um sandbox.
    """
    def __init__(self,
                 work_dir: Optional[Path] = None,
                 use_fakeroot: bool = True,
                 extra_mounts: Optional[List[Tuple[Path, Path]]] = None):
        self.base_tmp = Path(tempfile.mkdtemp(prefix='pmgr_sandbox_'))
        self.work_dir = Path(work_dir) if work_dir else (self.base_tmp / 'work')
        self.build_dir = self.base_tmp / 'build'
        self.destdir = self.base_tmp / 'dest'
        self.use_fakeroot = use_fakeroot
        self.extra_mounts = extra_mounts or []
        ensure_dirs(self.work_dir, self.build_dir, self.destdir)
        logger.debug('Sandbox criado em %s', self.base_tmp)

    def _make_bwrap_cmd(self, cmd: List[str], env: Optional[Dict[str, str]] = None) -> List[str]:
        """Constrói o comando bwrap."""
        bcmd = ['bwrap',
                '--unshare-all',
                '--die-with-parent',
                '--ro-bind', '/usr', '/usr',
                '--ro-bind', '/bin', '/bin',
                '--ro-bind', '/lib', '/lib',
                '--ro-bind', '/lib64', '/lib64',
                '--proc', '/proc',
                '--dev', '/dev',
                '--tmpfs', '/tmp',
                '--bind', str(self.work_dir), '/work',
                '--bind', str(self.build_dir), '/build',
                '--bind', str(self.destdir), '/dest']

        for src, dst in self.extra_mounts:
            bcmd += ['--bind', str(src), str(dst)]

        # Iniciar no diretório de build
        bcmd += ['--chdir', '/build']
        # Executar comando
        if env:
            for k, v in env.items():
                bcmd += ['--setenv', k, v]
        bcmd += ['--'] + cmd
        return bcmd

    def run(self, cmd: List[str], env: Optional[Dict[str, str]] = None, check: bool = True) -> subprocess.CompletedProcess:
        """Executa um comando dentro do sandbox usando bwrap."""
        fullcmd = self._make_bwrap_cmd(cmd, env=env)
        logger.info('Sandbox exec: %s', ' '.join(fullcmd))
        try:
            cp = subprocess.run(fullcmd, check=check, capture_output=True, text=True)
            if cp.stdout:
                logger.debug('stdout: %s', cp.stdout.strip())
            if cp.stderr:
                logger.debug('stderr: %s', cp.stderr.strip())
            return cp
        except subprocess.CalledProcessError as e:
            logger.error('Erro no sandbox run: %s', e)
            raise SandboxError(f'Erro executando {cmd}: {e}')

    def install_with_fakeroot(self, cmd: List[str], env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
        """Executa instalação no DESTDIR com fakeroot."""
        fullcmd = ['fakeroot'] + self._make_bwrap_cmd(cmd, env=env)
        logger.info('Sandbox fakeroot install: %s', ' '.join(fullcmd))
        try:
            cp = subprocess.run(fullcmd, check=True, capture_output=True, text=True)
            if cp.stdout:
                logger.debug('stdout: %s', cp.stdout.strip())
            if cp.stderr:
                logger.debug('stderr: %s', cp.stderr.strip())
            return cp
        except subprocess.CalledProcessError as e:
            logger.error('Erro no fakeroot install: %s', e)
            raise SandboxError(f'Erro executando install: {e}')

    def cleanup(self):
        logger.info('Limpando sandbox %s', self.base_tmp)
        try:
            shutil.rmtree(self.base_tmp)
        except Exception as e:
            logger.warning('Falha ao limpar sandbox %s: %s', self.base_tmp, e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cleanup()


# -------------------- CLI de teste --------------------
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(prog='pmgr_sandbox', description='Sandbox runner para pmgr')
    p.add_argument('command', nargs='+')
    args = p.parse_args()

    with SandboxSession() as s:
        s.run(args.command)
