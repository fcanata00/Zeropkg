"""
repo_manager.py

Gerenciador de repositórios e indexador de /usr/ports para o projeto "pmgr".

Dependências: utiliza as funções e classes do arquivo gerado anteriormente
`ports_manager_initial_modules.py` (MetaFile, setup_logging, ensure_dirs, PORTS_DIR_DEFAULT,
CACHE_DIR_DEFAULT). Coloque este arquivo no mesmo diretório do módulo anterior.

Funcionalidades:
- Gerenciar repositórios configurados (git e diretórios locais)
- Sincronizar (clone/pull) repositórios git de forma segura
- Escanear diretórios de ports (por padrão /usr/ports) em busca de metafiles TOML
- Construir um índice local (JSON) com informações básicas extraídas dos metafiles
- Consultas simples: listar pacotes indexados e obter informação do metafile

Este módulo foi desenhado para ser autônomo e reutilizável pelo `core` mais à frente.
"""
from __future__ import annotations
import subprocess
import json
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
import argparse
import tempfile
import time

# Importar utilitários e MetaFile do módulo inicial (criado previamente)
try:
    from ports_manager_initial_modules import (
        setup_logging,
        MetaFile,
        ensure_dirs,
        PORTS_DIR_DEFAULT,
        CACHE_DIR_DEFAULT,
    )
except Exception:
    # Em caso de import falhar (se o arquivo estiver em outro lugar), fornecer mensagens claras
    raise

logger = setup_logging('pmgr_repo', log_dir=Path('./logs'))

DEFAULT_REPOS_DIR = Path.cwd() / 'repos'
INDEX_FILENAME = 'ports_index.json'


class RepoConfig:
    def __init__(self, name: str, url: str, kind: str = 'git', branch: Optional[str] = None):
        self.name = name
        self.url = url
        self.kind = kind  # 'git' or 'fs'
        self.branch = branch or 'master'

    def to_dict(self) -> Dict[str, Any]:
        return {'name': self.name, 'url': self.url, 'kind': self.kind, 'branch': self.branch}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        return cls(name=d['name'], url=d['url'], kind=d.get('kind', 'git'), branch=d.get('branch'))


class RepoManager:
    def __init__(self,
                 repos_dir: Path = DEFAULT_REPOS_DIR,
                 ports_dirs: Optional[List[Path]] = None,
                 cache_dir: Path = CACHE_DIR_DEFAULT):
        self.repos_dir = Path(repos_dir)
        ensure_dirs(self.repos_dir, cache_dir)
        self.cache_dir = Path(cache_dir)
        self.index_path = self.cache_dir / INDEX_FILENAME
        self.repos_cfg_path = self.repos_dir / 'repos.json'
        self.ports_dirs = [Path(d) for d in (ports_dirs or [PORTS_DIR_DEFAULT])]
        self.repos: Dict[str, RepoConfig] = {}
        self.index: Dict[str, Any] = {}
        self._load_repos_config()
        self._load_index()

    # -------------------- repos config --------------------
    def _load_repos_config(self) -> None:
        if self.repos_cfg_path.exists():
            try:
                data = json.loads(self.repos_cfg_path.read_text(encoding='utf-8'))
                for r in data.get('repos', []):
                    rc = RepoConfig.from_dict(r)
                    self.repos[rc.name] = rc
                logger.debug('Loaded repos config: %s', list(self.repos.keys()))
            except Exception as e:
                logger.error('Falha ao carregar repos config: %s', e)
        else:
            # criar arquivo padrão
            self._save_repos_config()

    def _save_repos_config(self) -> None:
        payload = {'repos': [r.to_dict() for r in self.repos.values()]}
        try:
            self.repos_cfg_path.parent.mkdir(parents=True, exist_ok=True)
            self.repos_cfg_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
            logger.debug('Salvou repos config em %s', self.repos_cfg_path)
        except Exception as e:
            logger.error('Erro salvando repos config: %s', e)

    def add_repo(self, name: str, url: str, kind: str = 'git', branch: Optional[str] = None) -> None:
        if name in self.repos:
            raise ValueError(f"Repositório com nome '{name}' já existe")
        rc = RepoConfig(name=name, url=url, kind=kind, branch=branch)
        self.repos[name] = rc
        self._save_repos_config()
        logger.info('Adicionado repo %s -> %s', name, url)

    def remove_repo(self, name: str) -> None:
        if name not in self.repos:
            raise ValueError(f"Repositório '{name}' não encontrado")
        del self.repos[name]
        self._save_repos_config()
        logger.info('Removido repo %s', name)

    # -------------------- git helper --------------------
    def _run_git(self, args: List[str], cwd: Optional[Path] = None, capture: bool = False) -> subprocess.CompletedProcess:
        cmd = ['git'] + args
        logger.debug('Executando git: cwd=%s cmd=%s', cwd, ' '.join(cmd))
        try:
            if capture:
                return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            else:
                return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)
        except subprocess.CalledProcessError as e:
            logger.error('Erro no git: %s', e)
            raise

    def sync_repo(self, name: str, shallow: bool = True) -> Path:
        if name not in self.repos:
            raise ValueError(f"Repositório '{name}' não configurado")
        rc = self.repos[name]
        target = self.repos_dir / rc.name
        ensure_dirs(self.repos_dir)
        if rc.kind == 'fs':
            # Para repositórios locais apenas registramos o path
            logger.info('Repo %s é local (fs) em %s', name, rc.url)
            return Path(rc.url)

        # git repo
        if not target.exists():
            logger.info('Clonando repo %s para %s', rc.url, target)
            args = ['clone', rc.url, str(target)]
            if shallow:
                args = ['clone', '--depth', '1', rc.url, str(target)]
            try:
                self._run_git(args)
            except Exception:
                # tentar clone sem depth caso falhe
                logger.warning('Clone raso falhou; tentando clone completo para %s', rc.url)
                self._run_git(['clone', rc.url, str(target)])
        else:
            logger.info('Atualizando repo %s em %s', rc.url, target)
            # fetch + reset/checkout branch
            try:
                self._run_git(['-C', str(target), 'fetch', '--all', '--tags'])
                self._run_git(['-C', str(target), 'reset', '--hard', f'origin/{rc.branch}'])
                self._run_git(['-C', str(target), 'clean', '-fd'])
            except Exception as e:
                logger.error('Falha ao atualizar repo %s: %s', name, e)
        return target

    def sync_all(self) -> None:
        logger.info('Sincronizando todos os repositórios (%d)', len(self.repos))
        for name in list(self.repos.keys()):
            try:
                self.sync_repo(name)
            except Exception as e:
                logger.error('Erro ao sincronizar repo %s: %s', name, e)

    # -------------------- indexer --------------------
    def _load_index(self) -> None:
        if self.index_path.exists():
            try:
                self.index = json.loads(self.index_path.read_text(encoding='utf-8'))
                logger.debug('Índice carregado (%d pacotes)', len(self.index.get('packages', {})))
            except Exception as e:
                logger.error('Falha ao carregar índice: %s', e)
                self.index = {}
        else:
            self.index = {}

    def _save_index(self) -> None:
        try:
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            self.index_path.write_text(json.dumps(self.index, indent=2, ensure_ascii=False), encoding='utf-8')
            logger.info('Índice salvo em %s', self.index_path)
        except Exception as e:
            logger.error('Erro salvando índice: %s', e)

    def scan_ports_dirs(self, force: bool = False) -> Dict[str, Any]:
        """
        Escaneia os diretórios de ports declarados em busca de arquivos TOML.
        Reconstrói o índice com informações básicas (name, version, path, repo if known).
        """
        start = time.time()
        packages: Dict[str, Any] = {}
        for pd in self.ports_dirs:
            logger.info('Escaneando %s', pd)
            if not pd.exists():
                logger.warning('Diretório não existe: %s', pd)
                continue
            for path in pd.rglob('*.toml'):
                try:
                    mf = MetaFile.from_path(path)
                    ok, issues = mf.validate()
                    pkg_key = f"{mf.name}-{mf.version}"
                    packages[pkg_key] = {
                        'name': mf.name,
                        'version': mf.version,
                        'summary': mf.summary,
                        'license': mf.license,
                        'maintainers': mf.maintainers,
                        'path': str(path),
                        'validated': ok,
                        'issues': issues,
                    }
                except Exception as e:
                    logger.warning('Falha ao parsear %s: %s', path, e)
        self.index = {'generated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'packages': packages}
        self._save_index()
        elapsed = time.time() - start
        logger.info('Scan completo em %.2fs — pacotes indexados: %d', elapsed, len(packages))
        return self.index

    def list_indexed(self) -> List[str]:
        return list(self.index.get('packages', {}).keys())

    def find_package(self, name: str) -> List[Dict[str, Any]]:
        res = []
        for k, v in self.index.get('packages', {}).items():
            if v.get('name') == name:
                res.append(v)
        return res

    def show_package_info(self, pkg_key: str) -> Dict[str, Any]:
        return self.index.get('packages', {}).get(pkg_key, {})


# -------------------- CLI para demonstração --------------------
def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(prog='repo_manager', description='Gerenciador de repositórios e indexador de ports')
    sub = p.add_subparsers(dest='cmd')

    add = sub.add_parser('add', help='Adicionar repositório')
    add.add_argument('name')
    add.add_argument('url')
    add.add_argument('--kind', choices=['git', 'fs'], default='git')
    add.add_argument('--branch', default=None)

    remove = sub.add_parser('remove', help='Remover repositório')
    remove.add_argument('name')

    sync = sub.add_parser('sync', help='Sincronizar repositórios (todos ou um)')
    sync.add_argument('--repo', default=None)

    scan = sub.add_parser('scan', help='Escanear diretórios de ports e gerar índice')
    scan.add_argument('--ports-dir', action='append', default=None, help='Adicionar diretório de ports para escanear (pode repetir)')

    ls = sub.add_parser('list', help='Listar pacotes indexados')

    info = sub.add_parser('info', help='Mostrar info de pacote (usa chave name-version)')
    info.add_argument('pkg_key')

    args = p.parse_args(argv)

    rm = RepoManager()
    if args.cmd == 'add':
        rm.add_repo(args.name, args.url, kind=args.kind, branch=args.branch)
    elif args.cmd == 'remove':
        rm.remove_repo(args.name)
    elif args.cmd == 'sync':
        if args.repo:
            rm.sync_repo(args.repo)
        else:
            rm.sync_all()
    elif args.cmd == 'scan':
        if args.ports_dir:
            rm.ports_dirs = [Path(d) for d in args.ports_dir]
        rm.scan_ports_dirs()
    elif args.cmd == 'list':
        for k in rm.list_indexed():
            print(k)
    elif args.cmd == 'info':
        info = rm.show_package_info(args.pkg_key)
        print(json.dumps(info, indent=2, ensure_ascii=False))
    else:
        p.print_help()


if __name__ == '__main__':
    main()
