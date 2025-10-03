"""
core_cli_v2.py

Versão melhorada do CLI principal (`pmgr`) com comandos adicionais e fluxos mais robustos.
Melhorias incluídas:
 - Subcomando `repo` com `add`, `remove`, `list`, `sync`.
 - `scan` para indexar /usr/ports (aceita --ports-dir múltiplos).
 - `search` por nome ou substring.
 - `info` que mostra o metafile completo (ou versões disponíveis).
 - `fetch` para baixar fontes declaradas em um metafile ou chave do índice.
 - `build` capaz de construir a partir de um metafile ou chave do índice, usando campos do metafile ("build" e "install") se presentes.
 - `install` que resolve dependências com `Resolver` e executa builds, marcando pacotes no DB local.
 - `logs` para ver os últimos N bytes de logs do diretório de logs.
 - `history` para listar pacotes marcados instalados no DB local.

Este arquivo foi feito para ser robusto diante de pequenas incompatibilidades entre módulos pré-existentes
(e.g. campos opcionais em MetaFile). Ele tenta usar `mf.raw` quando campos específicos não existem.
"""
from __future__ import annotations
import argparse
import sys
import shutil
import tempfile
import tarfile
import os
import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

from ports_manager_initial_modules import setup_logging, MetaFile, ensure_dirs, CACHE_DIR_DEFAULT
from repo_manager import RepoManager
from downloader import download_sources_from_metafile
from sandbox import SandboxSession
from packaging import create_package, strip_binaries, atomic_deploy
from resolver import Resolver, ResolveError

logger = setup_logging('pmgr', log_dir=Path('./logs'))

# Helpers

def _load_metafile_from_arg(arg: str, rm: RepoManager) -> MetaFile:
    """Aceita uma path para um TOML ou uma chave do índice (name-version) ou um nome (pega versão mais alta).
    Retorna MetaFile carregado.
    """
    p = Path(arg)
    if p.exists():
        logger.debug('Carregando metafile de arquivo %s', p)
        return MetaFile.from_path(p)

    # procurar no índice
    idx = rm.index.get('packages', {})
    # exato key
    if arg in idx:
        path = Path(idx[arg]['path'])
        return MetaFile.from_path(path)
    # buscar por name e pegar versão mais alta
    candidates = [v for v in idx.values() if v.get('name') == arg]
    if candidates:
        # ordenar por versão (heurística: string compare dividido por '.')
        def ver_key(item):
            return tuple(int(x) if x.isdigit() else x for x in str(item.get('version', '')).split('.'))
        best = sorted(candidates, key=ver_key, reverse=True)[0]
        return MetaFile.from_path(Path(best['path']))

    raise FileNotFoundError(f'Metafile ou pacote não encontrado: {arg}')


def _download_and_prepare_sources(mf: MetaFile, work_dir: Path) -> List[Path]:
    cache = CACHE_DIR_DEFAULT
    ensure_dirs(cache)
    results = download_sources_from_metafile(mf, cache_dir=cache)
    downloaded_paths: List[Path] = []
    for r in results:
        if r.get('ok') and r.get('path'):
            downloaded_paths.append(Path(r['path']))
        else:
            logger.warning('Source falhou: %s error=%s', getattr(r.get('source'), 'url', None), r.get('error'))
    # extrair arquivos no work_dir/build
    build_dir = work_dir / 'build'
    ensure_dirs(build_dir)
    for p in downloaded_paths:
        if p.is_file() and tarfile.is_tarfile(p):
            try:
                with tarfile.open(p, 'r:*') as tf:
                    tf.extractall(path=build_dir)
                    logger.info('Extraído %s para %s', p, build_dir)
            except Exception as e:
                logger.error('Falha ao extrair %s: %s', p, e)
                raise
        else:
            # copiar arquivos que não são tar para o build dir
            try:
                dst = build_dir / p.name
                shutil.copy2(p, dst)
                logger.info('Copiado %s -> %s', p, dst)
            except Exception as e:
                logger.error('Falha ao copiar %s: %s', p, e)
                raise
    return [work_dir / 'build']


def _run_command_list(sandbox: SandboxSession, cmds: List[str], env: Optional[Dict[str, str]] = None, use_fakeroot: bool = False):
    for line in cmds:
        if not line:
            continue
        logger.info('Executando: %s', line)
        parts = line if isinstance(line, list) else line.split()
        if use_fakeroot:
            sandbox.install_with_fakeroot(parts, env=env)
        else:
            sandbox.run(parts, env=env)


def cmd_repo(args):
    rm = RepoManager()
    if args.action == 'add':
        rm.add_repo(args.name, args.url, kind=args.kind, branch=args.branch)
        print('Repo adicionado')
    elif args.action == 'remove':
        rm.remove_repo(args.name)
        print('Repo removido')
    elif args.action == 'list':
        for n, rc in rm.repos.items():
            print(n, rc.url, rc.kind, rc.branch)
    elif args.action == 'sync':
        if args.name:
            rm.sync_repo(args.name)
        else:
            rm.sync_all()
        print('Sync concluído')


def cmd_scan(args):
    rm = RepoManager()
    if args.ports_dir:
        rm.ports_dirs = [Path(d) for d in args.ports_dir]
    idx = rm.scan_ports_dirs()
    print('Scan completado — pacotes indexados:', len(idx.get('packages', {})))


def cmd_search(args):
    rm = RepoManager()
    q = args.query.lower()
    matches = []
    for k, v in rm.index.get('packages', {}).items():
        if q in k.lower() or q in (v.get('name') or '').lower() or q in (v.get('summary') or '').lower():
            matches.append((k, v))
    for k, v in sorted(matches):
        print(k, '-', v.get('summary'))


def cmd_info(args):
    rm = RepoManager()
    try:
        mf = _load_metafile_from_arg(args.pkg, rm)
    except Exception as e:
        print('Erro ao localizar metafile:', e)
        return
    # mostrar JSON do metafile raw
    print(json.dumps(mf.raw, indent=2, ensure_ascii=False))


def cmd_fetch(args):
    rm = RepoManager()
    try:
        mf = _load_metafile_from_arg(args.pkg, rm)
    except Exception as e:
        print('Erro:', e)
        return
    cache = Path(args.cache) if args.cache else CACHE_DIR_DEFAULT
    ensure_dirs(cache)
    res = download_sources_from_metafile(mf, cache_dir=cache, parallel=args.parallel)
    print(json.dumps([{'url': getattr(r.get('source'), 'url', None), 'ok': r.get('ok'), 'path': r.get('path'), 'error': r.get('error')} for r in res], indent=2, ensure_ascii=False))


def cmd_build(args):
    rm = RepoManager()
    try:
        mf = _load_metafile_from_arg(args.pkg, rm)
    except Exception as e:
        print('Erro:', e)
        return

    work_root = Path(args.workdir) if args.workdir else Path(tempfile.mkdtemp(prefix=f'pmgr_build_{mf.name}_'))
    ensure_dirs(work_root)
    try:
        # download + extract
        _download_and_prepare_sources(mf, work_root)
        # create sandbox and run build/install
        env = dict(mf.variables or {})
        build_steps = mf.raw.get('build') or mf.raw.get('build_commands') or []
        install_steps = mf.raw.get('install') or mf.raw.get('install_commands') or []
        with SandboxSession(work_dir=work_root) as sb:
            # pre hooks
            for h in mf.hooks.get('pre_build', [] if isinstance(mf.hooks.get('pre_build'), list) else [mf.hooks.get('pre_build')]):
                if h:
                    sb.run(h.split())
            if build_steps:
                _run_command_list(sb, build_steps, env=env, use_fakeroot=False)
            if install_steps:
                _run_command_list(sb, install_steps, env=env, use_fakeroot=True)
            # post hooks
            for h in mf.hooks.get('post_build', [] if isinstance(mf.hooks.get('post_build'), list) else [mf.hooks.get('post_build')]):
                if h:
                    sb.run(h.split())
        # package
        destdir = work_root / 'dest'
        pkgdir = Path(args.outdir) if args.outdir else Path('./pkg')
        ensure_dirs(pkgdir)
        # strip
        strip_binaries(destdir)
        pkg_path = pkgdir / f"{mf.name}-{mf.version}.tar.xz"
        create_package(destdir, pkg_path, format='tar.xz', metadata={'name': mf.name, 'version': mf.version})
        print('Build concluído — pacote criado em', pkg_path)
        if args.deploy:
            print('Executando deploy atômico — é necessário root')
            deploy_id = atomic_deploy(pkg_path, target_root=Path(args.deploy_target))
            print('Deploy ID:', deploy_id)
    except Exception as e:
        logger.error('Build falhou: %s', e)
        print('Erro no build:', e)
    finally:
        if not args.keep:
            try:
                shutil.rmtree(work_root)
            except Exception:
                pass


def cmd_install(args):
    resolver = Resolver()
    rm = RepoManager()
    # aceitar nome único ou lista
    try:
        order = resolver.resolve(args.packages)
    except ResolveError as e:
        print('Erro ao resolver dependências:', e)
        return
    print('Ordem de instalação:', order)
    for key in order:
        info = rm.index.get('packages', {}).get(key)
        if not info:
            print('Pacote não encontrado no índice:', key)
            continue
        mf = MetaFile.from_path(Path(info['path']))
        # reutilizar cmd_build logic but keep workdir under /tmp and keep artifacts if requested
        tmpdir = Path(tempfile.mkdtemp(prefix=f'pmgr_install_{key}_'))
        try:
            _download_and_prepare_sources(mf, tmpdir)
            env = dict(mf.variables or {})
            build_steps = mf.raw.get('build') or mf.raw.get('build_commands') or []
            install_steps = mf.raw.get('install') or mf.raw.get('install_commands') or []
            with SandboxSession(work_dir=tmpdir) as sb:
                _run_command_list(sb, build_steps, env=env, use_fakeroot=False)
                _run_command_list(sb, install_steps, env=env, use_fakeroot=True)
            # mark installed
            resolver.mark_installed(key, dependencies=sum(mf.dependencies.values(), []), explicit=True)
            print('Instalado:', key)
        except Exception as e:
            print('Erro instalando', key, e)
        finally:
            if not args.keep:
                shutil.rmtree(tmpdir)


def cmd_remove(args):
    resolver = Resolver()
    for pkg in args.packages:
        resolver.mark_removed(pkg)
        print('Removido metadata:', pkg)


def cmd_logs(args):
    logdir = Path('./logs')
    if not logdir.exists():
        print('Diretório de logs não encontrado:', logdir)
        return
    files = sorted(logdir.glob('*.log'), key=lambda p: p.stat().st_mtime, reverse=True)
    if args.file:
        path = Path(args.file)
    elif files:
        path = files[0]
    else:
        print('Nenhum arquivo de log encontrado')
        return
    # tail last N lines
    with path.open('rb') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        to_read = min(size, args.bytes)
        f.seek(size - to_read)
        data = f.read().decode('utf-8', errors='replace')
        print(data)


def cmd_history(args):
    # lê local DB simples do resolver
    from resolver import LOCAL_DB_PATH
    if not LOCAL_DB_PATH.exists():
        print('DB local não encontrado')
        return
    db = json.loads(LOCAL_DB_PATH.read_text(encoding='utf-8'))
    for k, v in db.get('installed', {}).items():
        print(k, 'installed_at=', v.get('installed_at'))


def make_parser():
    p = argparse.ArgumentParser(prog='pmgr', description='Gerenciador de pacotes source-based (v2 CLI)')
    sub = p.add_subparsers(dest='cmd')

    # repo subcommands
    repo_p = sub.add_parser('repo', help='Gerenciar repositórios')
    repo_sub = repo_p.add_subparsers(dest='action')
    r_add = repo_sub.add_parser('add')
    r_add.add_argument('name')
    r_add.add_argument('url')
    r_add.add_argument('--kind', choices=['git', 'fs'], default='git')
    r_add.add_argument('--branch', default=None)
    r_add.set_defaults(func=cmd_repo)
    r_rm = repo_sub.add_parser('remove')
    r_rm.add_argument('name')
    r_rm.set_defaults(func=cmd_repo)
    r_list = repo_sub.add_parser('list')
    r_list.set_defaults(func=cmd_repo)
    r_sync = repo_sub.add_parser('sync')
    r_sync.add_argument('--name', default=None)
    r_sync.set_defaults(func=cmd_repo)

    scan = sub.add_parser('scan', help='Escanear diretórios de ports e indexar')
    scan.add_argument('--ports-dir', action='append')
    scan.set_defaults(func=cmd_scan)

    search = sub.add_parser('search', help='Procurar pacotes no índice')
    search.add_argument('query')
    search.set_defaults(func=cmd_search)

    info = sub.add_parser('info', help='Mostrar metafile/raw info de um pacote (key ou path ou name)')
    info.add_argument('pkg')
    info.set_defaults(func=cmd_info)

    fetch = sub.add_parser('fetch', help='Baixar sources de um metafile (key, name ou path)')
    fetch.add_argument('pkg')
    fetch.add_argument('--cache', default=None)
    fetch.add_argument('--parallel', default=4, type=int)
    fetch.set_defaults(func=cmd_fetch)

    build = sub.add_parser('build', help='Construir pacote (metafile key/name/path)')
    build.add_argument('pkg')
    build.add_argument('--workdir', default=None)
    build.add_argument('--outdir', default=None)
    build.add_argument('--keep', action='store_true')
    build.add_argument('--deploy', action='store_true')
    build.add_argument('--deploy-target', default='/')
    build.set_defaults(func=cmd_build)

    install = sub.add_parser('install', help='Instalar pacotes (resolve + build)')
    install.add_argument('packages', nargs='+')
    install.add_argument('--keep', action='store_true')
    install.set_defaults(func=cmd_install)

    remove = sub.add_parser('remove', help='Remover marcação de pacote instalado')
    remove.add_argument('packages', nargs='+')
    remove.set_defaults(func=cmd_remove)

    logs = sub.add_parser('logs', help='Mostrar logs recentes')
    logs.add_argument('--file', default=None)
    logs.add_argument('--bytes', default=32768, type=int)
    logs.set_defaults(func=cmd_logs)

    hist = sub.add_parser('history', help='Mostrar histórico de instalações')
    hist.set_defaults(func=cmd_history)

    return p


def main(argv: Optional[List[str]] = None):
    p = make_parser()
    args = p.parse_args(argv)
    if not hasattr(args, 'func'):
        p.print_help()
        return
    args.func(args)


if __name__ == '__main__':
    main()
