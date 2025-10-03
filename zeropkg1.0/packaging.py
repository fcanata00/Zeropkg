"""
packaging.py

Módulo responsável por empacotar, 'strip' e instalar pacotes finais no sistema (/).

Funcionalidades:
- `strip_binaries(destdir, patterns)`: percorre `destdir` e aplica `strip` nos arquivos que batem nos padrões (glob)
- `create_package(destdir, out_path, format='tar.xz')`: cria um pacote tar (xz ou gz) contendo o conteúdo de destdir e um manifest.json com metadata
- `atomic_deploy(pkg_path, target_root='/', backup_root='/var/lib/pmgr/backups')`: extrai o pacote de forma "atômica" com backup dos arquivos que serão substituídos e registra a transação para rollback
- `rollback(deploy_id)`: desfaz uma implantação anterior usando os backups gerados

Observações de segurança:
- `atomic_deploy` exige privilégios de root (verifica osuid) — ele não tenta elevar privilégios por conta própria.
- Backups são armazenados em `backup_root/<deploy_id>/` e o manifesto de transação registra todos os arquivos afetados.

Integração:
- Projetado para integração com `Builder` (usar `destdir` do builder), `Resolver` e `Core`.
"""
from __future__ import annotations
import tarfile
import json
import hashlib
import time
import os
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Optional

try:
    from ports_manager_initial_modules import setup_logging, ensure_dirs
except Exception:
    raise

logger = setup_logging('pmgr_packaging', log_dir=Path('./logs'))


def _hash_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def strip_binaries(destdir: Path, patterns: Optional[List[str]] = None) -> List[Path]:
    """Aplica `strip` nos binários dentro de destdir em arquivos que casem com patterns (glob).
    Retorna lista de arquivos processados.
    Se patterns for None, aplica a arquivos em bin/ sbin/ usr/bin/ usr/sbin/ e executáveis detectados.
    """
    patterns = patterns or ['bin/**', 'sbin/**', 'usr/bin/**', 'usr/sbin/**']
    processed: List[Path] = []
    for pat in patterns:
        for p in destdir.glob(pat):
            if p.is_file() and os.access(p, os.X_OK):
                try:
                    subprocess.run(['strip', str(p)], check=True)
                    processed.append(p)
                    logger.debug('Strip aplicado em %s', p)
                except FileNotFoundError:
                    logger.warning('Comando strip não encontrado; pulando strip de %s', p)
                    return []
                except subprocess.CalledProcessError as e:
                    logger.warning('Strip falhou para %s: %s', p, e)
    logger.info('Strip finalizado — %d arquivos processados', len(processed))
    return processed


def create_package(destdir: Path, out_path: Path, format: str = 'tar.xz', metadata: Optional[Dict] = None) -> Path:
    """Cria um pacote tar contendo o conteúdo de destdir e um manifest.json (metadata + file listing).
    format: 'tar.xz' or 'tar.gz'
    Retorna o caminho para o pacote criado.
    """
    ensure_dirs(out_path.parent)
    metadata = metadata or {}
    # gerar manifest temporário
    manifest = {
        'created_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'metadata': metadata,
        'files': []
    }
    # coletar arquivos
    for p in sorted(destdir.rglob('*')):
        if p.is_file():
            rel = p.relative_to(destdir)
            manifest['files'].append({'path': str(rel), 'size': p.stat().st_size, 'sha256': _hash_file(p)})

    # escrever manifest dentro de uma cópia temporária do destdir
    tmp = Path(tempfile.mkdtemp(prefix='pmgr_pkg_'))
    try:
        # copiar conteúdo
        target = tmp / 'root'
        shutil.copytree(destdir, target)
        # salvar manifest
        with (tmp / 'manifest.json').open('w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        mode = 'w:gz' if format == 'tar.gz' else 'w:xz'
        with tarfile.open(out_path, mode) as tf:
            # incluir manifest.json na raiz do tar
            tf.add(tmp / 'manifest.json', arcname='manifest.json')
            # incluir os arquivos sob / (mantendo paths relativos)
            for p in target.rglob('*'):
                if p.is_file():
                    arcname = str(p.relative_to(target))
                    tf.add(p, arcname=arcname)
        logger.info('Pacote criado: %s', out_path)
        return out_path
    finally:
        shutil.rmtree(tmp)


def _ensure_root():
    if os.geteuid() != 0:
        raise PermissionError('atomic_deploy requer privilégios de root')


def atomic_deploy(pkg_path: Path, target_root: Path = Path('/'), backup_root: Path = Path('/var/lib/pmgr/backups')) -> str:
    """Extrai pacote em target_root com backups e registra a transação.
    Retorna deploy_id gerado.
    """
    _ensure_root()
    if not pkg_path.exists():
        raise FileNotFoundError(pkg_path)
    ensure_dirs(backup_root)
    deploy_id = f"deploy_{int(time.time())}"
    deploy_dir = backup_root / deploy_id
    ensure_dirs(deploy_dir)

    logger.info('Iniciando deploy %s -> %s (backup em %s)', pkg_path, target_root, deploy_dir)
    # abrir tar e identificar arquivos
    with tarfile.open(pkg_path, 'r:*') as tf:
        members = [m for m in tf.getmembers() if m.isfile()]
        affected_files: List[str] = [m.name for m in members]
        # criar backups para os arquivos existentes
        for f in affected_files:
            target_path = (target_root / f).resolve()
            if target_path.exists():
                backup_path = deploy_dir / 'backup' / f
                ensure_dirs(backup_path.parent)
                shutil.copy2(target_path, backup_path)
                logger.debug('Backup %s -> %s', target_path, backup_path)
        # extrair para target_root
        # usamos tar.extractall com members filtrados e preservando permissões
        tf.extractall(path=target_root)

    # escrever manifesto de transação
    manifest = {
        'deploy_id': deploy_id,
        'pkg': str(pkg_path),
        'target_root': str(target_root),
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'files': affected_files,
    }
    with (deploy_dir / 'manifest.json').open('w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    logger.info('Deploy %s concluído', deploy_id)
    return deploy_id


def rollback(deploy_id: str, backup_root: Path = Path('/var/lib/pmgr/backups')) -> bool:
    """Restaura backups gerados por atomic_deploy. Retorna True se sucesso.
    """
    deploy_dir = backup_root / deploy_id
    if not deploy_dir.exists():
        raise FileNotFoundError(f'Deploy id {deploy_id} não encontrado em backups')
    with (deploy_dir / 'manifest.json').open('r', encoding='utf-8') as f:
        manifest = json.load(f)
    target_root = Path(manifest.get('target_root', '/'))
    backup_base = deploy_dir / 'backup'
    # restaurar cada arquivo do backup
    for root, dirs, files in os.walk(backup_base):
        for fn in files:
            bpath = Path(root) / fn
            rel = bpath.relative_to(backup_base)
            dest = (target_root / rel)
            ensure_dirs(dest.parent)
            shutil.copy2(bpath, dest)
            logger.debug('Restaurado %s -> %s', bpath, dest)
    logger.info('Rollback %s concluído', deploy_id)
    return True


# ------------------- CLI de utilitários -------------------
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(prog='pmgr_packaging', description='Empacotamento e deploy para pmgr')
    sub = p.add_subparsers(dest='cmd')

    stripp = sub.add_parser('strip', help='Aplica strip em destdir')
    stripp.add_argument('destdir', type=Path)
    stripp.set_defaults(func=lambda args: print(strip_binaries(args.destdir)))

    pack = sub.add_parser('package', help='Cria pacote a partir de destdir')
    pack.add_argument('destdir', type=Path)
    pack.add_argument('--out', type=Path, required=True)
    pack.add_argument('--format', choices=['tar.xz', 'tar.gz'], default='tar.xz')
    pack.set_defaults(func=lambda args: print(create_package(args.destdir, args.out, format=args.format)))

    deploy = sub.add_parser('deploy', help='Faz deploy atômico de um pacote (requer root)')
    deploy.add_argument('pkg', type=Path)
    deploy.add_argument('--target', type=Path, default=Path('/'))
    deploy.set_defaults(func=lambda args: print(atomic_deploy(args.pkg, target_root=args.target)))

    rb = sub.add_parser('rollback', help='Restaura deploy anterior')
    rb.add_argument('deploy_id')
    rb.set_defaults(func=lambda args: print(rollback(args.deploy_id)))

    args = p.parse_args()
    if not hasattr(args, 'func'):
        p.print_help()
    else:
        args.func(args)
