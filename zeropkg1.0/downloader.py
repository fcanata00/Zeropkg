"""
downloader.py

Módulo de download para o gerenciador "pmgr". Fornece:
- Funções para baixar múltiplas fontes declaradas em um MetaFile (TOML).
- Suporte para tipos: url/http(s), git
- Verificação de checksum (sha256) quando fornecida
- Cache local compartilhado (CACHE_DIR_DEFAULT)
- Downloads paralelos com ThreadPoolExecutor
- Retentativas, timeouts e logging

Dependências: integra com o arquivo `ports_manager_initial_modules.py` e `repo_manager.py` criados anteriormente.

Observação: este é um módulo funcional pronto para uso. Ao executar, ele fará downloads reais da internet se URLs válidas forem fornecidas.
"""
from __future__ import annotations
import hashlib
import urllib.request
import urllib.error
import shutil
import subprocess
import tempfile
import os
import time
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from ports_manager_initial_modules import (
        setup_logging,
        ensure_dirs,
        CACHE_DIR_DEFAULT,
        MetaFile,
        SourceEntry,
    )
except Exception:
    raise

logger = setup_logging('pmgr_downloader', log_dir=Path('./logs'))

DEFAULT_PARALLEL = 4
DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT = 30  # segundos por conexão


def _compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def _verify_checksum(path: Path, checksum: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Verifica checksum. checksum pode ser 'sha256:hex' ou apenas hex (assume sha256).
    Retorna (ok, computed_hex).
    """
    if not checksum:
        return True, None
    cs = checksum.strip()
    if ':' in cs:
        algo, val = cs.split(':', 1)
        algo = algo.lower()
    else:
        algo = 'sha256'
        val = cs
    val = val.lower()
    if algo != 'sha256':
        logger.warning('Somente sha256 é suportado atualmente (checksum: %s). Ignorando verificação.', checksum)
        return True, None
    computed = _compute_sha256(path)
    ok = (computed.lower() == val.lower())
    return ok, computed


def _atomic_write_from_stream(path: Path, stream) -> None:
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('wb') as f:
        shutil.copyfileobj(stream, f)
    tmp.replace(path)


def download_url(url: str, dest: Path, timeout: int = DOWNLOAD_TIMEOUT, retries: int = DOWNLOAD_RETRIES) -> Path:
    """Baixa uma URL para `dest` (arquivo). Retenta em caso de falha.

    Retorna o Path do arquivo baixado.
    """
    ensure_dirs(dest.parent)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            logger.info('Downloading %s -> %s (attempt %d/%d)', url, dest, attempt, retries)
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                _atomic_write_from_stream(dest, resp)
            logger.info('Download concluído: %s', dest)
            return dest
        except urllib.error.HTTPError as e:
            last_err = e
            logger.warning('HTTPError ao baixar %s: %s', url, e)
        except urllib.error.URLError as e:
            last_err = e
            logger.warning('URLError ao baixar %s: %s', url, e)
        except Exception as e:
            last_err = e
            logger.warning('Erro ao baixar %s: %s', url, e)
        time.sleep(1 + attempt)
    raise RuntimeError(f'Falha ao baixar {url}: {last_err}')


def clone_git_repo(url: str, dest_dir: Path, branch: Optional[str] = None, shallow: bool = True) -> Path:
    """Clona um repositório git para dest_dir. Retorna o path do diretório clonado.
    Se já existir, faz fetch+reset.
    """
    ensure_dirs(dest_dir.parent)
    if not dest_dir.exists():
        args = ['git', 'clone']
        if shallow:
            args += ['--depth', '1']
        if branch:
            args += ['--branch', branch]
        args += [url, str(dest_dir)]
        logger.info('Clonando git: %s', ' '.join(args))
        subprocess.run(args, check=True)
    else:
        logger.info('Atualizando git em %s', dest_dir)
        subprocess.run(['git', '-C', str(dest_dir), 'fetch', '--all', '--tags'], check=True)
        if branch:
            subprocess.run(['git', '-C', str(dest_dir), 'reset', '--hard', f'origin/{branch}'], check=False)
        else:
            subprocess.run(['git', '-C', str(dest_dir), 'reset', '--hard'], check=False)
        subprocess.run(['git', '-C', str(dest_dir), 'clean', '-fd'], check=False)
    return dest_dir


def _download_single_source(s: SourceEntry, cache_dir: Path, pkg_name: Optional[str] = None) -> Dict[str, Any]:
    """Tenta baixar uma única source, seguindo prioridades. Retorna metadata com status e path.
    """
    result: Dict[str, Any] = {'source': s, 'ok': False, 'path': None, 'error': None, 'checksum': s.checksum}
    try:
        if s.type in ('url', 'http', 'https', 'archive'):
            filename = Path(urllib.parse.urlparse(s.url).path).name or f'{pkg_name or "source"}.dat'
            dest = cache_dir / (s.url.replace('://', '_').replace('/', '_'))
            # ensure unique filename extension
            if not dest.suffix and s.format:
                dest = dest.with_suffix('.' + s.format.replace('tar.', '').replace('.', ''))
            ensure_dirs(dest.parent)
            # se já existe e checksum bate, retornamos
            if dest.exists():
                ok, comp = _verify_checksum(dest, s.checksum)
                if ok:
                    logger.info('Usando cache para %s -> %s', s.url, dest)
                    result.update({'ok': True, 'path': str(dest), 'computed_checksum': comp})
                    return result
                else:
                    logger.info('Checksum no cache diverge (computed=%s); rebaixando', comp)
                    dest.unlink()
            downloaded = download_url(s.url, dest)
            ok, comp = _verify_checksum(downloaded, s.checksum)
            if not ok:
                raise RuntimeError(f'Checksum mismatch para {s.url} (computed {comp})')
            result.update({'ok': True, 'path': str(downloaded), 'computed_checksum': comp})
            return result

        elif s.type == 'git':
            # clonamos para cache_dir/<repo_slug>
            slug = s.url.replace('://', '_').replace('/', '_')
            dest = cache_dir / slug
            branch = None
            # tentar extrair ref do url se tiver '#ref'
            if '#' in s.url:
                base, ref = s.url.split('#', 1)
                url = base
                branch = ref
            else:
                url = s.url
            clone_git_repo(url, dest, branch=branch)
            result.update({'ok': True, 'path': str(dest)})
            return result
        else:
            raise RuntimeError(f'Tipo de source não suportado: {s.type}')
    except Exception as e:
        logger.error('Falha ao baixar source %s: %s', getattr(s, 'url', str(s)), e)
        result.update({'ok': False, 'error': str(e)})
        return result


def download_sources_from_metafile(mf: MetaFile, cache_dir: Path = CACHE_DIR_DEFAULT, parallel: int = DEFAULT_PARALLEL) -> List[Dict[str, Any]]:
    """Recebe um MetaFile (já com variables expandidas) e tenta baixar todas as sources declaradas.

    Retorna lista de dicts com status para cada source (preferências, multiple entries attempted in order).
    """
    ensure_dirs(cache_dir)
    tasks = []
    results: List[Dict[str, Any]] = []
    # Agrupar por priority: maior prioridade primeiro (menor valor = maior prioridade?)
    sorted_sources = sorted(mf.sources, key=lambda s: s.priority, reverse=False)

    # Usamos ThreadPoolExecutor para baixar em paralelo
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        future_to_source = {ex.submit(_download_single_source, s, cache_dir, mf.name): s for s in sorted_sources}
        for fut in as_completed(future_to_source):
            s = future_to_source[fut]
            try:
                res = fut.result()
                results.append(res)
            except Exception as e:
                logger.error('Exception no download de %s: %s', s.url, e)
                results.append({'source': s, 'ok': False, 'error': str(e)})
    return results


# -------------------- CLI de demonstração --------------------
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(prog='pmgr_downloader', description='Downloader de fontes para pmgr')
    p.add_argument('--cache', default=None)
    p.add_argument('--parallel', default=4, type=int)
    p.add_argument('metafile', nargs='?')
    args = p.parse_args()

    cache = Path(args.cache) if args.cache else CACHE_DIR_DEFAULT
    ensure_dirs(cache)
    if args.metafile:
        mfpath = Path(args.metafile)
        if not mfpath.exists():
            print('Metafile não encontrado:', mfpath)
            raise SystemExit(1)
        mf = MetaFile.from_path(mfpath)
        mf.expand_variables({'PREFIX': '/usr'})
        res = download_sources_from_metafile(mf, cache_dir=cache, parallel=args.parallel)
        import json
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        p.print_help()
