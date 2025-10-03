"""
zeropkg_downloader.py

Downloader multi-source para Zeropkg — versão revisada e integrada.

Suporta:
- http(s), ftp, file://, git+...
- retries com backoff
- nomes únicos no cache (hash curto da URL)
- opção prefer_existing
- verbose ou log via zeropkg_logger
"""

from __future__ import annotations
import os
import shutil
import hashlib
import urllib.request
import urllib.parse
import subprocess
import tempfile
import time
import logging
from typing import Optional
from zeropkg_toml import PackageMeta, SourceEntry
from zeropkg_logger import log_event, setup_logger

logger = setup_logger(pkg_name=None, stage="downloader")

class DownloadError(Exception):
    pass

class ChecksumMismatch(Exception):
    pass

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _calc_checksum(path: str, algo: str) -> str:
    algo = algo.lower()
    if algo == 'sha256':
        h = hashlib.sha256()
    elif algo == 'sha1':
        h = hashlib.sha1()
    elif algo == 'md5':
        h = hashlib.md5()
    else:
        raise ValueError(f"Algoritmo de checksum não suportado: {algo}")
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def _verify_checksum(path: str, checksum_spec: Optional[str]) -> bool:
    if not checksum_spec:
        return True
    if ':' not in checksum_spec:
        raise ValueError("Checksum precisa estar no formato 'algo:hexdigest'")
    algo, hexd = checksum_spec.split(':', 1)
    actual = _calc_checksum(path, algo)
    return actual.lower() == hexd.lower()

def _safe_filename_from_url(url: str) -> str:
    p = urllib.parse.urlparse(url)
    name = os.path.basename(p.path)
    if not name:
        name = hashlib.sha1(url.encode()).hexdigest()[:12]
    return name

def resolve_cache_name(meta: PackageMeta, src: SourceEntry) -> str:
    base = _safe_filename_from_url(src.url)
    urlhash = hashlib.sha1(src.url.encode()).hexdigest()[:8]
    return f"{meta.name}-{meta.version}-{base}-{urlhash}"

def _download_http(url: str, dest: str, timeout: int = 30, retries: int = 3, verbose: bool = False):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if verbose:
                print(f"[zeropkg] Baixando {url} (tentativa {attempt}/{retries})")
            urllib.request.urlretrieve(url, dest)
            return
        except Exception as e:
            last_err = e
            wait = 2 ** attempt
            if verbose:
                print(f"[zeropkg] Falha ao baixar {url}: {e} → retry em {wait}s")
            time.sleep(wait)
            continue
    raise DownloadError(f"Falha ao baixar {url}: {last_err}")

def _download_file_url(url: str, dest: str):
    p = urllib.parse.urlparse(url)
    if p.netloc:
        src = os.path.abspath(os.path.join(os.sep, p.netloc, p.path.lstrip('/')))
    else:
        src = os.path.abspath(p.path)
    if not os.path.exists(src):
        raise DownloadError(f"Fonte local não encontrada: {src}")
    shutil.copy2(src, dest)

def _git_clone_to_tar(url: str, out_dir: str, verbose: bool = False) -> str:
    real = url
    if url.startswith("git+"):
        real = url[len("git+"):]
    try:
        subprocess.run(["git", "--version"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        raise DownloadError("git não disponível para clonar repositórios")
    tmpd = tempfile.mkdtemp(prefix="zeropkg_git_")
    try:
        if verbose:
            print(f"[zeropkg] Clonando {real}")
        subprocess.run(["git", "clone", "--depth", "1", real, tmpd],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        base = os.path.join(out_dir, "repo")
        tarpath = shutil.make_archive(base, "gztar", root_dir=tmpd)
        return tarpath
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)

def download_package(meta: PackageMeta,
                     cache_dir: str = "/usr/ports/distfiles",
                     timeout: int = 30,
                     prefer_existing: bool = True,
                     verbose: bool = False) -> str:
    """
    Baixa as fontes crençadas em meta.sources. Retorna caminho no cache.
    Lança DownloadError ou ChecksumMismatch.
    """
    _ensure_dir(cache_dir)

    sources = sorted(meta.sources, key=lambda s: (s.priority if getattr(s, "priority", None) is not None else 1000))
    last_err = None

    for src in sources:
        url = src.url
        filename = resolve_cache_name(meta, src)
        cache_path = os.path.join(cache_dir, filename)

        if prefer_existing and os.path.exists(cache_path) and src.checksum:
            try:
                if _verify_checksum(cache_path, src.checksum):
                    if verbose:
                        print(f"[zeropkg] Usando cache existente: {cache_path}")
                    log_event(meta.name, "downloader", f"Usou cache {cache_path}")
                    return cache_path
                else:
                    if verbose:
                        print(f"[zeropkg] Cache inválido (checksum mismatch), removendo: {cache_path}")
                    try:
                        os.remove(cache_path)
                    except Exception:
                        pass
            except Exception:
                try:
                    os.remove(cache_path)
                except Exception:
                    pass

        try:
            td = tempfile.NamedTemporaryFile(delete=False)
            td.close()
            tmp_dest = td.name

            if url.startswith(("http://", "https://", "ftp://")):
                _download_http(url, tmp_dest, timeout=timeout, retries=3, verbose=verbose)
            elif url.startswith("file://"):
                _download_file_url(url, tmp_dest)
            elif url.startswith(("git+", "git://")):
                git_out = tempfile.mkdtemp(prefix="zeropkg_git_out_")
                try:
                    tarpath = _git_clone_to_tar(url, git_out, verbose=verbose)
                    shutil.move(tarpath, cache_path)
                    if src.checksum and not _verify_checksum(cache_path, src.checksum):
                        try:
                            os.remove(cache_path)
                        except Exception:
                            pass
                        raise ChecksumMismatch("Checksum mismatch para git source")
                    log_event(meta.name, "downloader", f"Clonou git e salvou em {cache_path}")
                    return cache_path
                finally:
                    shutil.rmtree(git_out, ignore_errors=True)
            else:
                raise DownloadError(f"Scheme não suportado: {url}")

            shutil.move(tmp_dest, cache_path)

            if src.checksum and not _verify_checksum(cache_path, src.checksum):
                try:
                    os.remove(cache_path)
                except Exception:
                    pass
                raise ChecksumMismatch(f"Checksum mismatch para {url}")

            if verbose:
                print(f"[zeropkg] Download concluído: {cache_path}")
            log_event(meta.name, "downloader", f"Download concluído {cache_path}")
            return cache_path

        except (DownloadError, ChecksumMismatch, Exception) as e:
            last_err = e
            if verbose:
                print(f"[zeropkg] Erro com {url}, tentando próximo mirror: {e}")
            log_event(meta.name, "downloader", f"Erro ao baixar {url}: {e}")
            continue

    raise DownloadError(f"Falha ao baixar todas as fontes. Último erro: {last_err}")
