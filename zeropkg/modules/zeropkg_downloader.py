"""
zeropkg_downloader.py

Downloader multi-source para Zeropkg — versão melhorada.

Suporta:
- http(s), ftp, file://, git+, rsync://, scp://
- retries com backoff + resume
- download paralelo opcional
- nomes únicos no cache
- verificação de checksum
"""

import os
import shutil
import hashlib
import urllib.request
import urllib.parse
import subprocess
import tempfile
import time
import logging
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

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


# ----------------------------
# Downloaders
# ----------------------------
def _download_http(url: str, dest: str, timeout: int = 30, retries: int = 3, verbose: bool = False):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url)
            if os.path.exists(dest):
                # resume download
                current_size = os.path.getsize(dest)
                req.add_header("Range", f"bytes={current_size}-")
            with urllib.request.urlopen(req, timeout=timeout) as resp, open(dest, "ab") as out:
                shutil.copyfileobj(resp, out)
            return
        except Exception as e:
            last_err = e
            wait = 2 ** attempt
            if verbose:
                print(f"[zeropkg] Falha ao baixar {url}: {e} → retry em {wait}s")
            time.sleep(wait)
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
    real = url[len("git+"):] if url.startswith("git+") else url
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

def _download_rsync(url: str, dest: str):
    subprocess.run(["rsync", "-avz", url, dest], check=True)

def _download_scp(url: str, dest: str):
    # Ex: scp://user@host:/path/to/file
    real = url[len("scp://"):]
    subprocess.run(["scp", real, dest], check=True)


# ----------------------------
# API principal
# ----------------------------
def download_source(meta: PackageMeta,
                    src: SourceEntry,
                    cache_dir: str,
                    timeout: int = 30,
                    prefer_existing: bool = True,
                    verbose: bool = False) -> Dict[str, Any]:
    """Baixa um único source e retorna dict com {path, url, from_cache}"""
    _ensure_dir(cache_dir)
    url = src.url
    filename = resolve_cache_name(meta, src)
    cache_path = os.path.join(cache_dir, filename)

    # cache já válido
    if prefer_existing and os.path.exists(cache_path) and src.checksum:
        if _verify_checksum(cache_path, src.checksum):
            return {"path": cache_path, "url": url, "from_cache": True}

    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.close()
    tmp_dest = tmp.name

    try:
        if url.startswith(("http://", "https://", "ftp://")):
            _download_http(url, tmp_dest, timeout=timeout, retries=3, verbose=verbose)
        elif url.startswith("file://"):
            _download_file_url(url, tmp_dest)
        elif url.startswith(("git+", "git://")):
            git_out = tempfile.mkdtemp(prefix="zeropkg_git_out_")
            try:
                tarpath = _git_clone_to_tar(url, git_out, verbose=verbose)
                shutil.move(tarpath, cache_path)
                return {"path": cache_path, "url": url, "from_cache": False}
            finally:
                shutil.rmtree(git_out, ignore_errors=True)
        elif url.startswith("rsync://"):
            _download_rsync(url, tmp_dest)
        elif url.startswith("scp://"):
            _download_scp(url, tmp_dest)
        else:
            raise DownloadError(f"Scheme não suportado: {url}")

        shutil.move(tmp_dest, cache_path)

        if src.checksum and not _verify_checksum(cache_path, src.checksum):
            os.remove(cache_path)
            raise ChecksumMismatch(f"Checksum mismatch para {url}")

        return {"path": cache_path, "url": url, "from_cache": False}
    finally:
        if os.path.exists(tmp_dest):
            os.remove(tmp_dest)


def download_package(meta: PackageMeta,
                     cache_dir: str = "/usr/ports/distfiles",
                     timeout: int = 30,
                     prefer_existing: bool = True,
                     verbose: bool = False,
                     parallel: bool = False) -> List[Dict[str, Any]]:
    """
    Baixa todos os sources de um pacote.
    Retorna lista de dicts [{path, url, from_cache}, ...]
    """
    sources = sorted(meta.sources, key=lambda s: (s.priority if getattr(s, "priority", None) is not None else 1000))

    results = []
    if parallel and len(sources) > 1:
        with ThreadPoolExecutor(max_workers=min(4, len(sources))) as ex:
            futs = {ex.submit(download_source, meta, s, cache_dir, timeout, prefer_existing, verbose): s for s in sources}
            for fut in as_completed(futs):
                results.append(fut.result())
    else:
        for s in sources:
            results.append(download_source(meta, s, cache_dir, timeout, prefer_existing, verbose))

    return results
