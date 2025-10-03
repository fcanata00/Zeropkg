"""
zeropkg_downloader.py

Módulo downloader para zeropkg.

Funcionalidades:
- Recebe um PackageMeta (do zeropkg_toml) e baixa as fontes listadas em meta.sources.
- Ordena por priority asc (menor = mais preferido).
- Suporta: http(s), file://, ftp (via urllib), e (limitado) git+... via subprocess git clone, se git disponível.
- Verifica checksum quando presente (formato "sha256:...", "sha1:...", "md5:...").
- Guarda arquivo em cache_dir (por padrão '/usr/ports/distfiles' mas pode ser sobrescrito).
- Se o arquivo já existe e o checksum confere, retorna caminho do cache sem baixar.
- API principal: download_package(meta, cache_dir='/usr/ports/distfiles')
"""

from __future__ import annotations
import os
import shutil
import hashlib
import urllib.request
import subprocess
import tempfile
import pathlib
from typing import Optional
from zeropkg_toml import PackageMeta, SourceEntry

class DownloadError(Exception):
    pass

class ChecksumMismatch(Exception):
    pass

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _calc_checksum(path: str, algo: str) -> str:
    h = None
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
    # checksum_spec like "sha256:abcdef..."
    if not checksum_spec:
        return True
    if ':' not in checksum_spec:
        raise ValueError("Checksum precisa no formato 'algo:hexdigest'")
    algo, hexd = checksum_spec.split(':', 1)
    actual = _calc_checksum(path, algo)
    return actual.lower() == hexd.lower()

def _safe_filename_from_url(url: str) -> str:
    # tenta extrair nome base, caso não haja, usa hash
    p = urllib.request.urlparse(url)
    name = os.path.basename(p.path)
    if not name:
        # usar hash do url
        name = hashlib.sha1(url.encode()).hexdigest()[:12]
    return name

def _download_http(url: str, dest: str, timeout: int = 30):
    try:
        # urllib.request.urlretrieve pode lançar vários erros
        urllib.request.urlretrieve(url, dest)
    except Exception as e:
        raise DownloadError(f"Falha ao baixar {url}: {e}") from e

def _download_file_url(url: str, dest: str):
    # file:// scheme
    p = urllib.request.urlparse(url)
    # netloc + path can be used on windows; produce absolute path
    if p.netloc:
        src = os.path.abspath(os.path.join(os.sep, p.netloc, p.path.lstrip('/')))
    else:
        src = os.path.abspath(p.path)
    if not os.path.exists(src):
        raise DownloadError(f"Fonte local não encontrada: {src}")
    shutil.copy2(src, dest)

def _git_clone_to_tar(url: str, out_dir: str) -> str:
    # url expected like git+https://... or git+file://...
    if url.startswith("git+"):
        real = url[len("git+"):]
    else:
        real = url
    # require git available
    try:
        subprocess.run(["git", "--version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        raise DownloadError("git não disponível no sistema para clonar repositórios") from e
    # clone (shallow) into temp dir, then archive as tar.gz
    tmpd = tempfile.mkdtemp(prefix="zeropkg_git_")
    try:
        subprocess.run(["git", "clone", "--depth", "1", real, tmpd], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # criar um tarball do conteúdo
        base = os.path.join(out_dir, "repo")
        tarpath = shutil.make_archive(base, "gztar", root_dir=tmpd)
        return tarpath
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)

def download_package(meta: PackageMeta, cache_dir: str = "/usr/ports/distfiles", timeout: int = 30, prefer_existing: bool = True) -> str:
    """
    Baixa as fontes indicadas por meta.sources em ordem de priority.
    Retorna o caminho para o arquivo no cache.
    Lança DownloadError em falhas, ChecksumMismatch quando o checksum não bate.
    """
    _ensure_dir(cache_dir)
    sources = sorted(meta.sources, key=lambda s: (s.priority if getattr(s, "priority", None) is not None else 1000))
    last_err = None
    for src in sources:
        url = src.url
        filename = _safe_filename_from_url(url)
        # garantir nome único por versão: prefixar com package-version
        safe_name = f"{meta.name}-{meta.version}-{filename}"
        cache_path = os.path.join(cache_dir, safe_name)

        # se já existe e checksum confere, retornar (short-circuit)
        if os.path.exists(cache_path) and src.checksum:
            try:
                if _verify_checksum(cache_path, src.checksum):
                    return cache_path
                else:
                    # checksum mismatch: remover e tentar baixar
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
            # preparar destino temporário para download
            td = tempfile.NamedTemporaryFile(delete=False)
            td.close()
            tmp_dest = td.name

            if url.startswith(("http://", "https://", "ftp://")):
                _download_http(url, tmp_dest, timeout=timeout)
            elif url.startswith("file://"):
                _download_file_url(url, tmp_dest)
            elif url.startswith(("git+", "git://")):
                # clone and produce tarball in temp dir, then move to cache path
                git_out_dir = tempfile.mkdtemp(prefix="zeropkg_git_out_")
                try:
                    tarpath = _git_clone_to_tar(url, git_out_dir)
                    # tarpath is inside git_out_dir; move/rename into cache_path
                    shutil.move(tarpath, cache_path)
                    # verify checksum if given
                    if src.checksum:
                        if not _verify_checksum(cache_path, src.checksum):
                            try:
                                os.remove(cache_path)
                            except Exception:
                                pass
                            raise ChecksumMismatch("Checksum mismatch for git source")
                    return cache_path
                finally:
                    shutil.rmtree(git_out_dir, ignore_errors=True)
            else:
                raise DownloadError(f"Scheme não suportado ou URL inválida: {url}")

            # mover tmp_dest para cache_path
            shutil.move(tmp_dest, cache_path)

            # verificar checksum se fornecida
            if src.checksum:
                if not _verify_checksum(cache_path, src.checksum):
                    # remover arquivo e tentar próximo source
                    try:
                        os.remove(cache_path)
                    except Exception:
                        pass
                    raise ChecksumMismatch(f"Checksum mismatch para {url}")

            return cache_path
        except (DownloadError, ChecksumMismatch, Exception) as e:
            last_err = e
            # Tentar próximo source (silencioso)
            continue

    # se chegou aqui, todos falharam
    raise DownloadError(f"Falha ao baixar todas as fontes. Último erro: {last_err}")
