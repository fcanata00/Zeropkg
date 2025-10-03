#!/usr/bin/env python3
# zeropkg_downloader.py - Downloader avançado do Zeropkg
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import hashlib
import shutil
import subprocess
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Union
from zeropkg_logger import get_logger, log_event

logger = get_logger(stage="downloader")

DISTFILES_DIR = "/usr/ports/distfiles"
RETRIES = 3


class DownloadError(Exception):
    pass


class Downloader:
    def __init__(self, dist_dir: str = DISTFILES_DIR, max_workers: int = 4, env: Optional[Dict[str, str]] = None):
        self.dist_dir = os.path.abspath(dist_dir)
        self.max_workers = max_workers
        self.env = os.environ.copy()
        if env:
            self.env.update(env)
        os.makedirs(self.dist_dir, exist_ok=True)

    # --------------------------
    # Checksum helpers
    # --------------------------
    def _hash_file(self, path: str, algo: str = "sha256") -> str:
        h = hashlib.new(algo)
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def _validate_checksum(self, path: str, expected: str, algo: str = "sha256") -> bool:
        if not expected:
            return True
        actual = self._hash_file(path, algo)
        return actual.lower() == expected.lower()

    # --------------------------
    # Core download
    # --------------------------
    def _download_file(self, url: str, dest: str, retries: int = RETRIES) -> str:
        log_event(os.path.basename(dest), "download", f"Baixando {url} → {dest}")
        for attempt in range(1, retries + 1):
            try:
                urllib.request.urlretrieve(url, dest)
                return dest
            except Exception as e:
                logger.warning(f"Tentativa {attempt}/{retries} falhou: {e}")
                if attempt == retries:
                    raise DownloadError(f"Falha ao baixar {url}: {e}")
        return dest

    def _download_git(self, url: str, dest_dir: str):
        """Clona ou atualiza repositório git."""
        pkg_name = os.path.basename(dest_dir)
        log_event(pkg_name, "download", f"Clonando git {url} em {dest_dir}")
        if os.path.exists(dest_dir):
            subprocess.run(["git", "-C", dest_dir, "pull"], check=False, env=self.env)
        else:
            subprocess.run(["git", "clone", "--depth", "1", url, dest_dir], check=True, env=self.env)
        return dest_dir

    # --------------------------
    # Interface pública
    # --------------------------
    def fetch_sources(self, pkg_name: str, sources: List[Dict[str, str]], parallel: bool = True) -> List[str]:
        """
        Faz o download de múltiplas fontes de um pacote.
        sources: lista de dicts com { "url": str, "checksum": str, "algo": "sha256"|"md5" }
        """
        results = []
        os.makedirs(self.dist_dir, exist_ok=True)

        def _fetch(entry):
            url = entry.get("url")
            checksum = entry.get("checksum")
            algo = entry.get("algo", "sha256")
            if not url:
                return None

            if url.startswith("git+"):
                dest_dir = os.path.join(self.dist_dir, pkg_name)
                path = self._download_git(url[4:], dest_dir)
                return path

            filename = os.path.basename(url)
            dest_path = os.path.join(self.dist_dir, filename)

            # Se arquivo já existe e checksum bate, reutiliza
            if os.path.exists(dest_path) and self._validate_checksum(dest_path, checksum, algo):
                log_event(pkg_name, "download", f"Usando cache existente: {filename}")
                return dest_path

            # Tenta baixar
            self._download_file(url, dest_path)
            if checksum and not self._validate_checksum(dest_path, checksum, algo):
                os.remove(dest_path)
                raise DownloadError(f"Checksum incorreto para {filename}")

            log_event(pkg_name, "download", f"Download completo: {filename}")
            return dest_path

        if parallel:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(_fetch, s) for s in sources]
                for f in as_completed(futures):
                    try:
                        results.append(f.result())
                    except Exception as e:
                        logger.error(f"Erro de download: {e}")
                        raise
        else:
            for s in sources:
                results.append(_fetch(s))

        return results


# --------------------------
# Teste rápido
# --------------------------
if __name__ == "__main__":
    dl = Downloader()
    srcs = [
        {"url": "https://ftp.gnu.org/gnu/m4/m4-1.4.19.tar.xz", "checksum": "", "algo": "sha256"},
        {"url": "git+https://git.savannah.gnu.org/git/bash.git"},
    ]
    files = dl.fetch_sources("testpkg", srcs)
    print("Arquivos baixados:", files)
