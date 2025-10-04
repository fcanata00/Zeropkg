#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_downloader.py — Downloader completo do Zeropkg
Suporte a: múltiplas fontes, git+, extract_to, checksums e integração com builder.
"""

from __future__ import annotations
import os
import hashlib
import shutil
import subprocess
import tarfile
import zipfile
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
    # Download helpers
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

    def _download_git(self, url: str, dest_dir: str) -> str:
        """Clona ou atualiza repositório Git."""
        pkg_name = os.path.basename(dest_dir)
        log_event(pkg_name, "download", f"Clonando repositório git {url} → {dest_dir}")
        try:
            if os.path.exists(dest_dir):
                subprocess.run(["git", "-C", dest_dir, "pull"], check=False, env=self.env)
            else:
                subprocess.run(["git", "clone", "--depth", "1", url, dest_dir], check=True, env=self.env)
        except subprocess.CalledProcessError as e:
            raise DownloadError(f"Erro ao clonar {url}: {e}")
        return dest_dir

    # --------------------------
    # Extração
    # --------------------------
    def _extract_file(self, file_path: str, build_root: str, extract_to: Optional[str] = None) -> str:
        """Extrai arquivo para build_root ou subdiretório indicado."""
        target_dir = os.path.join(build_root, extract_to) if extract_to else build_root
        os.makedirs(target_dir, exist_ok=True)
        log_event(os.path.basename(file_path), "extract", f"Extraindo para {target_dir}")

        try:
            if tarfile.is_tarfile(file_path):
                with tarfile.open(file_path, "r:*") as tar:
                    tar.extractall(path=target_dir)
            elif zipfile.is_zipfile(file_path):
                with zipfile.ZipFile(file_path, "r") as z:
                    z.extractall(target_dir)
            else:
                logger.warning(f"Formato não reconhecido para extração: {file_path}")
        except Exception as e:
            raise DownloadError(f"Falha ao extrair {file_path}: {e}")

        # Verifica se algo foi extraído
        if not any(os.scandir(target_dir)):
            raise DownloadError(f"Nada extraído de {file_path}")
        return target_dir

    # --------------------------
    # Interface pública
    # --------------------------
    def fetch_sources(
        self,
        pkg_name: str,
        sources: List[Dict[str, str]],
        build_root: str = "/var/zeropkg/build",
        parallel: bool = True
    ) -> List[str]:
        """
        Faz download de múltiplas fontes e as extrai se necessário.
        Campos suportados:
          - url: link de download
          - checksum: hash sha256 (opcional)
          - algo: algoritmo do hash (default sha256)
          - extract_to: subdiretório onde extrair
          - method: forçar 'git', 'wget' ou 'curl'
        """
        results = []
        os.makedirs(self.dist_dir, exist_ok=True)
        os.makedirs(build_root, exist_ok=True)

        def _fetch(entry: Dict[str, str]) -> Optional[str]:
            url = entry.get("url")
            checksum = entry.get("checksum", "")
            algo = entry.get("algo", "sha256")
            extract_to = entry.get("extract_to")
            method = entry.get("method", "")

            if not url:
                return None

            # GIT
            if url.startswith("git+") or method == "git":
                dest_dir = os.path.join(self.dist_dir, pkg_name)
                return self._download_git(url.replace("git+", ""), dest_dir)

            # HTTP(S) download
            filename = os.path.basename(url)
            dest_path = os.path.join(self.dist_dir, filename)

            if os.path.exists(dest_path) and self._validate_checksum(dest_path, checksum, algo):
                log_event(pkg_name, "download", f"Usando cache existente: {filename}")
            else:
                self._download_file(url, dest_path)
                if checksum and not self._validate_checksum(dest_path, checksum, algo):
                    os.remove(dest_path)
                    raise DownloadError(f"Checksum incorreto: {filename}")

            # Extração se necessário
            try:
                extracted_dir = self._extract_file(dest_path, build_root, extract_to)
                results.append(extracted_dir)
                return extracted_dir
            except DownloadError as e:
                logger.warning(f"Falha de extração ignorada: {e}")
                results.append(dest_path)
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

        return [r for r in results if r]
    

# --------------------------
# Teste rápido
# --------------------------
if __name__ == "__main__":
    dl = Downloader()
    sources = [
        {"url": "https://ftp.gnu.org/gnu/m4/m4-1.4.19.tar.xz"},
        {"url": "git+https://git.savannah.gnu.org/git/bash.git", "method": "git"},
        {"url": "https://ftp.gnu.org/gnu/mpfr/mpfr-4.2.1.tar.xz", "extract_to": "gcc-13.2.0/mpfr"}
    ]
    files = dl.fetch_sources("gcc", sources, build_root="/tmp/build")
    print("Arquivos baixados:", files)
