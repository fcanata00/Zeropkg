#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zeropkg Downloader — módulo responsável por baixar, validar e extrair fontes
para construção de pacotes no Zeropkg.

Características principais:
 - Suporte a múltiplas URLs (mirrors e fallback automático)
 - Verificação SHA256/SHA512 e GPG opcional
 - Cache global em /usr/ports/distfiles
 - Downloads paralelos com ThreadPoolExecutor
 - Integrado com zeropkg_db, zeropkg_logger, zeropkg_toml e zeropkg_config
 - Suporte a dry-run e retries automáticos
"""

import os
import sys
import hashlib
import tarfile
import zipfile
import shutil
import subprocess
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import requests

# Dependências internas
from zeropkg_logger import log
from zeropkg_toml import resolve_macros
from zeropkg_config import ZeropkgConfig
from zeropkg_db import Database


class Downloader:
    def __init__(self, config: ZeropkgConfig):
        self.config = config
        self.cache_dir = Path(self.config.get("paths", "distfiles", fallback="/usr/ports/distfiles"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.db = Database(config)

    # ===============================================================
    # Baixar múltiplas fontes de uma receita (em paralelo)
    # ===============================================================
    def fetch_all(self, sources, jobs=4, dry_run=False):
        log("📥 Iniciando downloads paralelos...")
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = [executor.submit(self._fetch_single, src, dry_run=dry_run) for src in sources]
            results = [f.result() for f in as_completed(futures)]
        log(f"✅ Todos os downloads concluídos ({len(results)} arquivos).")
        return results

    # ===============================================================
    # Baixar e validar uma única fonte
    # ===============================================================
    def _fetch_single(self, source, dry_run=False):
        urls = [source.get("url")] + source.get("mirrors", [])
        urls = [resolve_macros(u) for u in urls if u]
        filename = os.path.basename(urlparse(urls[0]).path)
        dest_path = self.cache_dir / filename

        if dry_run:
            log(f"[dry-run] baixaria {filename}")
            return dest_path

        # Se já existe e checksum bate, reaproveitar
        if dest_path.exists() and self._validate_checksum(dest_path, source):
            log(f"✔️ Cache válido encontrado: {filename}")
            return dest_path

        for url in urls:
            try:
                self._download_file(url, dest_path)
                if self._validate_checksum(dest_path, source):
                    log(f"✅ Download válido: {filename}")
                    self._verify_gpg(source, dest_path)
                    self.db.record_event("download", filename, {"url": url})
                    return dest_path
            except Exception as e:
                log(f"⚠️ Falha em {url}: {e}")
                continue
        raise RuntimeError(f"❌ Falha ao baixar {filename} de todas as URLs.")

    # ===============================================================
    # Download direto com requests
    # ===============================================================
    def _download_file(self, url, dest_path):
        log(f"⬇️ Baixando {url} → {dest_path}")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        tmp.write(chunk)
                tmp.flush()
                shutil.move(tmp.name, dest_path)

    # ===============================================================
    # Validação de checksum
    # ===============================================================
    def _validate_checksum(self, file_path, source):
        algo = None
        checksum = None
        for key in ("sha512", "sha256", "md5"):
            if key in source:
                algo, checksum = key, source[key]
                break
        if not algo or not checksum:
            return True  # sem verificação obrigatória

        h = hashlib.new(algo)
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        result = h.hexdigest()
        if result != checksum:
            log(f"❌ Checksum incorreto: esperado {checksum[:12]}..., obtido {result[:12]}...")
            file_path.unlink(missing_ok=True)
            return False
        return True

    # ===============================================================
    # Verificação de assinatura GPG opcional
    # ===============================================================
    def _verify_gpg(self, source, file_path):
        sig_url = source.get("sig_url")
        if not sig_url:
            return
        sig_path = self.cache_dir / (os.path.basename(file_path) + ".asc")
        try:
            log(f"🔐 Baixando assinatura GPG de {sig_url}")
            self._download_file(sig_url, sig_path)
            subprocess.run(["gpg", "--verify", str(sig_path), str(file_path)],
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            log("🧾 Assinatura GPG verificada com sucesso.")
        except Exception as e:
            log(f"⚠️ Falha na verificação GPG: {e}")

    # ===============================================================
    # Extração segura
    # ===============================================================
    def extract(self, archive_path, extract_to=None):
        archive_path = Path(archive_path)
        target = Path(extract_to) if extract_to else archive_path.parent
        target.mkdir(parents=True, exist_ok=True)

        log(f"📦 Extraindo {archive_path} → {target}")
        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as tar:
                for member in tar.getmembers():
                    self._safe_extract(member)
                tar.extractall(target)
        elif zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as zf:
                for member in zf.namelist():
                    self._safe_extract(member)
                zf.extractall(target)
        else:
            log(f"⚠️ Formato desconhecido, copiando arquivo para {target}")
            shutil.copy(archive_path, target)

        self.db.record_event("extract", archive_path.name, {"to": str(target)})
        log(f"✅ Extração concluída: {target}")

    def _safe_extract(self, member):
        name = member.name if hasattr(member, "name") else member
        if ".." in name or name.startswith("/"):
            raise ValueError(f"⚠️ Caminho inseguro detectado: {name}")

    # ===============================================================
    # Baixar e extrair em um único passo
    # ===============================================================
    def fetch_and_extract(self, source, jobs=4, dry_run=False):
        dest = self._fetch_single(source, dry_run=dry_run)
        extract_to = source.get("extract_to")
        if extract_to and not dry_run:
            self.extract(dest, extract_to)
        return dest


# ===============================================================
# CLI direto para testes
# ===============================================================
if __name__ == "__main__":
    cfg = ZeropkgConfig()
    dl = Downloader(cfg)
    sources = [{
        "url": "https://ftp.gnu.org/gnu/m4/m4-1.4.19.tar.xz",
        "sha256": "63a6a2b5c94f3d8d28bb4a980b2dfeb0d7dc2c7bba7c5e56ef653a3b59c6e08f",
        "extract_to": "/tmp/m4"
    }]
    dl.fetch_all(sources)
