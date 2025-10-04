#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_downloader.py — robust downloader + extractor for Zeropkg
Pattern B: integrated, lean, functional.

Public API:
- class Downloader(cfg=None)
    - download(url, dest_dir, checksum=None, algo="sha256", extract=True, extract_to=None, resume=True, dry_run=False) -> dict
    - fetch_meta_sources(meta, dest_root=None, dry_run=False) -> List[dict]
    - set_logger(logger) -> override internal logger
    - configure_retries(n, backoff)
"""

from __future__ import annotations
import os
import sys
import time
import hashlib
import shutil
import tempfile
import tarfile
import zipfile
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable

# Try to use requests (preferred). Fallback to urllib.
try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except Exception:
    import urllib.request as _urllib_request  # type: ignore
    _HAS_REQUESTS = False

# Integrations (optional)
try:
    from zeropkg_config import load_config, get_cache_dir, get_build_root
except Exception:
    def load_config(*a, **k):
        return {"paths": {"cache_dir": "/usr/ports/distfiles", "build_root": "/var/zeropkg/build"}}
    def get_cache_dir(cfg=None):
        return "/usr/ports/distfiles"
    def get_build_root(cfg=None):
        return "/var/zeropkg/build"

try:
    from zeropkg_logger import log_event, get_logger, log_global
    _LOGGER = get_logger("downloader")
except Exception:
    import logging
    _LOGGER = logging.getLogger("zeropkg_downloader")
    if not _LOGGER.handlers:
        _LOGGER.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_LOGGER, level if hasattr(_LOGGER, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(_LOGGER, level if hasattr(_LOGGER, level) else "info")(msg)

# DB optional
try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# Utilities
_CHUNK = 1024 * 64

def _sha256_file(path: str, algo: str = "sha256") -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()

def _safe_extract_tar(tar_path: str, dest: str) -> str:
    """
    Extract a tar archive safely into dest and return the top-level extracted dir (or dest if multiple entries).
    Prevent path traversal.
    """
    dest_p = Path(dest)
    dest_p.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "r:*") as tar:
        members = tar.getmembers()
        # Protect against path traversal
        for m in members:
            mpath = Path(m.name)
            if mpath.is_absolute() or ".." in mpath.parts:
                raise RuntimeError(f"Archive contains unsafe path: {m.name}")
        tar.extractall(path=dest)
    # detect single top-level directory
    entries = [p for p in dest_p.iterdir() if p.name != '.' and p.exists()]
    if len(entries) == 1 and entries[0].is_dir():
        return str(entries[0])
    return str(dest_p)

def _safe_extract_zip(zip_path: str, dest: str) -> str:
    dest_p = Path(dest)
    dest_p.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            p = Path(name)
            if p.is_absolute() or ".." in p.parts:
                raise RuntimeError(f"Zip contains unsafe path: {name}")
        z.extractall(path=dest)
    entries = [p for p in dest_p.iterdir() if p.name != '.' and p.exists()]
    if len(entries) == 1 and entries[0].is_dir():
        return str(entries[0])
    return str(dest_p)

def _is_archive(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in (".tar.gz", ".tgz", ".tar.xz", ".tar.bz2", ".tar", ".zip", ".gz", ".xz", ".bz2"))

# Downloader implementation
class Downloader:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or load_config()
        self.cache_dir = _safe_path(get_cache_dir(self.cfg))
        self.build_root = _safe_path(get_build_root(self.cfg))
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        Path(self.build_root).mkdir(parents=True, exist_ok=True)
        self.retries = 3
        self.backoff = 1.5
        self.logger = _LOGGER
        self.register_db = DBManager is not None

    def set_logger(self, logger):
        self.logger = logger

    def configure_retries(self, n: int, backoff: float = 1.5):
        self.retries = max(1, int(n))
        self.backoff = float(backoff)

    def _record_db(self, pkg: Optional[str], url: str, path: str, success: bool, note: str = ""):
        if not DBManager:
            return
        try:
            with DBManager() as db:
                ts = int(time.time())
                payload = {"url": url, "path": path, "success": bool(success), "note": note}
                db._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                            (pkg or "downloader", "download", json.dumps(payload), ts))
        except Exception:
            # don't fail on DB logging
            pass

    def _log(self, pkg: Optional[str], stage: str, msg: str, level: str = "info"):
        try:
            log_event(pkg or "downloader", stage, msg, level=level)
        except Exception:
            getattr(self.logger, level if hasattr(self.logger, level) else "info")(f"{pkg}:{stage} {msg}")

    def download(self, url: str, dest_dir: str, checksum: Optional[str] = None, algo: str = "sha256",
                 extract: bool = True, extract_to: Optional[str] = None, resume: bool = True, dry_run: bool = False) -> Dict[str, Any]:
        """
        Download a single URL to dest_dir (under cache_dir by default). Returns dict with:
            { "url": ..., "path": downloaded_file, "extracted_to": path or None, "checksum_ok": bool }
        """
        dest_base = Path(dest_dir)
        dest_base.mkdir(parents=True, exist_ok=True)
        parsed_name = os.path.basename(url.split("?", 1)[0]) or f"dl-{int(time.time())}"
        # ensure unique file name if collision in cache
        dest_file = dest_base / parsed_name
        # if URL includes https://example/archive.tar.gz -> dest_file will be archive.tar.gz
        # allow collisions by appending numeric suffix
        i = 1
        while dest_file.exists():
            # if checksum provided, check match
            if checksum and dest_file.exists():
                try:
                    if self.verify_checksum(str(dest_file), checksum, algo):
                        self._log(None, "download", f"Using cached file {dest_file}", "debug")
                        break
                except Exception:
                    pass
            parsed_name = f"{parsed_name}.{i}"
            dest_file = dest_base / parsed_name
            i += 1

        # perform download (dry-run: just report)
        if dry_run:
            self._log(None, "download", f"[dry-run] would download {url} -> {dest_file}", "info")
            return {"url": url, "path": str(dest_file), "extracted_to": None, "checksum_ok": None}

        last_exc = None
        for attempt in range(1, self.retries + 1):
            try:
                if _HAS_REQUESTS:
                    self._download_requests(url, str(dest_file), resume=resume)
                else:
                    self._download_urllib(url, str(dest_file), resume=resume)
                break
            except Exception as e:
                last_exc = e
                self._log(None, "download", f"Attempt {attempt} failed for {url}: {e}", "warning")
                time.sleep(self.backoff * attempt)
        else:
            self._log(None, "download", f"All attempts failed for {url}: {last_exc}", "error")
            self._record_db(None, url, str(dest_file), False, note=str(last_exc))
            raise RuntimeError(f"Download failed: {url}") from last_exc

        # verify checksum if provided
        checksum_ok = None
        if checksum:
            try:
                checksum_ok = self.verify_checksum(str(dest_file), checksum, algo)
                if not checksum_ok:
                    raise RuntimeError(f"Checksum mismatch for {dest_file}")
            except Exception as e:
                self._log(None, "download", f"Checksum verification failed: {e}", "error")
                self._record_db(None, url, str(dest_file), False, note=f"checksum: {e}")
                raise

        # record success
        self._record_db(None, url, str(dest_file), True, note="downloaded")
        self._log(None, "download", f"Downloaded {url} -> {dest_file}", "info")

        extracted_to = None
        if extract and _is_archive(str(dest_file)):
            try:
                extract_dest = extract_to or str(dest_base)
                extracted_to = self.extract_archive(str(dest_file), extract_dest, dry_run=dry_run)
                self._log(None, "extract", f"Extracted {dest_file} -> {extracted_to}", "info")
            except Exception as e:
                self._log(None, "extract", f"Extraction failed for {dest_file}: {e}", "error")
                raise

        return {"url": url, "path": str(dest_file), "extracted_to": extracted_to, "checksum_ok": checksum_ok}

    def _download_requests(self, url: str, dest: str, resume: bool = True):
        # Use streaming GET with optional Range support
        headers = {}
        dest_p = Path(dest)
        mode = "wb"
        existing = dest_p.exists()
        if resume and existing:
            current = dest_p.stat().st_size
            headers["Range"] = f"bytes={current}-"
            mode = "ab"
        else:
            current = 0
        with requests.get(url, stream=True, headers=headers, timeout=30) as r:
            r.raise_for_status()
            # if partial content and 206, we can append; otherwise overwrite
            if r.status_code == 200 and mode == "ab":
                # server didn't honor Range; overwrite
                mode = "wb"
            with open(dest, mode) as f:
                for chunk in r.iter_content(chunk_size=_CHUNK):
                    if chunk:
                        f.write(chunk)

    def _download_urllib(self, url: str, dest: str, resume: bool = True):
        # Simple urllib downloader, best-effort resume using Range
        dest_p = Path(dest)
        existing = dest_p.exists()
        req = _urllib_request.Request(url)
        if resume and existing:
            current = dest_p.stat().st_size
            req.add_header("Range", f"bytes={current}-")
            mode = "ab"
        else:
            mode = "wb"
        with _urllib_request.urlopen(req, timeout=30) as resp:
            with open(dest, mode) as f:
                while True:
                    chunk = resp.read(_CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)

    def verify_checksum(self, path: str, checksum: str, algo: str = "sha256") -> bool:
        algo = (algo or "sha256").lower()
        if algo not in hashlib.algorithms_available:
            raise RuntimeError(f"Checksum algorithm {algo} not available")
        got = _sha256_file(path, algo) if algo == "sha256" else _sha256_file(path, algo)
        return got.lower() == checksum.lower()

    def extract_archive(self, archive_path: str, dest_dir: str, dry_run: bool = False) -> str:
        p = Path(archive_path)
        if dry_run:
            self._log(None, "extract", f"[dry-run] would extract {archive_path} -> {dest_dir}", "info")
            return str(Path(dest_dir) / (p.stem if p.stem else "extracted"))
        # choose extraction method
        lower = p.name.lower()
        if tarfile.is_tarfile(str(p)):
            return _safe_extract_tar(str(p), dest_dir)
        elif zipfile.is_zipfile(str(p)):
            return _safe_extract_zip(str(p), dest_dir)
        else:
            # Not a recognized archive; treat as single file copy
            dest_p = Path(dest_dir) / p.name
            dest_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(p), str(dest_p))
            return str(dest_p)

    def fetch_meta_sources(self, meta: Dict[str, Any], dest_root: Optional[str] = None, dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Convenience: given recipe meta (as produced by zeropkg_toml.load_toml),
        download all sources and return list of result dicts.
        """
        results: List[Dict[str, Any]] = []
        pkg = (meta.get("package") or {}).get("name")
        dest_root = dest_root or self.cache_dir
        sources = meta.get("sources", []) or []
        for s in sources:
            url = s.get("url") or s.get("src")
            if not url:
                self._log(pkg, "download", f"Skipping source entry without url: {s}", "warning")
                continue
            checksum = s.get("checksum")
            algo = s.get("algo", "sha256")
            extract_to = s.get("extract_to") or None
            # if subpath provided, keep it as metadata; extraction returns full path
            try:
                res = self.download(url, dest_root, checksum=checksum, algo=algo, extract=True, extract_to=extract_to, resume=True, dry_run=dry_run)
                # attach meta info
                res["pkg"] = pkg
                res["source_entry"] = s
                results.append(res)
                self._log(pkg, "download", f"Source fetched: {url}", "info")
            except Exception as e:
                self._log(pkg, "download", f"Failed to fetch {url}: {e}", "error")
                raise
        return results

# Helpers
def _safe_path(p: Optional[str]) -> str:
    if not p:
        return ""
    return str(Path(p).expanduser().resolve())

# Module quick-test CLI
if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser(prog="zeropkg-downloader", description="Downloader test harness")
    p.add_argument("url", nargs="?", help="URL to download")
    p.add_argument("--dest", help="Destination dir (defaults to cache_dir)", default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    dl = Downloader()
    if not args.url:
        print("Provide a URL to download")
        sys.exit(1)
    dest = args.dest or dl.cache_dir
    out = dl.download(args.url, dest, dry_run=args.dry_run)
    print(json.dumps(out, indent=2))
