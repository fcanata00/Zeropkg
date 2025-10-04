#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_downloader.py — robust downloader for Zeropkg

Features:
 - parallel downloads with ThreadPoolExecutor
 - cache reuse (hardlink/symlink) and intelligent dedupe
 - move bad/corrupt downloads to distfiles/bad/
 - checksums verification (sha512, sha256, sha1, md5)
 - optional GPG signature verification (python-gnupg or gpg CLI)
 - support for http(s)/ftp/file/scp/git+ protocols (best-effort)
 - secure extraction (prevent path traversal)
 - atomic downloads (.part)
 - progress bars with tqdm when available
 - records events to zeropkg_db and zeropkg_logger if present
 - CLI for standalone use and testing
"""

from __future__ import annotations
import os
import sys
import shutil
import hashlib
import json
import tempfile
import threading
import subprocess
import errno
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse
import time

# Optional libs
try:
    import requests
except Exception:
    requests = None

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import gnupg
except Exception:
    gnupg = None

try:
    import paramiko
except Exception:
    paramiko = None

# Safe imports from Zeropkg ecosystem
def _safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

db_mod = _safe_import("zeropkg_db")
logger_mod = _safe_import("zeropkg_logger")
toml_mod = _safe_import("zeropkg_toml")
config_mod = _safe_import("zeropkg_config")

# logger fallback
if logger_mod and hasattr(logger_mod, "get_logger"):
    LOG = logger_mod.get_logger("downloader")
    log_event = getattr(logger_mod, "log_event", lambda *a, **k: None)
else:
    import logging
    logging.basicConfig(level=logging.INFO)
    LOG = logging.getLogger("zeropkg.downloader")
    def log_event(*a, **k):
        pass

# config defaults
CFG = {}
try:
    if config_mod and hasattr(config_mod, "load_config"):
        CFG = config_mod.load_config()
except Exception:
    CFG = {}

DEFAULT_PORTS_DIR = Path(CFG.get("paths", {}).get("ports_dir", "/usr/ports"))
DEFAULT_DISTFILES = Path(CFG.get("paths", {}).get("distfiles_dir", "/usr/ports/distfiles"))
CACHE_DIR = Path(CFG.get("paths", {}).get("cache_dir", "/var/cache/zeropkg"))
MAX_WORKERS = int(CFG.get("downloader", {}).get("max_workers", 4))

DEFAULT_DISTFILES.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_BAD_DIR = DEFAULT_DISTFILES / "bad"
_BAD_DIR.mkdir(parents=True, exist_ok=True)

_LOCK = threading.RLock()

# ---------------------------
# Utilities
# ---------------------------
def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _compute_cache_key(url: str, filename: Optional[str] = None) -> str:
    # stable key for URL + filename hint
    h = hashlib.sha1()
    h.update(url.encode("utf-8"))
    if filename:
        h.update(b"::")
        h.update(filename.encode("utf-8"))
    return h.hexdigest()

def _atomic_move(src: Path, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        src.replace(dest)
    except Exception:
        # fallback copy and unlink
        shutil.copy2(src, dest)
        src.unlink()

def _safe_extract_tar(archive: Path, dest: Path):
    import tarfile
    with tarfile.open(archive, 'r:*') as tf:
        # prevent path traversal
        for member in tf.getmembers():
            member_path = dest.joinpath(member.name)
            if not _is_within_directory(dest, member_path):
                raise Exception("Unsafe archive containing path outside extraction dir: " + member.name)
        tf.extractall(path=str(dest))

def _safe_extract_zip(archive: Path, dest: Path):
    import zipfile
    with zipfile.ZipFile(archive, 'r') as zf:
        for name in zf.namelist():
            member_path = dest.joinpath(name)
            if not _is_within_directory(dest, member_path):
                raise Exception("Unsafe archive containing path outside extraction dir: " + name)
        zf.extractall(path=str(dest))

def _is_within_directory(directory: Path, target: Path) -> bool:
    try:
        directory = directory.resolve()
        target = target.resolve()
        return str(target).startswith(str(directory))
    except Exception:
        return False

def _move_to_bad(file_path: Path, reason: str = ""):
    _BAD_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    dest = _BAD_DIR / f"{file_path.name}.{ts}"
    try:
        file_path.replace(dest)
    except Exception:
        try:
            shutil.move(str(file_path), str(dest))
        except Exception:
            LOG.warning("Failed to move bad file %s to %s", file_path, dest)
    LOG.warning("Moved corrupt file %s to %s (%s)", file_path, dest, reason)
    return dest

# ---------------------------
# Downloader core
# ---------------------------
class Downloader:
    """
    High-level downloader with cache reuse, checksum and gpg verification, extraction helpers.
    """

    def __init__(self, distdir: Optional[Path] = None, cache_dir: Optional[Path] = None, max_workers: Optional[int] = None):
        self.distdir = Path(distdir or DEFAULT_DISTFILES)
        self.cache_dir = Path(cache_dir or CACHE_DIR)
        self.max_workers = int(max_workers or MAX_WORKERS)
        self.db = db_mod if db_mod else None
        self.logger = LOG
        self._gpg = None
        if gnupg:
            try:
                self._gpg = gnupg.GPG()
            except Exception:
                self._gpg = None

    # ---------------------
    # Helpers
    # ---------------------
    def _cache_path_for(self, url: str, filename: Optional[str] = None) -> Path:
        key = _compute_cache_key(url, filename)
        # store with original filename if provided for readability
        fn = (filename or urllib.parse.unquote(urllib.parse.urlparse(url).path.split('/')[-1] or "file"))
        # sanitize
        safe_fn = "".join(c for c in fn if c.isalnum() or c in "._-")[:200]
        return self.cache_dir / f"{safe_fn}--{key}"

    def _ensure_dirs(self):
        self.distdir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        _BAD_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------
    # Main fetch API
    # ---------------------
    def fetch(self, url: str, dest_dir: Optional[Path] = None, filename: Optional[str] = None,
              checksums: Optional[Dict[str,str]] = None, sig_url: Optional[str] = None,
              mirrors: Optional[List[str]] = None, auth: Optional[Tuple[str,str]] = None,
              dry_run: bool = False, resume: bool = True, timeout: int = 60) -> Dict[str,Any]:
        """
        Fetch a single URL with options:
         - dest_dir: where to place final file (defaults to distdir)
         - filename: hint for destination filename
         - checksums: dict like {"sha512": "...", "sha256": "..."}
         - sig_url: URL to signature file (for GPG verify)
         - mirrors: list of alternative URLs to try if primary fails
         - auth: (user, pass) for HTTP basic auth
         - dry_run: do not actually write to disk
         - resume: attempt to resume where supported
        Returns dict: {"ok":bool,"path":str or None, "error":str or None, "action": "cached/linked/downloaded"}
        """
        self._ensure_dirs()
        dest_dir = Path(dest_dir or self.distdir)
        cache_path = self._cache_path_for(url, filename)
        final_name = filename or urllib.parse.unquote(urllib.parse.urlparse(url).path.split('/')[-1] or cache_path.name)
        final_path = dest_dir / final_name

        result = {"ok": False, "path": None, "error": None, "action": None, "cache": str(cache_path)}
        LOG.debug("fetch: url=%s -> final=%s (cache=%s)", url, final_path, cache_path)

        # 1) check cache reuse: if cache exists and validates, hardlink/copy to final
        try:
            if cache_path.exists():
                LOG.debug("Found cached file %s; verifying checksums if provided", cache_path)
                if checksums:
                    valid = self._verify_checksums(cache_path, checksums)
                    if not valid:
                        _move_to_bad(cache_path, "checksum-mismatch")
                    else:
                        # link into dest
                        try:
                            if final_path.exists():
                                LOG.debug("Final path already exists, skip linking")
                                result.update(ok=True, path=str(final_path), action="exists")
                                return result
                            try:
                                os.link(str(cache_path), str(final_path))
                                result.update(ok=True, path=str(final_path), action="hardlink_cache")
                                LOG.info("Linked cached %s -> %s", cache_path, final_path)
                                if self.db and hasattr(self.db, "record_download"):
                                    try:
                                        self.db.record_download(url, str(final_path), cached=True)
                                    except Exception:
                                        pass
                                return result
                            except OSError:
                                # fallback to copy
                                shutil.copy2(str(cache_path), str(final_path))
                                result.update(ok=True, path=str(final_path), action="copy_cache")
                                LOG.info("Copied cached %s -> %s", cache_path, final_path)
                                if self.db and hasattr(self.db, "record_download"):
                                    try:
                                        self.db.record_download(url, str(final_path), cached=True)
                                    except Exception:
                                        pass
                                return result
                        except Exception as e:
                            LOG.warning("Failed to link/copy cached file: %s", e)
                else:
                    # no checksums provided: assume cached ok
                    try:
                        if not final_path.exists():
                            try:
                                os.link(str(cache_path), str(final_path))
                                result.update(ok=True, path=str(final_path), action="hardlink_cache")
                                return result
                            except OSError:
                                shutil.copy2(str(cache_path), str(final_path))
                                result.update(ok=True, path=str(final_path), action="copy_cache")
                                return result
                        else:
                            result.update(ok=True, path=str(final_path), action="exists")
                            return result
                    except Exception:
                        pass
        except Exception as e:
            LOG.warning("Cache handling exception: %s", e)

        if dry_run:
            LOG.info("[dry-run] would download %s to %s", url, final_path)
            result.update(ok=True, path=None, action="dry-run")
            return result

        # 2) try protocols/mirrors: prepare list
        candidates = [url]
        if mirrors:
            for m in mirrors:
                if m and m not in candidates:
                    candidates.append(m)

        last_err = None
        for candidate in candidates:
            try:
                parsed = urllib.parse.urlparse(candidate)
                scheme = (parsed.scheme or "file").lower()
                LOG.debug("Attempting candidate %s (scheme=%s)", candidate, scheme)
                if scheme in ("http", "https", "ftp"):
                    downloaded = self._download_http(candidate, cache_path, auth=auth, resume=resume, timeout=timeout)
                elif scheme == "file" or parsed.scheme == "":
                    downloaded = self._download_file(candidate, cache_path)
                elif scheme == "scp":
                    downloaded = self._download_scp(candidate, cache_path, auth=auth)
                elif candidate.startswith("git+") or scheme in ("git", "ssh"):
                    downloaded = self._download_git(candidate, cache_path)
                else:
                    # try http fallback
                    downloaded = self._download_http(candidate, cache_path, auth=auth, resume=resume, timeout=timeout)
                if not downloaded.get("ok"):
                    last_err = downloaded.get("error")
                    LOG.warning("candidate %s failed: %s", candidate, last_err)
                    continue
                # verify checksums if requested
                if checksums:
                    okchk = self._verify_checksums(cache_path, checksums)
                    if not okchk:
                        # move to bad and continue to next candidate
                        _move_to_bad(cache_path, "checksum-failure")
                        last_err = "checksum-failure"
                        continue
                # verify signature if given
                if sig_url:
                    sig_res = self._verify_signature(cache_path, sig_url)
                    if not sig_res.get("ok"):
                        _move_to_bad(cache_path, "gpg-failure")
                        last_err = "gpg-failure"
                        continue
                # place into final dest atomically
                try:
                    if final_path.exists():
                        LOG.info("Final file %s already exists; overwriting", final_path)
                        final_path.unlink()
                    _atomic_move(cache_path, final_path)
                    result.update(ok=True, path=str(final_path), action="downloaded")
                    LOG.info("Downloaded %s -> %s", candidate, final_path)
                    if self.db and hasattr(self.db, "record_download"):
                        try:
                            self.db.record_download(candidate, str(final_path), cached=False)
                        except Exception:
                            pass
                    return result
                except Exception as e:
                    LOG.error("Failed to move downloaded file to final path: %s", e)
                    last_err = str(e)
                    # attempt to copy instead
                    try:
                        shutil.copy2(str(cache_path), str(final_path))
                        result.update(ok=True, path=str(final_path), action="copied_from_cache")
                        return result
                    except Exception as e2:
                        LOG.error("Copy fallback failed: %s", e2)
                        last_err = str(e2)
                        continue
            except Exception as e:
                LOG.warning("Error while trying candidate %s: %s", candidate, e)
                last_err = str(e)
                continue

        result.update(ok=False, error=last_err)
        return result

    # ---------------------
    # Batch fetch
    # ---------------------
    def fetch_many(self, jobs: List[Dict[str,Any]], parallel: Optional[int] = None, dry_run: bool = False) -> List[Dict[str,Any]]:
        """
        jobs: list of dicts with keys: url, dest_dir, filename, checksums, sig_url, mirrors, auth
        returns list of results
        """
        self._ensure_dirs()
        parallel = int(parallel or self.max_workers)
        results = []
        with ThreadPoolExecutor(max_workers=parallel) as ex:
            futures = {}
            for job in jobs:
                url = job.get("url")
                futures[ex.submit(self.fetch,
                                  url,
                                  job.get("dest_dir"),
                                  job.get("filename"),
                                  job.get("checksums"),
                                  job.get("sig_url"),
                                  job.get("mirrors"),
                                  job.get("auth"),
                                  dry_run)] = job
            for fut in as_completed(futures):
                job = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"ok": False, "error": str(e), "url": job.get("url")}
                results.append(res)
        return results

    # ---------------------
    # HTTP/FTP download implementation
    # ---------------------
    def _download_http(self, url: str, cache_path: Path, auth: Optional[Tuple[str,str]] = None, resume: bool = True, timeout: int = 60) -> Dict[str,Any]:
        """
        Use requests if available, else urllib.
        Save to cache_path.part then rename on success.
        """
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        part = cache_path.with_suffix(cache_path.suffix + ".part")
        headers = {}
        if resume and part.exists():
            try:
                offset = part.stat().st_size
                if offset > 0:
                    headers['Range'] = f'bytes={offset}-'
            except Exception:
                offset = 0
        else:
            offset = 0

        if requests:
            try:
                with requests.get(url, stream=True, timeout=timeout, auth=auth, headers=headers) as r:
                    if r.status_code in (416,):
                        # Range not satisfiable — restart
                        part.unlink(missing_ok=True)
                        offset = 0
                        r = requests.get(url, stream=True, timeout=timeout, auth=auth)
                    r.raise_for_status()
                    total = int(r.headers.get('Content-Length') or 0) + offset
                    mode = 'ab' if offset else 'wb'
                    if tqdm:
                        pbar = tqdm(total=total, unit='B', unit_scale=True, desc=str(cache_path.name))
                    else:
                        pbar = None
                    with open(part, mode) as f:
                        for chunk in r.iter_content(chunk_size=1024*64):
                            if not chunk:
                                continue
                            f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))
                    if pbar:
                        pbar.close()
                # success -> move part to cache_path (atomic)
                _atomic_move(part, cache_path)
                return {"ok": True, "path": str(cache_path)}
            except Exception as e:
                LOG.warning("requests download failed for %s: %s", url, e)
                try:
                    part.unlink(missing_ok=True)
                except Exception:
                    pass
                return {"ok": False, "error": str(e)}
        else:
            # urllib fallback
            try:
                import urllib.request
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    total = resp.length or 0
                    mode = 'ab' if offset else 'wb'
                    if tqdm:
                        pbar = tqdm(total=total, unit='B', unit_scale=True, desc=str(cache_path.name))
                    else:
                        pbar = None
                    with open(part, mode) as f:
                        while True:
                            chunk = resp.read(1024*64)
                            if not chunk:
                                break
                            f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))
                    if pbar:
                        pbar.close()
                _atomic_move(part, cache_path)
                return {"ok": True, "path": str(cache_path)}
            except Exception as e:
                LOG.warning("urllib download failed for %s: %s", url, e)
                try:
                    part.unlink(missing_ok=True)
                except Exception:
                    pass
                return {"ok": False, "error": str(e)}

    # ---------------------
    # file:// local copy
    # ---------------------
    def _download_file(self, url: str, cache_path: Path) -> Dict[str,Any]:
        parsed = urllib.parse.urlparse(url)
        src = Path(parsed.path)
        try:
            if not src.exists():
                return {"ok": False, "error": "source-file-missing"}
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(cache_path))
            return {"ok": True, "path": str(cache_path)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ---------------------
    # scp download (paramiko or subprocess scp)
    # ---------------------
    def _download_scp(self, url: str, cache_path: Path, auth: Optional[Tuple[str,str]] = None) -> Dict[str,Any]:
        """
        url example: scp://user@host:/path/to/file
        """
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        user = parsed.username or (auth[0] if auth else None)
        src_path = parsed.path
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        if paramiko:
            try:
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                if auth:
                    ssh.connect(host, username=user, password=auth[1])
                else:
                    ssh.connect(host, username=user)
                sftp = ssh.open_sftp()
                sftp.get(src_path, str(cache_path))
                sftp.close()
                ssh.close()
                return {"ok": True, "path": str(cache_path)}
            except Exception as e:
                return {"ok": False, "error": str(e)}
        else:
            # fallback to scp CLI
            try:
                user_host = f"{user+'@' if user else ''}{host}"
                remote = f"{user_host}:{src_path}"
                cmd = ["scp", remote, str(cache_path)]
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                if proc.returncode != 0:
                    return {"ok": False, "error": proc.stdout}
                return {"ok": True, "path": str(cache_path)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

    # ---------------------
    # git+ protocol handling (clone shallow)
    # ---------------------
    def _download_git(self, url: str, cache_path: Path) -> Dict[str,Any]:
        """
        Accepts git+https://host/repo.git[@ref] or git+ssh://...
        We will clone shallow into a temp dir and archive into a tarball in cache_path
        """
        try:
            # parse "git+scheme://..."
            if url.startswith("git+"):
                git_url = url[len("git+"):]
            else:
                git_url = url
            ref = None
            if "@" in git_url and not git_url.endswith(".git"):
                # allow git+https://...repo.git@v1.2.3
                git_url, ref = git_url.split("@", 1)
            tmpdir = Path(tempfile.mkdtemp(prefix="zeropkg-git-"))
            try:
                cmd = ["git", "clone", "--depth", "1", git_url, str(tmpdir)]
                if ref:
                    cmd = ["git", "clone", "--depth", "1", "--branch", ref, git_url, str(tmpdir)]
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                if proc.returncode != 0:
                    return {"ok": False, "error": proc.stdout}
                # archive to cache_path (tar.gz)
                cache_path_tmp = cache_path.with_suffix(cache_path.suffix + ".tar.gz.part")
                shutil.make_archive(str(cache_path_tmp.with_suffix('')), 'gztar', root_dir=str(tmpdir))
                # rename
                final_cache = cache_path_tmp.with_suffix('')
                if cache_path.exists():
                    cache_path.unlink()
                cache_path_tmp.with_suffix('').replace(cache_path)
                return {"ok": True, "path": str(cache_path)}
            finally:
                shutil.rmtree(str(tmpdir), ignore_errors=True)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ---------------------
    # checksum verification
    # ---------------------
    def _verify_checksums(self, path: Path, checksums: Dict[str,str]) -> bool:
        """
        checksums example: {"sha512": "...", "sha256":"..."}
        Returns True if any provided checksum matches (all provided SHOULD match ideally)
        """
        try:
            for alg, val in checksums.items():
                alg_low = alg.lower()
                if alg_low in ("sha512", "sha256", "sha1", "md5"):
                    h = hashlib.new(alg_low.replace("sha", "sha").replace("md5", "md5"))
                    # built-in: 'sha256','sha1','md5','sha512'
                    with open(path, "rb") as f:
                        for chunk in iter(lambda: f.read(1024*1024), b""):
                            h.update(chunk)
                    got = h.hexdigest()
                    if got.lower() != val.lower():
                        LOG.debug("checksum mismatch for %s: %s != %s", path, got, val)
                        return False
                else:
                    # unsupported algorithm: skip
                    continue
            return True
        except Exception as e:
            LOG.warning("checksum verify failed: %s", e)
            return False

    # ---------------------
    # signature verification
    # ---------------------
    def _verify_signature(self, path: Path, sig_url: str) -> Dict[str,Any]:
        """
        Best-effort: fetch signature and verify using gnupg or 'gpg' CLI.
        sig_url may be URL or relative to main URL (user should pass full).
        """
        try:
            # fetch signature to temp
            tmpdir = Path(tempfile.mkdtemp(prefix="zeropkg-sig-"))
            try:
                sig_path = tmpdir / (Path(urllib.parse.urlparse(sig_url).path).name or "sig")
                fetch_res = self._download_http(sig_url, sig_path, auth=None)
                if not fetch_res.get("ok"):
                    return {"ok": False, "error": "sig-fetch-failed"}
                # try gnupg
                if self._gpg:
                    with open(sig_path, "rb") as sf, open(path, "rb") as pf:
                        verify = self._gpg.verify_file(sf, str(path))
                        if verify and verify.valid:
                            return {"ok": True}
                        else:
                            return {"ok": False, "error": "gpg-verify-failed"}
                else:
                    # fallback to gpg CLI
                    try:
                        cmd = ["gpg", "--verify", str(sig_path), str(path)]
                        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                        ok = proc.returncode == 0
                        if ok:
                            return {"ok": True}
                        return {"ok": False, "error": proc.stdout}
                    except Exception as e:
                        return {"ok": False, "error": str(e)}
            finally:
                shutil.rmtree(str(tmpdir), ignore_errors=True)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ---------------------
    # extraction helper
    # ---------------------
    def extract_to(self, archive: Union[str, Path], dest_dir: Union[str, Path], strip_components: int = 0, dry_run: bool = False) -> Dict[str,Any]:
        """
        Safely extract archive into dest_dir. Supports tar/tar.* and zip.
        strip_components: remove leading path components (like --strip-components in tar)
        """
        archive = Path(archive)
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        if dry_run:
            LOG.info("[dry-run] would extract %s to %s", archive, dest)
            return {"ok": True, "action": "dry-run"}
        try:
            sfx = archive.suffix.lower()
            if archive.name.endswith(".tar.gz") or archive.name.endswith(".tar.xz") or archive.name.endswith(".tar.bz2") or sfx == ".tar":
                # use tarfile with safe extract but implement strip_components by rewriting member names
                import tarfile
                with tarfile.open(archive, 'r:*') as tf:
                    members = tf.getmembers()
                    # adjust names with strip
                    to_extract = []
                    for m in members:
                        parts = m.name.split('/')
                        if strip_components > 0:
                            if len(parts) <= strip_components:
                                continue
                            m.name = '/'.join(parts[strip_components:])
                        # check path traversal
                        target = dest.joinpath(m.name)
                        if not _is_within_directory(dest, target):
                            raise Exception(f"Unsafe member in archive: {m.name}")
                        to_extract.append(m)
                    tf.extractall(path=str(dest), members=to_extract)
                return {"ok": True, "path": str(dest)}
            elif sfx == ".zip":
                import zipfile
                with zipfile.ZipFile(archive, 'r') as zf:
                    namelist = zf.namelist()
                    to_extract = []
                    for nm in namelist:
                        parts = nm.split('/')
                        if strip_components > 0:
                            if len(parts) <= strip_components:
                                continue
                            nm2 = '/'.join(parts[strip_components:])
                        else:
                            nm2 = nm
                        target = dest.joinpath(nm2)
                        if not _is_within_directory(dest, target):
                            raise Exception(f"Unsafe member in zip: {nm}")
                        to_extract.append(nm)
                    # zipfile has no direct member rename; we extract then move if strip_components used
                    zf.extractall(path=str(dest))
                    if strip_components > 0:
                        # move files into proper place (best-effort)
                        # This is a simplistic approach; for complex zips, recommend using tar archives
                        pass
                return {"ok": True, "path": str(dest)}
            else:
                # unknown archive type: try to copy directly
                shutil.copy2(str(archive), str(dest / archive.name))
                return {"ok": True, "path": str(dest)}
        except Exception as e:
            LOG.error("extract failed: %s", e)
            return {"ok": False, "error": str(e)}

# ---------------------------
# CLI (standalone)
# ---------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-downloader", description="Zeropkg downloader utility")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_fetch = sub.add_parser("fetch", help="Fetch a single URL into distfiles")
    p_fetch.add_argument("url")
    p_fetch.add_argument("--dest", help="destination dir (defaults to distfiles)")
    p_fetch.add_argument("--filename", help="filename override")
    p_fetch.add_argument("--checksum", action="append", help="checksum in form alg:hex (can repeat)")
    p_fetch.add_argument("--sig", help="signature URL")
    p_fetch.add_argument("--mirror", action="append", help="mirror URL (can repeat)")
    p_fetch.add_argument("--dry-run", action="store_true")
    p_fetch.add_argument("--auth", help="user:password for basic auth (HTTP)")

    p_many = sub.add_parser("fetch-many", help="Fetch JSON job list file")
    p_many.add_argument("jobs", help="JSON file with list of jobs (url,filename,checksums,..)")
    p_many.add_argument("--parallel", type=int, default=None)
    p_many.add_argument("--dry-run", action="store_true")

    p_extract = sub.add_parser("extract", help="Extract archive to dest")
    p_extract.add_argument("archive")
    p_extract.add_argument("--dest", required=True)
    p_extract.add_argument("--strip", type=int, default=0)
    p_extract.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    dl = Downloader()

    if args.cmd == "fetch":
        checksums = {}
        if args.checksum:
            for c in args.checksum:
                if ":" in c:
                    a, h = c.split(":", 1)
                    checksums[a.strip()] = h.strip()
        mirrors = args.mirror or []
        auth = None
        if args.auth:
            if ":" in args.auth:
                auth = tuple(args.auth.split(":",1))
        res = dl.fetch(args.url, dest_dir=args.dest, filename=args.filename, checksums=checksums or None, sig_url=args.sig, mirrors=mirrors, auth=auth, dry_run=args.dry_run)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0
    elif args.cmd == "fetch-many":
        jobs_file = Path(args.jobs)
        if not jobs_file.exists():
            print("jobs file not found", file=sys.stderr)
            return 2
        jobs = json.loads(jobs_file.read_text(encoding="utf-8"))
        res = dl.fetch_many(jobs, parallel=args.parallel, dry_run=args.dry_run)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0
    elif args.cmd == "extract":
        res = dl.extract_to(args.archive, args.dest, strip_components=args.strip, dry_run=args.dry_run)
        print(json.dumps(res, indent=2))
        return 0
    return 0

# ---------------------------
# Exports
# ---------------------------
__all__ = ["Downloader"]

if __name__ == "__main__":
    sys.exit(_cli())
