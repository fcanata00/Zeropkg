#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_db.py — SQLite state & metadata manager for Zeropkg

Features:
 - Class ZeroPKGDB wrapping DB access and providing high-level API:
     - install_record(), remove_package(), record_upgrade_event()
     - get_package_manifest(), list_installed(), get_orphaned_packages(), find_revdeps()
 - WAL mode, pragmas for performance
 - In-memory caching for frequent reads, with simple invalidation
 - Integration with zeropkg_logger (log_event) if available
 - Snapshot (dump) before critical operations and rollback from snapshot
 - Integrity validation (SHA256) for registered files
 - Export / import helpers
"""

from __future__ import annotations
import os
import sqlite3
import json
import time
import hashlib
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Try to import logger (optional)
try:
    from zeropkg_logger import log_event
    LOG_AVAILABLE = True
except Exception:
    LOG_AVAILABLE = False
    def log_event(pkg, stage, msg, level="info", extra=None):
        # fallback simple print
        print(f"[{level.upper()}] {pkg}:{stage} - {msg}")

# Default DB path
DEFAULT_DB_PATH = Path(os.environ.get("ZEROPKG_DB", "/var/lib/zeropkg/installed.sqlite3"))

# Ensure parent dir exists
DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Thread lock for DB operations
_db_lock = threading.RLock()

# Simple in-memory cache wrapper
class _SimpleCache:
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._ts = 0

    def get(self, key: str):
        return self._cache.get(key)

    def set(self, key: str, value: Any):
        self._cache[key] = value
        self._ts = int(time.time())

    def invalidate(self, key: Optional[str] = None):
        if key:
            self._cache.pop(key, None)
        else:
            self._cache.clear()
            self._ts = int(time.time())

    def snapshot(self) -> Dict[str, Any]:
        return dict(self._cache)

    def load_snapshot(self, snap: Dict[str, Any]):
        self._cache = dict(snap)
        self._ts = int(time.time())

# Utility helpers
def _sha256_of_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _now_ts() -> int:
    return int(time.time())

class ZeroPKGDB:
    """
    High-level DB manager for Zeropkg.
    Use as context manager:
        with ZeroPKGDB() as db:
            db.record_install_quick(...)
    Or instantiate and call methods.
    """
    def __init__(self, db_path: Optional[Path] = None, pragmas: Optional[Dict[str, Any]] = None):
        self.db_path = Path(db_path or DEFAULT_DB_PATH)
        self.pragmas = pragmas or {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "temp_store": "MEMORY",
            "foreign_keys": 1,
            "cache_size": -20000
        }
        self._conn: Optional[sqlite3.Connection] = None
        self.cache = _SimpleCache()
        self._ensure_db()

    def _connect(self):
        if self._conn:
            return self._conn
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # apply pragmas
        for k, v in self.pragmas.items():
            try:
                if k == "journal_mode":
                    cur.execute(f"PRAGMA journal_mode={v}")
                else:
                    cur.execute(f"PRAGMA {k}={v}")
            except Exception:
                pass
        self._conn = conn
        return conn

    def _ensure_db(self):
        with _db_lock:
            conn = self._connect()
            cur = conn.cursor()
            # Create tables if not exists (idempotent)
            cur.executescript("""
            CREATE TABLE IF NOT EXISTS packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                version TEXT,
                install_ts INTEGER,
                install_size INTEGER,
                metadata JSON
            );
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pkg_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                sha256 TEXT,
                size INTEGER,
                mtime INTEGER,
                FOREIGN KEY(pkg_id) REFERENCES packages(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pkg_id INTEGER NOT NULL,
                dependee TEXT NOT NULL,
                version_req TEXT,
                FOREIGN KEY(pkg_id) REFERENCES packages(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                type TEXT,
                pkg TEXT,
                payload JSON
            );
            CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
            CREATE INDEX IF NOT EXISTS idx_deps_pkg ON dependencies(pkg_id);
            """)
            conn.commit()

    def close(self):
        with _db_lock:
            if self._conn:
                try:
                    self._conn.commit()
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # ------------------------------
    # Low level exec helpers
    # ------------------------------
    def _execute(self, sql: str, params: Tuple = (), commit: bool = False) -> sqlite3.Cursor:
        with _db_lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(sql, params)
            if commit:
                conn.commit()
            return cur

    def _executemany(self, sql: str, seq_of_params: List[Tuple], commit: bool = False) -> None:
        with _db_lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.executemany(sql, seq_of_params)
            if commit:
                conn.commit()

    # ------------------------------
    # High-level operations
    # ------------------------------
    def record_install_quick(self, pkg_name: str, manifest: Dict[str, Any], deps: List[Dict[str,Any]] = None, metadata: Dict[str,Any] = None) -> bool:
        """
        Record a package installation using its manifest dictionary.
        manifest: {"files": [{"dst": "/usr/bin/...", "sha256": "...", "size": ...}, ...], "version": "x.y"}
        deps: list of dicts { "name": "...", "version": ">=1.2" }
        metadata: additional metadata dict
        """
        deps = deps or []
        metadata = metadata or {}
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                version = manifest.get("version") or metadata.get("version")
                install_size = sum([f.get("size", 0) for f in manifest.get("files", [])])
                ts = _now_ts()
                # upsert package
                cur.execute("INSERT OR REPLACE INTO packages (id, name, version, install_ts, install_size, metadata) VALUES ((SELECT id FROM packages WHERE name=?), ?, ?, ?, ?, ?)",
                            (pkg_name, pkg_name, version, ts, install_size, json.dumps(metadata)))
                # get pkg_id
                cur.execute("SELECT id FROM packages WHERE name=?", (pkg_name,))
                row = cur.fetchone()
                if not row:
                    raise RuntimeError("failed to get pkg id")
                pkg_id = row["id"]
                # remove old files and deps
                cur.execute("DELETE FROM files WHERE pkg_id=?", (pkg_id,))
                cur.execute("DELETE FROM dependencies WHERE pkg_id=?", (pkg_id,))
                # insert files
                files_inserts = []
                for f in manifest.get("files", []):
                    dst = f.get("dst")
                    sha = f.get("sha256") or f.get("sha")
                    size = f.get("size") or (Path(dst).stat().st_size if Path(dst).exists() else 0)
                    mtime = int(Path(dst).stat().st_mtime) if Path(dst).exists() else int(time.time())
                    files_inserts.append((pkg_id, dst, sha, size, mtime))
                if files_inserts:
                    cur.executemany("INSERT INTO files (pkg_id, path, sha256, size, mtime) VALUES (?, ?, ?, ?, ?)", files_inserts)
                # insert deps
                deps_inserts = []
                for d in deps:
                    deps_inserts.append((pkg_id, d.get("name"), d.get("version")))
                if deps_inserts:
                    cur.executemany("INSERT INTO dependencies (pkg_id, dependee, version_req) VALUES (?, ?, ?)", deps_inserts)
                # record event
                payload = {"manifest": manifest, "deps": deps, "metadata": metadata}
                cur.execute("INSERT INTO events (ts, type, pkg, payload) VALUES (?, 'install', ?, ?)", (ts, pkg_name, json.dumps(payload)))
                conn.commit()
                # invalidate cache entries relevant
                self.cache.invalidate(pkg_name)
                # optional logger
                try:
                    log_event(pkg_name, "db", f"Recorded install {pkg_name} version={version}", level="info", extra={"size": install_size})
                except Exception:
                    pass
                return True
            except Exception as e:
                try:
                    log_event("zeropkg.db", "record_install", f"error: {e}", level="error")
                except Exception:
                    pass
                return False

    def remove_package_quick(self, pkg_name: str) -> bool:
        """
        Remove package record and its files from DB (does not touch filesystem).
        """
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                # ensure exists
                cur.execute("SELECT id FROM packages WHERE name=?", (pkg_name,))
                r = cur.fetchone()
                if not r:
                    return False
                pkg_id = r["id"]
                cur.execute("DELETE FROM files WHERE pkg_id=?", (pkg_id,))
                cur.execute("DELETE FROM dependencies WHERE pkg_id=?", (pkg_id,))
                cur.execute("DELETE FROM packages WHERE id=?", (pkg_id,))
                cur.execute("INSERT INTO events (ts, type, pkg, payload) VALUES (?, 'remove', ?, ?)", (_now_ts(), pkg_name, json.dumps({"removed": True})))
                conn.commit()
                self.cache.invalidate(pkg_name)
                try:
                    log_event(pkg_name, "db", "Recorded removal", level="info")
                except Exception:
                    pass
                return True
            except Exception as e:
                try:
                    log_event("zeropkg.db", "remove_package", f"error: {e}", level="error")
                except Exception:
                    pass
                return False

    def record_upgrade_event(self, pkg_name: str, old_version: Optional[str], new_version: Optional[str], success: bool = True, extra: Optional[Dict[str,Any]] = None) -> bool:
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                payload = {"old": old_version, "new": new_version, "success": bool(success), "extra": extra or {}}
                cur.execute("INSERT INTO events (ts, type, pkg, payload) VALUES (?, 'upgrade', ?, ?)", (_now_ts(), pkg_name, json.dumps(payload)))
                conn.commit()
                try:
                    log_event(pkg_name, "db", f"Upgrade recorded {old_version} -> {new_version}", level="info", extra=payload)
                except Exception:
                    pass
                return True
            except Exception as e:
                try:
                    log_event("zeropkg.db", "record_upgrade", f"error: {e}", level="error")
                except Exception:
                    pass
                return False

    # ------------------------------
    # Queries / utilities
    # ------------------------------
    def get_package_manifest(self, pkg_name: str) -> Optional[Dict[str,Any]]:
        """
        Return manifest-like dict for package (files list with sha/size/mtime).
        """
        cached = self.cache.get(f"manifest:{pkg_name}")
        if cached:
            return cached
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                cur.execute("SELECT id, name, version, install_ts, install_size, metadata FROM packages WHERE name=?", (pkg_name,))
                p = cur.fetchone()
                if not p:
                    return None
                pkg_id = p["id"]
                cur.execute("SELECT path, sha256, size, mtime FROM files WHERE pkg_id=?", (pkg_id,))
                files = []
                for row in cur.fetchall():
                    files.append({"dst": row["path"], "sha256": row["sha256"], "size": row["size"], "mtime": row["mtime"]})
                manifest = {"package": {"name": p["name"], "version": p["version"]}, "files": files, "install_ts": p["install_ts"], "install_size": p["install_size"], "metadata": json.loads(p["metadata"]) if p["metadata"] else {}}
                self.cache.set(f"manifest:{pkg_name}", manifest)
                return manifest
            except Exception as e:
                log_event("zeropkg.db", "get_manifest", f"error: {e}", level="error")
                return None

    def list_installed_quick(self) -> List[Dict[str,Any]]:
        """
        Return list of installed packages with basic metadata.
        """
        cached = self.cache.get("installed:list")
        if cached:
            return cached
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                cur.execute("SELECT name, version, install_ts, install_size FROM packages ORDER BY name")
                rows = []
                for r in cur.fetchall():
                    rows.append({"name": r["name"], "version": r["version"], "install_ts": r["install_ts"], "size": r["install_size"]})
                self.cache.set("installed:list", rows)
                return rows
            except Exception as e:
                log_event("zeropkg.db", "list_installed", f"error: {e}", level="error")
                return []

    def get_orphaned_packages(self) -> List[str]:
        """
        Identify orphan packages: installed packages that are not required by any other installed package.
        """
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                # packages that appear as dependee in dependencies
                cur.execute("SELECT DISTINCT pkg_id FROM dependencies")
                dep_pkg_ids = set([r["pkg_id"] for r in cur.fetchall()])
                # packages referenced as dependee by name
                cur.execute("SELECT DISTINCT dependee FROM dependencies")
                dependees = set([r["dependee"] for r in cur.fetchall()])
                # collect all package names
                cur.execute("SELECT id, name FROM packages")
                orphans = []
                id_to_name = {}
                for r in cur.fetchall():
                    id_to_name[r["id"]] = r["name"]
                # An orphan is a package with no other package depending on it (no record where dependee == its name)
                for pid, name in id_to_name.items():
                    if name not in dependees:
                        # exclude essential core packages? leave to caller
                        orphans.append(name)
                return orphans
            except Exception as e:
                log_event("zeropkg.db", "orphans", f"error: {e}", level="error")
                return []

    def find_revdeps(self, pkg_name: str) -> List[str]:
        """
        Return list of package names that depend on the given package (reverse deps).
        """
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                # find packages whose dependencies list includes pkg_name as dependee
                cur.execute("SELECT p.name FROM dependencies d JOIN packages p ON p.id = d.pkg_id WHERE d.dependee = ?", (pkg_name,))
                return [r["name"] for r in cur.fetchall()]
            except Exception as e:
                log_event("zeropkg.db", "revdeps", f"error: {e}", level="error")
                return []

    def export_db(self, dest_path: Path, compress: bool = True) -> Path:
        """
        Export DB to file (SQLite copy). If compress True, create .tar.gz containing dump.
        Returns path to created file.
        """
        with _db_lock:
            dest_path = Path(dest_path)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            tmpf = tempfile.NamedTemporaryFile(delete=False)
            tmpf.close()
            # Use sqlite online backup API
            try:
                src_conn = self._connect()
                bconn = sqlite3.connect(tmpf.name)
                with bconn:
                    src_conn.backup(bconn)
                bconn.close()
                if compress:
                    archive = str(dest_path)
                    shutil.make_archive(base_name=str(dest_path), format="gztar", root_dir=os.path.dirname(tmpf.name), base_dir=os.path.basename(tmpf.name))
                    os.unlink(tmpf.name)
                    return Path(archive + ".tar.gz") if not str(dest_path).endswith(".tar.gz") else Path(archive)
                else:
                    shutil.move(tmpf.name, str(dest_path))
                    return dest_path
            except Exception as e:
                try:
                    log_event("zeropkg.db", "export", f"error: {e}", level="error")
                except Exception:
                    pass
                raise

    def import_db(self, src_path: Path, replace: bool = False) -> bool:
        """
        Import DB from SQLite file or tar.gz produced by export_db. If replace True, replace active DB.
        """
        with _db_lock:
            try:
                src = Path(src_path)
                if not src.exists():
                    raise FileNotFoundError(src)
                # if tar.gz, extract
                tmpdir = tempfile.mkdtemp(prefix="zeropkg-import-")
                if str(src).endswith(".tar.gz") or str(src).endswith(".tgz"):
                    shutil.unpack_archive(str(src), tmpdir)
                    # assume contains one file
                    files = list(Path(tmpdir).glob("*"))
                    if not files:
                        raise RuntimeError("import archive empty")
                    src_file = files[0]
                else:
                    src_file = src
                if replace:
                    # backup current DB
                    bak = str(self.db_path) + ".bak." + str(int(time.time()))
                    shutil.copy2(str(self.db_path), bak)
                    shutil.copy2(str(src_file), str(self.db_path))
                    # reconnect
                    self.close()
                    self._connect()
                else:
                    # merge: open both and copy entries (simple approach: attach db and copy)
                    conn = self._connect()
                    cur = conn.cursor()
                    cur.execute(f"ATTACH DATABASE '{str(src_file)}' AS srcdb")
                    # copy packages and files where not exist (best-effort)
                    cur.execute("""
                    INSERT OR IGNORE INTO packages (id, name, version, install_ts, install_size, metadata)
                    SELECT id, name, version, install_ts, install_size, metadata FROM srcdb.packages
                    """)
                    cur.execute("""
                    INSERT OR IGNORE INTO files (id, pkg_id, path, sha256, size, mtime)
                    SELECT id, pkg_id, path, sha256, size, mtime FROM srcdb.files
                    """)
                    cur.execute("""
                    INSERT INTO events (ts, type, pkg, payload)
                    SELECT ts, type, pkg, payload FROM srcdb.events
                    """)
                    conn.commit()
                    cur.execute("DETACH DATABASE srcdb")
                self.cache.invalidate()
                return True
            except Exception as e:
                log_event("zeropkg.db", "import", f"error: {e}", level="error")
                return False

    def validate_integrity(self, pkg_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate SHA256 and sizes for files recorded in DB.
        If pkg_name is None, validate all packages.
        Returns dict with results per package.
        """
        results = {}
        with _db_lock:
            try:
                conn = self._connect()
                cur = conn.cursor()
                if pkg_name:
                    cur.execute("SELECT id, name FROM packages WHERE name=?", (pkg_name,))
                    rows = cur.fetchall()
                else:
                    cur.execute("SELECT id, name FROM packages")
                    rows = cur.fetchall()
                for r in rows:
                    pid = r["id"]
                    name = r["name"]
                    cur.execute("SELECT path, sha256, size FROM files WHERE pkg_id=?", (pid,))
                    bad = []
                    for f in cur.fetchall():
                        p = f["path"]
                        expected_sha = f["sha256"]
                        expected_size = f["size"]
                        actual_sha = _sha256_of_file(p) if os.path.exists(p) else None
                        actual_size = os.path.getsize(p) if os.path.exists(p) else None
                        if expected_sha and actual_sha != expected_sha:
                            bad.append({"path": p, "error": "sha-mismatch", "expected": expected_sha, "actual": actual_sha})
                        elif expected_size and actual_size != expected_size:
                            bad.append({"path": p, "error": "size-mismatch", "expected": expected_size, "actual": actual_size})
                    results[name] = {"ok": len(bad) == 0, "issues": bad}
                return results
            except Exception as e:
                log_event("zeropkg.db", "validate", f"error: {e}", level="error")
                return {}

    # ------------------------------
    # Snapshot / rollback (simple file copy)
    # ------------------------------
    def snapshot(self, dest_dir: Optional[Path] = None) -> Path:
        """
        Save a copy of the DB file to dest_dir and return path.
        """
        with _db_lock:
            dest_dir = Path(dest_dir or (self.db_path.parent / "snapshots"))
            dest_dir.mkdir(parents=True, exist_ok=True)
            snap_path = dest_dir / f"installed.sqlite3.{int(time.time())}.snapshot"
            self.close()
            shutil.copy2(str(self.db_path), str(snap_path))
            # reconnect
            self._connect()
            return snap_path

    def rollback_from_snapshot(self, snapshot_path: Path) -> bool:
        with _db_lock:
            try:
                if not Path(snapshot_path).exists():
                    return False
                self.close()
                shutil.copy2(str(snapshot_path), str(self.db_path))
                self._connect()
                self.cache.invalidate()
                log_event("zeropkg.db", "rollback", f"Rolled back DB from {snapshot_path}", level="warning")
                return True
            except Exception as e:
                log_event("zeropkg.db", "rollback", f"error: {e}", level="error")
                return False

    # ------------------------------
    # Convenience wrapper: install (builder -> installer handshake)
    # ------------------------------
    def install(self, pkg_name: str, manifest: Dict[str,Any], deps: List[Dict[str,Any]] = None, metadata: Dict[str,Any] = None) -> bool:
        """
        High-level helper for installer to call after successful install.
        Calls record_install_quick and emits event.
        """
        ok = self.record_install_quick(pkg_name, manifest, deps=deps, metadata=metadata)
        if ok:
            try:
                log_event(pkg_name, "db", f"install recorded (install helper)", level="info")
            except Exception:
                pass
        return ok

    # ------------------------------
    # Helper: get packages that depend on pkg_name recursively (deep revdeps)
    # ------------------------------
    def find_revdeps_recursive(self, pkg_name: str) -> List[str]:
        """
        Return reverse dependencies recursively (breadth first).
        """
        found = set()
        queue = [pkg_name]
        while queue:
            current = queue.pop(0)
            revs = self.find_revdeps(current)
            for r in revs:
                if r not in found:
                    found.add(r)
                    queue.append(r)
        return list(found)

# Convenience module-level functions for backward compatibility
_default_db = None
def _get_default_db():
    global _default_db
    if _default_db is None:
        _default_db = ZeroPKGDB()
    return _default_db

def record_install_quick(pkg_name: str, manifest: Dict[str,Any], deps: List[Dict[str,Any]] = None, metadata: Dict[str,Any] = None) -> bool:
    return _get_default_db().record_install_quick(pkg_name, manifest, deps=deps, metadata=metadata)

def remove_package_quick(pkg_name: str) -> bool:
    return _get_default_db().remove_package_quick(pkg_name)

def record_upgrade_event(event: Dict[str,Any]) -> bool:
    try:
        pkg = event.get("pkg") or event.get("package")
        old = event.get("old")
        new = event.get("new")
        ok = _get_default_db().record_upgrade_event(pkg, old, new, success=event.get("success", True), extra=event.get("extra"))
        return ok
    except Exception:
        return False

def get_package_manifest(pkg_name: str) -> Optional[Dict[str,Any]]:
    return _get_default_db().get_package_manifest(pkg_name)

def list_installed_quick() -> List[Dict[str,Any]]:
    return _get_default_db().list_installed_quick()

def get_orphaned_packages() -> List[str]:
    return _get_default_db().get_orphaned_packages()

def find_revdeps(pkg_name: str) -> List[str]:
    return _get_default_db().find_revdeps(pkg_name)

# End of module
