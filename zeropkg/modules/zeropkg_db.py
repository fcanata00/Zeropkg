#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_db.py — Database manager for Zeropkg (extended)

Augmented with:
 - record_install(name_or_namever, manifest, version=None, deps=None, metadata=None)
 - get_manifest(name_or_namever) -> returns categorized manifest dict
 - convenience quick wrappers for record_install/get_manifest
 - keeps existing API (add_package, remove_package, list_installed, etc.)

The DB is SQLite (path defined via zeropkg_config or default /var/lib/zeropkg/installed.sqlite3)
"""

from __future__ import annotations
import sqlite3
import os
import shutil
import json
import time
import tempfile
import re
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

# integrate with config/logger if present
try:
    from zeropkg_config import load_config, get_db_path
except Exception:
    def load_config(path=None):
        return {"paths": {"db_path": "/var/lib/zeropkg/installed.sqlite3"}}
    def get_db_path(cfg=None):
        return "/var/lib/zeropkg/installed.sqlite3"

try:
    from zeropkg_logger import log_event, get_logger
    _logger = get_logger("db")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_db")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler())

_DB_LOCK = RLock()  # protect sqlite connection use across threads/processes

# SQL schema
_SCHEMA = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    category TEXT,
    installed_at INTEGER NOT NULL,
    metadata TEXT,            -- json blob with extra metadata
    UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    mtime INTEGER,
    size INTEGER,
    FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id INTEGER NOT NULL,
    dep_name TEXT NOT NULL,
    dep_version_req TEXT,
    FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pkg_name TEXT,
    event_type TEXT,
    payload TEXT,
    ts INTEGER
);

CREATE TABLE IF NOT EXISTS upgrade_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pkg_name TEXT NOT NULL,
    old_version TEXT,
    new_version TEXT,
    ts INTEGER
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_files_path ON files(file_path);
CREATE INDEX IF NOT EXISTS idx_deps_depname ON dependencies(dep_name);
CREATE INDEX IF NOT EXISTS idx_packages_name ON packages(name);
CREATE INDEX IF NOT EXISTS idx_events_pkg ON events(pkg_name);
"""

class DBError(RuntimeError):
    pass

class DBManager:
    """
    DBManager provides a transactional interface to the Zeropkg SQLite DB.

    Usage:
        with DBManager() as db:
            db.add_package(...)
            db.record_install(...)
    """
    def __init__(self, db_path: Optional[str] = None):
        cfg = load_config() if db_path is None else None
        self.db_path = db_path or (get_db_path(cfg) if cfg is not None else get_db_path())
        self.db_path = str(Path(self.db_path).expanduser())
        os.makedirs(str(Path(self.db_path).parent), exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._in_transaction = False

    def _connect(self):
        if self._conn:
            return
        # check_same_thread=False to allow using from threads; we still guard with RLock
        self._conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # performance pragma
        self._conn.execute("PRAGMA journal_mode = WAL;")
        self._conn.execute("PRAGMA synchronous = NORMAL;")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # Context manager protocol
    def __enter__(self):
        _DB_LOCK.acquire()
        try:
            self._connect()
            self._in_transaction = True
            return self
        except Exception as e:
            _DB_LOCK.release()
            raise DBError(f"DB connect error: {e}") from e

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                try:
                    self._conn.commit()
                except Exception as e:
                    _logger.exception("Commit failed")
                    raise DBError(f"Commit failed: {e}") from e
            else:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
            self._in_transaction = False
        finally:
            self.close()
            _DB_LOCK.release()

    # low-level helpers
    def _execute(self, sql: str, params: Tuple = ()):
        if self._conn is None:
            raise DBError("DB not connected")
        cur = self._conn.execute(sql, params)
        return cur

    def _many(self, sql: str, params: Tuple = ()):
        cur = self._execute(sql, params)
        return cur.fetchall()

    # -----------------------
    # helpers for name/version parsing
    # -----------------------
    @staticmethod
    def _split_name_version(name_or_namever: str) -> Tuple[str, Optional[str]]:
        """
        Try to split a string like 'pkg-1.2.3' into ('pkg','1.2.3').
        Heuristic: last dash followed by digit starts version.
        Returns (name, version_or_None)
        """
        if not name_or_namever:
            return (name_or_namever, None)
        m = re.match(r"^(?P<name>.+)-(?P<ver>\d[\w\.\+\-]*)$", name_or_namever)
        if m:
            return (m.group("name"), m.group("ver"))
        return (name_or_namever, None)

    # -----------------------
    # High-level API (existing)
    # -----------------------
    def add_package(self, name: str, version: str, files: List[str], deps: List[Dict[str, str]] = None, metadata: Dict[str, Any] = None):
        """
        Register a package and its files + dependencies.
        `deps` is list of dicts: {"dep_name": "...", "dep_version_req": "..."}
        `metadata` stored as json blob.
        """
        ts = int(time.time())
        deps = deps or []
        metadata = metadata or {}

        # check if package exists same version
        existing = self.get_package(name, version)
        if existing:
            _logger.info(f"Package {name}-{version} already registered")
            return existing["id"]

        cur = self._execute(
            "INSERT INTO packages (name, version, category, installed_at, metadata) VALUES (?, ?, ?, ?, ?)",
            (name, version, None, ts, json.dumps(metadata))
        )
        pkg_id = cur.lastrowid

        for f in files:
            try:
                p = Path(f)
                mtime = int(p.stat().st_mtime) if p.exists() else None
                size = int(p.stat().st_size) if p.exists() else None
            except Exception:
                mtime = None
                size = None
            self._execute(
                "INSERT INTO files (package_id, file_path, mtime, size) VALUES (?, ?, ?, ?)",
                (pkg_id, str(f), mtime, size)
            )

        for d in deps:
            dep_name = d.get("dep_name") or d.get("name") or d
            dep_version_req = d.get("dep_version_req") or d.get("version") or None
            self._execute(
                "INSERT INTO dependencies (package_id, dep_name, dep_version_req) VALUES (?, ?, ?)",
                (pkg_id, dep_name, dep_version_req)
            )

        self._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                      (name, "install", json.dumps({"version": version}), ts))
        _logger.info(f"Registered package {name}-{version} (id={pkg_id})")
        return pkg_id

    def remove_package(self, name: str, version: Optional[str] = None) -> List[str]:
        """
        Remove a package (and cascade files/deps). Returns list of file paths that were removed from DB.
        If version is None, removes all versions.
        """
        q = "SELECT id FROM packages WHERE name = ?" + (" AND version = ?" if version else "")
        params = (name, version) if version else (name,)
        rows = self._many(q, params)
        removed_paths: List[str] = []
        now = int(time.time())
        for r in rows:
            pkg_id = r["id"]
            files = [row["file_path"] for row in self._many("SELECT file_path FROM files WHERE package_id = ?", (pkg_id,))]
            removed_paths.extend(files)
            # delete package (cascade)
            self._execute("DELETE FROM packages WHERE id = ?", (pkg_id,))
            self._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                          (name, "remove", json.dumps({"version": version}), now))
            _logger.info(f"Removed package {name} (id={pkg_id})")
        return removed_paths

    def list_installed(self) -> List[Dict[str, Any]]:
        rows = self._many("SELECT id, name, version, installed_at, metadata FROM packages ORDER BY name, version")
        return [dict(r) for r in rows]

    def get_package(self, name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if version:
            row = self._many("SELECT * FROM packages WHERE name = ? AND version = ?", (name, version))
        else:
            row = self._many("SELECT * FROM packages WHERE name = ? ORDER BY installed_at DESC LIMIT 1", (name,))
        if not row:
            return None
        r = row[0]
        return dict(r)

    def get_files_for_pkg(self, name: str, version: Optional[str] = None) -> List[str]:
        pkg = self.get_package(name, version)
        if not pkg:
            return []
        pkg_id = pkg["id"]
        rows = self._many("SELECT file_path FROM files WHERE package_id = ?", (pkg_id,))
        return [r["file_path"] for r in rows]

    def find_revdeps(self, package_name: str) -> List[str]:
        """
        Find packages that depend on package_name (reverse dependencies).
        """
        rows = self._many("SELECT p.name, p.version FROM dependencies d JOIN packages p ON d.package_id = p.id WHERE d.dep_name = ?",
                          (package_name,))
        return [f"{r['name']}-{r['version']}" for r in rows]

    def get_dependency_tree(self, package_name: str) -> Dict[str, List[str]]:
        """
        Return dependency tree dict: {pkg: [deps...]} for the package (single latest version).
        """
        pkg = self.get_package(package_name)
        if not pkg:
            return {}
        pkg_id = pkg["id"]
        res = {}
        visited = set()

        def _walk(pid):
            rows = self._many("SELECT dep_name FROM dependencies WHERE package_id = ?", (pid,))
            deps = [r["dep_name"] for r in rows]
            return deps

        def _recurse(name):
            if name in visited:
                return
            visited.add(name)
            p = self.get_package(name)
            if not p:
                res[name] = []
                return
            deps = _walk(p["id"])
            res[name] = deps
            for d in deps:
                _recurse(d)

        _recurse(package_name)
        return res

    def get_all_dependencies(self) -> Dict[str, List[str]]:
        """
        Return dependencies for all installed packages as dict.
        """
        rows = self._many("SELECT p.name as pkg, d.dep_name as dep FROM dependencies d JOIN packages p ON d.package_id = p.id")
        out: Dict[str, List[str]] = {}
        for r in rows:
            out.setdefault(r["pkg"], []).append(r["dep"])
        return out

    def get_orphaned_packages(self) -> List[str]:
        """
        Return packages that no other installed package depends on (candidates for depclean).
        Excludes packages marked as 'manual' in metadata (if metadata contains manual:true).
        """
        rows = self._many("""
            SELECT p.name, p.version, p.metadata FROM packages p
            WHERE p.id NOT IN (SELECT DISTINCT package_id FROM dependencies)
        """)
        orphans = []
        for r in rows:
            try:
                meta = json.loads(r["metadata"] or "{}")
            except Exception:
                meta = {}
            if meta.get("manual"):
                continue
            orphans.append(f"{r['name']}-{r['version']}")
        return orphans

    def get_outdated_packages(self, version_map: Optional[Dict[str, str]] = None) -> List[Tuple[str, str, str]]:
        """
        Determine packages with versions older than `version_map` (pkg -> latest_ver).
        If version_map not provided, returns empty. Returns list of tuples (pkg, installed_ver, latest_ver).
        """
        if not version_map:
            return []
        out = []
        rows = self._many("SELECT name, version FROM packages")
        for r in rows:
            name = r["name"]
            ver = r["version"]
            latest = version_map.get(name)
            if latest and latest != ver:
                out.append((name, ver, latest))
        return out

    def record_upgrade(self, name: str, old_version: str, new_version: str):
        ts = int(time.time())
        self._execute("INSERT INTO upgrade_history (pkg_name, old_version, new_version, ts) VALUES (?, ?, ?, ?)",
                      (name, old_version, new_version, ts))
        _logger.info(f"Recorded upgrade {name}: {old_version} -> {new_version}")

    def validate_integrity(self) -> Dict[str, Any]:
        """
        Check that files registered in DB actually exist on filesystem and sizes/mtimes match.
        Returns a report dict with missing and mismatched files.
        """
        report = {"missing": [], "mismatch": []}
        rows = self._many("SELECT f.file_path, f.size, f.mtime, p.name, p.version FROM files f JOIN packages p ON f.package_id = p.id")
        for r in rows:
            fp = r["file_path"]
            try:
                p = Path(fp)
                if not p.exists():
                    report["missing"].append(fp)
                else:
                    m = int(p.stat().st_mtime)
                    s = int(p.stat().st_size)
                    if r["mtime"] is not None and r["mtime"] != m:
                        report["mismatch"].append({"file": fp, "kind": "mtime", "db": r["mtime"], "fs": m})
                    if r["size"] is not None and r["size"] != s:
                        report["mismatch"].append({"file": fp, "kind": "size", "db": r["size"], "fs": s})
            except Exception:
                report["missing"].append(fp)
        return report

    def export_db(self, dest_path: str):
        """
        Export a copy of the DB file to dest_path (atomic copy).
        """
        dest = Path(dest_path).expanduser()
        dest.parent.mkdir(parents=True, exist_ok=True)
        # ensure flush
        if self._conn:
            self._conn.commit()
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(dest.parent))
        tmp.close()
        try:
            shutil.copy2(self.db_path, tmp.name)
            os.replace(tmp.name, str(dest))
            _logger.info(f"Exported DB to {dest}")
        except Exception as e:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            raise DBError(f"Export failed: {e}")

    def import_db(self, src_path: str, overwrite: bool = False):
        """
        Import DB file. If overwrite is True, replace existing DB file.
        """
        src = Path(src_path).expanduser()
        if not src.exists():
            raise DBError("Source DB not found")
        if os.path.exists(self.db_path) and not overwrite:
            raise DBError("DB exists — pass overwrite=True to replace")
        # close existing connection if any
        self.close()
        shutil.copy2(str(src), self.db_path)
        _logger.info(f"Imported DB from {src}")

    # ---------------------------
    # New methods for installer integration
    # ---------------------------
    def _categorize_paths(self, paths: List[str]) -> Dict[str, List[str]]:
        """
        Simple heuristic to categorize a list of file paths into keys:
        bin, sbin, lib, include, doc, man, conf, other
        """
        mapping = {
            "bin": [],
            "sbin": [],
            "lib": [],
            "include": [],
            "doc": [],
            "man": [],
            "conf": [],
            "other": []
        }
        for p in paths:
            # normalize leading slash
            rp = p if p.startswith("/") else "/" + p
            parts = rp.split("/")
            # heuristics (similar to installer)
            if ("/usr/bin/" in rp) or (rp.startswith("/bin/")) or ("/bin/" in rp and len(parts) > 2 and parts[1] == "bin"):
                mapping["bin"].append(rp)
            elif ("/usr/sbin/" in rp) or (rp.startswith("/sbin/")):
                mapping["sbin"].append(rp)
            elif ("/lib/" in rp) or ("/lib64/" in rp) or ("/usr/lib/" in rp):
                mapping["lib"].append(rp)
            elif "/include/" in rp or rp.endswith(".h"):
                mapping["include"].append(rp)
            elif "/share/man/" in rp or re.search(r"/man[0-9]/", rp):
                mapping["man"].append(rp)
            elif "/share/doc/" in rp or "/doc/" in rp:
                mapping["doc"].append(rp)
            elif rp.startswith("/etc/") or "/etc/" in rp:
                mapping["conf"].append(rp)
            else:
                mapping["other"].append(rp)
        # dedupe and sort
        for k in mapping:
            mapping[k] = sorted(set(mapping[k]))
        return mapping

    def record_install(self, name_or_namever: str, manifest: Dict[str, List[str]],
                       version: Optional[str] = None, deps: Optional[List[Dict[str, str]]] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> int:
        """
        High-level helper to register an installed package from a manifest dict.
        name_or_namever can be 'pkg' or 'pkg-1.2.3'. If version provided, it overrides parsing.
        manifest: dict with categories->list-of-paths (absolute or relative)
        Returns package_id.
        """
        deps = deps or []
        metadata = metadata or {}
        # parse name/version
        parsed_name, parsed_version = self._split_name_version(name_or_namever)
        ver = version or parsed_version or "0"
        name = parsed_name

        # flatten files list
        files: List[str] = []
        for cat, lst in (manifest or {}).items():
            for p in lst:
                # ensure absolute-like path stored
                if not p:
                    continue
                if not p.startswith("/"):
                    files.append("/" + p.lstrip("/"))
                else:
                    files.append(p)

        # include original manifest in metadata for future reference
        metadata = dict(metadata)
        metadata["manifest"] = manifest

        # call existing add_package
        pkg_id = self.add_package(name=name, version=ver, files=files, deps=deps, metadata=metadata)
        _logger.info(f"record_install: registered {name}-{ver} with {len(files)} files (pkg_id={pkg_id})")
        return pkg_id

    def get_manifest(self, name_or_namever: str) -> Optional[Dict[str, List[str]]]:
        """
        Return the categorized manifest for a package (if stored).
        Accepts 'pkg' or 'pkg-1.2.3'. If version not provided, returns latest installed version.
        """
        parsed_name, parsed_version = self._split_name_version(name_or_namever)
        name = parsed_name
        version = parsed_version

        pkg = None
        if version:
            pkg = self.get_package(name, version)
        else:
            pkg = self.get_package(name)

        if not pkg:
            return None

        pkg_id = pkg["id"]

        # try to fetch manifest from metadata first
        try:
            meta = json.loads(pkg.get("metadata") or "{}")
            if meta and isinstance(meta, dict) and "manifest" in meta:
                # metadata manifest may already be categorized; return it
                return meta["manifest"]
        except Exception:
            pass

        # otherwise reconstruct from files table
        rows = self._many("SELECT file_path FROM files WHERE package_id = ?", (pkg_id,))
        files = [r["file_path"] for r in rows]
        categorized = self._categorize_paths(files)
        return categorized

    # ---------------------------
    # convenience static helper
    # ---------------------------
    @staticmethod
    def open(db_path: Optional[str] = None) -> "DBManager":
        return DBManager(db_path=db_path)


# ---------------------------
# Module-level helpers (convenient)
# ---------------------------
def connect(db_path: Optional[str] = None) -> DBManager:
    return DBManager.open(db_path)

def add_package_quick(name: str, version: str, files: List[str], deps: List[Dict[str, str]] = None, metadata: Dict[str, Any] = None, db_path: Optional[str] = None):
    with DBManager(db_path) as db:
        return db.add_package(name, version, files, deps, metadata)

def remove_package_quick(name: str, version: Optional[str] = None, db_path: Optional[str] = None) -> List[str]:
    with DBManager(db_path) as db:
        return db.remove_package(name, version)

def list_installed_quick(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    with DBManager(db_path) as db:
        return db.list_installed()

def record_install_quick(name_or_namever: str, manifest: Dict[str, List[str]], version: Optional[str] = None,
                         deps: Optional[List[Dict[str, str]]] = None, metadata: Optional[Dict[str, Any]] = None,
                         db_path: Optional[str] = None):
    with DBManager(db_path) as db:
        return db.record_install(name_or_namever, manifest, version=version, deps=deps, metadata=metadata)

def get_manifest_quick(name_or_namever: str, db_path: Optional[str] = None) -> Optional[Dict[str, List[str]]]:
    with DBManager(db_path) as db:
        return db.get_manifest(name_or_namever)


# ---------------------------
# Quick test
# ---------------------------
if __name__ == "__main__":
    # Basic smoke test (creates DB in /tmp)
    dbfile = "/tmp/zeropkg_test_installed.sqlite3"
    if os.path.exists(dbfile):
        os.unlink(dbfile)
    with DBManager(dbfile) as db:
        sample_manifest = {
            "bin": ["/usr/bin/true"],
            "lib": ["/usr/lib/libsample.so"],
            "doc": ["/usr/share/doc/sample"]
        }
        pid = db.record_install("sample-1.0", sample_manifest, deps=[{"dep_name":"libc","dep_version_req":">=2.35"}], metadata={"manual": True})
        print("Added package id:", pid)
        print("Installed:", db.list_installed())
        print("Files:", db.get_files_for_pkg("sample"))
        print("Revdeps for libc:", db.find_revdeps("libc"))
        print("Integrity:", db.validate_integrity())
        print("Manifest reconstructed:", db.get_manifest("sample-1.0"))
