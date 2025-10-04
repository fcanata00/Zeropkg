#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_db.py — Database layer for Zeropkg

Features:
 - SQLite3 WAL database with schema for packages, files, deps, events, snapshots, meta
 - Thread-safe API with simple in-memory caching
 - Snapshot / rollback support by copying DB file
 - Export/import (tar.gz) for backup/restore
 - Integration with zeropkg_logger (if present) via log_event
 - All queries parameterized to avoid injection
"""

from __future__ import annotations
import os
import sqlite3
import json
import shutil
import threading
import time
import tempfile
import tarfile
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# Safe import logger/config (optional)
def _safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

logger_mod = _safe_import("zeropkg_logger")
config_mod = _safe_import("zeropkg_config")

if logger_mod and hasattr(logger_mod, "log_event"):
    def _log_event(evt, msg, level="INFO", metadata=None):
        try:
            logger_mod.log_event(evt, msg, level=level, metadata=metadata)
        except Exception:
            pass
else:
    def _log_event(evt, msg, level="INFO", metadata=None):
        # fallback simple print to stderr minimally (avoid noisy output)
        try:
            if level and level.upper() == "ERROR":
                print(f"[ERROR] {evt}: {msg}", file=sys.stderr)
        except Exception:
            pass

# Default paths (config manager may override)
DEFAULT_STATE_DIR = Path("/var/lib/zeropkg")
DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_DB_PATH = DEFAULT_STATE_DIR / "zeropkg.db"
DEFAULT_SNAP_DIR = DEFAULT_STATE_DIR / "snapshots"
DEFAULT_SNAP_DIR.mkdir(parents=True, exist_ok=True)

# Simple in-memory cache structure
class _SimpleCache:
    def __init__(self):
        self._lock = threading.RLock()
        self.packages_by_name: Dict[str, Dict[str, Any]] = {}
        self.last_refresh = 0

    def get(self, name: str):
        with self._lock:
            return self.packages_by_name.get(name)

    def set(self, name: str, value: Dict[str, Any]):
        with self._lock:
            self.packages_by_name[name] = value

    def delete(self, name: str):
        with self._lock:
            if name in self.packages_by_name:
                del self.packages_by_name[name]

    def clear(self):
        with self._lock:
            self.packages_by_name.clear()

# Main DB class
class ZeroPKGDB:
    def __init__(self, db_path: Optional[Path] = None, timeout: float = 30.0):
        # allow config to override
        if config_mod and hasattr(config_mod, "get_config_manager"):
            try:
                mgr = config_mod.get_config_manager()
                cfg_db = mgr.get("paths", "db_path", default=None)
                if cfg_db:
                    db_path = Path(cfg_db)
            except Exception:
                pass

        self.db_path = Path(db_path or DEFAULT_DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
        self._timeout = timeout
        self.cache = _SimpleCache()
        self._connect_and_init()

    # -------------------------
    # Connection and schema
    # -------------------------
    def _connect_and_init(self):
        with self._lock:
            exists = self.db_path.exists()
            self._conn = sqlite3.connect(str(self.db_path), timeout=self._timeout, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.row_factory = sqlite3.Row
            if not exists:
                self._ensure_schema()
            else:
                # ensure schema up-to-date (idempotent)
                self._ensure_schema()
            _log_event("db", f"Database opened at {self.db_path}", level="INFO")

    def _ensure_schema(self):
        # run schema creation statements (if tables already exist, ignore)
        schema_sql = """
        BEGIN;
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        );
        CREATE TABLE IF NOT EXISTS packages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            version TEXT,
            installed_at INTEGER,
            size INTEGER DEFAULT 0,
            manifest_json TEXT,
            UNIQUE(name)
        );
        CREATE INDEX IF NOT EXISTS idx_packages_name ON packages(name);

        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
            path TEXT NOT NULL,
            mode INTEGER,
            uid INTEGER,
            gid INTEGER,
            size INTEGER,
            sha256 TEXT,
            UNIQUE(package_id, path)
        );
        CREATE INDEX IF NOT EXISTS idx_files_pkg ON files(package_id);

        CREATE TABLE IF NOT EXISTS deps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
            depends_on TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_deps_pkg ON deps(package_id);

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER,
            type TEXT,
            level TEXT,
            package TEXT,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER,
            path TEXT,
            note TEXT
        );
        COMMIT;
        """
        cur = self._conn.cursor()
        cur.executescript(schema_sql)
        self._conn.commit()

    def close(self):
        with self._lock:
            if self._conn:
                try:
                    self._conn.commit()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
                _log_event("db", "Database closed", level="INFO")

    # -------------------------
    # Low-level helpers
    # -------------------------
    def _execute(self, sql: str, params: Tuple = (), commit: bool = False):
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            if commit:
                self._conn.commit()
            return cur

    def _get_package_row(self, name: str) -> Optional[sqlite3.Row]:
        cur = self._execute("SELECT * FROM packages WHERE name = ? LIMIT 1", (name,))
        return cur.fetchone()

    # -------------------------
    # Core APIs
    # -------------------------
    def record_install_quick(self, name: str, version: str, manifest: Dict[str, Any], files: List[Dict[str, Any]], deps: List[str] = None) -> Dict[str, Any]:
        """
        Record a package installation quickly:
         - name, version, manifest (metadata), files (list of {path, size, sha256, mode, uid, gid}), deps list
        Returns dict with status and package id.
        """
        deps = deps or []
        ts = int(time.time())
        manifest_json = json.dumps(manifest or {}, ensure_ascii=False)
        size_total = sum((f.get("size") or 0) for f in files)
        with self._lock:
            row = self._get_package_row(name)
            if row:
                # update
                pkg_id = row["id"]
                self._execute("UPDATE packages SET version=?, installed_at=?, size=?, manifest_json=? WHERE id=?", (version, ts, size_total, manifest_json, pkg_id), commit=True)
                # delete old files and deps
                self._execute("DELETE FROM files WHERE package_id = ?", (pkg_id,), commit=True)
                self._execute("DELETE FROM deps WHERE package_id = ?", (pkg_id,), commit=True)
            else:
                cur = self._execute("INSERT INTO packages(name, version, installed_at, size, manifest_json) VALUES(?,?,?,?,?)", (name, version, ts, size_total, manifest_json), commit=True)
                pkg_id = cur.lastrowid
            # insert files
            ins_files = []
            for f in files:
                p = f.get("path")
                mode = f.get("mode")
                uid = f.get("uid")
                gid = f.get("gid")
                size = f.get("size")
                sha = f.get("sha256")
                self._execute("INSERT OR REPLACE INTO files(package_id, path, mode, uid, gid, size, sha256) VALUES(?,?,?,?,?,?,?)", (pkg_id, p, mode, uid, gid, size, sha))
            # deps
            for d in deps:
                self._execute("INSERT INTO deps(package_id, depends_on) VALUES(?,?)", (pkg_id, d))
            self._conn.commit()
            # update cache
            try:
                self.cache.set(name, {"name": name, "version": version, "installed_at": ts, "size": size_total, "manifest": manifest})
            except Exception:
                pass
            _log_event("install", f"Recorded install {name}-{version} (id={pkg_id})", level="INFO", metadata={"pkg": name, "ver": version})
            return {"ok": True, "pkg_id": pkg_id}

    def remove_package_quick(self, name: str) -> Dict[str, Any]:
        """
        Remove package metadata entry (does NOT attempt to remove files on disk).
        Returns summary.
        """
        with self._lock:
            row = self._get_package_row(name)
            if not row:
                return {"ok": False, "error": "not_found"}
            pkg_id = row["id"]
            self._execute("DELETE FROM packages WHERE id = ?", (pkg_id,), commit=True)
            # cache invalidate
            self.cache.delete(name)
            _log_event("remove", f"Removed metadata for package {name}", level="INFO", metadata={"pkg": name})
            return {"ok": True, "pkg": name}

    def get_package_manifest(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Return manifest for package (manifest_json + files list + deps)
        """
        # try cache
        cached = self.cache.get(name)
        if cached:
            return cached.get("manifest")
        with self._lock:
            row = self._get_package_row(name)
            if not row:
                return None
            pkg_id = row["id"]
            manifest = json.loads(row["manifest_json"]) if row["manifest_json"] else {}
            cur = self._execute("SELECT path, mode, uid, gid, size, sha256 FROM files WHERE package_id = ?", (pkg_id,))
            files = [dict(r) for r in cur.fetchall()]
            cur = self._execute("SELECT depends_on FROM deps WHERE package_id = ?", (pkg_id,))
            deps = [r["depends_on"] for r in cur.fetchall()]
            result = {"name": name, "version": row["version"], "installed_at": row["installed_at"], "size": row["size"], "manifest": manifest, "files": files, "deps": deps}
            # update cache
            try:
                self.cache.set(name, result)
            except Exception:
                pass
            return result

    def list_installed_quick(self) -> List[Dict[str, Any]]:
        """
        Return quick list of installed packages (name, version, size, installed_at)
        """
        with self._lock:
            cur = self._execute("SELECT name, version, size, installed_at FROM packages ORDER BY name")
            return [dict(r) for r in cur.fetchall()]

    def find_revdeps(self, name: str) -> List[str]:
        """
        Return list of packages that depend on 'name'
        """
        with self._lock:
            cur = self._execute("SELECT p.name FROM deps d JOIN packages p ON p.id = d.package_id WHERE d.depends_on = ? GROUP BY p.name", (name,))
            return [r["name"] for r in cur.fetchall()]

    def get_orphaned_packages(self) -> List[str]:
        """
        Find packages that are not required by any other installed package (orphans).
        This is a best-effort: excludes packages marked as required in meta (if any).
        """
        with self._lock:
            cur = self._execute("""
                SELECT p.name FROM packages p
                WHERE p.name NOT IN (SELECT DISTINCT depends_on FROM deps)
                ORDER BY p.name
            """)
            return [r["name"] for r in cur.fetchall()]

    # -------------------------
    # Integrity and audit
    # -------------------------
    def validate_integrity(self, package_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate file integrity for a package (or all packages if package_name is None).
        Computes sha256 for recorded files and compares to stored values.
        Returns report dict.
        """
        report = {"checked": 0, "errors": []}
        with self._lock:
            if package_name:
                rows = [self._get_package_row(package_name)]
            else:
                cur = self._execute("SELECT name FROM packages")
                rows = [self._get_package_row(r["name"]) for r in cur.fetchall()]
            for row in rows:
                if not row:
                    continue
                name = row["name"]
                pkg_id = row["id"]
                cur = self._execute("SELECT path, sha256 FROM files WHERE package_id = ?", (pkg_id,))
                for frow in cur.fetchall():
                    p = Path(frow["path"])
                    expected = frow["sha256"]
                    report["checked"] += 1
                    try:
                        if not p.exists():
                            report["errors"].append({"pkg": name, "path": str(p), "error": "missing"})
                            continue
                        # compute sha256
                        h = self._compute_sha256(p)
                        if expected and h.lower() != expected.lower():
                            report["errors"].append({"pkg": name, "path": str(p), "error": "sha_mismatch", "expected": expected, "got": h})
                    except Exception as e:
                        report["errors"].append({"pkg": name, "path": str(p), "error": str(e)})
        return report

    def _compute_sha256(self, path: Path) -> str:
        import hashlib
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024*1024), b""):
                h.update(chunk)
        return h.hexdigest()

    # -------------------------
    # Events and auditing
    # -------------------------
    def record_event(self, etype: str, level: str = "INFO", package: Optional[str] = None, payload: Optional[Dict[str, Any]] = None) -> None:
        ts = int(time.time())
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        with self._lock:
            self._execute("INSERT INTO events(ts, type, level, package, payload_json) VALUES(?,?,?,?,?)", (ts, etype, level, package, payload_json), commit=True)
            _log_event(etype, f"{package or '-'}: {etype} {payload or {}}", level=level, metadata=payload)

    def query_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._execute("SELECT ts,type,level,package,payload_json FROM events ORDER BY ts DESC LIMIT ?", (limit,))
            out = []
            for r in cur.fetchall():
                o = dict(r)
                try:
                    o["payload"] = json.loads(o.pop("payload_json") or "{}")
                except Exception:
                    o["payload"] = {}
                out.append(o)
            return out

    # -------------------------
    # Snapshot / export / import
    # -------------------------
    def snapshot(self, note: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a snapshot copy of the DB file and register it.
        Returns snapshot metadata.
        """
        with self._lock:
            ts = int(time.time())
            name = f"snapshot-{ts}.db"
            snap_dir = DEFAULT_SNAP_DIR
            snap_dir.mkdir(parents=True, exist_ok=True)
            dest = snap_dir / name
            # flush and copy
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except Exception:
                pass
            # ensure closed briefly for copy safety
            self._conn.commit()
            shutil.copy2(str(self.db_path), str(dest))
            cur = self._execute("INSERT INTO snapshots(ts, path, note) VALUES(?,?,?)", (ts, str(dest), note or ""), commit=True)
            snap_id = cur.lastrowid
            _log_event("snapshot", f"Created snapshot {dest}", level="INFO", metadata={"snapshot_id": snap_id})
            return {"ok": True, "snapshot_id": snap_id, "path": str(dest), "ts": ts}

    def rollback_from_snapshot(self, snapshot_id: int) -> Dict[str, Any]:
        """
        Rollback DB to a snapshot (by id).
        WARNING: this replaces the DB file; callers should ensure service restart if needed.
        """
        with self._lock:
            cur = self._execute("SELECT path FROM snapshots WHERE id = ? LIMIT 1", (snapshot_id,))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "error": "snapshot_not_found"}
            snap_path = Path(row["path"])
            if not snap_path.exists():
                return {"ok": False, "error": "snapshot_file_missing"}
            # close connection, replace file, reconnect
            try:
                self.close()
                shutil.copy2(str(snap_path), str(self.db_path))
                # reopen
                self._connect_and_init()
                self.cache.clear()
                _log_event("rollback", f"Rolled back DB from snapshot {snapshot_id}", level="INFO", metadata={"snapshot_id": snapshot_id})
                return {"ok": True}
            except Exception as e:
                return {"ok": False, "error": str(e)}

    def export_db(self, dest: Optional[Path] = None, compress: bool = True) -> str:
        """
        Export DB to dest (file or directory) as tar.gz if compress True, else raw copy.
        Returns path to exported file.
        """
        dest = Path(dest or (DEFAULT_STATE_DIR / f"zeropkg-db-export-{int(time.time())}.tar.gz"))
        with self._lock:
            try:
                # ensure checkpoint to avoid WAL issues
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                except Exception:
                    pass
                self._conn.commit()
                if compress:
                    with tarfile.open(str(dest), "w:gz") as tf:
                        tf.add(str(self.db_path), arcname=self.db_path.name)
                else:
                    shutil.copy2(str(self.db_path), str(dest))
                _log_event("export", f"Exported DB to {dest}", level="INFO")
                return str(dest)
            except Exception as e:
                _log_event("export_error", str(e), level="ERROR")
                raise

    def import_db(self, src: Path, overwrite: bool = False) -> Dict[str, Any]:
        """
        Import DB from tar.gz or raw DB file. If overwrite=True, replace current DB.
        """
        src = Path(src)
        with self._lock:
            try:
                tmpdir = Path(tempfile.mkdtemp(prefix="zeropkg-import-"))
                try:
                    if tarfile.is_tarfile(str(src)):
                        with tarfile.open(str(src), "r:*") as tf:
                            tf.extractall(path=str(tmpdir))
                        # find a .db file inside
                        found = list(tmpdir.glob("**/*.db"))
                        if not found:
                            return {"ok": False, "error": "no_db_in_archive"}
                        src_db = found[0]
                    else:
                        src_db = src
                    # copy to place
                    if overwrite:
                        self.close()
                        shutil.copy2(str(src_db), str(self.db_path))
                        self._connect_and_init()
                    else:
                        # import into same DB by reading and merging; basic approach: attach and copy
                        # attach src in sqlite and copy tables
                        attach_name = "srcdb"
                        cur = self._execute(f"ATTACH DATABASE ? AS {attach_name}", (str(src_db),))
                        # naive copy: insert or ignore into packages then files/deps/events
                        self._execute(f"INSERT OR IGNORE INTO packages(name, version, installed_at, size, manifest_json) SELECT name, version, installed_at, size, manifest_json FROM {attach_name}.packages", commit=True)
                        # copy files by joining
                        self._execute(f"INSERT OR IGNORE INTO files(package_id, path, mode, uid, gid, size, sha256) SELECT p.id, f.path, f.mode, f.uid, f.gid, f.size, f.sha256 FROM {attach_name}.files f JOIN {attach_name}.packages sp ON sp.id = f.package_id JOIN packages p ON p.name = sp.name", commit=True)
                        # deps and events
                        self._execute(f"INSERT INTO deps(package_id, depends_on) SELECT p.id, d.depends_on FROM {attach_name}.deps d JOIN {attach_name}.packages sp ON sp.id = d.package_id JOIN packages p ON p.name = sp.name", commit=True)
                        self._execute(f"INSERT INTO events(ts, type, level, package, payload_json) SELECT ts, type, level, package, payload_json FROM {attach_name}.events", commit=True)
                        self._execute(f"DETACH DATABASE {attach_name}", (), commit=True)
                    self.cache.clear()
                    _log_event("import", f"Imported DB from {src}", level="INFO")
                    return {"ok": True}
                finally:
                    try:
                        shutil.rmtree(str(tmpdir))
                    except Exception:
                        pass
            except Exception as e:
                _log_event("import_error", str(e), level="ERROR")
                return {"ok": False, "error": str(e)}

    # -------------------------
    # Utility / close
    # -------------------------
    def close_and_cleanup(self):
        try:
            self.close()
        except Exception:
            pass

# Singleton convenience
_DEFAULT_DB: Optional[ZeroPKGDB] = None

def _get_default_db() -> ZeroPKGDB:
    global _DEFAULT_DB
    if _DEFAULT_DB is None:
        _DEFAULT_DB = ZeroPKGDB()
    return _DEFAULT_DB

# Provide module-level simple functions for quick use
def record_install_quick(name: str, version: str, manifest: Dict[str, Any], files: List[Dict[str, Any]], deps: List[str] = None):
    db = _get_default_db()
    return db.record_install_quick(name, version, manifest, files, deps)

def remove_package_quick(name: str):
    db = _get_default_db()
    return db.remove_package_quick(name)

def get_package_manifest(name: str):
    db = _get_default_db()
    return db.get_package_manifest(name)

def list_installed_quick():
    db = _get_default_db()
    return db.list_installed_quick()

def find_revdeps(name: str):
    db = _get_default_db()
    return db.find_revdeps(name)

def get_orphaned_packages():
    db = _get_default_db()
    return db.get_orphaned_packages()

def record_event(etype: str, level: str = "INFO", package: Optional[str] = None, payload: Optional[Dict[str, Any]] = None):
    db = _get_default_db()
    return db.record_event(etype, level, package, payload)

# Basic CLI for quick introspection
if __name__ == "__main__":
    import argparse, pprint
    p = argparse.ArgumentParser(prog="zeropkg-db", description="Zeropkg DB inspector")
    p.add_argument("--list", action="store_true", help="List installed packages")
    p.add_argument("--manifest", help="Show package manifest")
    p.add_argument("--export", help="Export db to path (tar.gz recommended)")
    p.add_argument("--import", dest="import_path", help="Import db from path")
    p.add_argument("--snapshot", action="store_true", help="Create snapshot")
    p.add_argument("--validate", action="store_true", help="Validate integrity (may be slow)")
    args = p.parse_args()
    db = _get_default_db()
    if args.list:
        pprint.pprint(db.list_installed_quick())
    if args.manifest:
        pprint.pprint(db.get_package_manifest(args.manifest))
    if args.export:
        print(db.export_db(Path(args.export)))
    if args.import_path:
        print(db.import_db(Path(args.import_path)))
    if args.snapshot:
        print(db.snapshot())
    if args.validate:
        print(db.validate_integrity())
