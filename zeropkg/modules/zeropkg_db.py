"""
zeropkg_db.py

Banco de dados SQLite para registro das operações do zeropkg:
- pacotes instalados (packages)
- arquivos instalados por pacote (files)
- builds realizados (builds)
- eventos/log de operações (events)

API principal (resumida):
- init_db(db_path) -> None
- connect(db_path) -> sqlite3.Connection
- register_package(conn, meta, pkgfile, files_list, build_id=None) -> package_id
- remove_package(conn, name, version=None) -> list_of_removed_files
- list_installed(conn) -> list of package rows (dict)
- get_package(conn, name, version=None) -> package row (dict) or None
- record_build_start(conn, meta) -> build_id
- record_build_finish(conn, build_id, status, log_path=None) -> None
- find_revdeps(conn, pkg_name) -> list of packages depending on pkg_name
"""

from __future__ import annotations
import sqlite3
import json
import os
import datetime
from typing import Optional, List, Dict, Any, Tuple

# --- Schema versioning can be added later ---
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    variant TEXT,
    installed_at TEXT NOT NULL,
    pkgfile TEXT,
    manifest_json TEXT,
    dependencies_json TEXT,
    status TEXT NOT NULL DEFAULT 'installed',
    UNIQUE(name, version, variant)
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    file_hash TEXT,
    UNIQUE(package_id, path)
);

CREATE TABLE IF NOT EXISTS builds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id INTEGER,
    meta_json TEXT,
    start_time TEXT,
    end_time TEXT,
    status TEXT,
    log_path TEXT,
    FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    level TEXT,
    component TEXT,
    message TEXT,
    metadata_json TEXT
);
"""

# Helper to convert sqlite row to dict
def _row_to_dict(cursor: sqlite3.Cursor, row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[idx] for idx, k in enumerate([d[0] for d in cursor.description])}

# Initialize DB file and schema
def init_db(db_path: str) -> None:
    """Create DB file and schema if not exists."""
    dirname = os.path.dirname(db_path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

def connect(db_path: str) -> sqlite3.Connection:
    """Return a sqlite3.Connection with row factory dict-like behavior."""
    if not os.path.exists(db_path):
        init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # ensure foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

# --- Package operations ---

def _now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def register_package(conn: sqlite3.Connection,
                     meta,
                     pkgfile: Optional[str],
                     files_list: List[Tuple[str, Optional[str]]],
                     build_id: Optional[int] = None) -> int:
    """
    Register a package as installed.

    Args:
      conn: sqlite3.Connection
      meta: PackageMeta or object with name, version, variant, dependencies (list of dicts)
      pkgfile: path to package archive (or None)
      files_list: list of tuples (path, file_hash)
      build_id: optional build id to link

    Returns:
      package_id (int)
    """
    deps_json = None
    manifest_json = None
    # gather dependencies if present
    try:
        deps = getattr(meta, "dependencies", None)
        if deps is None and isinstance(meta, dict):
            deps = meta.get("dependencies")
        deps_json = json.dumps(deps or [])
    except Exception:
        deps_json = json.dumps([])

    try:
        manifest = {
            "name": getattr(meta, "name", None),
            "version": getattr(meta, "version", None),
            "variant": getattr(meta, "variant", None),
        }
        manifest_json = json.dumps(manifest)
    except Exception:
        manifest_json = "{}"

    cur = conn.cursor()
    installed_at = _now_iso()
    cur.execute("""
        INSERT OR REPLACE INTO packages (name, version, variant, installed_at, pkgfile, manifest_json, dependencies_json, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (getattr(meta, "name", None),
          getattr(meta, "version", None),
          getattr(meta, "variant", None),
          installed_at,
          pkgfile,
          manifest_json,
          deps_json,
          "installed"))
    conn.commit()
    # get id (if replaced previous entry, sqlite3 returns the replaced row's rowid from last insert)
    # Fetch the inserted/updated package id
    cur.execute("SELECT id FROM packages WHERE name=? AND version=? AND variant IS ?", (getattr(meta, "name", None), getattr(meta, "version", None), getattr(meta, "variant", None)))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao obter package id após insert")
    package_id = row["id"]

    # insert files
    for path, file_hash in files_list:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO files (package_id, path, file_hash)
                VALUES (?, ?, ?)
            """, (package_id, path, file_hash))
        except Exception:
            # ignore file insert errors but continue
            pass
    conn.commit()

    # if build_id provided, link it
    if build_id:
        cur.execute("UPDATE builds SET package_id = ? WHERE id = ?", (package_id, build_id))
        conn.commit()

    return package_id

def list_installed(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("SELECT id, name, version, variant, installed_at, pkgfile, status FROM packages ORDER BY name, version")
    rows = [dict(r) for r in cur.fetchall()]
    return rows

def get_package(conn: sqlite3.Connection, name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    if version:
        cur.execute("SELECT * FROM packages WHERE name=? AND version=? LIMIT 1", (name, version))
    else:
        cur.execute("SELECT * FROM packages WHERE name=? ORDER BY installed_at DESC LIMIT 1", (name,))
    row = cur.fetchone()
    return dict(row) if row else None

def remove_package(conn: sqlite3.Connection, name: str, version: Optional[str] = None) -> List[str]:
    """
    Remove a package record (and cascade files). Returns list of file paths that were recorded for the package.
    NOTE: This only alters DB records — actual filesystem files should be removed by the installer/remover logic.
    """
    cur = conn.cursor()
    if version:
        cur.execute("SELECT id FROM packages WHERE name=? AND version=? LIMIT 1", (name, version))
    else:
        cur.execute("SELECT id FROM packages WHERE name=? ORDER BY installed_at DESC LIMIT 1", (name,))
    row = cur.fetchone()
    if not row:
        return []
    pkg_id = row["id"]
    cur.execute("SELECT path FROM files WHERE package_id=?", (pkg_id,))
    files = [r["path"] for r in cur.fetchall()]
    cur.execute("DELETE FROM packages WHERE id=?", (pkg_id,))
    conn.commit()
    return files

# --- Build tracking ---

def record_build_start(conn: sqlite3.Connection, meta) -> int:
    """
    Create a build record and return its id.
    meta can be PackageMeta or a dict.
    """
    cur = conn.cursor()
    meta_json = json.dumps({
        "name": getattr(meta, "name", None),
        "version": getattr(meta, "version", None),
        "variant": getattr(meta, "variant", None),
    })
    start_time = _now_iso()
    cur.execute("""
        INSERT INTO builds (meta_json, start_time, status)
        VALUES (?, ?, ?)
    """, (meta_json, start_time, "running"))
    conn.commit()
    return cur.lastrowid

def record_build_finish(conn: sqlite3.Connection, build_id: int, status: str, log_path: Optional[str] = None) -> None:
    cur = conn.cursor()
    end_time = _now_iso()
    cur.execute("UPDATE builds SET end_time=?, status=?, log_path=? WHERE id=?", (end_time, status, log_path, build_id))
    conn.commit()

# --- Events / logs ---

def record_event(conn: sqlite3.Connection, level: str, component: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> int:
    cur = conn.cursor()
    ts = _now_iso()
    md = json.dumps(metadata or {})
    cur.execute("INSERT INTO events (ts, level, component, message, metadata_json) VALUES (?, ?, ?, ?, ?)", (ts, level, component, message, md))
    conn.commit()
    return cur.lastrowid

def query_events(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,))
    return [dict(r) for r in cur.fetchall()]

# --- reverse dependencies (revdep) ---

def find_revdeps(conn: sqlite3.Connection, pkg_name: str) -> List[Dict[str, Any]]:
    """
    Find installed packages that list pkg_name in their dependencies.
    Assumes dependencies_json is a JSON list of objects with 'name' key, or simple strings.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, name, version, dependencies_json FROM packages")
    res = []
    for row in cur.fetchall():
        deps = row["dependencies_json"]
        if not deps:
            continue
        try:
            deps_obj = json.loads(deps)
        except Exception:
            continue
        # deps can be list of dicts or strings
        for d in deps_obj:
            if isinstance(d, dict):
                dep_name = d.get("name")
            else:
                dep_name = d
            if dep_name == pkg_name:
                res.append({"id": row["id"], "name": row["name"], "version": row["version"]})
                break
    return res
