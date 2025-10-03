"""
zeropkg_db.py

Gerenciamento do banco de dados SQLite para Zeropkg:
- pacotes instalados e seus arquivos
- builds
- eventos / logs
- consultas revdep / depclean

Uso padrão:
    init_db(path)
    conn = connect(path)
    package_id = register_package(conn, meta, pkgfile, files_list, build_id)
    ...
    remove_package(conn, name, version)
    list_installed(conn)
    get_package(conn, name, version)
    record_build_start/finish
    record_event / query_events
    find_revdeps(conn, pkg_name)
"""

import sqlite3
import os
import datetime
import json
from typing import Optional, List, Dict, Any, Tuple

# --- esquema de criação do banco ---
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS packages (
    id INTEGER PRIMARY KEY,
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
    id INTEGER PRIMARY KEY,
    package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    file_hash TEXT,
    UNIQUE(package_id, path)
);

CREATE TABLE IF NOT EXISTS builds (
    id INTEGER PRIMARY KEY,
    package_id INTEGER,
    meta_json TEXT,
    start_time TEXT,
    end_time TEXT,
    status TEXT,
    log_path TEXT,
    FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    level TEXT,
    component TEXT,
    message TEXT,
    metadata_json TEXT
);
"""

def init_db(db_path: str) -> None:
    """Inicializa o banco (cria diretórios e esquema)."""
    d = os.path.dirname(db_path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

def connect(db_path: str) -> sqlite3.Connection:
    """Conecta ao DB, ativa foreign keys e row_factory."""
    if not os.path.exists(db_path):
        init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def _now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def register_package(conn: sqlite3.Connection,
                     meta,
                     pkgfile: Optional[str],
                     files_list: List[Tuple[str, Optional[str]]],
                     build_id: Optional[int] = None) -> int:
    """
    Registra um pacote no banco, com sua lista de arquivos.
    meta: PackageMeta ou objeto com name/version/variant/dependencies
    files_list: lista de (path, file_hash)
    build_id: opcional, para ligar build prévio
    Retorna: package_id (int)
    """
    deps = getattr(meta, "dependencies", None)
    deps_json = json.dumps(deps) if deps is not None else json.dumps([])

    manifest = {
        "name": getattr(meta, "name", None),
        "version": getattr(meta, "version", None),
        "variant": getattr(meta, "variant", None)
    }
    manifest_json = json.dumps(manifest)

    cur = conn.cursor()
    installed_at = _now_iso()
    cur.execute("""
        INSERT OR REPLACE INTO packages
        (name, version, variant, installed_at, pkgfile, manifest_json, dependencies_json, status)
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

    # buscar id
    cur.execute("""
        SELECT id FROM packages WHERE name=? AND version=? AND variant IS ?
    """, (getattr(meta, "name", None),
          getattr(meta, "version", None),
          getattr(meta, "variant", None)))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao recuperar package id")
    pkg_id = row["id"]

    # inserir arquivos
    for path, fhash in files_list:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO files (package_id, path, file_hash)
                VALUES (?, ?, ?)
            """, (pkg_id, path, fhash))
        except Exception:
            pass
    conn.commit()

    if build_id is not None:
        cur.execute("UPDATE builds SET package_id = ? WHERE id = ?", (pkg_id, build_id))
        conn.commit()

    return pkg_id

def list_installed(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, version, variant, installed_at, pkgfile, status
        FROM packages
        ORDER BY name ASC, version ASC
    """)
    return [dict(row) for row in cur.fetchall()]

def get_package(conn: sqlite3.Connection, name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    if version is not None:
        cur.execute("SELECT * FROM packages WHERE name=? AND version=? LIMIT 1", (name, version))
    else:
        cur.execute("SELECT * FROM packages WHERE name=? ORDER BY installed_at DESC LIMIT 1", (name,))
    row = cur.fetchone()
    return dict(row) if row else None

def remove_package(conn: sqlite3.Connection, name: str, version: Optional[str] = None) -> List[str]:
    """
    Remove registro do pacote (e arquivos correspondentes pelo ON DELETE CASCADE).
    Retorna lista de paths de arquivos registrados.
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
    paths = [r["path"] for r in cur.fetchall()]
    cur.execute("DELETE FROM packages WHERE id=?", (pkg_id,))
    conn.commit()
    return paths

def record_build_start(conn: sqlite3.Connection, meta) -> int:
    cur = conn.cursor()
    meta_json = json.dumps({
        "name": getattr(meta, "name", None),
        "version": getattr(meta, "version", None),
        "variant": getattr(meta, "variant", None)
    })
    start_time = _now_iso()
    cur.execute("""
        INSERT INTO builds (meta_json, start_time, status)
        VALUES (?, ?, ?)
    """, (meta_json, start_time, "running"))
    conn.commit()
    return cur.lastrowid

def record_build_finish(conn: sqlite3.Connection, build_id: int, status: str, log_path: Optional[str] = None):
    cur = conn.cursor()
    end_time = _now_iso()
    cur.execute("""
        UPDATE builds
        SET end_time = ?, status = ?, log_path = ?
        WHERE id = ?
    """, (end_time, status, log_path, build_id))
    conn.commit()

def record_event(conn: sqlite3.Connection, level: str, component: str, message: str,
                 metadata: Optional[Dict[str, Any]] = None) -> int:
    cur = conn.cursor()
    ts = _now_iso()
    md = json.dumps(metadata or {})
    cur.execute("""
        INSERT INTO events (ts, level, component, message, metadata_json)
        VALUES (?, ?, ?, ?, ?)
    """, (ts, level, component, message, md))
    conn.commit()
    return cur.lastrowid

def query_events(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM events ORDER BY ts DESC LIMIT ?
    """, (limit,))
    return [dict(row) for row in cur.fetchall()]

def find_revdeps(conn: sqlite3.Connection, pkg_name: str) -> List[Dict[str, Any]]:
    """
    Retorna os pacotes instalados que dependem de pkg_name.
    Assume que dependencies_json é JSON de lista de dicts ou strings com 'name' campo.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, name, version, dependencies_json FROM packages")
    res = []
    for row in cur.fetchall():
        deps_json = row["dependencies_json"]
        if not deps_json:
            continue
        try:
            deps_list = json.loads(deps_json)
        except Exception:
            continue
        for dep in deps_list:
            dep_name = None
            if isinstance(dep, dict):
                dep_name = dep.get("name")
            elif isinstance(dep, str):
                dep_name = dep
            if dep_name == pkg_name:
                res.append({"id": row["id"], "name": row["name"], "version": row["version"]})
                break
    return res
