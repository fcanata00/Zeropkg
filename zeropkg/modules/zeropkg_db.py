#!/usr/bin/env python3
# zeropkg_db.py — Banco de dados de pacotes do Zeropkg
# -*- coding: utf-8 -*-

import os
import sqlite3
import time
from typing import List, Dict, Optional

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS packages (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    install_date INTEGER,
    build_options TEXT,
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    FOREIGN KEY(package_name) REFERENCES packages(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_name TEXT NOT NULL,
    dep_name TEXT NOT NULL,
    dep_version TEXT,
    FOREIGN KEY(package_name) REFERENCES packages(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pkg_name TEXT,
    stage TEXT,
    message TEXT,
    level TEXT,
    timestamp INTEGER
);
"""


class DBManager:
    def __init__(self, db_path: str = "/var/lib/zeropkg/installed.sqlite3"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.executescript(DB_SCHEMA)

    # ----------------------------
    # Pacotes
    # ----------------------------
    def add_package(self, name: str, version: str, files: List[str], deps: List[Dict], build_options: str = ""):
        """Adiciona pacote ao DB com seus arquivos e dependências"""
        now = int(time.time())
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO packages(name, version, install_date, build_options) VALUES (?, ?, ?, ?)",
                (name, version, now, build_options),
            )
            # limpar registros antigos
            self.conn.execute("DELETE FROM files WHERE package_name=?", (name,))
            self.conn.execute("DELETE FROM dependencies WHERE package_name=?", (name,))
            # adicionar arquivos
            for f in files:
                self.conn.execute(
                    "INSERT INTO files(package_name, file_path) VALUES (?, ?)", (name, f)
                )
            # adicionar dependências
            for d in deps:
                dep_name = d.get("name") if isinstance(d, dict) else str(d)
                dep_ver = d.get("version") if isinstance(d, dict) else None
                self.conn.execute(
                    "INSERT INTO dependencies(package_name, dep_name, dep_version) VALUES (?, ?, ?)",
                    (name, dep_name, dep_ver),
                )

    def remove_package(self, name: str) -> List[str]:
        """Remove pacote e retorna lista de arquivos que estavam registrados"""
        with self.conn:
            rows = self.conn.execute("SELECT file_path FROM files WHERE package_name=?", (name,)).fetchall()
            files = [r["file_path"] for r in rows]
            self.conn.execute("DELETE FROM packages WHERE name=?", (name,))
            return files

    def get_package(self, name: str) -> Optional[Dict]:
        row = self.conn.execute("SELECT * FROM packages WHERE name=?", (name,)).fetchone()
        if not row:
            return None
        files = self.list_files(name)
        deps = self.list_deps(name)
        return {
            "name": row["name"],
            "version": row["version"],
            "install_date": row["install_date"],
            "build_options": row["build_options"],
            "files": files,
            "deps": deps,
        }

    def list_installed(self) -> List[Dict]:
        rows = self.conn.execute("SELECT * FROM packages ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def is_installed(self, name: str, version: Optional[str] = None) -> bool:
        if version:
            row = self.conn.execute("SELECT 1 FROM packages WHERE name=? AND version=?", (name, version)).fetchone()
        else:
            row = self.conn.execute("SELECT 1 FROM packages WHERE name=?", (name,)).fetchone()
        return row is not None

    # ----------------------------
    # Arquivos e dependências
    # ----------------------------
    def list_files(self, name: str) -> List[str]:
        rows = self.conn.execute("SELECT file_path FROM files WHERE package_name=?", (name,)).fetchall()
        return [r["file_path"] for r in rows]

    def list_deps(self, name: str) -> List[Dict]:
        rows = self.conn.execute("SELECT dep_name, dep_version FROM dependencies WHERE package_name=?", (name,)).fetchall()
        return [{"name": r["dep_name"], "version": r["dep_version"]} for r in rows]

    def find_revdeps(self, pkg_name: str) -> List[str]:
        rows = self.conn.execute("SELECT package_name FROM dependencies WHERE dep_name=?", (pkg_name,)).fetchall()
        return [r["package_name"] for r in rows]

    # ----------------------------
    # Eventos / logs
    # ----------------------------
    def log_event(self, pkg_name: str, stage: str, message: str, level: str = "INFO"):
        now = int(time.time())
        with self.conn:
            self.conn.execute(
                "INSERT INTO events(pkg_name, stage, message, level, timestamp) VALUES (?, ?, ?, ?, ?)",
                (pkg_name, stage, message, level, now),
            )

    def list_events(self, pkg_name: Optional[str] = None) -> List[Dict]:
        if pkg_name:
            rows = self.conn.execute("SELECT * FROM events WHERE pkg_name=? ORDER BY id DESC", (pkg_name,)).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM events ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()
