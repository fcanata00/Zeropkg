"""
Zeropkg - core.py
União: Logger (colorido + JSONL), DB (sqlite3 robusto), Registry e API única.
Local: Zeropkg/zeropkg/modules/core.py

Características principais:
- Logger thread-safe, cores apenas em TTY, grava JSON lines em arquivo (se configurado).
- DB com PRAGMA recomendadas (WAL, foreign_keys, busy_timeout), thread-safe,
  inicialização automática do schema e métodos utilitários.
- Registry para operações de registro/instalação/falha/listagem.
- ZeropkgAPI: API única fixa que integra tudo (install, remove, list, get, record_step, fail).
"""

from __future__ import annotations
import os
import sys
import json
import sqlite3
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

# -------------------------
# Logger
# -------------------------
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}

class SimpleLogger:
    """
    Logger minimalista, thread-safe.
    - Console: colorized if stdout.isatty()
    - File: JSON lines (one JSON per log entry) if logfile configured
    """
    _instances: Dict[str, "SimpleLogger"] = {}
    _global_cfg = {"level": "INFO", "logfile": None, "file_mode": "a"}

    COLOR_MAP = {
        "DEBUG": "\033[36m",    # cyan
        "INFO": "\033[32m",     # green
        "WARNING": "\033[33m",  # yellow
        "ERROR": "\033[31m",    # red
        "CRITICAL": "\033[41m", # red bg
    }
    RESET = "\033[0m"

    def __init__(self, name: str):
        self.name = name
        self._lock = threading.RLock()

    @classmethod
    def basic_config(cls, level: str = "INFO", logfile: Optional[str] = None, file_mode: str = "a"):
        level = (level or "INFO").upper()
        if level not in _LEVELS:
            raise ValueError("Invalid log level: %r" % level)
        cls._global_cfg["level"] = level
        cls._global_cfg["logfile"] = logfile
        cls._global_cfg["file_mode"] = file_mode

        # ensure logfile directory exists if provided
        if logfile:
            dirname = os.path.dirname(os.path.abspath(logfile))
            if dirname and not os.path.exists(dirname):
                os.makedirs(dirname, exist_ok=True)

    @classmethod
    def get_logger(cls, name: str = "zeropkg") -> "SimpleLogger":
        if name not in cls._instances:
            cls._instances[name] = SimpleLogger(name)
        return cls._instances[name]

    def _should_log(self, level: str) -> bool:
        return _LEVELS[level] >= _LEVELS[SimpleLogger._global_cfg["level"]]

    def _emit(self, level: str, msg: str, **extra):
        if not self._should_log(level):
            return

        ts = datetime.utcnow().isoformat() + "Z"
        text = f"[{ts}] [{level}] {self.name}: {msg}"
        try:
            with self._lock:
                # Console
                stream = sys.stdout
                is_tty = hasattr(stream, "isatty") and stream.isatty()
                if is_tty and level in self.COLOR_MAP:
                    color = self.COLOR_MAP[level]
                    stream.write(f"{color}{text}{self.RESET}\n")
                else:
                    stream.write(text + "\n")
                stream.flush()

                # File as JSONL
                logfile = SimpleLogger._global_cfg.get("logfile")
                if logfile:
                    rec = {
                        "ts": ts,
                        "level": level,
                        "logger": self.name,
                        "msg": msg,
                        "extra": extra or {},
                    }
                    # append JSON line
                    with open(logfile, SimpleLogger._global_cfg.get("file_mode", "a"), encoding="utf-8") as f:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            # Logging must not raise in normal operation
            try:
                sys.stderr.write("Logger internal error\n")
            except Exception:
                pass

    # convenience methods
    def debug(self, msg: str, **extra): self._emit("DEBUG", msg, **extra)
    def info(self, msg: str, **extra):  self._emit("INFO", msg, **extra)
    def warning(self, msg: str, **extra): self._emit("WARNING", msg, **extra)
    def error(self, msg: str, **extra): self._emit("ERROR", msg, **extra)
    def critical(self, msg: str, **extra): self._emit("CRITICAL", msg, **extra)


# module-level helpers
def basic_config(level: str = "INFO", logfile: Optional[str] = None, file_mode: str = "a"):
    SimpleLogger.basic_config(level=level, logfile=logfile, file_mode=file_mode)

def get_logger(name: str = "zeropkg") -> SimpleLogger:
    return SimpleLogger.get_logger(name)


# -------------------------
# Database (sqlite3)
# -------------------------
class DBError(Exception):
    pass

class DB:
    """
    Thread-safe sqlite wrapper.
    Uses:
      - WAL mode
      - foreign_keys = ON
      - busy_timeout set to avoid 'database is locked'
    """
    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self._conn: sqlite3.Connection = None  # type: ignore
        self._lock = threading.RLock()
        self.log = get_logger("DB")

        # ensure directory exists
        db_dir = os.path.dirname(self.path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self._connect()
        self._init_schema()

    def _connect(self):
        # check_same_thread=False allows using connection across threads;
        # we'll still guard via self._lock for safety.
        try:
            self._conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
            self._conn.row_factory = sqlite3.Row
            # recommended pragmas
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.execute("PRAGMA busy_timeout=5000;")
        except Exception as e:
            raise DBError("Failed to open DB: %s" % e)

    def close(self):
        with self._lock:
            try:
                if self._conn:
                    self._conn.commit()
                    self._conn.close()
                    self._conn = None
            except Exception as e:
                self.log.error("Error closing DB: %s" % e)

    def _init_schema(self):
        with self._lock:
            c = self._conn.cursor()
            # packages table
            c.execute("""
            CREATE TABLE IF NOT EXISTS packages (
                name TEXT PRIMARY KEY,
                version TEXT,
                source TEXT,
                status TEXT,
                metadata TEXT,
                installed_at TEXT
            )""")
            # build logs
            c.execute("""
            CREATE TABLE IF NOT EXISTS build_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pkg_name TEXT NOT NULL,
                step TEXT,
                message TEXT,
                level TEXT,
                ts TEXT,
                FOREIGN KEY(pkg_name) REFERENCES packages(name) ON DELETE CASCADE
            )""")
            self._conn.commit()
            self.log.debug("DB schema initialized at %s" % self.path)

    # low-level helpers
    def _execute(self, query: str, params: Tuple = (), commit: bool = False):
        with self._lock:
            try:
                cur = self._conn.execute(query, params)
                if commit:
                    self._conn.commit()
                return cur
            except sqlite3.DatabaseError as e:
                self.log.error("SQL error: %s; query=%s params=%s" % (e, query, params))
                raise DBError(str(e))

    def fetchone(self, query: str, params: Tuple = ()):
        cur = self._execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None

    def fetchall(self, query: str, params: Tuple = ()):
        cur = self._execute(query, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    # high-level operations
    def upsert_package(self, name: str, version: str, source: str, status: str, metadata: Optional[Dict[str, Any]] = None):
        meta_json = json.dumps(metadata or {}, ensure_ascii=False)
        now = None  # leave installed_at untouched here unless status == 'installed'
        # Use ON CONFLICT DO UPDATE pattern (SQLite >= 3.24). Safer fallback below if not supported.
        query = """
        INSERT INTO packages (name, version, source, status, metadata, installed_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            version=excluded.version,
            source=excluded.source,
            status=excluded.status,
            metadata=excluded.metadata
        """
        try:
            self._execute(query, (name, version, source, status, meta_json, now), commit=True)
        except DBError:
            # Fallback for older SQLite: do manual upsert
            existing = self.fetchone("SELECT name FROM packages WHERE name=?", (name,))
            if existing:
                self._execute("""
                    UPDATE packages SET version=?, source=?, status=?, metadata=? WHERE name=?
                """, (version, source, status, meta_json, name), commit=True)
            else:
                self._execute("""
                    INSERT INTO packages (name, version, source, status, metadata, installed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (name, version, source, status, meta_json, None), commit=True)

        self.log.info("Upserted package %s (status=%s)" % (name, status))

    def remove_package(self, name: str):
        self._execute("DELETE FROM packages WHERE name=?", (name,), commit=True)
        self.log.info("Removed package %s" % name)

    def get_package(self, name: str) -> Optional[Dict[str, Any]]:
        row = self.fetchone("SELECT * FROM packages WHERE name=?", (name,))
        if not row:
            return None
        # parse metadata JSON
        try:
            row['metadata'] = json.loads(row.get('metadata') or "{}")
        except Exception:
            row['metadata'] = {}
        return row

    def list_packages(self) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT * FROM packages")
        out = []
        for r in rows:
            try:
                r['metadata'] = json.loads(r.get('metadata') or "{}")
            except Exception:
                r['metadata'] = {}
            out.append(r)
        return out

    def list_installed(self) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT * FROM packages WHERE status='installed'")
        out = []
        for r in rows:
            try:
                r['metadata'] = json.loads(r.get('metadata') or "{}")
            except Exception:
                r['metadata'] = {}
            out.append(r)
        return out

    def set_installed_at(self, name: str, ts: str):
        self._execute("UPDATE packages SET installed_at=? WHERE name=?", (ts, name), commit=True)

    def record_build_log(self, pkg_name: str, step: str, message: str, level: str = "INFO"):
        ts = datetime.utcnow().isoformat() + "Z"
        self._execute("""
            INSERT INTO build_logs (pkg_name, step, message, level, ts)
            VALUES (?, ?, ?, ?, ?)
        """, (pkg_name, step, message, level, ts), commit=True)
        self.log.debug("Recorded build log for %s: %s - %s" % (pkg_name, step, message))

    def get_build_logs(self, pkg_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT * FROM build_logs WHERE pkg_name=? ORDER BY id DESC LIMIT ?", (pkg_name, limit))
        return rows

# -------------------------
# Registry (business logic)
# -------------------------
class Registry:
    """
    High-level registry using DB. Uses logger for actions.
    """
    def __init__(self, db: DB):
        self.db = db
        self.log = get_logger("Registry")

    def register_package(self, name: str, version: str, source: str, metadata: Optional[Dict[str, Any]] = None):
        if not name or not version:
            raise ValueError("name and version required")
        self.db.upsert_package(name, version, source, "registered", metadata or {})
        self.log.info("Registered package %s-%s" % (name, version))

    def unregister_package(self, name: str):
        pkg = self.db.get_package(name)
        if not pkg:
            self.log.warning("Attempted to unregister missing package %s" % name)
            return
        self.db.remove_package(name)
        self.log.info("Unregistered package %s" % name)

    def mark_installed(self, name: str, version: Optional[str] = None):
        pkg = self.db.get_package(name)
        if not pkg:
            raise KeyError("Package not found: %s" % name)
        ver = version or pkg.get("version")
        self.db.upsert_package(name, ver, pkg.get("source"), "installed", pkg.get("metadata") or {})
        self.db.set_installed_at(name, datetime.utcnow().isoformat() + "Z")
        self.log.info("Marked installed: %s-%s" % (name, ver))

    def mark_failed(self, name: str, reason: str):
        pkg = self.db.get_package(name)
        if not pkg:
            raise KeyError("Package not found: %s" % name)
        self.db.upsert_package(name, pkg.get("version"), pkg.get("source"), "failed", pkg.get("metadata") or {})
        self.record_step(name, "fail", reason, level="ERROR")
        self.log.error("Marked failed: %s reason=%s" % (name, reason))

    def list_installed(self) -> List[Dict[str, Any]]:
        return self.db.list_installed()

    def get(self, name: str) -> Optional[Dict[str, Any]]:
        return self.db.get_package(name)

    def record_step(self, name: str, step: str, message: str, level: str = "INFO"):
        if not self.db.get_package(name):
            self.log.warning("Recording step for unknown package %s" % name)
        self.db.record_build_log(name, step, message, level)
        # mirror to logger
        log_fn = getattr(self.log, level.lower(), self.log.info)
        log_fn("[%s] %s: %s" % (name, step, message))


# -------------------------
# API única fixa
# -------------------------
class ZeropkgAPI:
    """
    API única que integra logger, DB e registry.
    Métodos principais:
      - install(name, version, source, metadata=None)   # registra -> (download/build simulated) -> marca instalado
      - remove(name)
      - list()
      - get(name)
      - record_step(name, step, message, level='INFO')
      - fail(name, reason)
    Observação: aqui o método install NÃO executa builds reais — ele simula o fluxo de registro e marca instalado.
    Para um builder real, substitua a parte de build por integração ao módulo build/worker.
    """
    def __init__(self, db_path: str):
        self.log = get_logger("zeropkg.api")
        self.db = DB(db_path)
        self.registry = Registry(self.db)

    def register(self, name: str, version: str, source: str, metadata: Optional[Dict[str, Any]] = None):
        self.registry.register_package(name, version, source, metadata)

    def install(self, name: str, version: Optional[str] = None, source: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        """
        Fluxo básico:
         - Se pacote não existir, registra.
         - Registra passo 'fetch' e 'install' (simulados).
         - Marca como installed.
        NOTE: não baixa/compila nada — é ponto de integração para o módulo de download/build.
        """
        pkg = self.db.get_package(name)
        if not pkg:
            if not version or not source:
                raise ValueError("Para novo pacote, forneça version e source")
            # register new
            self.registry.register_package(name, version, source, metadata or {})
            pkg = self.db.get_package(name)

        # fetch step (user/implementer may replace with actual download)
        self.record_step(name, "fetch", f"Fetching source: {pkg.get('source')}", level="INFO")

        # placeholder for checksum/verify (implemented by build module in future)
        self.record_step(name, "verify", "Verification skipped (no verifier configured)", level="WARNING")

        # install step (simulate success)
        try:
            # In a real system this is where build/compile happens.
            self.record_step(name, "install", "Simulated install start", level="INFO")
            # Simulate success
            ver = version or pkg.get("version")
            self.registry.mark_installed(name, ver)
            self.record_step(name, "install", "Simulated install complete", level="INFO")
        except Exception as e:
            self.registry.mark_failed(name, str(e))
            raise

    def remove(self, name: str):
        self.registry.unregister_package(name)

    def list(self) -> List[Dict[str, Any]]:
        return self.db.list_packages()

    def get(self, name: str) -> Optional[Dict[str, Any]]:
        return self.registry.get(name)

    def record_step(self, name: str, step: str, message: str, level: str = "INFO"):
        self.registry.record_step(name, step, message, level)

    def fail(self, name: str, reason: str):
        self.registry.mark_failed(name, reason)

    def close(self):
        self.db.close()


# -------------------------
# Demo / sanity test (executa quando chamado diretamente)
# -------------------------
if __name__ == "__main__":
    # Demonstration of basic usage. Execute: python core.py
    basic_config(level="DEBUG", logfile=os.path.join("/tmp", "zeropkg-demo.log"))
    log = get_logger("demo")

    api = ZeropkgAPI(os.path.join("/tmp", "zeropkg-demo.db"))

    # Register & install a package
    try:
        log.info("Registering package 'demo-pkg'...")
        api.register("demo-pkg", "0.1.0", "https://example.test/demo-pkg.tar.gz", {"maintainer": "you"})
        log.info("Installing 'demo-pkg' (simulated)...")
        api.install("demo-pkg")
        log.info("Listing installed packages:")
        for p in api.list():
            log.info("PACKAGE: %s" % json.dumps(p, ensure_ascii=False))
        log.info("Build logs for demo-pkg:")
        for bl in api.db.get_build_logs("demo-pkg"):
            log.info("LOG: %s" % json.dumps(bl, ensure_ascii=False))
    finally:
        api.close()
        log.info("Demo finished.")
