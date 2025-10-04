#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_logger.py — Advanced logging for Zeropkg

Features:
 - Configurable log level via zeropkg_config
 - Session-based logging (session-YYYYmmdd-HHMMSS)
 - Console (with colors) + file logging
 - JSON log file per session (rotated/compressed .gz/.xz on close)
 - Optional SQLite event recording (if zeropkg_db provides API)
 - Optional upload of logs to remote endpoint (requests required)
 - Performance measurement utilities and decorator
 - Cleanup of old logs
 - Safe operation when optional modules absent
"""

from __future__ import annotations
import os
import sys
import json
import time
import gzip
import shutil
import logging
import sqlite3
import hashlib
import tempfile
import datetime
import atexit
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from functools import wraps

# Optional dependencies
try:
    import requests
    REQUESTS_AVAILABLE = True
except Exception:
    REQUESTS_AVAILABLE = False

# Try to import project config and db (optional)
try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        # safe defaults
        return {
            "paths": {
                "log_dir": "/var/log/zeropkg",
                "state_dir": "/var/lib/zeropkg",
                "cache_dir": "/var/cache/zeropkg"
            },
            "logging": {
                "level": "INFO",
                "compress": "gzip",   # gzip or xz or none
                "max_age_days": 30,
                "upload": {"enabled": False, "url": None, "token": None}
            }
        }

try:
    from zeropkg_db import ZeroPKGDB, record_log_event
    DB_AVAILABLE = True
except Exception:
    ZeroPKGDB = None
    record_log_event = None
    DB_AVAILABLE = False

try:
    from zeropkg_update import ZeropkgUpdate
    UPDATE_AVAILABLE = True
except Exception:
    UPDATE_AVAILABLE = False

try:
    from zeropkg_vuln import ZeroPKGVulnManager
    VULN_AVAILABLE = True
except Exception:
    VULN_AVAILABLE = False

# -------------------------
# Utilities
# -------------------------
CFG = load_config()
LOG_DIR = Path(CFG.get("paths", {}).get("log_dir", "/var/log/zeropkg")).resolve()
STATE_DIR = Path(CFG.get("paths", {}).get("state_dir", "/var/lib/zeropkg")).resolve()
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

LOGGING_CFG = CFG.get("logging", {})
DEFAULT_LEVEL = LOGGING_CFG.get("level", "INFO").upper()
COMPRESS_MODE = LOGGING_CFG.get("compress", "gzip")  # gzip, xz, none
MAX_AGE_DAYS = int(LOGGING_CFG.get("max_age_days", 30))
UPLOAD_CFG = LOGGING_CFG.get("upload", {"enabled": False})

ANSI_ENABLED = sys.stdout.isatty()

LEVEL_COLORS = {
    "DEBUG": "\033[36m",   # cyan
    "INFO": "\033[32m",    # green
    "WARNING": "\033[33m", # yellow
    "ERROR": "\033[31m",   # red
    "CRITICAL": "\033[41m" # red bg
}
RESET_COLOR = "\033[0m"

def _colorize(level: str, text: str) -> str:
    if not ANSI_ENABLED:
        return text
    color = LEVEL_COLORS.get(level.upper(), "")
    return f"{color}{text}{RESET_COLOR}" if color else text

def _safe_write_json(path: Path, data: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(path)

# -------------------------
# Logger Manager
# -------------------------
class ZeropkgLoggerManager:
    def __init__(self):
        self.session_id = None
        self.session_dir: Optional[Path] = None
        self.text_log_path: Optional[Path] = None
        self.json_log_path: Optional[Path] = None
        self.handlers = []
        self.loggers: Dict[str, logging.Logger] = {}
        self.level = getattr(logging, DEFAULT_LEVEL, logging.INFO)
        self.compress = COMPRESS_MODE.lower()
        self.db = ZeroPKGDB() if DB_AVAILABLE else None
        self.upload_cfg = UPLOAD_CFG or {}
        self._open_session()

    def _open_session(self):
        ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        self.session_id = f"session-{ts}"
        self.session_dir = LOG_DIR / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        # text and json logs
        self.text_log_path = self.session_dir / f"{self.session_id}.log"
        self.json_log_path = self.session_dir / f"{self.session_id}.json"
        # base logger config
        logging.basicConfig(level=self.level)
        # create root logger file handler
        fh = logging.FileHandler(self.text_log_path, encoding="utf-8")
        fh.setLevel(self.level)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        self.handlers.append(fh)
        # console handler with color
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(self.level)
        ch.setFormatter(AnsiFormatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        self.handlers.append(ch)
        # attach handlers lazily to created loggers
        atexit.register(self.close_session)

    def get_logger(self, name: str) -> logging.Logger:
        if name in self.loggers:
            return self.loggers[name]
        logger = logging.getLogger(f"zeropkg.{name}")
        logger.setLevel(self.level)
        # prevent duplicate handlers if reloaded
        for h in self.handlers:
            logger.addHandler(h)
        # ensure no propagation to avoid double printing
        logger.propagate = False
        self.loggers[name] = logger
        return logger

    def emit_json(self, record: Dict[str, Any]):
        # append a JSON object per line to json log
        try:
            with open(self.json_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def record_event_db(self, record: Dict[str, Any]):
        if not DB_AVAILABLE or self.db is None:
            return False
        try:
            if record_log_event:
                record_log_event(record)
                return True
            # fallback: try db method names
            if hasattr(self.db, "record_event"):
                self.db.record_event(record)
                return True
        except Exception:
            pass
        return False

    def close_session(self, compress: Optional[str] = None, upload: Optional[bool] = None):
        # detach handlers from loggers
        for logger in list(self.loggers.values()):
            for h in list(logger.handlers):
                logger.removeHandler(h)
        # compress json log
        compress = (compress or self.compress).lower()
        if compress not in ("gzip", "xz", "none", ""):
            compress = "gzip"
        try:
            if self.json_log_path and self.json_log_path.exists():
                if compress == "gzip":
                    with open(self.json_log_path, "rb") as f_in:
                        with gzip.open(str(self.json_log_path) + ".gz", "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    self.json_log_path.unlink(missing_ok=True)
                elif compress == "xz":
                    # use system xz if available
                    if shutil.which("xz"):
                        _p = str(self.json_log_path)
                        os.system(f"xz -z -9 {_p}")
                    else:
                        # fallback to gzip
                        with open(self.json_log_path, "rb") as f_in:
                            with gzip.open(str(self.json_log_path) + ".gz", "wb") as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        self.json_log_path.unlink(missing_ok=True)
            # also compress plain text log if desired (optional)
            if self.text_log_path and self.text_log_path.exists():
                if compress == "gzip":
                    with open(self.text_log_path, "rb") as f_in:
                        with gzip.open(str(self.text_log_path) + ".gz", "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    self.text_log_path.unlink(missing_ok=True)
        except Exception as e:
            # best-effort; do not raise
            print(f"[zeropkg_logger] compress failed: {e}", file=sys.stderr)

        # optionally upload
        upload = upload if upload is not None else bool(self.upload_cfg.get("enabled", False))
        if upload and REQUESTS_AVAILABLE:
            try:
                self.upload_logs()
            except Exception:
                pass

    def upload_logs(self) -> bool:
        """Upload compressed logs to configured endpoint (best-effort)."""
        if not REQUESTS_AVAILABLE:
            return False
        url = self.upload_cfg.get("url")
        token = self.upload_cfg.get("token")
        if not url:
            return False
        # find compressed files in session_dir
        files = list(self.session_dir.glob("*.gz")) + list(self.session_dir.glob("*.xz"))
        if not files:
            return False
        files_payload = {}
        for f in files:
            files_payload[f.name] = open(f, "rb")
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            r = requests.post(url, files=files_payload, headers=headers, timeout=30)
            for fh in files_payload.values():
                fh.close()
            return r.status_code in (200, 201)
        except Exception:
            return False

    def cleanup_old_logs(self, max_age_days: Optional[int] = None):
        """Remove session directories older than max_age_days (best-effort)."""
        max_age_days = max_age_days if max_age_days is not None else MAX_AGE_DAYS
        cutoff = time.time() - (max_age_days * 86400)
        for d in LOG_DIR.iterdir():
            try:
                if not d.is_dir():
                    continue
                mtime = d.stat().st_mtime
                if mtime < cutoff:
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass

# -------------------------
# Console Formatter with ANSI colors
# -------------------------
class AnsiFormatter(logging.Formatter):
    def format(self, record):
        level = record.levelname
        msg = super().format(record)
        return _colorize(level, msg)

# -------------------------
# Global manager instance
# -------------------------
_manager = ZeropkgLoggerManager()

# -------------------------
# Public API
# -------------------------
def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger instance. Use like:
        log = get_logger("builder")
        log.info("Starting build")
    """
    return _manager.get_logger(name)

def log_event(pkg: str, stage: str, message: str, level: str = "info", extra: Optional[Dict[str,Any]] = None):
    """
    High-level event logging (records to file, json log and db if available).
    pkg: package or component name
    stage: stage name (build/install/remove/update)
    message: free text
    level: logging level string
    extra: optional dict with structured data
    """
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger = get_logger(pkg)
    msg = f"[{stage}] {message}"
    # log text
    logger.log(lvl, msg)
    # log structured JSON line
    rec = {
        "ts": int(time.time()),
        "session": _manager.session_id,
        "pkg": pkg,
        "stage": stage,
        "level": level.upper(),
        "message": message,
        "extra": extra or {}
    }
    _manager.emit_json(rec)
    # record in DB if available
    try:
        _manager.record_event_db(rec)
    except Exception:
        pass

def log_exception(pkg: str, stage: str, exc: Exception, level: str = "error", extra: Optional[Dict[str,Any]] = None):
    tb = getattr(exc, "__traceback__", None)
    msg = f"{exc}"
    if tb:
        import traceback as _tb
        msg += "\n" + "".join(_tb.format_tb(tb))
    log_event(pkg, stage, msg, level=level, extra=extra)

def record_perf(name: str, op: str, duration_s: float, meta: Optional[Dict[str,Any]] = None):
    """
    Record a performance metric for operation 'op' under component 'name'.
    """
    rec = {
        "ts": int(time.time()),
        "session": _manager.session_id,
        "component": name,
        "operation": op,
        "duration_s": duration_s,
        "meta": meta or {}
    }
    # emit into json log and DB optionally
    _manager.emit_json({"perf": rec})
    try:
        _manager.record_event_db({"perf": rec})
    except Exception:
        pass

def perf_timer(name: str, op: str):
    """
    Decorator/context manager to measure execution time and record_perf automatically.
    Usage:
        @perf_timer("builder", "build")
        def build(...): ...
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                res = func(*args, **kwargs)
                return res
            finally:
                duration = time.time() - start
                record_perf(name, op, duration, meta={"args": str(args)[:200], "kwargs": str(kwargs)[:200]})
        return wrapper
    return decorator

def new_session(prefix: Optional[str] = None):
    """Start a new logging session (closing previous one)."""
    try:
        _manager.close_session()
    except Exception:
        pass
    # recreate manager (simple approach)
    global _manager
    _manager = ZeropkgLoggerManager()
    return _manager.session_id

def close_session(compress: Optional[str] = None, upload: Optional[bool] = None):
    _manager.close_session(compress=compress, upload=upload)

def upload_logs_now() -> bool:
    return _manager.upload_logs()

def cleanup_old_logs(max_age_days: Optional[int] = None):
    _manager.cleanup_old_logs(max_age_days)

# -------------------------
# Convenience bootstrap
# -------------------------
# expose top-level root logger
root = get_logger("root")

# Example convenience functions that integrate with update/vuln if available
def log_update_summary(summary: Dict[str,Any]):
    """
    Write an update summary both to logs and to update module if present.
    """
    pkg = "zeropkg.update"
    log_event(pkg, "summary", json.dumps(summary), level="info")
    if UPDATE_AVAILABLE:
        try:
            # attempt to call update module API to push summary (best-effort)
            upd = ZeropkgUpdate()
            # if update module exposes a hook, call it - best-effort only
            if hasattr(upd, "record_update_summary"):
                upd.record_update_summary(summary)
        except Exception:
            pass

def log_vuln_scan_result(result: Dict[str,Any]):
    pkg = "zeropkg.vuln"
    log_event(pkg, "scan", json.dumps(result), level="warning" if result.get("critical") else "info")
    if VULN_AVAILABLE:
        try:
            vm = ZeroPKGVulnManager()
            if hasattr(vm, "record_scan"):
                vm.record_scan(result)
        except Exception:
            pass

# Ensure session closed on exit (redundant with atexit in manager)
atexit.register(lambda: _manager.close_session())

# -------------------------
# CLI for debugging
# -------------------------
if __name__ == "__main__":
    import argparse, pprint
    parser = argparse.ArgumentParser(prog="zeropkg-logger", description="Zeropkg advanced logger CLI")
    parser.add_argument("--list-sessions", action="store_true", help="List session directories")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup old session logs")
    parser.add_argument("--upload", action="store_true", help="Upload current session logs now")
    args = parser.parse_args()
    if args.list_sessions:
        for d in sorted(LOG_DIR.iterdir()):
            if d.is_dir():
                print(d.name)
        sys.exit(0)
    if args.cleanup:
        cleanup_old_logs()
        print("Cleanup triggered")
        sys.exit(0)
    if args.upload:
        ok = upload_logs_now()
        print("Upload:", ok)
        sys.exit(0)
    parser.print_help()
