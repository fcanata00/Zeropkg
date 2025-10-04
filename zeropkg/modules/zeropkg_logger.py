#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_logger.py — Sistema de logging central do Zeropkg
Integrado com zeropkg_config e zeropkg_db.
"""

import os
import sys
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from threading import Lock

try:
    from zeropkg_config import load_config
except ImportError:
    load_config = lambda: {"paths": {"log_dir": "/var/log/zeropkg"}, "logging": {"level": "INFO", "json": False}}

try:
    from zeropkg_db import DBManager
except ImportError:
    DBManager = None


# 🔒 Lock global para acesso thread-safe
_log_lock = Lock()
_logger_cache = {}


def _ensure_log_dir(path: str):
    """Garante que o diretório de logs existe e possui permissões seguras."""
    if not os.path.exists(path):
        os.makedirs(path, mode=0o750, exist_ok=True)
    elif not os.access(path, os.W_OK):
        raise PermissionError(f"Sem permissão para gravar em {path}")


def get_logger(name: str = "zeropkg", stage: str = None) -> logging.Logger:
    """Retorna (ou cria) um logger configurado com rotação e formato adequado."""
    with _log_lock:
        config = load_config()
        log_dir = config.get("paths", {}).get("log_dir", "/var/log/zeropkg")
        log_json = config.get("logging", {}).get("json", False)
        log_level = getattr(logging, config.get("logging", {}).get("level", "INFO").upper(), logging.INFO)

        _ensure_log_dir(log_dir)
        logger_name = f"{name}.{stage}" if stage else name

        if logger_name in _logger_cache:
            return _logger_cache[logger_name]

        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        logger.propagate = False

        # Formato
        fmt = (
            json.dumps({"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"})
            if log_json
            else "%(asctime)s [%(levelname)s] %(message)s"
        )
        formatter = logging.Formatter(fmt, "%Y-%m-%d %H:%M:%S")

        # Handler de arquivo rotativo
        log_path = os.path.join(log_dir, f"{logger_name}.log")
        file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Handler de console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        _logger_cache[logger_name] = logger
        return logger


def log_event(pkg: str, stage: str, message: str, level: str = "info"):
    """Loga um evento para um pacote e opcionalmente registra no banco de dados."""
    logger = get_logger(pkg, stage)
    log_func = getattr(logger, level.lower(), logger.info)
    log_func(message)

    # Registrar também no banco de dados, se disponível
    if DBManager:
        try:
            with DBManager() as db:
                db.conn.execute(
                    "INSERT INTO events (pkg_name, stage, event, timestamp) VALUES (?, ?, ?, ?)",
                    (pkg, stage, message, int(time.time())),
                )
        except Exception:
            pass


def log_global(message: str, level: str = "info"):
    """Loga mensagens globais (fora do contexto de pacote)."""
    logger = get_logger("zeropkg")
    log_func = getattr(logger, level.lower(), logger.info)
    log_func(message)

    if DBManager:
        try:
            with DBManager() as db:
                db.conn.execute(
                    "INSERT INTO events (pkg_name, stage, event, timestamp) VALUES (?, ?, ?, ?)",
                    ("global", "system", message, int(time.time())),
                )
        except Exception:
            pass


def rotate_logs(max_age_days: int = 30):
    """Remove logs antigos automaticamente."""
    config = load_config()
    log_dir = config.get("paths", {}).get("log_dir", "/var/log/zeropkg")
    now = time.time()

    for fname in os.listdir(log_dir):
        fpath = os.path.join(log_dir, fname)
        if not os.path.isfile(fpath):
            continue
        if now - os.path.getmtime(fpath) > max_age_days * 86400:
            try:
                os.remove(fpath)
            except Exception:
                pass


def get_session_logger():
    """Cria logger específico para sessões (ex: chamadas CLI)."""
    ts = time.strftime("%Y%m%d-%H%M%S")
    return get_logger(f"session-{ts}")


# Exemplo de uso
if __name__ == "__main__":
    log_global("Inicializando Zeropkg Logger", "info")
    log_event("gcc", "build", "Compilando GCC etapa 1", "info")
    log_event("glibc", "install", "Instalação concluída com sucesso", "info")
