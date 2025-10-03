#!/usr/bin/env python3
"""
zeropkg_logger.py - Sistema de logging do Zeropkg

- Logs em /var/log/zeropkg
- RotatingFileHandler para evitar logs gigantes
- Suporte a logs globais e por pacote/estágio
- Integração opcional com zeropkg_db.log_event
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional

LOG_DIR = "/var/log/zeropkg"
DEFAULT_LOG = "zeropkg.log"

# tentar integrar com DB se disponível
try:
    from zeropkg_db import log_event as db_log_event
    HAS_DB = True
except Exception:
    HAS_DB = False


def setup_logger(pkg_name: Optional[str] = None, stage: Optional[str] = None) -> logging.Logger:
    """
    Configura logger para um pacote/estágio específico.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    logger_name = "zeropkg"
    if pkg_name:
        logger_name += f".{pkg_name}"
    if stage:
        logger_name += f".{stage}"

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch_formatter = logging.Formatter("[%(levelname)s] %(message)s")
        ch.setFormatter(ch_formatter)
        logger.addHandler(ch)

        # File handler
        if pkg_name and stage:
            logfile = os.path.join(LOG_DIR, f"{pkg_name}-{stage}.log")
        elif pkg_name:
            logfile = os.path.join(LOG_DIR, f"{pkg_name}.log")
        else:
            logfile = os.path.join(LOG_DIR, DEFAULT_LOG)

        fh = RotatingFileHandler(logfile, maxBytes=2*1024*1024, backupCount=3)
        fh.setLevel(logging.DEBUG)
        fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)

    return logger


def log_event(pkg_name: Optional[str], stage: Optional[str], message: str, level: str = "info"):
    """
    Loga um evento para um pacote/estágio específico.
    Também envia para o banco de dados, se disponível.
    """
    logger = setup_logger(pkg_name, stage)

    level = level.lower()
    if level not in ("debug", "info", "warning", "error", "critical"):
        level = "info"

    log_method = getattr(logger, level)
    log_method(message)

    # opcional: persistir no banco
    if HAS_DB and pkg_name:
        try:
            db_log_event(pkg_name, stage or "general", f"[{level.upper()}] {message}")
        except Exception:
            # não interromper por falha no DB
            pass
