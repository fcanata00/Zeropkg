#!/usr/bin/env python3
# zeropkg_logger.py - Sistema de logging do Zeropkg
# -*- coding: utf-8 -*-

import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional

# Configuração padrão, pode ser sobrescrita pelo config.toml
LOG_DIR = "/var/log/zeropkg"
DEFAULT_LOG = "zeropkg.log"

# Integração com DBManager
try:
    from zeropkg_db import DBManager
    HAS_DB = True
except Exception:
    HAS_DB = False


def setup_logger(pkg_name: Optional[str] = None, stage: Optional[str] = None,
                 log_dir: Optional[str] = None, level: str = "INFO") -> logging.Logger:
    """
    Configura logger para um pacote/estágio específico.
    """
    log_dir = log_dir or LOG_DIR
    os.makedirs(log_dir, exist_ok=True)

    logger_name = "zeropkg"
    if pkg_name:
        logger_name += f".{pkg_name}"
    if stage:
        logger_name += f".{stage}"

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch_formatter = logging.Formatter("[%(levelname)s] %(message)s")
        ch.setFormatter(ch_formatter)
        logger.addHandler(ch)

        # File handler
        if pkg_name and stage:
            logfile = os.path.join(log_dir, f"{pkg_name}-{stage}.log")
        elif pkg_name:
            logfile = os.path.join(log_dir, f"{pkg_name}.log")
        else:
            logfile = os.path.join(log_dir, DEFAULT_LOG)

        fh = RotatingFileHandler(logfile, maxBytes=2*1024*1024, backupCount=3)
        fh.setLevel(logging.DEBUG)
        fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)

    return logger


def log_event(pkg_name: Optional[str], stage: Optional[str], message: str,
              level: str = "info", log_dir: Optional[str] = None):
    """
    Loga um evento para um pacote/estágio específico.
    Também envia para o banco de dados, se disponível.
    """
    logger = setup_logger(pkg_name, stage, log_dir=log_dir)

    level = level.lower()
    if level not in ("debug", "info", "warning", "error", "critical"):
        level = "info"

    log_method = getattr(logger, level)
    log_method(message)

    # Persistir no DB
    if HAS_DB and pkg_name:
        try:
            db = DBManager()
            db.log_event(pkg_name, stage or "general", message, level.upper())
            db.close()
        except Exception:
            # não interromper por falha no DB
            pass


def get_logger(pkg: Optional[str] = None, stage: Optional[str] = None) -> logging.Logger:
    """
    Atalho simples para pegar logger configurado.
    """
    return setup_logger(pkg, stage)
