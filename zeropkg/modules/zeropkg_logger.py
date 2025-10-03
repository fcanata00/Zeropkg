import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional

LOG_DIR = "/var/log/zeropkg"

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
            logfile = os.path.join(LOG_DIR, "zeropkg.log")

        fh = RotatingFileHandler(logfile, maxBytes=2*1024*1024, backupCount=3)
        fh.setLevel(logging.DEBUG)
        fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)

    return logger

def log_event(pkg_name: str, stage: str, message: str, level: str = "info"):
    """
    Loga um evento para um pacote/estágio específico.
    """
    logger = setup_logger(pkg_name, stage)
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)
