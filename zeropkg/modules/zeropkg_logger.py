#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zeropkg Logger Module
---------------------
Sistema unificado de logging para todos os módulos Zeropkg.

Recursos:
- Logs em texto e JSON (com compressão .gz)
- Rotação automática
- Integração com zeropkg_db (DBManager)
- Captura de exceções com tracebacks detalhados
- Criação de logs por sessão de build/instalação
- Compatível com chroot e fakeroot
"""

import os
import sys
import json
import gzip
import shutil
import traceback
import datetime
import logging
from logging.handlers import RotatingFileHandler

try:
    from zeropkg_db import DBManager
except ImportError:
    DBManager = None


class ZeropkgLogger:
    def __init__(self, log_dir="/var/log/zeropkg", db_path="/var/lib/zeropkg/zeropkg.db"):
        self.log_dir = os.path.abspath(log_dir)
        self.db_path = db_path
        self.db = DBManager(db_path) if DBManager else None
        os.makedirs(self.log_dir, exist_ok=True)

        # Caminho principal dos logs
        self.text_log_path = os.path.join(self.log_dir, "zeropkg.log")
        self.json_log_path = os.path.join(self.log_dir, "zeropkg.jsonl")

        # Configuração do logger padrão
        self.logger = logging.getLogger("zeropkg")
        self.logger.setLevel(logging.DEBUG)

        # Evita duplicação de handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self):
        # Log em texto com rotação
        text_handler = RotatingFileHandler(
            self.text_log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        text_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(module)s] %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        text_handler.setFormatter(text_formatter)

        # Saída no terminal
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(text_formatter)

        self.logger.addHandler(text_handler)
        self.logger.addHandler(console_handler)

    def _write_json(self, level, module, message, **kwargs):
        """Escreve log em formato JSON + compressão automática."""
        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "level": level,
            "module": module,
            "message": message,
            "extra": kwargs,
        }
        os.makedirs(os.path.dirname(self.json_log_path), exist_ok=True)
        with open(self.json_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")
        self._compress_json_if_needed()

    def _compress_json_if_needed(self):
        """Compacta JSON se o arquivo ultrapassar 10MB."""
        if os.path.exists(self.json_log_path) and os.path.getsize(self.json_log_path) > 10 * 1024 * 1024:
            gz_path = f"{self.json_log_path}.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.gz"
            with open(self.json_log_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            open(self.json_log_path, "w").close()

    def log(self, level, message, module="core", **kwargs):
        """Log unificado (texto + JSON + DB)."""
        if level.lower() == "debug":
            self.logger.debug(message)
        elif level.lower() == "info":
            self.logger.info(message)
        elif level.lower() == "warning":
            self.logger.warning(message)
        elif level.lower() == "error":
            self.logger.error(message)
        elif level.lower() == "critical":
            self.logger.critical(message)
        else:
            self.logger.info(message)

        self._write_json(level.upper(), module, message, **kwargs)

        # Registro opcional no DB
        if self.db:
            try:
                self.db.insert_log(level.upper(), module, message)
            except Exception as e:
                self.logger.error(f"Falha ao registrar log no DB: {e}")

    def log_exception(self, module, exc: Exception):
        """Captura exceções e loga traceback completo."""
        tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.log("error", f"Exception in {module}: {exc}", module=module, traceback=tb_str)

    def new_session(self, action_type, package=None):
        """Cria um log de sessão para build/instalação."""
        session_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        session_log = os.path.join(self.log_dir, f"session-{session_id}.log")
        self.log("info", f"Iniciando nova sessão: {action_type} para {package or 'desconhecido'}")
        if self.db:
            self.db.insert_session(session_id, action_type, package)
        return session_id, session_log

    def cleanup_old_logs(self, keep_days=30):
        """Remove logs antigos."""
        now = datetime.datetime.now()
        for f in os.listdir(self.log_dir):
            fpath = os.path.join(self.log_dir, f)
            if os.path.isfile(fpath):
                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fpath))
                if (now - mtime).days > keep_days:
                    os.remove(fpath)
                    self.log("info", f"Log antigo removido: {fpath}")

# Exemplo de uso
if __name__ == "__main__":
    logger = ZeropkgLogger()
    logger.log("info", "Teste de log de informação.")
    logger.log("error", "Erro simulado no sistema.", module="installer")
    try:
        raise RuntimeError("Exceção de teste")
    except Exception as e:
        logger.log_exception("builder", e)
