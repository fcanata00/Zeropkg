"""
zeropkg_logger.py — Sistema de logging, métricas e auditoria do Zeropkg.
Totalmente integrado com zeropkg_config, zeropkg_db e demais módulos.

Recursos:
- Log híbrido (texto + JSON)
- Sessões automáticas de log por execução
- Compressão automática e limpeza de logs antigos
- Registro de métricas com @perf_timer
- Log remoto opcional via HTTPS
- Integração total com Zeropkg Builder, Installer, DepClean, Vuln, Update
"""

import os
import sys
import atexit
import json
import gzip
import lzma
import time
import shutil
import signal
import threading
import datetime
import traceback
from functools import wraps

# Integração opcional com outros módulos
try:
    from zeropkg_config import get_config
except ImportError:
    get_config = lambda: {}

try:
    from zeropkg_db import record_event
except ImportError:
    def record_event(*args, **kwargs): pass


LOG_DIR = "/var/log/zeropkg"
SESSION_FILE = None
LOCK = threading.Lock()
CONFIG = get_config()
SESSION_CONTEXT = {
    "pid": os.getpid(),
    "start_time": datetime.datetime.now().isoformat(),
    "module": "zeropkg_logger",
}

# ---- Utilidades ---- #

def _colorize(level: str, msg: str) -> str:
    if not sys.stdout.isatty():
        return msg
    colors = {
        "INFO": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "DEBUG": "\033[94m",
        "SECURITY": "\033[95m",
        "PERF": "\033[96m",
        "HOOK": "\033[90m",
    }
    reset = "\033[0m"
    return f"{colors.get(level, '')}{msg}{reset}"


def _compress_log(file_path: str, method="gzip"):
    if not os.path.exists(file_path):
        return
    compressed_path = file_path + (".xz" if method == "xz" else ".gz")
    try:
        with open(file_path, "rb") as src, \
             (lzma.open(compressed_path, "wb") if method == "xz" else gzip.open(compressed_path, "wb")) as dst:
            shutil.copyfileobj(src, dst)
        os.remove(file_path)
    except Exception as e:
        print(f"Falha ao comprimir log: {e}")


def _cleanup_old_logs(max_age_days=7):
    cutoff = time.time() - (max_age_days * 86400)
    for f in os.listdir(LOG_DIR):
        path = os.path.join(LOG_DIR, f)
        if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
            try:
                os.remove(path)
            except Exception:
                pass


def _upload_logs():
    url = CONFIG.get("log", {}).get("upload_url")
    if not url:
        return
    try:
        import requests
        for f in os.listdir(LOG_DIR):
            path = os.path.join(LOG_DIR, f)
            if f.endswith((".gz", ".xz")):
                with open(path, "rb") as fh:
                    requests.post(url, files={"file": fh})
    except Exception as e:
        log_event("UPLOAD_FAIL", f"Falha ao enviar logs: {e}", level="WARNING")


# ---- Inicialização ---- #

def start_session():
    global SESSION_FILE
    os.makedirs(LOG_DIR, exist_ok=True)
    session_name = datetime.datetime.now().strftime("session-%Y%m%d-%H%M%S.log")
    SESSION_FILE = os.path.join(LOG_DIR, session_name)
    with open(SESSION_FILE, "w") as f:
        f.write(f"==== Zeropkg Log Session Started at {SESSION_CONTEXT['start_time']} ====\n")
    _cleanup_old_logs(CONFIG.get("log", {}).get("max_age_days", 7))
    log_event("SESSION_START", "Sessão de log iniciada", level="INFO")


def end_session():
    if not SESSION_FILE:
        return
    log_event("SESSION_END", "Encerrando sessão de log", level="INFO")
    compression = CONFIG.get("log", {}).get("compression", "gzip")
    _compress_log(SESSION_FILE, compression)
    _upload_logs()


atexit.register(end_session)
signal.signal(signal.SIGTERM, lambda *_: end_session())
signal.signal(signal.SIGINT, lambda *_: end_session())


# ---- Logging e Métricas ---- #

def log_event(event_type: str, message: str, level="INFO", metadata=None):
    """Registra evento em log e banco."""
    timestamp = datetime.datetime.now().isoformat()
    line = f"[{timestamp}] [{level}] [{event_type}] {message}"
    entry = {
        "timestamp": timestamp,
        "level": level,
        "event_type": event_type,
        "message": message,
        "pid": SESSION_CONTEXT["pid"],
        "module": SESSION_CONTEXT.get("module"),
        "metadata": metadata or {},
    }

    with LOCK:
        # Log no terminal
        print(_colorize(level, line))
        # Log em arquivo
        if SESSION_FILE:
            with open(SESSION_FILE, "a") as f:
                f.write(line + "\n")
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        # Log no banco
        try:
            record_event(event_type, message, level, metadata)
        except Exception:
            pass


def perf_timer(func):
    """Decorator que mede o tempo de execução e registra métricas."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start
            log_event("PERF", f"{func.__name__} executado em {duration:.2f}s", "PERF")
            record_event("PERF", f"{func.__name__}", "INFO", {"duration": duration})
            return result
        except Exception as e:
            duration = time.time() - start
            log_event("ERROR", f"{func.__name__} falhou em {duration:.2f}s: {e}", "ERROR")
            raise
    return wrapper


def log_perf_summary(stats: dict):
    """Registra sumário geral de desempenho."""
    total_time = stats.get("total_time", 0)
    pkgs_built = stats.get("pkgs_built", 0)
    deps_resolved = stats.get("deps_resolved", 0)
    msg = f"Resumo: {pkgs_built} pacotes em {total_time:.2f}s, {deps_resolved} dependências."
    log_event("SUMMARY", msg, "INFO", stats)


# ---- CLI ---- #

def main():
    import argparse
    parser = argparse.ArgumentParser(description="CLI de Logs do Zeropkg")
    parser.add_argument("--list-sessions", action="store_true", help="Lista sessões de log")
    parser.add_argument("--cleanup", action="store_true", help="Limpa logs antigos")
    parser.add_argument("--upload", action="store_true", help="Envia logs para o servidor remoto")
    args = parser.parse_args()

    if args.list_sessions:
        print("\n".join(sorted(os.listdir(LOG_DIR))))
    elif args.cleanup:
        _cleanup_old_logs()
        print("Logs antigos removidos.")
    elif args.upload:
        _upload_logs()
        print("Logs enviados com sucesso.")
    else:
        parser.print_help()


if __name__ == "__main__":
    start_session()
    try:
        main()
    finally:
        end_session()
