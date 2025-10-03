# ports_manager_initial_modules.py
# Módulos iniciais colocados em um único arquivo para revisão: utils, logging_ui, metafile
# NÃO COLOQUEI MÓDULOS complexos (resolver, builder, sandbox) — esses vêm depois.

"""
Este arquivo contém implementações iniciais (prontas para revisão e uso) de:
 - utils: constantes, helpers comuns
 - logging_ui: logger colorido para terminal + logger JSON/arquivo rotativo
 - metafile: parser/validador básico de metafiles TOML + expansão de variáveis

Objetivo: fornecer blocos estáveis que raramente precisam de refatoração profunda.
"""

# -----------------------------
# utils
# -----------------------------
from __future__ import annotations
import os
import sys
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Caminhos padrão e constantes
PORTS_DIR_DEFAULT = Path("/usr/ports")
LOG_DIR_DEFAULT = Path("/var/log/pmgr")
CACHE_DIR_DEFAULT = Path("/var/cache/pmgr")
DB_PATH_DEFAULT = Path("/var/lib/pmgr/db.sqlite3")

def ensure_dirs(*dirs: Path) -> None:
    for d in dirs:
        if not d.exists():
            d.mkdir(parents=True, exist_ok=True)

def read_text_file(p: Path, encoding: str = "utf-8") -> str:
    with p.open("r", encoding=encoding) as f:
        return f.read()

def atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
    tmp.replace(path)

# -----------------------------
# logging_ui
# -----------------------------
import logging
import time
from logging.handlers import RotatingFileHandler

# ANSI color sequences
ANSI = {
    'reset': '\u001b[0m',
    'bold': '\u001b[1m',
    'red': '\u001b[31m',
    'green': '\u001b[32m',
    'yellow': '\u001b[33m',
    'blue': '\u001b[34m',
    'magenta': '\u001b[35m',
    'cyan': '\u001b[36m',
    'white': '\u001b[37m',
}

LEVEL_TO_COLOR = {
    logging.DEBUG: ANSI['cyan'],
    logging.INFO: ANSI['blue'],
    logging.WARNING: ANSI['yellow'],
    logging.ERROR: ANSI['red'],
    logging.CRITICAL: ANSI['bold'] + ANSI['red'],
}

class ColorFormatter(logging.Formatter):
    def __init__(self, fmt: Optional[str] = None, use_color: bool = True):
        super().__init__(fmt or "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        self.use_color = use_color and sys.stdout.isatty()

    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        if self.use_color:
            color = LEVEL_TO_COLOR.get(record.levelno, '')
            return f"{color}{msg}{ANSI['reset']}"
        return msg

class JsonFileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)),
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'funcName': record.funcName,
            'lineno': record.lineno,
        }
        if record.exc_info:
            payload['exc'] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(app_name: str = 'pmgr', *,
                  log_dir: Path = LOG_DIR_DEFAULT,
                  level: int = logging.INFO,
                  max_bytes: int = 10 * 1024 * 1024,
                  backup_count: int = 5) -> logging.Logger:
    """Configura logging com handler para terminal (colorido) e arquivo JSON rotativo.

    Retorna o logger raiz nomeado `app_name`.
    """
    ensure_dirs(log_dir)
    logger = logging.getLogger(app_name)
    logger.setLevel(level)
    # Evita duplicação se já configurado
    if logger.handlers:
        return logger

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(ColorFormatter())
    logger.addHandler(ch)

    # Rotating JSON file handler
    logfile = log_dir / f"{app_name}.log"
    fh = RotatingFileHandler(str(logfile), maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonFileFormatter())
    logger.addHandler(fh)

    # Make logs less verbose for noisy libs by default
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('git').setLevel(logging.WARNING)

    return logger

# -----------------------------
# metafile (TOML parser + validator)
# -----------------------------
import tomllib
from dataclasses import dataclass, field
from typing import List, Union

# Esquema mínimo esperado para um metafile — representado em checagens
REQUIRED_FIELDS = ['name', 'version']

@dataclass
class SourceEntry:
    type: str
    url: str
    checksum: Optional[str] = None
    format: Optional[str] = None
    priority: int = 0

@dataclass
class MetaFile:
    name: str
    version: str
    summary: Optional[str] = None
    license: Optional[str] = None
    maintainers: List[str] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)
    sources: List[SourceEntry] = field(default_factory=list)
    dependencies: Dict[str, List[str]] = field(default_factory=dict)
    hooks: Dict[str, Union[str, List[str]]] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_toml_bytes(cls, data: bytes) -> 'MetaFile':
        parsed = tomllib.loads(data.decode('utf-8'))
        return cls.from_dict(parsed)

    @classmethod
    def from_path(cls, path: Path) -> 'MetaFile':
        data = path.read_bytes()
        return cls.from_toml_bytes(data)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> 'MetaFile':
        # Validação básica
        for f in REQUIRED_FIELDS:
            if f not in raw:
                raise ValueError(f"Campo obrigatório ausente no metafile: {f}")
        name = raw['name']
        version = raw['version']
        summary = raw.get('summary')
        license = raw.get('license')
        maintainers = raw.get('maintainers', []) or []
        variables = raw.get('variables', {}) or {}
        deps = raw.get('dependencies', {}) or {}
        hooks = raw.get('hooks', {}) or {}

        sources_raw = raw.get('sources', []) or []
        sources: List[SourceEntry] = []
        for s in sources_raw:
            if isinstance(s, str):
                sources.append(SourceEntry(type='url', url=s))
            elif isinstance(s, dict):
                sources.append(SourceEntry(
                    type=s.get('type', 'url'),
                    url=s.get('url', ''),
                    checksum=s.get('checksum'),
                    format=s.get('format'),
                    priority=int(s.get('priority', 0) or 0),
                ))
            else:
                raise ValueError("Formato inválido em 'sources' do metafile")

        mf = cls(
            name=name,
            version=version,
            summary=summary,
            license=license,
            maintainers=maintainers,
            variables=variables,
            sources=sources,
            dependencies=deps,
            hooks=hooks,
            raw=raw,
        )
        return mf

    def validate(self) -> Tuple[bool, List[str]]:
        issues = []
        if not self.name.strip():
            issues.append('name vazio')
        if not self.version.strip():
            issues.append('version vazio')
        # Checagens simples sobre sources
        if not self.sources:
            issues.append('nenhuma source declarada')
        else:
            for i, s in enumerate(self.sources):
                if not s.url:
                    issues.append(f'source[{i}] sem url')
        return (len(issues) == 0, issues)

    def expand_variables(self, extra: Optional[Dict[str, str]] = None) -> None:
        """Expande variáveis em strings dentro do metafile (sources.url, hooks, etc.).

        Usa self.variables como base e sobrescreve com `extra`.
        Formato de variável suportada: {VAR_NAME}
        """
        vars_combined = dict(self.variables)
        if extra:
            vars_combined.update(extra)

        def _expand_in_str(s: str) -> str:
            try:
                return s.format(**vars_combined)
            except Exception:
                # Falha na expansão não é fatal aqui; mantemos original para debug
                return s

        # Expand in sources
        for se in self.sources:
            se.url = _expand_in_str(se.url)
            if se.format:
                se.format = _expand_in_str(se.format)

        # Expand hooks
        for k, v in list(self.hooks.items()):
            if isinstance(v, str):
                self.hooks[k] = _expand_in_str(v)
            elif isinstance(v, list):
                self.hooks[k] = [_expand_in_str(x) for x in v]

# -----------------------------
# Exemplo de uso (quando executado como script)
# -----------------------------
if __name__ == '__main__':
    # Demonstração rápida — não roda nada perigoso.
    logger = setup_logging('pmgr_demo', log_dir=Path('./logs'))
    logger.info('Logger inicializado para demonstração')

    # Criar diretórios
    ensure_dirs(Path('./logs'), Path('./cache'))

    # Ler um exemplo de metafile (se existir)
    example = Path('./example_metafile.toml')
    if example.exists():
        logger.info('Lendo example_metafile.toml')
        mf = MetaFile.from_path(example)
        ok, issues = mf.validate()
        if not ok:
            logger.error('Metafile inválido: %s', issues)
        else:
            logger.info('Metafile válido: %s %s', mf.name, mf.version)
            mf.expand_variables({'PREFIX': '/usr'})
            logger.info('Sources após expansão: %s', [s.url for s in mf.sources])
    else:
        logger.info('Nenhum example_metafile.toml encontrado — pronto para os próximos módulos')
