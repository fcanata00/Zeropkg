#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_toml.py
Parser/normalizador de receitas TOML para Zeropkg.

- Usa tomllib (Python 3.11+) quando disponível, senão tenta biblioteca 'toml'.
- Expõe:
    - load_recipe(path) -> dict  (normalize/validate)
    - parse_toml(bytes_or_path) -> dict
    - to_builder_spec(raw_dict) -> dict (formato esperado pelo builder)
    - helper classes: PackageMeta, SourceEntry, PatchEntry, HookEntry (dataclass quando possível)
"""

from __future__ import annotations
import os
import sys
import typing as _t
from pathlib import Path

# toml loader: prefer tomllib (py3.11+), fallback to toml package
try:
    import tomllib as _toml_lib  # type: ignore
    def _load_toml_bytes(b: bytes) -> dict:
        return _toml_lib.loads(b.decode("utf-8")) if isinstance(b, (bytes,bytearray)) else _toml_lib.loads(b)
except Exception:
    try:
        import toml as _toml_lib
        def _load_toml_bytes(b: bytes) -> dict:
            return _toml_lib.loads(b.decode("utf-8")) if isinstance(b, (bytes,bytearray)) else _toml_lib.loads(b)
    except Exception:
        raise RuntimeError("Nenhum parser TOML disponível: instale a biblioteca 'toml' ou use Python 3.11+ (tomllib).")

# optional dataclass
try:
    from dataclasses import dataclass, asdict  # type: ignore
    _HAS_DATACLASSES = True
except Exception:
    _HAS_DATACLASSES = False
    # provide a simple asdict fallback
    def asdict(obj):
        if hasattr(obj, "__dict__"):
            return dict(obj.__dict__)
        return obj

# optional logger (non obrigatorio)
try:
    from zeropkg_logger import log_event  # type: ignore
    def _log(evt: str, msg: str, level: str = "info", metadata: _t.Optional[dict] = None):
        try:
            log_event(evt, msg, level=level, metadata=metadata)
        except Exception:
            pass
except Exception:
    def _log(evt: str, msg: str, level: str = "info", metadata: _t.Optional[dict] = None):
        # fallback silencioso para não poluir em import
        return

# -------------------------
# Data structures
# -------------------------
if _HAS_DATACLASSES:
    @dataclass
    class SourceEntry:
        url: str
        filename: _t.Optional[str] = None
        checksum: _t.Optional[str] = None
        mirrors: _t.Optional[_t.List[str]] = None
        type: _t.Optional[str] = None  # tar.gz, tar.xz, git, etc
        extract_to: _t.Optional[str] = None

    @dataclass
    class PatchEntry:
        path: str
        stage: str = "pre_configure"  # pre_configure, post_configure, pre_install, post_install etc
        strip: int = 1
        apply_as: str = "patch"  # or 'patch -p1' semantics

    @dataclass
    class HookEntry:
        name: str
        cmd: str
        stage: str = "pre_install"  # pre_install, post_install, pre_remove, post_remove

    @dataclass
    class PackageMeta:
        name: str
        version: str
        summary: _t.Optional[str] = None
        description: _t.Optional[str] = None
        sources: _t.List[SourceEntry] = None
        patches: _t.List[PatchEntry] = None
        hooks: _t.List[HookEntry] = None
        dependencies: _t.List[str] = None
        build: _t.Dict[str, _t.Any] = None
        install: _t.Dict[str, _t.Any] = None
        environment: _t.Dict[str, str] = None
else:
    # fallback simple classes
    class SourceEntry:
        def __init__(self, url, filename=None, checksum=None, mirrors=None, type=None, extract_to=None):
            self.url = url
            self.filename = filename
            self.checksum = checksum
            self.mirrors = mirrors or []
            self.type = type
            self.extract_to = extract_to

    class PatchEntry:
        def __init__(self, path, stage="pre_configure", strip=1, apply_as="patch"):
            self.path = path
            self.stage = stage
            self.strip = strip
            self.apply_as = apply_as

    class HookEntry:
        def __init__(self, name, cmd, stage="pre_install"):
            self.name = name
            self.cmd = cmd
            self.stage = stage

    class PackageMeta:
        def __init__(self, name, version, summary=None, description=None, sources=None, patches=None, hooks=None, dependencies=None, build=None, install=None, environment=None):
            self.name = name
            self.version = version
            self.summary = summary
            self.description = description
            self.sources = sources or []
            self.patches = patches or []
            self.hooks = hooks or []
            self.dependencies = dependencies or []
            self.build = build or {}
            self.install = install or {}
            self.environment = environment or {}

# -------------------------
# Parsing / Normalization
# -------------------------
def _ensure_list(v: _t.Any) -> _t.List:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

def _normalize_source(src_raw: _t.Mapping) -> SourceEntry:
    # Accept both simple string and dict forms
    if isinstance(src_raw, str):
        return SourceEntry(url=src_raw)
    if not isinstance(src_raw, dict):
        raise ValueError("source entry must be string or table/dict")
    return SourceEntry(
        url=src_raw.get("url") or src_raw.get("path"),
        filename=src_raw.get("filename"),
        checksum=src_raw.get("checksum") or src_raw.get("hash"),
        mirrors=_ensure_list(src_raw.get("mirrors")),
        type=src_raw.get("type"),
        extract_to=src_raw.get("extract_to")
    )

def _normalize_patch(p_raw: _t.Union[str, _t.Mapping]) -> PatchEntry:
    if isinstance(p_raw, str):
        return PatchEntry(path=p_raw)
    if not isinstance(p_raw, dict):
        raise ValueError("patch entry must be string or table/dict")
    return PatchEntry(
        path=p_raw.get("path"),
        stage=p_raw.get("stage", "pre_configure"),
        strip=int(p_raw.get("strip", 1)),
        apply_as=p_raw.get("apply_as", "patch")
    )

def _normalize_hook(h_raw: _t.Union[str, _t.Mapping]) -> HookEntry:
    if isinstance(h_raw, str):
        # shorthand: "cmd"
        return HookEntry(name="inline", cmd=h_raw, stage="pre_install")
    if not isinstance(h_raw, dict):
        raise ValueError("hook entry must be string or table/dict")
    return HookEntry(
        name=h_raw.get("name", "hook"),
        cmd=h_raw.get("cmd") or h_raw.get("command"),
        stage=h_raw.get("stage", "pre_install")
    )

# ---------------
# Public functions
# ---------------
def parse_toml_input(src: _t.Union[bytes, str, Path]) -> dict:
    """
    Carrega TOML a partir de bytes, string ou caminho de arquivo e retorna dict cru.
    """
    if isinstance(src, (bytes, bytearray)):
        raw = _load_toml_bytes(src)
        return raw
    if isinstance(src, Path) or (isinstance(src, str) and os.path.exists(src)):
        p = Path(src)
        b = p.read_bytes()
        raw = _load_toml_bytes(b)
        return raw
    if isinstance(src, str):
        # treat as toml text
        raw = _load_toml_bytes(src.encode("utf-8"))
        return raw
    raise ValueError("parse_toml_input aceita bytes, string (conteúdo) ou caminho para arquivo")

def load_recipe(path: _t.Union[str, Path]) -> dict:
    """
    Lê e normaliza uma receita TOML para um dict canônico.
    Retorna um dicionário com chaves simples, e mantém campos originais em raw.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"recipe not found: {p}")
    raw = parse_toml_input(p)
    # normalize
    meta = {}
    pkg = raw.get("package") or raw.get("pkg") or {}
    meta["name"] = pkg.get("name") or raw.get("name") or p.stem
    meta["version"] = pkg.get("version") or pkg.get("ver") or raw.get("version") or "0.0"
    meta["summary"] = pkg.get("summary") or raw.get("summary")
    meta["description"] = pkg.get("description") or raw.get("description")
    # sources
    srcs = raw.get("source") or raw.get("sources") or raw.get("distfiles") or []
    meta["sources"] = [_normalize_source(s) for s in _ensure_list(srcs)]
    # patches
    patches = raw.get("patches") or []
    meta["patches"] = [_normalize_patch(p) for p in _ensure_list(patches)]
    # hooks
    hooks = raw.get("hooks") or []
    meta["hooks"] = [_normalize_hook(h) for h in _ensure_list(hooks)]
    # deps
    meta["dependencies"] = _ensure_list(raw.get("dependencies") or raw.get("depends") or raw.get("requires"))
    # build/install/env
    meta["build"] = raw.get("build", {}) or {}
    meta["install"] = raw.get("install", {}) or {}
    meta["environment"] = raw.get("environment", {}) or raw.get("env", {}) or {}
    # keep raw
    meta["_raw"] = raw
    return meta

def to_builder_spec(raw_recipe: _t.Union[dict, Path, str]) -> dict:
    """
    Converte o dicionário retornado por load_recipe (ou TOML cru) para o dicionário
    esperado pelo Zeropkg Builder (chaves: name, version, sources:list of dicts,
    patches:list, build: {commands, directory, env}, install:{commands}, dependencies:list).
    """
    if isinstance(raw_recipe, (str, Path)):
        meta = load_recipe(raw_recipe)
    elif isinstance(raw_recipe, dict) and "_raw" not in raw_recipe:
        # maybe raw toml dict
        # attempt to normalize similar shape by creating a temporary file? Simpler: use parse flow via parse_toml_input expectation
        # but we'll try to map common keys
        tmp = {}
        tmp["name"] = raw_recipe.get("package", {}).get("name") if raw_recipe.get("package") else raw_recipe.get("name")
        tmp["version"] = raw_recipe.get("package", {}).get("version") if raw_recipe.get("package") else raw_recipe.get("version")
        tmp["sources"] = raw_recipe.get("source") or raw_recipe.get("sources") or raw_recipe.get("distfiles") or []
        tmp["patches"] = raw_recipe.get("patches", [])
        tmp["build"] = raw_recipe.get("build", {})
        tmp["install"] = raw_recipe.get("install", {})
        tmp["dependencies"] = raw_recipe.get("dependencies", [])
        meta = tmp
    else:
        meta = raw_recipe

    # Now create builder-spec
    spec = {}
    spec["name"] = meta.get("name") or meta.get("package", {}).get("name") if isinstance(meta.get("package", {}), dict) else meta.get("name")
    spec["version"] = meta.get("version") or "0.0"
    # sources as list of dicts
    spec["sources"] = []
    for s in meta.get("sources") or []:
        if isinstance(s, SourceEntry):
            spec["sources"].append(asdict(s) if _HAS_DATACLASSES else s.__dict__)
        elif isinstance(s, dict):
            spec["sources"].append(s)
        else:
            # could be string URL
            spec["sources"].append({"url": s})
    # patches
    spec["patches"] = []
    for p in meta.get("patches") or []:
        if isinstance(p, PatchEntry):
            spec["patches"].append(asdict(p) if _HAS_DATACLASSES else p.__dict__)
        elif isinstance(p, dict):
            spec["patches"].append(p)
        else:
            spec["patches"].append({"path": p})
    # hooks
    spec["hooks"] = []
    for h in meta.get("hooks") or []:
        if isinstance(h, HookEntry):
            spec["hooks"].append(asdict(h) if _HAS_DATACLASSES else h.__dict__)
        elif isinstance(h, dict):
            spec["hooks"].append(h)
        else:
            spec["hooks"].append({"cmd": h})
    # build/install/env
    spec["build"] = meta.get("build") or {}
    spec["install"] = meta.get("install") or {}
    spec["environment"] = meta.get("environment") or {}
    spec["dependencies"] = meta.get("dependencies") or []
    # convenience: top-level raw
    spec["_raw"] = meta.get("_raw") if isinstance(meta, dict) and meta.get("_raw") else meta
    return spec

# small validator
def validate_recipe_dict(spec: dict) -> None:
    """
    Lança ValueError se campos obrigatórios estiverem faltando ou inconsistentes.
    """
    if not spec.get("name"):
        raise ValueError("recipe missing 'name'")
    if not spec.get("version"):
        raise ValueError("recipe missing 'version'")
    # sources can be empty (local builds) but warn
    if not spec.get("sources"):
        _log("toml", f"recipe {spec.get('name')} has no sources", level="info")
    return

# helper: convenience loader alias
def load_toml(path: _t.Union[str, Path]) -> dict:
    return load_recipe(path)

# -------------------------
# Exported API
# -------------------------
__all__ = [
    "load_recipe",
    "parse_toml_input",
    "to_builder_spec",
    "load_toml",
    "validate_recipe_dict",
    "SourceEntry",
    "PatchEntry",
    "HookEntry",
    "PackageMeta",
]

# -------------------------
# Quick self-test when invoked directly
# -------------------------
if __name__ == "__main__":  # pragma: no cover - quick manual test
    # cria exemplo rapido em memória e valida
    example = r'''
[package]
name = "hello"
version = "1.0.0"
summary = "Hello example"

[[source]]
url = "https://example.org/hello-1.0.tar.gz"
filename = "hello-1.0.tar.gz"

[[patches]]
path = "fix-issue.patch"
stage = "pre_configure"

[build]
commands = ["./configure --prefix=/usr", "make -j4"]

[install]
commands = ["make DESTDIR=/tmp/staging install"]
'''
    print("Parsing example TOML...")
    raw = parse_toml_input(example)
    print("raw keys:", list(raw.keys()))
    spec = load_recipe(Path("example.toml")) if False else (to_builder_spec(parse_toml_input(example)))
    print("builder spec keys:", list(spec.keys()))
    print("name, version:", spec.get("name"), spec.get("version"))
