#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_toml.py — Parser / normalizer / validator de receitas TOML para Zeropkg

Melhorias aplicadas:
 - includes/overlays (merge profundo)
 - fingerprinting (conteúdo + includes + overlays) para cache
 - expansão de variáveis (${VAR}, @VAR@) via recipe.env, config globals e os.environ
 - validação extensiva e mensagens estruturadas
 - normalização de paths (com base em ports_root)
 - compatibilidade com builder: campos extract_to, build.commands, install, patches, hooks, environment
 - variants (variant.* tables) aplicáveis via overlay
 - export TOML → JSON/YAML via CLI
 - API pública:
     load_recipe(path, apply_variants=None, respect_cache=True)
     load_recipe_cached(path, force=False)
     validate_recipe(recipe)
     to_builder_spec(recipe)  -> retorna estrutura pronta para builder
     list_dependencies(recipe)
     detect_type(recipe)
"""

from __future__ import annotations
import os
import sys
import json
import hashlib
import shutil
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime

# tomllib on py3.11+, else try 'toml' package
try:
    import tomllib  # type: ignore
    def _toml_load(fp):
        return tomllib.load(fp)
    def _toml_loads(s: str):
        return tomllib.loads(s)
except Exception:
    try:
        import toml  # type: ignore
        def _toml_load(fp):
            return toml.load(fp)
        def _toml_loads(s: str):
            return toml.loads(s)
    except Exception:
        raise RuntimeError("neither tomllib nor toml available; instale 'toml' para suporte a TOML")

# optional yaml output
try:
    import yaml  # type: ignore
    _yaml_ok = True
except Exception:
    _yaml_ok = False

# safe imports of project modules
def _safe_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

cfg_mod = _safe_import("zeropkg_config")
logger_mod = _safe_import("zeropkg_logger")
db_mod = _safe_import("zeropkg_db")

# logger
if logger_mod and hasattr(logger_mod, "get_logger"):
    log = logger_mod.get_logger("toml")
    def log_event(a,b,c,**k): 
        try:
            return logger_mod.log_event(a,b,c,**k)
        except Exception:
            return None
else:
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("zeropkg.toml")
    def log_event(a,b,c,**k):
        logging.getLogger("zeropkg.toml").info(f"{a}:{b} - {c}")

# paths / config defaults
CONFIG = {}
try:
    if cfg_mod and hasattr(cfg_mod, "load_config"):
        CONFIG = cfg_mod.load_config()
except Exception:
    CONFIG = {}

PORTS_ROOTS = CONFIG.get("repos", {}).get("roots", []) or [CONFIG.get("paths", {}).get("ports_dir", "/usr/ports")]
CACHE_DIR = Path(CONFIG.get("paths", {}).get("cache_dir", "/var/cache/zeropkg")) / "toml_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_OVERLAYS_DIRS = CONFIG.get("overlays", []) or ["/etc/zeropkg/overlays"]
GLOBAL_ENV_FILES = CONFIG.get("globals", {}).get("env_files", []) if CONFIG.get("globals") else []

# Utilities ------------------------------------------------------------------
def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def _read_text(path: Path) -> str:
    with open(path, "rb") as f:
        data = f.read()
    try:
        return data.decode("utf-8")
    except Exception:
        return data.decode("latin-1", errors="ignore")

def _safe_write_atomic(path: Path, data: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(path)

def _hash_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

def _hash_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _deep_merge(a: Dict[str,Any], b: Dict[str,Any]) -> Dict[str,Any]:
    # merge b into a (a updated), lists replaced
    for k, v in b.items():
        if k in a and isinstance(a[k], dict) and isinstance(v, dict):
            a[k] = _deep_merge(a[k], v)
        else:
            a[k] = v
    return a

# TOML loading + includes/overlays ------------------------------------------
def _load_toml_file(path: Path) -> Dict[str,Any]:
    content = _read_text(path)
    return _toml_loads(content) if False else _toml_load(path.open("rb"))

def _resolve_includes(base_path: Path, loaded: Dict[str,Any]) -> Tuple[Dict[str,Any], List[Path]]:
    """
    Procura chave 'includes' no root da receita; inclui arquivos (relative a base_path.parent).
    Retorna (merged_recipe, list_of_included_paths)
    """
    includes = loaded.get("includes") or []
    included_paths: List[Path] = []
    merged = dict(loaded)  # shallow copy
    for inc in includes:
        try:
            inc_path = Path(inc)
            if not inc_path.is_absolute():
                inc_path = (base_path.parent / inc).resolve()
            if not inc_path.exists():
                log.warning("include file not found: %s", inc_path)
                continue
            inc_loaded = _load_toml_file(inc_path)
            merged = _deep_merge(merged, inc_loaded)
            included_paths.append(inc_path)
        except Exception as e:
            log.warning("failed include %s: %s", inc, e)
    return merged, included_paths

def _collect_overlays(recipe: Dict[str,Any], overlays_dirs: Optional[List[str]] = None) -> Tuple[Dict[str,Any], List[Path]]:
    """
    Se a receita declarar overlays (ou config global tiver overlays), mescla por ordem.
    """
    overlays_dirs = overlays_dirs or DEFAULT_OVERLAYS_DIRS
    merged = dict(recipe)
    used = []
    # recipe may have overlay names list
    overlay_names = recipe.get("overlays") or []
    for od in overlays_dirs:
        for name in overlay_names:
            p = Path(od) / name
            if p.exists():
                try:
                    loaded = _load_toml_file(p)
                    merged = _deep_merge(merged, loaded)
                    used.append(p)
                except Exception as e:
                    log.warning("failed overlay %s: %s", p, e)
    return merged, used

# Variable expansion ---------------------------------------------------------
import re
_VAR_RE = re.compile(r'\$\{([A-Za-z0-9_]+)\}|\@([A-Za-z0-9_]+)\@')

def _build_env_sources(recipe: Dict[str,Any]) -> Dict[str,str]:
    env = {}
    # recipe-provided environment
    for k, v in (recipe.get("environment") or {}).items():
        env[str(k)] = str(v)
    # config globals if provided
    cfg_globals = CONFIG.get("globals", {}).get("vars", {}) if CONFIG.get("globals") else {}
    for k, v in cfg_globals.items():
        if k not in env:
            env[k] = str(v)
    # OS env
    for k, v in os.environ.items():
        if k not in env:
            env[k] = v
    return env

def _expand_value(val: Any, env_sources: Dict[str,str]) -> Any:
    if isinstance(val, str):
        def _repl(m):
            name = m.group(1) or m.group(2)
            return env_sources.get(name, m.group(0))
        return _VAR_RE.sub(_repl, val)
    elif isinstance(val, dict):
        return {k: _expand_value(v, env_sources) for k,v in val.items()}
    elif isinstance(val, list):
        return [_expand_value(v, env_sources) for v in val]
    else:
        return val

def expand_recipe_env(recipe: Dict[str,Any]) -> Dict[str,Any]:
    env_sources = _build_env_sources(recipe)
    expanded = _expand_value(recipe, env_sources)
    return expanded

# Path normalization ---------------------------------------------------------
def _normalize_paths(recipe: Dict[str,Any], base_path: Optional[Path]=None) -> Dict[str,Any]:
    """
    Normaliza campos relacionados a arquivos/caminhos:
      - sources[*].url stays as URL
      - sources[*].path -> absolute resolved relative to base_path or ports roots
      - patches[*].file -> resolved
      - build.directory -> absolute if relative
      - extract_to -> absolute (resolve relative)
    """
    rp = dict(recipe)
    base = Path(base_path) if base_path else None
    def _resolve_candidate(pth):
        if not pth:
            return pth
        p = Path(pth)
        if p.is_absolute():
            return str(p)
        # if base provided, resolve relative to base.parent
        if base:
            cand = (base.parent / pth).resolve()
            return str(cand)
        # else try ports roots
        for root in PORTS_ROOTS:
            cand = Path(root) / pth
            if cand.exists():
                return str(cand.resolve())
        # fallback to making absolute relative to cwd
        return str(Path.cwd().joinpath(pth).resolve())

    # sources
    srcs = rp.get("sources") or []
    new_srcs = []
    for s in srcs:
        ns = dict(s)
        if "path" in s and s["path"]:
            ns["path"] = _resolve_candidate(s["path"])
        # keep url as-is
        new_srcs.append(ns)
    rp["sources"] = new_srcs

    # patches
    patches = rp.get("patches") or []
    new_patches = []
    for p in patches:
        np = dict(p)
        if isinstance(p, str):
            np = {"file": p}
        if "file" in np and np["file"]:
            np["file"] = _resolve_candidate(np["file"])
        new_patches.append(np)
    rp["patches"] = new_patches

    # build.directory
    b = rp.get("build") or {}
    if "directory" in b and b["directory"]:
        b["directory"] = _resolve_candidate(b["directory"])
    rp["build"] = b

    # extract_to
    if "extract_to" in rp and rp["extract_to"]:
        rp["extract_to"] = _resolve_candidate(rp["extract_to"])

    return rp

# Fingerprint & cache --------------------------------------------------------
def _compute_fingerprint(toml_path: Path, recipe_obj: Dict[str,Any], included_paths: List[Path], overlays_used: List[Path]) -> str:
    h = hashlib.sha1()
    h.update(_read_text(toml_path).encode("utf-8"))
    for p in included_paths:
        try:
            h.update(_read_text(p).encode("utf-8"))
        except Exception:
            continue
    for p in overlays_used:
        try:
            h.update(_read_text(p).encode("utf-8"))
        except Exception:
            continue
    # also incorporate normalized keys that might represent build commands or sources
    # (ensure deterministic)
    try:
        payload = json.dumps(recipe_obj, sort_keys=True, default=str)
        h.update(payload.encode("utf-8"))
    except Exception:
        pass
    return h.hexdigest()

def _cache_path_for_fp(fp: str) -> Path:
    return CACHE_DIR / f"{fp}.json"

def load_recipe(path: Union[str, Path], apply_variants: Optional[List[str]] = None, respect_cache: bool = True) -> Dict[str,Any]:
    """
    Carrega e processa uma receita TOML:
      - resolve includes
      - apply overlays
      - apply variants (variant.<name> tables)
      - expand environment variables
      - normalize paths
      - compute fingerprint (used for cache)
    Retorna dicionário pronto para o builder (normalizado).
    """
    toml_path = Path(path)
    if not toml_path.exists():
        raise FileNotFoundError(str(toml_path))
    # raw load
    raw = _load_toml_file(toml_path)
    # includes
    merged_includes, included_paths = _resolve_includes(toml_path, raw)
    # overlays
    merged_overlays, overlays_used = _collect_overlays(merged_includes)
    # variants
    if apply_variants:
        for v in apply_variants:
            variants = merged_overlays.get("variant") or merged_overlays.get("variants") or {}
            # variant table may be variant.<name>
            vdata = variants.get(v) if isinstance(variants, dict) else None
            if vdata:
                merged_overlays = _deep_merge(merged_overlays, vdata)
    # env expansion
    expanded = expand_recipe_env(merged_overlays)
    # normalize paths
    normalized = _normalize_paths(expanded, base_path=toml_path)
    # compute fingerprint
    fp = _compute_fingerprint(toml_path, normalized, included_paths, overlays_used)
    # cache
    cache_p = _cache_path_for_fp(fp)
    if respect_cache and cache_p.exists():
        try:
            cached = _read_text(cache_p)
            return json.loads(cached)
        except Exception:
            # fallback to rebuild
            pass
    # add metadata
    normalized["_meta"] = {
        "path": str(toml_path),
        "fingerprint": fp,
        "includes": [str(p) for p in included_paths],
        "overlays": [str(p) for p in overlays_used],
        "loaded_at": _now_iso()
    }
    # minimal normalization for builder: ensure build.commands list exists
    b = normalized.get("build") or {}
    if "commands" not in b and "script" in b:
        b["commands"] = b.get("script")
    if "commands" not in b:
        b["commands"] = []
    normalized["build"] = b

    # validate a bit and fill defaults
    validated = validate_recipe(normalized, fill_defaults=True)
    # write cache
    try:
        _safe_write_atomic(cache_p, json.dumps(validated, indent=2, ensure_ascii=False))
    except Exception as e:
        log.warning("failed write recipe cache: %s", e)
    return validated

def load_recipe_cached(path: Union[str, Path], force: bool = False, apply_variants: Optional[List[str]] = None) -> Dict[str,Any]:
    """
    Carrega a receita e respeita cache fingerprint unless force=True.
    """
    r = load_recipe(path, apply_variants=apply_variants, respect_cache=(not force))
    return r

# Validation -----------------------------------------------------------------
def _warn(msg: str):
    log_event("toml", "warn", msg, level="warning")
    log.warning(msg)

def validate_recipe(recipe: Dict[str,Any], fill_defaults: bool = False) -> Dict[str,Any]:
    """
    Validação leve:
     - checa presença de keys importantes
     - normaliza tipos
     - se fill_defaults=True adiciona campos mínimos
    Retorna recipe (possivelmente modificada).
    """
    r = dict(recipe)
    errors = []
    warnings = []
    name = r.get("package", {}).get("name") or r.get("name") or None
    if not name:
        errors.append("missing-package-name")
    version = r.get("package", {}).get("version") or r.get("version") or None
    if not version:
        warnings.append("missing-package-version")

    # sources must be list
    if "sources" in r and not isinstance(r["sources"], list):
        warnings.append("sources-not-list, converting")
        r["sources"] = [r["sources"]]

    # patches normalization
    if "patches" in r and not isinstance(r["patches"], list):
        r["patches"] = [r["patches"]]

    # ensure build.commands is list of strings
    b = r.get("build") or {}
    if "commands" in b and isinstance(b["commands"], str):
        b["commands"] = [b["commands"]]
    elif "commands" not in b:
        if fill_defaults:
            b["commands"] = []
    r["build"] = b

    # environment should be dict
    if "environment" in r and not isinstance(r["environment"], dict):
        warnings.append("environment-not-dict, converting")
        r["environment"] = dict(r["environment"])

    # hooks should be dict of lists/strings
    hooks = r.get("hooks") or {}
    if hooks and not isinstance(hooks, dict):
        warnings.append("hooks-not-dict, clearing")
        hooks = {}
    else:
        for k,v in list(hooks.items()):
            if isinstance(v, str):
                hooks[k] = [v]
            elif isinstance(v, list):
                hooks[k] = v
            else:
                hooks[k] = [str(v)]
    r["hooks"] = hooks

    # variants normalization
    variants = r.get("variant") or r.get("variants") or {}
    if not isinstance(variants, dict):
        variants = {}
    r["variants"] = variants

    # install stanzas
    inst = r.get("install") or {}
    if inst and not isinstance(inst, dict):
        r["install"] = {"commands": inst} if isinstance(inst, list) else {"commands": [str(inst)]}
    else:
        if "commands" in inst and isinstance(inst["commands"], str):
            inst["commands"] = [inst["commands"]]
        r["install"] = inst

    # metadata warnings
    if errors:
        raise ValueError(f"recipe validation failed: {errors}")
    for w in warnings:
        _warn(w)
    # attach validation summary
    r["_validation"] = {"warnings": warnings, "checked_at": _now_iso()}
    return r

# Helper: detect type --------------------------------------------------------
def detect_type(recipe: Dict[str,Any]) -> str:
    """
    Heurística para tipo: 'toolchain','package','metapackage'
    """
    name = (recipe.get("package") or {}).get("name") or recipe.get("name") or ""
    # heuristic: toolchain packages often called gcc, binutils, linux-headers, glibc
    lower = name.lower()
    if any(k in lower for k in ("gcc","binutils","glibc","linux-headers","linux-headers","toolchain")):
        return "toolchain"
    # metapackage: no sources and only dependencies
    if (not recipe.get("sources")) and recipe.get("package", {}).get("meta", False):
        return "metapackage"
    return "package"

# Convert recipe to builder format -----------------------------------------
def to_builder_spec(recipe: Dict[str,Any]) -> Dict[str,Any]:
    """
    Retorna um dict com chaves que o builder espera:
      - name, version, sources (list of dict {url,path,sha}), patches, build (commands, directory),
        extract_to, environment, hooks, install (commands), variants, meta
    """
    spec = {}
    pkg = recipe.get("package") or {}
    spec["name"] = pkg.get("name") or recipe.get("name")
    spec["version"] = pkg.get("version") or recipe.get("version")
    spec["type"] = detect_type(recipe)
    # sources normalization
    srcs = []
    for s in recipe.get("sources") or []:
        if isinstance(s, str):
            srcs.append({"url": s})
        else:
            srcs.append(dict(s))
    spec["sources"] = srcs
    # patches
    patch_list = []
    for p in recipe.get("patches") or []:
        if isinstance(p, str):
            patch_list.append({"file": p})
        else:
            patch_list.append(dict(p))
    spec["patches"] = patch_list
    # build
    b = recipe.get("build") or {}
    spec["build"] = {"commands": b.get("commands", []), "directory": b.get("directory"), "env": recipe.get("environment", {})}
    # extract_to
    spec["extract_to"] = recipe.get("extract_to")
    # install
    inst = recipe.get("install") or {}
    spec["install"] = {"commands": inst.get("commands", []), "dir_install": inst.get("dir_install", False)}
    # hooks
    spec["hooks"] = recipe.get("hooks", {})
    # variants
    spec["variants"] = recipe.get("variants", {})
    # original metadata
    spec["_meta"] = recipe.get("_meta", {})
    return spec

# Dependency helpers --------------------------------------------------------
def list_dependencies(recipe: Dict[str,Any]) -> List[str]:
    """
    Retorna lista simples de dependências a partir de recipe.package.dependencies ou recipe.dependencies
    """
    deps = recipe.get("package", {}).get("dependencies") or recipe.get("dependencies") or []
    if isinstance(deps, dict):
        # support {build=[...], runtime=[...]}
        out = []
        for k,v in deps.items():
            if isinstance(v, list):
                out.extend(v)
            elif isinstance(v, str):
                out.append(v)
        return out
    elif isinstance(deps, list):
        return deps
    elif isinstance(deps, str):
        return [deps]
    return []

# CLI utilities (convert, validate) ----------------------------------------
def _convert_and_print(recipe: Dict[str,Any], fmt: str = "json"):
    if fmt == "json":
        print(json.dumps(recipe, indent=2, ensure_ascii=False))
    elif fmt == "yaml":
        if not _yaml_ok:
            raise RuntimeError("yaml requested but PyYAML not installed")
        print(yaml.safe_dump(recipe, sort_keys=False))
    elif fmt == "toml":
        # best-effort: convert back to toml via toml lib if present
        try:
            import toml  # type: ignore
            print(toml.dumps(recipe))
        except Exception:
            print(json.dumps(recipe, indent=2))
    else:
        print(json.dumps(recipe, indent=2))

def _cli():
    import argparse
    p = argparse.ArgumentParser(prog="zeropkg-toml", description="Zeropkg recipe TOML utilities")
    p.add_argument("path", help="recipe toml path")
    p.add_argument("--variant", "-v", action="append", help="Apply variant(s)")
    p.add_argument("--force-cache", action="store_true", help="Ignore cache and refresh")
    p.add_argument("--convert", choices=["json","yaml","toml"], default="json", help="Output format")
    p.add_argument("--show-deps", action="store_true")
    args = p.parse_args()
    r = load_recipe(args.path, apply_variants=args.variant or [], respect_cache=(not args.force_cache))
    if args.show_deps:
        print(json.dumps(list_dependencies(r), indent=2, ensure_ascii=False))
    _convert_and_print(r, fmt=args.convert)

# Exports
__all__ = ["load_recipe", "load_recipe_cached", "validate_recipe", "expand_recipe_env", "to_builder_spec", "list_dependencies", "detect_type"]

if __name__ == "__main__":
    _cli()
