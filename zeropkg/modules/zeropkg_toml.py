#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_toml.py — Parser e validador de receitas TOML para o Zeropkg

Suporte:
- Includes e overlays
- Variantes de build (static, minimal, debug)
- Macros de ambiente
- Cache incremental baseado em hash
- Validação de hooks e patches
- Compatibilidade com YAML (fallback)
"""

import os
import re
import tomllib
import hashlib
import json
import yaml
from pathlib import Path
from typing import Any, Dict, Optional, List, Union
from copy import deepcopy

# --------------------------------------------------------
# Imports internos (com fallback para execução isolada)
# --------------------------------------------------------
try:
    from zeropkg_logger import log_event, log_global
except Exception:
    def log_event(pkg, stage, msg, level="info"):
        print(f"[{level}] {pkg}:{stage} - {msg}")
    def log_global(msg, level="info"):
        print(f"[{level}] {msg}")

try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        return {"paths": {"cache_dir": "/var/cache/zeropkg"}}


# ========================================================
# Funções auxiliares
# ========================================================
def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p


def _hash_file(path: Path) -> str:
    """Calcula o hash SHA1 de um arquivo."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _expand_macros(data: Any, env: Dict[str, str]) -> Any:
    """Expande macros ${VAR} e @VAR@ recursivamente em dicionários, listas e strings."""
    if isinstance(data, dict):
        return {k: _expand_macros(v, env) for k, v in data.items()}
    elif isinstance(data, list):
        return [_expand_macros(i, env) for i in data]
    elif isinstance(data, str):
        for k, v in env.items():
            data = data.replace(f"${{{k}}}", v).replace(f"@{k}@", v)
        return data
    else:
        return data


def _load_yaml(path: Path) -> Dict[str, Any]:
    """Fallback para YAML se o TOML não existir."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _validate_hook(path: str) -> bool:
    """Confirma se o hook existe e é executável."""
    if not path:
        return False
    hook_path = Path(path)
    return hook_path.exists() and os.access(hook_path, os.X_OK)


# ========================================================
# Classe principal
# ========================================================
class ZeropkgTOML:
    def __init__(self, cache_dir: Optional[Path] = None):
        cfg = load_config()
        self.cache_dir = _ensure_dir(Path(cfg["paths"]["cache_dir"]) / "recipes")
        self._cache_path = self.cache_dir / "recipes_cache.json"
        self._cache = self._load_cache()

    # ----------------------------------------------------
    # Cache incremental baseado em hash
    # ----------------------------------------------------
    def _load_cache(self) -> Dict[str, str]:
        if self._cache_path.exists():
            try:
                with open(self._cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self):
        with open(self._cache_path, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, indent=2)

    # ----------------------------------------------------
    # Carregamento de receita
    # ----------------------------------------------------
    def load(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Carrega uma receita TOML (ou YAML fallback) com cache e overlays."""
        path = Path(path)
        if not path.exists():
            alt = path.with_suffix(".yaml")
            if alt.exists():
                path = alt
            else:
                raise FileNotFoundError(f"Recipe not found: {path}")

        file_hash = _hash_file(path)
        if self._cache.get(str(path)) == file_hash:
            log_event(path.name, "toml", "Cache hit")
            with open(self.cache_dir / f"{path.stem}.json", "r", encoding="utf-8") as f:
                return json.load(f)

        log_event(path.name, "toml", "Parsing recipe...")
        recipe = self._parse_file(path)
        recipe = self._apply_includes(recipe, path.parent)
        recipe = self._apply_overlay(recipe)
        recipe = self._normalize(recipe)
        recipe = self._expand_env(recipe)

        # Salvar cache
        with open(self.cache_dir / f"{path.stem}.json", "w", encoding="utf-8") as f:
            json.dump(recipe, f, indent=2)
        self._cache[str(path)] = file_hash
        self._save_cache()

        return recipe

    # ----------------------------------------------------
    # Parsing
    # ----------------------------------------------------
    def _parse_file(self, path: Path) -> Dict[str, Any]:
        try:
            if path.suffix in (".yaml", ".yml"):
                return _load_yaml(path)
            with open(path, "rb") as f:
                return tomllib.load(f)
        except Exception as e:
            log_global(f"Error parsing {path}: {e}", "error")
            raise

    # ----------------------------------------------------
    # Includes
    # ----------------------------------------------------
    def _apply_includes(self, recipe: Dict[str, Any], base_dir: Path) -> Dict[str, Any]:
        includes = recipe.get("includes", [])
        for inc in includes:
            inc_path = (base_dir / inc).resolve()
            if inc_path.exists():
                sub = self._parse_file(inc_path)
                recipe = self._merge(recipe, sub)
        return recipe

    # ----------------------------------------------------
    # Overlay
    # ----------------------------------------------------
    def _apply_overlay(self, recipe: Dict[str, Any]) -> Dict[str, Any]:
        pkg_name = recipe.get("package", {}).get("name")
        overlay_dir = Path("/etc/zeropkg/overlays")
        overlay_file = overlay_dir / f"{pkg_name}.toml"
        if overlay_file.exists():
            log_event(pkg_name, "toml", "Applying overlay")
            overlay = self._parse_file(overlay_file)
            recipe = self._merge(recipe, overlay)
        return recipe

    # ----------------------------------------------------
    # Normalização e Validação
    # ----------------------------------------------------
    def _normalize(self, recipe: Dict[str, Any]) -> Dict[str, Any]:
        defaults = {
            "package": {"name": "unknown", "version": "0.0", "meta": {}},
            "sources": [],
            "patches": [],
            "build": {"commands": [], "environment": {}, "variant": "default"},
            "dependencies": [],
            "hooks": {"pre_build": "", "post_build": "", "pre_install": "", "post_install": ""},
        }

        merged = deepcopy(defaults)
        merged = self._merge(merged, recipe)

        # Hooks
        for hook_name, hook_path in merged["hooks"].items():
            if hook_path and not _validate_hook(hook_path):
                log_event(merged["package"]["name"], "validate", f"Invalid hook: {hook_path}", "warning")

        # Variantes
        variant = merged["build"].get("variant", "default")
        if variant not in ["default", "minimal", "static", "debug"]:
            log_event(merged["package"]["name"], "validate", f"Unknown variant '{variant}'", "warning")

        # Patches
        for patch in merged["patches"]:
            if isinstance(patch, dict) and "url" in patch:
                patch_path = Path(patch.get("file", ""))
                if patch_path.exists() and "checksum" in patch:
                    chksum = hashlib.sha256(patch_path.read_bytes()).hexdigest()
                    if chksum != patch["checksum"]:
                        log_event(merged["package"]["name"], "patch", f"Checksum mismatch: {patch_path}", "error")

        return merged

    # ----------------------------------------------------
    # Expansão de ambiente
    # ----------------------------------------------------
    def _expand_env(self, recipe: Dict[str, Any]) -> Dict[str, Any]:
        env = os.environ.copy()
        env.update(recipe.get("build", {}).get("environment", {}))
        env.update({
            "LFS": env.get("LFS", "/mnt/lfs"),
            "SRC_DIR": "/usr/ports/distfiles",
            "BUILD_DIR": "/usr/ports/build",
            "PKG_NAME": recipe["package"]["name"],
            "PKG_VERSION": recipe["package"]["version"],
        })
        return _expand_macros(recipe, env)

    # ----------------------------------------------------
    # Merge utilitário
    # ----------------------------------------------------
    def _merge(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        merged = deepcopy(base)
        for k, v in overlay.items():
            if isinstance(v, dict) and k in merged:
                merged[k] = self._merge(merged[k], v)
            else:
                merged[k] = deepcopy(v)
        return merged


# ========================================================
# API pública
# ========================================================
_toml_loader = ZeropkgTOML()

def load_recipe(path: Union[str, Path]) -> Dict[str, Any]:
    return _toml_loader.load(path)


# CLI simples para debug
if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Usage: zeropkg_toml.py <recipe.toml>")
        sys.exit(1)
    data = load_recipe(sys.argv[1])
    print(json.dumps(data, indent=2))
