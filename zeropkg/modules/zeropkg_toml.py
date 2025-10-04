#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zeropkg TOML Parser
-------------------
Responsável por carregar, validar, normalizar e expandir receitas (.toml)
utilizadas em todos os estágios do Zeropkg — desde LFS até BLFS e beyond.

Suporta includes, overlays, múltiplos sources, hooks, environment, etc.
"""

import os
import tomllib
import hashlib
import json
import re
from pathlib import Path
from packaging import version
from zeropkg_logger import log


CACHE_DIR = Path("/var/cache/zeropkg")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "recipes.json"


class ZeropkgTOML:
    def __init__(self, config=None):
        self.config = config
        self.cache = self._load_cache()

    # ===============================================================
    # Cache de receitas normalizadas
    # ===============================================================
    def _load_cache(self):
        if CACHE_FILE.exists():
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self):
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            log(f"⚠️ Falha ao salvar cache de receitas: {e}")

    # ===============================================================
    # Carregar e normalizar uma receita TOML
    # ===============================================================
    def load(self, toml_path: str | Path):
        toml_path = Path(toml_path)
        if not toml_path.exists():
            raise FileNotFoundError(f"Receita TOML não encontrada: {toml_path}")

        mtime = os.path.getmtime(toml_path)
        cache_key = str(toml_path)
        if cache_key in self.cache and self.cache[cache_key].get("mtime") == mtime:
            return self.cache[cache_key]["data"]

        with open(toml_path, "rb") as f:
            data = tomllib.load(f)

        recipe = self._process_includes(data, toml_path.parent)
        recipe = self._apply_overlays(recipe)
        recipe = self._normalize(recipe)

        self.cache[cache_key] = {"mtime": mtime, "data": recipe}
        self._save_cache()
        return recipe

    # ===============================================================
    # Includes (permite receitas modulares)
    # ===============================================================
    def _process_includes(self, data, base_dir):
        includes = data.get("includes", [])
        for inc in includes:
            inc_path = Path(base_dir) / inc
            if inc_path.exists():
                with open(inc_path, "rb") as f:
                    inc_data = tomllib.load(f)
                data.update(inc_data)
        return data

    # ===============================================================
    # Overlay (permite camadas locais sobre receitas base)
    # ===============================================================
    def _apply_overlays(self, recipe):
        overlay_dir = Path("/etc/zeropkg/overlays")
        overlay_file = overlay_dir / f"{recipe.get('package', {}).get('name', '')}.toml"
        if overlay_file.exists():
            log(f"🧩 Aplicando overlay local: {overlay_file}")
            with open(overlay_file, "rb") as f:
                overlay_data = tomllib.load(f)
            recipe.update(overlay_data)
        return recipe

    # ===============================================================
    # Normalização de estrutura
    # ===============================================================
    def _normalize(self, recipe):
        pkg = recipe.get("package", {})
        recipe["package"] = {
            "name": pkg.get("name"),
            "version": pkg.get("version"),
            "description": pkg.get("description", ""),
        }

        if "sources" in recipe:
            recipe["sources"] = [self._normalize_source(s) for s in recipe["sources"]]

        if "patches" in recipe:
            recipe["patches"] = [self._normalize_patch(p) for p in recipe["patches"]]

        if "build" in recipe:
            recipe["build"]["commands"] = recipe["build"].get("commands", [])
            recipe["build"]["environment"] = recipe["build"].get("environment", {})

        if "hooks" in recipe:
            for stage, cmds in recipe["hooks"].items():
                if not isinstance(cmds, list):
                    recipe["hooks"][stage] = [cmds]

        if "dependencies" in recipe:
            recipe["dependencies"] = self._normalize_deps(recipe["dependencies"])

        self._validate(recipe)
        return recipe

    # ===============================================================
    # Normalização individual de seções
    # ===============================================================
    def _normalize_source(self, source):
        if isinstance(source, str):
            source = {"url": source}
        source.setdefault("method", self._infer_method(source.get("url", "")))
        source.setdefault("extract_to", None)
        return source

    def _normalize_patch(self, patch):
        if isinstance(patch, str):
            patch = {"url": patch}
        if "url" in patch and patch["url"].startswith(("http://", "https://")):
            patch.setdefault("sha256", None)
        return patch

    def _normalize_deps(self, deps):
        result = []
        for d in deps:
            if isinstance(d, str):
                name, _, ver = d.partition(">=")
                result.append({"name": name.strip(), "version": ver.strip() or None})
            elif isinstance(d, dict):
                result.append(d)
        return result

    def _infer_method(self, url):
        if url.endswith(".git"):
            return "git"
        elif any(url.endswith(ext) for ext in [".tar.gz", ".tar.xz", ".zip", ".bz2"]):
            return "archive"
        elif url.startswith("file://"):
            return "file"
        return "unknown"

    # ===============================================================
    # Validação completa da receita
    # ===============================================================
    def _validate(self, recipe):
        pkg = recipe.get("package", {})
        if not pkg.get("name") or not pkg.get("version"):
            raise ValueError("Campo obrigatório ausente: package.name ou package.version")

        if not re.match(r"^[a-zA-Z0-9._+-]+$", pkg["name"]):
            raise ValueError(f"Nome de pacote inválido: {pkg['name']}")

        try:
            version.Version(pkg["version"])
        except Exception:
            log(f"⚠️ Versão não semântica detectada em {pkg['name']}: {pkg['version']}")

        if "sources" in recipe:
            for s in recipe["sources"]:
                if "url" not in s:
                    raise ValueError(f"Fonte sem URL definida em {pkg['name']}")

        if "dependencies" in recipe:
            for dep in recipe["dependencies"]:
                if not dep.get("name"):
                    raise ValueError(f"Dependência sem nome em {pkg['name']}")

    # ===============================================================
    # Substituição de variáveis / macros
    # ===============================================================
    @staticmethod
    def resolve_macros(value, env=None):
        if not isinstance(value, str):
            return value
        env = env or os.environ
        for k, v in env.items():
            value = value.replace(f"${{{k}}}", v).replace(f"@{k}@", v)
        return value


# ===============================================================
# Função utilitária global (para compatibilidade com módulos antigos)
# ===============================================================
def resolve_macros(value, env=None):
    return ZeropkgTOML.resolve_macros(value, env)


# ===============================================================
# Execução direta
# ===============================================================
if __name__ == "__main__":
    parser = ZeropkgTOML()
    example = parser.load("/usr/ports/gcc/gcc-13.2.0.toml")
    print(json.dumps(example, indent=2))
