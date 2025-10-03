#!/usr/bin/env python3
# zeropkg_toml.py — Parser TOML compatível com o Builder do Zeropkg
# -*- coding: utf-8 -*-

from __future__ import annotations
import tomllib
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
import os

# -------------------------------
# Estruturas de dados principais
# -------------------------------

@dataclass
class SourceEntry:
    url: str
    checksum: Optional[str] = None
    algo: Optional[str] = "sha256"
    type: Optional[str] = "file"
    priority: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "checksum": self.checksum,
            "algo": self.algo,
            "type": self.type,
            "priority": self.priority,
        }


@dataclass
class PatchEntry:
    path: str
    stage: Optional[str] = None
    strip: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "stage": self.stage,
            "strip": self.strip,
        }


@dataclass
class PackageMeta:
    name: str
    version: str
    variant: Optional[str] = None
    sources: List[SourceEntry] = field(default_factory=list)
    patches: List[PatchEntry] = field(default_factory=list)
    environment: Dict[str, str] = field(default_factory=dict)
    hooks: Dict[str, List[str]] = field(default_factory=dict)
    build: Dict[str, Any] = field(default_factory=dict)
    package: Dict[str, Any] = field(default_factory=dict)
    dependencies: Dict[str, Any] = field(default_factory=dict)
    options: Dict[str, Any] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Converte o objeto completo para um dicionário plano compatível com o Builder."""
        return {
            "package": {
                "name": self.name,
                "version": self.version,
                "variant": self.variant,
            },
            "sources": [s.to_dict() for s in self.sources],
            "patches": [p.to_dict() for p in self.patches],
            "environment": self.environment,
            "hooks": self.hooks,
            "build": self.build,
            "package_extra": self.package,
            "dependencies": self.dependencies,
            "options": self.options,
        }


# -------------------------------
# Funções auxiliares
# -------------------------------

def _parse_sources(raw: Any) -> List[SourceEntry]:
    sources: List[SourceEntry] = []
    if not raw:
        return sources
    if isinstance(raw, list):
        for s in raw:
            if isinstance(s, str):
                sources.append(SourceEntry(url=s))
            elif isinstance(s, dict):
                sources.append(SourceEntry(**s))
    elif isinstance(raw, dict):
        for url, info in raw.items():
            if isinstance(info, dict):
                sources.append(SourceEntry(url=url, **info))
            else:
                sources.append(SourceEntry(url=url))
    return sources


def _parse_patches(raw: Any) -> List[PatchEntry]:
    patches: List[PatchEntry] = []
    if not raw:
        return patches
    if isinstance(raw, list):
        for p in raw:
            if isinstance(p, str):
                patches.append(PatchEntry(path=p))
            elif isinstance(p, dict):
                patches.append(PatchEntry(**p))
    elif isinstance(raw, dict):
        for path, info in raw.items():
            if isinstance(info, dict):
                patches.append(PatchEntry(path=path, **info))
            else:
                patches.append(PatchEntry(path=path))
    return patches


def _parse_dependencies(raw: Any) -> Dict[str, Any]:
    deps = {"runtime": [], "build": []}
    if not raw:
        return deps
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k in ("runtime", "build") and isinstance(v, list):
                deps[k].extend(v)
            elif k in ("runtime", "build") and isinstance(v, dict):
                deps[k].extend([f"{name}-{ver}" for name, ver in v.items()])
            else:
                deps["runtime"].append(v)
    elif isinstance(raw, list):
        deps["runtime"].extend(raw)
    return deps


# -------------------------------
# Função principal
# -------------------------------

def load_toml(path: str, pkgname: Optional[str] = None) -> Dict[str, Any]:
    """
    Lê um metafile TOML e retorna um dicionário plano compatível com o Builder.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo {path} não encontrado.")

    with open(path, "rb") as f:
        data = tomllib.load(f)

    pkg_info = data.get("package", {})
    name = pkg_info.get("name", pkgname or os.path.basename(path).split(".")[0])
    version = pkg_info.get("version", "0.0.0")
    variant = pkg_info.get("variant")

    meta = PackageMeta(
        name=name,
        version=version,
        variant=variant,
        sources=_parse_sources(data.get("source") or data.get("sources")),
        patches=_parse_patches(data.get("patches")),
        environment=data.get("environment", {}),
        hooks=data.get("hooks", {}),
        build=data.get("build", {}),
        package=data.get("package", {}),
        dependencies=_parse_dependencies(data.get("dependencies")),
        options=data.get("options", {}),
        raw=data,
    )

    return meta.to_dict()


# -------------------------------
# Teste rápido
# -------------------------------

if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Uso: zeropkg_toml.py <arquivo.toml>")
        sys.exit(1)
    meta = load_toml(sys.argv[1])
    print(json.dumps(meta, indent=2))
