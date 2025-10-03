#!/usr/bin/env python3
"""
zeropkg_toml.py

Parser e validador de metafiles TOML para Zeropkg.

API pública:
- parse_toml(path: str) -> PackageMeta
- parse_package_file(path: str) -> PackageMeta  (alias)
- validate_metadata(data: dict) -> None
- package_id(meta: PackageMeta) -> str
- ValidationError exception
"""

from __future__ import annotations
import os
import hashlib
import dataclasses
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

# compatibilidade tomllib / tomli
try:
    import tomllib  # Python 3.11+
except Exception:
    try:
        import tomli as tomllib  # type: ignore
    except Exception as e:
        raise ImportError("É necessário ter 'tomllib' (Py3.11+) ou 'tomli' (pip) instalado") from e


class ValidationError(Exception):
    """Lançada quando o metafile TOML não passa na validação."""
    pass


@dataclass
class SourceEntry:
    url: str
    checksum: Optional[str] = None
    type: Optional[str] = None
    priority: int = 0


@dataclass
class PatchEntry:
    path: str
    apply_as: str = "patch"
    stage: str = "pre_configure"
    strip: int = 1


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
    dependencies: List[Any] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)


def validate_metadata(data: Dict[str, Any]) -> None:
    """Valida a estrutura básica do dicionário resultante do TOML.

    Lança ValidationError em caso de problema.
    """
    if not isinstance(data, dict):
        raise ValidationError("Metafile TOML deve ser uma tabela no nível top-level")

    # se o arquivo usa [package] como subtable, normalize antes de validar
    working = data.get("package", data)

    required = ["name", "version"]
    for r in required:
        if r not in working:
            raise ValidationError(f"Campo obrigatório ausente: {r}")

    if "sources" in working:
        if not isinstance(working["sources"], list):
            raise ValidationError("Campo 'sources' deve ser uma lista de tabelas")
        for src in working["sources"]:
            if not isinstance(src, dict):
                raise ValidationError("Cada source deve ser uma tabela/dicionário")
            if "url" not in src or not isinstance(src["url"], str) or not src["url"].strip():
                raise ValidationError("Cada source precisa ter 'url' (string não-vazia)")
            if "checksum" in src and src["checksum"] is not None and not isinstance(src["checksum"], str):
                raise ValidationError("O campo 'checksum' em source deve ser string quando presente")

    if "patches" in working and not isinstance(working["patches"], list):
        raise ValidationError("Campo 'patches' deve ser uma lista de tabelas")

    if "environment" in working and not isinstance(working["environment"], dict):
        raise ValidationError("Campo 'environment' deve ser um dicionário string->string")

    if "hooks" in working and not isinstance(working["hooks"], dict):
        raise ValidationError("Campo 'hooks' deve ser um dicionário (ex.: pre_configure = ['cmd1'])")


def _to_source_entry(s: Dict[str, Any]) -> SourceEntry:
    # prioridade pode vir como string/numérico; converta com segurança
    pr = 0
    try:
        if "priority" in s and s["priority"] is not None:
            pr = int(s["priority"])
    except Exception:
        pr = 0
    return SourceEntry(
        url=str(s.get("url")),
        checksum=s.get("checksum"),
        type=s.get("type"),
        priority=pr
    )


def _to_patch_entry(p: Dict[str, Any]) -> PatchEntry:
    strip = 1
    try:
        if "strip" in p and p["strip"] is not None:
            strip = int(p["strip"])
    except Exception:
        strip = 1
    return PatchEntry(
        path=str(p.get("path")),
        apply_as=p.get("apply_as", "patch"),
        stage=p.get("stage", "pre_configure"),
        strip=strip
    )


def _normalize_hooks(raw_hooks: Any) -> Dict[str, List[str]]:
    """
    Garante que hooks seja dict[str, list[str]].
    Aceita valores: { pre_install = "cmd" } ou { pre_install = ["cmd1","cmd2"] }
    """
    if not raw_hooks:
        return {}
    if not isinstance(raw_hooks, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for k, v in raw_hooks.items():
        if v is None:
            out[k] = []
        elif isinstance(v, str):
            out[k] = [v]
        elif isinstance(v, list):
            # garantir strings
            out[k] = [str(x) for x in v]
        else:
            # caso estranho: transformar em string
            out[k] = [str(v)]
    return out


def parse_toml(path: str) -> PackageMeta:
    """Lê um arquivo TOML, valida e converte para PackageMeta.

    path: caminho para o arquivo .toml
    Retorna: PackageMeta
    Lança FileNotFoundError ou ValidationError.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Metafile não encontrado: {path}")

    with open(path, "rb") as f:
        data = tomllib.load(f)

    # se o autor colocou os campos em [package], use esse bloco
    if "package" in data and isinstance(data["package"], dict):
        working = data["package"]
    else:
        working = data

    # validação inicial
    validate_metadata(data)

    # sources
    sources: List[SourceEntry] = []
    for s in working.get("sources", []):
        if isinstance(s, dict):
            sources.append(_to_source_entry(s))
        elif isinstance(s, SourceEntry):
            sources.append(s)
        else:
            raise ValidationError("Item inválido em 'sources'")

    # patches
    patches: List[PatchEntry] = []
    for p in working.get("patches", []):
        if isinstance(p, dict):
            patches.append(_to_patch_entry(p))
        elif isinstance(p, PatchEntry):
            patches.append(p)
        else:
            raise ValidationError("Item inválido em 'patches'")

    # environment
    env_map = {k: str(v) for k, v in (working.get("environment") or {}).items()}

    # hooks (normalizar string -> list)
    hooks_map = _normalize_hooks(working.get("hooks", {}))

    return PackageMeta(
        name=str(working["name"]),
        version=str(working["version"]),
        variant=working.get("variant"),
        sources=sources,
        patches=patches,
        environment=env_map,
        hooks=hooks_map,
        build=working.get("build", {}) or {},
        package=working.get("package", {}) or {},
        dependencies=working.get("dependencies", []) or [],
        raw=data
    )


# alias usado em alguns outros módulos/CLI
def parse_package_file(path: str) -> PackageMeta:
    return parse_toml(path)


def package_id(meta: PackageMeta) -> str:
    """Gera um id curto determinístico para o pacote (útil para staging)."""
    base = f"{meta.name}-{meta.version}-{meta.variant or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
