"""
zeropkg_toml.py

Parser e validador de metafiles TOML para Zeropkg.

API pública:
- parse_toml(path: str) -> PackageMeta
- parse_package_file(path: str) -> PackageMeta  (alias para compatibilidade)
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
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError as e:
        raise ImportError("É necessário ter 'tomllib' (Python 3.11+) ou 'tomli' instalado") from e


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

    required = ["name", "version"]
    for r in required:
        if r not in data:
            raise ValidationError(f"Campo obrigatório ausente: {r}")

    if "sources" in data:
        if not isinstance(data["sources"], list):
            raise ValidationError("Campo 'sources' deve ser uma lista de tabelas")
        for src in data["sources"]:
            if not isinstance(src, dict):
                raise ValidationError("Cada source deve ser uma tabela/dicionário")
            if "url" not in src or not isinstance(src["url"], str):
                raise ValidationError("Cada source precisa ter 'url' (string)")
            if "checksum" in src and src["checksum"] is not None and not isinstance(src["checksum"], str):
                raise ValidationError("O campo 'checksum' em source deve ser string quando presente")

    if "patches" in data and not isinstance(data["patches"], list):
        raise ValidationError("Campo 'patches' deve ser uma lista de tabelas")

    if "environment" in data and not isinstance(data["environment"], dict):
        raise ValidationError("Campo 'environment' deve ser um dicionário string->string")

    if "hooks" in data and not isinstance(data["hooks"], dict):
        raise ValidationError("Campo 'hooks' deve ser um dicionário (ex.: pre_configure = ['cmd1'])")


def _to_source_entry(s: Dict[str, Any]) -> SourceEntry:
    return SourceEntry(
        url=s.get("url"),
        checksum=s.get("checksum"),
        type=s.get("type"),
        priority=int(s.get("priority", 0)) if s.get("priority") is not None else 0
    )


def _to_patch_entry(p: Dict[str, Any]) -> PatchEntry:
    return PatchEntry(
        path=p.get("path"),
        apply_as=p.get("apply_as", "patch"),
        stage=p.get("stage", "pre_configure"),
        strip=int(p.get("strip", 1))
    )


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

    validate_metadata(data)

    sources = []
    for s in data.get("sources", []):
        # aceitar tanto dicts quanto objetos já compatíveis
        if isinstance(s, dict):
            sources.append(_to_source_entry(s))
        elif isinstance(s, SourceEntry):
            sources.append(s)
        else:
            raise ValidationError("Item inválido em 'sources'")

    patches = []
    for p in data.get("patches", []):
        if isinstance(p, dict):
            patches.append(_to_patch_entry(p))
        elif isinstance(p, PatchEntry):
            patches.append(p)
        else:
            raise ValidationError("Item inválido em 'patches'")

    return PackageMeta(
        name=data["name"],
        version=data["version"],
        variant=data.get("variant"),
        sources=sources,
        patches=patches,
        environment={k: str(v) for k, v in (data.get("environment") or {}).items()},
        hooks={k: list(v) for k, v in (data.get("hooks") or {}).items()},
        build=data.get("build", {}) or {},
        package=data.get("package", {}) or {},
        dependencies=data.get("dependencies", []) or [],
        raw=data
    )


# alias usado em alguns outros módulos/CLI
def parse_package_file(path: str) -> PackageMeta:
    return parse_toml(path)


def package_id(meta: PackageMeta) -> str:
    """Gera um id curto determinístico para o pacote (útil para staging)."""
    base = f"{meta.name}-{meta.version}-{meta.variant or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
