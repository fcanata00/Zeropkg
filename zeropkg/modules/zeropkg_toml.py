
import hashlib
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import os

# compatibilidade tomllib / tomli
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        raise ImportError("Precisa de 'tomllib' (Python 3.11+) ou 'tomli' instalado.")

class ValidationError(Exception):
    """Erro de validação de metafile TOML."""
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
    dependencies: List[Dict[str, str]] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)

def validate_metadata(data: Dict[str, Any]) -> None:
    """Valida a estrutura básica do TOML."""
    required = ["name", "version"]
    for r in required:
        if r not in data:
            raise ValidationError(f"Campo obrigatório ausente: {r}")

    if "sources" in data:
        if not isinstance(data["sources"], list):
            raise ValidationError("sources deve ser uma lista")
        for src in data["sources"]:
            if "url" not in src:
                raise ValidationError("Cada source precisa de campo 'url'")

    if "patches" in data and not isinstance(data["patches"], list):
        raise ValidationError("patches deve ser lista")

    if "environment" in data and not isinstance(data["environment"], dict):
        raise ValidationError("environment deve ser dict")

    if "hooks" in data and not isinstance(data["hooks"], dict):
        raise ValidationError("hooks deve ser dict")

def parse_toml(path: str) -> PackageMeta:
    """Lê e valida um metafile TOML, retornando um PackageMeta."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "rb") as f:
        data = tomllib.load(f)

    validate_metadata(data)

    sources = [SourceEntry(**src) for src in data.get("sources", [])]
    patches = [PatchEntry(**p) for p in data.get("patches", [])]

    return PackageMeta(
        name=data["name"],
        version=data["version"],
        variant=data.get("variant"),
        sources=sources,
        patches=patches,
        environment=data.get("environment", {}),
        hooks=data.get("hooks", {}),
        build=data.get("build", {}),
        package=data.get("package", {}),
        dependencies=data.get("dependencies", []),
        raw=data
    )

def package_id(meta: PackageMeta) -> str:
    """Gera um id determinístico curto baseado no nome e versão."""
    base = f"{meta.name}-{meta.version}-{meta.variant or ''}"
    return hashlib.sha1(base.encode()).hexdigest()[:12]
