#!/usr/bin/env python3
# zeropkg_deps.py
"""
Módulo de resolução de dependências para Zeropkg.

Funcionalidades principais:
- DependencyResolver: carrega deps de receitas, constrói grafos, detecta ciclos
- missing_deps(pkgname): lista dependências não instaladas
- reverse_deps(pkgname): lista pacotes que dependem de pkgname (no conjunto instalado)
- resolve_and_install(resolver, pkgname, BuilderCls, InstallerCls, args):
    resolve deps em ordem topológica e, para cada dep faltante, chama BuilderCls.build(...)
    e espera que o Installer registre no DB.

Observações:
- Depende de:
    - zeropkg_toml.load_toml(ports_dir, pkgname) ou load_toml(path) (tratamos com flexibilidade)
    - zeropkg_db.DBManager com get_package(name) e list_installed()
    - Builder/Installer com assinaturas conforme discutido (Builder.build(pkg, args, dir_install=...))
"""

from __future__ import annotations

import os
import glob
import logging
from typing import Dict, List, Set, Tuple, Optional

from zeropkg_toml import load_toml
from zeropkg_db import DBManager
from zeropkg_logger import log_event

logger = logging.getLogger("zeropkg.deps")


class DependencyError(Exception):
    pass


class DependencyResolver:
    def __init__(self, db_path: str, ports_dir: str = "/usr/ports"):
        """
        db_path: caminho para DB do zeropkg
        ports_dir: raiz onde as receitas .toml estão (ex: /usr/ports)
        """
        self.db_path = db_path
        self.ports_dir = ports_dir
        self.db = DBManager(db_path)

    # ---------------------
    # helpers para carregar TOML / deps
    # ---------------------
    def _find_metafiles_for(self, pkgname: str) -> List[str]:
        """
        Procura arquivos <pkgname>-*.toml em ports_dir e retorna lista de caminhos.
        """
        pattern = os.path.join(self.ports_dir, "**", f"{pkgname}-*.toml")
        return glob.glob(pattern, recursive=True)

    def _load_meta(self, pkgname: str) -> Dict:
        """
        Tenta carregar a receita TOML do pacote.
        Tentamos múltiplas assinaturas de load_toml para compatibilidade:
          - load_toml(ports_dir, pkgname)
          - load_toml(path_to_metafile)
        Lança FileNotFoundError se não encontrar metafile.
        """
        # 1) tenta load_toml com (ports_dir, pkgname)
        try:
            return load_toml(self.ports_dir, pkgname)
        except TypeError:
            # assinatura diferente — tenta localizar arquivo
            pass
        except Exception:
            # pode falhar; tentar localizar por glob abaixo
            pass

        # 2) procurar metafiles sob ports_dir
        matches = self._find_metafiles_for(pkgname)
        if not matches:
            raise FileNotFoundError(f"Metafile for package '{pkgname}' not found under {self.ports_dir}")
        # escolher a versão "mais alta" heurística: nome com versão maior lexicograficamente não é perfeito,
        # mas escolher primeiro match é aceitável; caller deve garantir versões corretas.
        # Melhor: ordenar por versão numérica — aqui mantemos simples e escolhemos primeiro (poderia melhorar).
        path = sorted(matches)[-1]  # escolher o 'ultimo' ordenado para dar preferência a versões maiores
        try:
            return load_toml(path)
        except TypeError:
            # possível outra assinatura; tentar load_toml(ports_dir, pkgname) novamente falharia, mas rethrow
            return load_toml(path)

    def _extract_deps_from_meta(self, meta: Dict) -> Dict[str, List[str]]:
        """
        Retorna dict com chaves 'build', 'runtime', 'optional' (listas).
        Trata diferentes formatos do metafile TOML (tolerante).
        """
        deps = {"build": [], "runtime": [], "optional": []}
        if not meta:
            return deps

        # chaves possíveis: [dependencies] build/runtime/optional OR [depends] build/runtime
        section = meta.get("dependencies") or meta.get("depends") or {}
        if isinstance(section, dict):
            for k in ["build", "runtime", "optional"]:
                v = section.get(k, [])
                if isinstance(v, list):
                    deps[k] = [str(x) for x in v]
                elif isinstance(v, str):
                    deps[k] = [v]
        else:
            # seção inesperada, tentar pegar campos comuns
            for k in ["build", "runtime"]:
                v = meta.get(k, [])
                if isinstance(v, list):
                    deps[k] = [str(x) for x in v]

        # algumas receitas podem ter deps inline em meta['package']['deps']
        pkgsec = meta.get("package", {}) or {}
        inline = pkgsec.get("deps") or pkgsec.get("depends")
        if inline:
            if isinstance(inline, list):
                # append to runtime if not present
                for d in inline:
                    if d not in deps["runtime"]:
                        deps["runtime"].append(str(d))
            elif isinstance(inline, str):
                if inline not in deps["runtime"]:
                    deps["runtime"].append(inline)

        return deps

    # ---------------------
    # API principal
    # ---------------------
    def missing_deps(self, pkgname: str) -> List[str]:
        """
        Retorna lista única de dependências runtime (recursivas) que NÃO estão instaladas no DB.
        Não instala automaticamente; apenas informa.
        """
        try:
            # construir grafo de dependências recursivamente começando por pkgname
            graph = self._build_dep_graph([pkgname], include_build=False)
            needed = set()
            for node in graph:
                if node == pkgname:
                    continue
                # checar se instalado
                if not self.db.get_package(node):
                    needed.add(node)
            return sorted(needed)
        except Exception as e:
            logger.exception("missing_deps failed")
            raise DependencyError(f"missing_deps failed: {e}")

    def reverse_deps(self, pkgname: str) -> List[str]:
        """
        Retorna lista de pacotes (instalados) que dependem de pkgname (dependências reversas).
        Usa apenas o conjunto de pacotes listados como instalados no DB.
        """
        try:
            installed = self.db.list_installed()  # lista de dicts
            installed_names = [p["name"] for p in installed]
            revs = []
            for pkg in installed_names:
                # carregar meta do pacote instalado (pode não existir no ports, então ignorar)
                try:
                    meta = self._load_meta(pkg)
                except FileNotFoundError:
                    continue
                deps = self._extract_deps_from_meta(meta)
                runtime = deps.get("runtime", []) or []
                if pkgname in runtime:
                    revs.append(pkg)
            return sorted(revs)
        except Exception as e:
            logger.exception("reverse_deps failed")
            raise DependencyError(f"reverse_deps failed: {e}")

    def _build_dep_graph(self, roots: List[str], include_build: bool = False) -> Dict[str, Set[str]]:
        """
        Constrói grafo de dependências (direcionado) partindo de 'roots' (lista de package names).
        O grafo contém arestas A -> B where A depends on B.
        include_build: se True, inclui dependências de build também.
        Retorna dict: {pkg: set(of dependencies)} incluindo nós para todos pacotes visitados.
        """
        graph: Dict[str, Set[str]] = {}
        visited: Set[str] = set()

        def visit(pkg: str):
            if pkg in visited:
                return
            visited.add(pkg)
            try:
                meta = self._load_meta(pkg)
            except FileNotFoundError:
                # pacote não tem metafile no ports: deixamos sem dependências (é external / host-provided)
                graph.setdefault(pkg, set())
                return
            deps = self._extract_deps_from_meta(meta)
            runtime = deps.get("runtime", []) or []
            build = deps.get("build", []) or []
            reqs = set(runtime)
            if include_build:
                reqs |= set(build)
            graph.setdefault(pkg, set())
            for d in reqs:
                graph[pkg].add(d)
                visit(d)

        for r in roots:
            visit(r)
        return graph

    def resolve_graph(self, pkgname: str, include_build: bool = False) -> List[str]:
        """
        Retorna lista topologicamente ordenada de pacotes que precisam ser considerados para
        instalação/construção na ordem correta: dependências primeiro, pacote último.
        include_build: incluir dependências de build também.
        """
        graph = self._build_dep_graph([pkgname], include_build=include_build)
        order = self._topological_sort(graph)
        # queremos ordem com dependências primeiro e o pkgname por último
        return order

    def _topological_sort(self, graph: Dict[str, Set[str]]) -> List[str]:
        """
        Ordenação topológica. Lança DependencyError se ciclo detectado.
        Retorna lista com nós ordenados (deps antes dos que usam).
        """
        visited: Set[str] = set()
        temp: Set[str] = set()
        order: List[str] = []

        def visit(n: str):
            if n in visited:
                return
            if n in temp:
                # ciclo
                raise DependencyError(f"Dependency cycle detected at {n}")
            temp.add(n)
            for m in graph.get(n, set()):
                visit(m)
            temp.remove(n)
            visited.add(n)
            order.append(n)

        for node in list(graph.keys()):
            if node not in visited:
                visit(node)
        # order contém nós com dependências antes — remover duplicados e manter ordem
        return order

    # ---------------------
    # utilitário que instala dependências faltantes usando Builder/Installer
    # Nota: esta função é independente, mas usa classes passadas para chamar build/install.
    # ---------------------
def resolve_and_install(resolver: DependencyResolver, pkgname: str,
                        BuilderCls, InstallerCls, args) -> List[str]:
    """
    Resolve dependências transitivas de 'pkgname' e, para cada dependência que NÃO está instalada,
    chama BuilderCls(...).build(dep, args) para construir/instalar.

    Parâmetros:
      - resolver: DependencyResolver instance
      - pkgname: pacote alvo (string)
      - BuilderCls: class reference para Builder (será instanciada com db_path e ports_dir)
      - InstallerCls: class reference para Installer (pode ser útil; atualmente não usado diretamente)
      - args: objeto args do CLI (contém db_path, ports_dir, dry_run, root, fakeroot, build_root, etc.)

    Retorna a lista de pacotes efetivamente construídos/instalados (em ordem).
    """
    logger = logging.getLogger("zeropkg.deps.resolve_and_install")
    built: List[str] = []

    # extrair opções do args (com defaults)
    db_path = getattr(args, "db_path", "/var/lib/zeropkg/installed.sqlite3")
    ports_dir = getattr(args, "ports_dir", "/usr/ports")
    build_root = getattr(args, "build_root", "/var/zeropkg/build")
    cache_dir = getattr(args, "cache_dir", "/usr/ports/distfiles")
    packages_dir = getattr(args, "packages_dir", "/var/zeropkg/packages")
    dry_run = getattr(args, "dry_run", False)
    fakeroot = getattr(args, "fakeroot", False)

    # instanciar builder/installer
    builder = BuilderCls(db_path=db_path, ports_dir=ports_dir,
                         build_root=build_root, cache_dir=cache_dir, packages_dir=packages_dir)
    installer = InstallerCls(db_path=db_path, ports_dir=ports_dir,
                             root=getattr(args, "root", "/"), dry_run=dry_run, use_fakeroot=fakeroot)

    # construir grafo e ordem
    try:
        order = resolver.resolve_graph(pkgname, include_build=False)
    except DependencyError as de:
        logger.error(f"Failed to resolve graph for {pkgname}: {de}")
        raise

    # order tem dependências primeiro; queremos instalar todos exceto o próprio pkgname (por enquanto)
    # mas incluir o pkgname também, já que às vezes queremos construir tudo (resolve_and_install)
    # vamos iterar até o penúltimo (instalar deps) e deixar o pkgname para o caller (builder)
    # No entanto, para conveniência deste helper, construiremos todos na ordem completa.
    db = DBManager(db_path)

    for pkg in order:
        # não tentar construir pacotes que já estão instalados
        if db.get_package(pkg):
            logger.debug(f"{pkg} is already installed; skipping")
            continue
        # ignorar placeholders vazios (por segurança)
        if not pkg or pkg.strip() == "":
            continue
        logger.info(f"Resolving & building dependency: {pkg}")
        try:
            # builder.build aceita target (nome do pacote), args, dir_install opcional
            builder.build(pkg, args)
            # depois do builder.build, assumir que installer registrou no DB; conferir
            if not db.get_package(pkg):
                # pode ser que builder não registrou; tentamos instalar pacote gerado do cache
                # tente localizar pacote em packages_dir
                pkg_file = None
                candidate = os.path.join(packages_dir, f"{pkg}.tar.xz")
                if os.path.exists(candidate):
                    pkg_file = candidate
                if pkg_file:
                    installer.install(pkg, args, pkg_file=pkg_file, meta=None)
            built.append(pkg)
        except Exception as e:
            logger.error(f"Failed to build/install dependency {pkg}: {e}")
            raise DependencyError(f"Failed to build/install dependency {pkg}: {e}")

    return built
