#!/usr/bin/env python3
"""
zeropkg_deps.py
Gerenciamento avançado de dependências para Zeropkg.
"""

import logging
from collections import defaultdict, deque

from zeropkg_logger import log_event
from zeropkg_db import DBManager
from zeropkg_toml import load_toml

logger = logging.getLogger("zeropkg.deps")


class DependencyResolver:
    def __init__(self, db_path, ports_dir):
        self.db = DBManager(db_path)
        self.ports_dir = ports_dir

    def _load_deps_from_toml(self, pkg_name):
        """Carrega dependências de um pacote via TOML."""
        meta = load_toml(self.ports_dir, pkg_name)
        deps = {
            "runtime": meta.get("dependencies", {}).get("runtime", []),
            "build": meta.get("dependencies", {}).get("build", []),
            "optional": meta.get("dependencies", {}).get("optional", []),
        }
        return deps

    def resolve_graph(self, pkg_name, include_build=False):
        """
        Resolve dependências em forma de grafo (BFS).
        Retorna lista ordenada de pacotes a instalar.
        """
        graph = defaultdict(list)
        visited = set()
        order = []
        queue = deque([pkg_name])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            deps = self._load_deps_from_toml(current)
            all_deps = deps["runtime"] + (deps["build"] if include_build else [])
            graph[current] = all_deps

            for dep in all_deps:
                if dep not in visited:
                    queue.append(dep)

        # Ordenar pacotes (dependências antes do alvo)
        def dfs(node, seen, stack):
            if node in seen:
                return
            seen.add(node)
            for dep in graph.get(node, []):
                dfs(dep, seen, stack)
            stack.append(node)

        stack = []
        dfs(pkg_name, set(), stack)
        order = stack[::-1]

        return order

    def detect_cycles(self, pkg_name):
        """Detecta ciclos no grafo de dependências."""
        graph = defaultdict(list)
        deps = self._load_deps_from_toml(pkg_name)
        graph[pkg_name] = deps["runtime"] + deps["build"]

        visited = set()
        stack = set()

        def visit(node):
            if node in stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            stack.add(node)
            for dep in graph.get(node, []):
                if visit(dep):
                    return True
            stack.remove(node)
            return False

        return visit(pkg_name)

    def missing_deps(self, pkg_name):
        """Retorna dependências faltando (não instaladas)."""
        deps = self._load_deps_from_toml(pkg_name)
        missing = []
        for dep in deps["runtime"] + deps["build"]:
            if not self.db.is_installed(dep):
                missing.append(dep)
        return missing

    def reverse_deps(self, pkg_name):
        """Encontra pacotes que dependem do pacote dado."""
        revdeps = []
        installed = self.db.list_installed()
        for pkg in installed:
            deps = self._load_deps_from_toml(pkg)
            if pkg_name in deps["runtime"] or pkg_name in deps["build"]:
                revdeps.append(pkg)
        return revdeps


# Funções auxiliares
def resolve_and_install(resolver, pkg_name, builder, installer, args):
    """
    Resolve dependências e instala todas em ordem correta.
    """
    order = resolver.resolve_graph(pkg_name, include_build=True)
    for dep in order:
        if not resolver.db.is_installed(dep):
            log_event("deps", "install", f"Instalando dependência {dep}")
            builder.build(dep, args)
            installer.install(dep, args)
