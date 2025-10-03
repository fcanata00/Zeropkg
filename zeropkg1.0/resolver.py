"""
resolver.py

Módulo de resolução de dependências para pmgr.

Funcionalidades principais:
- Resolver dependências recursivas com constraints simples (==, >=, <=, >, <, ~=)
- Operar sobre o índice gerado pelo RepoManager (nome-version -> metadata)
- revdep: mostrar dependências reversas usando o banco local de pacotes instalados
- depclean: detectar órfãos e, opcionalmente, removê-los do DB (dry-run por padrão)
- Marcar pacotes como instalados/removidos no DB local (simples JSON DB)

Observação: este resolver é propositalmente autocontido e funcional — adequado para integração
com o builder e o core. Ele usa um algoritmo de backtracking simples para tentar encontrar
uma combinação de versões que satisfaça as dependências.
"""
from __future__ import annotations
import json
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set

try:
    from ports_manager_initial_modules import (
        setup_logging,
        ensure_dirs,
        CACHE_DIR_DEFAULT,
        DB_PATH_DEFAULT,
        PORTS_DIR_DEFAULT,
        MetaFile,
    )
    from repo_manager import RepoManager
except Exception:
    # Se os módulos estiverem em lugares diferentes durante testes, importe relativo
    raise

logger = setup_logging('pmgr_resolver', log_dir=Path('./logs'))

# Banco local simples (JSON) para registrar pacotes instalados e 'explicitly_installed' flag
LOCAL_DB_PATH = Path('./var/lib/pmgr/local_db.json')
ensure_dirs(LOCAL_DB_PATH.parent, CACHE_DIR_DEFAULT)

VERSION_RE = re.compile(r"^(?P<name>[A-Za-z0-9_+.-]+)(?:(?P<op>>=|<=|==|~=|>|<)(?P<ver>.+))?$")


def _load_local_db() -> Dict[str, Any]:
    if LOCAL_DB_PATH.exists():
        try:
            return json.loads(LOCAL_DB_PATH.read_text(encoding='utf-8'))
        except Exception as e:
            logger.error('Falha ao ler local DB: %s', e)
            return {'installed': {}, 'explicit': []}
    return {'installed': {}, 'explicit': []}


def _save_local_db(db: Dict[str, Any]) -> None:
    LOCAL_DB_PATH.write_text(json.dumps(db, indent=2, ensure_ascii=False), encoding='utf-8')


def parse_dependency(dep: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Parse dependency strings like 'libfoo>=1.2.3' or 'bar' -> (name, op, version)"""
    m = VERSION_RE.match(dep.strip())
    if not m:
        raise ValueError(f'Dependência inválida: {dep}')
    return m.group('name'), m.group('op'), m.group('ver')


def _ver_to_tuple(v: str) -> Tuple:
    parts = v.split('.')
    tup = []
    for p in parts:
        try:
            tup.append(int(p))
        except Exception:
            tup.append(p)
    return tuple(tup)


def _compare_versions(a: str, op: Optional[str], b: Optional[str]) -> bool:
    if op is None:
        return True
    if b is None:
        return True
    at = _ver_to_tuple(a)
    bt = _ver_to_tuple(b)
    if op == '==':
        return at == bt
    if op == '>=':
        return at >= bt
    if op == '<=':
        return at <= bt
    if op == '>':
        return at > bt
    if op == '<':
        return at < bt
    if op == '~=':  # compatible (major.minor match)
        # interpret ~= as same major and at >= bt
        if len(at) == 0 or len(bt) == 0:
            return False
        return at[0] == bt[0] and at >= bt
    return False


class ResolveError(Exception):
    pass


class Resolver:
    def __init__(self, repo_manager: Optional[RepoManager] = None):
        self.rm = repo_manager or RepoManager()
        self.index = self.rm.index.get('packages', {})
        # available[name] -> list of versions (sorted desc)
        self.available: Dict[str, List[str]] = {}
        for k, v in self.index.items():
            n = v.get('name')
            ver = v.get('version')
            self.available.setdefault(n, []).append(ver)
        for n, vers in self.available.items():
            # sort versions descending using tuple compare
            self.available[n] = sorted(vers, key=lambda x: _ver_to_tuple(x), reverse=True)
        logger.debug('Resolver available keys: %d', len(self.available))

    def _find_candidate_versions(self, name: str, op: Optional[str], ver: Optional[str]) -> List[str]:
        vers = self.available.get(name, [])
        res = [v for v in vers if _compare_versions(_ver_to_tuple(v) and v, op, ver)]
        return res

    def _index_key_for(self, name: str, version: str) -> Optional[str]:
        # find the index key like name-version
        for k, v in self.index.items():
            if v.get('name') == name and v.get('version') == version:
                return k
        return None

    def _load_deps_for(self, name: str, version: str) -> List[str]:
        key = self._index_key_for(name, version)
        if not key:
            raise ResolveError(f'Pacote {name}-{version} não encontrado no índice')
        path = Path(self.index[key]['path'])
        mf = MetaFile.from_path(path)
        # dependencies pode ser dict com categorias
        deps = []
        for cat, lst in mf.dependencies.items():
            if isinstance(lst, list):
                deps.extend(lst)
        return deps

    def resolve(self, requests: List[str]) -> List[str]:
        """Resolve uma lista de pacotes solicitados (cada um pode ter constraint).
        Retorna uma lista de index keys (name-version) na ordem topológica de instalação.
        """
        # Parse requests into (name,op,ver)
        parsed = [parse_dependency(r) for r in requests]
        logger.info('Iniciando resolução para: %s', parsed)

        # state: chosen[name] = version
        chosen: Dict[str, str] = {}

        # backtracking search
        def dfs(to_resolve: List[Tuple[str, Optional[str], Optional[str]]]) -> bool:
            if not to_resolve:
                return True
            name, op, ver = to_resolve.pop(0)
            if name in chosen:
                # já escolhido, verificar compatibilidade
                if not _compare_versions(chosen[name], op, ver):
                    to_resolve.insert(0, (name, op, ver))
                    return False
                return dfs(to_resolve)

            candidates = self.available.get(name, [])
            if op or ver:
                # filter
                candidates = [v for v in candidates if _compare_versions(v, op, ver)]
            if not candidates:
                logger.error('Nenhuma versão disponível para %s com restrição %s%s', name, op or '', ver or '')
                to_resolve.insert(0, (name, op, ver))
                return False

            # try candidates in order
            for v in candidates:
                chosen[name] = v
                try:
                    deps = self._load_deps_for(name, v)
                except ResolveError as e:
                    logger.debug('Falha ao carregar deps para %s-%s: %s', name, v, e)
                    del chosen[name]
                    continue
                # prepare new list: prepend deps
                parsed_deps = [parse_dependency(d) for d in deps]
                # Merge: new_to_resolve = parsed_deps + remaining
                new_to = parsed_deps + list(to_resolve)
                if dfs(new_to):
                    return True
                # backtrack
                del chosen[name]
            # all candidates failed
            to_resolve.insert(0, (name, op, ver))
            return False

        requests_copy = list(parsed)
        ok = dfs(requests_copy)
        if not ok:
            raise ResolveError('Falha ao resolver dependências para: %s' % requests)

        # chosen contains mapping name->version, produce install order via topo sort
        # build adjacency
        adj: Dict[str, Set[str]] = {}
        for n, v in chosen.items():
            key = f'{n}-{v}'
            adj[key] = set()
        for n, v in chosen.items():
            key = f'{n}-{v}'
            deps = self._load_deps_for(n, v)
            for d in deps:
                dn, dop, dver = parse_dependency(d)
                dv = chosen.get(dn)
                if dv:
                    adj[key].add(f'{dn}-{dv}')

        # topo sort
        visited: Dict[str, int] = {}
        result: List[str] = []

        def visit(node: str):
            if visited.get(node, 0) == 1:
                return
            if visited.get(node, 0) == -1:
                raise ResolveError('Ciclo detectado no grafo de dependências')
            visited[node] = -1
            for nb in adj.get(node, []):
                visit(nb)
            visited[node] = 1
            result.append(node)

        for n in adj.keys():
            if visited.get(n, 0) == 0:
                visit(n)

        # result is install order (deps first)
        logger.info('Resolução completada — ordem de instalação: %s', result)
        return result

    # ---------------- revdep / depclean ----------------
    def revdep(self, package_name: str) -> List[str]:
        """Retorna lista de pacotes instalados (name-version) que dependem direta/indiretamente de package_name"""
        db = _load_local_db()
        installed = db.get('installed', {})
        # build graph: installed_key -> set(dependency_keys)
        graph: Dict[str, Set[str]] = {}
        for key, info in installed.items():
            deps = info.get('dependencies', [])
            dep_keys = set()
            for d in deps:
                dn, dop, dver = parse_dependency(d)
                # find installed matching
                for ik in installed.keys():
                    if ik.startswith(dn + '-'):
                        dep_keys.add(ik)
            graph[key] = dep_keys
        # invert and BFS from target
        targets = [k for k in installed.keys() if k.startswith(package_name + '-')]
        if not targets:
            return []
        # find all nodes that (transitively) depend on any target
        reverse_graph: Dict[str, Set[str]] = {}
        for k, deps in graph.items():
            for d in deps:
                reverse_graph.setdefault(d, set()).add(k)
        result: Set[str] = set()
        stack = list(targets)
        while stack:
            cur = stack.pop()
            for parent in reverse_graph.get(cur, []):
                if parent not in result:
                    result.add(parent)
                    stack.append(parent)
        return sorted(result)

    def depclean(self, dry_run: bool = True) -> List[str]:
        """Detecta pacotes órfãos e, se dry_run False, remove-os do DB local.
        Critério: pacote instalado que não é explicitamente instalado (db['explicit']) e não é dependência
        de nenhum outro pacote instalado.
        Retorna lista de pacotes candidatos à remoção.
        """
        db = _load_local_db()
        installed = db.get('installed', {})
        explicit = set(db.get('explicit', []))
        # build reverse dependency map
        reverse: Dict[str, Set[str]] = {}
        for key, info in installed.items():
            for d in info.get('dependencies', []):
                dn, dop, dver = parse_dependency(d)
                # map to installed matching keys
                for ik in installed.keys():
                    if ik.startswith(dn + '-'):
                        reverse.setdefault(ik, set()).add(key)
        orphans: List[str] = []
        for key in installed.keys():
            if key in explicit:
                continue
            dependents = reverse.get(key, set())
            if not dependents:
                orphans.append(key)
        if not orphans:
            logger.info('Nenhum pacote órfão detectado')
            return []
        logger.info('Pacotes órfãos detectados: %s', orphans)
        if not dry_run:
            for k in orphans:
                logger.info('Removendo %s do DB local', k)
                installed.pop(k, None)
            db['installed'] = installed
            _save_local_db(db)
        return orphans

    # ----------------- local DB helpers -----------------
    def mark_installed(self, pkg_key: str, dependencies: Optional[List[str]] = None, explicit: bool = True) -> None:
        db = _load_local_db()
        inst = db.setdefault('installed', {})
        inst[pkg_key] = {
            'installed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'dependencies': dependencies or [],
        }
        if explicit:
            db.setdefault('explicit', []).append(pkg_key)
        _save_local_db(db)
        logger.info('Marcado como instalado: %s', pkg_key)

    def mark_removed(self, pkg_key: str) -> None:
        db = _load_local_db()
        inst = db.setdefault('installed', {})
        if pkg_key in inst:
            inst.pop(pkg_key)
            # also remove from explicit if present
            if pkg_key in db.get('explicit', []):
                db['explicit'].remove(pkg_key)
            _save_local_db(db)
            logger.info('Removido do DB local: %s', pkg_key)
        else:
            logger.warning('Pacote não estava marcado como instalado: %s', pkg_key)


# ---------------- CLI -----------------
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(prog='pmgr_resolver', description='Resolver + revdep + depclean para pmgr')
    sub = p.add_subparsers(dest='cmd')

    res = sub.add_parser('resolve')
    res.add_argument('packages', nargs='+', help='Pacotes solicitados (ex: libfoo>=1.2)')

    rdep = sub.add_parser('revdep')
    rdep.add_argument('package', help='Nome do pacote (sem versão)')

    depc = sub.add_parser('depclean')
    depc.add_argument('--apply', action='store_true', help='Aplica remoção dos órfãos')

    mark = sub.add_parser('mark')
    mark.add_argument('pkg_key')
    mark.add_argument('--explicit', action='store_true')

    rm = sub.add_parser('remove')
    rm.add_argument('pkg_key')

    args = p.parse_args()
    resolver = Resolver()
    if args.cmd == 'resolve':
        try:
            order = resolver.resolve(args.packages)
            print('\n'.join(order))
        except ResolveError as e:
            print('ResolveError:', e)
    elif args.cmd == 'revdep':
        deps = resolver.revdep(args.package)
        print('\n'.join(deps))
    elif args.cmd == 'depclean':
        orphans = resolver.depclean(dry_run=not args.apply)
        print('\n'.join(orphans))
    elif args.cmd == 'mark':
        resolver.mark_installed(args.pkg_key, explicit=args.explicit)
    elif args.cmd == 'remove':
        resolver.mark_removed(args.pkg_key)
    else:
        p.print_help()
