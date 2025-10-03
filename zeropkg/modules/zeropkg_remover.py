#!/usr/bin/env python3
"""
zeropkg_remover.py

Módulo responsável por remover pacotes e executar hooks de pré/post-remover.

Funcionalidades:
- Verifica revdeps (quem depende do pacote) antes de remover (a menos que force=True).
- Carrega metafile TOML do ports para descobrir hooks (pre_remove/post_remove).
- Chama Installer.remove(...) (que executa hooks e atualiza o DB).
- Retorna relatório estruturado das ações realizadas.

API pública:
- remove_package(name, version=None, db_path=..., ports_dir=..., root="/", dry_run=False, use_fakeroot=True, force=False)
- find_metafile_for_installed(name, ports_dir)
- can_remove_package(name, version=None, db_path=...)
"""

from __future__ import annotations
import os
import logging
import argparse
from typing import Optional, Dict, Any, List

from zeropkg_toml import parse_toml, ValidationError
from zeropkg_installer import Installer
from zeropkg_db import connect, find_revdeps, get_package
from zeropkg_logger import log_event

logger = logging.getLogger("zeropkg.remover")


class RemoveError(Exception):
    pass


def find_metafile_for_installed(name: str, ports_dir: str = "/usr/ports") -> Optional[str]:
    """
    Procura por um metafile .toml adequado para 'name' dentro de ports_dir.
    Retorna caminho do metafile (o primeiro que achar) ou None.
    """
    for root, _, files in os.walk(ports_dir):
        for f in files:
            if f.endswith(".toml") and f.startswith(name + "-"):
                return os.path.join(root, f)
    candidate = os.path.join(ports_dir, name, f"{name}.toml")
    if os.path.exists(candidate):
        return candidate
    return None


def can_remove_package(name: str, version: Optional[str] = None, db_path: str = "/var/lib/zeropkg/installed.sqlite3") -> Dict[str, Any]:
    """
    Verifica se o pacote pode ser removido:
    - retorna dict com keys: can_remove (bool), revdeps (list)
    """
    conn = connect(db_path)
    revs = find_revdeps(conn, name)
    conn.close()
    return {"can_remove": len(revs) == 0, "revdeps": revs}


def remove_package(name: str,
                   version: Optional[str] = None,
                   db_path: str = "/var/lib/zeropkg/installed.sqlite3",
                   ports_dir: str = "/usr/ports",
                   root: str = "/",
                   dry_run: bool = False,
                   use_fakeroot: bool = True,
                   force: bool = False) -> Dict[str, Any]:
    """
    Remove um pacote do sistema (prefix 'root'), chamando hooks pre/post remove quando disponíveis.

    Parâmetros:
      - name: nome do pacote a remover
      - version: versão específica (opcional)
      - db_path: caminho do sqlite DB
      - ports_dir: onde procurar metafiles (para obter hooks)
      - root: prefixo de instalação (ex: / ou /mnt/lfs)
      - dry_run: se True, apenas simula e não modifica nada
      - use_fakeroot: repassa para Installer (fakeroot ou não)
      - force: se True, ignora revdeps e força a remoção

    Retorna:
      dict { "status": "ok"|"blocked"|"error", "removed": True/False, "revdeps": [...], "message": str }
    """
    log_event(name, "remove", f"Solicitada remoção: {name} {version or ''} (root={root})")
    # 1) verificar revdeps
    conn = connect(db_path)
    revs = find_revdeps(conn, name)
    conn.close()
    if revs and not force:
        msg = f"Remoção bloqueada: {len(revs)} pacotes dependem de {name}"
        log_event(name, "remove", msg, level="warning")
        return {"status": "blocked", "removed": False, "revdeps": revs, "message": msg}

    # 2) tentar ler hooks do metafile (se disponível)
    metafile = find_metafile_for_installed(name, ports_dir)
    hooks = None
    meta_info = None
    if metafile:
        try:
            meta = parse_toml(metafile)
            hooks = getattr(meta, "hooks", None) or {}
            meta_info = {"name": meta.name, "version": meta.version, "meta_path": metafile}
            log_event(name, "remove", f"Metafile encontrado: {metafile}")
        except (FileNotFoundError, ValidationError) as e:
            # continua sem hooks se falhar
            log_event(name, "remove", f"Erro ao ler metafile ({metafile}): {e}", level="warning")
            hooks = None
    else:
        log_event(name, "remove", "Nenhum metafile encontrado; remoção prossegue sem hooks")

    # 3) executar remoção via Installer (que já registra no DB e aplica hooks)
    inst = Installer(db_path=db_path, dry_run=dry_run, root=root, use_fakeroot=use_fakeroot)
    try:
        # Installer.remove aceita hooks optional
        inst.remove(name, version, hooks=hooks)
        msg = f"Remoção executada: {name} {version or ''}"
        log_event(name, "remove", msg)
        return {"status": "ok", "removed": True, "revdeps": revs, "message": msg, "meta": meta_info}
    except Exception as e:
        log_event(name, "remove", f"Erro na remoção de {name}: {e}", level="error")
        raise RemoveError(str(e)) from e


# -----------------------
# CLI / utilitário mínimo
# -----------------------
def _build_argparser():
    p = argparse.ArgumentParser(prog="zeropkg-remove", description="Remover pacote Zeropkg (executa hooks pre/post)")
    p.add_argument("package", help="nome do pacote (ex: gcc)")
    p.add_argument("-v", "--version", help="versão específica (opcional)", default=None)
    p.add_argument("--root", default="/", help="prefixo de instalação (ex: /mnt/lfs)")
    p.add_argument("--ports-dir", default="/usr/ports", help="diretório de recipes/ports")
    p.add_argument("--db-path", default="/var/lib/zeropkg/installed.sqlite3", help="caminho do DB sqlite")
    p.add_argument("--dry-run", action="store_true", help="simula a remoção sem alterar nada")
    p.add_argument("-f", "--force", action="store_true", help="força remoção mesmo se houver revdeps")
    p.add_argument("--no-fakeroot", action="store_true", help="não usar fakeroot (por padrão usa fakeroot)")
    return p


def main(argv: Optional[List[str]] = None):
    p = _build_argparser()
    args = p.parse_args(argv)

    try:
        res = remove_package(
            args.package,
            version=args.version,
            db_path=args.db_path,
            ports_dir=args.ports_dir,
            root=args.root,
            dry_run=args.dry_run,
            use_fakeroot=not args.no_fakeroot,
            force=args.force
        )
        if res["status"] == "blocked":
            print("Remoção bloqueada — pacotes dependem deste:", res["revdeps"])
            print("Use --force para forçar (cuidado).")
            return 2
        elif res["status"] == "ok":
            print("Remoção concluída:", res.get("message"))
            return 0
        else:
            print("Erro:", res.get("message"))
            return 1
    except RemoveError as e:
        print("Falha na remoção:", e)
        return 1


if __name__ == "__main__":
    exit(main())
