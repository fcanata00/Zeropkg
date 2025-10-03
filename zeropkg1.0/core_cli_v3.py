core_cli_v3.py

import argparse import logging

from repo import RepoManager from resolver import Resolver from metafile import MetaFile from builder import Builder

logger = logging.getLogger("pmgr")

def main(): parser = argparse.ArgumentParser(prog="pmgr", description="Package Manager") sub = parser.add_subparsers(dest="command")

# Repo
p_repo = sub.add_parser("repo", help="Gerenciar repositórios")
p_repo.add_argument("-S", "--sync", action="store_true", help="Sincronizar repositórios")
p_repo.add_argument("-l", "--list", action="store_true", help="Listar repositórios")
p_repo.add_argument("-a", "--add", metavar="URL", help="Adicionar repositório")
p_repo.add_argument("-r", "--remove", metavar="URL", help="Remover repositório")

# Scan
p_scan = sub.add_parser("scan", help="Atualizar índice de pacotes")

# Search
p_search = sub.add_parser("search", help="Procurar pacotes")
p_search.add_argument("term", help="Nome ou parte do nome do pacote")

# Build
p_build = sub.add_parser("build", help="Construir pacote")
p_build.add_argument("pkg", help="Nome do pacote/metafile")
p_build.add_argument("-d", "--deploy", action="store_true", help="Instalar após build")
p_build.add_argument("-k", "--keep", action="store_true", help="Manter workdir")
p_build.add_argument("-o", "--outdir", default="pkg", help="Diretório para salvar pacotes")

# Install
p_install = sub.add_parser("install", help="Resolver deps e instalar")
p_install.add_argument("pkg", help="Nome do pacote/metafile")
p_install.add_argument("-d", "--deploy", action="store_true", help="Instalar após build")
p_install.add_argument("-k", "--keep", action="store_true", help="Manter workdir")
p_install.add_argument("-o", "--outdir", default="pkg", help="Diretório para salvar pacotes")

# Depclean
sub.add_parser("depclean", help="Remover pacotes órfãos")

# Revdep
p_revdep = sub.add_parser("revdep", help="Mostrar dependentes")
p_revdep.add_argument("pkg", help="Pacote alvo")

args = parser.parse_args()

if args.command == "repo":
    rm = RepoManager()
    if args.sync:
        rm.sync()
    if args.list:
        for r in rm.list():
            print(r)
    if args.add:
        rm.add(args.add)
    if args.remove:
        rm.remove(args.remove)

elif args.command == "scan":
    rm = RepoManager()
    rm.scan()

elif args.command == "search":
    rm = RepoManager()
    for mf in rm.search(args.term):
        print(f"{mf.name}-{mf.version}: {mf.summary}")

elif args.command == "build":
    mf = MetaFile.load_from_path(args.pkg)
    b = Builder(mf, outdir=args.outdir)
    b.full_build(resolve_deps=False, deploy=args.deploy, keep_workdir=args.keep)

elif args.command == "install":
    mf = MetaFile.load_from_path(args.pkg)
    b = Builder(mf, outdir=args.outdir)
    b.full_build(resolve_deps=True, deploy=args.deploy, keep_workdir=args.keep)

elif args.command == "depclean":
    resolver = Resolver([])
    resolver.depclean()

elif args.command == "revdep":
    resolver = Resolver([])
    print(resolver.revdep(args.pkg))

else:
    parser.print_help()

if name == "main": logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s") main()

