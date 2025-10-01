#!/usr/bin/env python3
import os
import sys
import subprocess
import argparse
import tomllib
import shutil
from datetime import datetime

# Importa o módulo de dependências
from plugins import deps

LOGDIR = "logs"
WORKDIR = "work"
PKGDIR = "pkgdir"

# ========= Utilidades =========

def log_message(msg, log_file, quiet=False, color=None):
    colors = {
        "green": "\033[92m",
        "yellow": "\033[93m",
        "red": "\033[91m",
        "blue": "\033[94m",
        "reset": "\033[0m",
    }
    prefix = f"{colors.get(color,'')}{msg}{colors['reset'] if color else ''}"
    if not quiet:
        print(prefix)
    log_file.write(f"[{datetime.now()}] {msg}\n")
    log_file.flush()

def setup_logging(pkg):
    os.makedirs(LOGDIR, exist_ok=True)
    log_path = os.path.join(LOGDIR, f"{pkg}.log")
    return open(log_path, "w", encoding="utf-8")

def load_recipe(pkg):
    recipe_path = os.path.join("recipes", f"{pkg}.toml")
    if not os.path.exists(recipe_path):
        raise FileNotFoundError(f"Receita não encontrada: {recipe_path}")
    with open(recipe_path, "rb") as f:
        return tomllib.load(f)

def run_commands(commands, workdir, log_file, quiet=False, env=None):
    for cmd in commands:
        log_message(f"Executando: {cmd}", log_file, quiet, color="blue")
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env
        )
        for line in proc.stdout:
            decoded = line.decode("utf-8", errors="ignore")
            log_file.write(decoded)
            if not quiet:
                print(decoded, end="")
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"Falha ao executar: {cmd}")

def run_hooks(hook_stage, recipe, workdir, log_file, quiet=False, env=None):
    hooks = recipe.get("hooks", {})
    if hook_stage in hooks:
        log_message(f"[HOOK] {hook_stage}", log_file, quiet, color="blue")
        run_commands(hooks[hook_stage], workdir, log_file, quiet, env)

# ========= Funções principais =========

def fetch_sources(pkg, url, log_file, quiet=False):
    os.makedirs(WORKDIR, exist_ok=True)
    filename = os.path.join(WORKDIR, os.path.basename(url))
    if not os.path.exists(filename):
        log_message(f">>>> Baixando {pkg} <<<<", log_file, quiet, color="blue")
        cmd = ["wget", "-O", filename, url]
        subprocess.check_call(cmd)
    else:
        log_message(f"Fonte já existe: {filename}", log_file, quiet)
    return filename

def unpack_sources(pkg, tarball, log_file, quiet=False):
    src_dir = os.path.join(WORKDIR, f"{pkg}-src")
    if os.path.exists(src_dir):
        shutil.rmtree(src_dir)
    os.makedirs(src_dir, exist_ok=True)
    log_message(f">>>> Descompactando {pkg} em {src_dir} <<<<", log_file, quiet, color="yellow")
    subprocess.check_call(["tar", "--strip-components=1", "-xf", tarball, "-C", src_dir])
    return src_dir

def apply_patches(pkg, src_dir, patches, log_file, quiet=False):
    for patch in patches:
        log_message(f">>>> Aplicando patch {patch['file']} <<<<", log_file, quiet, color="green")
        subprocess.check_call(
            ["patch", f"-p{patch['strip']}", "-i", patch["file"]],
            cwd=src_dir
        )

# ========= Pipelines =========

def build_pipeline(pkg, quiet=False):
    recipe = load_recipe(pkg)
    log_file = setup_logging(pkg)

    env = os.environ.copy()
    env["PKGDIR"] = os.path.abspath(PKGDIR)

    # pre_fetch
    run_hooks("pre_fetch", recipe, ".", log_file, quiet, env)

    # fetch
    tarball = fetch_sources(pkg, recipe["source"]["url"], log_file, quiet)

    # post_fetch
    run_hooks("post_fetch", recipe, ".", log_file, quiet, env)

    # pre_unpack
    run_hooks("pre_unpack", recipe, ".", log_file, quiet, env)

    # unpack
    src_dir = unpack_sources(pkg, tarball, log_file, quiet)

    # post_unpack
    run_hooks("post_unpack", recipe, src_dir, log_file, quiet, env)

    # patches
    if "patches" in recipe:
        run_hooks("pre_patch", recipe, src_dir, log_file, quiet, env)
        apply_patches(pkg, src_dir, recipe["patches"], log_file, quiet)
        run_hooks("post_patch", recipe, src_dir, log_file, quiet, env)

    # build
    if "build" in recipe:
        run_hooks("pre_build", recipe, src_dir, log_file, quiet, env)
        run_commands(recipe["build"]["commands"], src_dir, log_file, quiet, env)
        run_hooks("post_build", recipe, src_dir, log_file, quiet, env)

    # check
    if "check" in recipe:
        run_hooks("pre_check", recipe, src_dir, log_file, quiet, env)
        run_commands(recipe["check"]["commands"], src_dir, log_file, quiet, env)
        run_hooks("post_check", recipe, src_dir, log_file, quiet, env)

    # install
    if "install" in recipe:
        run_hooks("pre_install", recipe, src_dir, log_file, quiet, env)
        os.makedirs(PKGDIR, exist_ok=True)
        run_commands(recipe["install"]["commands"], src_dir, log_file, quiet, env)
        run_hooks("post_install", recipe, src_dir, log_file, quiet, env)

    # final hook
    run_hooks("post_all", recipe, src_dir, log_file, quiet, env)

    log_message(f">>>> {pkg} instalado com sucesso! <<<<", log_file, quiet, color="green")
    print(f"Log detalhado: {os.path.abspath(log_file.name)}")

def remove_pipeline(pkg, quiet=False, force=False, recursive=False):
    recipes = deps.load_all_recipes()
    dependents = deps.get_dependents(pkg, recipes)

    log_file = setup_logging(f"{pkg}-remove")
    env = os.environ.copy()

    if dependents and not force and not recursive:
        log_message(f"Erro: {pkg} é requerido por: {dependents}", log_file, quiet, color="red")
        print("Use --force para forçar ou --recursive para remover também os dependentes.")
        return

    # Se --recursive, remove dependentes antes
    if recursive:
        for dep_pkg in dependents:
            remove_pipeline(dep_pkg, quiet=quiet, force=force, recursive=recursive)

    recipe = recipes.get(pkg)
    if not recipe:
        log_message(f"Receita de {pkg} não encontrada.", log_file, quiet, color="red")
        return

    # pre_remove
    run_hooks("pre_remove", recipe, "/", log_file, quiet, env)

    if "remove" in recipe:
        log_message(f">>>> Removendo {pkg} <<<<", log_file, quiet, color="red")
        run_commands(recipe["remove"]["commands"], "/", log_file, quiet, env)

    # post_remove
    run_hooks("post_remove", recipe, "/", log_file, quiet, env)

    log_message(f">>>> {pkg} removido com sucesso! <<<<", log_file, quiet, color="green")
    print(f"Log detalhado: {os.path.abspath(log_file.name)}")

# ========= CLI =========

def main():
    parser = argparse.ArgumentParser(description="ZeroPKG - Gerenciador de build")
    sub = parser.add_subparsers(dest="command")

    build_cmd = sub.add_parser("build", help="Construir pacote")
    build_cmd.add_argument("package", help="Nome do pacote (sem extensão)")
    build_cmd.add_argument("--quiet", action="store_true", help="Modo silencioso")

    remove_cmd = sub.add_parser("remove", help="Remover pacote")
    remove_cmd.add_argument("package", help="Nome do pacote (sem extensão)")
    remove_cmd.add_argument("--quiet", action="store_true", help="Modo silencioso")
    remove_cmd.add_argument("--force", action="store_true", help="Forçar remoção mesmo com dependentes")
    remove_cmd.add_argument("--recursive", action="store_true", help="Remover também pacotes dependentes")

    args = parser.parse_args()

    if args.command == "build":
        build_pipeline(args.package, quiet=args.quiet)
    elif args.command == "remove":
        remove_pipeline(args.package, quiet=args.quiet, force=args.force, recursive=args.recursive)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
