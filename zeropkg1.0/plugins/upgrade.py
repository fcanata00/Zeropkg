# plugins/upgrade.py
import os
from datetime import datetime
from plugins import deps
import core  # Importa o core para usar build_pipeline e remove_pipeline

LOGDIR = "logs"

def setup_upgrade_log(pkg):
    os.makedirs(LOGDIR, exist_ok=True)
    log_path = os.path.join(LOGDIR, f"{pkg}-upgrade.log")
    return open(log_path, "w", encoding="utf-8")

def log_upgrade(msg, log_file, quiet=False, color=None):
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

def upgrade_package(pkg, quiet=False, force=False):
    """
    Atualiza um pacote para a versão mais recente, junto com seus dependentes.
    """
    recipes = deps.load_all_recipes()
    log_file = setup_upgrade_log(pkg)

    if pkg not in recipes:
        log_upgrade(f"Receita não encontrada para {pkg}", log_file, quiet, color="red")
        return

    # 1. Descobrir dependentes do pacote
    dependents = deps.get_dependents(pkg, recipes)

    if dependents:
        log_upgrade(f"Dependentes encontrados: {dependents}", log_file, quiet, color="yellow")
    else:
        log_upgrade("Nenhum dependente encontrado", log_file, quiet, color="blue")

    # 2. Ordem de remoção: dependentes -> pacote alvo
    remove_order = dependents + [pkg]
    log_upgrade(f"Ordem de remoção: {remove_order}", log_file, quiet, color="red")

    for p in remove_order:
        log_upgrade(f">>>> Removendo {p} para upgrade...", log_file, quiet, color="red")
        core.remove_pipeline(p, quiet=quiet, force=True, recursive=False)

    # 3. Ordem de reinstalação: pacote alvo -> dependentes
    install_order = [pkg] + dependents
    log_upgrade(f"Ordem de reinstalação: {install_order}", log_file, quiet, color="green")

    for p in install_order:
        log_upgrade(f">>>> Instalando {p} na versão mais recente...", log_file, quiet, color="green")
        core.build_pipeline(p, quiet=quiet)

    log_upgrade(f">>>> Upgrade de {pkg} concluído com sucesso! <<<<", log_file, quiet, color="green")
    print(f"Log detalhado do upgrade: {os.path.abspath(log_file.name)}")
