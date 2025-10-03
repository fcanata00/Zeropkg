#!/bin/bash
# Instalador completo do Zeropkg
# Cria diretórios, instala módulos e binário, e verifica dependências e repositórios

set -e

MODULES_DIR="/usr/lib/zeropkg/modules"
BIN_PATH="/usr/bin/zeropkg"
DB_DIR="/var/lib/zeropkg"
PKG_CACHE="/usr/ports/distfiles"
PKG_BUILD="/var/zeropkg/build"
PKG_LOG="/var/log/zeropkg"
CONFIG_DIR="/etc/zeropkg"
CONFIG_FILE="$CONFIG_DIR/config.toml"
PORTS_DIR="/usr/ports"
MAIN_REPO="https://github.com/fcanata00/Zeropkg-Ports.git"

echo "[*] Verificando permissões..."
if [[ $EUID -ne 0 ]]; then
    echo "[!] Este script deve ser executado como root."
    echo "    Use: sudo $0"
    exit 1
fi

echo "[*] Verificando dependências do sistema..."
DEPS=("python3" "pip3" "sqlite3" "fakeroot" "wget" "git" "patch" "tar")

for dep in "${DEPS[@]}"; do
    if ! command -v "$dep" >/dev/null 2>&1; then
        echo "[!] Dependência faltando: $dep"
        echo "    Instale com: sudo apt install $dep   (ou equivalente na sua distro)"
        exit 1
    fi
done

echo "[*] Conferindo bibliotecas Python..."
PY_DEPS=("requests" "beautifulsoup4" "tomli")

for pkg in "${PY_DEPS[@]}"; do
    if ! python3 -m pip show "$pkg" >/dev/null 2>&1; then
        echo "[!] Biblioteca Python ausente: $pkg"
        python3 -m pip install "$pkg"
    fi
done

echo "[*] Criando diretórios necessários..."
mkdir -p "$MODULES_DIR" "$DB_DIR" "$PKG_CACHE" "$PKG_BUILD" "$PKG_LOG" "$CONFIG_DIR" "$PORTS_DIR"
touch "$MODULES_DIR/__init__.py"

echo "[*] Copiando módulos..."
if [ -d "zeropkg/modules" ]; then
    cp -u zeropkg/modules/*.py "$MODULES_DIR/"
else
    echo "[!] Diretório zeropkg/modules não encontrado no local atual."
    echo "    Execute este script a partir da raiz do projeto Zeropkg."
    exit 1
fi

echo "[*] Criando executável $BIN_PATH ..."
cat > "$BIN_PATH" <<'EOF'
#!/usr/bin/env python3
import sys
sys.path.insert(0, "/usr/lib/zeropkg/modules")
from zeropkg_cli import main
if __name__ == "__main__":
    main()
EOF

chmod +x "$BIN_PATH"

# Configuração padrão
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "[*] Criando configuração padrão em $CONFIG_FILE ..."
    cat > "$CONFIG_FILE" <<'CFG'
[paths]
db_path = "/var/lib/zeropkg/installed.sqlite3"
ports_dir = "/usr/ports"
build_root = "/var/zeropkg/build"
cache_dir = "/usr/ports/distfiles"
packages_dir = "/var/zeropkg/packages"

[options]
jobs = 4
fakeroot = true
chroot_enabled = true
auto_clean = true
log_level = "INFO"

[repos]
main = "https://github.com/fcanata00/Zeropkg-Ports.git"
testing = "https://github.com/fcanata00/Zeropkg-Testing.git"
local = "/mnt/repos/zeropkg-local"

[network]
proxy = ""
verify_ssl = true
retries = 3
timeout = 30

[lfs]
root = "/mnt/lfs"
chroot_shell = "/bin/bash"
user = "lfs"
group = "lfs"
CFG
else
    echo "[=] Configuração existente detectada, mantendo $CONFIG_FILE"
fi

# Clonar repositório principal se /usr/ports estiver vazio
if [ ! "$(ls -A $PORTS_DIR 2>/dev/null)" ]; then
    echo "[*] Nenhum repositório de ports detectado."
    echo "[*] Clonando repositório principal do Zeropkg..."
    git clone --depth 1 "$MAIN_REPO" "$PORTS_DIR"
else
    echo "[=] Diretório /usr/ports já contém arquivos, ignorando clone inicial."
fi

echo
echo "[✓] Zeropkg instalado com sucesso!"
echo "---------------------------------------"
echo "Executável:    $BIN_PATH"
echo "Módulos:       $MODULES_DIR"
echo "Configuração:  $CONFIG_FILE"
echo "Repositório:   $PORTS_DIR"
echo
echo "Comandos úteis:"
echo "  zeropkg --help"
echo "  zeropkg sync"
echo "  zeropkg build binutils"
echo "  zeropkg install binutils"
echo "---------------------------------------"
echo "Pronto para começar a construir o Linux From Scratch!"
