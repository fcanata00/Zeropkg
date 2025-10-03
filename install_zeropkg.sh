#!/bin/bash
# Instalador do Zeropkg
# Instala os módulos em /usr/lib/zeropkg/modules
# Cria o binário zeropkg em /usr/bin
# Confere dependências

set -e

MODULES_DIR="/usr/lib/zeropkg/modules"
BIN_PATH="/usr/bin/zeropkg"
DB_DIR="/var/lib/zeropkg"
PKG_CACHE="/var/zeropkg/packages"

echo "[*] Verificando dependências do sistema..."

# Dependências obrigatórias
DEPS=("python3" "pip3" "sqlite3" "fakeroot" "wget" "tar")

for dep in "${DEPS[@]}"; do
    if ! command -v "$dep" >/dev/null 2>&1; then
        echo "[!] Dependência faltando: $dep"
        echo "    Instale com: sudo apt install $dep   (ou equivalente na sua distro)"
        exit 1
    fi
done

# Bibliotecas Python necessárias
echo "[*] Conferindo bibliotecas Python..."
PY_DEPS=("requests" "beautifulsoup4" "tomli")

for pkg in "${PY_DEPS[@]}"; do
    if ! python3 -m pip show "$pkg" >/dev/null 2>&1; then
        echo "[!] Biblioteca Python ausente: $pkg"
        echo "    Instalando..."
        sudo python3 -m pip install "$pkg"
    fi
done

echo "[*] Criando diretórios..."
sudo mkdir -p "$MODULES_DIR"
sudo mkdir -p "$DB_DIR"
sudo mkdir -p "$PKG_CACHE"

echo "[*] Copiando módulos..."
# Supondo que você esteja rodando este script na raiz do projeto Zeropkg
sudo cp zeropkg/modules/*.py "$MODULES_DIR/"
sudo touch "$MODULES_DIR/__init__.py"

echo "[*] Criando executável $BIN_PATH ..."
sudo tee "$BIN_PATH" > /dev/null <<'EOF'
#!/usr/bin/env python3
import sys
sys.path.insert(0, "/usr/lib/zeropkg/modules")
from zeropkg_cli import main

if __name__ == "__main__":
    main()
EOF

sudo chmod +x "$BIN_PATH"

echo "[*] Zeropkg instalado com sucesso!"
echo "    Executável: $BIN_PATH"
echo "    Módulos: $MODULES_DIR"
echo
echo "Você pode rodar agora: zeropkg --help"
