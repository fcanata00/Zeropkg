"""
zeropkg_update.py

Módulo responsável por buscar novas versões dos pacotes em seus upstreams,
comparar com o repositório local e gerar relatórios de atualização.

Saídas:
- updates.json: lista detalhada de pacotes com novas versões
- update_notify.txt: resumo para notificações
"""

import os
import json
import logging
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Optional

from zeropkg_toml import parse_toml, PackageMeta
from zeropkg_upgrade import compare_versions

logger = logging.getLogger("zeropkg.update")

PORTS_DIR_DEFAULT = "/usr/ports"
UPDATES_JSON = "/var/lib/zeropkg/updates.json"
NOTIFY_TXT = "/var/lib/zeropkg/update_notify.txt"

# Regras de upstreams: { pacote: url da página }
UPSTREAMS: Dict[str, str] = {
    "gcc": "https://gcc.gnu.org/releases.html",
    "python": "https://www.python.org/ftp/python/",
    "curl": "https://curl.se/download.html",
    # outros pacotes podem ser adicionados aqui
}

def fetch_upstream_version(pkgname: str, url: str) -> Optional[str]:
    """Busca a versão mais recente no upstream definido para o pacote."""
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        html = r.text

        if pkgname == "gcc":
            # GCC: parse releases.html
            soup = BeautifulSoup(html, "html.parser")
            versions = []
            for a in soup.find_all("a"):
                text = a.get_text(strip=True)
                if text and text[0].isdigit():
                    versions.append(text)
            if versions:
                return sorted(versions, key=lambda v: [int(x) for x in v.split(".")])[-1]

        elif pkgname == "python":
            # Python: versão maior no diretório ftp
            soup = BeautifulSoup(html, "html.parser")
            versions = []
            for a in soup.find_all("a"):
                t = a.get_text(strip=True).strip("/")
                if t and t[0].isdigit():
                    versions.append(t)
            if versions:
                return sorted(versions, key=lambda v: [int(x) for x in v.split(".")])[-1]

        elif pkgname == "curl":
            # Curl: procurar versão no HTML
            for line in html.splitlines():
                if "Released" in line and "curl" in line:
                    parts = line.split()
                    for p in parts:
                        if p[0].isdigit():
                            return p

        return None
    except Exception as e:
        logger.error(f"Erro ao buscar upstream de {pkgname}: {e}")
        return None

def classify_update(pkgname: str, old: str, new: str) -> str:
    """Classifica criticidade da atualização (exemplo simplificado)."""
    if old is None:
        return "normal"
    cmp = compare_versions(new, old)
    if cmp <= 0:
        return "none"
    # Exemplo de regra:
    # major upgrade => crítico
    # minor upgrade => urgente
    # patch upgrade => normal
    o_parts = old.split(".")
    n_parts = new.split(".")
    if o_parts[0] != n_parts[0]:
        return "critical"
    elif len(o_parts) > 1 and len(n_parts) > 1 and o_parts[1] != n_parts[1]:
        return "urgent"
    else:
        return "normal"

def scan_updates(ports_dir: str = PORTS_DIR_DEFAULT) -> List[Dict]:
    updates = []
    for pkgname, url in UPSTREAMS.items():
        metafile = os.path.join(ports_dir, pkgname, f"{pkgname}.toml")
        if not os.path.exists(metafile):
            continue
        meta = parse_toml(metafile)
        local_ver = meta.version
        upstream_ver = fetch_upstream_version(pkgname, url)
        if upstream_ver and compare_versions(upstream_ver, local_ver) > 0:
            severity = classify_update(pkgname, local_ver, upstream_ver)
            updates.append({
                "name": pkgname,
                "local": local_ver,
                "upstream": upstream_ver,
                "severity": severity,
                "url": url
            })
    return updates

def generate_reports(updates: List[Dict]) -> None:
    os.makedirs(os.path.dirname(UPDATES_JSON), exist_ok=True)
    with open(UPDATES_JSON, "w") as f:
        json.dump(updates, f, indent=2)

    counts = {"critical": 0, "urgent": 0, "normal": 0}
    for u in updates:
        if u["severity"] in counts:
            counts[u["severity"]] += 1
    total = sum(counts.values())

    with open(NOTIFY_TXT, "w") as f:
        f.write(f"{total} novas atualizações disponíveis\n")
        f.write(f"{counts['critical']} críticas, {counts['urgent']} urgentes, {counts['normal']} normais\n")

    logger.info(f"Atualizações: {total} (crit:{counts['critical']} urg:{counts['urgent']} norm:{counts['normal']})")

def run_update_scan(ports_dir: str = PORTS_DIR_DEFAULT) -> None:
    updates = scan_updates(ports_dir)
    generate_reports(updates)
