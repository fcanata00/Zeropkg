#!/usr/bin/env python3
"""
zeropkg_update.py

Scanner de updates para Zeropkg:
- Verifica upstreams de pacotes (de config interno ou metafile)
- Compara versões instaladas vs upstream
- Gera relatório JSON + notificação em texto
- Integra com zeropkg_config e zeropkg_logger
"""

import os
import re
import json
import requests
import logging
from bs4 import BeautifulSoup
from typing import Dict, Optional, Tuple

from zeropkg_toml import parse_toml
from zeropkg_logger import log_event
from zeropkg_config import get_paths

logger = logging.getLogger("zeropkg.update")

# ----------------------------
# Helpers
# ----------------------------
def fetch_upstream_version(url: str, regex: str) -> Optional[str]:
    """Busca página HTML/texto e extrai versão mais nova via regex."""
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            log_event("update", "fetch", f"Falha HTTP {resp.status_code} em {url}", level="error")
            return None
        matches = re.findall(regex, resp.text)
        if not matches:
            log_event("update", "parse", f"Nenhuma versão encontrada em {url}", level="warning")
            return None
        # Ordena por ordem numérica (maior primeiro)
        versions = sorted(matches, key=lambda v: [int(x) for x in v.split(".") if x.isdigit()], reverse=True)
        return versions[0]
    except Exception as e:
        log_event("update", "error", f"Erro ao buscar {url}: {e}", level="error")
        return None


def classify_update(local_ver: str, upstream_ver: str) -> str:
    """Classifica severidade do update: critical, urgent ou normal."""
    if not local_ver:
        return "normal"
    try:
        lparts = [int(x) for x in local_ver.split(".")]
        uparts = [int(x) for x in upstream_ver.split(".")]
    except ValueError:
        return "normal"

    if uparts[0] > lparts[0]:
        return "critical"
    elif uparts[1] > lparts[1]:
        return "urgent"
    else:
        return "normal"


# ----------------------------
# Scanner principal
# ----------------------------
def scan_updates(ports_dir: Optional[str] = None, dry_run: bool = False) -> Dict:
    """
    Verifica todos os pacotes no ports_dir e busca novas versões em upstream.
    """
    paths = get_paths()
    ports_dir = ports_dir or paths["ports_dir"]

    updates = {"updates": [], "counts": {"critical": 0, "urgent": 0, "normal": 0}}

    for root, _, files in os.walk(ports_dir):
        for f in files:
            if not f.endswith(".toml"):
                continue
            metafile = os.path.join(root, f)
            try:
                meta = parse_toml(metafile)
            except Exception as e:
                log_event("update", "parse", f"Erro lendo {metafile}: {e}", level="error")
                continue

            pkgname, local_ver = meta.name, meta.version
            upstream_url = getattr(meta, "upstream_url", None)
            upstream_regex = getattr(meta, "upstream_regex", None)

            if not upstream_url or not upstream_regex:
                # sem upstream definido → ignora
                continue

            latest = fetch_upstream_version(upstream_url, upstream_regex)
            if not latest or latest == local_ver:
                continue

            sev = classify_update(local_ver, latest)
            updates["updates"].append({
                "name": pkgname,
                "local": local_ver,
                "upstream": latest,
                "severity": sev,
                "metafile": metafile,
            })
            updates["counts"][sev] += 1
            log_event(pkgname, "update", f"{local_ver} → {latest} ({sev})")

    if not dry_run:
        out_json = os.path.join(paths["cache_dir"], "updates.json")
        out_txt = os.path.join(paths["cache_dir"], "update_notify.txt")
        os.makedirs(paths["cache_dir"], exist_ok=True)
        with open(out_json, "w") as jf:
            json.dump(updates, jf, indent=2)
        with open(out_txt, "w") as tf:
            total = sum(updates["counts"].values())
            tf.write(f"{total} updates disponíveis "
                     f"({updates['counts']['critical']} críticos, "
                     f"{updates['counts']['urgent']} urgentes, "
                     f"{updates['counts']['normal']} normais)\n")
        log_event("update", "scan", f"Relatórios escritos em {out_json} e {out_txt}")

    return updates


def run_update_scan(dry_run: bool = False):
    result = scan_updates(dry_run=dry_run)
    total = sum(result["counts"].values())
    print(f"[+] {total} updates encontrados "
          f"({result['counts']['critical']} críticos, "
          f"{result['counts']['urgent']} urgentes, "
          f"{result['counts']['normal']} normais)")
    return result


# ----------------------------
# CLI standalone
# ----------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Scanner de updates Zeropkg")
    ap.add_argument("--dry-run", action="store_true", help="Não escreve arquivos, só mostra resultado")
    args = ap.parse_args()
    run_update_scan(dry_run=args.dry_run)
