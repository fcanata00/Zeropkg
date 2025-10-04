#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_update.py — check upstream for new versions of packages in ports
Pattern B: integrated, lean, functional.

Public API:
- check_updates(report:bool=True, dry_run:bool=False) -> dict (summary + details)
- get_last_updates() -> dict
- diff_updates(old:dict, new:dict) -> dict
- record_update_event(pkg, old_ver, new_ver, severity, note="", db=True, log=True)
"""

from __future__ import annotations
import os
import sys
import json
import time
import re
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Try optional requests for nicer HTTP; fallback to urllib
try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except Exception:
    import urllib.request as _urllib  # type: ignore
    _HAS_REQUESTS = False

# Integrations (optional)
try:
    from zeropkg_config import load_config, get_ports_dirs
except Exception:
    def load_config(*a, **k):
        return {"paths": {"ports_dir": "/usr/ports", "db_path": "/var/lib/zeropkg/installed.sqlite3", "state_dir": "/var/lib/zeropkg"}, "update": {"enabled": True}}
    def get_ports_dirs(cfg=None):
        return ["/usr/ports"]

try:
    from zeropkg_logger import log_event, log_global, get_logger
    _logger = get_logger("update")
except Exception:
    import logging
    _logger = logging.getLogger("zeropkg_update")
    if not _logger.handlers:
        _logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(_logger, level if hasattr(_logger, level) else "info")(msg)

try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# state paths
_DEFAULT_STATE_DIR = "/var/lib/zeropkg"
_DEFAULT_UPDATES_FILE = "updates.json"
_DEFAULT_NOTIFY_FILE = "update_notify.txt"

# http defaults
_HTTP_TIMEOUT = 12  # seconds
_HTTP_RETRIES = 2

# semver basic regex
_SEMVER_RE = re.compile(r"v?(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:[-+].*)?$")

# keywords that increase severity
_SECURITY_KEYWORDS = ("security", "CVE", "vuln", "critical", "fixes", "patch")


# -----------------------
# Utilities
# -----------------------
def _state_dir(cfg: Optional[Dict[str, Any]] = None) -> str:
    cfg = cfg or load_config()
    sd = cfg.get("paths", {}).get("state_dir") or _DEFAULT_STATE_DIR
    Path(sd).mkdir(parents=True, exist_ok=True)
    return sd

def _updates_file(cfg: Optional[Dict[str, Any]] = None) -> str:
    return os.path.join(_state_dir(cfg), _DEFAULT_UPDATES_FILE)

def _notify_file(cfg: Optional[Dict[str, Any]] = None) -> str:
    return os.path.join(_state_dir(cfg), _DEFAULT_NOTIFY_FILE)

def _http_get(url: str, timeout: int = _HTTP_TIMEOUT) -> Tuple[int, bytes]:
    """
    Return (status_code, content_bytes) or raise.
    """
    if _HAS_REQUESTS:
        last_exc = None
        for i in range(_HTTP_RETRIES):
            try:
                r = requests.get(url, timeout=timeout, allow_redirects=True)
                r.raise_for_status()
                return r.status_code, r.content
            except Exception as e:
                last_exc = e
                time.sleep(0.5 * (i + 1))
        raise last_exc
    else:
        last_exc = None
        for i in range(_HTTP_RETRIES):
            try:
                with _urllib.urlopen(url, timeout=timeout) as resp:
                    data = resp.read()
                    code = getattr(resp, "status", 200)
                    return code, data
            except Exception as e:
                last_exc = e
                time.sleep(0.5 * (i + 1))
        raise last_exc

def _http_head(url: str, timeout: int = _HTTP_TIMEOUT) -> Tuple[int, dict]:
    """
    Return (status_code, headers) - minimal HEAD attempt with fallback to GET.
    """
    if _HAS_REQUESTS:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code, dict(r.headers)
    else:
        req = _urllib.Request(url, method="HEAD")
        with _urllib.urlopen(req, timeout=timeout) as resp:
            headers = dict(resp.getheaders())
            status = getattr(resp, "status", 200)
            return status, headers

def _parse_semver(ver: str) -> Optional[Tuple[int,int,int]]:
    if not ver:
        return None
    m = _SEMVER_RE.search(ver.strip())
    if not m:
        return None
    try:
        return int(m.group("major")), int(m.group("minor")), int(m.group("patch"))
    except Exception:
        return None

def _score_severity(old_ver: Optional[str], new_ver: str, meta_entry: Dict[str,Any]) -> str:
    """
    Heuristic severity: 'critical' if major increases or security keywords matched,
    'urgent' if minor increases or patch includes security mention,
    else 'normal'.
    """
    # check textual hints in meta
    description = " ".join([
        str(meta_entry.get("package", {}).get("name", "")),
        str(meta_entry.get("package", {}).get("version", "")),
        str(meta_entry.get("build", {}).get("commands", ""))
    ]).lower()
    for key in _SECURITY_KEYWORDS:
        if key.lower() in description or key.lower() in str(new_ver).lower():
            return "critical"
    old_v = _parse_semver(old_ver or "")
    new_v = _parse_semver(new_ver or "")
    if old_v and new_v:
        if new_v[0] > old_v[0]:
            return "critical"
        if new_v[1] > old_v[1]:
            return "urgent"
        if new_v[2] > old_v[2]:
            return "normal"
    # fallback: if string changed drastically (length difference), mark urgent
    if old_ver and len(new_ver) - len(old_ver) > 3:
        return "urgent"
    return "normal"


# -----------------------
# Storage: read/write last updates
# -----------------------
def get_last_updates(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    path = _updates_file(cfg)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_updates(data: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None, dry_run: bool = False):
    path = _updates_file(cfg)
    if dry_run:
        log_global(f"[dry-run] would write updates summary to {path}")
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# -----------------------
# Diff logic
# -----------------------
def diff_updates(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare old and new updates dicts and return only changed entries.
    Structure used: { pkg_name: { "old": ver, "new": ver, "meta": {...} } }
    """
    diffs: Dict[str, Any] = {}
    old_pkgs = old.get("packages", {}) if isinstance(old, dict) else {}
    new_pkgs = new.get("packages", {}) if isinstance(new, dict) else {}
    for pkg, info in new_pkgs.items():
        new_ver = info.get("version")
        old_ver = old_pkgs.get(pkg, {}).get("version")
        if new_ver and new_ver != old_ver:
            diffs[pkg] = {"old": old_ver, "new": new_ver, "meta": info.get("meta", {})}
    return diffs


# -----------------------
# Recording events
# -----------------------
def record_update_event(pkg: str, old_ver: Optional[str], new_ver: str, severity: str, note: str = "", db: bool = True, log: bool = True):
    ts = int(time.time())
    msg = f"Update detected: {pkg} {old_ver or '(none)'} -> {new_ver} [severity={severity}] {note}"
    if log:
        log_event(pkg, "update.detect", msg, level="info" if severity in ("normal","urgent") else "warning")
    if db and DBManager:
        try:
            with DBManager() as dbm:
                payload = {"old": old_ver, "new": new_ver, "severity": severity, "note": note}
                dbm._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                             (pkg, "update.detect", json.dumps(payload), ts))
        except Exception:
            # do not fail on DB logging
            log_event(pkg, "update", f"Failed to record update in DB: {traceback.format_exc()}", level="warning")


# -----------------------
# Upstream check strategies
# -----------------------
def _guess_upstream_urls(meta_entry: Dict[str,Any]) -> List[str]:
    """
    Given a recipe meta (from zeropkg_toml), attempt to produce candidate upstream URLs:
     - any source URLs (archives)
     - common project pages built from package name (heuristic)
     - repo URL if present in meta
    """
    urls = []
    # sources
    for s in meta_entry.get("sources", []) or []:
        u = s.get("url")
        if u:
            urls.append(u)
    # if package block has "upstream" or "upstream_url"
    p = meta_entry.get("package", {}) or {}
    up = p.get("upstream") or p.get("upstream_url") or meta_entry.get("upstream")
    if up:
        urls.append(up)
    # common heuristics for named upstreams (gcc -> https://gcc.gnu.org)
    name = p.get("name") or ""
    if name:
        heuristics = [
            f"https://{name}.gnu.org",
            f"https://{name}.org",
            f"https://{name}.sourceforge.net",
            f"https://{name}.github.io",
            f"https://github.com/{name}/{name}",
            f"https://github.com/{name}"
        ]
        urls.extend(heuristics)
    # deduplicate preserving order
    seen = set()
    out = []
    for u in urls:
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _probe_for_version(url: str, meta_entry: Dict[str,Any]) -> Optional[str]:
    """
    Try probing the url for an upstream version string.
    Strategies:
      - If url points to an archive with version in filename -> extract via regex
      - If url is a github repo -> try tags via /tags or /releases/latest redirect
      - Query HEAD for Content-Disposition or filename
      - GET and search for `<version>` patterns on page (simple heuristic)
    Returns string if found, else None.
    """
    # first: if url ends with something like name-X.Y.Z.tar.xz, parse
    m = re.search(r"([0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z]+)?)", url)
    if m:
        return m.group(1)

    # attempt HEAD for redirect to latest (github)
    try:
        if "github.com" in url and "/releases/latest" not in url:
            # try releases/latest
            candidate = url.rstrip("/") + "/releases/latest"
            try:
                code, content = _http_get(candidate)
                # if requests followed redirects, content may be html; try final url from requests
                if _HAS_REQUESTS:
                    # try real HEAD to get final URL
                    r = requests.get(candidate, timeout=_HTTP_TIMEOUT, allow_redirects=True)
                    final = r.url
                    m2 = re.search(r"/tag/v?([^/]+)$", final)
                    if m2:
                        return m2.group(1)
                else:
                    # fallback GET parse
                    decoded = content.decode(errors="ignore")
                    m2 = re.search(r"/tag/v?([^\"/]+)\"", decoded)
                    if m2:
                        return m2.group(1)
            except Exception:
                pass

        # HEAD to the url
        try:
            code, headers = _http_head(url)
            # content-disposition check
            cd = headers.get("Content-Disposition") or headers.get("content-disposition")
            if cd:
                mcd = re.search(r"filename=.*?([0-9]+\.[0-9]+\.[0-9]+[^\s;\"']*)", cd)
                if mcd:
                    return mcd.group(1)
        except Exception:
            pass

        # GET and naive parse for version-looking tokens
        try:
            code, content = _http_get(url)
            text = content.decode(errors="ignore")
            # search for <title> or tags containing version-like strings
            mtitle = re.search(r">[^<]{0,80}([0-9]+\.[0-9]+\.[0-9]+[^<\s]*)[^<]*<", text)
            if mtitle:
                return mtitle.group(1)
            # fallback: first semver-ish occurrence
            m = re.search(r"v?([0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.]+)?)", text)
            if m:
                return m.group(1)
        except Exception:
            pass
    except Exception:
        return None
    return None


# -----------------------
# Main check loop
# -----------------------
def _collect_ports_meta(cfg: Optional[Dict[str,Any]] = None) -> Dict[str, Dict[str,Any]]:
    """
    Discover recipes in ports directories and return mapping:
      pkg_fullname -> meta_entry
    Expects that each port directory has toml files named like pkg-version.toml or provides a 'recipe.json' etc.
    This function is intentionally conservative: it will look for .toml files under ports_dir/*/*.toml
    """
    cfg = cfg or load_config()
    ports_dirs = get_ports_dirs(cfg)
    out: Dict[str, Dict[str,Any]] = {}
    for pd in ports_dirs:
        try:
            for root, dirs, files in os.walk(pd):
                for fn in files:
                    if fn.endswith(".toml"):
                        full = os.path.join(root, fn)
                        try:
                            # lazy import to avoid circular issues
                            from zeropkg_toml import load_toml, get_package_meta
                            meta = load_toml(full)
                            name, ver = get_package_meta(meta)
                            key = f"{name}"
                            out[key] = meta
                        except Exception:
                            # skip invalid toml
                            continue
        except Exception:
            continue
    return out


def check_updates(report: bool = True, cfg: Optional[Dict[str,Any]] = None, dry_run: bool = False) -> Dict[str,Any]:
    """
    Top-level: scans ports, probes upstreams, computes diffs against saved state and writes notification summary.
    Returns a dict with 'summary' and 'details'.
    If dry_run True, no writes made (no updates.json, no notify file, no DB events).
    """
    cfg = cfg or load_config()
    state_dir = _state_dir(cfg)
    # collect recipes
    metas = _collect_ports_meta(cfg)
    _logger.debug(f"Found {len(metas)} recipes to check")
    scanned = {}
    errors = {}
    for pkg, meta in sorted(metas.items()):
        try:
            candidates = _guess_upstream_urls(meta)
            found_ver = None
            found_url = None
            for cand in candidates:
                try:
                    ver = _probe_for_version(cand, meta)
                    if ver:
                        found_ver = ver
                        found_url = cand
                        break
                except Exception:
                    continue
            # fallback: try sources' urls again by GET
            if not found_ver:
                for s in meta.get("sources", []) or []:
                    try:
                        u = s.get("url")
                        if not u:
                            continue
                        v = _probe_for_version(u, meta)
                        if v:
                            found_ver = v
                            found_url = u
                            break
                    except Exception:
                        continue
            scanned[pkg] = {"version": found_ver, "checked_at": int(time.time()), "source": found_url, "meta": {"package": meta.get("package", {}), "notes": ""}}
        except Exception as e:
            errors[pkg] = str(e)
            _logger.debug(f"Error checking {pkg}: {e}")

    # load previous state
    prev = get_last_updates(cfg)
    new_state = {"ts": int(time.time()), "packages": scanned}
    diffs = diff_updates(prev, new_state)
    # process diffs: classify severity and record events
    summary = {"total_checked": len(scanned), "updates_found": len(diffs), "critical": 0, "urgent": 0, "normal": 0}
    details = {}
    for pkg, change in diffs.items():
        old = change.get("old")
        newv = change.get("new")
        meta = change.get("meta", {}) or {}
        sev = _score_severity(old, newv, meta)
        summary.setdefault(sev, 0)
        if sev == "critical":
            summary["critical"] += 1
        elif sev == "urgent":
            summary["urgent"] += 1
        else:
            summary["normal"] += 1
        details[pkg] = {"old": old, "new": newv, "severity": sev, "meta": meta}
        if not dry_run:
            record_update_event(pkg, old, newv, sev, note=f"detected via zeropkg_update", db=True, log=True)

    # write new state and notify summary (unless dry_run)
    if not dry_run:
        _write_updates(new_state, cfg, dry_run=dry_run)
        # write notify file
        notify_path = _notify_file(cfg)
        lines = []
        total_updates = len(diffs)
        lines.append(f"{total_updates} new updates available")
        lines.append(f"critical: {summary['critical']}  urgent: {summary['urgent']}  normal: {summary['normal']}")
        lines.append("")
        for pkg, info in details.items():
            lines.append(f"{pkg}: {info['old']} -> {info['new']} ({info['severity']})")
        try:
            with open(notify_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
        except Exception:
            log_global(f"Failed to write notify file {notify_path}", "warning")

    # prepare return payload
    result = {"summary": summary, "details": details, "errors": errors, "prev_count": len(prev.get("packages", {}) if isinstance(prev, dict) else {}), "new_count": len(scanned)}
    if report:
        # log summary at top-level
        log_global(f"Update check: {summary['updates_found']} updates found ({summary['critical']} critical, {summary['urgent']} urgent, {summary['normal']} normal)")
    return result


# -----------------------
# CLI helper (quick)
# -----------------------
if __name__ == "__main__":
    import argparse, pprint
    p = argparse.ArgumentParser(prog="zeropkg-update", description="Check upstream for package updates")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-report", dest="report", action="store_false")
    args = p.parse_args()
    try:
        res = check_updates(report=args.report, dry_run=args.dry_run)
        pprint.pprint(res)
        sys.exit(0)
    except Exception as e:
        print("Error:", e)
        traceback.print_exc()
        sys.exit(1)
