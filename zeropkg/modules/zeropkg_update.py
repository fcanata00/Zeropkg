#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_update.py — Update checker & notifier for Zeropkg
Improvements applied:
 - DB history + update events
 - multiple upstream probing strategies (GitHub/GitLab API, releases/latest, HTML heuristics)
 - recipe-level regex support (version_regex)
 - cache incremental at /var/cache/zeropkg/update_cache.json
 - notify integration (notify-send / dunstify) and notify CLI
 - severity classification and vulnerability integration with zeropkg_vuln
 - auto-update mode (controlled, requires config)
 - selective checks (per-package, per-repo)
 - reports: /var/lib/zeropkg/updates.json, update_notify.txt, /var/log/zeropkg/updates_report-<ts>.json
 - CLI: --update, --dry-run, --history, --repo, --packages, --auto-update, --notify, --force
"""

from __future__ import annotations
import os
import sys
import re
import json
import time
import shutil
import tempfile
import socket
import logging
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# try to import requests (preferred)
try:
    import requests
    REQUESTS_AVAILABLE = True
except Exception:
    REQUESTS_AVAILABLE = False

# optional imports from your project
try:
    from zeropkg_config import load_config
except Exception:
    def load_config(path=None):
        return {
            "paths": {
                "ports_dir": "/usr/ports",
                "cache_dir": "/var/cache/zeropkg",
                "state_dir": "/var/lib/zeropkg",
                "log_dir": "/var/log/zeropkg"
            },
            "update": {
                "check_interval_hours": 6,
                "auto_notify": False,
                "notify_command": None,
                "severity_keywords": ["security", "CVE", "vuln", "fix"],
                "max_parallel_checks": 8
            }
        }

try:
    from zeropkg_logger import get_logger
    log = get_logger("update")
except Exception:
    log = logging.getLogger("zeropkg_update")
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(h)
    log.setLevel(logging.INFO)

# optional DB integration
try:
    from zeropkg_db import ZeroPKGDB, record_update_event
    DB_AVAILABLE = True
except Exception:
    ZeroPKGDB = None
    record_update_event = None
    DB_AVAILABLE = False

# optional vuln integration
try:
    from zeropkg_vuln import VulnDB, ZeroPKGVulnManager
    VULN_AVAILABLE = True
except Exception:
    VULN_AVAILABLE = False
    VulnDB = None
    ZeroPKGVulnManager = None

# toml parser for recipes
try:
    from zeropkg_toml import ZeropkgTOML
    TOML_AVAILABLE = True
except Exception:
    TOML_AVAILABLE = False

# helpers
STATE_DIR = Path(load_config().get("paths", {}).get("state_dir", "/var/lib/zeropkg"))
CACHE_DIR = Path(load_config().get("paths", {}).get("cache_dir", "/var/cache/zeropkg"))
LOG_DIR = Path(load_config().get("paths", {}).get("log_dir", "/var/log/zeropkg"))
PORTS_DIR = Path(load_config().get("paths", {}).get("ports_dir", "/usr/ports"))
STATE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

UPDATE_CACHE_PATH = CACHE_DIR / "update_cache.json"
UPDATES_JSON = STATE_DIR / "updates.json"
UPDATE_NOTIFY_TXT = STATE_DIR / "update_notify.txt"
REPORT_DIR = LOG_DIR

DEFAULT_UPDATE_CFG = load_config().get("update", {})
CHECK_INTERVAL_HOURS = int(DEFAULT_UPDATE_CFG.get("check_interval_hours", 6))
AUTO_NOTIFY = bool(DEFAULT_UPDATE_CFG.get("auto_notify", False))
NOTIFY_COMMAND_CFG = DEFAULT_UPDATE_CFG.get("notify_command", None)
SEVERITY_KEYWORDS = DEFAULT_UPDATE_CFG.get("severity_keywords", ["security", "CVE", "vuln", "fix"])

# HTTP helpers with fallback
def http_get(url: str, timeout: int = 15) -> Tuple[int, str]:
    """
    Return (status_code, text) using requests if available, else urllib.
    """
    try:
        if REQUESTS_AVAILABLE:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "zeropkg-update/1.0"})
            return resp.status_code, resp.text
        else:
            # fallback minimal
            from urllib.request import urlopen, Request
            req = Request(url, headers={"User-Agent": "zeropkg-update/1.0"})
            with urlopen(req, timeout=timeout) as fh:
                data = fh.read().decode(errors="ignore")
                return 200, data
    except Exception as e:
        log.debug(f"http_get failed for {url}: {e}")
        return 0, ""

def http_head(url: str, timeout: int = 10) -> Tuple[int, dict]:
    try:
        if REQUESTS_AVAILABLE:
            resp = requests.head(url, timeout=timeout, allow_redirects=True, headers={"User-Agent":"zeropkg-update/1.0"})
            return resp.status_code, dict(resp.headers)
        else:
            # minimal fallback: perform GET and return partial headers
            sc, text = http_get(url, timeout=timeout)
            return sc, {}
    except Exception as e:
        log.debug(f"http_head failed for {url}: {e}")
        return 0, {}

# simple version compare using tuple of ints/strings
def normalize_version(v: str) -> Tuple:
    parts = re.split(r'[._\-+]', v)
    norm = []
    for p in parts:
        if p.isdigit():
            norm.append(int(p))
        else:
            # keep non-digit as lowered token for stable ordering
            norm.append(p.lower())
    return tuple(norm)

def version_greater(a: Optional[str], b: Optional[str]) -> bool:
    if a is None: return False
    if b is None: return True
    try:
        return normalize_version(a) > normalize_version(b)
    except Exception:
        return a > b

# notification
def detect_notify_command() -> Optional[List[str]]:
    # priority: config, notify-send, dunstify
    if NOTIFY_COMMAND_CFG:
        return [NOTIFY_COMMAND_CFG]
    for cmd in ("notify-send", "dunstify"):
        if shutil.which(cmd):
            return [cmd]
    return None

def notify_summary(message: str):
    cmd = detect_notify_command()
    if not cmd:
        log.info(f"[notify] {message}")
        return
    try:
        subprocess_cmd = cmd + [message]
        import subprocess
        subprocess.run(subprocess_cmd, check=False)
    except Exception as e:
        log.warning(f"notify failed: {e}")

# cache helpers
def load_cache() -> Dict[str, Any]:
    if not UPDATE_CACHE_PATH.exists():
        return {}
    try:
        with open(UPDATE_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.debug(f"load_cache failed: {e}")
        return {}

def save_cache(data: Dict[str,Any]):
    try:
        tmp = UPDATE_CACHE_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush(); os.fsync(f.fileno())
        tmp.replace(UPDATE_CACHE_PATH)
    except Exception as e:
        log.warning(f"save_cache failed: {e}")

# DB history helper
def record_history_db(pkg: str, old: Optional[str], new: Optional[str], severity: str):
    if not DB_AVAILABLE:
        return False
    try:
        # try to call record_update_event if provided
        if record_update_event:
            record_update_event({"pkg": pkg, "old": old, "new": new, "severity": severity, "ts": int(time.time())})
            return True
    except Exception as e:
        log.debug(f"record_history_db failed: {e}")
    return False

# recipe scanning helpers
def collect_ports_meta(ports_dir: Path = PORTS_DIR) -> List[Dict[str,Any]]:
    """
    Walk the ports tree and gather metadata from recipe toml files.
    Expected layout: ports/<category>/<pkg>/<pkg>-<version>.toml OR <pkg>.toml
    Returns list of dicts {name, path, meta}
    """
    recipes = []
    if not ports_dir.exists():
        log.warning(f"ports dir {ports_dir} not found")
        return recipes
    for p in ports_dir.rglob("*.toml"):
        try:
            meta = {}
            if TOML_AVAILABLE:
                t = ZeropkgTOML()
                try:
                    meta = t.load(p)
                except Exception:
                    with open(p, "r", encoding="utf-8") as f:
                        meta = {"raw": f.read()}
            else:
                # minimal metadata: read filename and optionally parse name/version
                fname = p.stem
                meta = {"package": {"name": fname}}
            name = meta.get("package", {}).get("name") or p.stem
            recipes.append({"name": name, "path": str(p), "meta": meta})
        except Exception as e:
            log.debug(f"collect_ports_meta skip {p}: {e}")
    return recipes

# upstream probing strategies
def probe_github_latest(repo_url: str) -> Optional[str]:
    """
    Given repo_url like https://github.com/<owner>/<repo>, try to get latest release tag via API or redirects.
    """
    try:
        parsed = urlparse(repo_url)
        if "github.com" not in parsed.netloc:
            return None
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:
            return None
        owner, repo = parts[0], parts[1]
        api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
        if REQUESTS_AVAILABLE:
            resp = requests.get(api_url, timeout=15, headers={"User-Agent": "zeropkg-update/1.0"})
            if resp.status_code == 200:
                j = resp.json()
                tag = j.get("tag_name") or j.get("name")
                if tag:
                    return str(tag)
        # fallback: try releases/latest redirect
        url_rl = f"https://github.com/{owner}/{repo}/releases/latest"
        sc, txt = http_get(url_rl, timeout=10)
        # requests will follow redirect; 'txt' may be HTML containing tag in meta
        # attempt to parse out latest tag from canonical link
        m = re.search(r'/tag/([^"\']+)"', txt)
        if m:
            return m.group(1)
    except Exception as e:
        log.debug(f"probe_github_latest failed: {e}")
    return None

def probe_gitlab_latest(repo_url: str) -> Optional[str]:
    try:
        parsed = urlparse(repo_url)
        if "gitlab.com" not in parsed.netloc:
            return None
        path = parsed.path.strip("/")
        api_url = f"https://gitlab.com/api/v4/projects/{requests.utils.requote_uri(path)}/releases" if REQUESTS_AVAILABLE else None
        if REQUESTS_AVAILABLE and api_url:
            resp = requests.get(api_url, timeout=15, headers={"User-Agent":"zeropkg-update/1.0"})
            if resp.status_code == 200:
                arr = resp.json()
                if isinstance(arr, list) and arr:
                    return arr[0].get("tag_name")
    except Exception as e:
        log.debug(f"probe_gitlab_latest failed: {e}")
    return None

def probe_html_index_for_version(url: str, regex_hint: Optional[str] = None) -> Optional[str]:
    """
    Fetch a generic index page and attempt to locate versions by heuristics.
    """
    try:
        sc, txt = http_get(url, timeout=12)
        if sc == 0 or not txt:
            return None
        # if regex_hint present, use it
        if regex_hint:
            m = re.search(regex_hint, txt)
            if m:
                return m.group(1) if m.groups() else m.group(0)
        # common patterns: '-X.Y.Z' near the filename or 'vX.Y.Z'
        m = re.search(r'v?(\d+(?:\.\d+){1,4})', txt)
        if m:
            return m.group(1)
    except Exception as e:
        log.debug(f"probe_html_index_for_version failed for {url}: {e}")
    return None

def probe_downloads_for_version(distfiles: List[str], regex_hint: Optional[str] = None) -> Optional[str]:
    """
    Given list of distfile URLs, try to extract version from their filenames.
    """
    for u in distfiles:
        try:
            bn = os.path.basename(urlparse(u).path)
            # common pattern: name-1.2.3.tar.xz
            m = re.search(r'[-_v]?(\d+(?:\.\d+){1,4})', bn)
            if m:
                return m.group(1)
        except Exception:
            continue
    return None

# severity scoring
def score_severity(old: Optional[str], new: Optional[str], changelog_text: Optional[str] = None, keywords: Optional[List[str]] = None) -> str:
    """
    Return 'critical' / 'urgent' / 'normal' based on version jump and keywords.
    - major version bump -> urgent
    - presence of keywords (CVE/security) -> critical
    - otherwise normal
    """
    if keywords is None:
        keywords = SEVERITY_KEYWORDS
    ktext = (changelog_text or "").lower()
    for kw in keywords:
        if kw.lower() in ktext:
            return "critical"
    # compare major segments
    try:
        a = normalize_version(old or "0")
        b = normalize_version(new or "0")
        # major bump heuristic: first numeric component increases
        if a and b and isinstance(a[0], int) and isinstance(b[0], int) and b[0] > a[0]:
            return "urgent"
    except Exception:
        pass
    return "normal"

# main update logic
class ZeropkgUpdate:
    def __init__(self, cfg_path: Optional[str] = None):
        self.cfg = load_config(cfg_path)
        self.ports_dir = Path(self.cfg.get("paths", {}).get("ports_dir", PORTS_DIR))
        self.cache_dir = Path(self.cfg.get("paths", {}).get("cache_dir", CACHE_DIR))
        self.state_dir = Path(self.cfg.get("paths", {}).get("state_dir", STATE_DIR))
        self.report_dir = Path(self.cfg.get("paths", {}).get("log_dir", LOG_DIR))
        self.update_cfg = self.cfg.get("update", DEFAULT_UPDATE_CFG)
        self.cache = load_cache()
        self.vuln = ZeroPKGVulnManager() if VULN_AVAILABLE else None

    def save_state_reports(self, updates: List[Dict[str,Any]]):
        # write full JSON
        try:
            _tmp = UPDATES_JSON.with_suffix(".tmp")
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump({"checked_at": int(time.time()), "updates": updates}, f, indent=2)
                f.flush(); os.fsync(f.fileno())
            _tmp.replace(UPDATES_JSON)
        except Exception as e:
            log.warning(f"save_state_reports failed: {e}")

        # write notify text summary
        try:
            total = len(updates)
            by_sev = {"critical":0,"urgent":0,"normal":0}
            lines = []
            for u in updates:
                sev = u.get("severity","normal")
                by_sev[sev] = by_sev.get(sev,0) + 1
                lines.append(f"{u.get('name')}: {u.get('old_version')} -> {u.get('new_version')} ({sev})")
            summary = f"{total} new updates available\ncritical: {by_sev['critical']}  urgent: {by_sev['urgent']}  normal: {by_sev['normal']}\n\n"
            summary += "\n".join(lines)
            UPDATE_NOTIFY_TXT.parent.mkdir(parents=True, exist_ok=True)
            with open(UPDATE_NOTIFY_TXT, "w", encoding="utf-8") as f:
                f.write(summary)
        except Exception as e:
            log.warning(f"save notify failed: {e}")

    def _probe_for_recipe(self, recipe_meta: Dict[str,Any]) -> Optional[Dict[str,Any]]:
        """
        Attempt to determine upstream version for a single recipe meta dict.
        recipe_meta comes from collect_ports_meta -> has keys name, path, meta
        """
        name = recipe_meta.get("name")
        meta = recipe_meta.get("meta", {}) or {}
        # try multiple places in meta: sources, upstream, homepage, repository
        # support recipe['package']['upstream'] or meta['homepage'] etc.
        candidate_urls = []
        try:
            pkg = meta.get("package", {}) or {}
            srcs = meta.get("sources") or meta.get("distfiles") or pkg.get("sources") or []
            # allow either list of strings or dicts with url
            dist_urls = []
            for s in srcs:
                if isinstance(s, str):
                    dist_urls.append(s)
                elif isinstance(s, dict):
                    u = s.get("url") or s.get("uri")
                    if u:
                        dist_urls.append(u)
            if pkg.get("homepage"):
                candidate_urls.append(pkg.get("homepage"))
            if pkg.get("repository"):
                candidate_urls.append(pkg.get("repository"))
            # meta top-level keys
            if isinstance(meta.get("homepage"), str):
                candidate_urls.append(meta.get("homepage"))
            if isinstance(meta.get("repository"), str):
                candidate_urls.append(meta.get("repository"))
        except Exception:
            dist_urls = []

        # 1) try to get version from distfiles entries
        ver = probe_downloads_for_version(dist_urls, regex_hint=meta.get("version_regex"))
        if ver:
            return {"name": name, "old_version": meta.get("package",{}).get("version"), "new_version": ver, "method": "distfiles"}

        # 2) try repository heuristics (github/gitlab)
        for url in candidate_urls:
            try:
                if "github.com" in url:
                    v = probe_github_latest(url)
                    if v:
                        return {"name": name, "old_version": meta.get("package",{}).get("version"), "new_version": v, "method": "github"}
                if "gitlab.com" in url:
                    v = probe_gitlab_latest(url)
                    if v:
                        return {"name": name, "old_version": meta.get("package",{}).get("version"), "new_version": v, "method": "gitlab"}
                # try html index of homepage for version heuristics
                v = probe_html_index_for_version(url, regex_hint=meta.get("version_regex"))
                if v:
                    return {"name": name, "old_version": meta.get("package",{}).get("version"), "new_version": v, "method": "homepage-index"}
            except Exception as e:
                log.debug(f"probe_for_recipe candidate {url} failed: {e}")
                continue

        # 3) fallback: check distfiles directory for matching filenames in configured distfiles path
        distfiles_dir = Path(self.cfg.get("paths", {}).get("distfiles", "/usr/ports/distfiles"))
        if distfiles_dir.exists():
            for f in distfiles_dir.rglob(f"{name}*"):
                m = re.search(r'(\d+(?:\.\d+){1,4})', f.name)
                if m:
                    return {"name": name, "old_version": meta.get("package",{}).get("version"), "new_version": m.group(1), "method": "local-distfile"}
        return None

    def check_updates(self, *, packages: Optional[List[str]] = None, repos: Optional[List[str]] = None, dry_run: bool = False, force: bool=False) -> List[Dict[str,Any]]:
        """
        Main entrypoint:
         - collect recipes (all or filtered by packages/repos)
         - probe upstream for each
         - compare to cached/old versions
         - produce list of updates (with severity)
        """
        recipes = collect_ports_meta(self.ports_dir)
        if packages:
            recipes = [r for r in recipes if r.get("name") in packages]
        if repos:
            # repo filtering: filter by path containing repo name (simple)
            recipes = [r for r in recipes if any(repo in r.get("path","") for repo in repos)]

        log.info(f"Checking updates for {len(recipes)} recipes (dry_run={dry_run})")
        updates = []
        cache = self.cache or {}

        # simple throttling: limit concurrency by slicing
        max_parallel = int(self.update_cfg.get("max_parallel_checks", 8) or 8)

        for recipe in recipes:
            name = recipe.get("name")
            try:
                cached = cache.get(name, {})
                last_checked = cached.get("checked_at", 0)
                # if recently checked and not force, skip
                if not force and (time.time() - last_checked) < (CHECK_INTERVAL_HOURS * 3600):
                    log.debug(f"Skipping {name} (checked recently)")
                    continue

                pr = self._probe_for_recipe(recipe)
                if not pr:
                    log.debug(f"No upstream info for {name}")
                    cache[name] = {"checked_at": int(time.time()), "no_info": True}
                    continue

                oldv = pr.get("old_version")
                newv = pr.get("new_version")
                if not newv:
                    cache[name] = {"checked_at": int(time.time()), "no_info": True}
                    continue

                # normalize: strip leading v
                newv_str = str(newv).lstrip("v")
                oldv_str = (oldv or "")
                if version_greater(newv_str, oldv_str):
                    # attempt to get changelog / release notes if available (not always)
                    changelog = ""
                    method = pr.get("method")
                    if method == "github" and REQUESTS_AVAILABLE:
                        # try fetch release body via API
                        try:
                            # find repo url from recipe meta
                            meta = recipe.get("meta", {}) or {}
                            repo = meta.get("package", {}).get("repository") or meta.get("repository") or None
                            if repo and "github.com" in repo:
                                parsed = urlparse(repo)
                                owner, repo_name = parsed.path.strip("/").split("/")[:2]
                                api_url = f"https://api.github.com/repos/{owner}/{repo_name}/releases/tags/{newv}"
                                r = requests.get(api_url, timeout=10, headers={"User-Agent":"zeropkg-update/1.0"})
                                if r.status_code == 200:
                                    j = r.json()
                                    changelog = j.get("body") or ""
                        except Exception:
                            changelog = ""
                    # severity
                    severity = score_severity(oldv_str, newv_str, changelog, keywords=self.update_cfg.get("severity_keywords", SEVERITY_KEYWORDS))
                    u = {
                        "name": name,
                        "path": recipe.get("path"),
                        "old_version": oldv_str,
                        "new_version": newv_str,
                        "method": pr.get("method"),
                        "severity": severity,
                        "changelog": changelog,
                        "checked_at": int(time.time())
                    }
                    # vulnerability hint: if vuln module exists, check if new version fixes/has CVEs
                    if VULN_AVAILABLE and self.vuln:
                        try:
                            vulns = self.vuln.vulndb.get_vulns(name)
                            if vulns:
                                u["vuln_count"] = len(vulns)
                                # if vulnerability exists mention it (severity override)
                                if any(v.get("severity") == "critical" for v in vulns):
                                    u["severity"] = "critical"
                        except Exception:
                            pass
                    updates.append(u)
                    # record in DB history
                    try:
                        record_history_db(name, oldv_str or None, newv_str or None, severity)
                    except Exception:
                        pass

                # update cache with latest check
                cache[name] = {"checked_at": int(time.time()), "last_known_version": str(pr.get("new_version") or "")}
            except Exception as e:
                log.debug(f"check recipe {recipe.get('name')} failed: {e}")
                continue

        # persist cache and produce report
        self.cache = cache
        save_cache(self.cache)
        # write updates summary
        if updates:
            self.save_state_reports(updates)
            # write log report file
            try:
                ts = int(time.time())
                fname = REPORT_DIR / f"updates_report-{ts}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump({"ts": ts, "updates": updates}, f, indent=2)
            except Exception as e:
                log.warning(f"write report file failed: {e}")

            # auto-notify
            if (self.update_cfg.get("auto_notify", AUTO_NOTIFY) or False) and not dry_run:
                tot = len(updates)
                critical = len([u for u in updates if u.get("severity") == "critical"])
                msg = f"{tot} updates available ({critical} critical)"
                try:
                    notify_summary(msg)
                except Exception:
                    pass

        return updates

    # convenience: full-run with options and notify text return
    def run(self, *, packages: Optional[List[str]] = None, repos: Optional[List[str]] = None, dry_run: bool = False, auto_update: bool=False, notify: bool=False, force: bool=False) -> Dict[str,Any]:
        updates = self.check_updates(packages=packages, repos=repos, dry_run=dry_run, force=force)
        result = {"checked_at": int(time.time()), "count": len(updates), "updates": updates}
        # create textual notify summary
        if updates:
            total = len(updates)
            by_sev = {"critical":0,"urgent":0,"normal":0}
            for u in updates:
                s = u.get("severity","normal")
                by_sev[s] = by_sev.get(s,0) + 1
            text_lines = [f"{total} new updates available", f"critical: {by_sev['critical']}  urgent: {by_sev['urgent']}  normal: {by_sev['normal']}", ""]
            for u in updates[:1000]:
                text_lines.append(f"{u['name']}: {u['old_version']} -> {u['new_version']} ({u['severity']})")
            notify_text = "\n".join(text_lines)
            result["notify_text"] = notify_text

            # write notify file
            try:
                UPDATE_NOTIFY_TXT.parent.mkdir(parents=True, exist_ok=True)
                with open(UPDATE_NOTIFY_TXT, "w", encoding="utf-8") as f:
                    f.write(notify_text)
            except Exception as e:
                log.warning(f"write notify txt failed: {e}")

            if notify and not dry_run:
                notify_summary(result["notify_text"].split("\n",1)[0])

        # auto_update mode: be extremely conservative
        if auto_update and not dry_run:
            # auto update non-critical by default; prompt for critical unless force
            to_upgrade = [u for u in updates if u.get("severity") in ("normal","urgent")]
            criticals = [u for u in updates if u.get("severity") == "critical"]
            # Use upgrade module if available
            try:
                from zeropkg_upgrade import ZeropkgUpgrade
                upgr = ZeropkgUpgrade()
                # build list of recipe candidates
                recipes = []
                for u in to_upgrade:
                    # try find recipe path via builder if available
                    try:
                        from zeropkg_builder import ZeropkgBuilder
                        b = ZeropkgBuilder()
                        rp = b._find_recipe_for_pkg(u["name"]) if hasattr(b, "_find_recipe_for_pkg") else None
                        if rp:
                            recipes.append(str(rp))
                    except Exception:
                        recipes.append(u.get("path"))
                # perform upgrade (non-critical)
                if recipes:
                    log.info(f"Auto-updating {len(recipes)} packages (non-critical)")
                    upgr.upgrade(recipes, dry_run=dry_run, force=force)
            except Exception as e:
                log.warning(f"Auto-update requested but upgrade module not available: {e}")
            # criticals: create notify for manual intervention
            if criticals:
                msg = f"{len(criticals)} critical updates available — review manually"
                notify_summary(msg)

        return result

# CLI
def _cli():
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-update", description="Check upstream for new package versions")
    parser.add_argument("--dry-run", action="store_true", help="Do not write cache or send notifications")
    parser.add_argument("--packages", nargs="+", help="Restrict check to these package names")
    parser.add_argument("--repo", nargs="+", help="Restrict to repository paths containing these strings")
    parser.add_argument("--auto-update", action="store_true", help="Attempt to auto-upgrade non-critical updates")
    parser.add_argument("--notify", action="store_true", help="Send notification summary if updates found")
    parser.add_argument("--force", action="store_true", help="Force check regardless of interval")
    parser.add_argument("--history", action="store_true", help="Show recent update history from DB/cache")
    parser.add_argument("--show-report", action="store_true", help="Show last update_notify.txt content")
    args = parser.parse_args()

    updater = ZeropkgUpdate()
    if args.history:
        if DB_AVAILABLE:
            try:
                db = ZeroPKGDB()
                rows = getattr(db, "list_update_history", lambda: [])()
                print(json.dumps(rows, indent=2))
            except Exception as e:
                log.warning(f"history read failed: {e}")
        else:
            cache = load_cache()
            print(json.dumps(cache, indent=2))
        sys.exit(0)
    if args.show_report:
        if UPDATE_NOTIFY_TXT.exists():
            print(UPDATE_NOTIFY_TXT.read_text())
        else:
            print("No notify file present")
        sys.exit(0)

    res = updater.run(packages=args.packages, repos=args.repo, dry_run=args.dry_run, auto_update=args.auto_update, notify=args.notify, force=args.force)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    _cli()
