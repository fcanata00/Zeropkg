#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_sync.py — synchronize git-backed ports repositories for Zeropkg
Pattern B: integrated, lean, functional.

Public API:
- list_repos(cfg=None) -> list of {name, path, url}
- check_repo_status(repo_path) -> dict {ok, branch, commit, dirty, behind, ahead}
- sync_repos(name=None, cfg=None, dry_run=False, force=False, update_submodules=True) -> dict of results
"""

from __future__ import annotations
import os
import sys
import subprocess
import shlex
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# optional integrations
try:
    from zeropkg_config import load_config, get_ports_dirs
except Exception:
    def load_config(*a, **k):
        return {"paths": {"ports_dir": "/usr/ports"}, "repos": []}
    def get_ports_dirs(cfg=None):
        p = (cfg or {}).get("paths", {}).get("ports_dir", "/usr/ports")
        return [p]

try:
    from zeropkg_logger import log_event, log_global, get_logger
    logger = get_logger("sync")
except Exception:
    import logging
    logger = logging.getLogger("zeropkg_sync")
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    def log_event(pkg, stage, msg, level="info"):
        getattr(logger, level if hasattr(logger, level) else "info")(f"{pkg}:{stage} {msg}")
    def log_global(msg, level="info"):
        getattr(logger, level if hasattr(logger, level) else "info")(msg)

try:
    from zeropkg_db import DBManager
except Exception:
    DBManager = None

# ---------- helpers ----------
def _run(cmd: List[str], cwd: Optional[str] = None, capture: bool = True, check: bool = False, env: Optional[Dict[str,str]] = None) -> Tuple[int, str, str]:
    """Run a command, return (rc, stdout, stderr)."""
    try:
        proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE if capture else None,
                              stderr=subprocess.PIPE if capture else None, env=env, check=check, text=True)
        out = proc.stdout if proc.stdout is not None else ""
        err = proc.stderr if proc.stderr is not None else ""
        return proc.returncode, out.strip(), err.strip()
    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout.strip() if e.stdout else "", e.stderr.strip() if e.stderr else ""
    except FileNotFoundError:
        raise RuntimeError(f"Command not found: {cmd[0]}")

def _is_git_available() -> bool:
    try:
        rc, _, _ = _run(["git", "--version"])
        return rc == 0
    except Exception:
        return False

def _safe_resolve(p: str) -> str:
    return str(Path(p).expanduser().resolve())

def _record_db_event(action: str, repo_name: str, payload: Dict[str, Any]):
    if not DBManager:
        return
    try:
        with DBManager() as db:
            db._execute("INSERT INTO events (pkg_name, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                        (repo_name, f"sync.{action}", json.dumps(payload), int(__import__("time").time())))
    except Exception:
        # do not fail sync on DB logging errors
        pass

def _validate_repo_url(url: str) -> bool:
    # simple policy: allow https or git+https only by default
    if url.startswith("https://") or url.startswith("git+https://") or url.startswith("ssh://") or url.startswith("git@"):
        return True
    return False

# ---------- core functions ----------
def list_repos(cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
    """
    Discover repositories from config:
    - cfg['repos'] if present (list of dicts with 'path' and 'url' optionally)
    - otherwise, discovers subdirectories under ports_dir
    Returns list of {name, path, url}
    """
    cfg = cfg or load_config()
    out: List[Dict[str,str]] = []

    # first, explicit repos in config
    repos_cfg = cfg.get("repos", []) or []
    for r in repos_cfg:
        path = r.get("path")
        url = r.get("remote") or r.get("url") or r.get("git")
        name = r.get("name") or (Path(path).name if path else None)
        if path:
            out.append({"name": name or str(path), "path": _safe_resolve(path), "url": url or ""})

    # fall back: list directories under ports_dir
    ports_dirs = get_ports_dirs(cfg)
    for ports_dir in ports_dirs:
        try:
            for entry in sorted(os.listdir(ports_dir)):
                p = os.path.join(ports_dir, entry)
                if os.path.isdir(p):
                    # if already listed via config, skip
                    if any(_safe_resolve(p) == _safe_resolve(r["path"]) for r in out):
                        continue
                    # attempt to read a .git or remote origin
                    url = ""
                    git_dir = os.path.join(p, ".git")
                    if os.path.isdir(git_dir):
                        # try to read origin url
                        try:
                            rc, out_str, err = _run(["git", "config", "--get", "remote.origin.url"], cwd=p)
                            if rc == 0 and out_str:
                                url = out_str.strip()
                        except Exception:
                            url = ""
                    out.append({"name": entry, "path": _safe_resolve(p), "url": url})
        except Exception:
            continue
    return out

def check_repo_status(repo_path: str) -> Dict[str, Any]:
    """
    Check repository status: branch, commit, dirty, ahead/behind counts.
    Returns dict with fields: ok(bool), branch, commit, dirty(bool), ahead(int), behind(int), message
    """
    repo = _safe_resolve(repo_path)
    if not os.path.isdir(repo):
        return {"ok": False, "message": f"Path not found: {repo}"}
    if not _is_git_available():
        return {"ok": False, "message": "git not available on PATH"}

    # ensure it's a git repo
    rc, _, _ = _run(["git", "rev-parse", "--is-inside-work-tree"], cwd=repo)
    if rc != 0:
        return {"ok": False, "message": "Not a git repository"}

    result = {"ok": True, "branch": None, "commit": None, "dirty": False, "ahead": 0, "behind": 0, "message": ""}

    # branch
    rc, branch, _ = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo)
    if rc == 0:
        result["branch"] = branch
    # commit
    rc, commit, _ = _run(["git", "rev-parse", "HEAD"], cwd=repo)
    if rc == 0:
        result["commit"] = commit
    # dirty
    rc, status_out, _ = _run(["git", "status", "--porcelain"], cwd=repo)
    result["dirty"] = bool(status_out.strip())
    # ahead/behind relative to origin/branch
    if result["branch"]:
        # fetch remote refs lightly (no change)
        _run(["git", "fetch", "--all", "--prune"], cwd=repo)
        rc, out_merge, _ = _run(["git", "rev-list", "--left-right", "--count", f"origin/{result['branch']}...{result['branch']}"], cwd=repo)
        if rc == 0 and out_merge:
            try:
                behind, ahead = map(int, out_merge.split())
                result["behind"] = behind
                result["ahead"] = ahead
            except Exception:
                pass

    return result

def _git_pull(repo_path: str, force: bool = False, update_submodules: bool = True, dry_run: bool = False) -> Dict[str, Any]:
    repo = _safe_resolve(repo_path)
    res: Dict[str, Any] = {"path": repo, "pulled": False, "message": "", "rc": 0}
    if dry_run:
        log_global(f"[dry-run] would pull {repo}")
        res["message"] = "dry-run"
        return res

    # attempt git pull
    try:
        # ensure no local modifications unless force
        status = check_repo_status(repo)
        if status.get("dirty") and not force:
            res["message"] = "local modifications present; skipping (use force=True to override)"
            res["rc"] = 2
            return res

        # prefer 'git pull --ff-only' to avoid merges
        rc, out, err = _run(["git", "pull", "--ff-only"], cwd=repo, capture=True)
        res["rc"] = rc
        if rc == 0:
            res["pulled"] = True
            res["message"] = out or "up-to-date"
        else:
            # try fallback: git pull (may create merge)
            rc2, out2, err2 = _run(["git", "pull"], cwd=repo, capture=True)
            res["rc"] = rc2
            if rc2 == 0:
                res["pulled"] = True
                res["message"] = out2 or "pulled (merged)"
            else:
                res["message"] = err2 or err or f"git pull failed rc={rc2}"
        # optionally update submodules
        if update_submodules:
            _run(["git", "submodule", "update", "--init", "--recursive"], cwd=repo)
    except Exception as e:
        res["rc"] = 1
        res["message"] = str(e)
    return res

def sync_repos(name: Optional[str] = None, cfg: Optional[Dict[str,Any]] = None, dry_run: bool = False, force: bool = False, update_submodules: bool = True) -> Dict[str,Any]:
    """
    Sync a single repo by name or all repos defined/discovered.
    Returns dict mapping repo_name -> result dict.
    """
    cfg = cfg or load_config()
    if not _is_git_available():
        raise RuntimeError("git not available; cannot sync repositories")

    results: Dict[str, Any] = {}
    repos = list_repos(cfg)
    # filter by name if given (match exact name or path basename)
    if name:
        repos = [r for r in repos if r["name"] == name or Path(r["path"]).name == name]
        if not repos:
            raise RuntimeError(f"Repository '{name}' not found in config or ports dir")

    for r in repos:
        repo_name = r.get("name") or Path(r.get("path")).name
        repo_path = r.get("path")
        repo_url = r.get("url") or ""
        log_event(repo_name, "sync", f"Starting sync for {repo_path} (url={repo_url})")
        # validate remote URL if present
        if repo_url and not _validate_repo_url(repo_url):
            msg = f"remote url {repo_url} not allowed by policy"
            log_event(repo_name, "sync", msg, level="warning")
            results[repo_name] = {"ok": False, "message": msg}
            _record_db_event("sync.skip", repo_name, {"path": repo_path, "url": repo_url, "reason": "url_policy"})
            continue
        # if path doesn't exist, try clone if url present
        if not os.path.exists(repo_path) or not os.path.isdir(repo_path):
            if repo_url:
                if dry_run:
                    log_event(repo_name, "sync", f"[dry-run] would clone {repo_url} -> {repo_path}")
                    results[repo_name] = {"ok": True, "cloned": False, "message": "dry-run clone"}
                    continue
                else:
                    try:
                        _run(["git", "clone", repo_url, repo_path])
                        log_event(repo_name, "sync", f"Cloned {repo_url} -> {repo_path}")
                        _record_db_event("clone", repo_name, {"path": repo_path, "url": repo_url})
                    except Exception as e:
                        log_event(repo_name, "sync", f"Clone failed: {e}", level="error")
                        results[repo_name] = {"ok": False, "message": str(e)}
                        _record_db_event("clone.fail", repo_name, {"path": repo_path, "url": repo_url, "error": str(e)})
                        continue
            else:
                msg = "repo path does not exist and no url configured"
                log_event(repo_name, "sync", msg, level="warning")
                results[repo_name] = {"ok": False, "message": msg}
                continue

        # perform pull/update
        try:
            pull_res = _git_pull(repo_path, force=force, update_submodules=update_submodules, dry_run=dry_run)
            results[repo_name] = {"ok": pull_res.get("pulled", False), **pull_res}
            _record_db_event("pull", repo_name, {"path": repo_path, "result": pull_res})
            log_event(repo_name, "sync", f"Sync result: {pull_res.get('message')}")
        except Exception as e:
            results[repo_name] = {"ok": False, "message": str(e)}
            _record_db_event("pull.fail", repo_name, {"path": repo_path, "error": str(e)})
            log_event(repo_name, "sync", f"Sync failed: {e}", level="error")

    return results

# CLI helper
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(prog="zeropkg-sync", description="Sync zeropkg repos")
    parser.add_argument("--name", help="repo name to sync (optional, syncs all if omitted)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--list", action="store_true", help="list discovered repos and exit")
    args = parser.parse_args()
    cfg = load_config()
    if args.list:
        for rp in list_repos(cfg):
            print(json.dumps(rp))
        sys.exit(0)
    try:
        res = sync_repos(name=args.name, cfg=cfg, dry_run=args.dry_run, force=args.force)
        print(json.dumps(res, indent=2))
    except Exception as e:
        print("Error:", e)
        sys.exit(1)
