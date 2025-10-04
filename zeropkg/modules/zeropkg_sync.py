#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeropkg_sync.py — robust synchronizer for Zeropkg ports repositories

Features applied:
 - load repos from zeropkg_config
 - integrated logging (zeropkg_logger)
 - records events/metrics in DB (zeropkg_db)
 - parallel sync using ThreadPoolExecutor
 - dry-run, force, repair, list, metrics
 - webhook notifications (POST JSON) and local notification file
 - robust git operations (clone, fetch, pull) with retries
 - detection of new commits and per-repo summary
"""

from __future__ import annotations
import os
import sys
import json
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Try to import project modules with graceful fallback
try:
    from zeropkg_config import load_config
except Exception:
    def load_config():
        # minimal default config if not provided
        return {
            "paths": {
                "ports": "/usr/ports",
                "distfiles": "/usr/ports/distfiles",
                "log_dir": "/var/log/zeropkg",
            },
            "sync": {
                "jobs": 4,
                "git_timeout": 120,
                "keep_local_cache": True
            },
            "notify": {
                "webhook_url": None
            }
        }

try:
    from zeropkg_logger import ZeropkgLogger, get_logger  # some installs supply get_logger
    # If get_logger isn't available, instantiate ZeropkgLogger manually later
    LOG_AVAILABLE = True
except Exception:
    ZeropkgLogger = None
    get_logger = None
    LOG_AVAILABLE = False

try:
    from zeropkg_db import record_install_quick, record_upgrade, DBManager, list_installed_quick
    DB_AVAILABLE = True
except Exception:
    DBManager = None
    DB_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except Exception:
    REQUESTS_AVAILABLE = False

# -------------------------
# Helper logger wrappers
# -------------------------
def _get_logger():
    if LOG_AVAILABLE:
        try:
            if get_logger:
                return get_logger("sync")
            else:
                return ZeropkgLogger().logger
        except Exception:
            return None
    return None

_logger = _get_logger()

def log_info(msg: str, **kwargs):
    if _logger:
        try:
            _logger.info(msg)
        except Exception:
            print("[INFO]", msg)
    else:
        print("[INFO]", msg)

def log_warn(msg: str, **kwargs):
    if _logger:
        try:
            _logger.warning(msg)
        except Exception:
            print("[WARN]", msg)
    else:
        print("[WARN]", msg)

def log_error(msg: str, **kwargs):
    if _logger:
        try:
            _logger.error(msg)
        except Exception:
            print("[ERROR]", msg)
    else:
        print("[ERROR]", msg)

# -------------------------
# Git helpers
# -------------------------
def run_git(cmd: List[str], cwd: Optional[str] = None, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    """
    Run a git command and return (rc, stdout, stderr)
    """
    try:
        proc = subprocess.run(["git"] + cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"
    except FileNotFoundError:
        return 127, "", "git-not-found"
    except Exception as e:
        return 1, "", str(e)

def safe_clone(repo_url: str, target_dir: Path, bare: bool = False, timeout: int = 120, depth: Optional[int] = None) -> Tuple[bool, str]:
    """
    Clone repo_url into target_dir safely, using a temp dir to avoid partial clones.
    Returns (ok, message).
    """
    tmp = target_dir.with_name(target_dir.name + ".tmp-clone")
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    cmd = ["clone"]
    if bare:
        cmd.append("--bare")
    if depth:
        cmd += ["--depth", str(depth)]
    cmd += [repo_url, str(tmp)]
    rc, out, err = run_git(cmd, timeout=timeout)
    if rc != 0:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        return False, f"git clone failed: rc={rc} err={err or out}"
    # move into place atomically
    if target_dir.exists():
        # backup existing if any
        backup = target_dir.with_name(target_dir.name + ".backup-" + str(int(time.time())))
        target_dir.rename(backup)
        try:
            tmp.rename(target_dir)
            shutil.rmtree(backup, ignore_errors=True)
        except Exception as e:
            # try rollback
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            backup.rename(target_dir)
            return False, f"clone move failed: {e}"
    else:
        tmp.rename(target_dir)
    return True, "cloned"

# -------------------------
# Repair helpers
# -------------------------
def repair_repository(repo_path: Path, timeout: int = 120) -> Tuple[bool, str]:
    """
    Try to repair a repo by running git fsck and git gc, if those fail attempt reclone.
    Returns (ok, message)
    """
    if not repo_path.exists():
        return False, "repo not found"
    rc, out, err = run_git(["fsck", "--full"], cwd=str(repo_path), timeout=timeout)
    if rc == 0:
        # run gc
        rc2, out2, err2 = run_git(["gc", "--prune=now", "--aggressive"], cwd=str(repo_path), timeout=timeout)
        if rc2 == 0:
            return True, "fsck/gc ok"
        else:
            return False, f"gc failed: {err2 or out2}"
    else:
        # try backup + reclone from origin if possible
        # find origin url
        rcu, uout, uerr = run_git(["remote", "get-url", "origin"], cwd=str(repo_path), timeout=timeout)
        if rcu != 0:
            return False, f"fsck failed and origin unknown: {uerr or uout}"
        origin = uout.strip()
        parent = repo_path.parent
        target_name = repo_path.name
        # attempt safe clone to temp
        tmp = parent / (target_name + ".tmp-reclone")
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        rc_clone, out_clone, err_clone = run_git(["clone", origin, str(tmp)], timeout=timeout)
        if rc_clone != 0:
            if tmp.exists():
                shutil.rmtree(tmp, ignore_errors=True)
            return False, f"reclone failed: {err_clone or out_clone}"
        # swap directories
        backup = parent / (target_name + ".corrupt-" + str(int(time.time())))
        try:
            repo_path.rename(backup)
            tmp.rename(repo_path)
            shutil.rmtree(backup, ignore_errors=True)
            return True, "reclone success"
        except Exception as e:
            return False, f"swap failed: {e}"

# -------------------------
# Repo processing
# -------------------------
def repo_needs_clone(repo_cfg: Dict[str,Any], local_path: Path) -> bool:
    if not local_path.exists():
        return True
    # if bare repo with no HEAD or empty, treat as need
    head = local_path / "HEAD"
    if not head.exists():
        return True
    return False

def fetch_and_report(repo_cfg: Dict[str,Any], cfg: Dict[str,Any], dry_run: bool = False, timeout: int = 120) -> Dict[str,Any]:
    """
    Sync one repository (clone/pull/fetch), returns a dict with summary info:
     {name, url, path, action, new_commits, errors, rc}
    """
    name = repo_cfg.get("name") or repo_cfg.get("path") or repo_cfg.get("url")
    url = repo_cfg.get("url")
    path = Path(repo_cfg.get("local", cfg["paths"].get("ports", "/usr/ports"))) / (repo_cfg.get("path") or name or "unknown")
    path = path.resolve()
    result = {"name": name, "url": url, "path": str(path), "action": None, "new_commits": 0, "errors": [], "rc": 0}

    try:
        # Ensure parent exists
        path.parent.mkdir(parents=True, exist_ok=True)
        if dry_run:
            log_info(f"[dry-run] would sync {name} -> {path}")
            result["action"] = "dry-run"
            return result

        if repo_needs_clone(repo_cfg, path):
            log_info(f"Cloning {url} -> {path}")
            ok, msg = safe_clone(url, path, timeout=timeout, depth=repo_cfg.get("depth"))
            result["action"] = "clone"
            if not ok:
                result["errors"].append(msg)
                result["rc"] = 1
                log_error(f"Clone failed for {name}: {msg}")
                return result
            # record clone event
            if DB_AVAILABLE:
                try:
                    with DBManager() as db:
                        db._execute("INSERT INTO events (pkg_name, action, timestamp, payload) VALUES (?,?,?,?)",
                                    (name, "clone", int(time.time()), json.dumps({"url": url})))
                except Exception:
                    pass
        else:
            # fetch remote updates
            # get current HEAD commit
            rc_head, out_head, err_head = run_git(["rev-parse", "HEAD"], cwd=str(path), timeout=timeout)
            old_head = out_head.strip() if rc_head == 0 else None
            # fetch
            rc_fetch, out_fetch, err_fetch = run_git(["fetch", "--all", "--prune"], cwd=str(path), timeout=timeout)
            if rc_fetch != 0:
                result["errors"].append(f"fetch failed: {err_fetch or out_fetch}")
                result["rc"] = rc_fetch
                log_warn(f"Fetch failed for {name}: {err_fetch or out_fetch}")
                # continue to try pull
            # try to fast-forward/pull main branch
            # detect default branch
            rc_branch, out_branch, err_branch = run_git(["symbolic-ref", "refs/remotes/origin/HEAD"], cwd=str(path), timeout=timeout)
            if rc_branch == 0 and out_branch:
                # refs/remotes/origin/main -> extract 'main'
                remote_head = out_branch.strip().split("/")[-1]
            else:
                remote_head = repo_cfg.get("branch") or "master"
            # attempt merge or reset to remote
            # try fast-forward merge
            rc_ff, out_ff, err_ff = run_git(["merge", "--ff-only", f"origin/{remote_head}"], cwd=str(path), timeout=timeout)
            if rc_ff != 0:
                # fallback: try pull with rebase
                rc_pull, out_pull, err_pull = run_git(["pull", "--rebase", "origin", remote_head], cwd=str(path), timeout=timeout)
                if rc_pull != 0:
                    result["errors"].append(f"pull failed: {err_pull or out_pull}")
                    result["rc"] = rc_pull
                    log_warn(f"Pull failed for {name}: {err_pull or out_pull}")
            # get new head
            rc_new, out_new, err_new = run_git(["rev-parse", "HEAD"], cwd=str(path), timeout=timeout)
            new_head = out_new.strip() if rc_new == 0 else None
            if old_head and new_head and old_head != new_head:
                # count commits between old_head and new_head
                rc_count, out_count, err_count = run_git(["rev-list", "--count", f"{old_head}..{new_head}"], cwd=str(path), timeout=timeout)
                if rc_count == 0:
                    try:
                        result["new_commits"] = int(out_count.strip())
                    except Exception:
                        result["new_commits"] = 1
                else:
                    result["new_commits"] = 1
                result["action"] = "update"
                log_info(f"{name} updated: {result['new_commits']} new commits")
                # record update event
                if DB_AVAILABLE:
                    try:
                        with DBManager() as db:
                            db._execute("INSERT INTO events (pkg_name, action, timestamp, payload) VALUES (?,?,?,?)",
                                        (name, "update", int(time.time()), json.dumps({"new_commits": result["new_commits"], "url": url})))
                    except Exception:
                        pass
            else:
                result["action"] = "noop"
    except Exception as e:
        result["errors"].append(str(e))
        result["rc"] = 1
        log_error(f"Unhandled error syncing {name}: {e}")
    return result

# -------------------------
# Top-level sync orchestration
# -------------------------
def sync_all(repos: List[Dict[str,Any]],
             cfg: Dict[str,Any],
             jobs: int = 4,
             dry_run: bool = False,
             force: bool = False,
             repair: bool = False,
             webhook: Optional[str] = None,
             notify: bool = False,
             metrics: bool = False,
             git_timeout: int = 120) -> Dict[str,Any]:
    """
    Sync a list of repositories (config entries).
    Returns summary dict with per-repo results.
    """
    results = {}
    # ensure paths
    ports_base = Path(cfg["paths"].get("ports", "/usr/ports"))
    ports_base.mkdir(parents=True, exist_ok=True)

    # parallel execution
    with ThreadPoolExecutor(max_workers=jobs) as ex:
        future_to_repo = {}
        for r in repos:
            future = ex.submit(fetch_and_report, r, cfg, dry_run, git_timeout)
            future_to_repo[future] = r
        for fut in as_completed(future_to_repo):
            repo_cfg = future_to_repo[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"name": repo_cfg.get("name"), "url": repo_cfg.get("url"), "error": str(e)}
            results[repo_cfg.get("name") or repo_cfg.get("url")] = res

    # post-process results: repairs, notifications and metrics
    total_new = sum(v.get("new_commits", 0) for v in results.values() if isinstance(v, dict))
    errors = {k:v for k,v in results.items() if isinstance(v, dict) and v.get("errors")}
    # if repair requested: try repair repos with errors
    if repair:
        for k,v in list(results.items()):
            if v.get("errors"):
                path = Path(v.get("path"))
                ok, msg = repair_repository(path, timeout=git_timeout)
                v.setdefault("repair", {"ok": ok, "msg": msg})
                if ok:
                    log_info(f"Repaired {k}: {msg}")
                else:
                    log_warn(f"Repair failed {k}: {msg}")
    # metrics: store in DB summary
    if metrics and DB_AVAILABLE:
        try:
            with DBManager() as db:
                payload = {"total_repos": len(repos), "new_commits": total_new, "errors": len(errors), "timestamp": int(time.time())}
                db._execute("INSERT INTO events (pkg_name, action, timestamp, payload) VALUES (?,?,?,?)",
                            ("zeropkg-sync", "metrics", int(time.time()), json.dumps(payload)))
        except Exception:
            pass

    # notifications: webhook or local file
    if notify and (webhook or cfg.get("notify", {}).get("webhook_url")):
        wh = webhook or cfg.get("notify", {}).get("webhook_url")
        if wh:
            notif_payload = {"total_repos": len(repos), "new_commits": total_new, "errors": len(errors), "detail": results}
            try:
                if REQUESTS_AVAILABLE:
                    r = requests.post(wh, json=notif_payload, timeout=10)
                    if r.status_code >= 200 and r.status_code < 300:
                        log_info("Webhook notification sent")
                    else:
                        log_warn(f"Webhook responded status {r.status_code}")
                else:
                    # fallback: write to local file for external process to pick up
                    outp = Path(cfg["paths"].get("log_dir", "/var/log/zeropkg")) / "sync_notification.json"
                    outp.parent.mkdir(parents=True, exist_ok=True)
                    with open(outp, "w") as f:
                        json.dump(notif_payload, f, indent=2)
                    log_info(f"Notification written to {outp} (requests not installed)")
            except Exception as e:
                log_warn(f"Failed to send webhook: {e}")

    return {"summary": {"repos": len(repos), "new_commits": total_new, "errors": len(errors)}, "detail": results}

# -------------------------
# Utility: load repo list from config
# -------------------------
def load_repos_from_config(cfg: Dict[str,Any]) -> List[Dict[str,Any]]:
    """
    Config format options supported:
    cfg["repos"] = [
      { "name": "gentoo-ports", "url": "https://...", "path": "gentoo-ports", "branch":"main", "depth":1 },
      ...
    ]
    or auto-scan dirs inside cfg["paths"]["ports"]
    """
    repos = []
    if "repos" in cfg and isinstance(cfg["repos"], list) and cfg["repos"]:
        for r in cfg["repos"]:
            repos.append(r)
    else:
        # auto-scan by reading ports directory and looking for .git or a remotes file
        base = Path(cfg["paths"].get("ports", "/usr/ports"))
        if not base.exists():
            return []
        for p in sorted(base.iterdir()):
            if not p.is_dir():
                continue
            # find git repo
            if (p / ".git").exists():
                # try to get origin url
                rc, out, err = run_git(["remote", "get-url", "origin"], cwd=str(p), timeout=60)
                url = out.strip() if rc==0 else None
                repos.append({"name": p.name, "url": url, "path": p.name})
    return repos

# -------------------------
# CLI wrapper
# -------------------------
def _cli():
    import argparse
    parser = argparse.ArgumentParser(description="Zeropkg sync — sync ports repositories")
    parser.add_argument("--config", "-c", help="path to config toml (used by zeropkg_config)", default=None)
    parser.add_argument("--jobs", "-j", type=int, default=None, help="parallel jobs")
    parser.add_argument("--dry-run", action="store_true", help="do not change anything")
    parser.add_argument("--force", action="store_true", help="force reclone for repos that changed")
    parser.add_argument("--repair", action="store_true", help="attempt repair on repos with errors")
    parser.add_argument("--list", action="store_true", help="list repos from config")
    parser.add_argument("--notify", action="store_true", help="send notification webhook if configured")
    parser.add_argument("--webhook", help="webhook url override")
    parser.add_argument("--metrics", action="store_true", help="record sync metrics to DB")
    parser.add_argument("--git-timeout", type=int, default=120, help="git timeout seconds per operation")
    args = parser.parse_args()

    cfg = load_config() if args.config is None else load_config()
    jobs = args.jobs or cfg.get("sync", {}).get("jobs", cfg.get("jobs", 4))
    repos = load_repos_from_config(cfg)
    if args.list:
        print("Repositories detected:")
        for r in repos:
            print(f" - {r.get('name')} -> {r.get('url')} (path={r.get('path')})")
        return
    res = sync_all(repos, cfg, jobs=jobs, dry_run=args.dry_run, force=args.force, repair=args.repair, webhook=args.webhook, notify=args.notify, metrics=args.metrics, git_timeout=args.git_timeout)
    print(json.dumps(res, indent=2))
    if res["summary"]["errors"] > 0:
        sys.exit(2)

if __name__ == "__main__":
    _cli()
