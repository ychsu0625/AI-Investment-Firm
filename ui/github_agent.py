"""
GitHub Agent — dual-mode
  Watch Mode : poll target repos for new releases → notify
  Push Mode  : stage + commit + tag + push with auto changelog
"""
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

# Repo root is one level above ui/
REPO_ROOT    = Path(__file__).parent.parent
DATA_DIR     = REPO_ROOT / "ui" / "data"
WATCH_FILE   = DATA_DIR / "github_watch.json"
VERSION_FILE = REPO_ROOT / "VERSION"
CHANGELOG    = REPO_ROOT / "CHANGELOG.md"

# Files staged on every push (never include secrets/db)
_SAFE_STAGE = [
    "ui/backend.py",
    "ui/index.html",
    "ui/info_center.html",
    "ui/simple.html",
    "ui/layout-plan.html",
    "ui/github_agent.py",
    "CHANGELOG.md",
    "VERSION",
    ".gitignore",
    "docs/",
]


# ─── Internal helpers ────────────────────────────────────────────────────────

def _git(args: list, cwd=None) -> tuple[int, str, str]:
    r = subprocess.run(
        ["git"] + args,
        capture_output=True, text=True,
        cwd=str(cwd or REPO_ROOT),
    )
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def _gh_api(path: str) -> dict:
    import urllib.request
    token = os.environ.get("GITHUB_TOKEN", "")
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "smart-monitor-agent/2.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"https://api.github.com{path}", headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _load_watch() -> dict:
    """{ "repos": [...], "last_seen": {"owner/repo": "v1.2.3"} }"""
    if WATCH_FILE.exists():
        return json.loads(WATCH_FILE.read_text("utf-8"))
    return {"repos": [], "last_seen": {}}


def _save_watch(state: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WATCH_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), "utf-8")


def _read_version() -> str:
    return VERSION_FILE.read_text("utf-8").strip() if VERSION_FILE.exists() else "2.0.0"


def _bump_version(current: str, bump: str) -> str:
    parts = current.lstrip("v").split(".")
    ma, mi, pa = int(parts[0]), int(parts[1]), int(parts[2])
    if bump == "major":
        return f"{ma+1}.0.0"
    if bump == "minor":
        return f"{ma}.{mi+1}.0"
    return f"{ma}.{mi}.{pa+1}"


def _append_changelog(version: str, message: str, files: str, timestamp: str):
    entry = (
        f"\n## v{version} — {timestamp}\n\n"
        f"{message}\n\n"
        f"**Changed files:**\n```\n{files}\n```\n"
    )
    if CHANGELOG.exists():
        existing = CHANGELOG.read_text("utf-8")
        lines = existing.split("\n")
        insert_at = next(
            (i for i, l in enumerate(lines) if l.startswith("## ")), 1
        )
        lines.insert(insert_at, entry)
        CHANGELOG.write_text("\n".join(lines), "utf-8")
    else:
        CHANGELOG.write_text(f"# Changelog\n{entry}", "utf-8")


# ─── Watch Mode ──────────────────────────────────────────────────────────────

def watch_add(owner: str, repo: str, label: str = "") -> dict:
    """Add a repo to the watch list."""
    state = _load_watch()
    key = f"{owner}/{repo}"
    existing_keys = {f"{r['owner']}/{r['repo']}" for r in state["repos"]}
    if key in existing_keys:
        return {"ok": False, "message": f"{key} already watched"}
    state["repos"].append({"owner": owner, "repo": repo, "label": label or key})
    _save_watch(state)
    return {"ok": True, "message": f"Now watching {key}"}


def watch_remove(owner: str, repo: str) -> dict:
    """Remove a repo from the watch list."""
    state = _load_watch()
    key = f"{owner}/{repo}"
    state["repos"] = [r for r in state["repos"] if f"{r['owner']}/{r['repo']}" != key]
    state["last_seen"].pop(key, None)
    _save_watch(state)
    return {"ok": True, "message": f"Removed {key}"}


def watch_list() -> list:
    """Return all watched repos with their last seen version."""
    state = _load_watch()
    return [
        {
            "owner": r["owner"],
            "repo": r["repo"],
            "label": r.get("label", f"{r['owner']}/{r['repo']}"),
            "last_seen": state["last_seen"].get(f"{r['owner']}/{r['repo']}", "unknown"),
        }
        for r in state["repos"]
    ]


def watch_check(notify_fn=None) -> list:
    """
    Poll all watched repos for new releases.
    notify_fn: callable(msg: str) — called for each new release found.
    Returns list of new-release dicts.
    """
    state  = _load_watch()
    found  = []

    for r in state["repos"]:
        key = f"{r['owner']}/{r['repo']}"
        try:
            data = _gh_api(f"/repos/{r['owner']}/{r['repo']}/releases/latest")
            tag  = data.get("tag_name", "")
            if not tag:
                continue
            if state["last_seen"].get(key) != tag:
                state["last_seen"][key] = tag
                entry = {
                    "repo":         key,
                    "label":        r.get("label", key),
                    "tag":          tag,
                    "url":          data.get("html_url", ""),
                    "published_at": data.get("published_at", ""),
                    "body":         (data.get("body") or "")[:300],
                }
                found.append(entry)
                if notify_fn:
                    body_preview = f"\n{entry['body'][:200]}" if entry["body"] else ""
                    notify_fn(
                        f"🆕 [{entry['label']}] 新版本：{tag}\n"
                        f"{entry['url']}{body_preview}"
                    )
        except Exception as e:
            print(f"[GH Watch] {key}: {e}")

    _save_watch(state)
    return found


# ─── Push Mode ───────────────────────────────────────────────────────────────

def git_status() -> dict:
    """Return current repo status (what would be staged/committed)."""
    _, modified, _ = _git(["diff", "--name-only"])
    _, untracked, _ = _git(["ls-files", "--others", "--exclude-standard"])
    _, staged, _  = _git(["diff", "--cached", "--name-only"])
    _, current_ver, _ = _git(["describe", "--tags", "--abbrev=0"])
    return {
        "current_version": _read_version(),
        "latest_tag":      current_ver or "none",
        "modified_files":  [f for f in modified.splitlines() if f],
        "untracked_files": [f for f in untracked.splitlines() if f],
        "staged_files":    [f for f in staged.splitlines() if f],
    }


def push_update(message: str, bump: str = "patch", include_docs: bool = True) -> dict:
    """
    Stage safe files, commit with auto-generated changelog entry, tag, and push.
    bump: "patch" | "minor" | "major"
    Returns {"ok": bool, "version": str, "tag": str, "message": str}
    """
    _, branch, _ = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    branch = branch.strip()
    if branch != "main" and branch != "master":
        return {"ok": False, "version": "", "tag": "", "message": f"Refusing to push from branch '{branch}'. Checkout main/master first."}

    current = _read_version()
    new_ver = _bump_version(current, bump)
    tag     = f"v{new_ver}"
    now     = datetime.now().strftime("%Y-%m-%d %H:%M")

    patterns = list(_SAFE_STAGE)
    if include_docs:
        patterns.append("docs/")

    for p in patterns:
        _git(["add", "--", p])

    _, staged, _ = _git(["diff", "--cached", "--name-only"])
    if not staged.strip():
        return {"ok": False, "version": current, "tag": "", "message": "Nothing staged to commit"}

    _append_changelog(new_ver, message, staged, now)
    _git(["add", "--", "CHANGELOG.md"])

    VERSION_FILE.write_text(new_ver, "utf-8")
    _git(["add", "--", "VERSION"])

    _, diff_stat, _ = _git(["diff", "--cached", "--stat"])

    commit_msg = (
        f"v{new_ver} — {message}\n\n"
        f"Changed files:\n{staged}\n\n"
        f"Diff stat:\n{diff_stat}"
    )
    rc, _, err = _git(["commit", "-m", commit_msg])
    if rc != 0:
        return {"ok": False, "version": current, "tag": "", "message": f"Commit failed: {err}"}

    _git(["tag", "-a", tag, "-m", f"Release {tag}: {message}"])

    rc, _, err = _git(["push", "origin", "main"])
    if rc != 0:
        return {"ok": False, "version": new_ver, "tag": tag, "message": f"Push failed: {err}"}

    _git(["push", "origin", "--tags"])

    return {"ok": True, "version": new_ver, "tag": tag, "message": f"Pushed {tag}"}
