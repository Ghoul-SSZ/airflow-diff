"""Programmatically build a two-commit sample repo for end-to-end testing.

The repo contains a `dags/` folder. Commit A has `dags_base/linear.py`;
commit B replaces it with a modified version (which differs in one bash_command).
"""
from __future__ import annotations

import subprocess
from pathlib import Path


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def build(repo_dir: Path, fixtures_root: Path, requirements_text: str) -> tuple[str, str]:
    """Build the repo. Returns (base_sha, head_sha)."""
    repo_dir.mkdir(parents=True, exist_ok=True)
    _git(repo_dir, "init", "-b", "main")
    _git(repo_dir, "config", "user.email", "test@example.com")
    _git(repo_dir, "config", "user.name", "test")

    (repo_dir / "requirements.txt").write_text(requirements_text)
    (repo_dir / "dags").mkdir()
    (repo_dir / "dags" / "linear.py").write_text(
        (fixtures_root / "dags_base" / "linear.py").read_text()
    )
    _git(repo_dir, "add", ".")
    _git(repo_dir, "commit", "-m", "base")
    base_sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()

    # Modify linear.py with a small change
    src = (fixtures_root / "dags_base" / "linear.py").read_text()
    modified = src.replace('bash_command="echo end"', 'bash_command="echo finished"')
    (repo_dir / "dags" / "linear.py").write_text(modified)
    _git(repo_dir, "add", ".")
    _git(repo_dir, "commit", "-m", "head")
    head_sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return base_sha, head_sha
