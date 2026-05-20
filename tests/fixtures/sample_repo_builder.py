"""Programmatically build a two-commit sample repo for end-to-end testing.

The repo contains a `dags/` folder. Commit A has `dags_base/linear.py`;
commit B replaces it with a modified version (which differs in one bash_command).
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def build(
    repo_dir: Path,
    fixtures_root: Path,
    requirements_text: str,
    *,
    mode: str = "linear",
) -> tuple[str, str]:
    """Build a two-commit sample repo. Returns (base_sha, head_sha).

    Modes:
      * "linear" (default) — one DAG, bash_command changes between commits.
      * "paired_dags"      — base has only upstream.py; head adds a downstream
                             ExternalTaskSensor missing execution_delta.
    """
    repo_dir.mkdir(parents=True, exist_ok=True)
    _git(repo_dir, "init", "-b", "main")
    _git(repo_dir, "config", "user.email", "test@example.com")
    _git(repo_dir, "config", "user.name", "test")

    (repo_dir / "requirements.txt").write_text(requirements_text)
    (repo_dir / "dags").mkdir()

    if mode == "linear":
        (repo_dir / "dags" / "linear.py").write_text(
            (fixtures_root / "dags_base" / "linear.py").read_text()
        )
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "base")
        base_sha = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        src = (fixtures_root / "dags_base" / "linear.py").read_text()
        modified = src.replace('bash_command="echo end"', 'bash_command="echo finished"')
        (repo_dir / "dags" / "linear.py").write_text(modified)
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "head")
    elif mode == "paired_dags":
        # Base: only upstream
        (repo_dir / "dags" / "upstream.py").write_text(
            (fixtures_root / "dags_paired" / "upstream.py").read_text()
        )
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "base: upstream only")
        base_sha = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        # Head: add downstream sensor missing execution_delta
        (repo_dir / "dags" / "downstream.py").write_text(
            (fixtures_root / "dags_paired" / "downstream_missing_delta.py").read_text()
        )
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "head: add downstream sensor")
    else:
        raise ValueError(f"unknown mode: {mode}")

    head_sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return base_sha, head_sha
