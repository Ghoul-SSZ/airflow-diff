"""Wraps `git worktree` for isolated per-commit checkouts.

Worktrees are cached under a root dir keyed by full SHA so concurrent runs
against the same SHA share the on-disk checkout.
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_WORKTREE_ROOT = Path("/tmp/airflow-diff/worktrees")


class WorktreeError(RuntimeError):
    pass


@dataclass
class _RunResult:
    returncode: int
    stdout: str
    stderr: str


def _run(args: list[str], **kwargs) -> _RunResult:
    res = subprocess.run(args, capture_output=True, text=True, **kwargs)
    return _RunResult(res.returncode, res.stdout, res.stderr)


def resolve_sha(repo_root: Path, ref: str) -> str:
    res = _run(["git", "-C", str(repo_root), "rev-parse", "--verify", f"{ref}^{{commit}}"])
    if res.returncode != 0:
        raise WorktreeError(f"could not resolve ref {ref!r}: {res.stderr.strip()}")
    return res.stdout.strip()


def ensure_sha_present(repo_root: Path, sha: str) -> None:
    res = _run(["git", "-C", str(repo_root), "cat-file", "-e", sha])
    if res.returncode != 0:
        raise WorktreeError(
            f"commit {sha} is not present in the repo. If running in CI, ensure "
            f"`actions/checkout` uses `fetch-depth: 0`."
        )


@contextmanager
def worktree_for(
    repo_root: Path, sha: str, *, root: Path = DEFAULT_WORKTREE_ROOT
) -> Iterator[Path]:
    root.mkdir(parents=True, exist_ok=True)
    target = root / sha
    if not target.exists():
        res = _run(["git", "-C", str(repo_root), "worktree", "add", "--detach", str(target), sha])
        if res.returncode != 0:
            raise WorktreeError(f"git worktree add failed: {res.stderr.strip()}")
    logger.debug("worktree path=%s sha=%s", target, sha)
    yield target
    # Note: we intentionally do not clean up on exit. The cache amortizes across
    # subsequent runs against the same SHA. Cleanup is the user's responsibility
    # (or `git worktree prune` in CI cleanup).
