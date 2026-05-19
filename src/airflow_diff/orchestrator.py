"""Top-level coordinator.

Resolves SHAs, prepares worktrees and venvs, spawns one renderer subprocess per
commit (in parallel), reads their JSON, runs the diff engine, and returns a
DiffDocument. The parent process never imports Airflow.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
from pathlib import Path

from airflow_diff.config import Config
from airflow_diff.diff import compute_diff
from airflow_diff.schema import DiffDocument, RenderedDagBag
from airflow_diff.validators.cross_dag import validate as _validate_cross_dag
from airflow_diff.venv import venv_for
from airflow_diff.worktree import (
    ensure_sha_present,
    resolve_sha,
    worktree_for,
)


class OrchestratorError(RuntimeError):
    pass


def _touched_files(repo_root: Path, base_sha: str, head_sha: str) -> list[str]:
    res = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--name-only", base_sha, head_sha],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        raise OrchestratorError(f"git diff failed: {res.stderr.strip()}")
    return [line for line in res.stdout.splitlines() if line.strip()]


def _parse_renderer_stdout(stdout: str, sha: str) -> RenderedDagBag:
    """Parse a RenderedDagBag from renderer stdout, skipping any leading log lines.

    Airflow may emit log output to stdout before the JSON payload — for example:
        [2026-05-19T09:40:11.935+0200] {crypto.py:82} WARNING - empty cryptography
        key - values will not be stored encrypted.
    Log lines may themselves contain '{' (e.g. "{crypto.py:82}"), so we find the
    first line that starts with '{' rather than naively seeking the first '{' char.
    If no such line exists we fall back to the full string so the resulting parse
    error is unchanged from the original behaviour.
    """
    payload = stdout
    for line in stdout.splitlines():
        if line.startswith("{"):
            # Locate this line's position and slice from there so any trailing
            # content on subsequent lines is preserved as part of the payload.
            payload = stdout[stdout.index(line) :]
            break
    try:
        return RenderedDagBag.model_validate_json(payload)
    except Exception as e:
        raise OrchestratorError(
            f"renderer for sha {sha} emitted invalid JSON: {e}\n"
            f"stdout (first 2000 chars): {stdout[:2000]}"
        ) from e


def _spawn_renderer(
    python: Path, worktree: Path, sha: str, config: Config, fixtures_yaml: Path | None
) -> RenderedDagBag:
    args = [
        str(python),
        "-m",
        "airflow_diff.renderer",
        "--worktree",
        str(worktree),
        "--commit-sha",
        sha,
        "--config",
        json.dumps(
            {
                "dags_folder": config.dags_folder,
                "plugins_folder": config.plugins_folder,
                "synthetic_logical_date": config.synthetic_logical_date.isoformat(),
                "excluded_files": config.excluded_files,
                "excluded_dag_ids": config.excluded_dag_ids,
            }
        ),
    ]
    if fixtures_yaml is not None:
        args.extend(["--fixtures", str(fixtures_yaml)])

    # Inject our package onto PYTHONPATH so the renderer can import airflow_diff.schema
    # even when running inside a venv that only has the user's Airflow installed.
    _pkg_src = str(Path(__file__).parent.parent)  # …/src/
    _existing = os.environ.get("PYTHONPATH", "")
    _pythonpath = f"{_pkg_src}:{_existing}" if _existing else _pkg_src
    env = {**os.environ, "PYTHONPATH": _pythonpath}

    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    try:
        out, err = proc.communicate(timeout=config.render_timeout_seconds)
    except subprocess.TimeoutExpired as e:
        # Popen.communicate raising TimeoutExpired leaves the child alive — we
        # must kill it ourselves or it leaks as an orphan/zombie. Per spec §7.
        proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)
        # SIGKILL'd but reap may be deferred; OS will clean up eventually.
        raise OrchestratorError(
            f"renderer subprocess timed out after {config.render_timeout_seconds}s "
            f"for sha {sha}; killed."
        ) from e
    if proc.returncode != 0:
        raise OrchestratorError(
            f"renderer subprocess failed (exit {proc.returncode}) for sha {sha}:\n"
            f"stderr (last 2000 chars): {err[-2000:]}"
        )
    return _parse_renderer_stdout(out, sha=sha)


def run_diff(repo_root: Path, base_ref: str, head_ref: str, config: Config) -> DiffDocument:
    base_sha = resolve_sha(repo_root, base_ref)
    head_sha = resolve_sha(repo_root, head_ref)
    ensure_sha_present(repo_root, base_sha)
    ensure_sha_present(repo_root, head_sha)

    touched = _touched_files(repo_root, base_sha, head_sha)

    with worktree_for(repo_root, base_sha) as wt_base, worktree_for(repo_root, head_sha) as wt_head:
        # Each worktree may carry its own fixtures file (per-commit)
        fixtures_base = wt_base / config.fixtures_path
        fixtures_head = wt_head / config.fixtures_path

        py_base = venv_for(wt_base)
        py_head = venv_for(wt_head)

        # Renderers run serially for simplicity in MVP; parallel is a later optimization
        rendered_base = _spawn_renderer(
            py_base,
            wt_base,
            base_sha,
            config,
            fixtures_base if fixtures_base.exists() else None,
        )
        rendered_head = _spawn_renderer(
            py_head,
            wt_head,
            head_sha,
            config,
            fixtures_head if fixtures_head.exists() else None,
        )

    diff = compute_diff(rendered_base, rendered_head, touched_files=touched)
    diff.sensor_mismatches = _validate_cross_dag(rendered_base, rendered_head, config)
    return diff
