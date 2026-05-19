"""Verify the two per-commit renderers run concurrently."""

from __future__ import annotations

import threading
import time
from pathlib import Path

from airflow_diff.config import Config
from airflow_diff.orchestrator import run_diff
from airflow_diff.schema import RenderedDagBag


def _make_empty_bag(sha: str) -> RenderedDagBag:
    return RenderedDagBag.model_validate(
        {
            "schema_version": 2,
            "commit_sha": sha,
            "airflow_version": "2.10.3",
            "rendered_at": "2026-05-19T00:00:00+00:00",
            "dags": [],
        }
    )


def test_renderers_run_in_parallel(tmp_path, monkeypatch):
    """Two _spawn_renderer calls that each sleep for 0.5s should finish in <0.9s wall time."""
    sleep_for = 0.5
    spawn_in_flight = [0]
    spawn_in_flight_lock = threading.Lock()
    max_concurrent = [0]

    def fake_spawn(python, worktree, sha, config, fixtures_yaml):
        with spawn_in_flight_lock:
            spawn_in_flight[0] += 1
            max_concurrent[0] = max(max_concurrent[0], spawn_in_flight[0])
        time.sleep(sleep_for)
        with spawn_in_flight_lock:
            spawn_in_flight[0] -= 1
        return _make_empty_bag(sha)

    monkeypatch.setattr("airflow_diff.orchestrator._spawn_renderer", fake_spawn)
    monkeypatch.setattr("airflow_diff.orchestrator.resolve_sha", lambda repo, ref: f"sha_{ref}")
    monkeypatch.setattr("airflow_diff.orchestrator.ensure_sha_present", lambda repo, sha: None)
    monkeypatch.setattr("airflow_diff.orchestrator._touched_files", lambda *a, **kw: [])

    class _DummyCM:
        def __init__(self, p):
            self.p = p

        def __enter__(self):
            return self.p

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        "airflow_diff.orchestrator.worktree_for",
        lambda repo, sha: _DummyCM(tmp_path / sha),
    )
    monkeypatch.setattr("airflow_diff.orchestrator.venv_for", lambda wt: Path("/usr/bin/python3"))
    monkeypatch.setattr(
        "airflow_diff.orchestrator._validate_cross_dag",
        lambda *a, **kw: [],
    )

    for sha in ("sha_base", "sha_head"):
        (tmp_path / sha).mkdir(parents=True, exist_ok=True)

    config = Config()
    t0 = time.monotonic()
    run_diff(tmp_path, "base", "head", config)
    elapsed = time.monotonic() - t0

    assert max_concurrent[0] == 2, (
        f"expected both renderers in flight concurrently, peak was {max_concurrent[0]}"
    )
    assert elapsed < sleep_for * 1.7, (
        f"renderers appear to run serially: elapsed={elapsed:.2f}s, sleep_for={sleep_for}s"
    )
