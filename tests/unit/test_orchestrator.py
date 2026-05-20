from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from airflow_diff.schema import SCHEMA_VERSION, DiffDocument, RenderedDagBag


def test_orchestrator_invokes_renderer_per_commit_and_diffs(tmp_path, monkeypatch):
    from airflow_diff import orchestrator
    from airflow_diff.config import Config

    base_bag_json = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha="aaa",
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, tzinfo=timezone.utc),
        dags=[],
    ).model_dump_json()
    head_bag_json = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha="bbb",
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, tzinfo=timezone.utc),
        dags=[],
    ).model_dump_json()

    # Patch worktree, venv, and subprocess
    monkeypatch.setattr(orchestrator, "resolve_sha", lambda r, s: s + "0" * (40 - len(s)))
    monkeypatch.setattr(orchestrator, "ensure_sha_present", lambda r, s: None)

    from contextlib import contextmanager

    @contextmanager
    def fake_wt(repo, sha, **kw):
        p = tmp_path / sha
        p.mkdir(exist_ok=True)
        yield p

    monkeypatch.setattr(orchestrator, "worktree_for", fake_wt)
    monkeypatch.setattr(orchestrator, "venv_for", lambda wt, **kw: Path("/usr/bin/python3"))
    monkeypatch.setattr(orchestrator, "_touched_files", lambda r, a, b: [])

    base_sha_full = "aaa" + "0" * 37

    def fake_popen(args, **kw):
        # Dispatch by --commit-sha so the test is robust to parallel renderer
        # ordering (the orchestrator runs both renderers concurrently).
        sha = args[args.index("--commit-sha") + 1]
        proc = MagicMock()
        proc.communicate.return_value = (
            base_bag_json if sha == base_sha_full else head_bag_json,
            "",
        )
        proc.returncode = 0
        return proc

    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    diff = orchestrator.run_diff(tmp_path, "aaa", "bbb", Config())
    assert isinstance(diff, DiffDocument)
    assert diff.base_sha.startswith("aaa")
    assert diff.head_sha.startswith("bbb")


def test_orchestrator_attaches_sensor_mismatches(tmp_path, monkeypatch):
    from airflow_diff import orchestrator
    from airflow_diff.config import Config
    from airflow_diff.schema import (
        SCHEMA_VERSION,
        ExternalTaskRef,
        RenderedDag,
        RenderedDagBag,
        RenderedTask,
    )

    # Build a head bag with a sensor missing its bridge; base bag has the sensor's
    # DAG on the same schedule so the mismatch is PR-introduced.
    def _bag(commit_sha, sensor_schedule):
        sensor_dag = RenderedDag(
            dag_id="downstream",
            status="ok",
            source_file="dags/d.py",
            attrs={"schedule": sensor_schedule},
            datasets={"inlets": [], "outlets": []},
            task_groups=[],
            tasks=[
                RenderedTask(
                    task_id="wait",
                    operator="airflow.sensors.external_task.ExternalTaskSensor",
                    task_group=None,
                    upstream=[],
                    downstream=[],
                    fields={},
                    external_ref=ExternalTaskRef(
                        kind="external_task_sensor",
                        external_dag_id="upstream",
                        external_task_id="x",
                    ),
                )
            ],
        )
        upstream = RenderedDag(
            dag_id="upstream",
            status="ok",
            source_file="dags/u.py",
            attrs={"schedule": "@daily"},
            datasets={"inlets": [], "outlets": []},
            task_groups=[],
            tasks=[
                RenderedTask(
                    task_id="x",
                    operator="x.Op",
                    task_group=None,
                    upstream=[],
                    downstream=[],
                    fields={},
                )
            ],
        )
        return RenderedDagBag(
            schema_version=SCHEMA_VERSION,
            commit_sha=commit_sha,
            airflow_version="2.10.3",
            rendered_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
            dags=[sensor_dag, upstream],
        ).model_dump_json()

    base_sha_full = "aaa" + "0" * 37
    head_sha_full = "bbb" + "0" * 37
    base_json = _bag(base_sha_full, "@daily")  # aligned → no mismatch at base
    head_json = _bag(head_sha_full, "@hourly")  # misaligned → PR-introduced

    monkeypatch.setattr(orchestrator, "resolve_sha", lambda r, s: s + "0" * (40 - len(s)))
    monkeypatch.setattr(orchestrator, "ensure_sha_present", lambda r, s: None)

    from contextlib import contextmanager

    @contextmanager
    def fake_wt(repo, sha, **kw):
        p = tmp_path / sha
        p.mkdir(exist_ok=True)
        yield p

    monkeypatch.setattr(orchestrator, "worktree_for", fake_wt)
    monkeypatch.setattr(orchestrator, "venv_for", lambda wt, **kw: Path("/usr/bin/python3"))
    monkeypatch.setattr(orchestrator, "_touched_files", lambda r, a, b: [])

    def fake_popen(args, **kw):
        # Dispatch by --commit-sha so the test is robust to parallel renderer
        # ordering (the orchestrator runs both renderers concurrently).
        sha = args[args.index("--commit-sha") + 1]
        proc = MagicMock()
        proc.communicate.return_value = (
            base_json if sha == base_sha_full else head_json,
            "",
        )
        proc.returncode = 0
        return proc

    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    diff = orchestrator.run_diff(tmp_path, "aaa", "bbb", Config())
    assert len(diff.sensor_mismatches) == 1
    assert diff.sensor_mismatches[0].reason == "missing_execution_delta"


def test_parse_renderer_stdout_strips_leading_log_lines():
    """_parse_renderer_stdout must tolerate Airflow log lines that appear before
    the JSON payload on stdout (e.g. the crypto WARNING emitted by Airflow's
    crypto module before any DAG code runs)."""
    from datetime import datetime, timezone

    from airflow_diff.orchestrator import _parse_renderer_stdout
    from airflow_diff.schema import SCHEMA_VERSION, RenderedDagBag

    valid_json = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha="abc" + "0" * 37,
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 19, tzinfo=timezone.utc),
        dags=[],
    ).model_dump_json()

    warning_prefix = (
        "[2026-05-19T09:40:11.935+0200] {crypto.py:82} WARNING - "
        "empty cryptography key - values will not be stored encrypted.\n"
    )
    stdout_with_prefix = warning_prefix + valid_json

    result = _parse_renderer_stdout(stdout_with_prefix, sha="abc" + "0" * 37)
    assert isinstance(result, RenderedDagBag)
    assert result.airflow_version == "2.10.3"


def test_orchestrator_kills_renderer_on_timeout_and_raises(tmp_path, monkeypatch):
    """When the renderer subprocess exceeds render_timeout_seconds, the
    orchestrator must kill it (so it doesn't leak as a zombie) and raise
    OrchestratorError with a clear timeout message."""
    import subprocess as _subprocess

    from airflow_diff import orchestrator
    from airflow_diff.config import Config
    from airflow_diff.orchestrator import OrchestratorError

    monkeypatch.setattr(orchestrator, "resolve_sha", lambda r, s: s + "0" * (40 - len(s)))
    monkeypatch.setattr(orchestrator, "ensure_sha_present", lambda r, s: None)

    from contextlib import contextmanager

    @contextmanager
    def fake_wt(repo, sha, **kw):
        p = tmp_path / sha
        p.mkdir(exist_ok=True)
        yield p

    monkeypatch.setattr(orchestrator, "worktree_for", fake_wt)
    monkeypatch.setattr(orchestrator, "venv_for", lambda wt, **kw: Path("/usr/bin/python3"))
    monkeypatch.setattr(orchestrator, "_touched_files", lambda r, a, b: [])

    kill_calls = {"n": 0}
    wait_calls = {"n": 0}

    def fake_popen(args, **kw):
        proc = MagicMock()
        # communicate() raises TimeoutExpired immediately — simulates a hang
        proc.communicate.side_effect = _subprocess.TimeoutExpired(cmd=args, timeout=1)

        def _kill():
            kill_calls["n"] += 1

        def _wait(timeout=None):
            wait_calls["n"] += 1

        proc.kill.side_effect = _kill
        proc.wait.side_effect = _wait
        return proc

    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    cfg = Config(render_timeout_seconds=1)
    with pytest.raises(OrchestratorError, match="timed out"):
        orchestrator.run_diff(tmp_path, "aaa", "bbb", cfg)

    # The subprocess must have been killed (not leaked).
    assert kill_calls["n"] >= 1, "renderer subprocess was not killed on timeout"
    # And the orchestrator should reap it to avoid a zombie.
    assert wait_calls["n"] >= 1, "renderer subprocess was not waited on after kill"
