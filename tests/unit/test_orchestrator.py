from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

from airflow_diff.schema import DiffDocument, RenderedDagBag, SCHEMA_VERSION


def test_orchestrator_invokes_renderer_per_commit_and_diffs(tmp_path, monkeypatch):
    from airflow_diff import orchestrator
    from airflow_diff.config import Config

    base_bag_json = RenderedDagBag(
        schema_version=SCHEMA_VERSION, commit_sha="aaa", airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, tzinfo=timezone.utc), dags=[],
    ).model_dump_json()
    head_bag_json = RenderedDagBag(
        schema_version=SCHEMA_VERSION, commit_sha="bbb", airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, tzinfo=timezone.utc), dags=[],
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

    call_count = {"n": 0}
    def fake_popen(args, **kw):
        proc = MagicMock()
        proc.communicate.return_value = (
            head_bag_json if call_count["n"] else base_bag_json, "",
        )
        proc.returncode = 0
        call_count["n"] += 1
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
        ExternalTaskRef, RenderedDag, RenderedDagBag, RenderedTask, SCHEMA_VERSION,
    )

    # Build a head bag with a sensor missing its bridge; base bag has the sensor's
    # DAG on the same schedule so the mismatch is PR-introduced.
    def _bag(commit_sha, sensor_schedule):
        sensor_dag = RenderedDag(
            dag_id="downstream", status="ok", source_file="dags/d.py",
            attrs={"schedule": sensor_schedule},
            datasets={"inlets": [], "outlets": []},
            task_groups=[],
            tasks=[RenderedTask(
                task_id="wait",
                operator="airflow.sensors.external_task.ExternalTaskSensor",
                task_group=None, upstream=[], downstream=[], fields={},
                external_ref=ExternalTaskRef(
                    kind="external_task_sensor",
                    external_dag_id="upstream",
                    external_task_id="x",
                ),
            )],
        )
        upstream = RenderedDag(
            dag_id="upstream", status="ok", source_file="dags/u.py",
            attrs={"schedule": "@daily"},
            datasets={"inlets": [], "outlets": []},
            task_groups=[],
            tasks=[RenderedTask(task_id="x", operator="x.Op", task_group=None,
                                upstream=[], downstream=[], fields={})],
        )
        return RenderedDagBag(
            schema_version=SCHEMA_VERSION, commit_sha=commit_sha,
            airflow_version="2.10.3",
            rendered_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
            dags=[sensor_dag, upstream],
        ).model_dump_json()

    base_json = _bag("aaa", "@daily")  # aligned → no mismatch at base
    head_json = _bag("bbb", "@hourly")  # misaligned → PR-introduced

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

    call_count = {"n": 0}
    def fake_popen(args, **kw):
        proc = MagicMock()
        proc.communicate.return_value = (head_json if call_count["n"] else base_json, "")
        proc.returncode = 0
        call_count["n"] += 1
        return proc
    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    diff = orchestrator.run_diff(tmp_path, "aaa", "bbb", Config())
    assert len(diff.sensor_mismatches) == 1
    assert diff.sensor_mismatches[0].reason == "missing_execution_delta"
