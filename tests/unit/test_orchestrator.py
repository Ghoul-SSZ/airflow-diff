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
