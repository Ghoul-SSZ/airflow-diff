import sys
from pathlib import Path
from unittest.mock import patch

from airflow_diff import cli
from airflow_diff.schema import DiffDocument, DiffSummary, SCHEMA_VERSION


def _empty_diff():
    return DiffDocument(
        schema_version=SCHEMA_VERSION, base_sha="aaa", head_sha="bbb",
        summary=DiffSummary(), dags=[], render_errors=[],
    )


def test_cli_diff_invokes_run_diff(monkeypatch, tmp_path, capsys):
    called = {}
    def fake_run_diff(repo, a, b, config):
        called["args"] = (repo, a, b)
        return _empty_diff()
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "abc", "def", "--repo", str(tmp_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No DAG differences detected" in out
    assert called["args"][1:] == ("abc", "def")


def test_cli_unknown_subcommand_exits_nonzero():
    rc = cli.main(["bogus"])
    assert rc != 0


def test_cli_exit_code_for_regression(monkeypatch, tmp_path, capsys):
    from airflow_diff.schema import DagDiff
    def fake_run_diff(repo, a, b, config):
        return DiffDocument(
            schema_version=SCHEMA_VERSION, base_sha="aaa", head_sha="bbb",
            summary=DiffSummary(dags_regressed=1),
            dags=[DagDiff(dag_id="x", classification="touched", pair_status="regressed")],
            render_errors=[],
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 1  # regression


def test_cli_exit_zero_when_sensor_mismatches_default(monkeypatch, tmp_path):
    from airflow_diff.config import Config
    from airflow_diff.schema import SensorMismatch
    def fake_run_diff(repo, a, b, config):
        return DiffDocument(
            schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
            summary=DiffSummary(), dags=[], render_errors=[],
            sensor_mismatches=[SensorMismatch(
                sensor_dag_id="d", sensor_task_id="t",
                target_dag_id="u", target_task_id="x",
                reason="missing_execution_delta",
            )],
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 0  # default config does not fail on sensor mismatches


def test_cli_exit_one_when_fail_on_sensor_mismatch(monkeypatch, tmp_path):
    from airflow_diff.config import Config
    from airflow_diff.schema import SensorMismatch

    def fake_run_diff(repo, a, b, config):
        return DiffDocument(
            schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
            summary=DiffSummary(), dags=[], render_errors=[],
            sensor_mismatches=[SensorMismatch(
                sensor_dag_id="d", sensor_task_id="t",
                target_dag_id="u", target_task_id="x",
                reason="missing_execution_delta",
            )],
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    monkeypatch.setattr(cli, "load_config", lambda repo: Config(fail_on_sensor_mismatch=True))
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 1


def test_cli_exit_two_on_worktree_error(monkeypatch, tmp_path, capsys):
    from airflow_diff.worktree import WorktreeError
    def fake_run_diff(repo, a, b, config):
        raise WorktreeError("could not resolve ref 'bogus': fatal: ...")
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "bogus", "bbb", "--repo", str(tmp_path)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "could not resolve ref" in err
    assert "Traceback" not in err  # clean error, not a stack trace


def test_cli_exit_two_on_venv_error(monkeypatch, tmp_path, capsys):
    from airflow_diff.venv import VenvError
    def fake_run_diff(repo, a, b, config):
        raise VenvError("uv pip install failed: ResolutionImpossible")
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "uv pip install failed" in err
    assert "Traceback" not in err


def test_cli_exit_three_on_orchestrator_error(monkeypatch, tmp_path, capsys):
    from airflow_diff.orchestrator import OrchestratorError
    def fake_run_diff(repo, a, b, config):
        raise OrchestratorError(
            "renderer subprocess failed (exit 1) for sha abc:\nstderr (last 2000 chars): ImportError: ..."
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 3
    err = capsys.readouterr().err
    assert "renderer subprocess failed" in err
    assert "Traceback" not in err


def test_cli_exit_unknown_exception_still_propagates(monkeypatch, tmp_path):
    """Surprises should NOT be swallowed — let unexpected exceptions surface
    with a real traceback so bugs are noisy."""
    def fake_run_diff(repo, a, b, config):
        raise RuntimeError("something we didn't anticipate")
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    import pytest
    with pytest.raises(RuntimeError, match="something we didn't anticipate"):
        cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
