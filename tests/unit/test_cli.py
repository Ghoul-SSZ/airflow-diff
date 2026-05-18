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
