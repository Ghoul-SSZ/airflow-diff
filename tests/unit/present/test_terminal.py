from pathlib import Path

from airflow_diff.present.terminal import render_terminal
from airflow_diff.schema import DiffDocument, DiffSummary, SCHEMA_VERSION, SensorMismatch

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def test_empty(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "empty.json").read_text())
    assert render_terminal(doc) == snapshot


def test_single_dag(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "single_dag_one_change.json").read_text())
    assert render_terminal(doc) == snapshot


def _doc_with(*mismatches):
    return DiffDocument(
        schema_version=SCHEMA_VERSION, base_sha="aaa00000", head_sha="bbb11111",
        summary=DiffSummary(), dags=[], render_errors=[],
        sensor_mismatches=list(mismatches),
    )


def test_terminal_missing_delta_uses_yellow():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="missing_execution_delta",
    )
    out = render_terminal(_doc_with(m))
    assert "Cross-DAG sensor mismatches" in out
    assert "\033[33m" in out  # YELLOW
    assert "\033[31m" not in out.split("Cross-DAG")[1]  # no red in the section


def test_terminal_incorrect_delta_uses_red():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="incorrect_execution_delta",
        expected_delta_seconds=43200, actual_delta_seconds=3600,
    )
    out = render_terminal(_doc_with(m))
    assert "\033[31m" in out  # RED


def test_terminal_dangling_uses_yellow():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="missing", target_task_id="x",
        reason="dangling_target",
    )
    out = render_terminal(_doc_with(m))
    assert "\033[33m" in out  # YELLOW


def test_terminal_no_section_when_empty():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
        summary=DiffSummary(), dags=[], render_errors=[],
        sensor_mismatches=[],
    )
    out = render_terminal(doc)
    assert "Cross-DAG sensor mismatches" not in out
