from pathlib import Path

from airflow_diff.present.terminal import render_terminal
from airflow_diff.schema import DiffDocument

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def test_empty(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "empty.json").read_text())
    assert render_terminal(doc) == snapshot


def test_single_dag(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "single_dag_one_change.json").read_text())
    assert render_terminal(doc) == snapshot
