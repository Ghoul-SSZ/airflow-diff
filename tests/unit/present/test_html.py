from pathlib import Path

from airflow_diff.present.html import render_html
from airflow_diff.schema import DiffDocument

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def test_empty(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "empty.json").read_text())
    out = render_html(doc)
    assert "<html" in out
    assert "airflow-diff" in out


def test_single_dag(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "single_dag_one_change.json").read_text())
    out = render_html(doc)
    assert "<table" in out
    assert "mermaid" in out
    assert out == snapshot
