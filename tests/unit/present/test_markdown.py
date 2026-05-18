import json
from pathlib import Path

from airflow_diff.present.markdown import render_markdown
from airflow_diff.schema import DiffDocument

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def _load(name: str) -> DiffDocument:
    return DiffDocument.model_validate_json((FIXTURES / name).read_text())


def test_empty_diff_renders(snapshot):
    doc = _load("empty.json")
    output = render_markdown(doc)
    assert output == snapshot


def test_single_dag_one_change(snapshot):
    output = render_markdown(_load("single_dag_one_change.json"))
    assert output == snapshot


def test_mermaid_block_in_output(snapshot):
    output = render_markdown(_load("single_dag_one_change.json"))
    # Snapshot the whole output (re-records the fixture above):
    assert output == snapshot
    # Sanity assertions that don't rely on the full snapshot:
    assert "```mermaid" in output
    assert "classDef added" in output
    assert "validate_data" in output
