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
