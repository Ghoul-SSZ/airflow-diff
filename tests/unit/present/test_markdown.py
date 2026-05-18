import json
from datetime import datetime, timezone
from pathlib import Path

from airflow_diff.present.markdown import render_markdown
from airflow_diff.schema import (
    AttrDiff, DagDiff, DiffDocument, DiffSummary, EdgeDiff, FieldDiff,
    ProvenanceEntry, RenderedField, RenderError, RenderErrorEntry, SCHEMA_VERSION,
    TaskDiff,
)

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def _load(name: str) -> DiffDocument:
    return DiffDocument.model_validate_json((FIXTURES / name).read_text())


def _make_large_dag_diff(n_tasks: int) -> DiffDocument:
    """Build a synthetic DiffDocument with n_tasks modified tasks for testing graph omission."""
    task_diffs = []
    for i in range(n_tasks):
        task_diffs.append(TaskDiff(
            task_id=f"task_{i:03d}",
            change_type="modified",
            field_diffs=[FieldDiff(
                name="bash_command", change_type="modified",
                before=f"echo old_{i}", after=f"echo new_{i}",
                provenance_before=[ProvenanceEntry(source="literal")],
                provenance_after=[ProvenanceEntry(source="literal")],
            )],
        ))
    dag_diff = DagDiff(
        dag_id="big_dag",
        classification="touched",
        status_a="ok", status_b="ok",
        pair_status="ok",
        source_file_before="dags/big_dag.py",
        source_file_after="dags/big_dag.py",
        task_diffs=task_diffs,
    )
    return DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha="aaa00000",
        head_sha="bbb11111",
        summary=DiffSummary(dags_touched=1),
        dags=[dag_diff],
    )


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


def test_truncates_when_output_exceeds_limit(monkeypatch):
    # Build a fake DiffDocument with enough DAGs that the rendered markdown
    # exceeds the limit. Easier: monkeypatch the cap to something small.
    from airflow_diff.present import markdown as mod
    monkeypatch.setattr(mod, "GITHUB_COMMENT_CHAR_LIMIT", 200)
    output = render_markdown(_load("single_dag_one_change.json"))
    assert "Output truncated" in output
    assert len(output) <= 200 + 500  # truncation suffix can push it slightly over


def test_large_dag_graph_omitted_by_default():
    """A 60-task DAG exceeds the default MAX_TASKS_FOR_GRAPH=50 so the graph is the summary box."""
    doc = _make_large_dag_diff(60)
    output = render_markdown(doc)
    assert "Graph omitted" in output
    assert "```mermaid" not in output


def test_large_dag_graph_rendered_with_higher_config_limit():
    """With max_tasks_for_graph=100, a 60-task DAG renders a full Mermaid graph."""
    from airflow_diff.config import Config
    config = Config(max_tasks_for_graph=100)
    doc = _make_large_dag_diff(60)
    output = render_markdown(doc, config=config)
    assert "```mermaid" in output
    assert "Graph omitted" not in output
