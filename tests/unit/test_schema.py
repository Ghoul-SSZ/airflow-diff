import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from airflow_diff.schema import (
    SCHEMA_VERSION,
    AttrDiff,
    DagDiff,
    DagStatus,
    DiffDocument,
    DiffSummary,
    EdgeDiff,
    FieldDiff,
    ProvenanceEntry,
    RenderError,
    RenderedDag,
    RenderedDagBag,
    RenderedField,
    RenderedTask,
    TaskDiff,
)


def test_rendered_field_literal_round_trip():
    field = RenderedField(rendered=3, provenance=[ProvenanceEntry(source="literal")])
    payload = field.model_dump_json()
    restored = RenderedField.model_validate_json(payload)
    assert restored == field


def test_rendered_field_stub_round_trip():
    field = RenderedField(
        rendered="aws s3 cp s3://<VAR:bucket>/foo /tmp/x",
        provenance=[ProvenanceEntry(source="stub", key="var.value.bucket")],
    )
    restored = RenderedField.model_validate_json(field.model_dump_json())
    assert restored == field


def test_provenance_source_validation():
    with pytest.raises(ValidationError):
        ProvenanceEntry(source="bogus")


def test_render_error_round_trip():
    err = RenderError(type="ImportError", message="boom", traceback="...")
    assert RenderError.model_validate_json(err.model_dump_json()) == err


def test_rendered_task_minimum():
    task = RenderedTask(
        task_id="extract",
        operator="airflow.operators.bash.BashOperator",
        task_group=None,
        upstream=[],
        downstream=["transform"],
        fields={},
    )
    assert RenderedTask.model_validate_json(task.model_dump_json()) == task


def test_rendered_dag_ok_status():
    dag = RenderedDag(
        dag_id="my_dag",
        status="ok",
        source_file="dags/my.py",
        attrs={"schedule": "@daily"},
        datasets={"inlets": [], "outlets": []},
        task_groups=[],
        tasks=[],
    )
    assert dag.status == "ok"
    assert RenderedDag.model_validate_json(dag.model_dump_json()) == dag


def test_rendered_dag_error_status():
    dag = RenderedDag(
        dag_id="broken",
        status="error",
        source_file="dags/broken.py",
        error=RenderError(type="ImportError", message="x", traceback="..."),
    )
    assert dag.status == "error"
    assert RenderedDag.model_validate_json(dag.model_dump_json()) == dag


def test_rendered_dag_rejects_error_field_when_status_ok():
    with pytest.raises(ValidationError):
        RenderedDag(
            dag_id="my_dag",
            status="ok",
            source_file="dags/my.py",
            error=RenderError(type="ImportError", message="x", traceback="..."),
        )


def test_rendered_dag_rejects_tasks_when_status_error():
    with pytest.raises(ValidationError):
        RenderedDag(
            dag_id="broken",
            status="error",
            source_file="dags/broken.py",
            tasks=[],
        )


def test_rendered_dag_bag_round_trip():
    bag = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha="abc123",
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc),
        dags=[],
    )
    restored = RenderedDagBag.model_validate_json(bag.model_dump_json())
    assert restored == bag


def test_rendered_dag_bag_rejects_wrong_schema_version():
    with pytest.raises(ValidationError):
        RenderedDagBag(
            schema_version=999,
            commit_sha="x",
            airflow_version="2.10.3",
            rendered_at=datetime.now(timezone.utc),
            dags=[],
        )


def test_rendered_dag_bag_rejects_naive_datetime():
    with pytest.raises(ValidationError):
        RenderedDagBag(
            schema_version=SCHEMA_VERSION,
            commit_sha="abc123",
            airflow_version="2.10.3",
            rendered_at=datetime(2026, 5, 17, 12, 0, 0),
            dags=[],
        )


def test_field_diff_modified():
    fd = FieldDiff(
        name="bash_command",
        change_type="modified",
        before="echo a",
        after="echo b",
        provenance_before=[ProvenanceEntry(source="literal")],
        provenance_after=[ProvenanceEntry(source="literal")],
    )
    assert FieldDiff.model_validate_json(fd.model_dump_json()) == fd


def test_diff_document_round_trip():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha="abc",
        head_sha="def",
        summary=DiffSummary(),
        dags=[],
        render_errors=[],
    )
    assert DiffDocument.model_validate_json(doc.model_dump_json()) == doc
