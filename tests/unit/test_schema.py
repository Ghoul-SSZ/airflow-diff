from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from airflow_diff.schema import (
    SCHEMA_VERSION,
    DiffDocument,
    DiffSummary,
    ExternalTaskRef,
    FieldDiff,
    ProvenanceEntry,
    RenderedDag,
    RenderedDagBag,
    RenderedField,
    RenderedTask,
    RenderError,
    SensorMismatch,
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


def test_external_task_ref_minimum_round_trip():
    ref = ExternalTaskRef(
        kind="external_task_sensor",
        external_dag_id="upstream",
    )
    assert ExternalTaskRef.model_validate_json(ref.model_dump_json()) == ref


def test_external_task_ref_full_round_trip():
    ref = ExternalTaskRef(
        kind="external_task_sensor",
        external_dag_id="upstream",
        external_task_id="finalize",
        execution_delta_seconds=3600,
        execution_date_fn_present=True,
    )
    assert ExternalTaskRef.model_validate_json(ref.model_dump_json()) == ref


def test_external_task_ref_with_task_ids_list():
    ref = ExternalTaskRef(
        kind="external_task_sensor",
        external_dag_id="upstream",
        external_task_ids=["t1", "t2"],
    )
    assert ExternalTaskRef.model_validate_json(ref.model_dump_json()) == ref


def test_external_task_ref_rejects_unknown_kind():
    with pytest.raises(ValidationError):
        ExternalTaskRef(kind="trigger_dag_run", external_dag_id="x")


def test_rendered_task_external_ref_defaults_to_none():
    task = RenderedTask(
        task_id="t", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
    )
    assert task.external_ref is None


def test_sensor_mismatch_round_trip():
    m = SensorMismatch(
        sensor_dag_id="d",
        sensor_task_id="t",
        target_dag_id="u",
        target_task_id="x",
        reason="missing_execution_delta",
        sensor_schedule="@hourly",
        target_schedule="@daily",
    )
    assert SensorMismatch.model_validate_json(m.model_dump_json()) == m


def test_sensor_mismatch_rejects_unknown_reason():
    with pytest.raises(ValidationError):
        SensorMismatch(
            sensor_dag_id="d",
            sensor_task_id="t",
            target_dag_id="u",
            reason="bogus",
        )


def test_external_task_ref_rejects_multiple_targets():
    with pytest.raises(ValidationError, match="At most one of"):
        ExternalTaskRef(
            kind="external_task_sensor",
            external_dag_id="upstream",
            external_task_id="a",
            external_task_ids=["b"],
        )


def test_external_task_ref_allows_zero_targets():
    # Setting neither target is valid — means "wait for the whole DAG"
    ref = ExternalTaskRef(kind="external_task_sensor", external_dag_id="upstream")
    assert ref.external_task_id is None
    assert ref.external_task_ids is None


def test_sensor_mismatch_rejects_incorrect_delta_without_required_fields():
    with pytest.raises(ValidationError, match="expected_delta_seconds and actual_delta_seconds"):
        SensorMismatch(
            sensor_dag_id="d",
            sensor_task_id="t",
            target_dag_id="u",
            target_task_id="x",
            reason="incorrect_execution_delta",
            # expected_delta_seconds and actual_delta_seconds intentionally missing
        )


def test_sensor_mismatch_incorrect_delta_with_required_fields_ok():
    m = SensorMismatch(
        sensor_dag_id="d",
        sensor_task_id="t",
        target_dag_id="u",
        target_task_id="x",
        reason="incorrect_execution_delta",
        expected_delta_seconds=43200,
        actual_delta_seconds=3600,
    )
    assert m.reason == "incorrect_execution_delta"


def test_sensor_mismatch_notes_max_length_enforced():
    with pytest.raises(ValidationError):
        SensorMismatch(
            sensor_dag_id="d",
            sensor_task_id="t",
            target_dag_id="u",
            reason="missing_execution_delta",
            notes="x" * 501,
        )


def test_diff_document_sensor_mismatches_default_empty():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha="a",
        head_sha="b",
        summary=DiffSummary(),
        dags=[],
        render_errors=[],
    )
    assert doc.sensor_mismatches == []


def test_schema_version_is_2():
    assert SCHEMA_VERSION == 2


def test_rendered_dag_bag_rejects_v1_payload():
    # An explicit v1 schema_version literal must fail under v2.
    payload = (
        '{"schema_version": 1, "commit_sha": "x", "airflow_version": "2.10.3", '
        '"rendered_at": "2026-05-17T00:00:00+00:00", "dags": []}'
    )
    with pytest.raises(ValidationError):
        RenderedDagBag.model_validate_json(payload)
