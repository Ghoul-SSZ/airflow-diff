from datetime import datetime, timezone

from airflow_diff.schema import (
    ExternalTaskRef, RenderedDag, RenderedDagBag, RenderedTask, SCHEMA_VERSION,
    SensorMismatch,
)
from airflow_diff.config import Config
from airflow_diff.validators.cross_dag import _normalize_schedule, _mismatch_key, _mismatches_for_bag


def test_normalize_passthrough_cron():
    assert _normalize_schedule("0 9 * * *") == "0 9 * * *"


def test_normalize_preset_daily():
    assert _normalize_schedule("@daily") == "0 0 * * *"


def test_normalize_preset_midnight():
    assert _normalize_schedule("@midnight") == "0 0 * * *"


def test_normalize_preset_hourly():
    assert _normalize_schedule("@hourly") == "0 * * * *"


def test_normalize_preset_weekly():
    assert _normalize_schedule("@weekly") == "0 0 * * 0"


def test_normalize_preset_monthly():
    assert _normalize_schedule("@monthly") == "0 0 1 * *"


def test_normalize_preset_yearly():
    assert _normalize_schedule("@yearly") == "0 0 1 1 *"


def test_normalize_preset_annually():
    assert _normalize_schedule("@annually") == "0 0 1 1 *"


def test_normalize_opaque_returns_none():
    assert _normalize_schedule(None) is None
    assert _normalize_schedule("@once") is None
    assert _normalize_schedule("@continuous") is None
    assert _normalize_schedule("PT3600S") is None  # timedelta repr
    assert _normalize_schedule([1, 2, 3]) is None  # dataset list
    assert _normalize_schedule("<Dataset uri='s3://x'>") is None  # repr fallback


def test_mismatch_key_with_singular_task_id():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="missing_execution_delta",
    )
    assert _mismatch_key(m) == ("d", "t", "u", ("id", "x"))


def test_mismatch_key_with_task_ids_sorted():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_ids=["z", "a", "m"],
        reason="dangling_target",
    )
    assert _mismatch_key(m) == ("d", "t", "u", ("ids", ("a", "m", "z")))


def test_mismatch_key_ignores_reason():
    a = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="missing_execution_delta",
    )
    b = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="incorrect_execution_delta",
        expected_delta_seconds=1, actual_delta_seconds=2,  # required by validator
    )
    assert _mismatch_key(a) == _mismatch_key(b)


# ===== dangling_target tests (Task 6) =====

def _bag(*dags: RenderedDag) -> RenderedDagBag:
    return RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha="x",
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
        dags=list(dags),
    )


def _sensor_task(task_id: str, *, external_dag_id: str,
                 external_task_id: str | None = None,
                 external_task_ids: list[str] | None = None,
                 execution_delta_seconds: int | None = None,
                 execution_date_fn_present: bool = False) -> RenderedTask:
    return RenderedTask(
        task_id=task_id,
        operator="airflow.sensors.external_task.ExternalTaskSensor",
        task_group=None, upstream=[], downstream=[], fields={},
        external_ref=ExternalTaskRef(
            kind="external_task_sensor",
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            external_task_ids=external_task_ids,
            execution_delta_seconds=execution_delta_seconds,
            execution_date_fn_present=execution_date_fn_present,
        ),
    )


def _ok_dag(dag_id: str, *, schedule: str, tasks: list[RenderedTask]) -> RenderedDag:
    return RenderedDag(
        dag_id=dag_id, status="ok", source_file=f"dags/{dag_id}.py",
        attrs={"schedule": schedule}, datasets={"inlets": [], "outlets": []},
        task_groups=[], tasks=tasks,
    )


def test_dangling_target_dag_missing():
    sensor_dag = _ok_dag("downstream", schedule="@daily", tasks=[
        _sensor_task("wait", external_dag_id="missing", external_task_id="x"),
    ])
    mismatches = _mismatches_for_bag(_bag(sensor_dag), Config())
    assert len(mismatches) == 1
    [m] = mismatches
    assert m.reason == "dangling_target"
    assert m.sensor_dag_id == "downstream"
    assert m.sensor_task_id == "wait"
    assert m.target_dag_id == "missing"


def test_dangling_target_task_missing():
    sensor_dag = _ok_dag("downstream", schedule="@daily", tasks=[
        _sensor_task("wait", external_dag_id="upstream", external_task_id="not_a_real_task"),
    ])
    upstream_dag = _ok_dag("upstream", schedule="@daily", tasks=[
        RenderedTask(task_id="finalize", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream_dag), Config())
    assert m.reason == "dangling_target"
    assert m.target_task_id == "not_a_real_task"


def test_dangling_target_one_of_task_ids_missing():
    sensor_dag = _ok_dag("downstream", schedule="@daily", tasks=[
        _sensor_task("wait", external_dag_id="upstream",
                     external_task_ids=["finalize", "missing"]),
    ])
    upstream_dag = _ok_dag("upstream", schedule="@daily", tasks=[
        RenderedTask(task_id="finalize", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream_dag), Config())
    assert m.reason == "dangling_target"
    assert m.target_task_ids == ["finalize", "missing"]
    assert "missing" in (m.notes or "")
