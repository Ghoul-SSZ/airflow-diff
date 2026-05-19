from datetime import datetime, timezone

from airflow_diff.config import Config
from airflow_diff.schema import (
    SCHEMA_VERSION,
    ExternalTaskRef,
    RenderedDag,
    RenderedDagBag,
    RenderedTask,
    SensorMismatch,
)
from airflow_diff.validators.cross_dag import (
    _mismatch_key,
    _mismatches_for_bag,
    _normalize_schedule,
    validate,
)


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
        sensor_dag_id="d",
        sensor_task_id="t",
        target_dag_id="u",
        target_task_id="x",
        reason="missing_execution_delta",
    )
    assert _mismatch_key(m) == ("d", "t", "u", ("id", "x"))


def test_mismatch_key_with_task_ids_sorted():
    m = SensorMismatch(
        sensor_dag_id="d",
        sensor_task_id="t",
        target_dag_id="u",
        target_task_ids=["z", "a", "m"],
        reason="dangling_target",
    )
    assert _mismatch_key(m) == ("d", "t", "u", ("ids", ("a", "m", "z")))


def test_mismatch_key_ignores_reason():
    a = SensorMismatch(
        sensor_dag_id="d",
        sensor_task_id="t",
        target_dag_id="u",
        target_task_id="x",
        reason="missing_execution_delta",
    )
    b = SensorMismatch(
        sensor_dag_id="d",
        sensor_task_id="t",
        target_dag_id="u",
        target_task_id="x",
        reason="incorrect_execution_delta",
        expected_delta_seconds=1,
        actual_delta_seconds=2,  # required by validator
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


def _sensor_task(
    task_id: str,
    *,
    external_dag_id: str,
    external_task_id: str | None = None,
    external_task_ids: list[str] | None = None,
    execution_delta_seconds: int | None = None,
    execution_date_fn_present: bool = False,
) -> RenderedTask:
    return RenderedTask(
        task_id=task_id,
        operator="airflow.sensors.external_task.ExternalTaskSensor",
        task_group=None,
        upstream=[],
        downstream=[],
        fields={},
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
        dag_id=dag_id,
        status="ok",
        source_file=f"dags/{dag_id}.py",
        attrs={"schedule": schedule},
        datasets={"inlets": [], "outlets": []},
        task_groups=[],
        tasks=tasks,
    )


def test_dangling_target_dag_missing():
    sensor_dag = _ok_dag(
        "downstream",
        schedule="@daily",
        tasks=[
            _sensor_task("wait", external_dag_id="missing", external_task_id="x"),
        ],
    )
    mismatches = _mismatches_for_bag(_bag(sensor_dag), Config())
    assert len(mismatches) == 1
    [m] = mismatches
    assert m.reason == "dangling_target"
    assert m.sensor_dag_id == "downstream"
    assert m.sensor_task_id == "wait"
    assert m.target_dag_id == "missing"


def test_dangling_target_task_missing():
    sensor_dag = _ok_dag(
        "downstream",
        schedule="@daily",
        tasks=[
            _sensor_task("wait", external_dag_id="upstream", external_task_id="not_a_real_task"),
        ],
    )
    upstream_dag = _ok_dag(
        "upstream",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="finalize",
                operator="x.Op",
                task_group=None,
                upstream=[],
                downstream=[],
                fields={},
            ),
        ],
    )
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream_dag), Config())
    assert m.reason == "dangling_target"
    assert m.target_task_id == "not_a_real_task"


def test_dangling_target_one_of_task_ids_missing():
    sensor_dag = _ok_dag(
        "downstream",
        schedule="@daily",
        tasks=[
            _sensor_task(
                "wait", external_dag_id="upstream", external_task_ids=["finalize", "missing"]
            ),
        ],
    )
    upstream_dag = _ok_dag(
        "upstream",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="finalize",
                operator="x.Op",
                task_group=None,
                upstream=[],
                downstream=[],
                fields={},
            ),
        ],
    )
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream_dag), Config())
    assert m.reason == "dangling_target"
    assert m.target_task_ids == ["finalize", "missing"]
    assert "missing" in (m.notes or "")


# ===== schedule-equality + execution_date_fn tests (Task 7) =====


def test_schedules_equal_no_mismatch():
    sensor_dag = _ok_dag(
        "d",
        schedule="@daily",
        tasks=[
            _sensor_task("wait", external_dag_id="u", external_task_id="x"),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


def test_schedules_equal_after_normalization():
    # "@midnight" normalizes to "0 0 * * *", same as "@daily"
    sensor_dag = _ok_dag(
        "d",
        schedule="@midnight",
        tasks=[
            _sensor_task("wait", external_dag_id="u", external_task_id="x"),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


def test_schedules_differ_with_execution_date_fn_no_mismatch():
    sensor_dag = _ok_dag(
        "d",
        schedule="@hourly",
        tasks=[
            _sensor_task(
                "wait", external_dag_id="u", external_task_id="x", execution_date_fn_present=True
            ),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


def test_schedules_differ_no_bridge_emits_missing():
    sensor_dag = _ok_dag(
        "downstream",
        schedule="@hourly",
        tasks=[
            _sensor_task("wait", external_dag_id="upstream", external_task_id="x"),
        ],
    )
    upstream = _ok_dag(
        "upstream",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "missing_execution_delta"
    assert m.sensor_schedule == "@hourly"
    assert m.target_schedule == "@daily"
    assert m.notes is None  # both schedules cron-parseable; no opacity note


def test_opaque_target_no_bridge_emits_missing_with_note():
    sensor_dag = _ok_dag(
        "downstream",
        schedule="@hourly",
        tasks=[
            _sensor_task("wait", external_dag_id="upstream", external_task_id="x"),
        ],
    )
    # Opaque schedule (dataset list serialized as JSON list, or any non-cron string)
    upstream = _ok_dag(
        "upstream",
        schedule="@once",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "missing_execution_delta"
    assert "opaque" in (m.notes or "").lower()


def test_wrong_literal_delta_emits_incorrect_mismatch():
    # sensor @hourly, target @daily, delta=1h.
    # synthetic_logical_date = 2025-01-01T00:00:00+00:00 (midnight).
    # target_logical = midnight - 1h = 2024-12-31 23:00, which is NOT a valid
    # @daily logical date (only midnights are). Expected delta should be 0
    # (midnight IS a valid @daily logical date, so most-recent is midnight itself).
    sensor_dag = _ok_dag(
        "d",
        schedule="@hourly",
        tasks=[
            _sensor_task(
                "wait", external_dag_id="u", external_task_id="x", execution_delta_seconds=3600
            ),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "incorrect_execution_delta"
    assert m.actual_delta_seconds == 3600
    assert m.expected_delta_seconds == 0


def test_zero_delta_for_offset_schedules_is_incorrect():
    # sensor = midnight daily, target = noon daily, delta = 0.
    # target_logical = midnight - 0 = midnight, NOT a valid noon-only cron match.
    # Expected delta = midnight - most-recent-noon-at-or-before-midnight
    #                = midnight - 2024-12-31 12:00 = 12h = 43200s.
    sensor_dag = _ok_dag(
        "d",
        schedule="0 0 * * *",
        tasks=[
            _sensor_task(
                "wait", external_dag_id="u", external_task_id="x", execution_delta_seconds=0
            ),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="0 12 * * *",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "incorrect_execution_delta"
    assert m.expected_delta_seconds == 43200
    assert m.actual_delta_seconds == 0


def test_correct_delta_for_offset_schedules_no_mismatch():
    # Sensor @ midnight daily, target @ noon daily, delta = 12h → sensor_date - 12h
    # = prior noon, which IS a valid "0 12 * * *" logical date. No mismatch.
    sensor_dag = _ok_dag(
        "d",
        schedule="0 0 * * *",
        tasks=[
            _sensor_task(
                "wait", external_dag_id="u", external_task_id="x", execution_delta_seconds=43200
            ),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="0 12 * * *",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


def test_evaluator_exception_swallowed(monkeypatch):
    from airflow_diff.validators import cross_dag as mod

    def boom(*a, **kw):
        raise RuntimeError("intentional test failure")

    monkeypatch.setattr(mod, "_evaluate_sensor", boom)

    sensor_dag = _ok_dag(
        "d",
        schedule="@hourly",
        tasks=[
            _sensor_task("wait", external_dag_id="u", external_task_id="x"),
        ],
    )
    upstream = _ok_dag(
        "u",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    # Should not raise; should return empty list.
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


# ===== validate() tests (Task 11) =====


def _bag_with_sensor(
    sensor_dag_schedule: str = "@hourly", target_schedule: str = "@daily"
) -> RenderedDagBag:
    sensor_dag = _ok_dag(
        "downstream",
        schedule=sensor_dag_schedule,
        tasks=[
            _sensor_task("wait", external_dag_id="upstream", external_task_id="x"),
        ],
    )
    upstream = _ok_dag(
        "upstream",
        schedule=target_schedule,
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    return _bag(sensor_dag, upstream)


def test_validate_pr_introduced_mismatch_emitted():
    # Base: schedules aligned. Head: schedules misaligned.
    base = _bag_with_sensor(sensor_dag_schedule="@daily", target_schedule="@daily")
    head = _bag_with_sensor(sensor_dag_schedule="@hourly", target_schedule="@daily")
    result = validate(base, head, Config())
    assert len(result) == 1
    assert result[0].reason == "missing_execution_delta"


def test_validate_pre_existing_mismatch_silenced():
    # Both base and head have the same mismatch → silenced.
    base = _bag_with_sensor()
    head = _bag_with_sensor()
    assert validate(base, head, Config()) == []


def test_validate_same_pair_different_reason_silenced():
    # Base: missing delta. Head: wrong delta (key excludes reason → silenced).
    base = _bag_with_sensor()
    head_sensor = _ok_dag(
        "downstream",
        schedule="@hourly",
        tasks=[
            _sensor_task(
                "wait",
                external_dag_id="upstream",
                external_task_id="x",
                execution_delta_seconds=999,
            ),
        ],
    )
    head_upstream = _ok_dag(
        "upstream",
        schedule="@daily",
        tasks=[
            RenderedTask(
                task_id="x", operator="x.Op", task_group=None, upstream=[], downstream=[], fields={}
            ),
        ],
    )
    head = _bag(head_sensor, head_upstream)
    # Sanity: head alone would report.
    assert len(_mismatches_for_bag(head, Config())) == 1
    # But because the (sensor, target) pair was already broken at base, silenced.
    assert validate(base, head, Config()) == []
