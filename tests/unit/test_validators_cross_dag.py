from airflow_diff.schema import SensorMismatch
from airflow_diff.validators.cross_dag import _normalize_schedule, _mismatch_key


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
