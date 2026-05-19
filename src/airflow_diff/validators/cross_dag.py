"""Cross-DAG sensor validation.

Operates only on canonical schema types — no Airflow imports.
Detects PR-introduced ExternalTaskSensor mismatches:

  * missing_execution_delta  — schedules differ; no delta/fn on sensor
  * incorrect_execution_delta — literal delta doesn't match cron arithmetic
  * dangling_target           — sensor target dag/task not in head bag
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from croniter import croniter

from airflow_diff.config import Config
from airflow_diff.schema import (
    ExternalTaskRef,
    RenderedDag,
    RenderedDagBag,
    RenderedTask,
    SensorMismatch,
)

_PRESETS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def _normalize_schedule(schedule: Any) -> str | None:
    """Return a 5-field cron string if `schedule` is cron-parseable, else None.

    None means "opaque" — either truly None, a preset that has no cron equivalent
    (`@once`, `@continuous`), a timedelta-derived string, a dataset list, or any
    other shape the validator cannot reason about.
    """
    if not isinstance(schedule, str):
        return None
    s = schedule.strip()
    if s in _PRESETS:
        return _PRESETS[s]
    # Heuristic: 5 whitespace-separated fields → assume cron.
    if len(s.split()) == 5:
        return s
    return None


def _mismatch_key(m: SensorMismatch) -> tuple:
    """Stable identity tuple for the PR-introduced silencing gate.

    Intentionally excludes `reason`: a pair that was already broken at base
    stays silenced even if the head-side rule that fires differs.
    """
    if m.target_task_ids is not None:
        target = ("ids", tuple(sorted(m.target_task_ids)))
    else:
        target = ("id", m.target_task_id)
    return (m.sensor_dag_id, m.sensor_task_id, m.target_dag_id, target)


def _mismatches_for_bag(bag: RenderedDagBag, config: Config) -> list[SensorMismatch]:
    out: list[SensorMismatch] = []
    head_dags = {d.dag_id: d for d in bag.dags if d.status == "ok"}
    for sensor_dag in head_dags.values():
        for task in sensor_dag.tasks or []:
            ref = task.external_ref
            if ref is None:
                continue
            try:
                m = _evaluate_sensor(sensor_dag, task, ref, head_dags, config)
            except Exception:
                # Per-sensor isolation: a validator bug must not crash the diff.
                continue
            if m is not None:
                out.append(m)
    return out


def _evaluate_sensor(
    sensor_dag: RenderedDag,
    sensor_task: RenderedTask,
    ref: ExternalTaskRef,
    head_dags: dict[str, RenderedDag],
    config: Config,
) -> SensorMismatch | None:
    # Step 1: dangling-target check
    target_dag = head_dags.get(ref.external_dag_id)
    if target_dag is None:
        return SensorMismatch(
            sensor_dag_id=sensor_dag.dag_id,
            sensor_task_id=sensor_task.task_id,
            target_dag_id=ref.external_dag_id,
            target_task_id=ref.external_task_id,
            target_task_ids=ref.external_task_ids,
            reason="dangling_target",
        )
    target_task_ids = {t.task_id for t in (target_dag.tasks or [])}
    if ref.external_task_id is not None and ref.external_task_id not in target_task_ids:
        return SensorMismatch(
            sensor_dag_id=sensor_dag.dag_id,
            sensor_task_id=sensor_task.task_id,
            target_dag_id=ref.external_dag_id,
            target_task_id=ref.external_task_id,
            reason="dangling_target",
        )
    if ref.external_task_ids is not None:
        missing = [tid for tid in ref.external_task_ids if tid not in target_task_ids]
        if missing:
            return SensorMismatch(
                sensor_dag_id=sensor_dag.dag_id,
                sensor_task_id=sensor_task.task_id,
                target_dag_id=ref.external_dag_id,
                target_task_ids=ref.external_task_ids,
                reason="dangling_target",
                notes=f"missing target task ids: {', '.join(missing)}",
            )

    # Step 2: schedule equality
    sensor_schedule = (sensor_dag.attrs or {}).get("schedule")
    target_schedule = (target_dag.attrs or {}).get("schedule")
    sensor_norm = _normalize_schedule(sensor_schedule)
    target_norm = _normalize_schedule(target_schedule)
    if sensor_norm is not None and target_norm is not None and sensor_norm == target_norm:
        return None
    if sensor_norm is None and target_norm is None and sensor_schedule == target_schedule:
        return None

    # Step 3: execution_date_fn short-circuit (treat as user-managed)
    if ref.execution_date_fn_present:
        return None

    def _str(s: Any) -> str | None:
        if s is None:
            return None
        return s if isinstance(s, str) else repr(s)

    # Step 4: missing bridge
    if ref.execution_delta_seconds is None:
        notes = None
        if target_norm is None:
            notes = "target schedule is opaque; cannot suggest a specific execution_delta value"
        return SensorMismatch(
            sensor_dag_id=sensor_dag.dag_id,
            sensor_task_id=sensor_task.task_id,
            target_dag_id=target_dag.dag_id,
            target_task_id=ref.external_task_id,
            target_task_ids=ref.external_task_ids,
            reason="missing_execution_delta",
            sensor_schedule=_str(sensor_schedule),
            target_schedule=_str(target_schedule),
            notes=notes,
        )

    # Step 5: literal-delta cron arithmetic
    if target_norm is not None and sensor_norm is not None:
        sensor_logical_date = config.synthetic_logical_date
        target_logical_date = sensor_logical_date - timedelta(seconds=ref.execution_delta_seconds)
        if croniter.match(target_norm, target_logical_date):
            return None
        # Compute expected delta: most recent valid logical date of target_norm at or
        # before sensor_logical_date.
        itr = croniter(target_norm, sensor_logical_date)
        if croniter.match(target_norm, sensor_logical_date):
            expected_prev = sensor_logical_date
        else:
            expected_prev = itr.get_prev(ret_type=type(sensor_logical_date))
        expected_delta = int((sensor_logical_date - expected_prev).total_seconds())
        return SensorMismatch(
            sensor_dag_id=sensor_dag.dag_id,
            sensor_task_id=sensor_task.task_id,
            target_dag_id=target_dag.dag_id,
            target_task_id=ref.external_task_id,
            target_task_ids=ref.external_task_ids,
            reason="incorrect_execution_delta",
            sensor_schedule=_str(sensor_schedule),
            target_schedule=_str(target_schedule),
            expected_delta_seconds=expected_delta,
            actual_delta_seconds=ref.execution_delta_seconds,
        )

    # Step 6: opaque-schedule fallthrough — delta is present, can't verify, skip silently
    return None


def validate(
    base_bag: RenderedDagBag,
    head_bag: RenderedDagBag,
    config: Config,
) -> list[SensorMismatch]:
    """Returns mismatches present in head that were NOT present in base."""
    base_keys = {_mismatch_key(m) for m in _mismatches_for_bag(base_bag, config)}
    head = _mismatches_for_bag(head_bag, config)
    return [m for m in head if _mismatch_key(m) not in base_keys]
