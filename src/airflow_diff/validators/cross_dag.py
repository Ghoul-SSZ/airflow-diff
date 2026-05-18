"""Cross-DAG sensor validation.

Operates only on canonical schema types — no Airflow imports.
Detects PR-introduced ExternalTaskSensor mismatches:

  * missing_execution_delta  — schedules differ; no delta/fn on sensor
  * incorrect_execution_delta — literal delta doesn't match cron arithmetic
  * dangling_target           — sensor target dag/task not in head bag
"""
from __future__ import annotations

from typing import Any, Optional

from airflow_diff.config import Config
from airflow_diff.schema import (
    ExternalTaskRef, RenderedDag, RenderedDagBag, RenderedTask, SensorMismatch,
)

_PRESETS = {
    "@yearly":   "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly":  "0 0 1 * *",
    "@weekly":   "0 0 * * 0",
    "@daily":    "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly":   "0 * * * *",
}


def _normalize_schedule(schedule: Any) -> Optional[str]:
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
            m = _evaluate_sensor(sensor_dag, task, ref, head_dags, config)
            if m is not None:
                out.append(m)
    return out


def _evaluate_sensor(
    sensor_dag: RenderedDag,
    sensor_task: RenderedTask,
    ref: ExternalTaskRef,
    head_dags: dict[str, RenderedDag],
    config: Config,
) -> Optional[SensorMismatch]:
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
    return None  # subsequent rules added in later tasks
