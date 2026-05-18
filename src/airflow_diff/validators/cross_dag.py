"""Cross-DAG sensor validation.

Operates only on canonical schema types — no Airflow imports.
Detects PR-introduced ExternalTaskSensor mismatches:

  * missing_execution_delta  — schedules differ; no delta/fn on sensor
  * incorrect_execution_delta — literal delta doesn't match cron arithmetic
  * dangling_target           — sensor target dag/task not in head bag
"""
from __future__ import annotations

from typing import Any, Optional

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
