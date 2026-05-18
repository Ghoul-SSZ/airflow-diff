# Cross-DAG Sensor Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect and report PR-introduced `ExternalTaskSensor` configurations whose target DAG has a different schedule and lacks an alignment bridge (`execution_delta` / `execution_date_fn`), plus dangling sensor targets and incorrect literal delta values verified via cron arithmetic.

**Architecture:** New optional `external_ref` field on `RenderedTask` populated by the renderer for any task whose MRO contains `ExternalTaskSensor`. New `validators/cross_dag.py` module called by the orchestrator after `compute_diff()`; results are attached to `DiffDocument.sensor_mismatches`. Markdown/terminal presenters gain a "Cross-DAG sensor mismatches" section above the per-DAG details. `cli._exit_code` consults a new config flag `fail_on_sensor_mismatch`.

**Tech Stack:** Python 3.10+, Pydantic v2, `croniter>=2.0` (new host runtime dep), pytest, syrupy. No new build/runtime tooling.

**Spec:** `docs/superpowers/specs/2026-05-18-cross-dag-sensor-validation-design.md`

---

## File Map

**New files:**
- `src/airflow_diff/validators/__init__.py` — empty package marker
- `src/airflow_diff/validators/cross_dag.py` — validator module
- `tests/unit/test_validators_cross_dag.py` — unit tests for validator

**Modified files:**
- `src/airflow_diff/schema.py` — add `ExternalTaskRef`, `SensorMismatch`; add fields; bump `SCHEMA_VERSION`
- `src/airflow_diff/renderer.py` — add `_extract_external_ref`; call site in `_render_dag`
- `src/airflow_diff/orchestrator.py` — call validator after `compute_diff`
- `src/airflow_diff/cli.py` — update `_exit_code` signature
- `src/airflow_diff/config.py` — add `fail_on_sensor_mismatch`
- `src/airflow_diff/present/markdown.py` — render sensor-mismatch section + summary row
- `src/airflow_diff/present/terminal.py` — render ANSI section
- `pyproject.toml` — add `croniter>=2.0`
- `tests/unit/test_schema.py` — round-trip new models; assert version bump
- `tests/unit/test_config.py` — assert `fail_on_sensor_mismatch` default + override
- `tests/unit/test_cli.py` — exit-code branch for sensor mismatches; update `schema_version=1` literals to `SCHEMA_VERSION`
- `tests/unit/test_orchestrator.py` — update `schema_version=1` literals to `SCHEMA_VERSION`
- `tests/unit/present/test_markdown.py` — section rendering + summary row + truncation
- `tests/unit/present/test_terminal.py` — ANSI color per reason
- `tests/integration/test_renderer.py` — assert `external_ref` extraction (three cases)
- `tests/integration/test_cli.py` — end-to-end paired-DAGs run
- `tests/fixtures/sample_repo_builder.py` — add `mode="paired_dags"` branch
- `tests/fixtures/config/full.toml` — add `fail_on_sensor_mismatch = true` entry (and assert in test_config)

---

## Task 1: Schema additions + version bump

**Files:**
- Modify: `src/airflow_diff/schema.py`
- Modify: `tests/unit/test_schema.py`
- Modify: `tests/unit/test_cli.py` (replace literal `schema_version=1` with `SCHEMA_VERSION`)
- Modify: `tests/unit/test_orchestrator.py` (same)

- [ ] **Step 1: Write failing tests for the new models in `tests/unit/test_schema.py`**

Append to the existing file:

```python
from airflow_diff.schema import ExternalTaskRef, SensorMismatch


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
        external_task_ids=None,
        external_task_group_id=None,
        execution_delta_seconds=3600,
        execution_date_fn_present=False,
    )
    assert ExternalTaskRef.model_validate_json(ref.model_dump_json()) == ref


def test_external_task_ref_rejects_unknown_kind():
    with pytest.raises(ValidationError):
        ExternalTaskRef(kind="trigger_dag_run", external_dag_id="x")


def test_rendered_task_external_ref_defaults_to_none():
    task = RenderedTask(task_id="t", operator="x.Op", task_group=None,
                        upstream=[], downstream=[], fields={})
    assert task.external_ref is None


def test_sensor_mismatch_round_trip():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="missing_execution_delta",
        sensor_schedule="@hourly", target_schedule="@daily",
    )
    assert SensorMismatch.model_validate_json(m.model_dump_json()) == m


def test_sensor_mismatch_rejects_unknown_reason():
    with pytest.raises(ValidationError):
        SensorMismatch(
            sensor_dag_id="d", sensor_task_id="t",
            target_dag_id="u", reason="bogus",
        )


def test_diff_document_sensor_mismatches_default_empty():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha="a", head_sha="b",
        summary=DiffSummary(), dags=[], render_errors=[],
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
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `pytest tests/unit/test_schema.py -v -k "external_task_ref or sensor_mismatch or schema_version_is_2 or rejects_v1"`
Expected: ImportError or AttributeError — the new symbols don't exist; `SCHEMA_VERSION == 2` assertion fails.

- [ ] **Step 3: Implement schema changes in `src/airflow_diff/schema.py`**

At the top of the file change:
```python
SCHEMA_VERSION = 1
```
to:
```python
SCHEMA_VERSION = 2
```

In `class RenderedDagBag`, change:
```python
schema_version: Literal[1]
```
to:
```python
schema_version: Literal[2]
```

In `class DiffDocument`, change:
```python
schema_version: Literal[1]
```
to:
```python
schema_version: Literal[2]
```

Add a new model just below `RenderedField` (before `DagStatus`):

```python
class ExternalTaskRef(_Model):
    """Cross-DAG metadata captured from an ExternalTaskSensor instance."""
    kind: Literal["external_task_sensor"]
    external_dag_id: str
    external_task_id: Optional[str] = None
    external_task_ids: Optional[list[str]] = None
    external_task_group_id: Optional[str] = None
    execution_delta_seconds: Optional[int] = None
    execution_date_fn_present: bool = False
```

In `class RenderedTask`, add one field at the bottom:

```python
    external_ref: Optional[ExternalTaskRef] = None
```

After `class RenderErrorEntry`, before `class DiffDocument`, add:

```python
class SensorMismatch(_Model):
    sensor_dag_id: str
    sensor_task_id: str
    target_dag_id: str
    target_task_id: Optional[str] = None
    target_task_ids: Optional[list[str]] = None
    reason: Literal[
        "missing_execution_delta",
        "incorrect_execution_delta",
        "dangling_target",
    ]
    sensor_schedule: Optional[str] = None
    target_schedule: Optional[str] = None
    expected_delta_seconds: Optional[int] = None
    actual_delta_seconds: Optional[int] = None
    notes: Optional[str] = None
```

In `class DiffDocument`, add one field at the bottom:

```python
    sensor_mismatches: list[SensorMismatch] = Field(default_factory=list)
```

- [ ] **Step 4: Update existing literal `schema_version=1` references**

In `tests/unit/test_cli.py`, replace the two occurrences of `schema_version=1` with `schema_version=SCHEMA_VERSION` and add `SCHEMA_VERSION` to the import line:

```python
from airflow_diff.schema import DiffDocument, DiffSummary, SCHEMA_VERSION
```

In `tests/unit/test_orchestrator.py`, replace the two occurrences of `schema_version=1` in the bag-building code with `schema_version=SCHEMA_VERSION` and add `SCHEMA_VERSION` to the import:

```python
from airflow_diff.schema import DiffDocument, RenderedDagBag, SCHEMA_VERSION
```

- [ ] **Step 5: Run the full unit suite to confirm everything passes**

Run: `pytest tests/unit -v`
Expected: PASS for all (including new schema tests; updated cli/orchestrator tests).

- [ ] **Step 6: Commit**

```bash
git add src/airflow_diff/schema.py tests/unit/test_schema.py tests/unit/test_cli.py tests/unit/test_orchestrator.py
git commit -m "feat(schema): add ExternalTaskRef + SensorMismatch; bump SCHEMA_VERSION to 2"
```

---

## Task 2: Config field `fail_on_sensor_mismatch`

**Files:**
- Modify: `src/airflow_diff/config.py`
- Modify: `tests/unit/test_config.py`
- Modify: `tests/fixtures/config/full.toml`

- [ ] **Step 1: Write failing test in `tests/unit/test_config.py`**

Append:

```python
def test_fail_on_sensor_mismatch_default_false(tmp_path: Path):
    cfg = load_config(tmp_path)
    assert cfg.fail_on_sensor_mismatch is False


def test_fail_on_sensor_mismatch_loads_from_toml(tmp_path: Path):
    (tmp_path / ".airflow-diff.toml").write_text((FIXTURES / "full.toml").read_text())
    cfg = load_config(tmp_path)
    assert cfg.fail_on_sensor_mismatch is True
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_config.py::test_fail_on_sensor_mismatch_default_false tests/unit/test_config.py::test_fail_on_sensor_mismatch_loads_from_toml -v`
Expected: First test errors with `AttributeError: 'Config' object has no attribute 'fail_on_sensor_mismatch'`; second test errors on the fixture (key not present yet) or on the same AttributeError.

- [ ] **Step 3: Implement**

In `src/airflow_diff/config.py`, add to the `Config` model (alongside `render_timeout_seconds`):

```python
    fail_on_sensor_mismatch: bool = False
```

Append to `tests/fixtures/config/full.toml`:

```toml
fail_on_sensor_mismatch = true
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_config.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/config.py tests/unit/test_config.py tests/fixtures/config/full.toml
git commit -m "feat(config): fail_on_sensor_mismatch flag (default false)"
```

---

## Task 3: Add `croniter` runtime dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add dependency**

In `pyproject.toml`, under `[project] dependencies`, add `"croniter>=2.0"`:

```toml
dependencies = [
    "pydantic>=2.5",
    "PyYAML>=6.0",
    "croniter>=2.0",
    "tomli>=2.0; python_version<'3.11'",
]
```

- [ ] **Step 2: Install and smoke-import**

Run:
```bash
uv pip install -e ".[dev]"
python -c "from croniter import croniter; print(croniter('@daily', __import__('datetime').datetime(2025,1,1)).get_prev())"
```
Expected: prints `2024-12-31 00:00:00` (or similar — confirms croniter is importable and working).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "build: add croniter>=2.0 runtime dep for cross-DAG validator"
```

---

## Task 4: Validator package + cron normalization

**Files:**
- Create: `src/airflow_diff/validators/__init__.py`
- Create: `src/airflow_diff/validators/cross_dag.py`
- Create: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_validators_cross_dag.py`:

```python
from airflow_diff.validators.cross_dag import _normalize_schedule


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
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: `ModuleNotFoundError: No module named 'airflow_diff.validators'`.

- [ ] **Step 3: Implement package + normalization**

Create `src/airflow_diff/validators/__init__.py` as an empty file (zero bytes).

Create `src/airflow_diff/validators/cross_dag.py`:

```python
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
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS for all normalization tests.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/__init__.py src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): cross_dag module skeleton + schedule normalization"
```

---

## Task 5: Mismatch-key helper

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing test**

Append to `tests/unit/test_validators_cross_dag.py`:

```python
from airflow_diff.schema import SensorMismatch
from airflow_diff.validators.cross_dag import _mismatch_key


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
    )
    assert _mismatch_key(a) == _mismatch_key(b)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -k mismatch_key -v`
Expected: `ImportError: cannot import name '_mismatch_key'`.

- [ ] **Step 3: Implement**

In `src/airflow_diff/validators/cross_dag.py`, add the import and helper:

```python
from airflow_diff.schema import SensorMismatch


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
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): mismatch-key helper (reason-free for PR-introduced gate)"
```

---

## Task 6: Dangling-target rule (Step 1 of rule tree)

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing test**

Append to the test file:

```python
from datetime import datetime, timezone

from airflow_diff.schema import (
    ExternalTaskRef, RenderedDag, RenderedDagBag, RenderedTask, SCHEMA_VERSION,
)
from airflow_diff.config import Config
from airflow_diff.validators.cross_dag import _mismatches_for_bag


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
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -k dangling -v`
Expected: `ImportError: cannot import name '_mismatches_for_bag'`.

- [ ] **Step 3: Implement**

In `src/airflow_diff/validators/cross_dag.py`, add imports and the function:

```python
from airflow_diff.config import Config
from airflow_diff.schema import (
    ExternalTaskRef, RenderedDag, RenderedDagBag, RenderedTask, SensorMismatch,
)


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
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): dangling-target rule (sensor target dag/task not in bag)"
```

---

## Task 7: Schedule-equality + `execution_date_fn` short-circuit (Steps 2 + 3)

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing tests**

Append:

```python
def test_schedules_equal_no_mismatch():
    sensor_dag = _ok_dag("d", schedule="@daily", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x"),
    ])
    upstream = _ok_dag("u", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


def test_schedules_equal_after_normalization():
    # "@midnight" normalizes to "0 0 * * *", same as "@daily"
    sensor_dag = _ok_dag("d", schedule="@midnight", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x"),
    ])
    upstream = _ok_dag("u", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []


def test_schedules_differ_with_execution_date_fn_no_mismatch():
    sensor_dag = _ok_dag("d", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x",
                     execution_date_fn_present=True),
    ])
    upstream = _ok_dag("u", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -k "schedules_equal or execution_date_fn" -v`
Expected: tests FAIL — the `_evaluate_sensor` after the dangling check returns `None` for everything, so `test_schedules_differ_with_execution_date_fn_no_mismatch` already passes; the equality tests pass too because nothing emits. Wait — that means we need a positive test. Add it now:

Append:

```python
def test_schedules_differ_missing_bridge_placeholder():
    """Without delta or fn the validator must emit. This test will be unblocked
    once Task 8 lands the missing-bridge rule; for now it asserts the current
    pre-rule behavior to make the regression visible."""
    sensor_dag = _ok_dag("d", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x"),
    ])
    upstream = _ok_dag("u", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    # Pre-Task-8 behavior: no rule yet → no emission. Task 8 changes this to 1.
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []
```

- [ ] **Step 3: Implement Steps 2 + 3 of the rule tree**

In `src/airflow_diff/validators/cross_dag.py`, replace the `return None  # subsequent rules added in later tasks` line with:

```python
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

    return None  # remaining rules added in later tasks
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS (all existing tests, including the placeholder, still pass).

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): schedule-equality + execution_date_fn short-circuits"
```

---

## Task 8: Missing-bridge rule (Step 4)

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Replace placeholder + add new tests**

In `tests/unit/test_validators_cross_dag.py`, replace `test_schedules_differ_missing_bridge_placeholder` with:

```python
def test_schedules_differ_no_bridge_emits_missing():
    sensor_dag = _ok_dag("downstream", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="upstream", external_task_id="x"),
    ])
    upstream = _ok_dag("upstream", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "missing_execution_delta"
    assert m.sensor_schedule == "@hourly"
    assert m.target_schedule == "@daily"
    assert m.notes is None  # both schedules cron-parseable; no opacity note


def test_opaque_target_no_bridge_emits_missing_with_note():
    sensor_dag = _ok_dag("downstream", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="upstream", external_task_id="x"),
    ])
    # Opaque schedule (dataset list serialized as JSON list, or any non-cron string)
    upstream = _ok_dag("upstream", schedule="@once", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "missing_execution_delta"
    assert "opaque" in (m.notes or "").lower()
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -k "no_bridge_emits or opaque_target_no_bridge" -v`
Expected: FAIL — `_mismatches_for_bag` returns `[]` instead of 1 mismatch.

- [ ] **Step 3: Implement Step 4**

In `src/airflow_diff/validators/cross_dag.py`, replace the trailing `return None  # remaining rules added in later tasks` with:

```python
    def _str(s: Any) -> Optional[str]:
        if s is None:
            return None
        return s if isinstance(s, str) else repr(s)

    # Step 4: missing bridge
    if ref.execution_delta_seconds is None:
        notes = None
        if target_norm is None:
            notes = (
                "target schedule is opaque; cannot suggest a specific "
                "execution_delta value"
            )
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

    return None  # delta-math rule added next
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS for all.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): missing-bridge rule (no execution_delta/fn when schedules differ)"
```

---

## Task 9: Literal-delta cron arithmetic (Step 5)

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing tests**

Append:

```python
def test_wrong_literal_delta_emits_incorrect_mismatch():
    # sensor @hourly, target @daily, delta=1h.
    # synthetic_logical_date = 2025-01-01T00:00:00+00:00 (midnight).
    # target_logical = midnight - 1h = 2024-12-31 23:00, which is NOT a valid
    # @daily logical date (only midnights are). Expected delta should be 0
    # (midnight IS a valid @daily logical date, so most-recent is midnight itself).
    sensor_dag = _ok_dag("d", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x",
                     execution_delta_seconds=3600),
    ])
    upstream = _ok_dag("u", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "incorrect_execution_delta"
    assert m.actual_delta_seconds == 3600
    assert m.expected_delta_seconds == 0


def test_zero_delta_for_offset_schedules_is_incorrect():
    # sensor = midnight daily, target = noon daily, delta = 0.
    # target_logical = midnight - 0 = midnight, NOT a valid noon-only cron match.
    # Expected delta = midnight - most-recent-noon-at-or-before-midnight
    #                = midnight - 2024-12-31 12:00 = 12h = 43200s.
    sensor_dag = _ok_dag("d", schedule="0 0 * * *", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x",
                     execution_delta_seconds=0),
    ])
    upstream = _ok_dag("u", schedule="0 12 * * *", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    [m] = _mismatches_for_bag(_bag(sensor_dag, upstream), Config())
    assert m.reason == "incorrect_execution_delta"
    assert m.expected_delta_seconds == 43200
    assert m.actual_delta_seconds == 0


def test_correct_delta_for_offset_schedules_no_mismatch():
    # Sensor @ midnight daily, target @ noon daily, delta = 12h → sensor_date - 12h
    # = prior noon, which IS a valid "0 12 * * *" logical date. No mismatch.
    sensor_dag = _ok_dag("d", schedule="0 0 * * *", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x",
                     execution_delta_seconds=43200),
    ])
    upstream = _ok_dag("u", schedule="0 12 * * *", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -k "literal_delta or zero_delta or correct_delta_for_offset" -v`
Expected: FAIL — the validator currently returns `None` after the missing-bridge rule.

- [ ] **Step 3: Implement Step 5**

In `src/airflow_diff/validators/cross_dag.py`, add the croniter import at the top:

```python
from datetime import timedelta

from croniter import croniter
```

Replace the trailing `return None  # delta-math rule added next` with:

```python
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
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): literal-delta cron arithmetic via croniter"
```

---

## Task 10: Failure isolation wrapper

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing test**

Append:

```python
def test_evaluator_exception_swallowed(monkeypatch):
    from airflow_diff.validators import cross_dag as mod

    def boom(*a, **kw):
        raise RuntimeError("intentional test failure")

    monkeypatch.setattr(mod, "_evaluate_sensor", boom)

    sensor_dag = _ok_dag("d", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="u", external_task_id="x"),
    ])
    upstream = _ok_dag("u", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    # Should not raise; should return empty list.
    assert _mismatches_for_bag(_bag(sensor_dag, upstream), Config()) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py::test_evaluator_exception_swallowed -v`
Expected: FAIL — `RuntimeError: intentional test failure` propagates.

- [ ] **Step 3: Implement try/except in `_mismatches_for_bag`**

In `src/airflow_diff/validators/cross_dag.py`, replace the body of `_mismatches_for_bag` to wrap the inner call:

```python
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
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): per-sensor exception isolation"
```

---

## Task 11: Public `validate()` + PR-introduced gate

**Files:**
- Modify: `src/airflow_diff/validators/cross_dag.py`
- Modify: `tests/unit/test_validators_cross_dag.py`

- [ ] **Step 1: Write failing tests**

Append:

```python
from airflow_diff.validators.cross_dag import validate


def _bag_with_sensor(sensor_dag_schedule: str = "@hourly",
                     target_schedule: str = "@daily") -> RenderedDagBag:
    sensor_dag = _ok_dag("downstream", schedule=sensor_dag_schedule, tasks=[
        _sensor_task("wait", external_dag_id="upstream", external_task_id="x"),
    ])
    upstream = _ok_dag("upstream", schedule=target_schedule, tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
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
    head_sensor = _ok_dag("downstream", schedule="@hourly", tasks=[
        _sensor_task("wait", external_dag_id="upstream", external_task_id="x",
                     execution_delta_seconds=999),
    ])
    head_upstream = _ok_dag("upstream", schedule="@daily", tasks=[
        RenderedTask(task_id="x", operator="x.Op", task_group=None,
                     upstream=[], downstream=[], fields={}),
    ])
    head = _bag(head_sensor, head_upstream)
    # Sanity: head alone would report.
    assert len(_mismatches_for_bag(head, Config())) == 1
    # But because the (sensor, target) pair was already broken at base, silenced.
    assert validate(base, head, Config()) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_validators_cross_dag.py -k validate_pr_introduced -v`
Expected: `ImportError: cannot import name 'validate'`.

- [ ] **Step 3: Implement**

In `src/airflow_diff/validators/cross_dag.py`, add:

```python
def validate(
    base_bag: RenderedDagBag,
    head_bag: RenderedDagBag,
    config: Config,
) -> list[SensorMismatch]:
    """Returns mismatches present in head that were NOT present in base."""
    base_keys = {_mismatch_key(m) for m in _mismatches_for_bag(base_bag, config)}
    head = _mismatches_for_bag(head_bag, config)
    return [m for m in head if _mismatch_key(m) not in base_keys]
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_validators_cross_dag.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/validators/cross_dag.py tests/unit/test_validators_cross_dag.py
git commit -m "feat(validators): public validate() with PR-introduced silencing gate"
```

---

## Task 12: Renderer `external_ref` extraction

**Files:**
- Modify: `src/airflow_diff/renderer.py`
- Modify: `tests/integration/test_renderer.py`
- Create: `tests/fixtures/dags_sensors/with_delta.py`
- Create: `tests/fixtures/dags_sensors/with_fn.py`
- Create: `tests/fixtures/dags_sensors/subclass.py`

- [ ] **Step 1: Create three DAG fixtures**

Create `tests/fixtures/dags_sensors/with_delta.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="with_delta",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    ExternalTaskSensor(
        task_id="wait",
        external_dag_id="some_upstream",
        external_task_id="finalize",
        execution_delta=timedelta(hours=1),
    )
```

Create `tests/fixtures/dags_sensors/with_fn.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="with_fn",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    ExternalTaskSensor(
        task_id="wait",
        external_dag_id="some_upstream",
        external_task_id="finalize",
        execution_date_fn=lambda dt: dt,
    )
```

Create `tests/fixtures/dags_sensors/subclass.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor


class WrappedSensor(ExternalTaskSensor):
    """House wrapper to verify MRO-walk detection."""
    pass


with DAG(
    dag_id="subclass_sensor",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    WrappedSensor(
        task_id="wait",
        external_dag_id="some_upstream",
        external_task_id="finalize",
    )
```

- [ ] **Step 2: Write failing integration tests**

Append to `tests/integration/test_renderer.py`:

```python
def test_external_ref_with_timedelta_execution_delta(tmp_path: Path):
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "with_delta.py").write_text(
        (FIXTURES_ROOT / "dags_sensors" / "with_delta.py").read_text()
    )
    bag = _run_renderer(tmp_path)
    [dag] = bag.dags
    [task] = dag.tasks
    assert task.external_ref is not None
    assert task.external_ref.kind == "external_task_sensor"
    assert task.external_ref.external_dag_id == "some_upstream"
    assert task.external_ref.external_task_id == "finalize"
    assert task.external_ref.execution_delta_seconds == 3600
    assert task.external_ref.execution_date_fn_present is False


def test_external_ref_with_execution_date_fn(tmp_path: Path):
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "with_fn.py").write_text(
        (FIXTURES_ROOT / "dags_sensors" / "with_fn.py").read_text()
    )
    bag = _run_renderer(tmp_path)
    [dag] = bag.dags
    [task] = dag.tasks
    assert task.external_ref is not None
    assert task.external_ref.execution_delta_seconds is None
    assert task.external_ref.execution_date_fn_present is True


def test_external_ref_user_subclass_via_mro(tmp_path: Path):
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "subclass.py").write_text(
        (FIXTURES_ROOT / "dags_sensors" / "subclass.py").read_text()
    )
    bag = _run_renderer(tmp_path)
    [dag] = bag.dags
    [task] = dag.tasks
    assert task.external_ref is not None
    assert task.external_ref.external_dag_id == "some_upstream"
```

- [ ] **Step 3: Run to verify failure**

Run: `pytest tests/integration/test_renderer.py -v -m integration -k external_ref`
Expected: FAIL — `task.external_ref` is `None` (field defaults; renderer hasn't been taught to extract).

- [ ] **Step 4: Implement extractor + wire into `_render_dag`**

In `src/airflow_diff/renderer.py`, add `timedelta` to the top-level imports:

```python
from datetime import datetime, timedelta, timezone
```

In the schema-import block inside `_render_dag` (currently importing `RenderedDag, RenderedTask, RenderedField, ProvenanceEntry, DatasetRefs, TaskGroupInfo`), add `ExternalTaskRef`:

```python
    from airflow_diff.schema import (
        RenderedDag, RenderedTask, RenderedField, ProvenanceEntry,
        DatasetRefs, TaskGroupInfo, ExternalTaskRef,
    )
```

Add a helper function above `_render_dag` (module-level):

```python
def _extract_external_ref(task) -> "ExternalTaskRef | None":
    """Capture cross-DAG metadata for any task whose MRO contains ExternalTaskSensor.

    Uses class-name MRO walk rather than isinstance to avoid hard-importing
    airflow.sensors.external_task (defensive, matches _extract_dataset_uris style).
    """
    from datetime import timedelta as _td
    from airflow_diff.schema import ExternalTaskRef

    if not any(c.__name__ == "ExternalTaskSensor" for c in type(task).__mro__):
        return None

    delta = getattr(task, "execution_delta", None)
    delta_seconds = int(delta.total_seconds()) if isinstance(delta, _td) else None

    return ExternalTaskRef(
        kind="external_task_sensor",
        external_dag_id=getattr(task, "external_dag_id", "") or "",
        external_task_id=getattr(task, "external_task_id", None),
        external_task_ids=list(getattr(task, "external_task_ids", []) or []) or None,
        external_task_group_id=getattr(task, "external_task_group_id", None),
        execution_delta_seconds=delta_seconds,
        execution_date_fn_present=callable(getattr(task, "execution_date_fn", None)),
    )
```

Inside `_render_dag`, modify the per-task block where `RenderedTask(...)` is constructed. Currently:

```python
        tasks_out.append(RenderedTask(
            task_id=task.task_id,
            operator=f"{type(task).__module__}.{type(task).__name__}",
            task_group=tg_id,
            upstream=sorted(t.task_id for t in task.upstream_list),
            downstream=sorted(t.task_id for t in task.downstream_list),
            fields=fields,
        ))
```

Replace with:

```python
        try:
            external_ref = _extract_external_ref(task)
        except Exception:
            external_ref = None  # per-task isolation matches existing policy
        tasks_out.append(RenderedTask(
            task_id=task.task_id,
            operator=f"{type(task).__module__}.{type(task).__name__}",
            task_group=tg_id,
            upstream=sorted(t.task_id for t in task.upstream_list),
            downstream=sorted(t.task_id for t in task.downstream_list),
            fields=fields,
            external_ref=external_ref,
        ))
```

- [ ] **Step 5: Run to verify pass**

Run: `pytest tests/integration/test_renderer.py -v -m integration`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/airflow_diff/renderer.py tests/integration/test_renderer.py tests/fixtures/dags_sensors/
git commit -m "feat(renderer): extract ExternalTaskRef from ExternalTaskSensor instances (incl. subclasses)"
```

---

## Task 13: Orchestrator wires validator into `run_diff`

**Files:**
- Modify: `src/airflow_diff/orchestrator.py`
- Modify: `tests/unit/test_orchestrator.py`

- [ ] **Step 1: Write failing test**

Append to `tests/unit/test_orchestrator.py`:

```python
def test_orchestrator_attaches_sensor_mismatches(tmp_path, monkeypatch):
    from airflow_diff import orchestrator
    from airflow_diff.config import Config
    from airflow_diff.schema import (
        ExternalTaskRef, RenderedDag, RenderedDagBag, RenderedTask, SCHEMA_VERSION,
    )

    # Build a head bag with a sensor missing its bridge; base bag has the sensor's
    # DAG on the same schedule so the mismatch is PR-introduced.
    def _bag(commit_sha, sensor_schedule):
        sensor_dag = RenderedDag(
            dag_id="downstream", status="ok", source_file="dags/d.py",
            attrs={"schedule": sensor_schedule},
            datasets={"inlets": [], "outlets": []},
            task_groups=[],
            tasks=[RenderedTask(
                task_id="wait",
                operator="airflow.sensors.external_task.ExternalTaskSensor",
                task_group=None, upstream=[], downstream=[], fields={},
                external_ref=ExternalTaskRef(
                    kind="external_task_sensor",
                    external_dag_id="upstream",
                    external_task_id="x",
                ),
            )],
        )
        upstream = RenderedDag(
            dag_id="upstream", status="ok", source_file="dags/u.py",
            attrs={"schedule": "@daily"},
            datasets={"inlets": [], "outlets": []},
            task_groups=[],
            tasks=[RenderedTask(task_id="x", operator="x.Op", task_group=None,
                                upstream=[], downstream=[], fields={})],
        )
        return RenderedDagBag(
            schema_version=SCHEMA_VERSION, commit_sha=commit_sha,
            airflow_version="2.10.3",
            rendered_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
            dags=[sensor_dag, upstream],
        ).model_dump_json()

    base_json = _bag("aaa", "@daily")  # aligned → no mismatch at base
    head_json = _bag("bbb", "@hourly")  # misaligned → PR-introduced

    monkeypatch.setattr(orchestrator, "resolve_sha", lambda r, s: s + "0" * (40 - len(s)))
    monkeypatch.setattr(orchestrator, "ensure_sha_present", lambda r, s: None)

    from contextlib import contextmanager
    @contextmanager
    def fake_wt(repo, sha, **kw):
        p = tmp_path / sha
        p.mkdir(exist_ok=True)
        yield p
    monkeypatch.setattr(orchestrator, "worktree_for", fake_wt)
    monkeypatch.setattr(orchestrator, "venv_for", lambda wt, **kw: Path("/usr/bin/python3"))
    monkeypatch.setattr(orchestrator, "_touched_files", lambda r, a, b: [])

    call_count = {"n": 0}
    def fake_popen(args, **kw):
        proc = MagicMock()
        proc.communicate.return_value = (head_json if call_count["n"] else base_json, "")
        proc.returncode = 0
        call_count["n"] += 1
        return proc
    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    diff = orchestrator.run_diff(tmp_path, "aaa", "bbb", Config())
    assert len(diff.sensor_mismatches) == 1
    assert diff.sensor_mismatches[0].reason == "missing_execution_delta"
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_orchestrator.py::test_orchestrator_attaches_sensor_mismatches -v`
Expected: FAIL — `diff.sensor_mismatches` is empty (orchestrator doesn't call validator yet).

- [ ] **Step 3: Implement**

In `src/airflow_diff/orchestrator.py`, add the import near the top:

```python
from airflow_diff.validators.cross_dag import validate as _validate_cross_dag
```

In `run_diff`, after the `compute_diff(...)` call, replace:

```python
    return compute_diff(rendered_base, rendered_head, touched_files=touched)
```

with:

```python
    diff = compute_diff(rendered_base, rendered_head, touched_files=touched)
    diff.sensor_mismatches = _validate_cross_dag(rendered_base, rendered_head, config)
    return diff
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_orchestrator.py -v`
Expected: PASS (both existing and new tests).

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/orchestrator.py tests/unit/test_orchestrator.py
git commit -m "feat(orchestrator): run cross-DAG validator after compute_diff"
```

---

## Task 14: CLI exit-code consults `fail_on_sensor_mismatch`

**Files:**
- Modify: `src/airflow_diff/cli.py`
- Modify: `tests/unit/test_cli.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_cli.py`:

```python
from airflow_diff.config import Config


def test_cli_exit_zero_when_sensor_mismatches_default(monkeypatch, tmp_path):
    from airflow_diff.schema import SensorMismatch
    def fake_run_diff(repo, a, b, config):
        return DiffDocument(
            schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
            summary=DiffSummary(), dags=[], render_errors=[],
            sensor_mismatches=[SensorMismatch(
                sensor_dag_id="d", sensor_task_id="t",
                target_dag_id="u", target_task_id="x",
                reason="missing_execution_delta",
            )],
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 0  # default config does not fail on sensor mismatches


def test_cli_exit_one_when_fail_on_sensor_mismatch(monkeypatch, tmp_path):
    from airflow_diff.schema import SensorMismatch

    def fake_run_diff(repo, a, b, config):
        return DiffDocument(
            schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
            summary=DiffSummary(), dags=[], render_errors=[],
            sensor_mismatches=[SensorMismatch(
                sensor_dag_id="d", sensor_task_id="t",
                target_dag_id="u", target_task_id="x",
                reason="missing_execution_delta",
            )],
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    monkeypatch.setattr(cli, "load_config", lambda repo: Config(fail_on_sensor_mismatch=True))
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_cli.py -k "exit_zero_when_sensor or exit_one_when_fail" -v`
Expected: FAIL on the second test — `_exit_code` ignores config.

- [ ] **Step 3: Implement**

In `src/airflow_diff/cli.py`, change the `_exit_code` signature and body:

```python
def _exit_code(diff: DiffDocument, config) -> int:
    """Non-zero only when the PR introduced a regression (per spec section 7)."""
    if diff.summary.dags_regressed > 0:
        return 1
    for d in diff.dags:
        if d.classification == "added" and d.status_b == "error":
            return 1
    if config.fail_on_sensor_mismatch and diff.sensor_mismatches:
        return 1
    return 0
```

Update the only caller in `_cmd_diff`:

```python
    return _exit_code(diff, config)
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/test_cli.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/cli.py tests/unit/test_cli.py
git commit -m "feat(cli): exit 1 when fail_on_sensor_mismatch=true and mismatches present"
```

---

## Task 15: Markdown presenter — section + summary row

**Files:**
- Modify: `src/airflow_diff/present/markdown.py`
- Modify: `tests/unit/present/test_markdown.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/present/test_markdown.py`:

```python
from airflow_diff.schema import SensorMismatch


def _doc_with_mismatches(*mismatches: SensorMismatch) -> DiffDocument:
    return DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha="aaa00000", head_sha="bbb11111",
        summary=DiffSummary(),
        dags=[], render_errors=[],
        sensor_mismatches=list(mismatches),
    )


def test_sensor_mismatch_section_omitted_when_empty():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
        summary=DiffSummary(), dags=[], render_errors=[],
        sensor_mismatches=[],
    )
    out = render_markdown(doc)
    assert "Cross-DAG sensor mismatches" not in out


def test_sensor_mismatch_section_rendered_when_present():
    m = SensorMismatch(
        sensor_dag_id="downstream", sensor_task_id="wait",
        target_dag_id="upstream", target_task_id="finalize",
        reason="missing_execution_delta",
        sensor_schedule="@hourly", target_schedule="@daily",
    )
    out = render_markdown(_doc_with_mismatches(m))
    assert "Cross-DAG sensor mismatches" in out
    assert "downstream" in out and "wait" in out
    assert "upstream" in out and "finalize" in out
    assert "@hourly" in out and "@daily" in out
    assert "Missing `execution_delta`" in out


def test_sensor_mismatch_incorrect_delta_renders_expected_actual():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="incorrect_execution_delta",
        sensor_schedule="@daily", target_schedule="0 12 * * *",
        expected_delta_seconds=43200, actual_delta_seconds=3600,
    )
    out = render_markdown(_doc_with_mismatches(m))
    assert "Likely incorrect" in out
    assert "3600" in out and "43200" in out


def test_sensor_mismatch_dangling_renders_dangling_phrase():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="missing", target_task_id="x",
        reason="dangling_target",
    )
    out = render_markdown(_doc_with_mismatches(m))
    assert "Target not in head DAG bag" in out


def test_sensor_mismatch_section_placed_before_per_dag_details():
    m = SensorMismatch(
        sensor_dag_id="downstream", sensor_task_id="wait",
        target_dag_id="upstream", target_task_id="finalize",
        reason="missing_execution_delta",
    )
    big = _make_large_dag_diff(2)
    big.sensor_mismatches = [m]
    out = render_markdown(big)
    section_idx = out.index("Cross-DAG sensor mismatches")
    dag_idx = out.index("### `big_dag`")
    assert section_idx < dag_idx


def test_sensor_mismatch_survives_truncation():
    # Stress test: produce a >65 KB diff and confirm mismatches section survives.
    m = SensorMismatch(
        sensor_dag_id="downstream", sensor_task_id="wait",
        target_dag_id="upstream", target_task_id="finalize",
        reason="missing_execution_delta",
        sensor_schedule="@hourly", target_schedule="@daily",
    )
    big = _make_large_dag_diff(500)  # well over 65 KB of per-DAG content
    big.sensor_mismatches = [m]
    out = render_markdown(big)
    assert len(out) <= 65_536 + 2_000  # truncation footer is small
    assert "Cross-DAG sensor mismatches" in out
    assert "Output truncated" in out
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/present/test_markdown.py -k sensor_mismatch -v`
Expected: FAIL — none of the new assertions hold; presenter doesn't emit the section.

- [ ] **Step 3: Implement**

In `src/airflow_diff/present/markdown.py`, add import at the top of the file:

```python
from airflow_diff.schema import DagDiff, DiffDocument, FieldDiff, SensorMismatch, TaskDiff
```

In `_render_internal`, after `parts.append(_header(doc))` and the existing warning-banner block, insert:

```python
    if doc.sensor_mismatches:
        parts.append(_render_sensor_mismatches(doc.sensor_mismatches))
```

(The placement is after the header/banner but before the per-DAG loop, so the section appears above per-DAG details.)

At the end of the file, add:

```python
def _render_sensor_mismatches(mismatches: list[SensorMismatch]) -> str:
    """Render the 'Cross-DAG sensor mismatches' section."""
    n = len(mismatches)
    plural = "configurations" if n != 1 else "configuration"
    lines = [
        f"## ⚠️ Cross-DAG sensor mismatches (PR-introduced)",
        "",
        f"This PR introduces {n} `ExternalTaskSensor` {plural} that may not align "
        f"with their upstream targets at runtime.",
        "",
        "| Sensor | Target | Issue |",
        "|---|---|---|",
    ]
    for m in mismatches:
        target_label = m.target_task_id or ",".join(m.target_task_ids or [])
        lines.append(
            f"| `{m.sensor_dag_id}.{m.sensor_task_id}` "
            f"| `{m.target_dag_id}.{target_label}` "
            f"| {_mismatch_issue_summary(m)} |"
        )
    lines.append("")
    lines.append("<details><summary>Details</summary>")
    lines.append("")
    for m in mismatches:
        lines.extend(_mismatch_detail_block(m))
        lines.append("")
    lines.append("</details>")
    return "\n".join(lines) + "\n"


def _mismatch_issue_summary(m: SensorMismatch) -> str:
    if m.reason == "missing_execution_delta":
        return (
            f"Missing `execution_delta` "
            f"(schedules: `{m.sensor_schedule or '?'}` vs `{m.target_schedule or '?'}`)"
        )
    if m.reason == "incorrect_execution_delta":
        return (
            f"Likely incorrect `execution_delta={m.actual_delta_seconds}s` "
            f"(expected `{m.expected_delta_seconds}s`)"
        )
    if m.reason == "dangling_target":
        return "Target not in head DAG bag"
    return m.reason  # defensive fallthrough


def _mismatch_detail_block(m: SensorMismatch) -> list[str]:
    target_label = m.target_task_id or ",".join(m.target_task_ids or [])
    out = [
        f"**`{m.sensor_dag_id}.{m.sensor_task_id}` → `{m.target_dag_id}.{target_label}`**",
        f"- Sensor DAG schedule: `{m.sensor_schedule or 'unknown'}`",
        f"- Target DAG schedule: `{m.target_schedule or 'unknown'}`",
    ]
    if m.reason != "dangling_target":
        delta_str = f"`{m.actual_delta_seconds}s`" if m.actual_delta_seconds is not None else "not set"
        out.append(f"- `execution_delta`: {delta_str}")
    if m.reason == "incorrect_execution_delta":
        out.append(f"- Expected `execution_delta`: `{m.expected_delta_seconds}s`")
    if m.reason == "dangling_target":
        out.append("- Target not found in head DAG bag.")
    if m.notes:
        out.append(f"- Notes: {m.notes}")
    return out
```

**Note on "summary table row":** The spec (§9.3) describes adding a row to the "top-of-comment summary table." Reading `markdown.py` shows the top-of-comment summary is actually a single bold header line built by `_header()` — there is no literal table at the top. The per-DAG `_summary_table()` (around line 120) is a different thing. The spec's intent is "show the cross-DAG mismatch count prominently above the fold"; the closest faithful implementation is to append the count to the header line's `bits` list. Update `_header`:

Find this line in `_header`:

```python
    if s.dags_incidentally_affected:
        bits.append(f"{s.dags_incidentally_affected} incidentally affected")
```

After it, add:

```python
    n_mismatch = len(doc.sensor_mismatches)
    if n_mismatch:
        bits.append(f"**{n_mismatch} cross-DAG mismatch{'es' if n_mismatch != 1 else ''}**")
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/present/test_markdown.py -v`
Expected: PASS for all (including snapshot tests if any drift was within tolerance — if snapshots break on `_header` change, regenerate with `--snapshot-update`).

If snapshots break: run `pytest tests/unit/present/test_markdown.py --snapshot-update -v`, eyeball the diffs in the snapshot files (only the new `bits` segment should differ when `sensor_mismatches` is non-empty), and re-run.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/present/markdown.py tests/unit/present/test_markdown.py
git commit -m "feat(present/md): cross-DAG sensor mismatches section + summary line"
```

---

## Task 16: Terminal presenter — ANSI section

**Files:**
- Modify: `src/airflow_diff/present/terminal.py`
- Modify: `tests/unit/present/test_terminal.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/present/test_terminal.py`:

```python
from airflow_diff.schema import DiffDocument, DiffSummary, SCHEMA_VERSION, SensorMismatch
from airflow_diff.present.terminal import render_terminal


def _doc_with(*mismatches):
    return DiffDocument(
        schema_version=SCHEMA_VERSION, base_sha="aaa00000", head_sha="bbb11111",
        summary=DiffSummary(), dags=[], render_errors=[],
        sensor_mismatches=list(mismatches),
    )


def test_terminal_missing_delta_uses_yellow():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="missing_execution_delta",
    )
    out = render_terminal(_doc_with(m))
    assert "Cross-DAG sensor mismatches" in out
    assert "\033[33m" in out  # YELLOW
    assert "\033[31m" not in out.split("Cross-DAG")[1]  # no red in the section


def test_terminal_incorrect_delta_uses_red():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="u", target_task_id="x",
        reason="incorrect_execution_delta",
        expected_delta_seconds=43200, actual_delta_seconds=3600,
    )
    out = render_terminal(_doc_with(m))
    assert "\033[31m" in out  # RED


def test_terminal_dangling_uses_yellow():
    m = SensorMismatch(
        sensor_dag_id="d", sensor_task_id="t",
        target_dag_id="missing", target_task_id="x",
        reason="dangling_target",
    )
    out = render_terminal(_doc_with(m))
    assert "\033[33m" in out  # YELLOW


def test_terminal_no_section_when_empty():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION, base_sha="a", head_sha="b",
        summary=DiffSummary(), dags=[], render_errors=[],
        sensor_mismatches=[],
    )
    out = render_terminal(doc)
    assert "Cross-DAG sensor mismatches" not in out
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/present/test_terminal.py -k "missing_delta_uses or incorrect_delta_uses or dangling_uses or no_section_when_empty" -v`
Expected: FAIL — terminal presenter does not emit the section.

- [ ] **Step 3: Implement**

In `src/airflow_diff/present/terminal.py`, add import:

```python
from airflow_diff.schema import DiffDocument, DagDiff, FieldDiff, SensorMismatch, TaskDiff
```

In `render_terminal`, just before the `for d in doc.dags:` loop, insert:

```python
    if doc.sensor_mismatches:
        lines.extend(_render_sensor_mismatches(doc.sensor_mismatches))
        lines.append("")
```

Add at the end of the file:

```python
_REASON_COLOR = {
    "missing_execution_delta": YELLOW,
    "dangling_target": YELLOW,
    "incorrect_execution_delta": RED,
}


def _render_sensor_mismatches(mismatches: list[SensorMismatch]) -> list[str]:
    out = [f"{BOLD}Cross-DAG sensor mismatches:{RESET}"]
    for m in mismatches:
        color = _REASON_COLOR.get(m.reason, YELLOW)
        target_label = m.target_task_id or ",".join(m.target_task_ids or [])
        out.append(
            f"  {color}{m.sensor_dag_id}.{m.sensor_task_id} → "
            f"{m.target_dag_id}.{target_label} [{m.reason}]{RESET}"
        )
        out.append(f"      sensor schedule: {m.sensor_schedule or 'unknown'}")
        out.append(f"      target schedule: {m.target_schedule or 'unknown'}")
        if m.reason == "incorrect_execution_delta":
            out.append(
                f"      execution_delta: actual={m.actual_delta_seconds}s, "
                f"expected={m.expected_delta_seconds}s"
            )
        if m.notes:
            out.append(f"      notes: {m.notes}")
    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/unit/present/test_terminal.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/airflow_diff/present/terminal.py tests/unit/present/test_terminal.py
git commit -m "feat(present/terminal): ANSI section for cross-DAG sensor mismatches"
```

---

## Task 17: End-to-end integration with paired DAGs

**Files:**
- Modify: `tests/fixtures/sample_repo_builder.py`
- Create: `tests/fixtures/dags_paired/upstream.py`
- Create: `tests/fixtures/dags_paired/downstream_missing_delta.py`
- Modify: `tests/integration/test_cli.py`

- [ ] **Step 1: Create paired-DAG fixtures**

Create `tests/fixtures/dags_paired/upstream.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="upstream",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    BashOperator(task_id="finalize", bash_command="echo done")
```

Create `tests/fixtures/dags_paired/downstream_missing_delta.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="downstream",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream",
        external_task_id="finalize",
        # NOTE: missing execution_delta — schedules differ (@hourly vs @daily)
    )
```

- [ ] **Step 2: Extend `sample_repo_builder.build`**

In `tests/fixtures/sample_repo_builder.py`, change the function signature and add a paired-DAGs path. Replace the existing `build` function with:

```python
def build(
    repo_dir: Path,
    fixtures_root: Path,
    requirements_text: str,
    *,
    mode: str = "linear",
) -> tuple[str, str]:
    """Build a two-commit sample repo. Returns (base_sha, head_sha).

    Modes:
      * "linear" (default) — one DAG, bash_command changes between commits.
      * "paired_dags"      — base has only upstream.py; head adds a downstream
                             ExternalTaskSensor missing execution_delta.
    """
    repo_dir.mkdir(parents=True, exist_ok=True)
    _git(repo_dir, "init", "-b", "main")
    _git(repo_dir, "config", "user.email", "test@example.com")
    _git(repo_dir, "config", "user.name", "test")

    (repo_dir / "requirements.txt").write_text(requirements_text)
    (repo_dir / "dags").mkdir()

    if mode == "linear":
        (repo_dir / "dags" / "linear.py").write_text(
            (fixtures_root / "dags_base" / "linear.py").read_text()
        )
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "base")
        base_sha = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

        src = (fixtures_root / "dags_base" / "linear.py").read_text()
        modified = src.replace('bash_command="echo end"', 'bash_command="echo finished"')
        (repo_dir / "dags" / "linear.py").write_text(modified)
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "head")
    elif mode == "paired_dags":
        # Base: only upstream
        (repo_dir / "dags" / "upstream.py").write_text(
            (fixtures_root / "dags_paired" / "upstream.py").read_text()
        )
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "base: upstream only")
        base_sha = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

        # Head: add downstream sensor missing execution_delta
        (repo_dir / "dags" / "downstream.py").write_text(
            (fixtures_root / "dags_paired" / "downstream_missing_delta.py").read_text()
        )
        _git(repo_dir, "add", ".")
        _git(repo_dir, "commit", "-m", "head: add downstream sensor")
    else:
        raise ValueError(f"unknown mode: {mode}")

    head_sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return base_sha, head_sha
```

- [ ] **Step 3: Write the failing integration test**

Append to `tests/integration/test_cli.py`:

```python
def test_paired_dags_missing_delta_surfaces_in_markdown(tmp_path):
    from tests.fixtures.sample_repo_builder import build
    repo = tmp_path / "repo"
    base_sha, head_sha = build(
        repo, FIXTURES_ROOT, "apache-airflow==2.10.3\n", mode="paired_dags",
    )
    out = tmp_path / "comment.md"
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff", "diff",
         base_sha, head_sha, "--repo", str(repo),
         "--out", str(out)],
        capture_output=True, text=True, check=False,
    )
    assert res.returncode == 0, f"stderr={res.stderr}"
    text = out.read_text()
    assert "Cross-DAG sensor mismatches" in text
    assert "downstream" in text and "wait_for_upstream" in text
    assert "upstream" in text and "finalize" in text
    assert "Missing `execution_delta`" in text


def test_paired_dags_fail_on_sensor_mismatch_exits_one(tmp_path):
    from tests.fixtures.sample_repo_builder import build
    repo = tmp_path / "repo"
    base_sha, head_sha = build(
        repo, FIXTURES_ROOT, "apache-airflow==2.10.3\n", mode="paired_dags",
    )
    # Opt in via .airflow-diff.toml committed to head (so it's picked up by load_config)
    (repo / ".airflow-diff.toml").write_text("fail_on_sensor_mismatch = true\n")
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff", "diff",
         base_sha, head_sha, "--repo", str(repo),
         "--out", str(tmp_path / "comment.md")],
        capture_output=True, text=True, check=False,
    )
    assert res.returncode == 1, f"expected exit 1; got {res.returncode}; stderr={res.stderr}"
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/integration/test_cli.py -v -m integration`
Expected: PASS for all three tests (existing `test_end_to_end_diff_emits_markdown` + two new).

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/sample_repo_builder.py tests/fixtures/dags_paired/ tests/integration/test_cli.py
git commit -m "test(integration): paired-DAGs end-to-end with sensor-mismatch detection"
```

---

## Final Verification

- [ ] **Step 1: Run full unit + integration suites**

Run:
```bash
pytest tests/unit -v
pytest tests/integration -v -m integration --timeout=600
bash tests/smoke/test_action_entrypoint.sh
```
Expected: all PASS, smoke prints `ALL SMOKE TESTS PASSED`.

- [ ] **Step 2: Manual CLI smoke run**

Run the paired-DAGs scenario end-to-end against a temp repo and read the rendered markdown to eyeball formatting (terminal + HTML too):

```bash
# (from any temp repo built via the paired_dags mode in test fixtures, then:)
airflow-diff diff <base-sha> <head-sha> --format terminal
airflow-diff diff <base-sha> <head-sha> --format html --out /tmp/report.html
```
Expected: the cross-DAG section appears above per-DAG details; terminal output shows yellow for missing-delta; HTML report renders the section as a styled block.

- [ ] **Step 3: Update CLAUDE.md if architecture notes shifted**

Open `CLAUDE.md` and add a sentence to the architecture section noting `validators/cross_dag.py` is called by the orchestrator after `compute_diff` and the new `external_ref` field on `RenderedTask`. Commit separately:

```bash
git add CLAUDE.md
git commit -m "docs: note validators package + external_ref field in CLAUDE.md"
```
