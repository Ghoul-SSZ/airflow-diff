# Cross-DAG Sensor Validation — Design Spec

**Date:** 2026-05-18
**Status:** Approved for implementation planning
**Scope:** Incremental feature on top of airflow-diff MVP (v0.1)
**Targets:** v0.2

## 1. Problem statement

When an Airflow team splits one large DAG into 2–3 smaller DAGs (a common refactor for clarity and independent scheduling), the new DAGs typically communicate through an `ExternalTaskSensor` on the downstream side that waits for a task in the upstream DAG. If the two DAGs run on different schedules, the sensor *must* set either `execution_delta` (a `timedelta`) or `execution_date_fn` (a callable) so the sensor knows which logical date of the upstream DAG to look for. Without it, the sensor silently waits forever — the PR looks fine in code review, and the bug surfaces in prod.

airflow-diff already renders both commits and produces a structural diff, which means it has the information needed to detect this bug class at PR time. The pipeline already extracts per-DAG `attrs.schedule`; it does not yet extract sensor metadata or cross-DAG references. This spec adds that extraction plus a validator that flags mismatches the PR introduces.

The same bug class also arises without a refactor — for example, when a contributor changes one DAG's schedule and forgets to update the sensor's `execution_delta` in a peer DAG. The detection logic catches both shapes equally; the "refactor" framing is just the motivating case.

## 2. Goals / Non-goals

### Goals

- Detect every `ExternalTaskSensor` (including user subclasses) whose target DAG has a different schedule than the sensor's DAG and which lacks an alignment bridge (`execution_delta` or `execution_date_fn`).
- When `execution_delta` is a literal `timedelta` and both schedules are simple cron, verify the delta actually aligns the sensor's logical date with a valid logical date of the target.
- Detect "dangling" sensor targets: sensors referencing a `(dag_id, task_id)` that does not exist in the head-commit DAG bag.
- Report only **PR-introduced** mismatches — a mismatch present in both base and head is silenced.
- Surface findings in the PR comment above per-DAG details, with a one-row addition to the summary table.
- Allow teams to opt into CI hard-fail behavior via `.airflow-diff.toml`.

### Non-goals

- Detecting mismatches that pre-exist at base commit (out of scope for the PR-comment use case).
- Validating arbitrary user-provided `execution_date_fn` callables. Presence is reported; correctness is not verified.
- Detecting cross-DAG bugs in non-`ExternalTaskSensor` patterns (`TriggerDagRunOperator`, dataset triggers, custom sensors). Future work — the validator module is designed as a pluggable extension point.
- `ExternalTaskMarker` extraction. Markers exist for clear-propagation, not for scheduling coordination, and are not required for the bug class this spec addresses. (Confirmed during brainstorming.)
- Static AST analysis as an alternative to rendering. Out of scope: misses factory-built sensors, contradicts airflow-diff's render-first philosophy.

## 3. Decision summary

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | What counts as the "bridge" | Any `ExternalTaskSensor` target task (subclasses included), not only Marker-paired targets | The schedule-alignment bug exists for all sensors regardless of whether the upstream is a Marker. |
| 2 | Detection scope | PR-introduced mismatches only | Matches airflow-diff's existing "show what changed" philosophy. |
| 3 | Strictness | Presence check + literal-`timedelta` cron arithmetic | Catches the "forgot the delta" case AND the "wrong delta number" case. Callable `execution_date_fn` is acknowledged but not verified. |
| 4 | CI severity | Warn by default; `fail_on_sensor_mismatch` config opt-in for hard-fail | Existing exit-code policy stays "1 = regression"; new finding is informational unless team opts in. |
| 5 | Code placement | New `validators/cross_dag.py` module called by orchestrator after `compute_diff()` | Keeps `diff.py` a pure structural differ. Validators is an extension point for future cross-DAG checks. |
| 6 | Renderer dispatch | MRO-walk for `__name__ == "ExternalTaskSensor"`, not `isinstance` | Avoids hard-importing `airflow.sensors.external_task` (defensive, matches existing dataset-extraction style). Catches user subclasses. |
| 7 | Cron library | Add `croniter>=2.0` to host runtime deps | Already a transitive Airflow dep so users have it; pure Python; small. |
| 8 | PR-introduced gate | Key = `(sensor_dag, sensor_task, target_dag, target_task_id_or_tuple)`; silence head-mismatch if key exists in base regardless of `reason` | Avoids double-reporting the same broken pair when the only thing that changed is which rule fired. |

## 4. Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│ Renderer subprocess (per commit, imports Airflow — unchanged module)  │
│   – existing: walks dag.tasks, renders template fields                │
│   – NEW:     for each task with ExternalTaskSensor in MRO, populate   │
│              RenderedTask.external_ref (ExternalTaskRef)              │
└───────────────────────────────────────────────────────────────────────┘
                       │   (JSON wire protocol, SCHEMA_VERSION 1 → 2)
                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│ Orchestrator (host Python, never imports Airflow)                     │
│   – existing: run_diff() → resolves SHAs, spawns renderers,           │
│               calls compute_diff(base_bag, head_bag, touched_files)   │
│   – NEW:     calls validators.cross_dag.validate(base_bag, head_bag,  │
│               config) and attaches result to diff.sensor_mismatches   │
└───────────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│ Presenter (markdown, terminal, html — all consume DiffDocument)       │
│   – existing sections unchanged                                       │
│   – NEW: "Cross-DAG sensor mismatches" section rendered above the     │
│          per-DAG details when sensor_mismatches is non-empty          │
│   – NEW: one summary-table row when count > 0                         │
└───────────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│ CLI exit code                                                         │
│   – existing: 1 on DAG-level regressions                              │
│   – NEW: also 1 when config.fail_on_sensor_mismatch and               │
│          diff.sensor_mismatches is non-empty                          │
└───────────────────────────────────────────────────────────────────────┘
```

Invariants preserved:

- Parent process still does not import Airflow.
- `diff.py` remains a pure structural differ — no semantic knowledge of sensors.
- Per-DAG isolation: a validator exception for one sensor must not affect any other sensor or DAG.

## 5. Schema additions (`src/airflow_diff/schema.py`)

`SCHEMA_VERSION` bumps from `1` to `2`. Old `diff.json` files produced by v0.1 can no longer be replayed via `airflow-diff report`. Acceptable because v0.1 is pre-users.

**Renderer-side:**

```python
class ExternalTaskRef(_Model):
    """Cross-DAG metadata captured from an ExternalTaskSensor instance."""
    kind: Literal["external_task_sensor"]
    external_dag_id: str
    external_task_id: Optional[str] = None         # one of these is set
    external_task_ids: Optional[list[str]] = None
    external_task_group_id: Optional[str] = None
    execution_delta_seconds: Optional[int] = None  # set only if execution_delta is a timedelta literal
    execution_date_fn_present: bool = False        # callable was provided (opaque)


class RenderedTask(_Model):
    # existing fields ...
    external_ref: Optional[ExternalTaskRef] = None
```

**Diff-document side:**

```python
class SensorMismatch(_Model):
    sensor_dag_id: str
    sensor_task_id: str
    target_dag_id: str
    target_task_id: Optional[str] = None
    target_task_ids: Optional[list[str]] = None
    reason: Literal[
        "missing_execution_delta",    # schedules differ, no delta and no fn
        "incorrect_execution_delta",  # literal delta doesn't match cron arithmetic
        "dangling_target",            # target dag_id/task_id not in head bag
    ]
    sensor_schedule: Optional[str] = None
    target_schedule: Optional[str] = None
    expected_delta_seconds: Optional[int] = None  # populated for incorrect_execution_delta
    actual_delta_seconds: Optional[int] = None
    notes: Optional[str] = None  # e.g. "execution_date_fn provided; correctness not verified"


class DiffDocument(_Model):
    # existing fields ...
    sensor_mismatches: list[SensorMismatch] = Field(default_factory=list)
```

All models keep `extra="forbid"`.

## 6. Renderer extraction (`src/airflow_diff/renderer.py`)

A new helper inspects each task; per-task try/except wraps the call so a failed extraction does not affect the rest of the DAG.

```python
def _extract_external_ref(task) -> ExternalTaskRef | None:
    cls = type(task)
    # MRO-walk by class name — avoids importing airflow.sensors.external_task
    # (defensive habit matching _extract_dataset_uris). Catches subclasses.
    if not any(c.__name__ == "ExternalTaskSensor" for c in cls.__mro__):
        return None

    delta = getattr(task, "execution_delta", None)
    delta_seconds = int(delta.total_seconds()) if isinstance(delta, timedelta) else None

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

Call site in `_render_dag` populates `external_ref` on the `RenderedTask` constructor call. If `_extract_external_ref` raises, the exception is swallowed; the `RenderedTask` is built with `external_ref=None`.

`execution_delta` that is not a `timedelta` instance (rare; would be e.g. a custom user object) leaves `execution_delta_seconds=None`; the validator treats this case as "delta presence not confirmed" and falls back to checking `execution_date_fn_present`.

No changes to the renderer's CLI arguments or the `--config` payload shape.

## 7. Validator (`src/airflow_diff/validators/cross_dag.py`)

New module. The validator operates only on `schema.py` types — no Airflow imports.

### 7.1 Public surface

```python
def validate(
    base_bag: RenderedDagBag,
    head_bag: RenderedDagBag,
    config: Config,
) -> list[SensorMismatch]:
    """Returns mismatches present in head that were NOT present in base."""
```

### 7.2 Algorithm

```python
def validate(base_bag, head_bag, config):
    base_keys = {m.key() for m in _mismatches_for_bag(base_bag, config)}
    head = _mismatches_for_bag(head_bag, config)
    return [m for m in head if m.key() not in base_keys]
```

`_mismatches_for_bag` iterates over every DAG with `status == "ok"`, every task with `external_ref is not None`, and applies the rule tree below. Each mismatch is identified by:

```python
def key(m: SensorMismatch) -> tuple:
    if m.target_task_ids is not None:
        target = ("ids", tuple(sorted(m.target_task_ids)))
    else:
        target = ("id", m.target_task_id)  # may be None
    return (m.sensor_dag_id, m.sensor_task_id, m.target_dag_id, target)
```

Note: `reason` is intentionally **not** part of the key. If base has `missing_execution_delta` for a pair and head has `incorrect_execution_delta` for the same pair (because the PR added a wrong value), the head finding is silenced — the pair was already "known broken" at base.

### 7.3 Rule tree (per sensor)

For each `(sensor_dag, sensor_task, ref)` in the head bag, apply the following short-circuit chain. Each step either emits at most one `SensorMismatch` and stops, or falls through to the next step.

**Step 1 — Dangling target check.** Look up `ref.external_dag_id` in `head_dags`.
- Target DAG missing → emit `reason="dangling_target"`, stop.
- `external_task_id` set and not found among the target DAG's task IDs → emit `dangling_target`, stop.
- `external_task_ids` set and any element not found in target DAG's task IDs → emit `dangling_target`, stop (`notes` lists the missing IDs).
- `external_task_group_id` is **captured but not validated** in this version (would require matching against the target DAG's `task_groups` list and is deferred to future work).

**Step 2 — Schedule equality.** Normalize both schedules (see 7.4). If they normalize to equal cron expressions, or both are opaque-and-equal-by-`==`, stop with no emission.

**Step 3 — `execution_date_fn` short-circuit.** If `ref.execution_date_fn_present is True`, treat alignment as the user's responsibility and stop with no emission. (Future work: invoke the callable in the renderer to verify.)

**Step 4 — Missing bridge.** If `execution_delta_seconds is None` (and fn is also absent, per Step 3), emit `reason="missing_execution_delta"`. If the target schedule is opaque (per 7.4), add `notes="target schedule is opaque; cannot suggest a specific execution_delta value"`. Stop.

**Step 5 — Literal delta math (cron-only).** At this point `execution_delta_seconds is not None`. If both schedules are cron-parseable:
- `sensor_logical_date = config.synthetic_logical_date`
- `target_logical_date = sensor_logical_date - timedelta(seconds=execution_delta_seconds)`
- If `croniter.match(target_schedule_cron, target_logical_date)` → no emission, stop.
- Else compute the most recent valid logical date of `target_schedule_cron` at or before `sensor_logical_date`; `expected_delta = (sensor_logical_date - that).total_seconds()`. Emit `reason="incorrect_execution_delta"` with `expected_delta_seconds` and `actual_delta_seconds` populated. Stop.

**Step 6 — Opaque-schedule fallthrough.** A delta literal is set, but at least one schedule is opaque so we can't verify the math. Stop silently — a present delta is the user's signal that they took alignment into account, and we have no basis to second-guess.

### 7.4 Cron normalization

A small in-module table translates Airflow's named presets to cron strings, then `croniter` does the rest:

| Preset | Cron |
|---|---|
| `@yearly` / `@annually` | `0 0 1 1 *` |
| `@monthly` | `0 0 1 * *` |
| `@weekly` | `0 0 * * 0` |
| `@daily` / `@midnight` | `0 0 * * *` |
| `@hourly` | `0 * * * *` |

`None`, `@once`, `@continuous`, dataset-list shapes (rendered as JSON arrays by `_jsonify`), and any `repr()` fallback are treated as opaque. `timedelta` schedules (serialized by `_jsonify` as `"PTnnnS"`) are parsed as continuous and treated as opaque for cron-math purposes — they fall through to the "opaque" branch.

### 7.5 Failure isolation

Cron-parse errors, croniter exceptions, attribute lookups — all wrapped in a `try/except` per sensor at the top of `_evaluate_sensor`. On exception, no mismatch is emitted for that sensor and processing continues. This matches the existing per-DAG isolation policy in the renderer.

## 8. Config & exit code

`src/airflow_diff/config.py`:

```python
class Config(BaseModel):
    # existing fields ...
    fail_on_sensor_mismatch: bool = False
```

`src/airflow_diff/cli.py` — change `_exit_code` signature and call site:

```python
def _exit_code(diff: DiffDocument, config: Config) -> int:
    if diff.summary.dags_regressed > 0:
        return 1
    for d in diff.dags:
        if d.classification == "added" and d.status_b == "error":
            return 1
    if config.fail_on_sensor_mismatch and diff.sensor_mismatches:
        return 1
    return 0
```

`_cmd_diff` already binds `config`; the call becomes `return _exit_code(diff, config)`. No new CLI flag is added — the toggle is config-file driven only, to keep `.airflow-diff.toml` the single source of truth for repo-wide policy.

## 9. Presenter (`src/airflow_diff/present/`)

`markdown.py` is the source of truth; `html.py` wraps it; `terminal.py` re-renders with ANSI colors.

### 9.1 Section placement

The new "Cross-DAG sensor mismatches" section is rendered **immediately above** the per-DAG details section (i.e. after the summary table + Mermaid graph, before the first per-DAG block). When `doc.sensor_mismatches` is empty, the section is omitted entirely — no "0 mismatches" noise.

### 9.2 Section content

```markdown
## ⚠️ Cross-DAG sensor mismatches (PR-introduced)

This PR introduces N `ExternalTaskSensor` configurations that may not align
with their upstream targets at runtime.

| Sensor | Target | Issue |
|---|---|---|
| `<sensor_dag_id>.<sensor_task_id>` | `<target_dag_id>.<target_task_id>` | <reason summary> |

<details>
<summary>Details</summary>

**`<sensor_dag_id>.<sensor_task_id>` → `<target_dag_id>.<target_task_id>`**
- Sensor DAG schedule: `<sensor_schedule>`
- Target DAG schedule: `<target_schedule>`
- `execution_delta`: `<value or "not set">`
- `execution_date_fn`: `<"set" or "not set">`
- (`incorrect_execution_delta` only) Expected at synthetic logical date `<date>`: `<expected_delta_seconds>s`
- (`dangling_target` only) Target not found in head DAG bag.
- Fix: <reason-specific guidance>

</details>
```

Issue summary phrasing per `reason`:

- `missing_execution_delta` → `` Missing `execution_delta` (schedules: `<a>` vs `<b>`) ``
- `incorrect_execution_delta` → `` Likely incorrect `execution_delta=<actual>s` (expected `<expected>s`) ``
- `dangling_target` → `` Target not in head DAG bag ``

### 9.3 Summary table row

The existing top-of-comment summary table gains one row when `len(doc.sensor_mismatches) > 0`:

```
| Cross-DAG mismatches | <count> |
```

Row is omitted when count is zero.

### 9.4 Terminal presenter

One block per mismatch, colored:

- `missing_execution_delta` → yellow
- `dangling_target` → yellow
- `incorrect_execution_delta` → red

Block content mirrors the markdown details.

### 9.5 HTML presenter

No code changes — `html.py` already wraps the markdown output into HTML, so the new section flows through automatically.

### 9.6 Truncation interaction

The existing markdown presenter truncates at 65 536 characters (GitHub PR-comment limit) and appends a footer pointing at the uploaded HTML artifact. Because the mismatches section renders **before** the per-DAG details, very large diffs will keep mismatches visible while per-DAG details get truncated — the right priority, since mismatches are actionable warnings.

## 10. Testing

### 10.1 Unit (`tests/unit/`)

- `test_schema.py`
  - Round-trip `ExternalTaskRef` and `SensorMismatch`.
  - Assert `SCHEMA_VERSION == 2`.
  - Assert a `RenderedDagBag` JSON with `schema_version: 1` fails validation.
- `test_validators_cross_dag.py` (new) — hand-built `RenderedDagBag` fixtures, one case per rule branch:
  - Schedules equal → no mismatch.
  - Schedules differ, no delta, no fn → `missing_execution_delta`.
  - Schedules differ, literal delta correct → no mismatch.
  - Schedules differ, literal delta wrong → `incorrect_execution_delta` with both delta fields populated.
  - Schedules differ, `execution_date_fn_present=True` → no mismatch.
  - Target DAG missing → `dangling_target`.
  - Target task missing (singular `external_task_id`) → `dangling_target`.
  - Target task missing (one of `external_task_ids`) → `dangling_target`.
  - Same mismatch present in base and head → silenced (PR-introduced gate).
  - Different reason for same pair across base/head → silenced (key excludes `reason`).
  - Mismatch in head only → emitted.
  - Opaque target schedule (e.g. dataset list), no delta → `missing_execution_delta` with notes.
  - Validator exception path (mock croniter to raise) → swallowed, no crash, no emission.
- `test_cli.py`
  - Exit-code 0: monkeypatch `run_diff` to return a `DiffDocument` with `sensor_mismatches` non-empty, default config → exit 0.
  - Exit-code 1: same input, `fail_on_sensor_mismatch=True` → exit 1.
- `test_config.py` — round-trip `fail_on_sensor_mismatch` default and override.
- `present/test_markdown.py`
  - Section omitted when `sensor_mismatches` is empty.
  - Section rendered above per-DAG details when non-empty.
  - Summary table row present iff `len > 0`.
  - Truncation stress test: render a >65 KB diff with one mismatch; assert mismatches section survives truncation.
- `present/test_terminal.py` — assert correct ANSI color per `reason`.

### 10.2 Integration (`tests/integration/`)

- `test_renderer.py`
  - Add a DAG fixture using `ExternalTaskSensor(execution_delta=timedelta(hours=1))`. Assert `external_ref` is populated with `execution_delta_seconds == 3600`, `execution_date_fn_present is False`.
  - Add a DAG fixture using `execution_date_fn=lambda dt: dt`. Assert `external_ref` is populated with `execution_delta_seconds is None`, `execution_date_fn_present is True`.
  - Add a DAG fixture using a user subclass of `ExternalTaskSensor`. Assert MRO-walk detection captures it.
- `test_cli.py` — extend `sample_repo_builder` with an optional "paired-DAGs" mode that writes:
  - `dags/upstream.py` — a daily DAG with one task.
  - `dags/downstream.py` — an hourly DAG with an `ExternalTaskSensor` targeting upstream's task, **without** `execution_delta`.
  End-to-end run asserts:
  - Exit code 0 (default config).
  - Markdown output contains `Cross-DAG sensor mismatches`, `upstream`, `downstream`, and `missing_execution_delta` phrasing.
  - With `fail_on_sensor_mismatch=true` in the test repo's `.airflow-diff.toml`, exit code 1.

### 10.3 Smoke

No new smoke tests required — the GitHub Action entrypoint behavior is unchanged.

## 11. Dependencies

`pyproject.toml` `[project].dependencies` gains:

```toml
"croniter>=2.0",
```

`croniter` is pure Python (~50 KB), a transitive Airflow dependency, and stable. Adding it to host-runtime deps means it is available in the orchestrator process (not just renderer subprocesses), which is where the validator runs.

## 12. Backwards compatibility

- `SCHEMA_VERSION` bump 1 → 2 is a **breaking** wire change. Any saved `diff.json` from v0.1 cannot be replayed via `airflow-diff report` against v0.2. Acceptable: v0.1 is unreleased / pre-users.
- `.airflow-diff.toml` files without `fail_on_sensor_mismatch` continue to work; the field defaults to `False`.
- `RenderedTask.external_ref` and `DiffDocument.sensor_mismatches` both default to `None` / empty list, so any code consuming these models without awareness of v0.2 continues to function (although Pydantic's `extra="forbid"` means a v0.1 *parser* would reject v0.2 JSON — hence the version bump).

## 13. Future work (explicitly out of scope here)

- Detect mismatches in `TriggerDagRunOperator` (active-trigger pattern).
- Detect Dataset-trigger graph cycles or orphans.
- Validate `execution_date_fn` by invoking it inside the renderer against the synthetic logical date.
- Detect when a sensor's target is *not* an `ExternalTaskMarker` (soft warning to encourage team conventions).
- Static analysis fallback for sensors built inside disabled DAGs (where the DAG fails to import).

## 14. Open questions

None at time of writing. All scope decisions are resolved in §3.
