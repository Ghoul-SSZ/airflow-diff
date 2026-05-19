# Showcase: two big DAGs, three case studies

Status: draft
Date: 2026-05-19
Branch: `feat/showcase`

## Goal

Give a new reader of the `airflow-diff` README a concrete, runnable demonstration
of what the tool catches that `git diff` and CI test suites do not. The
demonstration has to be vivid enough that a 30-second scan of the README sells
the project, and detailed enough that a curious reader can reproduce every
captured output locally.

## Constraints

- Two "big" DAGs (15+ tasks each) with `TaskGroup` usage, so the example looks
  like real production code rather than a toy.
- Three case studies that each isolate one core capability — hidden regression,
  cross-DAG sensor validation, shared-helper ripple — so readers can map a
  symptom they recognize to the feature that catches it.
- Examples live in the repo and are reproducible, but the inner "two commits"
  the tool diffs must not pollute the outer `airflow-diff` git history. The
  inner repo is materialized at demo time.
- README must stay scannable. Long captured outputs go in collapsed
  `<details>` blocks or in `docs/showcase/`.

## Non-goals

- Not a tutorial on Airflow itself.
- Not an exhaustive feature tour. Three cases, three capabilities, stop.
- The example DAGs do not have to *run* in Airflow — only to import and
  render. (They will be valid 2.10.x DAGs anyway, so a reader can drop them
  into a sandbox if they want.)
- No new code in `src/airflow_diff/`. This is a documentation + examples
  contribution; if the captured output exposes a renderer bug, that gets
  filed separately.

## The two DAGs

A generic e-commerce analytics pipeline. The two DAGs have a real producer →
consumer relationship via `ExternalTaskSensor`, so case 2 (sensor drift) is
honest rather than contrived.

### `orders_pipeline.py` — hourly ingestion + staging

- Schedule: `@hourly`
- ~19 tasks across 4 `TaskGroup`s plus `start`, `end`, and a
  `publish_orders_ready` marker:
  - `extract` (4): `extract_orders`, `extract_customers`, `extract_inventory`,
    `extract_returns` — `BashOperator` with `--date {{ ds }}` templates.
  - `validate` (3): `schema_check`, `null_check`, `row_count_check` —
    `PythonOperator`.
  - `transform` (5): `enrich_orders`, `denormalize_customers`,
    `compute_line_items`, `flag_anomalies`, `dedupe` — `BashOperator` with
    Jinja templates referencing `var.value.warehouse_bucket`.
  - `load` (4): `load_staging_orders`, `load_staging_customers`,
    `load_staging_inventory`, `load_audit_log` — `BashOperator`.
  - `publish_orders_ready` — `EmptyOperator`, the downstream sensor's target.

### `finance_rollup.py` — daily aggregations

- Schedule: `0 6 * * *`
- ~16 tasks across 3 `TaskGroup`s plus `start`, `end`:
  - `wait` (1): `wait_for_orders` — `ExternalTaskSensor` targeting
    `orders_pipeline.publish_orders_ready` with
    `execution_delta=timedelta(hours=1)`.
  - `aggregate` (6): `daily_revenue`, `daily_refunds`, `daily_margins`,
    `category_breakdown`, `region_breakdown`, `cohort_metrics` —
    mixed `PythonOperator`/`BashOperator` with templated SQL.
  - `report` (5): `build_exec_dashboard`, `build_finance_pdf`, `build_ops_csv`,
    `notify_finance`, `notify_ops` — `BashOperator` with templated
    `--out s3://{{ var.value.report_bucket }}/{{ ds_nodash }}/...`.
  - `archive_raw`, `cleanup_tmp` — `BashOperator`.

Both DAGs use `Variable.get`, `BaseHook.get_connection`, and templated
operator parameters so the renderer has interesting content to produce.

## The three case studies

Each case is one `.patch` file applied to the base. Together they cover the
three capabilities the project most wants to advertise.

### Case 1 — Hidden regression from a refactor

**Source change:** a teammate "refactors" `orders_pipeline.py` to pull the
staging-bucket prefix from a small helper. The helper has a typo
(`warehouse_buckte`).

**What `git diff` shows:** three innocuous-looking lines.

**What `airflow-diff` shows:** every `load_staging_*` task's rendered
`bash_command` changed — the bucket interpolated to the sentinel
`<VAR:warehouse_buckte>` instead of the real value, indicating the variable
does not exist. Provenance footnotes on each rendered field point at the typo.

**Capability showcased:** rendered-template diff with provenance.

### Case 2 — Sensor schedule drift

**Source change:** `orders_pipeline`'s schedule changes from `@hourly` to
`0 */4 * * *` (every 4 hours). `finance_rollup`'s `ExternalTaskSensor` still
has `execution_delta=timedelta(hours=1)`.

**What `git diff` shows:** a one-line schedule change.

**What `airflow-diff` shows:** an `incorrect_execution_delta` mismatch reported
above the per-DAG details, computed by `croniter` against the synthetic
logical date. With `fail_on_sensor_mismatch = true` in `.airflow-diff.toml`,
CI exits `1`.

**Capability showcased:** cross-DAG sensor validation, `fail_on_sensor_mismatch`.

### Case 3 — Shared-helper ripple

**Source change:** a new `dags/common/cmd.py` module exposes
`build_report_cmd(name)`. Both DAGs' report/transform tasks are switched to use
it. The helper prepends `--tenant {{ var.value.tenant_id }}` to every command.
`finance_rollup.py` is touched only at the imports + `aggregate`/`report`
groups; the rest of the file is unchanged.

**What `git diff` shows:** a few wrapped function calls plus one new module.

**What `airflow-diff` shows:** `orders_pipeline` classified `touched`,
`finance_rollup` classified `incidentally_affected`, and rendered-field diffs
on every task that goes through `build_report_cmd` — including tasks in DAGs
the PR never opened.

**Capability showcased:** `incidentally_affected` classification + ripple
detection across DAGs.

## Layout

Committed to `feat/showcase`:

```
examples/showcase/
  dags/
    orders_pipeline.py         # base state, ~19 tasks
    finance_rollup.py          # base state, ~16 tasks
    # case-1 and case-3 patches each introduce new modules under dags/
    # (e.g. dags/common/buckets.py for case 1, dags/common/cmd.py for case 3)
  requirements.txt             # apache-airflow==2.10.3
  constraints.txt              # matching constraints URL for repeatability
  .airflow-diff.toml           # dags_folder="dags", fail_on_sensor_mismatch=true
  scenarios/
    case-1-regression.patch
    case-2-sensor.patch
    case-3-ripple.patch
  make_history.sh              # see below
  README.md                    # how to run the demo, what each case shows
docs/showcase/
  case-1-output.md             # captured markdown output (verbatim)
  case-2-output.md
  case-3-output.md
```

The top-level `README.md` gains a new "See it in action" section linking into
the above.

## `make_history.sh`

A ~40-line bash script. Usage:

```
./make_history.sh case-1            # prepare the inner repo, print the diff command
./make_history.sh case-1 --run      # also run airflow-diff diff against the repo
./make_history.sh all               # prepare all three (sequentially)
```

Behavior:

1. Create `/tmp/airflow-diff-showcase/<case>/` (clear it if present).
2. Copy `dags/`, `requirements.txt`, `constraints.txt`, `.airflow-diff.toml`
   into the temp dir.
3. `git init && git add . && git commit -m "base"`, capture the SHA → `BASE`.
4. `git apply ../../scenarios/<case>.patch && git add . && git commit -m "head"`,
   capture the SHA → `HEAD`.
5. Print `airflow-diff diff $BASE $HEAD --repo <tmpdir> --format markdown`.
   If `--run` was passed and `airflow-diff` is on `PATH`, execute it.

This sidesteps the "git repo inside a git repo" problem: the inner repo
exists only at demo time. The outer `airflow-diff` repo just holds the
sources and patches.

## Captured outputs

`docs/showcase/case-N-output.md` are produced once locally (with Airflow
installed) and committed verbatim. They contain the exact markdown
`airflow-diff` would post to a PR for that case.

Refresh policy: regenerate when the renderer's markdown changes. This is the
same human cadence as the captured fixtures already in
`tests/fixtures/diff_documents/`. The captured files have a banner comment at
the top (`<!-- generated by ./make_history.sh case-N --run -->`) so it's
obvious how to regenerate them.

## README integration

A new section in the top-level `README.md`, inserted between **Why** and
**Install** and titled **"See it in action"**.

Structure (one block per case):

```markdown
### Case 1 — A refactor that silently broke 4 tasks

> A teammate consolidates the staging-bucket prefix into a helper. The helper
> has a typo. `git diff` shows three innocuous lines; production is now
> writing to a bucket that doesn't exist.

**What the PR diff shows:**

```diff
- bucket = Variable.get("warehouse_bucket")
+ from common.buckets import warehouse_prefix
+ bucket = warehouse_prefix()
```

<details><summary>What <code>airflow-diff</code> posts on the PR</summary>

<!-- contents of docs/showcase/case-1-output.md, included verbatim -->

</details>

Run it locally: `examples/showcase/make_history.sh case-1 --run`.
```

Same shape for cases 2 and 3. The collapsed `<details>` blocks keep the
section scrollable while the captured markdown stays one click away.

## Testing

This is a docs + examples change, not a code change. Validation is manual:

- `bash examples/showcase/make_history.sh all --run` produces the three
  captured outputs locally and they match what's committed to
  `docs/showcase/`. (Author-only check; CI does not run Airflow on every
  push for this directory.)
- README renders correctly on GitHub: scan the rendered preview before
  merging.
- The two base DAGs import cleanly under Airflow 2.10.3 in a fresh venv.

No new unit or integration tests. If a future renderer change shifts the
captured markdown, the author re-runs `make_history.sh all --run` and
commits the new captures as part of that PR.

## Open questions

None at spec-write time. Confirmed:

- Three separate case studies (not one combined).
- Layout: one `examples/showcase/` with patches + script; outputs in
  `docs/showcase/`.
- DAG domain: orders + finance rollup.
- DAGs are valid Airflow 2.10.x and importable.
