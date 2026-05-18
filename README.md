# airflow-diff

Render Apache Airflow 2.x DAGs at two git commits, structurally diff them
(including Jinja-rendered template fields), and emit a GitHub-flavored markdown
PR comment with a Mermaid diff graph, summary table, and collapsible per-field
text diffs.

## Why

When a PR touches a DAG, what GitHub shows you is a textual `git diff` of
Python source. What runs in production is the *imported, template-rendered*
DAG — a different object whose `bash_command`, `sql`, and other operator
parameters may have expanded differently than the source suggests. A change to
a shared helper or factory function can silently mutate dozens of unrelated
DAGs. `airflow-diff` surfaces all of that at PR time.

## Install

```bash
pip install airflow-diff
```

Requires Python 3.10+. The CLI needs `uv` and `git` on PATH; the GitHub Action
additionally needs `gh` (used by the wrapper script to post PR comments).

## CLI usage

```bash
# Render and diff against two commits in the current repo
airflow-diff diff <base-sha> <head-sha>

# Choose an output format
airflow-diff diff <base-sha> <head-sha> --format markdown   # default
airflow-diff diff <base-sha> <head-sha> --format terminal
airflow-diff diff <base-sha> <head-sha> --format html --out report.html

# Re-render an existing diff document
airflow-diff diff <base-sha> <head-sha> --json-out diff.json
airflow-diff report diff.json --format html --out report.html
```

Exit codes: `0` for no regressions; `1` when the PR introduces a DAG-level
regression (a DAG that imported cleanly at base now fails at head, or an added
DAG fails to import). Also `1` for PR-introduced cross-DAG sensor mismatches
when `fail_on_sensor_mismatch = true` is set in `.airflow-diff.toml` (see
[Cross-DAG sensor validation](#cross-dag-sensor-validation) below).

## GitHub Action usage

```yaml
- uses: actions/checkout@v4
  with: { fetch-depth: 0 }   # required so the base SHA is reachable
- uses: airflow-diff/airflow-diff@v0
  with:
    github-token: ${{ secrets.GITHUB_TOKEN }}
```

The Action refuses to run on PRs from forks (it imports arbitrary Python from
both commits, and that is not safe to run on untrusted code).

## Configuration

Optional `.airflow-diff.toml` at repo root:

```toml
dags_folder = "dags"
plugins_folder = "plugins"
fixtures_path = ".airflow-diff/fixtures.yaml"
excluded_files = []           # fnmatch patterns, relative to dags_folder
excluded_dag_ids = []         # fnmatch patterns matched against dag_id
synthetic_logical_date = "2025-01-01T00:00:00+00:00"
render_timeout_seconds = 300
max_tasks_for_graph = 50
fail_on_sensor_mismatch = false  # exit 1 on PR-introduced sensor mismatches
```

`excluded_files` and `excluded_dag_ids` both use `fnmatch`-style shell glob
patterns (`*`, `?`, `[abc]`). `excluded_files` patterns are matched against the
path relative to `dags_folder` (e.g. `"legacy/*"` skips anything under
`dags/legacy/`). `excluded_dag_ids` patterns are matched against `dag_id` (e.g.
`"sandbox_*"`). Excluded entries are skipped at the renderer level — they never
appear in the diff or in sensor-mismatch detection.

Optional `.airflow-diff/fixtures.yaml` to provide real Variables/Connections
that override the synthetic `<VAR:...>` / `<CONN:...>` stubs:

```yaml
variables:
  bucket: "my-prod-bucket"
connections:
  warehouse:
    host: "wh.example.com"
    schema: "analytics"
```

## Cross-DAG sensor validation

When a PR introduces an `ExternalTaskSensor` whose target DAG runs on a
different schedule, the sensor must set either `execution_delta` (a
`timedelta`) or `execution_date_fn` (a callable) so it can align with the
upstream's logical date. Forgetting this is a classic "looked fine in code
review, hangs forever in prod" bug.

`airflow-diff` detects three flavors of PR-introduced mismatch and reports
them above the per-DAG details in the PR comment:

- **`missing_execution_delta`** — schedules differ; sensor has neither
  `execution_delta` nor `execution_date_fn`.
- **`incorrect_execution_delta`** — `execution_delta` is a literal `timedelta`
  but doesn't actually align with the target's cron schedule (verified via
  `croniter` at the synthetic logical date).
- **`dangling_target`** — sensor references a `(dag_id, task_id)` that doesn't
  exist in the head DAG bag.

Only mismatches **introduced by the PR** are reported. A pair already broken
at the base commit is silenced.

By default these are surfaced as warnings in the PR comment and the CLI still
exits `0`. Set `fail_on_sensor_mismatch = true` in `.airflow-diff.toml` to
have CI exit `1` on any PR-introduced mismatch.

## Limitations

- Airflow 2.x only (2.8.x – 2.10.x).
- Linux and macOS only.
- `pip` installs of arbitrary user code happen in isolated venvs but are not
  sandboxed. Do not run against PRs from untrusted forks.
- Code that does real work at module import time (e.g., `Hook.get_records()`
  inside `dags/`) will hit stubs and may crash; the DAG appears as broken.
- Dynamic task mapping (`.expand()`) is captured structurally but not
  unrolled into mapped task instances.
- No rename detection — renamed tasks or DAGs appear as remove + add.
