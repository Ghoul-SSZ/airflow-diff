# `airflow-diff` showcase

This directory is a self-contained demo. It holds two big Airflow 2.x DAGs
in their "base" state plus three patches that each represent a hypothetical
PR. A small driver script materializes a temporary inner git repo,
applies one of the patches as a `head` commit, and prints the
`airflow-diff diff` invocation to run against it.

## What you need

- `uv`, `git`, and `airflow-diff` on `PATH` (the project's dev install gives
  you the last one).
- Internet access — the renderer's per-commit venv downloads
  `apache-airflow==2.10.3` on first run, then caches it under
  `~/.cache/airflow-diff/venvs/`.

## Run a case

```bash
./make_history.sh case-1            # prepare repo, print the diff command
./make_history.sh case-1 --run      # also run airflow-diff against it
./make_history.sh all --run         # all three, one after another
```

Short names (`case-1`, `case-2`, `case-3`) and full names
(`case-1-regression`, `case-2-sensor`, `case-3-ripple`) both work.

## What each case shows

- **case-1-regression** — a refactor that routes the staging-bucket
  variable through a helper. The helper has a typo. `git diff` shows three
  innocuous lines; `airflow-diff` shows four `load_staging_*` tasks rendering
  to the sentinel `<VAR:warehouse_buckte>`.
- **case-2-sensor** — `orders_pipeline` reschedules from `@hourly` to
  `0 */4 * * *`. `finance_rollup`'s `ExternalTaskSensor` keeps
  `execution_delta=timedelta(hours=1)`. The cross-DAG validator catches
  `incorrect_execution_delta`.
- **case-3-ripple** — a shared `build_report_cmd` helper is added and used
  by both DAGs. `finance_rollup` is classified `incidentally_affected` even
  though its `git diff` is small.

The captured markdown for each case lives in `../../docs/showcase/`.
