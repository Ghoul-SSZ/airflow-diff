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

Requires Python 3.10+ and `uv`, `git`, and `gh` on PATH for the CLI and Action
respectively.

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

Exit codes: `0` for no regressions, `1` when the PR introduces a DAG-level
regression (a DAG that imported cleanly at base now fails at head, or an added
DAG fails to import).

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
excluded_files = []
excluded_dag_ids = []
synthetic_logical_date = "2025-01-01T00:00:00+00:00"
render_timeout_seconds = 300
max_tasks_for_graph = 50
```

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
