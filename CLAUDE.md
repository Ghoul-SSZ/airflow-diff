# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`airflow-diff` renders an Apache Airflow 2.x DAG bag at two git commits, structurally diffs the rendered DAGs (including Jinja-expanded template fields), and emits a markdown PR comment / terminal / HTML report. Distributed as a PyPI CLI (`airflow-diff`) and a composite GitHub Action under `action/`.

Supported: Python 3.10–3.12 (host), Airflow 2.8.x / 2.9.x / 2.10.x (target), Linux + macOS.

## Common commands

Setup with `uv` (the project requires `uv`, `git`, and `gh` on PATH):

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

Tests:

```bash
pytest tests/unit -v                          # unit tests (no Airflow needed)
pytest tests/integration -v -m integration    # integration tests (require Airflow installed)
pytest tests/unit/test_diff.py::test_name -v  # single test
bash tests/smoke/test_action_entrypoint.sh    # smoke test for action/entrypoint.sh (uses fake `gh` and `airflow-diff` shims)
```

Integration tests are marked `@pytest.mark.integration` and import Airflow; CI runs them in a matrix of Python × Airflow versions (see `.github/workflows/test.yml`).

CLI smoke run:

```bash
airflow-diff diff <base-sha> <head-sha> --repo . --format markdown
airflow-diff report diff.json --format html --out report.html
```

Exit code `1` means the PR introduced a DAG-level regression (a DAG that imported cleanly at base now errors at head, or an added DAG fails to import). `0` otherwise.

## Architecture

The system is a 4-stage pipeline connected by JSON. The parent process **never imports Airflow** — only renderer subprocesses do, and each one runs against the user's own Airflow install at its commit.

```
orchestrator (host py)  →  renderer subprocess (per commit, in its own venv)
                       ↘                                              ↗
                         diff engine (pure)  →  presenter (md/term/html)
```

Key modules under `src/airflow_diff/`:

- **`cli.py`** — argparse entry point exposed as `airflow-diff`. Subcommands: `diff` (full pipeline), `report` (re-render a saved DiffDocument JSON), `render` (internal: render one commit).
- **`orchestrator.py`** — `run_diff()`. Resolves SHAs, prepares worktrees + venvs, spawns the renderer for each side **serially** (parallelism deferred), then calls `compute_diff`.
- **`worktree.py`** — wraps `git worktree add --detach`, cached under `/tmp/airflow-diff/worktrees/<sha>`. **Worktrees are intentionally not cleaned up** — the cache amortizes repeat runs against the same SHA. Cleanup is the user's responsibility (`git worktree prune`).
- **`venv.py`** — one venv per unique hash of (`requirements.txt` + `pyproject.toml` + `constraints.txt`), cached under `~/.cache/airflow-diff/venvs/<hash>`. Built via `uv`. Two commits with identical dep files share a venv. Also force-installs `pydantic` + `PyYAML` into every venv so the renderer subprocess can import `airflow_diff.schema` (the package itself is injected via `PYTHONPATH`, not pip-installed into each venv).
- **`renderer.py`** — runs inside the per-commit subprocess. Patches `Variable.get`, `BaseHook.get_connection`, and `TaskInstance.xcom_pull` with stubs **before** any user DAG import, then walks `dags_folder`, imports each `*.py`, finds `DAG` instances, and renders `template_fields` via `task.render_template(value, context)` against a synthetic Jinja context (`ds`, `ts`, `var`, `conn`, `ti`, `macros`, ...). Emits a `RenderedDagBag` JSON on stdout. Per-DAG and per-field errors are caught and recorded; one bad DAG does not abort the bag. **This is the only module that imports Airflow.**
- **`diff.py`** — pure function `compute_diff(base_bag, head_bag, touched_files)` → `DiffDocument`. Knows nothing about Airflow internals; operates only on `schema.py` types. Classifies DAGs as `touched` / `incidentally_affected` / `added` / `removed` and computes `pair_status` (`ok` / `regressed` / `fixed` / `still_broken`).
- **`schema.py`** — canonical Pydantic v2 models with `extra="forbid"`. `SCHEMA_VERSION = 1` is the wire-format version between renderer and orchestrator; **bump it when changing the wire shape.** `RenderedDag` has a model validator enforcing the `status`/payload invariant (status=`error` ⇒ no `tasks`/`attrs`/etc.; status=`ok` ⇒ no `error`).
- **`config.py`** — loads optional `.airflow-diff.toml` (repo root) and `.airflow-diff/fixtures.yaml` (path is configurable). Both are Pydantic models with `extra="forbid"`.
- **`present/{markdown,terminal,html}.py`** — presenters that consume a `DiffDocument`. Markdown is the default and is what gets posted as a PR comment; it honors GitHub's 65,536-char comment limit by truncating with a footer pointing at the uploaded HTML artifact.

### Provenance & stubs

The renderer never reads real Airflow Variables/Connections. Anything unresolved is rendered as a sentinel string the diff engine recognizes:

- `<VAR:bucket>` for `Variable.get("bucket")` / `var.value.bucket`
- `<CONN:warehouse.host>` for `conn.warehouse.host` or `BaseHook.get_connection("warehouse").host`
- `<XCOM:upstream.return_value>` for `ti.xcom_pull(...)`
- `<RENDER_ERROR: TypeName>` when an individual template field raises during rendering

A user can override these by committing `.airflow-diff/fixtures.yaml` with real values. The `ProvenanceEntry` list on each `RenderedField` records which stubs/fixtures contributed, so the presenter can annotate diffs.

## How the GitHub Action runs

`action/action.yml` is a composite action: it sets up Python, `pipx install uv`, `pip install airflow-diff==<version>`, then runs `action/entrypoint.sh`. The entrypoint:

1. Refuses to run on **fork PRs** (`pull_request.base.repo != head.repo`) — exits 0 with a warning. The renderer imports arbitrary Python; running it on untrusted forks is unsafe.
2. Reads `base.sha` / `head.sha` from `$GITHUB_EVENT_PATH`.
3. Runs `airflow-diff diff` → markdown + JSON, then `airflow-diff report` → HTML.
4. Posts the markdown via `gh pr comment --edit-last --body-file ...` (falls back to a new comment if no prior one).
5. Always uploads `/tmp/airflow-diff-report.html` as an artifact.

The action requires `actions/checkout@v4` with `fetch-depth: 0` so the base SHA is reachable.

## Conventions to preserve

- **Parent never imports Airflow.** All Airflow imports happen inside `renderer.py`, which only runs as a subprocess (`python -m airflow_diff.renderer`).
- **`extra="forbid"` on every Pydantic model.** Wire-shape drift between renderer and orchestrator should fail loudly, not silently.
- **Per-DAG isolation in the renderer.** A single failing DAG must not abort the bag; capture the exception into a `RenderError` and emit `status="error"` for that DAG only.
- **Defensive iteration over Airflow internals** that vary across 2.8/2.9/2.10 (see `_extract_dataset_uris` in `renderer.py` — `dataset_triggers` is not iterable on 2.9). When adding any DAG-attribute extraction, assume the attribute may not exist or may not be iterable, and test the integration matrix.
- **JSON contract via `schema.py`.** New fields on the renderer side must be added as `Optional` (or with defaults) and `SCHEMA_VERSION` bumped if the shape changes incompatibly.

## Docs

`docs/superpowers/specs/2026-05-17-airflow-diff-design.md` is the original design spec — the section numbering (e.g. "per spec section 7") is referenced from code comments.
