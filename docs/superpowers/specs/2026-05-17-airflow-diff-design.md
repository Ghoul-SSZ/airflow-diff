# airflow-diff — Design Spec

**Date:** 2026-05-17
**Status:** Approved for implementation planning
**Scope:** MVP (v1.0)

## 1. Problem statement

Airflow DAG bugs often hide behind layers of Python evaluation and Jinja templating. A reviewer looking at a PR's textual `git diff` sees Python source changes, but what actually runs in production is the *imported, template-rendered* DAG — a different object with its own task list, dependencies, schedule, and rendered operator parameters. Two common, painful failure modes:

1. **A refactor changes how a templated parameter expands.** The Python source looks fine in code review; the rendered `bash_command` is subtly different in prod.
2. **A change to a shared helper or factory function silently mutates dozens of unrelated DAGs.** The PR touches one file; the impact lands across the DAG bag.

`airflow-diff` renders the DAG bag at two commits (typically a PR's base and head), structurally compares them, and surfaces the differences — including rendered template values — in a GitHub PR comment. The goal is to catch "looked fine in code review, broke in prod" classes of bug at PR time.

## 2. Goals / Non-goals

### Goals (MVP)

- Render the full DAG bag at two arbitrary commits of a single repo.
- Surface differences in DAG attributes, task structure, dependencies, rendered template fields, TaskGroups, dataset/asset definitions, and DAG-bag membership (DAGs added/removed/factory output changed).
- Cover DAGs the PR touched directly *and* DAGs incidentally affected by shared-code changes.
- Ship as a `pip`-installable CLI on PyPI (`airflow-diff`).
- Ship as a published GitHub Action that posts the diff as a PR comment.
- Render text-diff + Mermaid graph + summary table, all in plain markdown that GitHub renders natively.
- Be fast enough for CI: target <60s wall-clock for a typical repo (≤100 DAGs, deps unchanged in PR).

### Supported environments

- **Host Python:** 3.10, 3.11, or 3.12 (the `airflow_diff` package itself).
- **Target Airflow:** 2.8.x, 2.9.x, 2.10.x (whatever is pinned in the user's repo at each commit).
- **OS:** Linux and macOS only. Windows is unsupported.

### Non-goals (explicit, for MVP)

- Airflow 3.x support (renderer errors out early if encountered).
- Rendering dynamic task mapping (`.expand()` / `.expand_kwargs()`) — captured structurally but not expanded into mapped instances.
- Callback diffing (`on_failure_callback`, etc.) — function references don't serialize usefully.
- Pulling real Variables/Connections from a live Airflow metastore.
- Diffing function-reference fields beyond their qualified name.
- Sandboxed execution of imported user code — see Limitations.
- Windows support.
- Cross-repo diffing (both commits must be in the same repo).
- Rename detection for tasks or DAGs (renames appear as add+remove).

## 3. Decision summary

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Rendering depth | Imported DAG + Jinja-rendered templates | Catches the actual class of bug we care about; structural diff alone misses template expansion changes. |
| 2 | Airflow versions | 2.x only | Largest user base; stable APIs; 3.x can be added later without rewriting the diff engine. |
| 3 | Rendering context | Synthetic stubs by default, optional fixtures file to override | Day-one usable with zero config; teams that want precision can layer fixtures. |
| 4 | Distribution | CLI first, GitHub Action second; both first-class on release | Engine has to exist regardless; CLI gives the engine a clean, testable surface. |
| 5 | PR comment layout | Summary table + collapsible per-change details + headline Mermaid diff graph | Scales from 1-task PRs to many-DAG PRs without losing scannability. |
| 6 | MVP feature scope | Core DAG attrs, operators+deps, Jinja stubs, TaskGroups, DAG factories, custom operators, datasets | Covers the common cases; defers expensive/low-value features. |
| 7 | Dependency isolation | Strict per-commit venv via `uv` | Accurate for the dep-drift case; fast enough thanks to uv. |
| 8 | DAG scope per run | Render all DAGs at both commits; layered output (touched prominent, incidentally affected collapsed) | The shared-helper case is the bug class we most need to catch. |
| 9 | Error handling | Per-DAG isolation; exit non-zero only for regressions introduced by the PR | Right CI signal: red means "this PR broke something." |

## 4. Architecture

The system is a Python CLI (`airflow-diff`) split into four execution stages connected by JSON. Subprocesses do the dangerous work (importing arbitrary user code under two different Airflow installs); the parent process never imports Airflow.

```
┌─────────────────────────────────────────────────────────────────┐
│  Orchestrator (parent process, host Python — no Airflow needed) │
│  ─ parses args, resolves base/head SHAs                         │
│  ─ creates two git worktrees                                    │
│  ─ spawns one renderer subprocess per commit                    │
│  ─ waits for both to finish, reads their JSON output            │
│  ─ runs diff engine in-process                                  │
│  ─ runs presenter in-process                                    │
│  ─ writes to stdout / file / PR comment                         │
└─────────────────────────────────────────────────────────────────┘
       │                                       │
       ▼                                       ▼
┌──────────────────────┐               ┌──────────────────────┐
│ Renderer subprocess  │               │ Renderer subprocess  │
│ commit: <sha-base>   │               │ commit: <sha-head>   │
│ worktree: /tmp/base  │               │ worktree: /tmp/head  │
│ venv: uv-managed,    │               │ venv: uv-managed,    │
│   isolated, installs │               │   isolated, installs │
│   base/requirements  │               │   head/requirements  │
│                      │               │                      │
│ → imports DAG bag    │               │ → imports DAG bag    │
│ → renders templates  │               │ → renders templates  │
│ → emits rendered.json│               │ → emits rendered.json│
└──────────────────────┘               └──────────────────────┘
       │                                       │
       └───────────────────┬───────────────────┘
                           ▼
              ┌─────────────────────────┐
              │ Diff engine (in parent) │
              │ matches DAGs by dag_id, │
              │ tasks by task_id        │
              │ → emits diff.json       │
              └─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │ Presenter (swappable)   │
              │ markdown │ terminal │ html
              └─────────────────────────┘
```

### Architectural commitments

- **The parent process never imports Airflow.** The host environment needs only `git`, `uv`, and our package. This avoids version conflicts between the host's Python deps and either commit's Airflow install.
- **Subprocesses communicate via JSON on stdout.** No pickle, no IPC libraries, no shared memory. Each renderer is independently runnable for debugging.
- **Per-commit uv venvs, keyed by hashed `requirements`.** `uv venv` + `uv pip install -r requirements.txt`. Cache key is the hash of `requirements.txt` + `pyproject.toml` + `constraints.txt` (whichever exist), not the commit SHA — so two commits with identical deps share a venv. Typical cache-miss install: 3–8s.
- **Presenters are pure strategies over the diff document.** Markdown for the PR comment, ANSI text for terminal, HTML for the standalone-artifact case. Each is a function `DiffDocument → str`.
- **The GitHub Action is a 30-line shim.** Resolves SHAs from `GITHUB_EVENT`, invokes the CLI with `--format markdown`, posts via `gh pr comment --edit-last` (subsequent pushes to the same PR update the comment in place).

## 5. Components

Single Python package `airflow_diff` with these modules. Each has one job, a small public interface, and minimal coupling.

### `airflow_diff.cli`

Argparse entry point. Subcommands:

- `render <ref> [--out FILE]` — internal/debug; runs a single renderer subprocess and emits its JSON.
- `diff <ref-a> <ref-b> [--format markdown|terminal|html] [--out FILE]` — main user command.
- `report <diff.json> [--format ...]` — re-format an existing diff document (useful for cached CI runs).

### `airflow_diff.orchestrator`

Public: `run_diff(repo_root, ref_a, ref_b, config) -> DiffDocument`. `repo_root` is an explicit `Path` rather than implicit cwd so the CLI's `--repo` flag and the GitHub Action's `$GITHUB_WORKSPACE` both flow through cleanly. Resolves SHAs, calls worktree manager twice, calls venv manager twice, spawns renderer subprocesses in parallel, reads their JSON, hands off to diff engine, returns diff document.

### `airflow_diff.worktree`

Thin wrapper around `git worktree`. Public: context manager `worktree_for(sha)` → path. Cache layout: `/tmp/airflow-diff/worktrees/<sha>/`. Re-runs on the same SHA reuse existing worktrees.

### `airflow_diff.venv`

Wraps `uv venv` + `uv pip install`. Cache layout: `~/.cache/airflow-diff/venvs/<hash-of-requirements>/`. Hash key is the contents of `requirements.txt` + `pyproject.toml` + `constraints.txt` (whichever exist), not the commit SHA. Public: `venv_for(worktree_path) -> Path` (returns the Python interpreter path inside the venv).

### `airflow_diff.renderer`

*Runs only inside a subprocess with the per-commit venv activated.* The most nuanced component.

Public entry point: `python -m airflow_diff.renderer --worktree <path> --config <json>`. Internally:

1. Adds `<worktree>/dags/` and `<worktree>/plugins/` to `sys.path`.
2. **Installs the stub layer before any Airflow import:**
   - `airflow.models.Variable.get(key, ...)` → `f"<VAR:{key}>"` (overridden if `fixtures.variables[key]` exists).
   - `airflow.hooks.base.BaseHook.get_connection(conn_id)` → a `Connection`-shaped object with stub fields like `host=f"<CONN:{conn_id}.host>"` (or fixture-supplied real values).
   - `TaskInstance.xcom_pull(...)` → `f"<XCOM:{task_ids}.{key}>"`. Always stubbed — no fixture support; XComs are inherently runtime.
3. Walks `dags/` collecting `.py` files. For each, imports as a module via `importlib.util.spec_from_file_location`, captures any `DAG` instances in module globals (handles DAG factories — multiple DAGs per file).
4. Builds a synthetic Jinja context: `{"ds": "2025-01-01", "logical_date": datetime(2025,1,1,tzinfo=UTC), "ti": <stub>, "params": dag.params, "var": <stub namespace>, "conn": <stub namespace>, ...}` — the full macro set Airflow injects at task runtime.
5. For each DAG → each task → each name in `task.template_fields`: calls `task.render_template(getattr(task, field), context)` and records the result.
6. Captures structural info: operator class FQN, `template_fields` (post-render), `__init__`-signature kwargs (literal capture of non-templated values), upstream/downstream task IDs, TaskGroup hierarchy, DAG-level attrs, `inlets`/`outlets`.
7. Per-DAG failures (import error, render error during step 3 or 5) are caught and recorded as error entries; the subprocess does not exit non-zero.
8. Emits canonical JSON on stdout (see Section 6 for shape).

### `airflow_diff.schema`

Pydantic models. Single source of truth for JSON shapes. Both renderer and diff engine validate against these. Versioned (`schema_version: 1`) so future changes don't silently break consumers.

Top-level types:
- `RenderedDagBag` — renderer's output.
- `RenderedDag`, `RenderedTask`, `RenderedField` (with `provenance: list[ProvenanceEntry]` — entries are `{source: literal|stub|fixture, key: str | None}`).
- `RenderError` (per-DAG and per-field flavors).
- `DiffDocument` — diff engine's output.
- `DagDiff`, `TaskDiff`, `FieldDiff`, `EdgeDiff`.

### `airflow_diff.diff`

Pure function: `compute_diff(rendered_a: RenderedDagBag, rendered_b: RenderedDagBag, touched_files: list[str]) -> DiffDocument`.

Algorithm:

1. **DAG matching** by `dag_id`. Classify as `compare` (both), `added` (B only), `removed` (A only).
2. **Per-DAG status comparison** for the `compare` set. If either side is `error`:
   - error→ok: `fixed`
   - ok→error: `regressed`
   - error→error: `still_broken`
3. **Per-task comparison** (when both sides rendered ok): match tasks by `task_id`. For each:
   - Field diff per field name in either side (added/removed/modified/unchanged). Modified fields capture both rendered values and both provenance arrays.
   - Edge diff on `upstream`/`downstream` sets.
   - Operator-class change flagged separately.
4. **DAG-level attribute diff**: `schedule`, `start_date`, `catchup`, `tags`, `default_args`, etc.
5. **Touched-DAG classification**: each DAG marked `touched` if its source file is in `touched_files` (from `git diff --name-only base head`), else `incidentally_affected` (if it has any diffs) or hidden (no diffs).

Knows nothing about Airflow — operates purely on the canonical schema. Highly testable.

### `airflow_diff.present`

Subpackage with one module per output format. Each exports `render(diff: DiffDocument, config) -> str`:

- `markdown.py` — GitHub-flavored markdown. Generates the Mermaid combined-diff graph, the summary table, and collapsible details. Warning banner if any DAGs failed to render. Truncates with link-to-artifact if output exceeds GitHub's 65,536-char comment limit.
- `terminal.py` — ANSI-colored text for CLI use.
- `html.py` — standalone HTML for the artifact case (large diffs, or `--format html` from CLI).

### `airflow_diff.config`

Loads `.airflow-diff.toml` from repo root, with these keys and defaults:

```toml
dags_folder         = "dags"                       # relative to worktree root
plugins_folder      = "plugins"
fixtures_path       = ".airflow-diff/fixtures.yaml"
excluded_files      = []                           # glob patterns matched against source file path (relative to dags_folder)
excluded_dag_ids    = []                           # glob patterns matched against dag_id
synthetic_logical_date = "2025-01-01T00:00:00+00:00"
render_timeout_seconds = 300
max_tasks_for_graph    = 50                        # above this, graph is simplified
```

Exclusion semantics: a DAG is excluded if its source file matches any `excluded_files` glob OR its `dag_id` matches any `excluded_dag_ids` glob. Excluded DAGs are not rendered at either commit and do not appear in the diff.

Loads fixtures YAML on top:

```yaml
variables:
  bucket: "my-prod-bucket"
  region: "us-east-1"
connections:
  warehouse:
    host: "wh.example.com"
    schema: "analytics"
```

### `action/` (separate top-level directory)

`action.yml` declaring inputs (`base-sha`, `head-sha`, `github-token`, `python-version`, `airflow-diff-version`). A small `entrypoint.sh` shim:

1. Resolves base/head SHAs from `$GITHUB_EVENT_PATH` if not passed explicitly.
2. Refuses to run if the PR's head repo differs from the base repo (i.e., the PR is from a fork — untrusted code, see Limitations). Applies to both `pull_request` and `pull_request_target` events.
3. Installs the CLI: `pip install airflow-diff==${{ inputs.airflow-diff-version }}`.
4. Runs: `airflow-diff diff "$BASE_SHA" "$HEAD_SHA" --format markdown --out /tmp/comment.md`.
5. Posts: `gh pr comment "$PR_NUMBER" --edit-last --body-file /tmp/comment.md`.

Total Action code: ~30 lines of shell.

### Dependency direction

`cli` → `orchestrator` → `{worktree, venv, diff, present}` → `schema`. `renderer` is only invoked by `orchestrator` via subprocess (no Python-level import from the parent). Everyone uses `schema`. **Only `renderer` imports Airflow.**

## 6. Data flow

End-to-end trace with concrete JSON shapes at each handoff.

### Step 0 — Invocation

```bash
$ airflow-diff diff abc1234 def5678 --format markdown --out comment.md
```

Or, from inside the GitHub Action:

```bash
$ airflow-diff diff $BASE_SHA $HEAD_SHA --format markdown --out /tmp/comment.md
$ gh pr comment $PR_NUMBER --edit-last --body-file /tmp/comment.md
```

### Step 1 — Orchestrator setup

- Parses args; loads `.airflow-diff.toml` from current repo root.
- Resolves SHAs to full hashes via `git rev-parse`.
- Calls `worktree.worktree_for(sha)` twice → two paths under `/tmp/airflow-diff/worktrees/`.
- Calls `venv.venv_for(worktree)` twice → cache hit returns existing venv path; cache miss triggers `uv venv` + `uv pip install`.
- Computes `touched_files = git diff --name-only <base> <head>`.

### Step 2 — Renderer subprocess (one per commit, in parallel)

Spawned as: `<venv>/bin/python -m airflow_diff.renderer --worktree <path> --config <json>`. See Section 5 for the internal flow.

Emits canonical JSON on stdout:

```json
{
  "schema_version": 1,
  "commit_sha": "abc1234...",
  "airflow_version": "2.10.3",
  "rendered_at": "2026-05-17T12:34:56Z",
  "dags": [
    {
      "dag_id": "my_dag",
      "status": "ok",
      "source_file": "dags/my_dag.py",
      "attrs": {
        "schedule": "0 5 * * *",
        "start_date": "2024-01-01T00:00:00+00:00",
        "catchup": false,
        "tags": ["etl", "daily"]
      },
      "datasets": {"inlets": [], "outlets": ["s3://bucket/output"]},
      "task_groups": [{"group_id": "transform", "tasks": ["clean", "enrich"]}],
      "tasks": [
        {
          "task_id": "extract_data",
          "operator": "airflow.operators.bash.BashOperator",
          "task_group": null,
          "upstream": ["start"],
          "downstream": ["transform"],
          "fields": {
            "bash_command": {
              "rendered": "aws s3 cp s3://<VAR:bucket>/2025-01-01 /tmp/in",
              "provenance": [{"source": "stub", "key": "var.value.bucket"}]
            },
            "retries": {"rendered": 3, "provenance": [{"source": "literal"}]}
          }
        }
      ]
    },
    {
      "dag_id": "broken_dag",
      "status": "error",
      "source_file": "dags/broken_dag.py",
      "error": {
        "type": "ImportError",
        "message": "cannot import name 'Foo' from 'my_company.helpers'",
        "traceback": "..."
      }
    }
  ]
}
```

### Step 3 — Orchestrator collects renderer output

Reads each renderer's stdout; validates against `RenderedDagBag`. If either subprocess emits invalid JSON or crashes, the whole run fails with the bad output captured.

### Step 4 — Diff engine

Pure function. See Section 5 for the algorithm. Emits:

```json
{
  "schema_version": 1,
  "base_sha": "abc1234...",
  "head_sha": "def5678...",
  "summary": {
    "dags_touched": 1, "dags_incidentally_affected": 3,
    "dags_added": 0, "dags_removed": 0,
    "dags_regressed": 0, "dags_fixed": 0
  },
  "dags": [
    {
      "dag_id": "my_dag",
      "classification": "touched",
      "status_a": "ok", "status_b": "ok",
      "attr_diffs": [{"name": "schedule", "before": "0 5 * * *", "after": "0 6 * * *"}],
      "task_diffs": [
        {
          "task_id": "extract_data",
          "change_type": "modified",
          "field_diffs": [
            {
              "name": "bash_command",
              "before": "aws s3 cp s3://<VAR:bucket>/2025-01-01 /tmp/in",
              "after":  "aws s3 cp s3://<VAR:bucket_v2>/2025-01-01 /tmp/in",
              "provenance_before": [{"source": "stub", "key": "var.value.bucket"}],
              "provenance_after":  [{"source": "stub", "key": "var.value.bucket_v2"}]
            }
          ],
          "edge_diffs": []
        },
        {"task_id": "validate_data", "change_type": "added"}
      ]
    }
  ],
  "render_errors": [
    {"dag_id": "broken_dag", "side": "both"}
  ]
}
```

### Step 5 — Presenter

`markdown.render(diff_document, config)` produces the comment: warning banner if any DAGs failed to render, summary line, per-DAG sections (touched first, then collapsed "incidentally affected"). Each DAG section contains:

1. Header with change counts.
2. Combined diff Mermaid graph (color-coded: green = added, red dashed = removed, yellow = modified, gray = unchanged).
3. Summary table of changes (one row per changed task/edge/attr).
4. Collapsible `<details>` per field showing the text diff with `<VAR:...>`/`<CONN:...>` placeholders rendered literally.
5. Optional collapsible "side-by-side base/head graphs" details.

### Step 6 — Output

Orchestrator writes to stdout or `--out <path>`. CLI exits per error-handling rules in Section 7.

### Step 7 — Action wrapper (CI only)

`gh pr comment --edit-last $PR --body-file /tmp/comment.md` so subsequent pushes update the existing comment rather than appending new ones.

## 7. Error handling & edge cases

### Process / orchestration errors

| Failure | Policy |
|---|---|
| Invalid SHA (`git rev-parse` fails) | Fail fast, exit 2, clear error pointing at the bad ref. |
| Shallow CI clone doesn't include base SHA | Detect (`git cat-file -e <sha>` fails); instruct user to set `fetch-depth: 0` in `actions/checkout`. |
| `requirements.txt` won't install (`uv` fails) | Fail fast, exit 2, include uv's error output. |
| Renderer subprocess crashes (segfault, OOM, killed) | Fail fast, exit 3, include worktree path and last bytes of stderr. |
| Renderer emits invalid JSON | Same — exit 3, with offending output captured. |
| Renderer exceeds timeout (default 5 min per commit, configurable via `render_timeout_seconds`) | Kill, fail with timeout error. |
| Concurrent runs against same SHAs | Worktrees keyed by SHA, safe to share. uv has its own venv cache locking. We add no extra lock. |

### Per-DAG errors (renderer catches, continues)

| Failure | DiffDocument status | PR comment shows | Exit code impact |
|---|---|---|---|
| Import error at A, OK at B | `fixed` | "Fixed: was broken at base, now imports." | None — green. |
| OK at A, import error at B | `regressed` | "Regression: this PR broke this DAG." | Non-zero. |
| Import error at both | `still_broken` | Collapsed section, no detail. | None — pre-existing. |
| Single field render error | Field marked `render_error` with traceback; other fields on same task still render. | "render error" row in the table; traceback in collapsible. | Non-zero only if the field rendered at A and errored at B. |
| DAG file produces N DAGs at A and M at B (factory) | Each `dag_id` independently added/removed/compared. | Each as its own entry. | Standard rules per dag_id. |

### Data integrity / schema

- Both renderers must emit the same `schema_version`. Both are invoked via the host-installed `airflow_diff` package, so this holds by construction. Validated at JSON load.
- Pydantic validation on both sides — schema violations fail fast.
- Tasks matched by `task_id` only. Renames appear as one `removed` + one `added`. Rename heuristics deferred to v1.1.
- DAGs matched by `dag_id` only. A `dag_id` move from `dags/old.py` to `dags/new.py` produces zero diffs (correct: nothing the scheduler sees changed).

### Output-size edge cases

- **Comment exceeds GitHub's 65,536-char limit:** markdown presenter checks length. If over, emits a truncated comment ("…12 more DAGs not shown") and writes the full report to `report.html`. Action wrapper uploads as a workflow artifact and links from the truncated comment.
- **Mermaid blocks have practical size limits in GitHub.** For any DAG over `max_tasks_for_graph` (default 50) tasks, the combined diff graph is replaced with a "summary box" (counts of added/removed/modified tasks) and the side-by-side graph section is omitted.

## 8. Testing strategy

### Test layers

| Layer | What's tested | How |
|---|---|---|
| Unit — `schema` | Pydantic models round-trip JSON; `schema_version` validation. | Plain pytest. |
| Unit — `diff` | Every combination of add/remove/modify across DAGs, tasks, fields, edges, attrs; status transitions; classification (touched vs. incidental). | Hand-built `RenderedDagBag` fixtures; no subprocess, no Airflow. |
| Unit — `worktree`, `venv` | Cache-key hashing, cleanup, idempotent reuse. | Subprocess calls mocked. |
| Unit — `config` | TOML parsing, defaults, fixtures-YAML loading, malformed config. | Plain pytest with tiny fixture files. |
| Snapshot — `present.{markdown,terminal,html}` | Output formatting doesn't silently regress. | Curated `DiffDocument` fixtures × each presenter, compared via `syrupy`. Updates require `--snapshot-update` and show in PR diff. |
| Integration — `renderer` | Renderer correctly imports, stubs, and renders real DAG code. | DAG fixture library (below). Real subprocess with real Airflow in test venv. Output validated against `schema` and snapshot-compared. |
| Integration — end-to-end CLI | Full pipeline works against a real two-commit repo. | `tests/fixtures/sample_repo/` built programmatically (session-scoped fixture: `git init`, write files, commit, modify, commit). `airflow-diff diff <a> <b>` runs end-to-end; markdown output snapshot-checked. |
| Smoke — Action wrapper | `entrypoint.sh` parses `GITHUB_EVENT`, invokes CLI correctly, posts via stubbed `gh`. | Test harness sets `GITHUB_EVENT_PATH`; `gh` stubbed by a wrapper script that records argv. |

### DAG fixture library (`tests/fixtures/dags/`)

Each fixture is one `.py` file targeting one rendering behavior. Required coverage:

- Linear DAG with `BashOperator`.
- DAG with nested + flat TaskGroups.
- DAG with custom operator defined in a sibling `plugins/`.
- DAG using `Variable.get('x')`, `{{ var.value.y }}`, `BaseHook.get_connection('z')`.
- DAG using `{{ ti.xcom_pull(task_ids='upstream') }}`.
- DAG with assets/datasets (`inlets` + `outlets`).
- DAG factory: single `.py` producing 3 DAGs from a config dict.
- DAG that raises `ImportError` at import time.
- DAG that raises during operator `__init__`.
- DAG with a template referencing a missing macro.
- DAG with operator whose `template_fields` includes a deeply-nested dict (e.g., `params={...}`).

Two parallel fixture trees — `dags_base/` and `dags_head/` — share most files but differ where integration tests need to assert specific changes.

### CI matrix

```yaml
python-version: ["3.10", "3.11", "3.12"]
airflow-version: ["2.8.4", "2.9.3", "2.10.3"]
```

Nine cells. Airflow's API drifts across minors (especially `Connection`, `Variable`, `BaseOperator.render_template`), so the matrix catches silent breaks. uv makes installs cheap (~10s/cell).

### Build order (TDD)

Tests written before each layer:

1. `schema` — define and test canonical types first.
2. `diff` — pure function against hand-built fixtures. Highest test density.
3. `present.markdown` — snapshot tests using diff fixtures from step 2. Now PR-comment output is visible before any subprocess exists.
4. `config`, `worktree`, `venv` — mock-heavy.
5. `renderer` — DAG fixture library + real-subprocess integration tests. Slowest layer.
6. `orchestrator` — wire everything; end-to-end test against sample-repo fixture.
7. `present.terminal`, `present.html` — once markdown and the diff document shape are stable.
8. Action wrapper — last, thin shim over a working CLI.

### Not tested in MVP

- Concurrency stress on worktree/venv caches (rely on git/uv internal locking).
- DAG fixtures for every operator in every provider package — just stdlib operators + a representative custom operator.
- Windows.

## 9. Known limitations

These get explicit callouts in the README so users aren't surprised.

- **Code that does real work at import time hits stubs and may crash.** Example: `hook = PostgresHook(conn_id='x'); SQL = hook.get_records('SELECT ...')` at module top level. `BaseHook.get_connection` returns a placeholder, not a working connection. Such DAGs render as broken; the fix is to defer the work into a `@task` or operator kwargs.
- **No sandboxing of imported user code.** The renderer `import`s arbitrary user code from both commits. **Do not run against PRs from untrusted forks.** Suitable for internal repos and trusted contributors only. The Action refuses to run when the PR's head repo differs from the base repo (i.e., any fork PR), regardless of event type.
- **No network calls during rendering.** Renderer does not connect to a live Airflow metastore. If user code reaches external services at import time, that's on the user code.
- **Dynamic task mapping (`.expand()`) not expanded.** Captured structurally (`expand_args` recorded) but not unrolled into mapped instances. Full mapping is v1.1.
- **Airflow 3.x not supported.** Renderer errors out early if either commit's Airflow is 3.x.
- **Linux + macOS only.** Windows is unsupported.
- **Single-repo only.** Both commits must exist in the same git repo.
- **No rename detection.** Renamed tasks or DAGs appear as remove + add.

## 10. Future work (post-MVP)

- v1.1: dynamic task mapping (option `e` from MVP scoping); rename detection heuristics; same-env fast path when deps unchanged.
- v1.2: Airflow 3.x support.
- Later: live Airflow metastore option for fixture values (with strict opt-in and clear security warnings); callback-reference diffing if a useful representation emerges; provider-operator-specific renderers for richer output on common operators.
