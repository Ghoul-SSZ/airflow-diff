# airflow-diff MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the v1.0 MVP of `airflow-diff` — a Python CLI plus GitHub Action that renders an Airflow 2.x DAG bag at two git commits, structurally compares the rendered outputs (including Jinja-rendered template fields), and emits a GitHub-flavored markdown PR comment with a Mermaid diff graph, summary table, and collapsible per-field text diffs.

**Architecture:** Single Python package `airflow_diff`. Parent process orchestrates; it never imports Airflow. Two renderer subprocesses (one per commit) each run in an isolated uv-managed venv built from that commit's `requirements.txt`, import the DAG bag, render templates against synthetic + fixture context, and emit canonical JSON. Parent reads both JSONs, runs a pure-function diff engine, and pipes the result through a swappable presenter (markdown / terminal / html). A 30-line shell shim wraps the CLI as a GitHub Action.

**Tech Stack:** Python 3.10+, Pydantic v2, pytest, syrupy (snapshot tests), PyYAML, tomllib (stdlib 3.11+) / tomli (3.10), Jinja2 (transitively via Airflow), `uv` (external, used as subprocess for venv mgmt), `git` (external), `gh` (external, used only in Action).

---

## Reference: target file layout

```
airflow-diff/
├── pyproject.toml
├── README.md
├── .gitignore                              # exists
├── .github/workflows/test.yml
├── action/
│   ├── action.yml
│   └── entrypoint.sh
├── docs/superpowers/
│   ├── specs/2026-05-17-airflow-diff-design.md    # exists
│   └── plans/2026-05-17-airflow-diff-mvp.md       # this file
├── src/airflow_diff/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── orchestrator.py
│   ├── worktree.py
│   ├── venv.py
│   ├── renderer.py
│   ├── schema.py
│   ├── diff.py
│   ├── config.py
│   └── present/
│       ├── __init__.py
│       ├── markdown.py
│       ├── terminal.py
│       └── html.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── unit/
    │   ├── test_schema.py
    │   ├── test_diff.py
    │   ├── test_worktree.py
    │   ├── test_venv.py
    │   ├── test_config.py
    │   └── present/
    │       ├── test_markdown.py
    │       ├── test_terminal.py
    │       └── test_html.py
    ├── integration/
    │   ├── test_renderer.py
    │   └── test_cli.py
    ├── smoke/
    │   └── test_action_entrypoint.sh
    ├── fixtures/
    │   ├── dags_base/                      # parallel DAG sets used by integration tests
    │   ├── dags_head/
    │   ├── plugins/operators.py
    │   ├── sample_repo_builder.py
    │   └── diff_documents/                  # JSON fixtures for presenter snapshot tests
    └── __snapshots__/                       # syrupy snapshot dir
```

---

## Phase 0 — Project scaffolding

### Task 0: pyproject, src layout, pytest

**Files:**
- Create: `pyproject.toml`
- Create: `README.md`
- Create: `src/airflow_diff/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 0.1: Create `pyproject.toml`**

```toml
[project]
name = "airflow-diff"
version = "0.1.0"
description = "Render and diff Apache Airflow DAGs across two git commits"
readme = "README.md"
requires-python = ">=3.10"
license = {text = "Apache-2.0"}
dependencies = [
    "pydantic>=2.5",
    "PyYAML>=6.0",
    "tomli>=2.0; python_version<'3.11'",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4",
    "pytest-cov>=4.1",
    "syrupy>=4.6",
    # Airflow installed only for renderer integration tests:
    "apache-airflow==2.10.3",
]

[project.scripts]
airflow-diff = "airflow_diff.cli:main"

[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"
markers = [
    "integration: integration tests that import Airflow",
]
```

- [ ] **Step 0.2: Create `README.md`** (stub — final content in Task 22)

```markdown
# airflow-diff

Render and diff Apache Airflow 2.x DAGs across two git commits.

Status: under development. See `docs/superpowers/specs/2026-05-17-airflow-diff-design.md`.
```

- [ ] **Step 0.3: Create empty package files**

```bash
mkdir -p src/airflow_diff/present tests/unit/present tests/integration tests/smoke tests/fixtures
touch src/airflow_diff/__init__.py src/airflow_diff/present/__init__.py
touch tests/__init__.py tests/unit/__init__.py tests/unit/present/__init__.py tests/integration/__init__.py
```

- [ ] **Step 0.4: Create `tests/conftest.py`** (empty for now; fixtures added per-task)

```python
"""Shared pytest fixtures and config."""
```

- [ ] **Step 0.5: Verify install works**

Run: `uv venv && uv pip install -e ".[dev]"`
Expected: install succeeds, no errors.

Run: `uv run pytest`
Expected: pytest runs, collects 0 tests, exits 0.

- [ ] **Step 0.6: Commit**

```bash
git add pyproject.toml README.md src/ tests/
git commit -m "chore: scaffold project (pyproject, src layout, pytest config)"
```

---

## Phase 1 — Schema foundation

### Task 1: Canonical Pydantic models

**Files:**
- Create: `src/airflow_diff/schema.py`
- Create: `tests/unit/test_schema.py`

This is the foundation — every other component consumes these types. Build it test-first.

- [ ] **Step 1.1: Write failing test for `RenderedField` round-trip**

`tests/unit/test_schema.py`:

```python
import json
from airflow_diff.schema import RenderedField, ProvenanceEntry


def test_rendered_field_literal_round_trip():
    field = RenderedField(rendered=3, provenance=[ProvenanceEntry(source="literal")])
    payload = field.model_dump_json()
    restored = RenderedField.model_validate_json(payload)
    assert restored == field


def test_rendered_field_stub_round_trip():
    field = RenderedField(
        rendered="aws s3 cp s3://<VAR:bucket>/foo /tmp/x",
        provenance=[ProvenanceEntry(source="stub", key="var.value.bucket")],
    )
    restored = RenderedField.model_validate_json(field.model_dump_json())
    assert restored == field


def test_provenance_source_validation():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ProvenanceEntry(source="bogus")
```

- [ ] **Step 1.2: Run test, confirm failure**

Run: `uv run pytest tests/unit/test_schema.py -v`
Expected: ImportError — `airflow_diff.schema` doesn't exist.

- [ ] **Step 1.3: Implement minimal `schema.py` for those tests**

`src/airflow_diff/schema.py`:

```python
"""Canonical Pydantic models for renderer output and diff documents.

Both the renderer subprocess and the parent orchestrator validate against these
types. The schema is versioned; bump SCHEMA_VERSION when changing wire shape.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


# ----- Renderer output ----------------------------------------------------

class ProvenanceEntry(_Model):
    source: Literal["literal", "stub", "fixture"]
    key: Optional[str] = None  # e.g. "var.value.bucket"; None for literal


class RenderedField(_Model):
    rendered: Any  # the post-render Python value (str, int, dict, list, ...)
    provenance: list[ProvenanceEntry] = Field(default_factory=list)
```

- [ ] **Step 1.4: Run the test, confirm pass**

Run: `uv run pytest tests/unit/test_schema.py -v`
Expected: all 3 tests pass.

- [ ] **Step 1.5: Add tests for `RenderError`, `RenderedTask`, `RenderedDag`, `RenderedDagBag`**

Append to `tests/unit/test_schema.py`:

```python
from airflow_diff.schema import (
    RenderError, RenderedTask, RenderedDag, RenderedDagBag,
    DagStatus, SCHEMA_VERSION,
)


def test_render_error_round_trip():
    err = RenderError(type="ImportError", message="boom", traceback="...")
    assert RenderError.model_validate_json(err.model_dump_json()) == err


def test_rendered_task_minimum():
    task = RenderedTask(
        task_id="extract",
        operator="airflow.operators.bash.BashOperator",
        task_group=None,
        upstream=[],
        downstream=["transform"],
        fields={},
    )
    assert RenderedTask.model_validate_json(task.model_dump_json()) == task


def test_rendered_dag_ok_status():
    dag = RenderedDag(
        dag_id="my_dag",
        status="ok",
        source_file="dags/my.py",
        attrs={"schedule": "@daily"},
        datasets={"inlets": [], "outlets": []},
        task_groups=[],
        tasks=[],
    )
    assert dag.status == "ok"
    assert RenderedDag.model_validate_json(dag.model_dump_json()) == dag


def test_rendered_dag_error_status():
    dag = RenderedDag(
        dag_id="broken",
        status="error",
        source_file="dags/broken.py",
        error=RenderError(type="ImportError", message="x", traceback="..."),
    )
    assert dag.status == "error"


def test_rendered_dag_bag_round_trip():
    bag = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha="abc123",
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, 12, 0, 0),
        dags=[],
    )
    restored = RenderedDagBag.model_validate_json(bag.model_dump_json())
    assert restored == bag


def test_rendered_dag_bag_rejects_wrong_schema_version():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        RenderedDagBag(
            schema_version=999,
            commit_sha="x",
            airflow_version="2.10.3",
            rendered_at=datetime.now(),
            dags=[],
        )
```

- [ ] **Step 1.6: Extend `schema.py`**

```python
DagStatus = Literal["ok", "error"]


class RenderError(_Model):
    type: str
    message: str
    traceback: str


class RenderedTask(_Model):
    task_id: str
    operator: str  # fully-qualified class name
    task_group: Optional[str] = None  # group_id of parent TaskGroup, or None
    upstream: list[str] = Field(default_factory=list)
    downstream: list[str] = Field(default_factory=list)
    fields: dict[str, RenderedField] = Field(default_factory=dict)


class TaskGroupInfo(_Model):
    group_id: str
    tasks: list[str]  # task_ids belonging directly to this group


class DatasetRefs(_Model):
    inlets: list[str] = Field(default_factory=list)
    outlets: list[str] = Field(default_factory=list)


class RenderedDag(_Model):
    dag_id: str
    status: DagStatus
    source_file: str
    # Present only when status == "ok":
    attrs: Optional[dict[str, Any]] = None
    datasets: Optional[DatasetRefs] = None
    task_groups: Optional[list[TaskGroupInfo]] = None
    tasks: Optional[list[RenderedTask]] = None
    # Present only when status == "error":
    error: Optional[RenderError] = None


class RenderedDagBag(_Model):
    schema_version: Literal[1]
    commit_sha: str
    airflow_version: str
    rendered_at: datetime
    dags: list[RenderedDag]
```

- [ ] **Step 1.7: Run tests, confirm all pass**

Run: `uv run pytest tests/unit/test_schema.py -v`
Expected: all tests pass (9 total).

- [ ] **Step 1.8: Add diff document types — tests first**

Append to `tests/unit/test_schema.py`:

```python
from airflow_diff.schema import (
    FieldDiff, EdgeDiff, TaskDiff, AttrDiff, DagDiff, DiffSummary, DiffDocument,
)


def test_field_diff_modified():
    fd = FieldDiff(
        name="bash_command",
        change_type="modified",
        before="echo a",
        after="echo b",
        provenance_before=[ProvenanceEntry(source="literal")],
        provenance_after=[ProvenanceEntry(source="literal")],
    )
    assert FieldDiff.model_validate_json(fd.model_dump_json()) == fd


def test_diff_document_round_trip():
    doc = DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha="abc",
        head_sha="def",
        summary=DiffSummary(),
        dags=[],
        render_errors=[],
    )
    assert DiffDocument.model_validate_json(doc.model_dump_json()) == doc
```

- [ ] **Step 1.9: Add the diff types to `schema.py`**

```python
# ----- Diff document ------------------------------------------------------

ChangeType = Literal["added", "removed", "modified", "unchanged"]
DagClassification = Literal["touched", "incidentally_affected", "added", "removed"]
DagPairStatus = Literal["ok", "regressed", "fixed", "still_broken"]


class FieldDiff(_Model):
    name: str
    change_type: ChangeType
    before: Any = None
    after: Any = None
    provenance_before: list[ProvenanceEntry] = Field(default_factory=list)
    provenance_after: list[ProvenanceEntry] = Field(default_factory=list)
    render_error_before: Optional[RenderError] = None
    render_error_after: Optional[RenderError] = None


class EdgeDiff(_Model):
    direction: Literal["upstream", "downstream"]
    change_type: Literal["added", "removed"]
    task_id: str
    related_task_id: str  # the other end of the edge


class AttrDiff(_Model):
    name: str
    before: Any = None
    after: Any = None


class TaskDiff(_Model):
    task_id: str
    change_type: ChangeType
    operator_before: Optional[str] = None
    operator_after: Optional[str] = None
    field_diffs: list[FieldDiff] = Field(default_factory=list)
    edge_diffs: list[EdgeDiff] = Field(default_factory=list)


class DagDiff(_Model):
    dag_id: str
    classification: DagClassification
    status_a: Optional[DagStatus] = None
    status_b: Optional[DagStatus] = None
    pair_status: DagPairStatus = "ok"
    source_file_before: Optional[str] = None
    source_file_after: Optional[str] = None
    attr_diffs: list[AttrDiff] = Field(default_factory=list)
    task_diffs: list[TaskDiff] = Field(default_factory=list)
    error_before: Optional[RenderError] = None
    error_after: Optional[RenderError] = None


class DiffSummary(_Model):
    dags_touched: int = 0
    dags_incidentally_affected: int = 0
    dags_added: int = 0
    dags_removed: int = 0
    dags_regressed: int = 0
    dags_fixed: int = 0


class RenderErrorEntry(_Model):
    dag_id: str
    side: Literal["base", "head", "both"]
    error_base: Optional[RenderError] = None
    error_head: Optional[RenderError] = None


class DiffDocument(_Model):
    schema_version: Literal[1]
    base_sha: str
    head_sha: str
    summary: DiffSummary
    dags: list[DagDiff]
    render_errors: list[RenderErrorEntry] = Field(default_factory=list)
```

- [ ] **Step 1.10: Run all schema tests**

Run: `uv run pytest tests/unit/test_schema.py -v`
Expected: all tests pass (11 total).

- [ ] **Step 1.11: Commit**

```bash
git add src/airflow_diff/schema.py tests/unit/test_schema.py
git commit -m "feat(schema): canonical Pydantic models for renderer and diff outputs"
```

---

## Phase 2 — Diff engine

### Task 2: Empty / identical bags

**Files:**
- Create: `src/airflow_diff/diff.py`
- Create: `tests/unit/test_diff.py`

- [ ] **Step 2.1: Write failing test — identical bags produce empty diff**

`tests/unit/test_diff.py`:

```python
from datetime import datetime, timezone

from airflow_diff.diff import compute_diff
from airflow_diff.schema import RenderedDagBag, DiffDocument, SCHEMA_VERSION


def _bag(sha: str, dags=()) -> RenderedDagBag:
    return RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha=sha,
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, tzinfo=timezone.utc),
        dags=list(dags),
    )


def test_two_empty_bags_produce_empty_diff():
    diff = compute_diff(_bag("a"), _bag("b"), touched_files=[])
    assert isinstance(diff, DiffDocument)
    assert diff.base_sha == "a"
    assert diff.head_sha == "b"
    assert diff.dags == []
    assert diff.render_errors == []
    assert diff.summary.dags_touched == 0
```

- [ ] **Step 2.2: Run, confirm failure**

Run: `uv run pytest tests/unit/test_diff.py -v`
Expected: ImportError.

- [ ] **Step 2.3: Minimal `diff.py`**

`src/airflow_diff/diff.py`:

```python
"""Pure-function diff engine.

Consumes two RenderedDagBag instances and produces a DiffDocument that describes
all structural, attribute, and field-level differences. Knows nothing about
Airflow internals — operates purely on the canonical schema.
"""
from __future__ import annotations

from airflow_diff.schema import (
    DiffDocument, DiffSummary, RenderedDagBag, SCHEMA_VERSION,
)


def compute_diff(
    base: RenderedDagBag,
    head: RenderedDagBag,
    touched_files: list[str],
) -> DiffDocument:
    return DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha=base.commit_sha,
        head_sha=head.commit_sha,
        summary=DiffSummary(),
        dags=[],
        render_errors=[],
    )
```

- [ ] **Step 2.4: Pass — run tests**

Run: `uv run pytest tests/unit/test_diff.py -v`
Expected: pass.

- [ ] **Step 2.5: Commit**

```bash
git add src/airflow_diff/diff.py tests/unit/test_diff.py
git commit -m "feat(diff): scaffold pure-function diff engine"
```

### Task 3: DAG membership (added / removed / compare)

- [ ] **Step 3.1: Tests for added / removed DAGs**

Append to `tests/unit/test_diff.py`:

```python
from airflow_diff.schema import RenderedDag


def _ok_dag(dag_id: str, source: str = None) -> RenderedDag:
    return RenderedDag(
        dag_id=dag_id, status="ok",
        source_file=source or f"dags/{dag_id}.py",
        attrs={}, datasets={"inlets": [], "outlets": []},
        task_groups=[], tasks=[],
    )


def test_dag_added_in_head():
    diff = compute_diff(_bag("a"), _bag("b", [_ok_dag("new_dag")]), touched_files=["dags/new_dag.py"])
    assert len(diff.dags) == 1
    d = diff.dags[0]
    assert d.dag_id == "new_dag"
    assert d.classification == "added"
    assert d.status_b == "ok"
    assert d.source_file_after == "dags/new_dag.py"
    assert diff.summary.dags_added == 1


def test_dag_removed_in_head():
    diff = compute_diff(_bag("a", [_ok_dag("gone")]), _bag("b"), touched_files=["dags/gone.py"])
    assert len(diff.dags) == 1
    d = diff.dags[0]
    assert d.dag_id == "gone"
    assert d.classification == "removed"
    assert d.status_a == "ok"
    assert d.source_file_before == "dags/gone.py"
    assert diff.summary.dags_removed == 1


def test_dag_unchanged_not_present_in_diff():
    a = _bag("a", [_ok_dag("same")])
    b = _bag("b", [_ok_dag("same")])
    diff = compute_diff(a, b, touched_files=[])
    assert diff.dags == []
```

- [ ] **Step 3.2: Run, confirm failures**

Run: `uv run pytest tests/unit/test_diff.py -v`
Expected: 3 failures.

- [ ] **Step 3.3: Implement DAG membership classification**

Replace `compute_diff` and add helpers in `src/airflow_diff/diff.py`:

```python
from airflow_diff.schema import (
    AttrDiff, DagDiff, DiffDocument, DiffSummary, RenderedDag, RenderedDagBag,
    RenderErrorEntry, SCHEMA_VERSION, TaskDiff,
)


def compute_diff(
    base: RenderedDagBag,
    head: RenderedDagBag,
    touched_files: list[str],
) -> DiffDocument:
    base_by_id = {d.dag_id: d for d in base.dags}
    head_by_id = {d.dag_id: d for d in head.dags}
    touched_set = set(touched_files)

    dag_diffs: list[DagDiff] = []
    render_errors: list[RenderErrorEntry] = []

    # Added: in head only
    for dag_id in sorted(head_by_id.keys() - base_by_id.keys()):
        h = head_by_id[dag_id]
        dag_diffs.append(DagDiff(
            dag_id=dag_id,
            classification="added",
            status_a=None,
            status_b=h.status,
            source_file_after=h.source_file,
            error_after=h.error,
        ))

    # Removed: in base only
    for dag_id in sorted(base_by_id.keys() - head_by_id.keys()):
        b = base_by_id[dag_id]
        dag_diffs.append(DagDiff(
            dag_id=dag_id,
            classification="removed",
            status_a=b.status,
            status_b=None,
            source_file_before=b.source_file,
            error_before=b.error,
        ))

    # Compare: in both
    for dag_id in sorted(base_by_id.keys() & head_by_id.keys()):
        b = base_by_id[dag_id]
        h = head_by_id[dag_id]
        dd = _compare_dag(b, h, touched_set)
        if dd is not None:
            dag_diffs.append(dd)

    summary = _summarize(dag_diffs)
    return DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha=base.commit_sha,
        head_sha=head.commit_sha,
        summary=summary,
        dags=dag_diffs,
        render_errors=render_errors,
    )


def _compare_dag(base: RenderedDag, head: RenderedDag, touched: set[str]) -> DagDiff | None:
    """Returns None if the two DAGs are byte-equivalent and not worth surfacing."""
    if base == head:
        return None
    classification = "touched" if (
        base.source_file in touched or head.source_file in touched
    ) else "incidentally_affected"
    return DagDiff(
        dag_id=base.dag_id,
        classification=classification,
        status_a=base.status, status_b=head.status,
        source_file_before=base.source_file,
        source_file_after=head.source_file,
        attr_diffs=[], task_diffs=[],
        error_before=base.error, error_after=head.error,
    )


def _summarize(dag_diffs: list[DagDiff]) -> DiffSummary:
    s = DiffSummary()
    for d in dag_diffs:
        if d.classification == "touched":
            s.dags_touched += 1
        elif d.classification == "incidentally_affected":
            s.dags_incidentally_affected += 1
        elif d.classification == "added":
            s.dags_added += 1
        elif d.classification == "removed":
            s.dags_removed += 1
        if d.pair_status == "regressed":
            s.dags_regressed += 1
        elif d.pair_status == "fixed":
            s.dags_fixed += 1
    return s
```

- [ ] **Step 3.4: Run, confirm all pass**

Run: `uv run pytest tests/unit/test_diff.py -v`
Expected: 4 passes.

- [ ] **Step 3.5: Commit**

```bash
git add src/airflow_diff/diff.py tests/unit/test_diff.py
git commit -m "feat(diff): DAG-bag membership classification (added/removed/compare)"
```

### Task 4: Status transitions (ok / error matrix)

- [ ] **Step 4.1: Tests for status pairs**

Append to `tests/unit/test_diff.py`:

```python
from airflow_diff.schema import RenderError


def _err_dag(dag_id: str) -> RenderedDag:
    return RenderedDag(
        dag_id=dag_id, status="error", source_file=f"dags/{dag_id}.py",
        error=RenderError(type="ImportError", message="boom", traceback="..."),
    )


def test_dag_regressed_ok_then_error():
    diff = compute_diff(_bag("a", [_ok_dag("d")]), _bag("b", [_err_dag("d")]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "regressed"
    assert d.status_a == "ok" and d.status_b == "error"
    assert diff.summary.dags_regressed == 1


def test_dag_fixed_error_then_ok():
    diff = compute_diff(_bag("a", [_err_dag("d")]), _bag("b", [_ok_dag("d")]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "fixed"
    assert diff.summary.dags_fixed == 1


def test_dag_still_broken_error_both_sides():
    a = _err_dag("d")
    b = _err_dag("d")
    # Slight diff in message so they're not equal:
    b.error = RenderError(type="ImportError", message="boom v2", traceback="...")
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "still_broken"
    assert diff.summary.dags_regressed == 0 and diff.summary.dags_fixed == 0
```

- [ ] **Step 4.2: Run, confirm failures (pair_status defaults to "ok")**

- [ ] **Step 4.3: Add `_pair_status` and wire into `_compare_dag`**

Replace `_compare_dag` in `diff.py`:

```python
def _compare_dag(base: RenderedDag, head: RenderedDag, touched: set[str]) -> DagDiff | None:
    if base == head:
        return None
    classification = "touched" if (
        base.source_file in touched or head.source_file in touched
    ) else "incidentally_affected"
    pair_status = _pair_status(base.status, head.status)
    return DagDiff(
        dag_id=base.dag_id,
        classification=classification,
        status_a=base.status, status_b=head.status,
        pair_status=pair_status,
        source_file_before=base.source_file,
        source_file_after=head.source_file,
        attr_diffs=[], task_diffs=[],
        error_before=base.error, error_after=head.error,
    )


def _pair_status(a: str, b: str) -> str:
    if a == "ok" and b == "ok":
        return "ok"
    if a == "ok" and b == "error":
        return "regressed"
    if a == "error" and b == "ok":
        return "fixed"
    return "still_broken"
```

- [ ] **Step 4.4: Run, confirm pass**

- [ ] **Step 4.5: Commit**

```bash
git add src/airflow_diff/diff.py tests/unit/test_diff.py
git commit -m "feat(diff): pair-status classification (ok/regressed/fixed/still_broken)"
```

### Task 5: Attr, task, field, and edge diffs

- [ ] **Step 5.1: Tests for attr diffs**

Append to `tests/unit/test_diff.py`:

```python
def test_attr_diff_schedule_changed():
    a = _ok_dag("d")
    a.attrs = {"schedule": "0 5 * * *", "catchup": False}
    b = _ok_dag("d")
    b.attrs = {"schedule": "0 6 * * *", "catchup": False}
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=["dags/d.py"])
    [dd] = diff.dags
    assert len(dd.attr_diffs) == 1
    assert dd.attr_diffs[0].name == "schedule"
    assert dd.attr_diffs[0].before == "0 5 * * *"
    assert dd.attr_diffs[0].after == "0 6 * * *"


def test_attr_added_and_removed():
    a = _ok_dag("d"); a.attrs = {"tags": ["x"]}
    b = _ok_dag("d"); b.attrs = {"tags": ["x"], "description": "new"}
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[])
    [dd] = diff.dags
    names = {ad.name for ad in dd.attr_diffs}
    assert "description" in names
```

- [ ] **Step 5.2: Tests for task added / removed / modified**

```python
from airflow_diff.schema import RenderedTask, RenderedField, ProvenanceEntry


def _task(task_id, *, bash: str = "echo x", upstream=(), downstream=()) -> RenderedTask:
    return RenderedTask(
        task_id=task_id,
        operator="airflow.operators.bash.BashOperator",
        upstream=list(upstream),
        downstream=list(downstream),
        fields={"bash_command": RenderedField(
            rendered=bash, provenance=[ProvenanceEntry(source="literal")],
        )},
    )


def test_task_added():
    a = _ok_dag("d"); a.tasks = [_task("t1")]
    b = _ok_dag("d"); b.tasks = [_task("t1"), _task("t2")]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    by_id = {td.task_id: td for td in dd.task_diffs}
    assert by_id["t2"].change_type == "added"
    assert "t1" not in by_id


def test_task_removed():
    a = _ok_dag("d"); a.tasks = [_task("t1"), _task("t2")]
    b = _ok_dag("d"); b.tasks = [_task("t1")]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    by_id = {td.task_id: td for td in dd.task_diffs}
    assert by_id["t2"].change_type == "removed"


def test_task_field_modified():
    a = _ok_dag("d"); a.tasks = [_task("t1", bash="echo old")]
    b = _ok_dag("d"); b.tasks = [_task("t1", bash="echo new")]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    [td] = dd.task_diffs
    assert td.change_type == "modified"
    [fd] = td.field_diffs
    assert fd.name == "bash_command"
    assert fd.before == "echo old"
    assert fd.after == "echo new"


def test_task_operator_class_changed():
    a = _ok_dag("d"); a.tasks = [_task("t1")]
    b = _ok_dag("d"); b.tasks = [_task("t1")]
    b.tasks[0].operator = "airflow.operators.python.PythonOperator"
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    [td] = dd.task_diffs
    assert td.change_type == "modified"
    assert td.operator_before == "airflow.operators.bash.BashOperator"
    assert td.operator_after == "airflow.operators.python.PythonOperator"


def test_edge_diff_upstream_added():
    a = _ok_dag("d"); a.tasks = [_task("t1"), _task("t2")]
    b = _ok_dag("d"); b.tasks = [_task("t1", downstream=["t2"]), _task("t2", upstream=["t1"])]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    # The two tasks both changed (edges added):
    by_id = {td.task_id: td for td in dd.task_diffs}
    # t2 has an upstream edge added pointing at t1:
    t2_edges = by_id["t2"].edge_diffs
    assert any(e.direction == "upstream" and e.change_type == "added"
               and e.related_task_id == "t1" for e in t2_edges)
```

- [ ] **Step 5.3: Run, confirm all fail**

- [ ] **Step 5.4: Implement attr / task / field / edge diffing**

Extend `diff.py` — add these helpers and wire them into `_compare_dag`:

```python
from airflow_diff.schema import EdgeDiff, FieldDiff, RenderedField, RenderedTask


def _compare_dag(base: RenderedDag, head: RenderedDag, touched: set[str]) -> DagDiff | None:
    if base == head:
        return None
    classification = "touched" if (
        base.source_file in touched or head.source_file in touched
    ) else "incidentally_affected"
    pair_status = _pair_status(base.status, head.status)

    attr_diffs: list[AttrDiff] = []
    task_diffs: list[TaskDiff] = []

    # Only attempt structural diff when both sides rendered ok:
    if base.status == "ok" and head.status == "ok":
        attr_diffs = _diff_attrs(base.attrs or {}, head.attrs or {})
        task_diffs = _diff_tasks(base.tasks or [], head.tasks or [])

    return DagDiff(
        dag_id=base.dag_id,
        classification=classification,
        status_a=base.status, status_b=head.status,
        pair_status=pair_status,
        source_file_before=base.source_file,
        source_file_after=head.source_file,
        attr_diffs=attr_diffs,
        task_diffs=task_diffs,
        error_before=base.error, error_after=head.error,
    )


def _diff_attrs(a: dict, b: dict) -> list[AttrDiff]:
    out: list[AttrDiff] = []
    for name in sorted(set(a) | set(b)):
        if a.get(name) != b.get(name):
            out.append(AttrDiff(name=name, before=a.get(name), after=b.get(name)))
    return out


def _diff_tasks(base: list[RenderedTask], head: list[RenderedTask]) -> list[TaskDiff]:
    by_id_a = {t.task_id: t for t in base}
    by_id_b = {t.task_id: t for t in head}
    diffs: list[TaskDiff] = []

    for tid in sorted(by_id_b.keys() - by_id_a.keys()):
        diffs.append(TaskDiff(
            task_id=tid, change_type="added",
            operator_after=by_id_b[tid].operator,
        ))
    for tid in sorted(by_id_a.keys() - by_id_b.keys()):
        diffs.append(TaskDiff(
            task_id=tid, change_type="removed",
            operator_before=by_id_a[tid].operator,
        ))
    for tid in sorted(by_id_a.keys() & by_id_b.keys()):
        td = _diff_one_task(by_id_a[tid], by_id_b[tid])
        if td is not None:
            diffs.append(td)
    return diffs


def _diff_one_task(a: RenderedTask, b: RenderedTask) -> TaskDiff | None:
    if a == b:
        return None
    field_diffs = _diff_fields(a.fields, b.fields)
    edge_diffs = _diff_edges(a, b)
    operator_changed = a.operator != b.operator
    return TaskDiff(
        task_id=a.task_id,
        change_type="modified",
        operator_before=a.operator if operator_changed else None,
        operator_after=b.operator if operator_changed else None,
        field_diffs=field_diffs,
        edge_diffs=edge_diffs,
    )


def _diff_fields(a: dict[str, RenderedField], b: dict[str, RenderedField]) -> list[FieldDiff]:
    out: list[FieldDiff] = []
    for name in sorted(set(a) | set(b)):
        fa = a.get(name)
        fb = b.get(name)
        if fa is None:
            out.append(FieldDiff(
                name=name, change_type="added",
                after=fb.rendered, provenance_after=fb.provenance,
            ))
        elif fb is None:
            out.append(FieldDiff(
                name=name, change_type="removed",
                before=fa.rendered, provenance_before=fa.provenance,
            ))
        elif fa != fb:
            out.append(FieldDiff(
                name=name, change_type="modified",
                before=fa.rendered, after=fb.rendered,
                provenance_before=fa.provenance, provenance_after=fb.provenance,
            ))
    return out


def _diff_edges(a: RenderedTask, b: RenderedTask) -> list[EdgeDiff]:
    edges: list[EdgeDiff] = []
    for direction in ("upstream", "downstream"):
        before = set(getattr(a, direction))
        after = set(getattr(b, direction))
        for related in sorted(after - before):
            edges.append(EdgeDiff(
                direction=direction, change_type="added",
                task_id=a.task_id, related_task_id=related,
            ))
        for related in sorted(before - after):
            edges.append(EdgeDiff(
                direction=direction, change_type="removed",
                task_id=a.task_id, related_task_id=related,
            ))
    return edges
```

- [ ] **Step 5.5: Run, confirm all pass**

Run: `uv run pytest tests/unit/test_diff.py -v`
Expected: all tests pass (~13 total).

- [ ] **Step 5.6: Commit**

```bash
git add src/airflow_diff/diff.py tests/unit/test_diff.py
git commit -m "feat(diff): per-task/field/edge/attr diff with operator-class change detection"
```

---

## Phase 3 — Markdown presenter

### Task 6: Skeleton + snapshot harness

**Files:**
- Create: `src/airflow_diff/present/markdown.py`
- Create: `tests/unit/present/test_markdown.py`
- Create: `tests/fixtures/diff_documents/empty.json`

- [ ] **Step 6.1: Build a minimal `DiffDocument` JSON fixture**

`tests/fixtures/diff_documents/empty.json`:

```json
{
  "schema_version": 1,
  "base_sha": "abc1234",
  "head_sha": "def5678",
  "summary": {"dags_touched": 0, "dags_incidentally_affected": 0, "dags_added": 0, "dags_removed": 0, "dags_regressed": 0, "dags_fixed": 0},
  "dags": [],
  "render_errors": []
}
```

- [ ] **Step 6.2: Failing snapshot test for the empty case**

`tests/unit/present/test_markdown.py`:

```python
import json
from pathlib import Path

from airflow_diff.present.markdown import render_markdown
from airflow_diff.schema import DiffDocument

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def _load(name: str) -> DiffDocument:
    return DiffDocument.model_validate_json((FIXTURES / name).read_text())


def test_empty_diff_renders(snapshot):
    doc = _load("empty.json")
    output = render_markdown(doc)
    assert output == snapshot
```

- [ ] **Step 6.3: Run, confirm import failure**

Run: `uv run pytest tests/unit/present/test_markdown.py -v`
Expected: ImportError.

- [ ] **Step 6.4: Minimal `markdown.py`**

`src/airflow_diff/present/markdown.py`:

```python
"""GitHub-flavored markdown presenter for DiffDocument.

Output is a single string suitable for posting as a PR comment. GitHub renders
Mermaid blocks natively, so the diff graph ships as plain markdown.
"""
from __future__ import annotations

from airflow_diff.schema import DiffDocument


def render_markdown(doc: DiffDocument) -> str:
    if not doc.dags and not doc.render_errors:
        return "## airflow-diff\n\nNo DAG differences detected.\n"
    return _render_full(doc)


def _render_full(doc: DiffDocument) -> str:
    # Filled in across subsequent tasks
    raise NotImplementedError
```

- [ ] **Step 6.5: Create snapshot — run with `--snapshot-update`**

Run: `uv run pytest tests/unit/present/test_markdown.py --snapshot-update -v`
Expected: snapshot created, test passes.

Verify created snapshot file `tests/unit/present/__snapshots__/test_markdown.ambr` exists and contains the "No DAG differences detected" string.

- [ ] **Step 6.6: Re-run without update flag**

Run: `uv run pytest tests/unit/present/test_markdown.py -v`
Expected: pass (snapshot matches).

- [ ] **Step 6.7: Commit**

```bash
git add src/airflow_diff/present/markdown.py tests/unit/present/test_markdown.py \
        tests/fixtures/diff_documents/empty.json tests/unit/present/__snapshots__/
git commit -m "feat(present): markdown presenter skeleton with snapshot harness"
```

### Task 7: Header, warning banner, summary table

- [ ] **Step 7.1: Fixture with one touched DAG, one task modified, no errors**

`tests/fixtures/diff_documents/single_dag_one_change.json`:

```json
{
  "schema_version": 1,
  "base_sha": "abc1234",
  "head_sha": "def5678",
  "summary": {"dags_touched": 1, "dags_incidentally_affected": 0, "dags_added": 0, "dags_removed": 0, "dags_regressed": 0, "dags_fixed": 0},
  "dags": [
    {
      "dag_id": "my_dag",
      "classification": "touched",
      "status_a": "ok",
      "status_b": "ok",
      "pair_status": "ok",
      "source_file_before": "dags/my_dag.py",
      "source_file_after": "dags/my_dag.py",
      "attr_diffs": [],
      "task_diffs": [
        {
          "task_id": "extract_data",
          "change_type": "modified",
          "field_diffs": [
            {
              "name": "bash_command",
              "change_type": "modified",
              "before": "aws s3 cp s3://<VAR:bucket>/2025-01-01 /tmp/in",
              "after":  "aws s3 cp s3://<VAR:bucket_v2>/2025-01-01 /tmp/in",
              "provenance_before": [{"source": "stub", "key": "var.value.bucket"}],
              "provenance_after":  [{"source": "stub", "key": "var.value.bucket_v2"}]
            }
          ],
          "edge_diffs": []
        }
      ]
    }
  ],
  "render_errors": []
}
```

- [ ] **Step 7.2: Add a snapshot test for this fixture**

Append to `tests/unit/present/test_markdown.py`:

```python
def test_single_dag_one_change(snapshot):
    output = render_markdown(_load("single_dag_one_change.json"))
    assert output == snapshot
```

- [ ] **Step 7.3: Run, confirm failure (`NotImplementedError`)**

- [ ] **Step 7.4: Implement header, warning banner, and per-DAG section + table**

Replace body of `present/markdown.py`:

```python
from __future__ import annotations

from airflow_diff.schema import DagDiff, DiffDocument, FieldDiff, TaskDiff


def render_markdown(doc: DiffDocument) -> str:
    if not doc.dags and not doc.render_errors:
        return "## airflow-diff\n\nNo DAG differences detected.\n"

    parts: list[str] = []
    parts.append(_header(doc))
    if doc.render_errors or any(d.pair_status != "ok" for d in doc.dags):
        parts.append(_warning_banner(doc))

    touched = [d for d in doc.dags if d.classification == "touched"]
    added = [d for d in doc.dags if d.classification == "added"]
    removed = [d for d in doc.dags if d.classification == "removed"]
    incidental = [d for d in doc.dags if d.classification == "incidentally_affected"]

    for d in touched + added + removed:
        parts.append(_render_dag_section(d, collapsed=False))
    if incidental:
        parts.append("<details><summary>DAGs incidentally affected (not touched by this PR)</summary>\n")
        for d in incidental:
            parts.append(_render_dag_section(d, collapsed=False))
        parts.append("\n</details>\n")
    return "\n".join(parts) + "\n"


def _header(doc: DiffDocument) -> str:
    s = doc.summary
    total = s.dags_touched + s.dags_incidentally_affected + s.dags_added + s.dags_removed
    line = f"## airflow-diff: {total} DAG{'s' if total != 1 else ''} changed"
    bits = []
    if s.dags_added: bits.append(f"{s.dags_added} added")
    if s.dags_removed: bits.append(f"{s.dags_removed} removed")
    if s.dags_regressed: bits.append(f"**{s.dags_regressed} regressed**")
    if s.dags_fixed: bits.append(f"{s.dags_fixed} fixed")
    if s.dags_incidentally_affected:
        bits.append(f"{s.dags_incidentally_affected} incidentally affected")
    suffix = f" ({', '.join(bits)})" if bits else ""
    return f"{line}{suffix}\n\nBase: `{doc.base_sha[:8]}` → Head: `{doc.head_sha[:8]}`"


def _warning_banner(doc: DiffDocument) -> str:
    msgs = []
    regressed = [d.dag_id for d in doc.dags if d.pair_status == "regressed"]
    if regressed:
        msgs.append(f"⚠️ **Regressions introduced by this PR:** {', '.join(f'`{i}`' for i in regressed)}")
    if doc.render_errors:
        ids = ", ".join(f"`{e.dag_id}`" for e in doc.render_errors)
        msgs.append(f"⚠️ **Render errors:** {ids}")
    return "\n".join(msgs) + "\n"


def _render_dag_section(d: DagDiff, *, collapsed: bool) -> str:
    parts: list[str] = []
    title = f"### `{d.dag_id}` — {_dag_change_summary(d)}"
    parts.append(title)
    table = _summary_table(d)
    if table:
        parts.append(table)
    for td in d.task_diffs:
        parts.extend(_render_task_details(td, d.dag_id))
    return "\n".join(parts)


def _dag_change_summary(d: DagDiff) -> str:
    if d.classification == "added": return "new DAG"
    if d.classification == "removed": return "removed"
    n_tasks_added = sum(1 for t in d.task_diffs if t.change_type == "added")
    n_tasks_removed = sum(1 for t in d.task_diffs if t.change_type == "removed")
    n_tasks_modified = sum(1 for t in d.task_diffs if t.change_type == "modified")
    bits = []
    if d.attr_diffs: bits.append(f"{len(d.attr_diffs)} attr change{'s' if len(d.attr_diffs) != 1 else ''}")
    if n_tasks_added: bits.append(f"{n_tasks_added} task{'s' if n_tasks_added != 1 else ''} added")
    if n_tasks_modified: bits.append(f"{n_tasks_modified} task{'s' if n_tasks_modified != 1 else ''} modified")
    if n_tasks_removed: bits.append(f"{n_tasks_removed} task{'s' if n_tasks_removed != 1 else ''} removed")
    return ", ".join(bits) or "no structural change"


def _summary_table(d: DagDiff) -> str:
    rows: list[str] = []
    for ad in d.attr_diffs:
        rows.append(f"| _(DAG-level)_ | `{ad.name}` | modified |")
    for td in d.task_diffs:
        if td.change_type == "added":
            rows.append(f"| `{td.task_id}` | _(whole task)_ | added |")
        elif td.change_type == "removed":
            rows.append(f"| `{td.task_id}` | _(whole task)_ | removed |")
        else:
            for fd in td.field_diffs:
                rows.append(f"| `{td.task_id}` | `{fd.name}` | {fd.change_type} |")
            for ed in td.edge_diffs:
                rows.append(
                    f"| `{td.task_id}` → `{ed.related_task_id}` "
                    f"| _({ed.direction} edge)_ | {ed.change_type} |"
                )
    if not rows:
        return ""
    return "| Task | Field | Change |\n|------|-------|--------|\n" + "\n".join(rows)


def _render_task_details(td: TaskDiff, dag_id: str) -> list[str]:
    out: list[str] = []
    for fd in td.field_diffs:
        if fd.change_type == "modified":
            out.append(_collapsible_field_diff(dag_id, td.task_id, fd))
    return out


def _collapsible_field_diff(dag_id: str, task_id: str, fd: FieldDiff) -> str:
    return (
        f"<details><summary>{dag_id}.{task_id}.{fd.name}</summary>\n\n"
        f"```diff\n"
        f"- {fd.before}\n"
        f"+ {fd.after}\n"
        f"```\n\n</details>"
    )
```

- [ ] **Step 7.5: Update snapshot and verify**

Run: `uv run pytest tests/unit/present/test_markdown.py --snapshot-update -v`
Then: `uv run pytest tests/unit/present/test_markdown.py -v`
Expected: pass.

Manually inspect `tests/unit/present/__snapshots__/test_markdown.ambr` to verify the output looks correct — should contain the summary header, the table with one row, and the collapsible `<details>` block.

- [ ] **Step 7.6: Commit**

```bash
git add src/airflow_diff/present/markdown.py tests/unit/present/test_markdown.py \
        tests/fixtures/diff_documents/single_dag_one_change.json \
        tests/unit/present/__snapshots__/
git commit -m "feat(present): markdown header, warning banner, summary table, per-field details"
```

### Task 8: Mermaid combined-diff graph

- [ ] **Step 8.1: Replace the fixture with the full Mermaid-exercising version**

Overwrite `tests/fixtures/diff_documents/single_dag_one_change.json`:

```json
{
  "schema_version": 1,
  "base_sha": "abc1234",
  "head_sha": "def5678",
  "summary": {"dags_touched": 1, "dags_incidentally_affected": 0, "dags_added": 0, "dags_removed": 0, "dags_regressed": 0, "dags_fixed": 0},
  "dags": [
    {
      "dag_id": "my_dag",
      "classification": "touched",
      "status_a": "ok",
      "status_b": "ok",
      "pair_status": "ok",
      "source_file_before": "dags/my_dag.py",
      "source_file_after": "dags/my_dag.py",
      "attr_diffs": [],
      "task_diffs": [
        {
          "task_id": "extract_data",
          "change_type": "modified",
          "field_diffs": [
            {
              "name": "bash_command",
              "change_type": "modified",
              "before": "aws s3 cp s3://<VAR:bucket>/2025-01-01 /tmp/in",
              "after":  "aws s3 cp s3://<VAR:bucket_v2>/2025-01-01 /tmp/in",
              "provenance_before": [{"source": "stub", "key": "var.value.bucket"}],
              "provenance_after":  [{"source": "stub", "key": "var.value.bucket_v2"}]
            }
          ],
          "edge_diffs": [
            {"direction": "downstream", "change_type": "removed", "task_id": "extract_data", "related_task_id": "transform"},
            {"direction": "downstream", "change_type": "added",   "task_id": "extract_data", "related_task_id": "validate_data"}
          ]
        },
        {
          "task_id": "validate_data",
          "change_type": "added",
          "operator_after": "airflow.operators.python.PythonOperator",
          "field_diffs": [],
          "edge_diffs": []
        }
      ]
    }
  ],
  "render_errors": []
}
```

This fixture covers: a modified templated field (with stub provenance change), a newly added task, and two edge changes (one removed, one added). It exercises the full Mermaid path including the `linkStyle` entries for added/removed edges.

- [ ] **Step 8.2: Add a snapshot test specifically for the Mermaid block**

Append to `tests/unit/present/test_markdown.py`:

```python
def test_mermaid_block_in_output(snapshot):
    output = render_markdown(_load("single_dag_one_change.json"))
    # Snapshot the whole output (re-records the fixture above):
    assert output == snapshot
    # Sanity assertions that don't rely on the full snapshot:
    assert "```mermaid" in output
    assert "classDef added" in output
    assert "validate_data" in output
```

- [ ] **Step 8.3: Run, confirm snapshot needs regeneration AND mermaid assertions fail**

- [ ] **Step 8.4: Implement Mermaid generation**

Add to `present/markdown.py`:

```python
MAX_TASKS_FOR_GRAPH = 50  # honored from config in orchestrator; constant here for unit-test simplicity


def _render_dag_section(d: DagDiff, *, collapsed: bool) -> str:
    parts: list[str] = []
    parts.append(f"### `{d.dag_id}` — {_dag_change_summary(d)}")
    graph = _render_mermaid(d)
    if graph:
        parts.append(graph)
    table = _summary_table(d)
    if table:
        parts.append(table)
    for td in d.task_diffs:
        parts.extend(_render_task_details(td, d.dag_id))
    return "\n".join(parts)


def _render_mermaid(d: DagDiff) -> str:
    # Build the union node set: every task touched by any diff
    nodes: dict[str, str] = {}  # task_id -> css class (added/removed/modified/unchanged)
    edges: list[tuple[str, str, str]] = []  # (from, to, class)

    for td in d.task_diffs:
        css = {
            "added": "added", "removed": "removed", "modified": "modified",
        }.get(td.change_type, "unchanged")
        nodes[td.task_id] = css
        for ed in td.edge_diffs:
            other = ed.related_task_id
            nodes.setdefault(other, "unchanged")
            if ed.direction == "downstream":
                edges.append((td.task_id, other, ed.change_type))
            else:
                edges.append((other, td.task_id, ed.change_type))

    if not nodes:
        return ""
    if len(nodes) > MAX_TASKS_FOR_GRAPH:
        return _graph_summary_box(d)

    lines = ["```mermaid", "graph LR",
             "  classDef added fill:#dafbe1,stroke:#1a7f37,stroke-width:2px,color:#1a7f37",
             "  classDef removed fill:#ffebe9,stroke:#cf222e,stroke-width:2px,color:#cf222e",
             "  classDef modified fill:#fff8c5,stroke:#9a6700,stroke-width:2px,color:#9a6700",
             "  classDef unchanged fill:#f6f8fa,stroke:#656d76,color:#1f2328"]
    for tid, css in sorted(nodes.items()):
        label = tid
        if css == "added": label = f"+ {tid}"
        elif css == "modified": label = f"{tid} ✎"
        elif css == "removed": label = f"- {tid}"
        lines.append(f'  {_mermaid_id(tid)}["{label}"]:::{css}')
    link_styles: list[str] = []
    for i, (a, b, change) in enumerate(edges):
        arrow = "==>" if change == "added" else ("-.->" if change == "removed" else "-->")
        lines.append(f"  {_mermaid_id(a)} {arrow} {_mermaid_id(b)}")
        if change == "added":
            link_styles.append(f"  linkStyle {i} stroke:#1a7f37,stroke-width:2.5px")
        elif change == "removed":
            link_styles.append(f"  linkStyle {i} stroke:#cf222e,stroke-width:1.5px,stroke-dasharray:5")
    lines.extend(link_styles)
    lines.append("```")
    return "\n".join(lines)


def _mermaid_id(task_id: str) -> str:
    """Mermaid node IDs cannot contain dots or special chars. Sanitize."""
    return "n_" + "".join(c if c.isalnum() else "_" for c in task_id)


def _graph_summary_box(d: DagDiff) -> str:
    n_add = sum(1 for t in d.task_diffs if t.change_type == "added")
    n_rem = sum(1 for t in d.task_diffs if t.change_type == "removed")
    n_mod = sum(1 for t in d.task_diffs if t.change_type == "modified")
    return (
        f"> _Graph omitted — DAG has more than {MAX_TASKS_FOR_GRAPH} tasks._\n"
        f"> Tasks: **{n_add} added**, **{n_mod} modified**, **{n_rem} removed**."
    )
```

- [ ] **Step 8.5: Regenerate snapshot, verify**

Run: `uv run pytest tests/unit/present/test_markdown.py --snapshot-update -v`
Then: `uv run pytest tests/unit/present/test_markdown.py -v`
Expected: pass.

Inspect the snapshot — Mermaid block should appear before the table; should include `classDef` lines and node references for `extract_data`, `validate_data`, `transform`.

- [ ] **Step 8.6: Commit**

```bash
git add src/airflow_diff/present/markdown.py tests/unit/present/test_markdown.py \
        tests/fixtures/diff_documents/single_dag_one_change.json \
        tests/unit/present/__snapshots__/
git commit -m "feat(present): Mermaid combined-diff graph in markdown output"
```

### Task 8b: Size-limit truncation (GitHub 65,536-char PR comment cap)

Spec section 7 requires that when the markdown output exceeds GitHub's PR-comment character limit, the presenter emits a truncated comment with a marker and the full content is preserved for an HTML artifact.

**Files:**
- Modify: `src/airflow_diff/present/markdown.py`
- Modify: `tests/unit/present/test_markdown.py`

- [ ] **Step 8b.1: Failing test for truncation**

Append to `tests/unit/present/test_markdown.py`:

```python
def test_truncates_when_output_exceeds_limit(monkeypatch):
    # Build a fake DiffDocument with enough DAGs that the rendered markdown
    # exceeds the limit. Easier: monkeypatch the cap to something small.
    from airflow_diff.present import markdown as mod
    monkeypatch.setattr(mod, "GITHUB_COMMENT_CHAR_LIMIT", 200)
    output = render_markdown(_load("single_dag_one_change.json"))
    assert "Output truncated" in output
    assert len(output) <= 200 + 500  # truncation suffix can push it slightly over
```

- [ ] **Step 8b.2: Run, confirm failure**

- [ ] **Step 8b.3: Implement truncation**

Add at the top of `present/markdown.py`:

```python
GITHUB_COMMENT_CHAR_LIMIT = 65_536
```

Wrap `render_markdown`:

```python
def render_markdown(doc: DiffDocument) -> str:
    full = _render_internal(doc)
    if len(full) <= GITHUB_COMMENT_CHAR_LIMIT:
        return full
    # Truncate to ~90% of the limit, then append a footer linking to the artifact.
    cutoff = int(GITHUB_COMMENT_CHAR_LIMIT * 0.9)
    truncated = full[:cutoff]
    # Avoid cutting in the middle of a code fence or details block:
    last_safe_newline = truncated.rfind("\n\n")
    if last_safe_newline > 0:
        truncated = truncated[:last_safe_newline]
    footer = (
        "\n\n---\n\n"
        "> ⚠️ **Output truncated** — the full diff exceeds GitHub's PR-comment "
        f"character limit ({GITHUB_COMMENT_CHAR_LIMIT:,}).\n"
        "> The complete HTML report has been uploaded as a workflow artifact "
        "(see the run's Artifacts panel for `airflow-diff-report.html`).\n"
    )
    return truncated + footer


def _render_internal(doc: DiffDocument) -> str:
    # Rename the existing body of render_markdown to this function:
    if not doc.dags and not doc.render_errors:
        return "## airflow-diff\n\nNo DAG differences detected.\n"
    # ... rest of the old render_markdown body unchanged ...
```

(Mechanical refactor — rename the original `render_markdown` body to `_render_internal`, then add the wrapper above.)

- [ ] **Step 8b.4: Run, confirm pass; existing snapshots still match**

Run: `uv run pytest tests/unit/present/test_markdown.py -v`
Expected: all pass — existing snapshots unaffected (their content is well under 65k chars), new truncation test passes.

- [ ] **Step 8b.5: Commit**

```bash
git add src/airflow_diff/present/markdown.py tests/unit/present/test_markdown.py
git commit -m "feat(present): truncate markdown output above GitHub 65k char limit"
```

> **Forward dependency note:** the truncated comment footer references an HTML artifact uploaded by the Action. Task 20's `action.yml` and `entrypoint.sh` include the HTML production and `actions/upload-artifact` step that makes that link valid.

---

## Phase 4 — Config and fixtures loading

### Task 9: Config loader

**Files:**
- Create: `src/airflow_diff/config.py`
- Create: `tests/unit/test_config.py`
- Create: `tests/fixtures/config/full.toml`
- Create: `tests/fixtures/config/full_fixtures.yaml`

- [ ] **Step 9.1: Create config fixtures**

`tests/fixtures/config/full.toml`:

```toml
dags_folder = "my_dags"
plugins_folder = "my_plugins"
fixtures_path = ".airflow-diff/fix.yaml"
excluded_files = ["legacy/*.py"]
excluded_dag_ids = ["sandbox_*"]
synthetic_logical_date = "2024-06-01T00:00:00+00:00"
render_timeout_seconds = 600
max_tasks_for_graph = 100
```

`tests/fixtures/config/full_fixtures.yaml`:

```yaml
variables:
  bucket: "prod-bucket"
  region: "us-east-1"
connections:
  warehouse:
    host: "wh.example.com"
    schema: "analytics"
```

- [ ] **Step 9.2: Failing tests**

`tests/unit/test_config.py`:

```python
from datetime import datetime
from pathlib import Path

import pytest

from airflow_diff.config import Config, Fixtures, load_config, load_fixtures

FIXTURES = Path(__file__).parent.parent / "fixtures" / "config"


def test_defaults_when_no_file(tmp_path: Path):
    cfg = load_config(tmp_path)  # no .airflow-diff.toml present
    assert cfg.dags_folder == "dags"
    assert cfg.plugins_folder == "plugins"
    assert cfg.fixtures_path == ".airflow-diff/fixtures.yaml"
    assert cfg.excluded_files == []
    assert cfg.excluded_dag_ids == []
    assert cfg.render_timeout_seconds == 300
    assert cfg.max_tasks_for_graph == 50
    assert cfg.synthetic_logical_date == datetime.fromisoformat("2025-01-01T00:00:00+00:00")


def test_loads_from_toml(tmp_path: Path):
    (tmp_path / ".airflow-diff.toml").write_text((FIXTURES / "full.toml").read_text())
    cfg = load_config(tmp_path)
    assert cfg.dags_folder == "my_dags"
    assert cfg.plugins_folder == "my_plugins"
    assert cfg.excluded_files == ["legacy/*.py"]
    assert cfg.excluded_dag_ids == ["sandbox_*"]
    assert cfg.render_timeout_seconds == 600
    assert cfg.max_tasks_for_graph == 100


def test_rejects_unknown_keys(tmp_path: Path):
    (tmp_path / ".airflow-diff.toml").write_text("bogus_key = 1\n")
    with pytest.raises(ValueError, match="bogus_key"):
        load_config(tmp_path)


def test_load_fixtures_missing_returns_empty(tmp_path: Path):
    fixtures = load_fixtures(tmp_path / "missing.yaml")
    assert fixtures == Fixtures()


def test_load_fixtures_parses_yaml(tmp_path: Path):
    p = tmp_path / "fix.yaml"
    p.write_text((FIXTURES / "full_fixtures.yaml").read_text())
    fix = load_fixtures(p)
    assert fix.variables == {"bucket": "prod-bucket", "region": "us-east-1"}
    assert fix.connections["warehouse"]["host"] == "wh.example.com"


def test_load_fixtures_bad_yaml_raises(tmp_path: Path):
    p = tmp_path / "fix.yaml"
    p.write_text("variables: [not a dict]\n")
    with pytest.raises(ValueError):
        load_fixtures(p)
```

- [ ] **Step 9.3: Run, confirm failures**

- [ ] **Step 9.4: Implement `config.py`**

`src/airflow_diff/config.py`:

```python
"""Config and fixtures loaders.

`.airflow-diff.toml` lives at the repo root; `fixtures_path` points at a YAML
file (default `.airflow-diff/fixtures.yaml`). Both are optional.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dags_folder: str = "dags"
    plugins_folder: str = "plugins"
    fixtures_path: str = ".airflow-diff/fixtures.yaml"
    excluded_files: list[str] = Field(default_factory=list)
    excluded_dag_ids: list[str] = Field(default_factory=list)
    synthetic_logical_date: datetime = Field(
        default_factory=lambda: datetime.fromisoformat("2025-01-01T00:00:00+00:00")
    )
    render_timeout_seconds: int = 300
    max_tasks_for_graph: int = 50

    @field_validator("synthetic_logical_date", mode="before")
    @classmethod
    def _parse_dt(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.fromisoformat(v)
        return v


class Fixtures(BaseModel):
    model_config = ConfigDict(extra="forbid")
    variables: dict[str, Any] = Field(default_factory=dict)
    connections: dict[str, dict[str, Any]] = Field(default_factory=dict)


def load_config(repo_root: Path) -> Config:
    toml_path = repo_root / ".airflow-diff.toml"
    if not toml_path.exists():
        return Config()
    raw = tomllib.loads(toml_path.read_text())
    try:
        return Config(**raw)
    except ValidationError as e:
        # Re-raise with a clearer error pointing to the offending key
        raise ValueError(str(e)) from e


def load_fixtures(path: Path) -> Fixtures:
    if not path.exists():
        return Fixtures()
    raw = yaml.safe_load(path.read_text()) or {}
    try:
        return Fixtures(**raw)
    except ValidationError as e:
        raise ValueError(str(e)) from e
```

- [ ] **Step 9.5: Run, confirm all pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: 6 passes.

- [ ] **Step 9.6: Commit**

```bash
git add src/airflow_diff/config.py tests/unit/test_config.py tests/fixtures/config/
git commit -m "feat(config): TOML config loader + YAML fixtures loader with strict validation"
```

---

## Phase 5 — Worktree and venv managers

### Task 10: Worktree manager

**Files:**
- Create: `src/airflow_diff/worktree.py`
- Create: `tests/unit/test_worktree.py`

- [ ] **Step 10.1: Failing tests**

`tests/unit/test_worktree.py`:

```python
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_diff.worktree import resolve_sha, worktree_for, WorktreeError


def test_resolve_sha_full(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = "abc1234567890\n"; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    full = resolve_sha(tmp_path, "abc1234")
    assert full == "abc1234567890"
    assert calls[0][:3] == ["git", "-C", str(tmp_path)]


def test_resolve_sha_bad_ref(monkeypatch, tmp_path):
    def fake_run(args, **kwargs):
        class R: returncode = 1; stdout = ""; stderr = "fatal: ambiguous argument"
        return R()
    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    with pytest.raises(WorktreeError, match="resolve"):
        resolve_sha(tmp_path, "bogus")


def test_worktree_for_creates_and_yields_path(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    with worktree_for(tmp_path, "abc1234567890", root=tmp_path / "wts") as wt:
        assert wt == tmp_path / "wts" / "abc1234567890"
    # First call: worktree add; we don't remove on exit (cache).
    assert any("worktree" in a and "add" in a for a in calls)


def test_worktree_for_reuses_existing(monkeypatch, tmp_path):
    target = tmp_path / "wts" / "abc1234567890"
    target.mkdir(parents=True)
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    with worktree_for(tmp_path, "abc1234567890", root=tmp_path / "wts") as wt:
        assert wt == target
    # Nothing called because cache hit:
    assert calls == []
```

- [ ] **Step 10.2: Run, confirm import failure**

- [ ] **Step 10.3: Implement `worktree.py`**

`src/airflow_diff/worktree.py`:

```python
"""Wraps `git worktree` for isolated per-commit checkouts.

Worktrees are cached under a root dir keyed by full SHA so concurrent runs
against the same SHA share the on-disk checkout.
"""
from __future__ import annotations

import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

DEFAULT_WORKTREE_ROOT = Path("/tmp/airflow-diff/worktrees")


class WorktreeError(RuntimeError):
    pass


@dataclass
class _RunResult:
    returncode: int
    stdout: str
    stderr: str


def _run(args: list[str], **kwargs) -> _RunResult:
    res = subprocess.run(args, capture_output=True, text=True, **kwargs)
    return _RunResult(res.returncode, res.stdout, res.stderr)


def resolve_sha(repo_root: Path, ref: str) -> str:
    res = _run(["git", "-C", str(repo_root), "rev-parse", "--verify", f"{ref}^{{commit}}"])
    if res.returncode != 0:
        raise WorktreeError(f"could not resolve ref {ref!r}: {res.stderr.strip()}")
    return res.stdout.strip()


def ensure_sha_present(repo_root: Path, sha: str) -> None:
    res = _run(["git", "-C", str(repo_root), "cat-file", "-e", sha])
    if res.returncode != 0:
        raise WorktreeError(
            f"commit {sha} is not present in the repo. If running in CI, ensure "
            f"`actions/checkout` uses `fetch-depth: 0`."
        )


@contextmanager
def worktree_for(repo_root: Path, sha: str, *, root: Path = DEFAULT_WORKTREE_ROOT) -> Iterator[Path]:
    root.mkdir(parents=True, exist_ok=True)
    target = root / sha
    if not target.exists():
        res = _run(["git", "-C", str(repo_root), "worktree", "add", "--detach", str(target), sha])
        if res.returncode != 0:
            raise WorktreeError(f"git worktree add failed: {res.stderr.strip()}")
    yield target
    # Note: we intentionally do not clean up on exit. The cache amortizes across
    # subsequent runs against the same SHA. Cleanup is the user's responsibility
    # (or `git worktree prune` in CI cleanup).
```

- [ ] **Step 10.4: Run, confirm pass**

- [ ] **Step 10.5: Commit**

```bash
git add src/airflow_diff/worktree.py tests/unit/test_worktree.py
git commit -m "feat(worktree): git worktree manager with SHA-keyed caching"
```

### Task 11: Venv manager

**Files:**
- Create: `src/airflow_diff/venv.py`
- Create: `tests/unit/test_venv.py`

- [ ] **Step 11.1: Failing tests**

`tests/unit/test_venv.py`:

```python
import hashlib
from pathlib import Path

import pytest

from airflow_diff.venv import VenvError, requirements_hash, venv_for


def test_requirements_hash_uses_requirements_txt(tmp_path):
    (tmp_path / "requirements.txt").write_text("apache-airflow==2.10.3\n")
    h = requirements_hash(tmp_path)
    expected = hashlib.sha256(b"apache-airflow==2.10.3\n").hexdigest()
    assert h == expected


def test_requirements_hash_includes_pyproject(tmp_path):
    (tmp_path / "requirements.txt").write_text("a==1\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    h1 = requirements_hash(tmp_path)
    (tmp_path / "pyproject.toml").write_text("[project]\nname='y'\n")
    h2 = requirements_hash(tmp_path)
    assert h1 != h2


def test_requirements_hash_no_files_uses_marker(tmp_path):
    h = requirements_hash(tmp_path)
    assert h == hashlib.sha256(b"<no-requirements>").hexdigest()


def test_venv_for_creates_when_missing(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.venv._run", fake_run)
    (tmp_path / "requirements.txt").write_text("a==1\n")
    cache = tmp_path / "cache"

    def fake_mark_ready(p: Path) -> None:
        # Simulate uv finishing
        (p / "bin").mkdir(parents=True, exist_ok=True)
        (p / "bin" / "python").write_text("#!/bin/sh\n")
        (p / ".airflow-diff-ready").write_text("ok")
    monkeypatch.setattr("airflow_diff.venv._mark_ready_for_test", fake_mark_ready)

    py = venv_for(tmp_path, root=cache)
    assert py.name == "python"
    assert any("uv" in " ".join(a) for a in calls)


def test_venv_for_reuses_when_ready(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.venv._run", fake_run)
    (tmp_path / "requirements.txt").write_text("a==1\n")
    cache = tmp_path / "cache"
    h = requirements_hash(tmp_path)
    venv_dir = cache / h
    (venv_dir / "bin").mkdir(parents=True)
    (venv_dir / "bin" / "python").write_text("#!/bin/sh\n")
    (venv_dir / ".airflow-diff-ready").write_text("ok")
    py = venv_for(tmp_path, root=cache)
    assert py == venv_dir / "bin" / "python"
    assert calls == []  # cache hit


def test_uv_failure_raises(monkeypatch, tmp_path):
    def fake_run(args, **kwargs):
        class R: returncode = 1; stdout = ""; stderr = "pip install failed"
        return R()
    monkeypatch.setattr("airflow_diff.venv._run", fake_run)
    (tmp_path / "requirements.txt").write_text("nonexistent==1\n")
    with pytest.raises(VenvError, match="failed"):
        venv_for(tmp_path, root=tmp_path / "cache")
```

- [ ] **Step 11.2: Run, confirm failures**

- [ ] **Step 11.3: Implement `venv.py`**

`src/airflow_diff/venv.py`:

```python
"""Per-commit venv manager built on top of `uv`.

Cache key is a hash of `requirements.txt` + `pyproject.toml` + `constraints.txt`
(whichever exist). Two commits with identical dep files share a venv.
"""
from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path

DEFAULT_VENV_ROOT = Path.home() / ".cache" / "airflow-diff" / "venvs"
_DEP_FILES = ("requirements.txt", "pyproject.toml", "constraints.txt")
_READY_MARKER = ".airflow-diff-ready"


class VenvError(RuntimeError):
    pass


@dataclass
class _RunResult:
    returncode: int
    stdout: str
    stderr: str


def _run(args: list[str], **kwargs) -> _RunResult:
    res = subprocess.run(args, capture_output=True, text=True, **kwargs)
    return _RunResult(res.returncode, res.stdout, res.stderr)


def _mark_ready_for_test(venv_dir: Path) -> None:  # noqa: D401 — test hook
    """Test hook: mark the venv as ready (real path writes the marker after install)."""
    (venv_dir / _READY_MARKER).write_text("ok")


def requirements_hash(worktree_path: Path) -> str:
    h = hashlib.sha256()
    found_any = False
    for name in _DEP_FILES:
        p = worktree_path / name
        if p.exists():
            found_any = True
            h.update(p.read_bytes())
    if not found_any:
        h.update(b"<no-requirements>")
    return h.hexdigest()


def venv_for(worktree_path: Path, *, root: Path = DEFAULT_VENV_ROOT) -> Path:
    """Return the Python interpreter path of a venv built from `worktree_path`'s deps."""
    root.mkdir(parents=True, exist_ok=True)
    key = requirements_hash(worktree_path)
    venv_dir = root / key
    python = venv_dir / "bin" / "python"
    if (venv_dir / _READY_MARKER).exists() and python.exists():
        return python

    # Create the venv
    res = _run(["uv", "venv", str(venv_dir)])
    if res.returncode != 0:
        raise VenvError(f"uv venv failed: {res.stderr.strip()}")

    # Install deps (prefer requirements.txt; otherwise install the project itself)
    req = worktree_path / "requirements.txt"
    if req.exists():
        res = _run([
            "uv", "pip", "install",
            "--python", str(python),
            "-r", str(req),
        ])
    elif (worktree_path / "pyproject.toml").exists():
        res = _run([
            "uv", "pip", "install",
            "--python", str(python),
            "-e", str(worktree_path),
        ])
    else:
        # No deps to install — venv with stdlib is fine
        class R:
            returncode = 0; stdout = ""; stderr = ""
        res = R()

    if res.returncode != 0:
        raise VenvError(f"uv pip install failed: {res.stderr.strip()}")

    _mark_ready_for_test(venv_dir)  # in real runs this still just touches the marker
    return python
```

- [ ] **Step 11.4: Run, confirm pass**

Run: `uv run pytest tests/unit/test_venv.py -v`
Expected: 5 passes.

- [ ] **Step 11.5: Commit**

```bash
git add src/airflow_diff/venv.py tests/unit/test_venv.py
git commit -m "feat(venv): uv-backed per-commit venv manager with requirements-hash caching"
```

---

## Phase 6 — Renderer (the hard part)

The renderer runs inside a subprocess with the per-commit venv activated. It imports the DAG bag, installs stubs for Variables/Connections/XCom, renders templates, and emits canonical JSON.

### Task 12: Stub layer + minimal renderer entry point

**Files:**
- Create: `src/airflow_diff/renderer.py`
- Create: `tests/fixtures/dags_base/linear.py`
- Create: `tests/fixtures/dags_head/linear.py`
- Create: `tests/integration/test_renderer.py`

The renderer is too complex for pure unit tests — it's tested integration-style against curated DAG fixtures with real Airflow installed.

- [ ] **Step 12.1: Build the simplest DAG fixture (linear, no templates)**

`tests/fixtures/dags_base/linear.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="linear",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"],
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")
    middle = BashOperator(task_id="middle", bash_command="echo middle")
    end = BashOperator(task_id="end", bash_command="echo end")
    start >> middle >> end
```

`tests/fixtures/dags_head/linear.py` — copy of the above (we'll mutate it later for diff tests).

- [ ] **Step 12.2: Failing integration test**

`tests/integration/test_renderer.py`:

```python
import json
import subprocess
import sys
from pathlib import Path

import pytest

from airflow_diff.schema import RenderedDagBag

pytestmark = pytest.mark.integration

FIXTURES_ROOT = Path(__file__).parent.parent / "fixtures"


def _run_renderer(worktree: Path) -> RenderedDagBag:
    """Invoke the renderer as a subprocess with the current Python interpreter."""
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff.renderer",
         "--worktree", str(worktree),
         "--commit-sha", "test_sha",
         "--config", "{}"],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        raise AssertionError(f"renderer exit={res.returncode} stderr={res.stderr}")
    return RenderedDagBag.model_validate_json(res.stdout)


def test_renders_linear_dag(tmp_path: Path):
    # Set up worktree with our fixture
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "linear.py").write_text(
        (FIXTURES_ROOT / "dags_base" / "linear.py").read_text()
    )
    bag = _run_renderer(tmp_path)
    assert bag.commit_sha == "test_sha"
    [dag] = bag.dags
    assert dag.dag_id == "linear"
    assert dag.status == "ok"
    assert dag.attrs["schedule"] == "@daily"
    assert {t.task_id for t in dag.tasks} == {"start", "middle", "end"}
    end_task = next(t for t in dag.tasks if t.task_id == "end")
    assert end_task.upstream == ["middle"]
    assert end_task.fields["bash_command"].rendered == "echo end"
    assert end_task.fields["bash_command"].provenance[0].source == "literal"
```

- [ ] **Step 12.3: Run, confirm import / subprocess failure**

Run: `uv run pytest tests/integration/test_renderer.py -v`
Expected: failure — renderer doesn't exist yet.

- [ ] **Step 12.4: Implement the minimal renderer**

`src/airflow_diff/renderer.py`:

```python
"""DAG-bag renderer (runs inside a per-commit subprocess).

Imports the DAG bag at `--worktree`, renders all template fields against a
synthetic Jinja context with stubbed Variables/Connections/XCom, and emits a
RenderedDagBag JSON document on stdout. This is the ONLY module that imports
Airflow.

Run as: python -m airflow_diff.renderer --worktree <path> --commit-sha <sha> \\
                                        --config <json> [--fixtures <yaml>]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Stubs MUST be installed before any DAG import. We patch Airflow's Variable,
# BaseHook, and TaskInstance.xcom_pull at module level.

_FIXTURES: dict[str, Any] = {"variables": {}, "connections": {}}


def _install_stubs() -> None:
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    from airflow.models.connection import Connection

    _real_variable_get = Variable.get

    def stub_variable_get(key, default_var=None, deserialize_json=False, **kw):
        if key in _FIXTURES["variables"]:
            v = _FIXTURES["variables"][key]
            if deserialize_json and isinstance(v, str):
                return json.loads(v)
            return v
        return f"<VAR:{key}>"

    Variable.get = staticmethod(stub_variable_get)

    def stub_get_connection(conn_id):
        fix = _FIXTURES["connections"].get(conn_id)
        if fix:
            return Connection(conn_id=conn_id, **fix)
        return Connection(
            conn_id=conn_id,
            host=f"<CONN:{conn_id}.host>",
            schema=f"<CONN:{conn_id}.schema>",
            login=f"<CONN:{conn_id}.login>",
            password=f"<CONN:{conn_id}.password>",
            port=0,
            extra="{}",
        )

    BaseHook.get_connection = staticmethod(stub_get_connection)

    # xcom_pull stub (patched on TaskInstance class)
    from airflow.models.taskinstance import TaskInstance

    def stub_xcom_pull(self, task_ids=None, key="return_value", **kw):
        ids = task_ids if isinstance(task_ids, str) else ",".join(task_ids or [])
        return f"<XCOM:{ids}.{key}>"

    TaskInstance.xcom_pull = stub_xcom_pull


def _airflow_version_ok() -> tuple[bool, str]:
    import airflow
    v = airflow.__version__
    major = int(v.split(".")[0])
    return major == 2, v


def _import_dag_file(path: Path) -> dict[str, Any]:
    """Return module globals after importing `path`. Raises on import error."""
    spec = importlib.util.spec_from_file_location(
        f"airflow_diff_user_dag_{path.stem}_{abs(hash(str(path)))}", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__dict__


def _build_context(dag, task, synthetic_logical_date: str) -> dict[str, Any]:
    """Build a Jinja context similar to what the scheduler injects at task run."""
    dt = datetime.fromisoformat(synthetic_logical_date)
    ds = dt.strftime("%Y-%m-%d")
    return {
        "ds": ds,
        "ts": dt.isoformat(),
        "logical_date": dt,
        "execution_date": dt,
        "dag": dag,
        "task": task,
        "ti": _StubTI(),
        "task_instance": _StubTI(),
        "params": dag.params if hasattr(dag, "params") else {},
        "var": _StubVarNamespace(),
        "conn": _StubConnNamespace(),
        "macros": __import__("airflow.macros", fromlist=["*"]),
    }


class _StubVarNamespace:
    class _ValueProxy:
        def __getattr__(self, name): return f"<VAR:{name}>"
        def __getitem__(self, name): return f"<VAR:{name}>"
    class _JsonProxy:
        def __getattr__(self, name): return f"<VAR:{name}>"  # placeholder
        def __getitem__(self, name): return f"<VAR:{name}>"
    value = _ValueProxy()
    json = _JsonProxy()


class _StubConnNamespace:
    def __getattr__(self, name): return _StubConnEntry(name)


class _StubConnEntry:
    def __init__(self, conn_id): self._id = conn_id
    def __getattr__(self, name): return f"<CONN:{self._id}.{name}>"


class _StubTI:
    def xcom_pull(self, task_ids=None, key="return_value", **kw):
        ids = task_ids if isinstance(task_ids, str) else ",".join(task_ids or [])
        return f"<XCOM:{ids}.{key}>"


def _render_dag(dag, synthetic_logical_date: str) -> dict[str, Any]:
    """Walk a DAG, render templates per task, return a dict matching RenderedDag schema."""
    from airflow_diff.schema import RenderedDag, RenderedTask, RenderedField, ProvenanceEntry, DatasetRefs, TaskGroupInfo

    tasks_out: list[RenderedTask] = []
    for task in dag.tasks:
        fields: dict[str, RenderedField] = {}
        context = _build_context(dag, task, synthetic_logical_date)
        for fname in (task.template_fields or ()):
            try:
                value = getattr(task, fname, None)
                rendered = task.render_template(value, context)
            except Exception as e:  # noqa: BLE001 — capture renderer errors per-field
                from airflow_diff.schema import RenderError
                fields[fname] = RenderedField(
                    rendered=None,
                    provenance=[],
                )
                # Field-level errors are recorded but the loop continues
                # (per spec section 7).
                tasks_out_field_error = RenderError(
                    type=type(e).__name__, message=str(e), traceback=traceback.format_exc()
                )
                fields[fname] = RenderedField(
                    rendered=f"<RENDER_ERROR: {tasks_out_field_error.type}>",
                    provenance=[ProvenanceEntry(source="literal")],
                )
                continue
            prov = _classify_provenance(rendered)
            fields[fname] = RenderedField(rendered=_jsonify(rendered), provenance=prov)
        # Also capture a couple of non-template literals that matter for diffs:
        for literal_name in ("retries", "retry_delay", "pool", "queue", "trigger_rule"):
            val = getattr(task, literal_name, None)
            if val is not None and literal_name not in fields:
                fields[literal_name] = RenderedField(
                    rendered=_jsonify(val),
                    provenance=[ProvenanceEntry(source="literal")],
                )

        tg_id = task.task_group.group_id if (task.task_group and task.task_group.group_id) else None
        tasks_out.append(RenderedTask(
            task_id=task.task_id,
            operator=f"{type(task).__module__}.{type(task).__name__}",
            task_group=tg_id,
            upstream=sorted(t.task_id for t in task.upstream_list),
            downstream=sorted(t.task_id for t in task.downstream_list),
            fields=fields,
        ))

    attrs = {
        "schedule": _jsonify(getattr(dag, "schedule_interval", None) or getattr(dag, "schedule", None)),
        "start_date": _jsonify(getattr(dag, "start_date", None)),
        "catchup": getattr(dag, "catchup", None),
        "tags": list(getattr(dag, "tags", []) or []),
        "description": getattr(dag, "description", None),
        "max_active_runs": getattr(dag, "max_active_runs", None),
    }
    # Strip None values
    attrs = {k: v for k, v in attrs.items() if v is not None}

    # TaskGroups (flat list of every non-root group)
    task_groups: list[TaskGroupInfo] = []
    try:
        for tg in dag.task_group_dict.values() if hasattr(dag, "task_group_dict") else []:
            if tg.group_id is None:
                continue
            task_groups.append(TaskGroupInfo(
                group_id=tg.group_id,
                tasks=sorted(t.task_id for t in tg.children.values() if hasattr(t, "task_id")),
            ))
    except Exception:
        pass

    # Datasets
    datasets = DatasetRefs(
        inlets=sorted(_extract_dataset_uris(getattr(dag, "dataset_triggers", None))),
        outlets=sorted(_extract_dataset_uris(_collect_outlets(dag))),
    )

    return RenderedDag(
        dag_id=dag.dag_id,
        status="ok",
        source_file=str(getattr(dag, "fileloc", "<unknown>")),
        attrs=attrs,
        datasets=datasets,
        task_groups=task_groups,
        tasks=tasks_out,
    )


def _collect_outlets(dag) -> list:
    outlets = []
    for t in dag.tasks:
        outlets.extend(getattr(t, "outlets", []) or [])
    return outlets


def _extract_dataset_uris(items) -> list[str]:
    if not items:
        return []
    uris = []
    for it in items:
        uri = getattr(it, "uri", None)
        if uri:
            uris.append(uri)
    return uris


def _classify_provenance(value):
    """Best-effort: scan a string for our stub markers and report what was used."""
    from airflow_diff.schema import ProvenanceEntry
    if not isinstance(value, str):
        return [ProvenanceEntry(source="literal")]
    import re
    prov = []
    for m in re.finditer(r"<(VAR|CONN|XCOM):([^>]+)>", value):
        kind = m.group(1)
        key = m.group(2)
        if kind == "VAR":
            prov.append(ProvenanceEntry(source="stub", key=f"var.value.{key}"))
        elif kind == "CONN":
            prov.append(ProvenanceEntry(source="stub", key=f"conn.{key}"))
        elif kind == "XCOM":
            prov.append(ProvenanceEntry(source="stub", key=f"xcom.{key}"))
    if not prov:
        prov.append(ProvenanceEntry(source="literal"))
    return prov


def _jsonify(value):
    """Make any value safe to put in JSON. Stringify datetimes, durations, etc."""
    from datetime import datetime, timedelta
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return f"PT{int(value.total_seconds())}S"
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return repr(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worktree", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--config", required=True, help="JSON-encoded config dict")
    parser.add_argument("--fixtures", default=None, help="path to fixtures YAML")
    args = parser.parse_args(argv)

    worktree = Path(args.worktree)
    config = json.loads(args.config)

    # Load fixtures if provided
    if args.fixtures:
        import yaml
        fix = yaml.safe_load(Path(args.fixtures).read_text()) or {}
        _FIXTURES["variables"] = fix.get("variables") or {}
        _FIXTURES["connections"] = fix.get("connections") or {}

    # Check Airflow version
    ok, version = _airflow_version_ok()
    if not ok:
        print(f"ERROR: airflow-diff requires Airflow 2.x; found {version}", file=sys.stderr)
        return 4

    _install_stubs()

    # Add dags + plugins to path
    dags_folder = worktree / config.get("dags_folder", "dags")
    plugins_folder = worktree / config.get("plugins_folder", "plugins")
    for p in (dags_folder, plugins_folder):
        if p.exists() and str(p) not in sys.path:
            sys.path.insert(0, str(p))

    synthetic_logical_date = config.get("synthetic_logical_date", "2025-01-01T00:00:00+00:00")

    from airflow.models import DAG
    from airflow_diff.schema import (
        RenderedDag, RenderedDagBag, RenderError, SCHEMA_VERSION,
    )

    rendered: list[RenderedDag] = []
    if dags_folder.exists():
        for py in sorted(dags_folder.rglob("*.py")):
            try:
                globs = _import_dag_file(py)
            except Exception as e:
                rendered.append(RenderedDag(
                    dag_id=py.stem, status="error", source_file=str(py.relative_to(worktree)),
                    error=RenderError(type=type(e).__name__, message=str(e), traceback=traceback.format_exc()),
                ))
                continue
            for v in globs.values():
                if isinstance(v, DAG):
                    try:
                        rendered.append(_render_dag(v, synthetic_logical_date))
                    except Exception as e:
                        rendered.append(RenderedDag(
                            dag_id=v.dag_id, status="error",
                            source_file=str(py.relative_to(worktree)),
                            error=RenderError(type=type(e).__name__, message=str(e), traceback=traceback.format_exc()),
                        ))

    import airflow
    bag = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha=args.commit_sha,
        airflow_version=airflow.__version__,
        rendered_at=datetime.now(timezone.utc),
        dags=rendered,
    )
    print(bag.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 12.5: Run integration test**

Run: `uv run pytest tests/integration/test_renderer.py -v -m integration`
Expected: pass (Airflow installed via dev extras).

If it fails because the spawned subprocess doesn't see the package, ensure `airflow-diff` itself is installed in the test venv (`uv pip install -e .`).

- [ ] **Step 12.6: Commit**

```bash
git add src/airflow_diff/renderer.py tests/integration/test_renderer.py \
        tests/fixtures/dags_base/linear.py tests/fixtures/dags_head/linear.py
git commit -m "feat(renderer): subprocess entry point with stub layer and Jinja rendering"
```

### Task 13: DAG fixtures — templates, TaskGroups, custom op, datasets, factory, broken

Build out the integration fixture library that lets future tests exercise every required behavior.

**Files:**
- Create: `tests/fixtures/plugins/operators.py`
- Create: `tests/fixtures/dags_base/templated.py`, `task_groups.py`, `custom_op.py`, `xcom.py`, `datasets.py`, `factory.py`, `broken_import.py`, `broken_init.py`, `missing_macro.py`, `nested_params.py`
- Create: matching `tests/fixtures/dags_head/...` files where the test plan calls for a paired diff (otherwise just copy)
- Modify: `tests/integration/test_renderer.py`

- [ ] **Step 13.1: Create the plugin file for the custom operator**

`tests/fixtures/plugins/operators.py`:

```python
from airflow.models.baseoperator import BaseOperator


class GreetingOperator(BaseOperator):
    template_fields = ("greeting", "name")

    def __init__(self, *, greeting: str, name: str, **kwargs):
        super().__init__(**kwargs)
        self.greeting = greeting
        self.name = name

    def execute(self, context):
        return f"{self.greeting}, {self.name}!"
```

- [ ] **Step 13.2: Create every fixture DAG (showing only key ones; copy pattern)**

`tests/fixtures/dags_base/templated.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(dag_id="templated", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    BashOperator(
        task_id="copy_bucket",
        bash_command="aws s3 cp s3://{{ var.value.bucket }}/{{ ds }} /tmp/in",
    )
    BashOperator(
        task_id="copy_conn",
        bash_command="psql -h {{ conn.warehouse.host }} -c 'select 1'",
    )
    # Demonstrates Variable.get() inside Python at DAG-build time:
    region = Variable.get("region", default_var=None)
    BashOperator(task_id="show_region", bash_command=f"echo {region}")
```

`tests/fixtures/dags_base/task_groups.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="task_groups", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    start = BashOperator(task_id="start", bash_command="echo s")
    with TaskGroup(group_id="transform") as tg:
        clean = BashOperator(task_id="clean", bash_command="echo c")
        enrich = BashOperator(task_id="enrich", bash_command="echo e")
        clean >> enrich
    end = BashOperator(task_id="end", bash_command="echo end")
    start >> tg >> end
```

`tests/fixtures/dags_base/custom_op.py`:

```python
from datetime import datetime
from airflow import DAG
from operators import GreetingOperator  # imported from plugins/ on sys.path

with DAG(dag_id="custom_op", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    GreetingOperator(task_id="hello", greeting="Hello", name="{{ var.value.user }}")
```

`tests/fixtures/dags_base/xcom.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="xcom", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    BashOperator(task_id="upstream", bash_command="echo data")
    BashOperator(
        task_id="downstream",
        bash_command="echo {{ ti.xcom_pull(task_ids='upstream') }}",
    )
```

`tests/fixtures/dags_base/datasets.py`:

```python
from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

OUT = Dataset("s3://bucket/output")
IN = Dataset("s3://bucket/input")

with DAG(dag_id="dataset_dag", start_date=datetime(2024, 1, 1), schedule=[IN], catchup=False) as dag:
    BashOperator(task_id="produce", bash_command="echo out", outlets=[OUT])
```

`tests/fixtures/dags_base/factory.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

CONFIGS = [{"name": "alpha"}, {"name": "beta"}, {"name": "gamma"}]


def make_dag(cfg):
    with DAG(dag_id=f"factory_{cfg['name']}", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
        BashOperator(task_id="t", bash_command=f"echo {cfg['name']}")
    return dag


for cfg in CONFIGS:
    globals()[f"dag_{cfg['name']}"] = make_dag(cfg)
```

`tests/fixtures/dags_base/broken_import.py`:

```python
from this_module_does_not_exist import nope  # noqa: F401

from datetime import datetime
from airflow import DAG
with DAG(dag_id="broken_import", start_date=datetime(2024, 1, 1)) as dag:
    pass
```

`tests/fixtures/dags_base/broken_init.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="broken_init", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    BashOperator(task_id="t", bash_command=None)  # raises in operator __init__
```

`tests/fixtures/dags_base/missing_macro.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="missing_macro", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    BashOperator(task_id="t", bash_command="echo {{ macros.this_does_not_exist }}")
```

`tests/fixtures/dags_base/nested_params.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="nested_params", start_date=datetime(2024, 1, 1), schedule=None, catchup=False,
         params={"region": "us-east-1", "bucket": "{{ var.value.bucket }}"}) as dag:
    BashOperator(task_id="t", bash_command="echo {{ params.region }} {{ params.bucket }}")
```

Copy each to `tests/fixtures/dags_head/` unchanged for now (Task 16 will introduce diffs).

- [ ] **Step 13.3: Integration tests for each scenario**

Append to `tests/integration/test_renderer.py`:

```python
def _setup_worktree(tmp_path: Path, fixture_files: list[str], include_plugins: bool = False) -> Path:
    (tmp_path / "dags").mkdir()
    for f in fixture_files:
        src = FIXTURES_ROOT / "dags_base" / f
        (tmp_path / "dags" / f).write_text(src.read_text())
    if include_plugins:
        (tmp_path / "plugins").mkdir()
        (tmp_path / "plugins" / "operators.py").write_text(
            (FIXTURES_ROOT / "plugins" / "operators.py").read_text()
        )
    return tmp_path


def test_renders_templated_dag(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["templated.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "templated"]
    copy = next(t for t in dag.tasks if t.task_id == "copy_bucket")
    assert copy.fields["bash_command"].rendered == "aws s3 cp s3://<VAR:bucket>/2025-01-01 /tmp/in"
    sources = {p.source for p in copy.fields["bash_command"].provenance}
    assert "stub" in sources


def test_renders_task_groups(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["task_groups.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "task_groups"]
    groups = {g.group_id for g in dag.task_groups}
    assert "transform" in groups
    clean = next(t for t in dag.tasks if t.task_id == "transform.clean" or t.task_id == "clean")
    assert clean.task_group == "transform"


def test_renders_custom_operator(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["custom_op.py"], include_plugins=True)
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "custom_op"]
    hello = dag.tasks[0]
    assert hello.operator.endswith("GreetingOperator")
    assert hello.fields["name"].rendered == "<VAR:user>"


def test_renders_xcom_stub(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["xcom.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "xcom"]
    down = next(t for t in dag.tasks if t.task_id == "downstream")
    assert "<XCOM:upstream.return_value>" in down.fields["bash_command"].rendered


def test_renders_dataset_dag(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["datasets.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "dataset_dag"]
    assert "s3://bucket/output" in dag.datasets.outlets


def test_renders_factory_produces_multiple_dags(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["factory.py"])
    bag = _run_renderer(wt)
    ids = {d.dag_id for d in bag.dags if d.dag_id.startswith("factory_")}
    assert ids == {"factory_alpha", "factory_beta", "factory_gamma"}


def test_broken_import_captured_as_error(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["broken_import.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "broken_import"]
    assert dag.status == "error"
    assert "this_module_does_not_exist" in dag.error.message


def test_broken_init_captured_as_error(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["broken_init.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "broken_init"]
    assert dag.status == "error"


def test_field_render_error_recorded(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["missing_macro.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "missing_macro"]
    assert dag.status == "ok"  # The DAG itself imported fine
    t = dag.tasks[0]
    assert "RENDER_ERROR" in str(t.fields["bash_command"].rendered)


def test_fixtures_override_variable(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["templated.py"])
    fixtures_yaml = tmp_path / "fixtures.yaml"
    fixtures_yaml.write_text("variables:\n  bucket: real-bucket\n")
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff.renderer",
         "--worktree", str(wt), "--commit-sha", "x",
         "--config", "{}",
         "--fixtures", str(fixtures_yaml)],
        capture_output=True, text=True, check=True,
    )
    bag = RenderedDagBag.model_validate_json(res.stdout)
    [dag] = [d for d in bag.dags if d.dag_id == "templated"]
    copy = next(t for t in dag.tasks if t.task_id == "copy_bucket")
    assert "real-bucket" in copy.fields["bash_command"].rendered
    assert "<VAR:bucket>" not in copy.fields["bash_command"].rendered
```

- [ ] **Step 13.4: Run the integration suite, address renderer gaps**

Run: `uv run pytest tests/integration/test_renderer.py -v -m integration`

If any test fails, fix the renderer (some likely needed touch-ups: TaskGroup task_id prefixing, dataset extraction from `schedule=[ds]`, fixture YAML loading path). Iterate until green.

- [ ] **Step 13.5: Commit**

```bash
git add tests/fixtures/ tests/integration/test_renderer.py src/airflow_diff/renderer.py
git commit -m "feat(renderer): comprehensive fixture coverage (templates, groups, datasets, factory, broken DAGs)"
```

### Task 14: Airflow-version guard

- [ ] **Step 14.1: Test that bogus version is rejected**

Append to `tests/integration/test_renderer.py`:

```python
def test_renderer_rejects_airflow_3_via_mock(tmp_path: Path, monkeypatch):
    """We can't actually install Airflow 3 in the test env; test the version check directly."""
    from airflow_diff.renderer import _airflow_version_ok
    import airflow
    real = airflow.__version__
    airflow.__version__ = "3.0.0"
    try:
        ok, v = _airflow_version_ok()
        assert ok is False
        assert v == "3.0.0"
    finally:
        airflow.__version__ = real
```

- [ ] **Step 14.2: Run, confirm pass** (the renderer already handles this)

- [ ] **Step 14.3: Commit**

```bash
git add tests/integration/test_renderer.py
git commit -m "test(renderer): explicit airflow 3.x rejection test"
```

---

## Phase 7 — Orchestrator and CLI

### Task 15: Orchestrator wiring

**Files:**
- Create: `src/airflow_diff/orchestrator.py`
- Create: `tests/unit/test_orchestrator.py` (mock-heavy)
- (Real end-to-end test comes in Task 17.)

- [ ] **Step 15.1: Failing test — orchestrator calls renderer twice, returns diff**

`tests/unit/test_orchestrator.py`:

```python
from pathlib import Path
from unittest.mock import patch, MagicMock

from airflow_diff.schema import DiffDocument, RenderedDagBag


def test_orchestrator_invokes_renderer_per_commit_and_diffs(tmp_path, monkeypatch):
    from airflow_diff import orchestrator
    from airflow_diff.config import Config

    base_bag_json = RenderedDagBag(
        schema_version=1, commit_sha="aaa", airflow_version="2.10.3",
        rendered_at=__import__("datetime").datetime(2026, 5, 17), dags=[],
    ).model_dump_json()
    head_bag_json = RenderedDagBag(
        schema_version=1, commit_sha="bbb", airflow_version="2.10.3",
        rendered_at=__import__("datetime").datetime(2026, 5, 17), dags=[],
    ).model_dump_json()

    # Patch worktree, venv, and subprocess
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
        proc.communicate.return_value = (
            head_bag_json if call_count["n"] else base_bag_json, "",
        )
        proc.returncode = 0
        call_count["n"] += 1
        return proc
    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    diff = orchestrator.run_diff(tmp_path, "aaa", "bbb", Config())
    assert isinstance(diff, DiffDocument)
    assert diff.base_sha.startswith("aaa")
    assert diff.head_sha.startswith("bbb")
```

- [ ] **Step 15.2: Run, confirm import failure**

- [ ] **Step 15.3: Implement `orchestrator.py`**

`src/airflow_diff/orchestrator.py`:

```python
"""Top-level coordinator.

Resolves SHAs, prepares worktrees and venvs, spawns one renderer subprocess per
commit (in parallel), reads their JSON, runs the diff engine, and returns a
DiffDocument. The parent process never imports Airflow.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from airflow_diff.config import Config, load_fixtures
from airflow_diff.diff import compute_diff
from airflow_diff.schema import DiffDocument, RenderedDagBag
from airflow_diff.venv import venv_for
from airflow_diff.worktree import (
    ensure_sha_present, resolve_sha, worktree_for,
)


class OrchestratorError(RuntimeError):
    pass


def _touched_files(repo_root: Path, base_sha: str, head_sha: str) -> list[str]:
    res = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--name-only", base_sha, head_sha],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        raise OrchestratorError(f"git diff failed: {res.stderr.strip()}")
    return [line for line in res.stdout.splitlines() if line.strip()]


def _spawn_renderer(python: Path, worktree: Path, sha: str, config: Config,
                    fixtures_yaml: Path | None) -> RenderedDagBag:
    args = [
        str(python), "-m", "airflow_diff.renderer",
        "--worktree", str(worktree),
        "--commit-sha", sha,
        "--config", json.dumps({
            "dags_folder": config.dags_folder,
            "plugins_folder": config.plugins_folder,
            "synthetic_logical_date": config.synthetic_logical_date.isoformat(),
        }),
    ]
    if fixtures_yaml is not None:
        args.extend(["--fixtures", str(fixtures_yaml)])
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = proc.communicate(timeout=config.render_timeout_seconds)
    if proc.returncode != 0:
        raise OrchestratorError(
            f"renderer subprocess failed (exit {proc.returncode}) for sha {sha}:\n"
            f"stderr (last 2000 chars): {err[-2000:]}"
        )
    try:
        return RenderedDagBag.model_validate_json(out)
    except Exception as e:
        raise OrchestratorError(
            f"renderer for sha {sha} emitted invalid JSON: {e}\n"
            f"stdout (first 2000 chars): {out[:2000]}"
        ) from e


def run_diff(repo_root: Path, base_ref: str, head_ref: str, config: Config) -> DiffDocument:
    base_sha = resolve_sha(repo_root, base_ref)
    head_sha = resolve_sha(repo_root, head_ref)
    ensure_sha_present(repo_root, base_sha)
    ensure_sha_present(repo_root, head_sha)

    touched = _touched_files(repo_root, base_sha, head_sha)

    with worktree_for(repo_root, base_sha) as wt_base, \
         worktree_for(repo_root, head_sha) as wt_head:

        # Each worktree may carry its own fixtures file (per-commit)
        fixtures_base = wt_base / config.fixtures_path
        fixtures_head = wt_head / config.fixtures_path

        py_base = venv_for(wt_base)
        py_head = venv_for(wt_head)

        # Renderers run serially for simplicity in MVP; parallel is a later optimization
        rendered_base = _spawn_renderer(
            py_base, wt_base, base_sha, config,
            fixtures_base if fixtures_base.exists() else None,
        )
        rendered_head = _spawn_renderer(
            py_head, wt_head, head_sha, config,
            fixtures_head if fixtures_head.exists() else None,
        )

    return compute_diff(rendered_base, rendered_head, touched_files=touched)
```

- [ ] **Step 15.4: Run, confirm pass**

Run: `uv run pytest tests/unit/test_orchestrator.py -v`
Expected: pass.

- [ ] **Step 15.5: Commit**

```bash
git add src/airflow_diff/orchestrator.py tests/unit/test_orchestrator.py
git commit -m "feat(orchestrator): wire worktree + venv + renderer + diff into a single entry point"
```

### Task 16: CLI

**Files:**
- Create: `src/airflow_diff/cli.py`
- Create: `src/airflow_diff/__main__.py`
- Create: `tests/unit/test_cli.py`

- [ ] **Step 16.1: Test CLI parses args and dispatches**

`tests/unit/test_cli.py`:

```python
import sys
from pathlib import Path
from unittest.mock import patch

from airflow_diff import cli
from airflow_diff.schema import DiffDocument, DiffSummary


def _empty_diff():
    return DiffDocument(
        schema_version=1, base_sha="aaa", head_sha="bbb",
        summary=DiffSummary(), dags=[], render_errors=[],
    )


def test_cli_diff_invokes_run_diff(monkeypatch, tmp_path, capsys):
    called = {}
    def fake_run_diff(repo, a, b, config):
        called["args"] = (repo, a, b)
        return _empty_diff()
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "abc", "def", "--repo", str(tmp_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No DAG differences detected" in out
    assert called["args"][1:] == ("abc", "def")


def test_cli_unknown_subcommand_exits_nonzero():
    rc = cli.main(["bogus"])
    assert rc != 0


def test_cli_exit_code_for_regression(monkeypatch, tmp_path, capsys):
    from airflow_diff.schema import DagDiff
    def fake_run_diff(repo, a, b, config):
        return DiffDocument(
            schema_version=1, base_sha="aaa", head_sha="bbb",
            summary=DiffSummary(dags_regressed=1),
            dags=[DagDiff(dag_id="x", classification="touched", pair_status="regressed")],
            render_errors=[],
        )
    monkeypatch.setattr(cli, "run_diff", fake_run_diff)
    rc = cli.main(["diff", "a", "b", "--repo", str(tmp_path)])
    assert rc == 1  # regression
```

- [ ] **Step 16.2: Run, confirm import failure**

- [ ] **Step 16.3: Implement `cli.py`**

`src/airflow_diff/cli.py`:

```python
"""Argparse CLI entry point for airflow-diff."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from airflow_diff.config import load_config
from airflow_diff.orchestrator import run_diff
from airflow_diff.present.markdown import render_markdown
from airflow_diff.schema import DiffDocument


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="airflow-diff")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_diff = sub.add_parser("diff", help="Render and diff DAGs across two commits")
    p_diff.add_argument("base_ref")
    p_diff.add_argument("head_ref")
    p_diff.add_argument("--repo", default=".", help="Path to repo (default: cwd)")
    p_diff.add_argument("--format", choices=["markdown", "terminal", "html"], default="markdown")
    p_diff.add_argument("--out", default=None, help="Write output to FILE instead of stdout")
    p_diff.add_argument("--json-out", default=None, help="Also write the raw DiffDocument JSON to FILE")

    p_report = sub.add_parser("report", help="Re-format an existing diff document")
    p_report.add_argument("diff_json", type=Path)
    p_report.add_argument("--format", choices=["markdown", "terminal", "html"], default="markdown")
    p_report.add_argument("--out", default=None)

    p_render = sub.add_parser("render", help="(internal) Render a single commit")
    p_render.add_argument("ref")
    p_render.add_argument("--repo", default=".")
    p_render.add_argument("--out", default=None)

    args = parser.parse_args(argv)

    if args.cmd == "diff":
        return _cmd_diff(args)
    if args.cmd == "report":
        return _cmd_report(args)
    if args.cmd == "render":
        return _cmd_render(args)
    return 2


def _cmd_diff(args) -> int:
    repo = Path(args.repo).resolve()
    config = load_config(repo)
    diff = run_diff(repo, args.base_ref, args.head_ref, config)
    _emit(diff, args.format, args.out)
    if args.json_out:
        Path(args.json_out).write_text(diff.model_dump_json(indent=2))
    return _exit_code(diff)


def _cmd_report(args) -> int:
    diff = DiffDocument.model_validate_json(args.diff_json.read_text())
    _emit(diff, args.format, args.out)
    return 0


def _cmd_render(args) -> int:
    # Convenience wrapper around `python -m airflow_diff.renderer`
    import subprocess as sp
    from airflow_diff.worktree import resolve_sha, worktree_for
    from airflow_diff.venv import venv_for
    repo = Path(args.repo).resolve()
    sha = resolve_sha(repo, args.ref)
    with worktree_for(repo, sha) as wt:
        py = venv_for(wt)
        res = sp.run(
            [str(py), "-m", "airflow_diff.renderer",
             "--worktree", str(wt), "--commit-sha", sha, "--config", "{}"],
            capture_output=True, text=True, check=False,
        )
    if res.returncode != 0:
        print(res.stderr, file=sys.stderr)
        return res.returncode
    if args.out:
        Path(args.out).write_text(res.stdout)
    else:
        sys.stdout.write(res.stdout)
    return 0


def _emit(diff: DiffDocument, fmt: str, out_path: str | None) -> None:
    if fmt == "markdown":
        text = render_markdown(diff)
    elif fmt == "terminal":
        from airflow_diff.present.terminal import render_terminal
        text = render_terminal(diff)
    else:
        from airflow_diff.present.html import render_html
        text = render_html(diff)
    if out_path:
        Path(out_path).write_text(text)
    else:
        sys.stdout.write(text)


def _exit_code(diff: DiffDocument) -> int:
    """Non-zero only when the PR introduced a regression (per spec section 7)."""
    if diff.summary.dags_regressed > 0:
        return 1
    # Added DAGs that failed to import are also regressions:
    for d in diff.dags:
        if d.classification == "added" and d.status_b == "error":
            return 1
    return 0


run_diff = run_diff  # re-export so tests can monkeypatch on cli module
```

- [ ] **Step 16.4: Create `__main__.py`**

`src/airflow_diff/__main__.py`:

```python
from airflow_diff.cli import main

raise SystemExit(main())
```

- [ ] **Step 16.5: Run, confirm pass**

Run: `uv run pytest tests/unit/test_cli.py -v`
Expected: 3 passes.

- [ ] **Step 16.6: Commit**

```bash
git add src/airflow_diff/cli.py src/airflow_diff/__main__.py tests/unit/test_cli.py
git commit -m "feat(cli): argparse entry point with diff/render/report subcommands"
```

### Task 17: End-to-end CLI integration test

**Files:**
- Create: `tests/fixtures/sample_repo_builder.py`
- Create: `tests/integration/test_cli.py`

- [ ] **Step 17.1: Sample repo builder**

`tests/fixtures/sample_repo_builder.py`:

```python
"""Programmatically build a two-commit sample repo for end-to-end testing.

The repo contains a `dags/` folder. Commit A has `dags_base/linear.py`;
commit B replaces it with `dags_head/linear.py` (which differs in one
bash_command).
"""
from __future__ import annotations

import subprocess
from pathlib import Path


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def build(repo_dir: Path, fixtures_root: Path, requirements_text: str) -> tuple[str, str]:
    """Build the repo. Returns (base_sha, head_sha)."""
    repo_dir.mkdir(parents=True, exist_ok=True)
    _git(repo_dir, "init", "-b", "main")
    _git(repo_dir, "config", "user.email", "test@example.com")
    _git(repo_dir, "config", "user.name", "test")

    (repo_dir / "requirements.txt").write_text(requirements_text)
    (repo_dir / "dags").mkdir()
    (repo_dir / "dags" / "linear.py").write_text(
        (fixtures_root / "dags_base" / "linear.py").read_text()
    )
    _git(repo_dir, "add", ".")
    _git(repo_dir, "commit", "-m", "base")
    base_sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()

    # Modify linear.py with a small change
    src = (fixtures_root / "dags_base" / "linear.py").read_text()
    modified = src.replace('bash_command="echo end"', 'bash_command="echo finished"')
    (repo_dir / "dags" / "linear.py").write_text(modified)
    _git(repo_dir, "add", ".")
    _git(repo_dir, "commit", "-m", "head")
    head_sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return base_sha, head_sha
```

- [ ] **Step 17.2: Test**

`tests/integration/test_cli.py`:

```python
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

FIXTURES_ROOT = Path(__file__).parent.parent / "fixtures"


def test_end_to_end_diff_emits_markdown(tmp_path):
    from tests.fixtures.sample_repo_builder import build
    repo = tmp_path / "repo"
    base_sha, head_sha = build(repo, FIXTURES_ROOT, "apache-airflow==2.10.3\n")
    out = tmp_path / "comment.md"
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff", "diff",
         base_sha, head_sha, "--repo", str(repo),
         "--out", str(out)],
        capture_output=True, text=True, check=False,
    )
    assert res.returncode == 0, f"stderr={res.stderr}"
    text = out.read_text()
    assert "linear" in text
    assert "echo end" in text
    assert "echo finished" in text
```

Note: this test is slow (creates a venv, installs Airflow). Mark it accordingly. Skip it in fast CI lanes if needed.

- [ ] **Step 17.3: Run the end-to-end test**

Run: `uv run pytest tests/integration/test_cli.py -v -m integration --timeout=600`
Expected: pass within a minute or two on a warm cache; first run will install Airflow into the per-commit venv (~30s with uv).

If it fails because the orchestrator's `venv_for` tries to install a heavy dep set and times out, increase `--render-timeout-seconds` via env or shorten the requirements file in the fixture.

- [ ] **Step 17.4: Commit**

```bash
git add tests/fixtures/sample_repo_builder.py tests/integration/test_cli.py
git commit -m "test: end-to-end CLI integration test with programmatic sample repo"
```

---

## Phase 8 — Terminal and HTML presenters

### Task 18: Terminal presenter

**Files:**
- Create: `src/airflow_diff/present/terminal.py`
- Create: `tests/unit/present/test_terminal.py`

- [ ] **Step 18.1: Snapshot test for terminal output**

`tests/unit/present/test_terminal.py`:

```python
from pathlib import Path

from airflow_diff.present.terminal import render_terminal
from airflow_diff.schema import DiffDocument

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def test_empty(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "empty.json").read_text())
    assert render_terminal(doc) == snapshot


def test_single_dag(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "single_dag_one_change.json").read_text())
    assert render_terminal(doc) == snapshot
```

- [ ] **Step 18.2: Run, confirm import failure**

- [ ] **Step 18.3: Implement terminal presenter**

`src/airflow_diff/present/terminal.py`:

```python
"""ANSI-colored text presenter."""
from __future__ import annotations

from airflow_diff.schema import DiffDocument, DagDiff, FieldDiff, TaskDiff

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
RESET = "\033[0m"


def render_terminal(doc: DiffDocument) -> str:
    if not doc.dags and not doc.render_errors:
        return f"{BOLD}airflow-diff:{RESET} no DAG differences detected.\n"
    lines: list[str] = []
    s = doc.summary
    total = s.dags_touched + s.dags_incidentally_affected + s.dags_added + s.dags_removed
    lines.append(f"{BOLD}airflow-diff: {total} DAG(s) changed{RESET}")
    lines.append(f"  base: {doc.base_sha[:8]}  head: {doc.head_sha[:8]}")
    if s.dags_regressed:
        lines.append(f"  {RED}{s.dags_regressed} regressed{RESET}")
    if s.dags_fixed:
        lines.append(f"  {GREEN}{s.dags_fixed} fixed{RESET}")
    lines.append("")
    for d in doc.dags:
        lines.extend(_render_dag(d))
    return "\n".join(lines) + "\n"


def _render_dag(d: DagDiff) -> list[str]:
    out = [f"{BOLD}{d.dag_id}{RESET} ({d.classification})"]
    for ad in d.attr_diffs:
        out.append(f"  attr {ad.name}: {RED}{ad.before}{RESET} -> {GREEN}{ad.after}{RESET}")
    for td in d.task_diffs:
        out.extend(_render_task(td))
    out.append("")
    return out


def _render_task(td: TaskDiff) -> list[str]:
    if td.change_type == "added":
        return [f"  {GREEN}+ task {td.task_id}{RESET}"]
    if td.change_type == "removed":
        return [f"  {RED}- task {td.task_id}{RESET}"]
    out = [f"  {YELLOW}~ task {td.task_id}{RESET}"]
    if td.operator_before and td.operator_after:
        out.append(f"      operator: {td.operator_before} -> {td.operator_after}")
    for fd in td.field_diffs:
        out.append(f"      {fd.name}:")
        out.append(f"        {RED}- {fd.before}{RESET}")
        out.append(f"        {GREEN}+ {fd.after}{RESET}")
    for ed in td.edge_diffs:
        sign = "+" if ed.change_type == "added" else "-"
        color = GREEN if ed.change_type == "added" else RED
        out.append(f"      {color}{sign} {ed.direction} edge: {td.task_id} -> {ed.related_task_id}{RESET}")
    return out
```

- [ ] **Step 18.4: Generate snapshots, confirm pass**

Run: `uv run pytest tests/unit/present/test_terminal.py --snapshot-update -v`
Then: `uv run pytest tests/unit/present/test_terminal.py -v`

- [ ] **Step 18.5: Commit**

```bash
git add src/airflow_diff/present/terminal.py tests/unit/present/test_terminal.py \
        tests/unit/present/__snapshots__/
git commit -m "feat(present): ANSI-colored terminal presenter"
```

### Task 19: HTML presenter

**Files:**
- Create: `src/airflow_diff/present/html.py`
- Create: `tests/unit/present/test_html.py`

- [ ] **Step 19.1: Snapshot tests**

`tests/unit/present/test_html.py`:

```python
from pathlib import Path

from airflow_diff.present.html import render_html
from airflow_diff.schema import DiffDocument

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "diff_documents"


def test_empty(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "empty.json").read_text())
    out = render_html(doc)
    assert "<html" in out
    assert "airflow-diff" in out


def test_single_dag(snapshot):
    doc = DiffDocument.model_validate_json((FIXTURES / "single_dag_one_change.json").read_text())
    out = render_html(doc)
    assert "<table" in out
    assert "mermaid" in out
    assert out == snapshot
```

- [ ] **Step 19.2: Implement**

`src/airflow_diff/present/html.py`:

```python
"""Standalone HTML presenter for cases where the markdown comment is too large.

Reuses the markdown presenter under the hood, then wraps the result in a
self-contained HTML document with Mermaid + GitHub-ish CSS for parity with how
the comment would render in a PR.
"""
from __future__ import annotations

import html

from airflow_diff.present.markdown import render_markdown
from airflow_diff.schema import DiffDocument

_TEMPLATE = """<!doctype html>
<html><head><meta charset="utf-8"><title>airflow-diff</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 980px; margin: 2rem auto; padding: 0 1rem; color: #1f2328; }}
table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
th, td {{ border: 1px solid #d0d7de; padding: 6px 12px; text-align: left; }}
th {{ background: #f6f8fa; }}
pre {{ background: #f6f8fa; padding: 12px; border-radius: 6px; overflow-x: auto; }}
code {{ background: #afb8c133; padding: .2em .4em; border-radius: 6px; }}
details {{ margin: 8px 0; }}
summary {{ cursor: pointer; color: #0969da; }}
.diff-add {{ color: #1a7f37; background: #dafbe1; display: block; }}
.diff-del {{ color: #cf222e; background: #ffebe9; display: block; }}
</style>
<script type="module">
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
mermaid.initialize({{ startOnLoad: true }});
</script>
</head><body>
<pre style="display:none" id="raw-md">{md_escaped}</pre>
<div id="rendered">{rendered}</div>
<script>
// Light client-side conversion: render the markdown via marked.js, then init mermaid for any code fences with lang=mermaid.
</script>
</body></html>
"""


def render_html(doc: DiffDocument) -> str:
    md = render_markdown(doc)
    rendered = _markdown_to_html(md)
    return _TEMPLATE.format(md_escaped=html.escape(md), rendered=rendered)


def _markdown_to_html(md: str) -> str:
    """A *very* minimal markdown-to-HTML conversion. Sufficient for our output
    shape (headers, tables, code fences with diff/mermaid, details blocks).
    We pin our markdown shape, so we don't need a full parser.
    """
    import re
    out = []
    i = 0
    lines = md.splitlines()
    while i < len(lines):
        line = lines[i]
        if line.startswith("```mermaid"):
            j = i + 1
            body = []
            while j < len(lines) and not lines[j].startswith("```"):
                body.append(lines[j])
                j += 1
            out.append('<pre class="mermaid">' + html.escape("\n".join(body)) + "</pre>")
            i = j + 1
            continue
        if line.startswith("```diff"):
            j = i + 1
            body = []
            while j < len(lines) and not lines[j].startswith("```"):
                body.append(lines[j])
                j += 1
            colored = []
            for b in body:
                cls = "diff-add" if b.startswith("+") else ("diff-del" if b.startswith("-") else "")
                if cls:
                    colored.append(f'<span class="{cls}">{html.escape(b)}</span>')
                else:
                    colored.append(html.escape(b))
            out.append("<pre>" + "\n".join(colored) + "</pre>")
            i = j + 1
            continue
        if line.startswith("## "):
            out.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("### "):
            out.append(f"<h3>{html.escape(line[4:])}</h3>")
        elif line.startswith("| "):
            # Collect a table
            j = i
            tbl = []
            while j < len(lines) and lines[j].startswith("|"):
                tbl.append(lines[j]); j += 1
            out.append(_render_table(tbl))
            i = j
            continue
        elif line.startswith("<details"):
            out.append(line)  # pass through, GitHub-flavored HTML
        elif line.startswith("</details>"):
            out.append(line)
        elif line.startswith("<summary"):
            out.append(line)
        else:
            out.append(html.escape(line) + "<br>" if line.strip() else "")
        i += 1
    return "\n".join(out)


def _render_table(lines: list[str]) -> str:
    rows = [[c.strip() for c in re.split(r"\s*\|\s*", l.strip("|"))] for l in lines]
    header = rows[0]
    body = rows[2:]  # rows[1] is the |---|---| separator
    out = ["<table><thead><tr>"]
    out.extend(f"<th>{html.escape(h)}</th>" for h in header)
    out.append("</tr></thead><tbody>")
    for r in body:
        out.append("<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in r) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


import re  # noqa: E402 — placed after for readability
```

- [ ] **Step 19.3: Generate snapshot, confirm pass**

Run: `uv run pytest tests/unit/present/test_html.py --snapshot-update -v`
Then: `uv run pytest tests/unit/present/test_html.py -v`

- [ ] **Step 19.4: Commit**

```bash
git add src/airflow_diff/present/html.py tests/unit/present/test_html.py \
        tests/unit/present/__snapshots__/
git commit -m "feat(present): standalone HTML presenter (markdown→HTML wrapper)"
```

---

## Phase 9 — GitHub Action wrapper

### Task 20: action.yml + entrypoint.sh

**Files:**
- Create: `action/action.yml`
- Create: `action/entrypoint.sh`

- [ ] **Step 20.1: Write `action.yml`**

`action/action.yml`:

```yaml
name: "airflow-diff"
description: "Render and diff Apache Airflow DAGs across a PR's base and head commits, posted as a PR comment."
author: "airflow-diff contributors"
branding:
  icon: "git-pull-request"
  color: "blue"
inputs:
  airflow-diff-version:
    description: "Version of airflow-diff to install"
    required: false
    default: "0.1.0"
  python-version:
    description: "Python version to use"
    required: false
    default: "3.11"
  base-sha:
    description: "Base SHA (defaults to GitHub PR base)"
    required: false
  head-sha:
    description: "Head SHA (defaults to GitHub PR head)"
    required: false
  github-token:
    description: "Token with permissions to post PR comments"
    required: true
runs:
  using: "composite"
  steps:
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ inputs.python-version }}
    - name: Install uv
      shell: bash
      run: pipx install uv
    - name: Install airflow-diff
      shell: bash
      run: pip install airflow-diff==${{ inputs.airflow-diff-version }}
    - name: Run airflow-diff
      shell: bash
      env:
        GH_TOKEN: ${{ inputs.github-token }}
        INPUT_BASE_SHA: ${{ inputs.base-sha }}
        INPUT_HEAD_SHA: ${{ inputs.head-sha }}
      run: ${{ github.action_path }}/entrypoint.sh
    - name: Upload HTML report
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: airflow-diff-report
        path: /tmp/airflow-diff-report.html
        if-no-files-found: ignore
```

- [ ] **Step 20.2: Write `entrypoint.sh`**

`action/entrypoint.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

if [ -z "${GITHUB_EVENT_PATH:-}" ]; then
  echo "::error::GITHUB_EVENT_PATH not set; this action must run in a GitHub Actions context."
  exit 2
fi

EVENT="$GITHUB_EVENT_PATH"

BASE_SHA="${INPUT_BASE_SHA:-}"
HEAD_SHA="${INPUT_HEAD_SHA:-}"
PR_NUMBER="$(jq -r '.pull_request.number // empty' "$EVENT")"
BASE_REPO="$(jq -r '.pull_request.base.repo.full_name // empty' "$EVENT")"
HEAD_REPO="$(jq -r '.pull_request.head.repo.full_name // empty' "$EVENT")"

if [ -z "$PR_NUMBER" ]; then
  echo "::error::This action must run on a pull_request event."
  exit 2
fi

if [ "$BASE_REPO" != "$HEAD_REPO" ]; then
  echo "::warning::Refusing to run on a fork PR (base=$BASE_REPO head=$HEAD_REPO). airflow-diff imports user code and is not safe to run on untrusted forks."
  exit 0
fi

[ -z "$BASE_SHA" ] && BASE_SHA="$(jq -r '.pull_request.base.sha' "$EVENT")"
[ -z "$HEAD_SHA" ] && HEAD_SHA="$(jq -r '.pull_request.head.sha' "$EVENT")"

COMMENT_PATH="$(mktemp -t airflow-diff-comment-XXXXXX.md)"
JSON_PATH="${COMMENT_PATH%.md}.json"
HTML_PATH="/tmp/airflow-diff-report.html"

set +e
airflow-diff diff "$BASE_SHA" "$HEAD_SHA" \
  --repo "$GITHUB_WORKSPACE" \
  --format markdown \
  --out "$COMMENT_PATH" \
  --json-out "$JSON_PATH"
DIFF_EXIT=$?
set -e

if [ ! -s "$COMMENT_PATH" ]; then
  echo "::error::airflow-diff produced no output (exit=$DIFF_EXIT)"
  exit 1
fi

# Always produce the HTML report so the action.yml upload step can include it.
airflow-diff report "$JSON_PATH" --format html --out "$HTML_PATH" || true

# Post (or update) the PR comment
gh pr comment "$PR_NUMBER" --edit-last --body-file "$COMMENT_PATH" \
  || gh pr comment "$PR_NUMBER" --body-file "$COMMENT_PATH"

exit "$DIFF_EXIT"
```

- [ ] **Step 20.3: Make executable**

```bash
chmod +x action/entrypoint.sh
```

- [ ] **Step 20.4: Commit**

```bash
git add action/
git commit -m "feat(action): GitHub Action wrapper (action.yml + entrypoint.sh)"
```

### Task 21: Action smoke test

**Files:**
- Create: `tests/smoke/test_action_entrypoint.sh`
- Create: `tests/smoke/fake_event.json`
- Create: `tests/smoke/fake_event_fork.json`
- Create: `tests/smoke/fake_gh.sh`
- Create: `tests/smoke/fake_airflow_diff.sh`

- [ ] **Step 21.1: Build fakes**

`tests/smoke/fake_event.json`:

```json
{
  "pull_request": {
    "number": 42,
    "base": {"sha": "aaaaaaa", "repo": {"full_name": "acme/repo"}},
    "head": {"sha": "bbbbbbb", "repo": {"full_name": "acme/repo"}}
  }
}
```

`tests/smoke/fake_event_fork.json`:

```json
{
  "pull_request": {
    "number": 42,
    "base": {"sha": "aaaaaaa", "repo": {"full_name": "acme/repo"}},
    "head": {"sha": "bbbbbbb", "repo": {"full_name": "outsider/repo"}}
  }
}
```

`tests/smoke/fake_gh.sh`:

```bash
#!/usr/bin/env bash
echo "[fake gh] $@" >> "${SMOKE_LOG:-/tmp/smoke.log}"
exit 0
```

`tests/smoke/fake_airflow_diff.sh`:

```bash
#!/usr/bin/env bash
# Mimic `airflow-diff diff ... --out PATH`
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) shift; OUT="$1" ;;
  esac
  shift
done
echo "## airflow-diff (stub)" > "$OUT"
exit 0
```

- [ ] **Step 21.2: Smoke test script**

`tests/smoke/test_action_entrypoint.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
TMPDIR="$(mktemp -d)"
SMOKE_LOG="$TMPDIR/log"
PATH_SHIM="$TMPDIR/bin"
mkdir -p "$PATH_SHIM"
cp tests/smoke/fake_gh.sh "$PATH_SHIM/gh"
cp tests/smoke/fake_airflow_diff.sh "$PATH_SHIM/airflow-diff"
chmod +x "$PATH_SHIM/gh" "$PATH_SHIM/airflow-diff"
export PATH="$PATH_SHIM:$PATH"
export GITHUB_EVENT_PATH="$(pwd)/tests/smoke/fake_event.json"
export GITHUB_WORKSPACE="$(pwd)"
export SMOKE_LOG

echo "=== happy path ==="
bash action/entrypoint.sh
grep -q "pr comment 42" "$SMOKE_LOG" || { echo "FAIL: gh not invoked correctly"; cat "$SMOKE_LOG"; exit 1; }
echo "ok"

echo "=== fork PR rejected ==="
export GITHUB_EVENT_PATH="$(pwd)/tests/smoke/fake_event_fork.json"
: > "$SMOKE_LOG"
bash action/entrypoint.sh
if grep -q "pr comment" "$SMOKE_LOG"; then
  echo "FAIL: should not have invoked gh on fork PR"
  cat "$SMOKE_LOG"
  exit 1
fi
echo "ok"

echo "ALL SMOKE TESTS PASSED"
```

- [ ] **Step 21.3: Make executable and run**

```bash
chmod +x tests/smoke/test_action_entrypoint.sh tests/smoke/fake_gh.sh tests/smoke/fake_airflow_diff.sh
bash tests/smoke/test_action_entrypoint.sh
```

Expected: prints "ALL SMOKE TESTS PASSED".

- [ ] **Step 21.4: Commit**

```bash
git add tests/smoke/
git commit -m "test(action): smoke test for entrypoint.sh (happy path + fork rejection)"
```

---

## Phase 10 — CI workflow + README polish

### Task 22: CI workflow and README

**Files:**
- Create: `.github/workflows/test.yml`
- Modify: `README.md`

- [ ] **Step 22.1: Write CI workflow**

`.github/workflows/test.yml`:

```yaml
name: test
on:
  push:
    branches: [main]
  pull_request:

jobs:
  unit:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: ${{ matrix.python-version }} }
      - run: pipx install uv
      - run: uv pip install --system -e ".[dev]"
      - run: pytest tests/unit -v

  integration:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
        airflow-version: ["2.8.4", "2.9.3", "2.10.3"]
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - uses: actions/setup-python@v5
        with: { python-version: ${{ matrix.python-version }} }
      - run: pipx install uv
      - name: Install with pinned Airflow
        run: |
          uv venv .venv
          source .venv/bin/activate
          uv pip install -e .
          uv pip install "apache-airflow==${{ matrix.airflow-version }}" pytest pytest-cov syrupy
      - name: Run integration tests
        run: |
          source .venv/bin/activate
          pytest tests/integration -v -m integration --timeout=600

  smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: bash tests/smoke/test_action_entrypoint.sh
```

- [ ] **Step 22.2: Update README**

Replace `README.md`:

````markdown
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
````

- [ ] **Step 22.3: Commit**

```bash
git add .github/workflows/test.yml README.md
git commit -m "ci: add test matrix workflow; expand README with usage and limitations"
```

---

## Final Verification

- [ ] **Step F.1: Full local test suite**

```bash
uv run pytest tests/ -v
```

Expected: all unit tests pass; integration tests pass (slow — Airflow installs).

- [ ] **Step F.2: Smoke test**

```bash
bash tests/smoke/test_action_entrypoint.sh
```

Expected: "ALL SMOKE TESTS PASSED".

- [ ] **Step F.3: Manual CLI run against the repo's own history**

```bash
# Generate two commits in a scratch repo and run the tool against it
# (or use the sample_repo_builder programmatically). Inspect the markdown
# output by eye to ensure it looks like the spec mockup.
```

- [ ] **Step F.4: Tag v0.1.0**

```bash
git tag v0.1.0
```

(Do NOT push the tag without explicit user approval — publishing to PyPI and GitHub Marketplace is out of scope for this plan.)
