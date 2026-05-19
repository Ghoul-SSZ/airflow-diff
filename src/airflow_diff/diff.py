"""Pure-function diff engine.

Consumes two RenderedDagBag instances and produces a DiffDocument that describes
all structural, attribute, and field-level differences. Knows nothing about
Airflow internals — operates purely on the canonical schema.
"""

from __future__ import annotations

from typing import Literal

from airflow_diff.schema import (
    SCHEMA_VERSION,
    AttrDiff,
    DagClassification,
    DagDiff,
    DagPairStatus,
    DagStatus,
    DiffDocument,
    DiffSummary,
    EdgeDiff,
    FieldDiff,
    RenderedDag,
    RenderedDagBag,
    RenderedField,
    RenderedTask,
    RenderError,
    RenderErrorEntry,
    TaskDiff,
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
        dag_diffs.append(
            DagDiff(
                dag_id=dag_id,
                classification="added",
                status_a=None,
                status_b=h.status,
                source_file_after=h.source_file,
                error_after=h.error,
            )
        )

    # Removed: in base only
    for dag_id in sorted(base_by_id.keys() - head_by_id.keys()):
        b = base_by_id[dag_id]
        dag_diffs.append(
            DagDiff(
                dag_id=dag_id,
                classification="removed",
                status_a=b.status,
                status_b=None,
                source_file_before=b.source_file,
                error_before=b.error,
            )
        )

    # Compare: in both
    for dag_id in sorted(base_by_id.keys() & head_by_id.keys()):
        b = base_by_id[dag_id]
        h = head_by_id[dag_id]
        dd = _compare_dag(b, h, touched_set)
        if dd is not None:
            dag_diffs.append(dd)

    # Populate render_errors from pair_status transitions
    for dd in dag_diffs:
        if dd.pair_status in ("regressed", "fixed", "still_broken"):
            side: Literal["base", "head", "both"] = (
                "both"
                if dd.pair_status == "still_broken"
                else ("head" if dd.pair_status == "regressed" else "base")
            )
            render_errors.append(
                RenderErrorEntry(
                    dag_id=dd.dag_id,
                    side=side,
                    error_base=dd.error_before,
                    error_head=dd.error_after,
                )
            )

    summary = _summarize(dag_diffs)
    return DiffDocument(
        schema_version=SCHEMA_VERSION,
        base_sha=base.commit_sha,
        head_sha=head.commit_sha,
        summary=summary,
        dags=dag_diffs,
        render_errors=render_errors,
    )


def _is_touched(source_file: str | None, touched: set[str]) -> bool:
    # `source_file` is Airflow's dag.fileloc — absolute in real runs
    # (e.g. /tmp/airflow-diff/worktrees/<sha>/dags/foo.py), repo-relative in
    # synthetic test fixtures. `touched` comes from `git diff --name-only`
    # and is always repo-relative. Match by exact membership first, then by
    # path-suffix so absolute fileloc entries pair up with relative touched
    # paths.
    if source_file is None:
        return False
    if source_file in touched:
        return True
    return any(source_file.endswith("/" + tf) for tf in touched)


def _compare_dag(base: RenderedDag, head: RenderedDag, touched: set[str]) -> DagDiff | None:
    """Returns None if the two DAGs are byte-equivalent and not worth surfacing."""
    if base == head:
        return None
    classification: DagClassification = (
        "touched"
        if (_is_touched(base.source_file, touched) or _is_touched(head.source_file, touched))
        else "incidentally_affected"
    )
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
        status_a=base.status,
        status_b=head.status,
        pair_status=pair_status,
        source_file_before=base.source_file,
        source_file_after=head.source_file,
        attr_diffs=attr_diffs,
        task_diffs=task_diffs,
        error_before=base.error,
        error_after=head.error,
    )


def _pair_status(a: DagStatus, b: DagStatus) -> DagPairStatus:
    if a == "ok" and b == "ok":
        return "ok"
    if a == "ok" and b == "error":
        return "regressed"
    if a == "error" and b == "ok":
        return "fixed"
    return "still_broken"


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
        diffs.append(
            TaskDiff(
                task_id=tid,
                change_type="added",
                operator_after=by_id_b[tid].operator,
            )
        )
    for tid in sorted(by_id_a.keys() - by_id_b.keys()):
        diffs.append(
            TaskDiff(
                task_id=tid,
                change_type="removed",
                operator_before=by_id_a[tid].operator,
            )
        )
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


def _extract_render_error(rendered) -> RenderError | None:
    """If rendered is a <RENDER_ERROR: ...> marker string, return a minimal RenderError."""
    if isinstance(rendered, str) and rendered.startswith("<RENDER_ERROR:"):
        # Extract the error type from the marker, e.g. "<RENDER_ERROR: ValueError>"
        inner = rendered[len("<RENDER_ERROR:") :].strip().rstrip(">").strip()
        return RenderError(
            type=inner,
            message=rendered,
            traceback="(see renderer logs)",
        )
    return None


def _diff_fields(a: dict[str, RenderedField], b: dict[str, RenderedField]) -> list[FieldDiff]:
    out: list[FieldDiff] = []
    for name in sorted(set(a) | set(b)):
        fa = a.get(name)
        fb = b.get(name)
        if fa is None:
            # name came from set(a) | set(b); if fa is None, fb must be present.
            if fb is None:  # pragma: no cover -- impossible by construction
                continue
            out.append(
                FieldDiff(
                    name=name,
                    change_type="added",
                    after=fb.rendered,
                    provenance_after=fb.provenance,
                    render_error_after=_extract_render_error(fb.rendered),
                )
            )
        elif fb is None:
            out.append(
                FieldDiff(
                    name=name,
                    change_type="removed",
                    before=fa.rendered,
                    provenance_before=fa.provenance,
                    render_error_before=_extract_render_error(fa.rendered),
                )
            )
        elif fa != fb:
            out.append(
                FieldDiff(
                    name=name,
                    change_type="modified",
                    before=fa.rendered,
                    after=fb.rendered,
                    provenance_before=fa.provenance,
                    provenance_after=fb.provenance,
                    render_error_before=_extract_render_error(fa.rendered),
                    render_error_after=_extract_render_error(fb.rendered),
                )
            )
    return out


def _diff_edges(a: RenderedTask, b: RenderedTask) -> list[EdgeDiff]:
    edges: list[EdgeDiff] = []
    for direction in ("upstream", "downstream"):
        before = set(getattr(a, direction))
        after = set(getattr(b, direction))
        for related in sorted(after - before):
            edges.append(
                EdgeDiff(
                    direction=direction,
                    change_type="added",
                    task_id=a.task_id,
                    related_task_id=related,
                )
            )
        for related in sorted(before - after):
            edges.append(
                EdgeDiff(
                    direction=direction,
                    change_type="removed",
                    task_id=a.task_id,
                    related_task_id=related,
                )
            )
    return edges
