"""Pure-function diff engine.

Consumes two RenderedDagBag instances and produces a DiffDocument that describes
all structural, attribute, and field-level differences. Knows nothing about
Airflow internals — operates purely on the canonical schema.
"""
from __future__ import annotations

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
