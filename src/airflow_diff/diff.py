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
