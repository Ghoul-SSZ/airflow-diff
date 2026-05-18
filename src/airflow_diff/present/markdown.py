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
