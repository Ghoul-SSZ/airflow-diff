"""GitHub-flavored markdown presenter for DiffDocument.

Output is a single string suitable for posting as a PR comment. GitHub renders
Mermaid blocks natively, so the diff graph ships as plain markdown.
"""
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
