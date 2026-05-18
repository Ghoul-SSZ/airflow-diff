"""GitHub-flavored markdown presenter for DiffDocument.

Output is a single string suitable for posting as a PR comment. GitHub renders
Mermaid blocks natively, so the diff graph ships as plain markdown.
"""
from __future__ import annotations

from airflow_diff.schema import DagDiff, DiffDocument, FieldDiff, TaskDiff

MAX_TASKS_FOR_GRAPH = 50  # honored from config in orchestrator; constant here for unit-test simplicity
GITHUB_COMMENT_CHAR_LIMIT = 65_536


def render_markdown(doc: DiffDocument, config=None) -> str:
    max_tasks = config.max_tasks_for_graph if config is not None else MAX_TASKS_FOR_GRAPH
    char_limit = config.github_comment_char_limit if (
        config is not None and hasattr(config, "github_comment_char_limit")
    ) else GITHUB_COMMENT_CHAR_LIMIT
    full = _render_internal(doc, max_tasks)
    if len(full) <= char_limit:
        return full
    # Truncate to ~90% of the limit, then append a footer linking to the artifact.
    cutoff = int(char_limit * 0.9)
    truncated = full[:cutoff]
    # Avoid cutting in the middle of a code fence or details block:
    last_safe_newline = truncated.rfind("\n\n")
    if last_safe_newline > 0:
        truncated = truncated[:last_safe_newline]
    footer = (
        "\n\n---\n\n"
        "> ⚠️ **Output truncated** — the full diff exceeds GitHub's PR-comment "
        f"character limit ({char_limit:,}).\n"
        "> The complete HTML report has been uploaded as a workflow artifact "
        "(see the run's Artifacts panel for `airflow-diff-report.html`).\n"
    )
    return truncated + footer


def _render_internal(doc: DiffDocument, max_tasks_for_graph: int = MAX_TASKS_FOR_GRAPH) -> str:
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
        parts.append(_render_dag_section(d, collapsed=False, max_tasks_for_graph=max_tasks_for_graph))
    if incidental:
        parts.append("<details><summary>DAGs incidentally affected (not touched by this PR)</summary>\n")
        for d in incidental:
            parts.append(_render_dag_section(d, collapsed=False, max_tasks_for_graph=max_tasks_for_graph))
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
    still_broken = [d.dag_id for d in doc.dags if d.pair_status == "still_broken"]
    if still_broken:
        msgs.append(f"⚠️ **Still broken (render errors on both sides):** {', '.join(f'`{i}`' for i in still_broken)}")
    if doc.render_errors:
        ids = ", ".join(f"`{e.dag_id}`" for e in doc.render_errors)
        msgs.append(f"⚠️ **Render errors:** {ids}")
    return "\n".join(msgs) + "\n"


def _render_dag_section(d: DagDiff, *, collapsed: bool, max_tasks_for_graph: int = MAX_TASKS_FOR_GRAPH) -> str:
    parts: list[str] = []
    parts.append(f"### `{d.dag_id}` — {_dag_change_summary(d)}")
    graph = _render_mermaid(d, max_tasks_for_graph=max_tasks_for_graph)
    if graph:
        parts.append(graph)
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
    error_notes = []
    if fd.render_error_before:
        error_notes.append(f"render error in base: {fd.render_error_before.type}")
    if fd.render_error_after:
        error_notes.append(f"render error in head: {fd.render_error_after.type}")
    suffix = f" ⚠️ {'; '.join(error_notes)}" if error_notes else ""
    return (
        f"<details><summary>{dag_id}.{task_id}.{fd.name}{suffix}</summary>\n\n"
        f"```diff\n"
        f"- {fd.before}\n"
        f"+ {fd.after}\n"
        f"```\n\n</details>"
    )


def _render_mermaid(d: DagDiff, max_tasks_for_graph: int = MAX_TASKS_FOR_GRAPH) -> str:
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
    if len(nodes) > max_tasks_for_graph:
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
