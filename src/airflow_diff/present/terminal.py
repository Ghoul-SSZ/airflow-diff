"""ANSI-colored text presenter."""
from __future__ import annotations

from airflow_diff.schema import DiffDocument, DagDiff, FieldDiff, TaskDiff

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
RESET = "\033[0m"


def render_terminal(doc: DiffDocument, config=None) -> str:
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
