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

    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return int(e.code) if e.code is not None else 2

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
