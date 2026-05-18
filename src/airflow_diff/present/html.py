"""Standalone HTML presenter for cases where the markdown comment is too large.

Reuses the markdown presenter under the hood, then wraps the result in a
self-contained HTML document with Mermaid + GitHub-ish CSS for parity with how
the comment would render in a PR.
"""
from __future__ import annotations

import html
import re

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


def render_html(doc: DiffDocument, config=None) -> str:
    md = render_markdown(doc, config=config)
    rendered = _markdown_to_html(md)
    return _TEMPLATE.format(md_escaped=html.escape(md), rendered=rendered)


def _markdown_to_html(md: str) -> str:
    """A *very* minimal markdown-to-HTML conversion. Sufficient for our output
    shape (headers, tables, code fences with diff/mermaid, details blocks).
    We pin our markdown shape, so we don't need a full parser.
    """
    out = []
    i = 0
    lines = md.splitlines()
    while i < len(lines):
        line = lines[i]
        if line.startswith("> "):
            # Collect consecutive blockquote lines
            j = i
            bq_lines = []
            while j < len(lines) and lines[j].startswith("> "):
                bq_lines.append(html.escape(lines[j][2:]))
                j += 1
            out.append("<blockquote>" + "<br>".join(bq_lines) + "</blockquote>")
            i = j
            continue
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
