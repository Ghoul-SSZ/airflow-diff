"""Re-run each showcase case and assert the rendered markdown matches the checked-in output.

Marked `integration` because it imports Airflow indirectly via the renderer subprocess.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SHOWCASE = REPO_ROOT / "examples" / "showcase"
EXPECTED_DIR = REPO_ROOT / "docs" / "showcase"


def _strip_generated_marker(text: str) -> str:
    """Strip any leading `<!-- ... -->` comment lines plus surrounding whitespace."""
    lines = text.splitlines()
    while lines and lines[0].startswith("<!--"):
        lines = lines[1:]
    return "\n".join(lines).strip()


@pytest.mark.integration
@pytest.mark.parametrize("case", ["case-1", "case-2", "case-3"])
def test_showcase_output_matches(case: str):
    result = subprocess.run(
        ["bash", str(SHOWCASE / "make_history.sh"), case, "--run"],
        capture_output=True,
        text=True,
        cwd=str(SHOWCASE),
    )
    # case-1 produces a regression (exit 1); make_history.sh wraps with `|| true`,
    # so the script itself should exit 0 regardless.
    assert result.returncode == 0, (
        f"make_history.sh {case} failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    marker = "--- airflow-diff output ---"
    assert marker in result.stdout, f"missing marker in output for {case}"
    actual = result.stdout.split(marker, 1)[1].strip()

    expected = _strip_generated_marker((EXPECTED_DIR / f"{case}-output.md").read_text())

    assert actual == expected, (
        f"showcase output for {case} drifted.\n"
        f"--- expected (docs/showcase/{case}-output.md, generated-marker stripped) ---\n{expected}\n"
        f"--- actual ---\n{actual}\n"
    )
