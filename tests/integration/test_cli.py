import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

FIXTURES_ROOT = Path(__file__).parent.parent / "fixtures"


def test_end_to_end_diff_emits_markdown(tmp_path):
    from tests.fixtures.sample_repo_builder import build
    repo = tmp_path / "repo"
    base_sha, head_sha = build(repo, FIXTURES_ROOT, "apache-airflow==2.10.3\n")
    out = tmp_path / "comment.md"
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff", "diff",
         base_sha, head_sha, "--repo", str(repo),
         "--out", str(out)],
        capture_output=True, text=True, check=False,
    )
    assert res.returncode == 0, f"stderr={res.stderr}"
    text = out.read_text()
    assert "linear" in text
    assert "echo end" in text
    assert "echo finished" in text
