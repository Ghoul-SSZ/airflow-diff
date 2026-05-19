"""Version must be single-sourced from airflow_diff.__version__."""

from __future__ import annotations

import re
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import airflow_diff

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_package_exposes_version_string():
    assert isinstance(airflow_diff.__version__, str)
    assert re.match(r"^\d+\.\d+\.\d+(?:[-.][\w.]+)?$", airflow_diff.__version__), (
        f"__version__ {airflow_diff.__version__!r} is not a PEP 440-shaped version"
    )


def test_pyproject_version_is_dynamic():
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    assert "version" in pyproject["project"].get("dynamic", []), (
        "pyproject.toml should declare version as dynamic; remove the static 'version' key"
    )
    assert "version" not in pyproject["project"], (
        "pyproject.toml has a static 'version' key — it must be dynamic and read from "
        "airflow_diff.__version__"
    )


def test_action_default_version_matches_package():
    action_yaml = (REPO_ROOT / "action" / "action.yml").read_text()
    m = re.search(
        r'airflow-diff-version:\s*\n\s+description:.*?\n\s+required:.*?\n\s+default:\s*"([^"]+)"',
        action_yaml,
        re.DOTALL,
    )
    assert m, "action.yml must declare airflow-diff-version input with a default"
    assert m.group(1) == airflow_diff.__version__, (
        f"action.yml default ({m.group(1)}) does not match __version__ ({airflow_diff.__version__}); "
        "the release workflow is responsible for bumping this."
    )
