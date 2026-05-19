"""Version must be single-sourced from airflow_diff.__version__."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import airflow_diff

REPO_ROOT = Path(__file__).resolve().parents[2]

# MAJOR.MINOR.PATCH with an optional pre/post/dev suffix. Tighter than full
# PEP 440 — adjust if we ever cut e.g. 0.2.0rc1 or 0.2.0+local builds.
_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-.][\w.]+)?$")


def test_package_exposes_version_string():
    assert isinstance(airflow_diff.__version__, str)
    assert _VERSION_RE.match(airflow_diff.__version__), (
        f"__version__ {airflow_diff.__version__!r} is not a "
        "MAJOR.MINOR.PATCH[-suffix] version string"
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


def test_action_default_version_is_well_formed():
    """The action's default version input must be a valid version string.

    We deliberately do NOT assert equality with `airflow_diff.__version__` here:
    `action.yml`'s default points to the last *published* PyPI release, while
    `__version__` is the version being prepared. They legitimately diverge in
    the window between a release PR landing on main and the release workflow
    bumping `action.yml` post-publish. The release workflow itself enforces
    tag↔__version__ alignment.
    """
    action = yaml.safe_load((REPO_ROOT / "action" / "action.yml").read_text())
    default = action["inputs"]["airflow-diff-version"]["default"]
    assert isinstance(default, str) and _VERSION_RE.match(default), (
        f"action.yml airflow-diff-version default ({default!r}) is not a "
        "MAJOR.MINOR.PATCH[-suffix] version string"
    )
