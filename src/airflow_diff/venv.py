"""Per-commit venv manager built on top of `uv`.

Cache key is a hash of `requirements.txt` + `pyproject.toml` + `constraints.txt`
(whichever exist). Two commits with identical dep files share a venv.
"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path

DEFAULT_VENV_ROOT = Path.home() / ".cache" / "airflow-diff" / "venvs"
_DEP_FILES = ("requirements.txt", "pyproject.toml", "constraints.txt")
_READY_MARKER = ".airflow-diff-ready"


class VenvError(RuntimeError):
    pass


@dataclass
class _RunResult:
    returncode: int
    stdout: str
    stderr: str


def _run(args: list[str], **kwargs) -> _RunResult:
    res = subprocess.run(args, capture_output=True, text=True, **kwargs)
    return _RunResult(res.returncode, res.stdout, res.stderr)


def _mark_ready_for_test(venv_dir: Path) -> None:
    """Test hook: mark the venv as ready (real path writes the marker after install)."""
    (venv_dir / _READY_MARKER).write_text("ok")


def requirements_hash(worktree_path: Path) -> str:
    h = hashlib.sha256()
    found_any = False
    for name in _DEP_FILES:
        p = worktree_path / name
        if p.exists():
            found_any = True
            h.update(p.read_bytes())
    if not found_any:
        h.update(b"<no-requirements>")
    return h.hexdigest()


def venv_for(worktree_path: Path, *, root: Path = DEFAULT_VENV_ROOT) -> Path:
    """Return the Python interpreter path of a venv built from `worktree_path`'s deps."""
    root.mkdir(parents=True, exist_ok=True)
    key = requirements_hash(worktree_path)
    venv_dir = root / key
    python = venv_dir / "bin" / "python"
    if (venv_dir / _READY_MARKER).exists() and python.exists():
        return python

    # Create the venv
    res = _run(["uv", "venv", str(venv_dir)])
    if res.returncode != 0:
        raise VenvError(f"uv venv failed: {res.stderr.strip()}")

    # Install deps (prefer requirements.txt; otherwise install the project itself)
    req = worktree_path / "requirements.txt"
    if req.exists():
        res = _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "-r",
                str(req),
            ]
        )
    elif (worktree_path / "pyproject.toml").exists():
        res = _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "-e",
                str(worktree_path),
            ]
        )
    else:
        # No deps to install — venv with stdlib is fine
        class R:
            returncode = 0
            stdout = ""
            stderr = ""

        res = R()

    if res.returncode != 0:
        raise VenvError(f"uv pip install failed: {res.stderr.strip()}")

    # Always ensure airflow_diff's own runtime deps are present so the renderer
    # subprocess can import airflow_diff.schema (injected via PYTHONPATH).
    res2 = _run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "pydantic>=2.5",
            "PyYAML>=6.0",
        ]
    )
    if res2.returncode != 0:
        raise VenvError(f"uv pip install (runtime deps) failed: {res2.stderr.strip()}")

    _mark_ready_for_test(venv_dir)  # in real runs this still just touches the marker
    return python
