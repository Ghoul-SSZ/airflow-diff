import hashlib
from pathlib import Path

import pytest

from airflow_diff.venv import VenvError, requirements_hash, venv_for


def test_requirements_hash_uses_requirements_txt(tmp_path):
    (tmp_path / "requirements.txt").write_text("apache-airflow==2.10.3\n")
    h = requirements_hash(tmp_path)
    expected = hashlib.sha256(b"apache-airflow==2.10.3\n").hexdigest()
    assert h == expected


def test_requirements_hash_includes_pyproject(tmp_path):
    (tmp_path / "requirements.txt").write_text("a==1\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    h1 = requirements_hash(tmp_path)
    (tmp_path / "pyproject.toml").write_text("[project]\nname='y'\n")
    h2 = requirements_hash(tmp_path)
    assert h1 != h2


def test_requirements_hash_no_files_uses_marker(tmp_path):
    h = requirements_hash(tmp_path)
    assert h == hashlib.sha256(b"<no-requirements>").hexdigest()


def test_venv_for_creates_when_missing(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.venv._run", fake_run)
    (tmp_path / "requirements.txt").write_text("a==1\n")
    cache = tmp_path / "cache"

    def fake_mark_ready(p: Path) -> None:
        # Simulate uv finishing
        (p / "bin").mkdir(parents=True, exist_ok=True)
        (p / "bin" / "python").write_text("#!/bin/sh\n")
        (p / ".airflow-diff-ready").write_text("ok")
    monkeypatch.setattr("airflow_diff.venv._mark_ready_for_test", fake_mark_ready)

    py = venv_for(tmp_path, root=cache)
    assert py.name == "python"
    assert any("uv" in " ".join(a) for a in calls)


def test_venv_for_reuses_when_ready(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, **kwargs):
        calls.append(args)
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr("airflow_diff.venv._run", fake_run)
    (tmp_path / "requirements.txt").write_text("a==1\n")
    cache = tmp_path / "cache"
    h = requirements_hash(tmp_path)
    venv_dir = cache / h
    (venv_dir / "bin").mkdir(parents=True)
    (venv_dir / "bin" / "python").write_text("#!/bin/sh\n")
    (venv_dir / ".airflow-diff-ready").write_text("ok")
    py = venv_for(tmp_path, root=cache)
    assert py == venv_dir / "bin" / "python"
    assert calls == []  # cache hit


def test_uv_failure_raises(monkeypatch, tmp_path):
    def fake_run(args, **kwargs):
        class R: returncode = 1; stdout = ""; stderr = "pip install failed"
        return R()
    monkeypatch.setattr("airflow_diff.venv._run", fake_run)
    (tmp_path / "requirements.txt").write_text("nonexistent==1\n")
    with pytest.raises(VenvError, match="failed"):
        venv_for(tmp_path, root=tmp_path / "cache")
