import json
import subprocess
import sys
from pathlib import Path

import pytest

from airflow_diff.schema import RenderedDagBag

pytestmark = pytest.mark.integration

FIXTURES_ROOT = Path(__file__).parent.parent / "fixtures"


def _run_renderer(worktree: Path) -> RenderedDagBag:
    """Invoke the renderer as a subprocess with the current Python interpreter."""
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff.renderer",
         "--worktree", str(worktree),
         "--commit-sha", "test_sha",
         "--config", "{}"],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        raise AssertionError(f"renderer exit={res.returncode} stderr={res.stderr}")
    return RenderedDagBag.model_validate_json(res.stdout)


def test_renders_linear_dag(tmp_path: Path):
    # Set up worktree with our fixture
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "linear.py").write_text(
        (FIXTURES_ROOT / "dags_base" / "linear.py").read_text()
    )
    bag = _run_renderer(tmp_path)
    assert bag.commit_sha == "test_sha"
    [dag] = bag.dags
    assert dag.dag_id == "linear"
    assert dag.status == "ok"
    assert dag.attrs["schedule"] == "@daily"
    assert {t.task_id for t in dag.tasks} == {"start", "middle", "end"}
    end_task = next(t for t in dag.tasks if t.task_id == "end")
    assert end_task.upstream == ["middle"]
    assert end_task.fields["bash_command"].rendered == "echo end"
    assert end_task.fields["bash_command"].provenance[0].source == "literal"


def _setup_worktree(tmp_path: Path, fixture_files: list, include_plugins: bool = False) -> Path:
    (tmp_path / "dags").mkdir()
    for f in fixture_files:
        src = FIXTURES_ROOT / "dags_base" / f
        (tmp_path / "dags" / f).write_text(src.read_text())
    if include_plugins:
        (tmp_path / "plugins").mkdir()
        (tmp_path / "plugins" / "operators.py").write_text(
            (FIXTURES_ROOT / "plugins" / "operators.py").read_text()
        )
    return tmp_path


def test_renders_templated_dag(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["templated.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "templated"]
    copy = next(t for t in dag.tasks if t.task_id == "copy_bucket")
    assert copy.fields["bash_command"].rendered == "aws s3 cp s3://<VAR:bucket>/2025-01-01 /tmp/in"
    sources = {p.source for p in copy.fields["bash_command"].provenance}
    assert "stub" in sources


def test_renders_task_groups(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["task_groups.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "task_groups"]
    groups = {g.group_id for g in dag.task_groups}
    assert "transform" in groups
    clean = next(t for t in dag.tasks if t.task_id == "transform.clean")
    assert clean.task_group == "transform"


def test_renders_custom_operator(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["custom_op.py"], include_plugins=True)
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "custom_op"]
    hello = dag.tasks[0]
    assert hello.operator.endswith("GreetingOperator")
    assert hello.fields["name"].rendered == "<VAR:user>"


def test_renders_xcom_stub(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["xcom.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "xcom"]
    down = next(t for t in dag.tasks if t.task_id == "downstream")
    assert "<XCOM:upstream.return_value>" in down.fields["bash_command"].rendered


def test_renders_dataset_dag(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["datasets.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "dataset_dag"]
    assert "s3://bucket/output" in dag.datasets.outlets


def test_renders_factory_produces_multiple_dags(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["factory.py"])
    bag = _run_renderer(wt)
    ids = {d.dag_id for d in bag.dags if d.dag_id.startswith("factory_")}
    assert ids == {"factory_alpha", "factory_beta", "factory_gamma"}


def test_broken_import_captured_as_error(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["broken_import.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "broken_import"]
    assert dag.status == "error"
    assert "this_module_does_not_exist" in dag.error.message


def test_broken_init_captured_as_error(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["broken_init.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "broken_init"]
    assert dag.status == "error"


def test_field_render_error_recorded(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["missing_macro.py"])
    bag = _run_renderer(wt)
    [dag] = [d for d in bag.dags if d.dag_id == "missing_macro"]
    assert dag.status == "ok"  # The DAG itself imported fine
    t = dag.tasks[0]
    assert "RENDER_ERROR" in str(t.fields["bash_command"].rendered)


def test_fixtures_override_variable(tmp_path: Path):
    wt = _setup_worktree(tmp_path, ["templated.py"])
    fixtures_yaml = tmp_path / "fixtures.yaml"
    fixtures_yaml.write_text("variables:\n  bucket: real-bucket\n")
    res = subprocess.run(
        [sys.executable, "-m", "airflow_diff.renderer",
         "--worktree", str(wt), "--commit-sha", "x",
         "--config", "{}",
         "--fixtures", str(fixtures_yaml)],
        capture_output=True, text=True, check=True,
    )
    bag = RenderedDagBag.model_validate_json(res.stdout)
    [dag] = [d for d in bag.dags if d.dag_id == "templated"]
    copy = next(t for t in dag.tasks if t.task_id == "copy_bucket")
    assert "real-bucket" in copy.fields["bash_command"].rendered
    assert "<VAR:bucket>" not in copy.fields["bash_command"].rendered


def test_renderer_rejects_airflow_3_via_mock(tmp_path: Path, monkeypatch):
    """We can't actually install Airflow 3 in the test env; test the version check directly."""
    from airflow_diff.renderer import _airflow_version_ok
    import airflow
    real = airflow.__version__
    airflow.__version__ = "3.0.0"
    try:
        ok, v = _airflow_version_ok()
        assert ok is False
        assert v == "3.0.0"
    finally:
        airflow.__version__ = real
