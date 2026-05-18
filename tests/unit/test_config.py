from datetime import datetime
from pathlib import Path

import pytest

from airflow_diff.config import Config, Fixtures, load_config, load_fixtures

FIXTURES = Path(__file__).parent.parent / "fixtures" / "config"


def test_defaults_when_no_file(tmp_path: Path):
    cfg = load_config(tmp_path)  # no .airflow-diff.toml present
    assert cfg.dags_folder == "dags"
    assert cfg.plugins_folder == "plugins"
    assert cfg.fixtures_path == ".airflow-diff/fixtures.yaml"
    assert cfg.excluded_files == []
    assert cfg.excluded_dag_ids == []
    assert cfg.render_timeout_seconds == 300
    assert cfg.max_tasks_for_graph == 50
    assert cfg.synthetic_logical_date == datetime.fromisoformat("2025-01-01T00:00:00+00:00")


def test_loads_from_toml(tmp_path: Path):
    (tmp_path / ".airflow-diff.toml").write_text((FIXTURES / "full.toml").read_text())
    cfg = load_config(tmp_path)
    assert cfg.dags_folder == "my_dags"
    assert cfg.plugins_folder == "my_plugins"
    assert cfg.excluded_files == ["legacy/*.py"]
    assert cfg.excluded_dag_ids == ["sandbox_*"]
    assert cfg.render_timeout_seconds == 600
    assert cfg.max_tasks_for_graph == 100


def test_rejects_unknown_keys(tmp_path: Path):
    (tmp_path / ".airflow-diff.toml").write_text("bogus_key = 1\n")
    with pytest.raises(ValueError, match="bogus_key"):
        load_config(tmp_path)


def test_load_fixtures_missing_returns_empty(tmp_path: Path):
    fixtures = load_fixtures(tmp_path / "missing.yaml")
    assert fixtures == Fixtures()


def test_load_fixtures_parses_yaml(tmp_path: Path):
    p = tmp_path / "fix.yaml"
    p.write_text((FIXTURES / "full_fixtures.yaml").read_text())
    fix = load_fixtures(p)
    assert fix.variables == {"bucket": "prod-bucket", "region": "us-east-1"}
    assert fix.connections["warehouse"]["host"] == "wh.example.com"


def test_load_fixtures_bad_yaml_raises(tmp_path: Path):
    p = tmp_path / "fix.yaml"
    p.write_text("variables: [not a dict]\n")
    with pytest.raises(ValueError):
        load_fixtures(p)
