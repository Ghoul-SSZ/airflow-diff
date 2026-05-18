"""Config and fixtures loaders.

`.airflow-diff.toml` lives at the repo root; `fixtures_path` points at a YAML
file (default `.airflow-diff/fixtures.yaml`). Both are optional.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dags_folder: str = "dags"
    plugins_folder: str = "plugins"
    fixtures_path: str = ".airflow-diff/fixtures.yaml"
    excluded_files: list[str] = Field(default_factory=list)
    excluded_dag_ids: list[str] = Field(default_factory=list)
    synthetic_logical_date: datetime = Field(
        default_factory=lambda: datetime.fromisoformat("2025-01-01T00:00:00+00:00")
    )
    render_timeout_seconds: int = 300
    max_tasks_for_graph: int = 50
    fail_on_sensor_mismatch: bool = False

    @field_validator("synthetic_logical_date", mode="before")
    @classmethod
    def _parse_dt(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.fromisoformat(v)
        return v


class Fixtures(BaseModel):
    model_config = ConfigDict(extra="forbid")
    variables: dict[str, Any] = Field(default_factory=dict)
    connections: dict[str, dict[str, Any]] = Field(default_factory=dict)


def load_config(repo_root: Path) -> Config:
    toml_path = repo_root / ".airflow-diff.toml"
    if not toml_path.exists():
        return Config()
    raw = tomllib.loads(toml_path.read_text())
    try:
        return Config(**raw)
    except ValidationError as e:
        # Re-raise with a clearer error pointing to the offending key
        raise ValueError(str(e)) from e


def load_fixtures(path: Path) -> Fixtures:
    if not path.exists():
        return Fixtures()
    raw = yaml.safe_load(path.read_text()) or {}
    try:
        return Fixtures(**raw)
    except ValidationError as e:
        raise ValueError(str(e)) from e
