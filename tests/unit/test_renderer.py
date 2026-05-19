"""Unit tests for renderer helpers (no Airflow install required)."""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch


def _make_airflow_macros_stub():
    """Return a minimal stub for airflow.macros so _build_context doesn't fail."""
    stub = types.ModuleType("airflow.macros")
    return stub


def _call_build_context(logical_date: str = "2025-01-15T00:00:00+00:00") -> dict:
    """Import _build_context with airflow.macros stubbed out, call it, return result."""
    # Stub airflow.macros before importing renderer
    macros_stub = _make_airflow_macros_stub()
    with patch.dict(sys.modules, {"airflow": MagicMock(), "airflow.macros": macros_stub}):
        from airflow_diff import renderer
        dag_mock = MagicMock()
        dag_mock.params = {}
        task_mock = MagicMock()
        return renderer._build_context(dag_mock, task_mock, logical_date)


def test_build_context_contains_ds_nodash():
    ctx = _call_build_context("2025-01-15T00:00:00+00:00")
    assert "ds_nodash" in ctx


def test_build_context_contains_ts_nodash():
    ctx = _call_build_context("2025-01-15T00:00:00+00:00")
    assert "ts_nodash" in ctx


def test_ds_nodash_has_no_dashes():
    ctx = _call_build_context("2025-01-15T00:00:00+00:00")
    assert "-" not in ctx["ds_nodash"]


def test_ts_nodash_has_no_dashes_or_colons():
    ctx = _call_build_context("2025-01-15T00:00:00+00:00")
    assert "-" not in ctx["ts_nodash"]
    assert ":" not in ctx["ts_nodash"]


def test_ds_nodash_value():
    ctx = _call_build_context("2025-01-15T00:00:00+00:00")
    assert ctx["ds_nodash"] == "20250115"


def test_ds_nodash_consistent_with_ds():
    ctx = _call_build_context("2025-03-07T12:00:00+00:00")
    expected = ctx["ds"].replace("-", "")
    assert ctx["ds_nodash"] == expected
