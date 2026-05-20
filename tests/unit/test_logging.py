"""--verbose / --quiet must configure logging levels deterministically."""

from __future__ import annotations

import logging

import pytest  # noqa: F401  (kept for parametrize support if added later)

from airflow_diff.cli import _configure_logging


def test_default_logs_warning_and_above():
    _configure_logging(verbose=False, quiet=False)
    assert logging.getLogger("airflow_diff").level == logging.WARNING


def test_verbose_logs_info():
    _configure_logging(verbose=True, quiet=False)
    assert logging.getLogger("airflow_diff").level == logging.INFO


def test_verbose_twice_logs_debug():
    _configure_logging(verbose=2, quiet=False)
    assert logging.getLogger("airflow_diff").level == logging.DEBUG


def test_quiet_logs_error_only():
    _configure_logging(verbose=False, quiet=True)
    assert logging.getLogger("airflow_diff").level == logging.ERROR


def test_quiet_overrides_verbose():
    _configure_logging(verbose=True, quiet=True)
    assert logging.getLogger("airflow_diff").level == logging.ERROR
