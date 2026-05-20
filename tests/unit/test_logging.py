"""--verbose / --quiet must configure logging levels deterministically."""

from __future__ import annotations

import logging

import pytest

from airflow_diff.cli import _configure_logging


@pytest.fixture(autouse=True)
def _isolate_airflow_diff_logger():
    """Restore the `airflow_diff` logger's level/handlers/propagate around each test.

    Without this, leftover state (e.g., the final test leaves level=ERROR) leaks
    into other unit tests that exercise code using this logger.
    """
    logger = logging.getLogger("airflow_diff")
    saved_level = logger.level
    saved_handlers = logger.handlers[:]
    saved_propagate = logger.propagate
    yield
    logger.setLevel(saved_level)
    logger.handlers = saved_handlers
    logger.propagate = saved_propagate


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
