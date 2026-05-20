# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Coverage reporting in CI with an 80% floor on unit tests.
- Structured logging across the orchestrator, renderer, venv, and worktree modules with `--verbose` / `--quiet` CLI flags.
- `ruff`, `mypy`, and `pre-commit` to the developer workflow, gated in CI.
- Tag-triggered PyPI release workflow via trusted publishing (OIDC).
- Dependabot for `pip` and `github-actions` ecosystems.
- Weekly `pip-audit` workflow.
- Showcase regression CI that re-runs `make_history.sh` per case and diffs against checked-in output.
- macOS coverage for the unit test job.

### Changed
- Renderers for the two commits now run in parallel via a `ThreadPoolExecutor`, roughly halving wall time.
- `airflow_diff.__version__` is now the single source of truth for the package version.

## [0.1.0] - 2026-05-17

### Added
- Initial MVP: CLI `airflow-diff diff` / `report` / `render`, composite GitHub Action, markdown / terminal / HTML presenters.
- Cross-DAG sensor validation (`ExternalTaskSensor` mismatches).
- Showcase with three case studies under `examples/showcase/`.
