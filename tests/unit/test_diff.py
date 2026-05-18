from datetime import datetime, timezone

from airflow_diff.diff import compute_diff
from airflow_diff.schema import RenderedDagBag, DiffDocument, SCHEMA_VERSION


def _bag(sha: str, dags=()) -> RenderedDagBag:
    return RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha=sha,
        airflow_version="2.10.3",
        rendered_at=datetime(2026, 5, 17, tzinfo=timezone.utc),
        dags=list(dags),
    )


def test_two_empty_bags_produce_empty_diff():
    diff = compute_diff(_bag("a"), _bag("b"), touched_files=[])
    assert isinstance(diff, DiffDocument)
    assert diff.base_sha == "a"
    assert diff.head_sha == "b"
    assert diff.dags == []
    assert diff.render_errors == []
    assert diff.summary.dags_touched == 0


from airflow_diff.schema import RenderedDag


def _ok_dag(dag_id: str, source: str = None) -> RenderedDag:
    return RenderedDag(
        dag_id=dag_id, status="ok",
        source_file=source or f"dags/{dag_id}.py",
        attrs={}, datasets={"inlets": [], "outlets": []},
        task_groups=[], tasks=[],
    )


def test_dag_added_in_head():
    diff = compute_diff(_bag("a"), _bag("b", [_ok_dag("new_dag")]), touched_files=["dags/new_dag.py"])
    assert len(diff.dags) == 1
    d = diff.dags[0]
    assert d.dag_id == "new_dag"
    assert d.classification == "added"
    assert d.status_b == "ok"
    assert d.source_file_after == "dags/new_dag.py"
    assert diff.summary.dags_added == 1


def test_dag_removed_in_head():
    diff = compute_diff(_bag("a", [_ok_dag("gone")]), _bag("b"), touched_files=["dags/gone.py"])
    assert len(diff.dags) == 1
    d = diff.dags[0]
    assert d.dag_id == "gone"
    assert d.classification == "removed"
    assert d.status_a == "ok"
    assert d.source_file_before == "dags/gone.py"
    assert diff.summary.dags_removed == 1


def test_dag_unchanged_not_present_in_diff():
    a = _bag("a", [_ok_dag("same")])
    b = _bag("b", [_ok_dag("same")])
    diff = compute_diff(a, b, touched_files=[])
    assert diff.dags == []


from airflow_diff.schema import RenderError


def _err_dag(dag_id: str) -> RenderedDag:
    return RenderedDag(
        dag_id=dag_id, status="error", source_file=f"dags/{dag_id}.py",
        error=RenderError(type="ImportError", message="boom", traceback="..."),
    )


def test_dag_regressed_ok_then_error():
    diff = compute_diff(_bag("a", [_ok_dag("d")]), _bag("b", [_err_dag("d")]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "regressed"
    assert d.status_a == "ok" and d.status_b == "error"
    assert diff.summary.dags_regressed == 1


def test_dag_fixed_error_then_ok():
    diff = compute_diff(_bag("a", [_err_dag("d")]), _bag("b", [_ok_dag("d")]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "fixed"
    assert diff.summary.dags_fixed == 1


def test_dag_still_broken_error_both_sides():
    a = _err_dag("d")
    # Construct a fresh RenderedDag rather than mutating post-construction (model_validator
    # rejects status="error" with tasks/attrs; similarly rejects inconsistent combos).
    b = _err_dag("d").model_copy(
        update={"error": RenderError(type="ImportError", message="boom v2", traceback="...")}
    )
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "still_broken"
    assert diff.summary.dags_regressed == 0 and diff.summary.dags_fixed == 0
