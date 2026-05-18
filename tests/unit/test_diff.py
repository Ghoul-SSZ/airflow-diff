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
