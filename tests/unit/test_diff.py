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
    # model_copy keeps the two _err_dag instances independent so we can vary the error message.
    b = _err_dag("d").model_copy(
        update={"error": RenderError(type="ImportError", message="boom v2", traceback="...")}
    )
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[])
    [d] = diff.dags
    assert d.pair_status == "still_broken"
    assert diff.summary.dags_regressed == 0 and diff.summary.dags_fixed == 0


def test_touched_classification_matches_absolute_source_file_against_relative_touched():
    # In real runs, source_file is an absolute path from Airflow's dag.fileloc
    # (e.g. /tmp/airflow-diff/worktrees/<sha>/dags/d.py), while touched_files
    # comes from `git diff --name-only` as repo-relative (dags/d.py). The
    # classifier must do a path-suffix match, not strict membership.
    a = _ok_dag("d", source="/tmp/airflow-diff/worktrees/abc123/dags/d.py")
    a.attrs = {"schedule": "0 5 * * *"}
    b = _ok_dag("d", source="/tmp/airflow-diff/worktrees/def456/dags/d.py")
    b.attrs = {"schedule": "0 6 * * *"}
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=["dags/d.py"])
    [dd] = diff.dags
    assert dd.classification == "touched"


def test_incidentally_affected_when_dag_path_not_in_touched():
    a = _ok_dag("d", source="/tmp/airflow-diff/worktrees/abc123/dags/d.py")
    a.attrs = {"schedule": "0 5 * * *"}
    b = _ok_dag("d", source="/tmp/airflow-diff/worktrees/def456/dags/d.py")
    b.attrs = {"schedule": "0 6 * * *"}
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=["dags/common/helper.py"])
    [dd] = diff.dags
    assert dd.classification == "incidentally_affected"


def test_attr_diff_schedule_changed():
    a = _ok_dag("d")
    a.attrs = {"schedule": "0 5 * * *", "catchup": False}
    b = _ok_dag("d")
    b.attrs = {"schedule": "0 6 * * *", "catchup": False}
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=["dags/d.py"])
    [dd] = diff.dags
    assert len(dd.attr_diffs) == 1
    assert dd.attr_diffs[0].name == "schedule"
    assert dd.attr_diffs[0].before == "0 5 * * *"
    assert dd.attr_diffs[0].after == "0 6 * * *"


def test_attr_added_and_removed():
    a = _ok_dag("d"); a.attrs = {"tags": ["x"]}
    b = _ok_dag("d"); b.attrs = {"tags": ["x"], "description": "new"}
    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[])
    [dd] = diff.dags
    names = {ad.name for ad in dd.attr_diffs}
    assert "description" in names
    assert "tags" not in names  # unchanged attrs must not appear


from airflow_diff.schema import RenderedTask, RenderedField, ProvenanceEntry


def _task(task_id, *, bash: str = "echo x", upstream=(), downstream=()) -> RenderedTask:
    return RenderedTask(
        task_id=task_id,
        operator="airflow.operators.bash.BashOperator",
        upstream=list(upstream),
        downstream=list(downstream),
        fields={"bash_command": RenderedField(
            rendered=bash, provenance=[ProvenanceEntry(source="literal")],
        )},
    )


def test_task_added():
    a = _ok_dag("d"); a.tasks = [_task("t1")]
    b = _ok_dag("d"); b.tasks = [_task("t1"), _task("t2")]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    by_id = {td.task_id: td for td in dd.task_diffs}
    assert by_id["t2"].change_type == "added"
    assert "t1" not in by_id


def test_task_removed():
    a = _ok_dag("d"); a.tasks = [_task("t1"), _task("t2")]
    b = _ok_dag("d"); b.tasks = [_task("t1")]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    by_id = {td.task_id: td for td in dd.task_diffs}
    assert by_id["t2"].change_type == "removed"


def test_task_field_modified():
    a = _ok_dag("d"); a.tasks = [_task("t1", bash="echo old")]
    b = _ok_dag("d"); b.tasks = [_task("t1", bash="echo new")]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    [td] = dd.task_diffs
    assert td.change_type == "modified"
    [fd] = td.field_diffs
    assert fd.name == "bash_command"
    assert fd.before == "echo old"
    assert fd.after == "echo new"


def test_task_operator_class_changed():
    a = _ok_dag("d"); a.tasks = [_task("t1")]
    b = _ok_dag("d"); b.tasks = [_task("t1")]
    b.tasks[0].operator = "airflow.operators.python.PythonOperator"
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    [td] = dd.task_diffs
    assert td.change_type == "modified"
    assert td.operator_before == "airflow.operators.bash.BashOperator"
    assert td.operator_after == "airflow.operators.python.PythonOperator"


def test_edge_diff_upstream_added():
    a = _ok_dag("d"); a.tasks = [_task("t1"), _task("t2")]
    b = _ok_dag("d"); b.tasks = [_task("t1", downstream=["t2"]), _task("t2", upstream=["t1"])]
    [dd] = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[]).dags
    # The two tasks both changed (edges added):
    by_id = {td.task_id: td for td in dd.task_diffs}
    # t2 has an upstream edge added pointing at t1:
    t2_edges = by_id["t2"].edge_diffs
    assert any(e.direction == "upstream" and e.change_type == "added"
               and e.related_task_id == "t1" for e in t2_edges)


def test_render_errors_populated_for_pair_status_transitions():
    """render_errors should include one entry per regressed/fixed/still_broken DAG."""
    regressed_dag = _ok_dag("dag_regressed")
    regressed_head = _err_dag("dag_regressed")

    fixed_base = _err_dag("dag_fixed")
    fixed_dag = _ok_dag("dag_fixed")

    still_broken_base = _err_dag("dag_still_broken")
    still_broken_head = _err_dag("dag_still_broken").model_copy(
        update={"error": RenderError(type="ValueError", message="different error", traceback="...")}
    )

    diff = compute_diff(
        _bag("x", [regressed_dag, fixed_base, still_broken_base]),
        _bag("y", [regressed_head, fixed_dag, still_broken_head]),
        touched_files=[],
    )

    by_dag = {e.dag_id: e for e in diff.render_errors}
    assert set(by_dag.keys()) == {"dag_regressed", "dag_fixed", "dag_still_broken"}
    assert by_dag["dag_regressed"].side == "head"
    assert by_dag["dag_fixed"].side == "base"
    assert by_dag["dag_still_broken"].side == "both"


def test_render_error_marker_propagated_to_field_diff():
    """When a rendered field contains a <RENDER_ERROR:> marker, FieldDiff should populate
    render_error_after with the extracted error type."""
    a = _ok_dag("d")
    a.tasks = [_task("t1", bash="echo ok")]

    b = _ok_dag("d")
    b.tasks = [RenderedTask(
        task_id="t1",
        operator="airflow.operators.bash.BashOperator",
        upstream=[], downstream=[],
        fields={"bash_command": RenderedField(
            rendered="<RENDER_ERROR: ValueError>",
            provenance=[ProvenanceEntry(source="literal")],
        )},
    )]

    diff = compute_diff(_bag("x", [a]), _bag("y", [b]), touched_files=[])
    [dd] = diff.dags
    [td] = dd.task_diffs
    [fd] = td.field_diffs
    assert fd.render_error_after is not None
    assert fd.render_error_after.type == "ValueError"
    assert fd.render_error_before is None
