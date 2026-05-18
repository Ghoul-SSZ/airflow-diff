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
