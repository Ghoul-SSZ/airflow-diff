import pytest

from airflow_diff.worktree import WorktreeError, resolve_sha, worktree_for


def test_resolve_sha_full(monkeypatch, tmp_path):
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)

        class R:
            returncode = 0
            stdout = "abc1234567890\n"
            stderr = ""

        return R()

    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    full = resolve_sha(tmp_path, "abc1234")
    assert full == "abc1234567890"
    assert calls[0][:3] == ["git", "-C", str(tmp_path)]


def test_resolve_sha_bad_ref(monkeypatch, tmp_path):
    def fake_run(args, **kwargs):
        class R:
            returncode = 1
            stdout = ""
            stderr = "fatal: ambiguous argument"

        return R()

    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    with pytest.raises(WorktreeError, match="resolve"):
        resolve_sha(tmp_path, "bogus")


def test_worktree_for_creates_and_yields_path(monkeypatch, tmp_path):
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)

        class R:
            returncode = 0
            stdout = ""
            stderr = ""

        return R()

    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    with worktree_for(tmp_path, "abc1234567890", root=tmp_path / "wts") as wt:
        assert wt == tmp_path / "wts" / "abc1234567890"
    # First call: worktree add; we don't remove on exit (cache).
    assert any("worktree" in a and "add" in a for a in calls)


def test_worktree_for_reuses_existing(monkeypatch, tmp_path):
    target = tmp_path / "wts" / "abc1234567890"
    target.mkdir(parents=True)
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)

        class R:
            returncode = 0
            stdout = ""
            stderr = ""

        return R()

    monkeypatch.setattr("airflow_diff.worktree._run", fake_run)
    with worktree_for(tmp_path, "abc1234567890", root=tmp_path / "wts") as wt:
        assert wt == target
    # Nothing called because cache hit:
    assert calls == []
