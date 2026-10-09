"""Paged, prefix-aware job enumeration over the job directories of every state."""

import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import Workspace, _fs
from httk.workflow.introspection import JOB_STATES, _reading, count_jobs, list_jobs

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("concurrency-timing")]


def _job(workspace: Workspace, state: str, placement: str, *, tag: str | None = None) -> str:
    """Create one job directory (a name is all a listing reads) and return its job id."""

    job_id = str(uuid.uuid4())
    job_key = f"{tag}--{job_id}" if tag else job_id
    directory = workspace.jobs.joinpath(state, *placement.split("/"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{job_key}~p500~{_fs.fresh_token()}").mkdir()
    return job_id


def _no_state(monkeypatch: pytest.MonkeyPatch, reads: list[str] | None = None) -> None:
    def fake_state(ref: Any) -> tuple[None, None]:
        if reads is not None:
            reads.append(ref.job_key)
        return None, None

    monkeypatch.setattr(_reading, "read_state", fake_state)


def test_large_count_and_page_are_bounded_and_read_only_the_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, test_profile: Any
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(100_000):
        _job(workspace, JOB_STATES[index % 3], f"p{index % 50:02d}")

    scandir_calls: list[str] = []
    real_scandir = os.scandir

    def spy(*args: Any, **kwargs: Any) -> Any:
        if args:
            scandir_calls.append(str(args[0]))
        return real_scandir(*args, **kwargs)

    monkeypatch.setattr(os, "scandir", spy)
    started = time.monotonic()
    counted = count_jobs(workspace, JOB_STATES[0])
    count_elapsed = time.monotonic() - started
    count_scans = len(scandir_calls)

    reads: list[str] = []
    _no_state(monkeypatch, reads)
    scandir_calls.clear()
    started = time.monotonic()
    page = list_jobs(workspace, limit=50)
    page_elapsed = time.monotonic() - started
    page_scans = len(scandir_calls)

    assert counted == 33_334
    assert len(page.jobs) == 50
    assert len(reads) == 50
    assert count_scans <= 60
    assert page_scans <= 4
    if test_profile.extended:
        assert count_elapsed < 2.0
        assert page_elapsed < 2.0


def test_cursor_paging_crosses_state_boundaries_without_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(100):
        _job(workspace, JOB_STATES[index % 3], f"p{index % 5}")
    _no_state(monkeypatch)

    found: list[tuple[str, str, str]] = []
    cursor = None
    while True:
        page = list_jobs(workspace, limit=7, after=cursor)
        found.extend((str(row["state"]), str(row["placement"]), str(row["job_key"])) for row in page.jobs)
        if page.next_after is None:
            break
        cursor = page.next_after

    assert len(found) == 100
    assert len(set(found)) == 100
    assert found == sorted(found, key=lambda item: (JOB_STATES.index(item[0]), item[1], item[2]))


def test_page_can_end_exactly_at_a_state_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for _ in range(2):
        _job(workspace, "ready", "jobs")
    for _ in range(5):
        _job(workspace, "waiting", "jobs")
    _job(workspace, "paused", "jobs")
    _no_state(monkeypatch)

    first = list_jobs(workspace, limit=7)
    second = list_jobs(workspace, limit=7, after=first.next_after)

    assert {str(row["state"]) for row in first.jobs} == {"ready", "waiting"}
    assert first.next_after is not None and first.next_after.startswith("waiting:")
    assert [row["state"] for row in second.jobs] == ["paused"]


def test_exact_limit_has_no_cursor_without_a_following_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A full page uses a lookahead before advertising another page."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(50):
        _job(workspace, "ready", "jobs", tag=f"job{index:02d}")
    _no_state(monkeypatch)

    page = list_jobs(workspace, kinds=("ready",), limit=50)

    assert len(page.jobs) == 50
    assert page.next_after is None


def test_cursor_state_must_be_selected_and_well_formed(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")

    with pytest.raises(ValueError, match="not among the selected states"):
        list_jobs(workspace, kinds=("ready",), after="waiting:jobs/x")
    with pytest.raises(ValueError, match="job list cursor must be"):
        list_jobs(workspace, kinds=("ready",), after="submitted:jobs/x")
    with pytest.raises(ValueError, match="unknown job state submitted"):
        list_jobs(workspace, kinds=("submitted",))


def test_owned_jobs_are_listed_and_paged_after_the_unowned_states(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _job(workspace, "ready", "jobs")
    owner = "0" * 32
    owned = workspace.jobs / "owned" / owner
    owned.mkdir(parents=True)
    names = sorted(f"{uuid.uuid4()}~p500~{_fs.fresh_token()}~ready" for _ in range(3))
    for name in names:
        (owned / name).mkdir()
    _no_state(monkeypatch)
    monkeypatch.setattr(_reading, "job_placement", lambda ref: None)

    first = list_jobs(workspace, limit=2)
    assert [row["state"] for row in first.jobs] == ["ready", "owned"]
    assert first.jobs[1]["owner_id"] == owner
    assert first.next_after == f"owned:{owner}/{names[0]}"
    second = list_jobs(workspace, limit=2, after=first.next_after)
    assert [row["state"] for row in second.jobs] == ["owned", "owned"] and second.next_after is None
    assert count_jobs(workspace, "owned") == 3


def test_placement_prefix_prunes_unrelated_state_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _job(workspace, "ready", "jobs/child")
    _job(workspace, "ready", "jobs2/child")
    opened: list[str] = []
    real_scandir = os.scandir

    def spy(path: Any) -> Any:
        opened.append(str(path))
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", spy)
    _no_state(monkeypatch)
    page = list_jobs(workspace, kinds=("ready",), placement_prefix="jobs")

    assert len(page.jobs) == 1
    assert all("jobs2" not in path for path in opened)
    assert any(path.endswith(os.path.join("ready", "jobs")) for path in opened)


def test_tag_contains_filters_before_state_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _job(workspace, "ready", "jobs", tag="silicon")
    _job(workspace, "ready", "jobs", tag="aluminium")
    reads: list[str] = []
    _no_state(monkeypatch, reads)

    page = list_jobs(workspace, kinds=("ready",), tag_contains="sil")

    assert len(page.jobs) == 1
    assert page.jobs[0]["job_key"].startswith("silicon--")
    assert reads == [page.jobs[0]["job_key"]]


def test_tag_filter_budget_cursor_does_not_skip_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A partial filtered page resumes after the last examined job."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(9_999):
        _job(workspace, "ready", "jobs", tag=f"a{index:05d}")
    for index in range(51):
        _job(workspace, "ready", "jobs", tag=f"z{index:05d}-needle")
    _no_state(monkeypatch)
    original = _reading._stream_after
    current = 0

    def counted(*args: Any, **kwargs: Any) -> Any:
        nonlocal current
        for ref in original(*args, **kwargs):
            current += 1
            yield ref

    monkeypatch.setattr(_reading, "_stream_after", counted)
    examined: list[int] = []
    first = list_jobs(workspace, kinds=("ready",), limit=50, tag_contains="needle")
    examined.append(current)
    current = 0
    second = list_jobs(workspace, kinds=("ready",), limit=50, tag_contains="needle", after=first.next_after)
    examined.append(current)

    assert len(first.jobs) == 1
    assert len(second.jobs) == 50
    assert len({row["job_id"] for row in first.jobs + second.jobs}) == 51
    assert examined == [10_000, 50]

    current = 0
    exact = list_jobs(workspace, kinds=("ready",), limit=1, tag_contains="needle")
    assert len(exact.jobs) == 1
    assert current == 10_000

    all_matches = list_jobs(workspace, kinds=("ready",), limit=None, tag_contains="needle")
    assert len(all_matches.jobs) == 51
    assert all_matches.next_after is None
