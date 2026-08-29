"""Paged, prefix-aware job enumeration."""

import json
import os
import shlex
import time
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote, register_ws
from httk.workflow import Workspace
from httk.workflow.introspection import _reading, count_markers, list_jobs
from httk.workflow.models import STATE_KINDS, marker_basename
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("concurrency-timing")]


def _marker(
    workspace: Workspace,
    kind: str,
    placement: str,
    *,
    tag: str | None = None,
    job_id: str | None = None,
    generation: int = 0,
    record_ref: str = "init",
) -> str:
    """Write one marker-shaped regular file and return its job id."""

    job_id = job_id or str(uuid.uuid4())
    job_key = f"{tag}--{job_id}" if tag else job_id
    directory = workspace.control.joinpath("state", kind, *placement.split("/"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / marker_basename(job_key, 500, generation, record_ref)).touch()
    return job_id


def test_large_count_and_page_are_bounded_and_do_not_read_extra_frames(
    tmp_path: Path, monkeypatch, test_profile
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(100_000):
        kind = STATE_KINDS[index % 3]
        placement = f"p{index % 50:02d}"
        _marker(workspace, kind, placement)

    scandir_calls: list[str] = []
    real_scandir = os.scandir

    def spy(*args, **kwargs):
        if args:
            scandir_calls.append(str(args[0]))
        return real_scandir(*args, **kwargs)

    monkeypatch.setattr(os, "scandir", spy)
    started = time.monotonic()
    counted = count_markers(workspace, STATE_KINDS[0])
    count_elapsed = time.monotonic() - started
    count_scans = len(scandir_calls)

    reads: list[str] = []

    def fake_state(_workspace: Workspace, marker) -> tuple[dict[str, object], None]:
        reads.append(marker.job_key)
        return {}, None

    monkeypatch.setattr(_reading, "_state_of", fake_state)
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


def test_cursor_paging_crosses_kind_boundaries_without_duplicates(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(100):
        _marker(workspace, STATE_KINDS[index % 3], f"p{index % 5}")
    monkeypatch.setattr(_reading, "_state_of", lambda *_: ({}, None))

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
    assert found == sorted(found, key=lambda item: (STATE_KINDS.index(item[0]), item[1], item[2]))


def test_page_can_end_exactly_at_a_kind_boundary(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for _ in range(2):
        _marker(workspace, "submitted", "jobs")
    for _ in range(5):
        _marker(workspace, "ready", "jobs")
    _marker(workspace, "claimed", "jobs")
    monkeypatch.setattr(_reading, "_state_of", lambda *_: ({}, None))

    first = list_jobs(workspace, limit=7)
    second = list_jobs(workspace, limit=7, after=first.next_after)

    assert {str(row["state"]) for row in first.jobs} == {"submitted", "ready"}
    assert first.next_after is not None and first.next_after.startswith("ready:")
    assert [row["state"] for row in second.jobs] == ["claimed"]


def test_exact_limit_has_no_cursor_without_a_following_marker(tmp_path: Path, monkeypatch) -> None:
    """A full page uses a lookahead before advertising another page."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(50):
        _marker(workspace, "ready", "jobs", tag=f"job{index:02d}")
    monkeypatch.setattr(_reading, "_state_of", lambda *_: ({}, None))

    page = list_jobs(workspace, kinds=("ready",), limit=50)

    assert len(page.jobs) == 50
    assert page.next_after is None


def test_cursor_kind_must_be_selected(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    cursor = "submitted:jobs/00000000-0000-0000-0000-000000000000"

    with pytest.raises(ValueError, match="not among the selected kinds"):
        list_jobs(workspace, kinds=("ready",), after=cursor)


def test_duplicate_current_markers_are_reported_and_only_first_is_listed(tmp_path: Path, caplog) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = str(uuid.uuid4())
    _marker(workspace, "ready", "jobs", job_id=job_id, generation=0)
    _marker(workspace, "ready", "jobs", job_id=job_id, generation=1)

    page = list_jobs(workspace, kinds=("ready",))

    assert len(page.jobs) == 1
    assert "duplicate current marker" in caplog.text


def test_counts_include_malformed_marker_shaped_names(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker(workspace, "ready", "jobs")
    malformed = workspace.control / "state" / "ready" / "jobs" / "not-a-job.p500.g0.bad"
    malformed.touch()

    assert count_markers(workspace, "ready", "jobs") == 2


def test_placement_prefix_prunes_unrelated_state_directories(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker(workspace, "ready", "jobs/child")
    _marker(workspace, "ready", "jobs2/child")
    opened: list[str] = []
    real_scandir = os.scandir

    def spy(path):
        opened.append(str(path))
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", spy)
    monkeypatch.setattr(_reading, "_state_of", lambda *_: ({}, None))
    page = list_jobs(workspace, kinds=("ready",), placement_prefix="jobs")

    assert len(page.jobs) == 1
    assert all("jobs2" not in path for path in opened)
    assert any(path.endswith(os.path.join("state", "ready", "jobs")) for path in opened)


def test_tag_contains_filters_before_state_reads(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker(workspace, "ready", "jobs", tag="silicon")
    _marker(workspace, "ready", "jobs", tag="aluminium")
    reads: list[str] = []

    def fake_state(_workspace: Workspace, marker) -> tuple[dict[str, object], None]:
        reads.append(marker.job_key)
        return {}, None

    monkeypatch.setattr(_reading, "_state_of", fake_state)

    page = list_jobs(workspace, kinds=("ready",), tag_contains="sil")

    assert len(page.jobs) == 1
    assert page.jobs[0]["job_key"].startswith("silicon--")
    assert reads == [page.jobs[0]["job_key"]]


def test_tag_filter_budget_cursor_does_not_skip_matches(tmp_path: Path, monkeypatch) -> None:
    """A partial filtered page resumes after the last examined marker."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(9_999):
        _marker(workspace, "ready", "jobs", tag=f"a{index:05d}")
    for index in range(51):
        _marker(workspace, "ready", "jobs", tag=f"z{index:05d}-needle")
    monkeypatch.setattr(_reading, "_state_of", lambda *_: ({}, None))
    original_iter = _reading.iter_markers
    examined: list[int] = []
    current = 0

    def counted_iter(*args, **kwargs):
        for marker in original_iter(*args, **kwargs):
            nonlocal current
            current += 1
            yield marker

    monkeypatch.setattr(_reading, "iter_markers", counted_iter)
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

    monkeypatch.setattr(_reading, "iter_markers", original_iter)
    all_matches = list_jobs(workspace, kinds=("ready",), limit=None, tag_contains="needle")
    assert len(all_matches.jobs) == 51
    assert all_matches.next_after is None


def test_remote_job_list_forwards_every_flag_and_json_is_optional(tmp_path: Path, remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="paged-remote")
    fake_remote(project)
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    job_id = _marker(workspace, "ready", "jobs", tag="remote")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    common = [
        "job",
        "list",
        "--workspace",
        "cluster:station",
        "--kind",
        "ready",
        "--placement",
        "jobs",
        "--after",
        f"ready:jobs/remote--{job_id}",
        "--tag-contains",
        "remote",
        "--counts",
        "--limit",
        "5",
        "--adapter-timeout",
        "17",
    ]
    assert command(common, context) == 0
    human = capsys.readouterr().out
    assert not human.lstrip().startswith("{")
    assert shlex.split(remote.commands()[-1]) == [
        "httk",
        "job",
        "list",
        "--counts",
        "--kind",
        "ready",
        "--placement",
        "jobs",
        "--after",
        f"ready:jobs/remote--{job_id}",
        "--limit",
        "5",
        "--tag-contains",
        "remote",
        "--workspace",
        "station",
    ]

    assert command([*common, "--json"], context) == 0
    document = json.loads(capsys.readouterr().out)

    assert document["format"] == "httk-workflow-job-list"
    assert document["jobs"] == []
    assert document["counts"] == {"ready": 1}
    assert shlex.split(remote.commands()[-1]) == [
        "httk",
        "job",
        "list",
        "--json",
        "--counts",
        "--kind",
        "ready",
        "--placement",
        "jobs",
        "--after",
        f"ready:jobs/remote--{job_id}",
        "--limit",
        "5",
        "--tag-contains",
        "remote",
        "--workspace",
        "station",
    ]


def test_remote_job_detail_commands_relay_and_forward_canonical_ids(tmp_path: Path, remote, capsys) -> None:
    """Remote detail readers use their pinned vectors and require canonical ids."""

    project = tmp_path / "project"
    initialize_project(project, name="paged-remote-details")
    fake_remote(project)
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    job_id = _marker(workspace, "ready", "jobs", tag="remote")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert (
        command(["job", "show", "--workspace", "cluster:station", "--json", "--adapter-timeout", "17", job_id], context)
        == 0
    )
    assert (
        command(
            [
                "job",
                "log",
                "--workspace",
                "cluster:station",
                "--json",
                "--limit",
                "2",
                "--adapter-timeout",
                "17",
                job_id,
            ],
            context,
        )
        == 0
    )
    assert (
        command(["job", "why", "--workspace", "cluster:station", "--json", "--adapter-timeout", "17", job_id], context)
        == 0
    )
    capsys.readouterr()

    commands = remote.commands()
    assert any("httk job show --json" in item and job_id in item for item in commands)
    assert any("httk job log --json --limit 2" in item and job_id in item for item in commands)
    assert any("httk job why --json" in item and job_id in item for item in commands)

    for action in ("show", "log", "why"):
        args = ["job", action, "--workspace", "cluster:station", "remote--" + job_id]
        assert command(args, context) == 2
        assert "canonical job ids" in capsys.readouterr().err
