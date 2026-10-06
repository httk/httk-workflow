"""The empty placement: a job directly below the workspace root, round-tripped everywhere."""

import dataclasses
import uuid
from pathlib import Path, PurePosixPath

import pytest

from conftest import configure_identity
from httk.workflow import TaskManager, Workspace, _confine_rank
from httk.workflow.collecting import JobRecord, record_of
from httk.workflow.errors import FormatError
from httk.workflow.introspection import list_jobs
from httk.workflow.models import payload_relative
from httk.workflow.protocol import (
    JobSpec,
    normalize_placement,
    parse_placement_text,
    placement_text,
    prepare_job_payload,
)
from httk.workflow.seals import seal_job
from test_confine_rank import launch  # noqa: F401  (pytest fixture)
from test_eject_adopt import _pair, _payload
from test_job_paging import _marker

_SRC = str(Path(__file__).parents[1] / "src")

_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.empty_placement")


@run.step
def start(a):
    a.spawn(ChildSpec(step="child"), label="c", placement="")
    a.gather("aggregate")


@run.step
def child(a):
    a.succeed()


@run.step
def aggregate(a):
    (a.workdir / "seen.json").write_text(str(len(a.children["c"].placement.parts)))
    a.succeed()


raise SystemExit(run.main())
'''


def test_text_helpers_round_trip_and_refuse() -> None:
    for text in ("", "."):
        assert normalize_placement(text) == PurePosixPath()
    assert placement_text(PurePosixPath()) == ""
    assert placement_text(normalize_placement("a/b")) == "a/b"
    for text in ("", "a/b"):
        assert placement_text(parse_placement_text(text)) == text
    for bad in ("/abs", "a/../b", ".httk-workspace", "a\x00b"):
        with pytest.raises(FormatError):
            normalize_placement(bad)
    for wrong in (None, 3, PurePosixPath("a")):
        with pytest.raises(FormatError):
            parse_placement_text(wrong)


def test_empty_placement_job_runs_joins_collects_and_pages(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    root = tmp_path / "src"
    root.mkdir()
    (root / "run.py").write_text(_RUNNER, encoding="utf-8")
    reference = workspace.publish_runner(root / "run.py", name="tests/run.py")
    job = prepare_job_payload(
        root / "parent",
        JobSpec(
            name="Parent",
            workflow="tests.empty_placement",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="parent",
            initial_step="start",
            maximum_attempts_per_activation=1,
        ),
    )
    marker = workspace.submit(root / "parent", "")
    assert marker.placement == PurePosixPath()
    assert (workspace.jobs / marker.job_key).is_dir()
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    parent = workspace.find_marker_by_id(job.id)
    assert parent is not None and parent.kind == "succeeded"
    markers = list(workspace.scan_markers())
    assert len(markers) == 2 and all(m.kind == "succeeded" and not m.placement.parts for m in markers)
    for found in markers:
        assert workspace.find_marker_by_id(found.job_id) == found
        assert found.path.parent == workspace.control / "state" / "succeeded"
    payload = workspace.payload_path(parent.placement, parent.job_key)
    assert (payload / "run" / "seen.json").read_text() == "0"

    record = record_of(workspace, parent)
    assert record is not None and record.placement == PurePosixPath()
    mapping = record.as_mapping()
    assert mapping["placement"] == ""
    assert JobRecord.from_mapping(mapping).placement == PurePosixPath()

    page = list_jobs(workspace, limit=1)
    assert [row["placement"] for row in page.jobs] == [""]
    assert page.next_after is not None and "/" not in page.next_after
    second = list_jobs(workspace, limit=1, after=page.next_after)
    assert len(second.jobs) == 1 and second.jobs[0]["job_id"] != page.jobs[0]["job_id"]


def test_paging_cursor_mixes_empty_and_nested_placements(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    ids = {name: _marker(workspace, "ready", name) for name in ("", "a", "a/b", "c")}
    seen: list[str] = []
    cursor = None
    while True:
        page = list_jobs(workspace, kinds=("ready",), limit=1, after=cursor)
        seen += [row["job_id"] for row in page.jobs]
        cursor = page.next_after
        if cursor is None:
            break
    assert sorted(seen) == sorted(ids.values()) and len(seen) == 4
    assert seen[0] == ids[""]


def test_eject_and_adopt_an_empty_placement_job(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "")
    seal_job(source, marker)
    loose = source.eject(marker.job_id, tmp_path / "loose")
    adopted = destination.adopt(loose)
    assert adopted.placement == PurePosixPath() and adopted.job_id == marker.job_id
    assert (destination.jobs / adopted.job_key / "job.json").is_file()
    assert adopted.path.parent == destination.control / "state" / adopted.kind


def test_confine_open_job_with_the_empty_placement(launch, tmp_path: Path) -> None:  # noqa: F811
    key = f"relax--{uuid.uuid4()}"
    directly = launch.workspace / "jobs" / key
    directly.mkdir()
    job, descriptor = _confine_rank.open_job(
        dataclasses.replace(launch.trusted, placement="", job_key=key), launch.workspace
    )
    import os

    os.close(descriptor)
    assert job == directly


def test_an_empty_placement_prefix_means_no_restriction(tmp_path: Path) -> None:
    configure_identity()
    workspace = Workspace.initialize(tmp_path / "ws")
    markers = [workspace.submit(_payload(tmp_path / "payloads", tag), place) for tag, place in (("x", ""), ("y", "b"))]
    with TaskManager(workspace, heartbeat_interval=0.01, placement_prefixes=["", "a"]) as manager:
        assert manager.placement_prefixes == ()
        manager.run_until_idle(timeout=120.0)
    for marker in markers:
        done = workspace.find_marker_by_id(marker.job_id)
        assert done is not None and done.kind == "succeeded"


def test_default_placed_job_lives_at_jobs_key_and_records_carry_jobs_prefix(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    marker = workspace.submit(_payload(tmp_path / "payloads", "x"), "")
    assert workspace.payload_path(marker.placement, marker.job_key) == workspace.root / "jobs" / marker.job_key
    assert (workspace.root / "jobs" / marker.job_key / "job.json").is_file()
    assert not (workspace.root / marker.job_key).exists()
    nested = workspace.submit(_payload(tmp_path / "payloads", "y"), "a/b")
    assert workspace.payload_path(nested.placement, nested.job_key) == workspace.jobs / "a" / "b" / nested.job_key
    assert payload_relative(nested.placement, nested.job_key).as_posix() == f"jobs/a/b/{nested.job_key}"
