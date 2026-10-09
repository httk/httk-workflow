"""The empty placement: a job directly below its state directory, round-tripped everywhere."""

import dataclasses
import json
import os
from pathlib import Path, PurePosixPath

import pytest

from conftest import configure_identity
from httk.workflow import TaskManager, _confine_rank, _kernel
from httk.workflow.collecting import JobRecord, record_of
from httk.workflow.errors import FormatError
from httk.workflow.introspection import list_jobs
from httk.workflow.protocol import normalize_placement, parse_placement_text, placement_text
from test_confine_rank import launch  # noqa: F401  (pytest fixture)
from test_manager_scheduling import _SPAWNING, _SUCCEED, _install, _submit
from v3_helpers import find, job_mapping, submit_mapping
from v3_helpers import workspace as initialize_workspace


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
    workspace = initialize_workspace(tmp_path / "ws")
    installed = _install(workspace, tmp_path / "package", 'CHILD_PLACEMENT = ""\n' + _SPAWNING)
    parent = _submit(workspace, installed, tag="parent", placement="")
    submitted = find(workspace, parent)
    assert submitted.placement == PurePosixPath()
    assert submitted.path.parent == workspace.jobs / "ready"
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    done = list(_kernel.list_jobs(workspace, "succeeded"))
    assert len(done) == 2 and all(ref.placement == PurePosixPath() for ref in done)
    for ref in done:
        assert find(workspace, ref.job_id) == ref
        assert ref.path.parent == workspace.jobs / "succeeded"
    (aggregated,) = [ref for ref in done if ref.job_id == parent]
    assert json.loads((aggregated.path / "run" / "seen.json").read_text()) == [""]

    record = record_of(workspace, aggregated)
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
    workspace = initialize_workspace(tmp_path / "ws")
    # The tag of the root job sorts before the placement directories beside it.
    ids = {
        placement: submit_mapping(
            workspace,
            job_mapping(("local:missing", "missing"), {}, tag="0root" if not placement else "job", placement=placement),
        ).job_id
        for placement in ("", "a", "a/b", "c")
    }
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


def test_confine_open_job_with_the_empty_placement(launch, tmp_path: Path) -> None:  # noqa: F811
    # A confined launch finds its job among its owner's jobs; the empty placement passes the placement check.
    job, descriptor = _confine_rank.open_job(
        dataclasses.replace(launch.trusted, placement=""), launch.workspace, launch.owner_id
    )
    os.close(descriptor)
    assert job == launch.job


def test_an_empty_placement_prefix_means_no_restriction(tmp_path: Path) -> None:
    configure_identity()
    workspace = initialize_workspace(tmp_path / "ws")
    installed = _install(workspace, tmp_path / "package", _SUCCEED)
    jobs = [_submit(workspace, installed, tag=tag, placement=place) for tag, place in (("x", ""), ("y", "b"))]
    with TaskManager(workspace, heartbeat_interval=0.01, placement_prefixes=["", "a"]) as manager:
        assert manager.placement_prefixes == ()
        manager.run_until_idle(timeout=120.0)
    for job_id in jobs:
        assert find(workspace, job_id).state == "succeeded"


def test_default_placed_job_lives_at_jobs_key_and_records_carry_jobs_prefix(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "ws")
    root = submit_mapping(workspace, job_mapping(("local:missing", "missing"), {}, tag="x", placement=""))
    assert root.path.parent == workspace.jobs / "ready"
    assert (root.path / "job.json").is_file()
    assert not (workspace.root / root.job_key).exists()
    nested = submit_mapping(workspace, job_mapping(("local:missing", "missing"), {}, tag="y", placement="a/b"))
    assert nested.path.parent == workspace.jobs / "ready" / "a" / "b"
    assert nested.placement == PurePosixPath("a/b")
