"""Job trees: spawn records, binding, whole-tree transfer, and detachment."""

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow import transfers as transfers_module
from httk.workflow._job_tree import (
    bound_parent,
    record_spawns,
    spawned_children,
    tree_children,
    tree_directory,
)
from httk.workflow.errors import WorkspaceCorruptionError
from httk.workflow.models import Marker
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.transfers import offer_transfers, select_transfer_jobs
from httk.workflow.workflow_cli._transfer import _require_offers_for_jobs

_SRC = str(Path(__file__).parents[1] / "src")

_TREE_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.tree")


@run.step
def start(a):
    for index in range(a.parameter("children", 2)):
        a.spawn(
            ChildSpec(
                step="child",
                parameters={{"depth": a.parameter("depth", 1)}},
                claim_pool=a.parameter("child_pool", "default"),
            ),
            label="child-%d" % index,
            placement="tree/children",
        )
    if a.parameter("gather", True):
        a.gather("done", when="all_terminal")
    else:
        a.succeed()


@run.step
def child(a):
    depth = a.parameter("depth", 1)
    if depth > 1:
        a.spawn(ChildSpec(step="child", parameters={{"depth": depth - 1}}), label="grandchild", placement="tree/deeper")
        a.gather("done", when="all_terminal")
    else:
        a.succeed()


@run.step
def done(a):
    a.succeed()


raise SystemExit(run.main())
'''


def _tree(workspace: Workspace, root: Path, **parameters: object) -> tuple[Marker, list[Marker]]:
    """Run one spawning parent under a real manager; return it and its children."""

    root.mkdir(parents=True)
    (root / "run.py").write_text(_TREE_RUNNER, encoding="utf-8")
    reference = workspace.publish_runner(root / "run.py", name="tree/run.py")
    payload = root / "parent"
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Tree",
            workflow="tests.tree",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="parent",
            initial_step="start",
            maximum_attempts_per_activation=1,
            parameters=parameters,
        ),
    )
    workspace.submit(payload, "tree/parents")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    parent = workspace.find_marker_by_id(job.id)
    assert parent is not None
    children = sorted(
        (marker for marker in workspace.scan_markers() if marker.job_id != job.id), key=lambda item: item.job_key
    )
    return parent, children


def _payload(workspace: Workspace, marker: Marker) -> Path:
    return workspace.payload_path(marker.placement, marker.job_key)


def test_a_spawning_commit_records_its_children_before_they_exist(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent, children = _tree(workspace, tmp_path / "source")
    assert parent.kind == "succeeded" and [child.kind for child in children] == ["succeeded", "succeeded"]
    recorded = spawned_children(_payload(workspace, parent))
    assert sorted(str(entry["job_key"]) for entry in recorded) == [child.job_key for child in children]
    assert {entry["placement"] for entry in recorded} == {"tree/children"}
    # One immutable fragment per spawning outcome, inside the payload-private area.
    assert len(list((tree_directory(_payload(workspace, parent)) / "spawns").glob("*.json"))) == 1


def test_a_replayed_spawn_record_must_be_identical(tmp_path: Path) -> None:
    key = "child--01234567-89ab-cdef-0123-456789abcdef"
    entry = {"job_id": key[-36:], "job_key": key, "label": "l", "placement": "p", "spawn_id": "s", "workspace_id": "w"}
    record_spawns(tmp_path, "attempt", [entry], durable=True)
    record_spawns(tmp_path, "attempt", [entry], durable=True)
    assert spawned_children(tmp_path) == [{name: entry[name] for name in entry if name != "workspace_id"}]
    fragment = next((tree_directory(tmp_path) / "spawns").glob("*.json"))
    fragment.write_bytes(b'{"children": [')  # a torn non-durable write is replaced
    record_spawns(tmp_path, "attempt", [entry], durable=False)
    assert len(spawned_children(tmp_path)) == 1
    with pytest.raises(WorkspaceCorruptionError, match="disagrees"):
        record_spawns(tmp_path, "attempt", [{**entry, "label": "other"}], durable=False)


def test_a_bound_child_cannot_leave_without_its_parent(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    parent, children = _tree(source, tmp_path / "runner")
    child = children[0]
    assert bound_parent(source, _payload(source, child), source.load_job(child)) == parent

    with pytest.raises(ValueError, match=f"travels with its parent {parent.job_key}"):
        offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[child.job_id])
    with pytest.raises(ValueError, match="travels with its parent"):
        source.detach(child.job_id, destination_workspace_id=destination.workspace_id)
    # Nor does the parent leave its children behind unless it moves as a tree.
    with pytest.raises(ValueError, match="2 child job.s. that travel with it"):
        source.detach(parent.job_id, destination_workspace_id=destination.workspace_id)
    assert {marker.kind for marker in source.scan_markers()} == {"succeeded"}


def test_selecting_a_parent_selects_its_whole_tree_root_first(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    parent, children = _tree(source, tmp_path / "runner")

    # A bulk fetch of finished work matches every job, and yields one tree.
    candidates = select_transfer_jobs(source, destination_workspace_id=destination.workspace_id)
    assert candidates[0].job_id == parent.job_id
    assert {candidate.job_id for candidate in candidates[1:]} == {child.job_id for child in children}
    assert all(candidate.tree_root == parent.job_id for candidate in candidates)
    assert all(candidate.tree_parent == parent.job_id for candidate in candidates[1:])

    offers = offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[parent.job_id])
    assert {offer["job_id"] for offer in offers} == {parent.job_id, *(child.job_id for child in children)}
    for offer in offers:
        destination.import_bundle(str(offer["bundle_path"]))
    # Placements are preserved, so the children find their parent again there.
    arrived = destination.find_marker_by_id(children[0].job_id)
    assert arrived is not None
    assert bound_parent(destination, _payload(destination, arrived), destination.load_job(arrived)) is not None


def test_rerunning_an_interrupted_tree_transfer_brings_the_members_left_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    parent, children = _tree(source, tmp_path / "runner", depth=2)
    real_detach = transfers_module.detach_job

    def interrupted(workspace: Workspace, job_id: str, **keywords: Any) -> Path:
        if job_id != parent.job_id:
            raise ValueError("interrupted")
        return real_detach(workspace, job_id, **keywords)

    monkeypatch.setattr(transfers_module, "detach_job", interrupted)
    with pytest.raises(ValueError, match="could not all be sealed"):
        offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[parent.job_id])
    monkeypatch.undo()
    # The root is sealed and gone from the state tree; its children stayed.
    assert source.find_marker_by_id(parent.job_id) is None
    assert {child.job_id for child in children} <= {marker.job_id for marker in source.scan_markers()}

    offers = offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[parent.job_id])
    assert {offer["job_id"] for offer in offers} == {parent.job_id, *(child.job_id for child in children)}
    # A fetching workspace accepts the members because each offer names its root.
    _require_offers_for_jobs(offers, [parent.job_id])


def test_a_tree_with_a_claimable_member_is_blocked_whole(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    # The children wait in a pool no manager serves, so they stay claimable.
    parent, children = _tree(source, tmp_path / "runner", gather=False, child_pool="elsewhere")
    assert parent.kind == "succeeded" and {child.kind for child in children} <= {"submitted", "ready"}

    assert offer_transfers(source, destination_workspace_id=destination.workspace_id) == []
    with pytest.raises(ValueError, match="its tree cannot leave yet"):
        offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[parent.job_id])
    assert source.find_marker_by_id(parent.job_id) == parent


def test_a_detached_child_leaves_alone_and_its_parent_without_it(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    parent, children = _tree(source, tmp_path / "runner", children=1)
    child = children[0]

    assert source.detach_from_parent(child.job_id, operator="tester") is True
    assert source.detach_from_parent(child.job_id) is False
    detached = json.loads((tree_directory(_payload(source, child)) / "detached.json").read_text(encoding="utf-8"))
    assert detached["operator"] == "tester"
    assert bound_parent(source, _payload(source, child), source.load_job(child)) is None
    assert tree_children(source, _payload(source, parent), source.load_job(parent)) == []

    offers = offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[child.job_id])
    assert [offer["job_id"] for offer in offers] == [child.job_id]
    offers = offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[parent.job_id])
    assert [offer["job_id"] for offer in offers] == [parent.job_id]


def test_detach_refuses_a_root_job(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent, _children = _tree(workspace, tmp_path / "runner", children=1)
    with pytest.raises(ValueError, match="has no parent"):
        workspace.detach_from_parent(parent.job_id)


def test_a_spawn_record_naming_an_unrelated_job_is_ignored(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent, children = _tree(workspace, tmp_path / "runner", children=1)
    stranger, _ = _tree(workspace, tmp_path / "other", children=0)
    record_spawns(
        _payload(workspace, parent),
        "forged",
        [
            {
                "job_id": stranger.job_id,
                "job_key": stranger.job_key,
                "label": "x",
                "placement": stranger.placement.as_posix(),
                "spawn_id": "not-a-spawn",
            }
        ],
        durable=False,
    )
    assert [
        marker for marker, _job in tree_children(workspace, _payload(workspace, parent), workspace.load_job(parent))
    ] == children


def test_a_child_whose_parent_is_gone_is_free(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _parent, children = _tree(workspace, tmp_path / "runner", children=1)
    job = workspace.load_job(children[0])
    assert job.parent is not None
    orphan = {**job.parent, "placement": "gone"}
    assert bound_parent(workspace, _payload(workspace, children[0]), replace(job, parent=orphan)) is None


def test_a_three_level_tree_moves_whole(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    parent, descendants = _tree(source, tmp_path / "runner", depth=2)
    assert len(descendants) == 4 and {marker.kind for marker in descendants} == {"succeeded"}

    candidates = select_transfer_jobs(source, destination_workspace_id=destination.workspace_id)
    assert candidates[0].job_id == parent.job_id
    assert sorted(candidate.job_id for candidate in candidates) == sorted(
        [parent.job_id, *(marker.job_id for marker in descendants)]
    )
    assert not any(candidate.tree_blocked for candidate in candidates)
    # A grandchild is fenced after its own parent, which is fenced after the root.
    position = {candidate.job_id: index for index, candidate in enumerate(candidates)}
    assert all(position[str(candidate.tree_parent)] < position[candidate.job_id] for candidate in candidates[1:])

    offers = offer_transfers(source, destination_workspace_id=destination.workspace_id, job_ids=[parent.job_id])
    assert len(offers) == 5
    assert all(offer.get("tree_root") == parent.job_id for offer in offers)
    assert list(source.scan_markers()) == []


def test_a_member_outside_the_scan_is_selected_once(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    parent, descendants = _tree(source, tmp_path / "runner", children=1, depth=2)
    leaf = next(marker for marker in descendants if marker.placement.as_posix() == "tree/deeper")
    candidates = select_transfer_jobs(
        source, destination_workspace_id=destination.workspace_id, known_markers=[parent, leaf]
    )
    ids = [candidate.job_id for candidate in candidates]
    assert len(ids) == len(set(ids)) == 3 and not any(candidate.tree_blocked for candidate in candidates)
