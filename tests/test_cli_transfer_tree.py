"""Job trees on the command line: whole-tree transfers, refusals, and ``job detach``.

Every tree here is spawned for real: a published runner spawns its children
through :class:`httk.workflow.ChildSpec` and a real :class:`TaskManager` commits
them, so the parent's spawn records exist exactly as they do in production.
"""

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import ensure_identity_key, write_identity_config

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.adapters import add_remote
from httk.workflow.models import Marker
from httk.workflow.projects import PROJECT_DIRECTORY, initialize_project
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._transfer import _require_offers_for_jobs, _transfer_local_to_local

_SRC = str(Path(__file__).parents[1] / "src")

_TREE_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.cli_tree")


@run.step
def start(a):
    (a.workdir / "shared.txt").write_text("from the parent", encoding="utf-8")
    for index in range(2):
        a.spawn(
            ChildSpec(step="child", parameters={{"grand": a.parameter("grand", False)}}),
            label="child-%d" % index,
            placement="tree/children",
        )
    a.succeed()


@run.step
def child(a):
    (a.workdir / "seen.txt").write_text((a.parent.workdir / "shared.txt").read_text(encoding="utf-8"))
    if a.parameter("grand", False):
        a.spawn(ChildSpec(step="child"), label="grandchild", placement="tree/deeper")
    a.succeed()


raise SystemExit(run.main())
'''


def _tree(workspace: Workspace, root: Path, **parameters: object) -> tuple[Marker, list[Marker]]:
    """Run one spawning parent to completion; return it and its two children."""

    root.mkdir(parents=True)
    (root / "run.py").write_text(_TREE_RUNNER, encoding="utf-8")
    reference = workspace.publish_runner(root / "run.py", name="tree/run.py")
    job = prepare_job_payload(
        root / "parent",
        JobSpec(
            name="Tree",
            workflow="tests.cli_tree",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="parent",
            initial_step="start",
            maximum_attempts_per_activation=1,
            parameters=parameters,
        ),
    )
    workspace.submit(root / "parent", "tree/parents")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    parent = workspace.find_marker_by_id(job.id)
    assert parent is not None and parent.kind == "succeeded"
    children = sorted(
        (marker for marker in workspace.scan_markers() if marker.placement.as_posix() == "tree/children"),
        key=lambda item: item.job_key,
    )
    assert [child.kind for child in children] == ["succeeded", "succeeded"]
    return parent, children


def _ids(workspace: Workspace) -> set[str]:
    return {marker.job_id for marker in workspace.scan_markers()}


@pytest.fixture
def local_pair(tmp_path: Path) -> tuple[Workspace, Workspace, CLIContext]:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    context = CLIContext("httk", tmp_path)
    register_ws(context, source.root, "source")
    register_ws(context, destination.root, "destination")
    return source, destination, context


def test_transferring_a_parent_moves_its_whole_tree(
    local_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, destination, context = local_pair
    parent, children = _tree(source, tmp_path / "runner")

    assert command(["job", "transfer", "--json", "--job", parent.job_id, "source", "destination"], context) == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    everyone = {parent.job_id, *(child.job_id for child in children)}
    assert {entry["job_id"] for entry in report["moved"]} == everyone
    assert "skipped" not in report
    for child in children:
        assert f"{child.job_key}: moves with its tree root {parent.job_key}" in captured.err
    assert _ids(source) == set() and _ids(destination) == everyone
    # Placements are kept, so each child still finds its parent at the destination.
    for child in children:
        moved = destination.find_marker_by_id(child.job_id)
        assert moved is not None and moved.placement.as_posix() == "tree/children"


def test_a_bound_child_alone_is_refused_until_it_is_detached(
    local_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, destination, context = local_pair
    parent, children = _tree(source, tmp_path / "runner")
    child = children[0]

    assert command(["job", "transfer", "--job", child.job_id, "source", "destination"], context) != 0
    error = capsys.readouterr().err
    assert f"travels with its parent {parent.job_key}" in error and "httk job detach" in error
    assert _ids(destination) == set()

    assert command(["job", "show", "--workspace", "source", child.job_id], context) == 0
    assert f"detached: no (parent {parent.job_key})" in capsys.readouterr().out

    assert command(["job", "detach", "--workspace", "source", child.job_id], context) == 0
    assert capsys.readouterr().out == f"{child.job_id}\tdetached\n"
    assert command(["job", "detach", "--workspace", "source", child.job_id], context) == 0
    assert capsys.readouterr().out == f"{child.job_id}\talready detached\n"

    assert command(["job", "show", "--workspace", "source", child.job_id], context) == 0
    assert f"detached: yes (parent {parent.job_key})" in capsys.readouterr().out
    assert command(["job", "show", "--json", "--workspace", "source", child.job_id, parent.job_id], context) == 0
    shown = json.loads(capsys.readouterr().out)
    assert [entry["detached"] for entry in shown] == [True, False]
    # A job with no parent has no line for it in the text form.
    assert command(["job", "show", "--workspace", "source", parent.job_id], context) == 0
    assert "detached:" not in capsys.readouterr().out

    assert command(["job", "transfer", "--job", child.job_id, "source", "destination"], context) == 0
    capsys.readouterr()
    assert _ids(destination) == {child.job_id}
    # The parent now brings only the child still bound to it.
    assert command(["job", "transfer", "--job", parent.job_id, "source", "destination"], context) == 0
    capsys.readouterr()
    assert _ids(source) == set()


def test_job_detach_refuses_a_job_without_a_parent(
    local_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, _destination, context = local_pair
    parent, children = _tree(source, tmp_path / "runner")

    assert command(["job", "detach", "--workspace", "source", parent.job_id, children[0].job_id], context) == 1
    captured = capsys.readouterr()
    assert "has no parent to detach from" in captured.err
    assert captured.out == f"{children[0].job_id}\tdetached\n"


def test_destination_placement_is_refused_for_a_tree(
    local_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, destination, context = local_pair
    parent, _children = _tree(source, tmp_path / "runner")

    argv = ["job", "transfer", "--job", parent.job_id, "--destination-placement", "elsewhere", "source", "destination"]
    assert command(argv, context) != 0
    assert "--destination-placement cannot re-place a job tree" in capsys.readouterr().err
    assert _ids(destination) == set() and {marker.kind for marker in source.scan_markers()} == {"succeeded"}


def test_an_offer_accepts_the_members_of_a_requested_tree_only() -> None:
    root, member, stranger = "root-id", "member-id", "stranger-id"
    offers = [
        {"job_id": root, "tree_root": root},
        {"job_id": member, "tree_root": root},
    ]
    _require_offers_for_jobs(offers, [root])
    _require_offers_for_jobs(offers, [root, member])
    _require_offers_for_jobs([{"job_id": member, "tree_root": root}, {"job_id": stranger}], [])
    with pytest.raises(ValueError, match=f"unexpected: {stranger}"):
        _require_offers_for_jobs([*offers, {"job_id": stranger}], [root])
    # A member names its own tree's root, not whatever else was requested.
    with pytest.raises(ValueError, match=f"unexpected: {member}"):
        _require_offers_for_jobs([{"job_id": member, "tree_root": "other-root"}], [root])


# ---------------------------------------------------------------------------
# Over the local remote adapter
# ---------------------------------------------------------------------------


@pytest.fixture
def remote_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Workspace, Workspace, CLIContext]:
    """One local workspace ``home`` and one workspace ``station`` behind a local remote."""

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    write_identity_config(
        {
            "identities": {"local": {"name": "Local User", "email": "local@example.test"}},
            "default_identity": "local",
        }
    )
    ensure_identity_key("local")
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    initialize_project(local_root, name="tree-local")
    initialize_project(remote_root, name="tree-remote")
    local = Workspace.initialize(local_root)
    remote = Workspace.initialize(remote_root)
    add_remote("cluster", template="local", project=local_root)
    metadata_path = local_root / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.setdefault("settings", {})["workspace_root"] = str(remote_root)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    context = CLIContext("httk", local_root)
    register_ws(context, local_root, "home")
    register_ws(context, remote_root, "station", remote="cluster")
    return local, remote, context


@pytest.mark.skipif(shutil.which("httk") is None, reason="the local remote adapter runs the httk command")
def test_fetching_a_parent_from_a_remote_brings_its_tree(
    remote_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    local, remote, context = remote_pair
    parent, children = _tree(remote, tmp_path / "runner")
    child = children[0]

    # A child named alone is refused by the offering side, naming its parent.
    assert command(["job", "transfer", "--job", child.job_id, "cluster:station", "home"], context) != 0
    assert f"travels with its parent {parent.job_key}" in capsys.readouterr().err
    assert _ids(local) == set()

    assert command(["job", "transfer", "--json", "--job", parent.job_id, "cluster:station", "home"], context) == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    everyone = {parent.job_id, *(member.job_id for member in children)}
    assert {entry["job_id"] for entry in report["moved"]} == everyone
    assert f"{child.job_key}: moves with its tree root {parent.job_id}" in captured.err
    assert _ids(local) == everyone and _ids(remote) == set()


@pytest.mark.skipif(shutil.which("httk") is None, reason="the local remote adapter runs the httk command")
def test_pushing_a_parent_to_a_remote_brings_its_tree(
    remote_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    local, remote, context = remote_pair
    parent, children = _tree(local, tmp_path / "runner")

    assert command(["job", "transfer", "--job", children[0].job_id, "home", "cluster:station"], context) != 0
    assert f"travels with its parent {parent.job_key}" in capsys.readouterr().err
    argv = [
        "job",
        "transfer",
        "--job",
        parent.job_id,
        "--destination-placement",
        "elsewhere",
        "home",
        "cluster:station",
    ]
    assert command(argv, context) != 0
    assert "--destination-placement cannot re-place a job tree" in capsys.readouterr().err
    assert _ids(remote) == set()

    assert command(["job", "transfer", "--json", "--job", parent.job_id, "home", "cluster:station"], context) == 0
    report = json.loads(capsys.readouterr().out)
    everyone = {parent.job_id, *(member.job_id for member in children)}
    assert {entry["job_id"] for entry in report["moved"]} == everyone
    assert _ids(remote) == everyone and _ids(local) == set()


def test_a_member_that_fails_to_fence_stays_behind_and_can_follow_later(
    local_pair: tuple[Workspace, Workspace, CLIContext],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source, destination, _context = local_pair
    parent, children = _tree(source, tmp_path / "runner")
    stuck = children[0]
    detach = source.detach

    def racing_detach(job_id: str, **kwargs: Any) -> Path:
        if job_id == stuck.job_id:
            raise ValueError("job was resumed by an operator")
        return detach(job_id, **kwargs)

    monkeypatch.setattr(source, "detach", racing_detach)
    moved = _transfer_local_to_local(source, destination, [parent.job_id])
    assert {entry["job_id"] for entry in moved} == {parent.job_id, children[1].job_id}
    assert f"warning: {stuck.job_key} stays behind: job was resumed by an operator" in capsys.readouterr().err
    assert _ids(source) == {stuck.job_id}

    # Its parent has left, so it is no longer bound and follows on its own.
    monkeypatch.setattr(source, "detach", detach)
    _transfer_local_to_local(source, destination, [stuck.job_id], quiet=True)
    assert _ids(source) == set() and len(_ids(destination)) == 3


def test_a_three_level_tree_moves_whole_on_the_command_line(
    local_pair: tuple[Workspace, Workspace, CLIContext], tmp_path: Path
) -> None:
    source, destination, context = local_pair
    parent, _children = _tree(source, tmp_path / "runner", grand=True)
    everyone = _ids(source)
    assert len(everyone) == 5

    assert command(["job", "transfer", "--json", "--job", parent.job_id, "source", "destination"], context) == 0
    assert _ids(source) == set() and _ids(destination) == everyone
