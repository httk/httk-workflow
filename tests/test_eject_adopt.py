"""Ejecting jobs to free-standing directories and adopting them into workspaces."""

import errno
import json
import os
import shutil
import time
import uuid
from pathlib import Path, PurePosixPath

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import TaskManager, Workspace, transfers
from httk.workflow._job_tree import bound_parent, record_spawns, tree_children
from httk.workflow.errors import FormatError, SealedError, WorkspaceCorruptionError
from httk.workflow.models import JobDefinition, Marker, make_job_key
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.seals import is_job_sealed, job_seal_path, seal_job, seal_workspace, verify_job_seal
from httk.workflow.transfers import TRANSFER_DIRECTORY, adopt_job, eject_job, validate_bundle
from httk.workflow.workflow_cli import command

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


_SRC = str(Path(__file__).parents[1] / "src")

# The spawning runner of tests/test_cli_transfer_tree.py: a parent spawns two
# children at tree/children, and with ``grand`` each child one grandchild at
# tree/deeper, which reads its own parent's shared file too.
_TREE_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.eject_tree")


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
    (a.workdir / "shared.txt").write_text("from a child", encoding="utf-8")
    if a.parameter("grand", False):
        a.spawn(ChildSpec(step="child"), label="grandchild", placement="tree/deeper")
    a.succeed()


raise SystemExit(run.main())
'''


def _payload(root: Path, tag: str = "job", *, runner: dict[str, object] | None = None) -> Path:
    """Write one minimal, valid job payload, with its own runner unless *runner* is given."""

    payload = root / tag
    payload.mkdir(parents=True)
    if runner is None:
        (payload / "files").mkdir()
        (payload / "files" / "runner").write_text(_SUCCEED_RUNNER, encoding="utf-8")
        (payload / "files" / "runner").chmod(0o755)
        runner = {"path": "files/runner", "arguments": []}
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": str(uuid.uuid4()),
                "tag": tag,
                "name": f"eject {tag}",
                "workflow": "tests.eject",
                "runner": runner,
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "start",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload


def _pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    configure_identity()
    return Workspace.initialize(tmp_path / "source"), Workspace.initialize(tmp_path / "destination")


def test_eject_then_adopt_moves_a_sealed_job_between_workspaces(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs/a")
    seal_job(source, marker)
    seal_bytes = job_seal_path(source.payload_path(marker.placement, marker.job_key)).read_bytes()

    loose = source.eject(marker.job_id, tmp_path / "loose")
    assert loose == tmp_path / "loose"
    # The workspace keeps nothing that could still schedule or claim the job.
    assert source.find_marker_by_id(marker.job_id) is None
    assert not source.payload_path(marker.placement, marker.job_key).exists()
    assert not list(source.scan_markers(("transferring",)))
    # The free-standing directory is the whole job, still sealed and verifiable on its own.
    manifest = validate_bundle(loose)
    assert manifest["destination_workspace_id"] is None
    assert job_seal_path(loose).read_bytes() == seal_bytes
    assert verify_job_seal(loose).valid

    adopted = destination.adopt(loose)
    assert not loose.exists()
    assert adopted.job_id == marker.job_id and adopted.kind == marker.kind
    assert adopted.placement == marker.placement
    payload = destination.payload_path(adopted.placement, adopted.job_key)
    assert not (payload / TRANSFER_DIRECTORY).exists()
    assert job_seal_path(payload).read_bytes() == seal_bytes
    assert verify_job_seal(payload).valid
    provenance = destination.read_state(adopted)["transfer"]
    assert provenance["source_workspace_id"] == source.workspace_id


def test_an_ejected_job_can_come_home_at_a_new_placement(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs/a")
    loose = source.eject(marker.job_id, tmp_path)
    assert loose == tmp_path / marker.job_key
    adopted = adopt_job(source, loose, placement="elsewhere")
    assert str(adopted.placement) == "elsewhere"
    assert source.payload_path(adopted.placement, adopted.job_key).is_dir()


def test_an_adopted_job_runs_with_the_shared_runner_it_carried(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    runner = tmp_path / "runner.py"
    runner.write_text(_SUCCEED_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    reference = source.publish_runner(runner)
    marker = source.submit(_payload(tmp_path / "payloads", runner=dict(reference)), "jobs")

    destination.adopt(source.eject(marker.job_id, tmp_path / "loose"))
    assert (destination.runners / "runner.py").is_file()
    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    finished = destination.find_marker_by_id(marker.job_id)
    assert finished is not None and finished.kind == "succeeded"


def test_eject_refuses_unusable_destinations_and_keeps_the_job(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    taken = tmp_path / "taken"
    taken.write_text("occupied", encoding="utf-8")
    with pytest.raises(FileExistsError):
        eject_job(source, marker.job_id, taken)
    with pytest.raises(ValueError, match="inside this workspace"):
        eject_job(source, marker.job_id, source.root / "out")
    with pytest.raises(ValueError, match="inside a workspace"):
        eject_job(source, marker.job_id, destination.root / "out")
    with pytest.raises(ValueError, match="parent is not a directory"):
        eject_job(source, marker.job_id, tmp_path / "missing" / "job")
    current = source.find_marker_by_id(marker.job_id)
    assert current is not None and current.kind == marker.kind


def test_a_sealed_workspace_neither_ejects_nor_adopts(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads", "a"), "jobs")
    other = destination.submit(_payload(tmp_path / "payloads", "b"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    seal_job(destination, other)
    seal_workspace(destination)
    with pytest.raises(SealedError):
        destination.adopt(loose)
    assert loose.is_dir()
    with pytest.raises(SealedError):
        destination.eject(other.job_id, tmp_path / "other")
    assert destination.find_marker_by_id(other.job_id) is not None


def test_adopt_refuses_a_stale_copy_and_leaves_it_in_place(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    stale = tmp_path / "stale"
    shutil.copytree(loose, stale, symlinks=True)
    destination.adopt(loose)
    destination.eject(marker.job_id, tmp_path / "again")
    with pytest.raises(ValueError, match="stale copy"):
        destination.adopt(stale)
    assert stale.is_dir()
    assert destination.find_marker_by_id(marker.job_id) is None


def test_adopt_refuses_an_addressed_bundle_and_a_tampered_directory(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    addressed = source.submit(_payload(tmp_path / "payloads", "a"), "jobs")
    bundle = source.detach(addressed.job_id, destination_workspace_id=destination.workspace_id)
    with pytest.raises(ValueError, match="addressed to workspace"):
        destination.adopt(bundle)

    marker = source.submit(_payload(tmp_path / "payloads", "b"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    (loose / "files" / "runner").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    with pytest.raises(FormatError, match="digest mismatch"):
        destination.adopt(loose)
    assert loose.is_dir() and destination.find_marker_by_id(marker.job_id) is None


def test_eject_copies_and_verifies_across_filesystems(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"
    real_rename = os.rename

    def rename(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        # Only the direct bundle move crosses the (simulated) filesystem boundary;
        # the verified staging copy is renamed into place normally.
        if Path(dst) == target and not Path(src).name.startswith(".loose.eject-"):
            raise OSError(errno.EXDEV, "cross-device link")
        real_rename(src, dst)

    monkeypatch.setattr(transfers.os, "rename", rename)
    loose = source.eject(marker.job_id, target)
    assert validate_bundle(loose)["job_id"] == marker.job_id
    assert not source.payload_path(marker.placement, marker.job_key).exists()
    assert not list(tmp_path.glob(".loose.eject-*"))


def test_an_interrupted_ejection_is_finished_by_recovery(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"
    # Fence and seal exactly as eject does, then stop before the move.
    transfers._detach_job(
        source,
        marker.job_id,
        marker=marker,
        destination_workspace_id=None,
        transfer_id=str(uuid.uuid4()),
        eject_to=target,
    )
    assert source.find_marker_by_id(marker.job_id) is None and not target.exists()

    recovered = source.recover_transfers()
    assert [item["status"] for item in recovered if item["status"] == "ejected"] == ["ejected"]
    assert validate_bundle(target)["job_id"] == marker.job_id
    assert not source.payload_path(marker.placement, marker.job_key).exists()
    # A second recovery has nothing left to do.
    assert not [item for item in source.recover_transfers() if item["status"] == "ejected"]


def test_a_finished_copy_left_beside_the_workspace_copy_is_resolved(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"
    transfer_id = str(uuid.uuid4())
    bundle = transfers._detach_job(
        source, marker.job_id, marker=marker, destination_workspace_id=None, transfer_id=transfer_id, eject_to=target
    )
    # The cross-filesystem copy completed, but the workspace copy was not yet removed.
    shutil.copytree(bundle, target, symlinks=True)
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    # Any later ejection first finishes the interrupted one.
    source.eject(other.job_id, tmp_path / "other")
    assert not bundle.exists() and validate_bundle(target)["transfer_id"] == transfer_id


def test_cli_eject_and_adopt(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, destination = _pair(tmp_path)
    first = source.submit(_payload(tmp_path / "payloads", "a"), "jobs")
    second = source.submit(_payload(tmp_path / "payloads", "b"), "jobs")
    seal_job(source, first)
    out = tmp_path / "out"
    context = CLIContext("httk", source.root)

    # Several jobs need an existing directory to land in.
    assert command(["job", "eject", first.job_id, second.job_id, str(tmp_path / "nowhere")], context) == 1
    out.mkdir()
    capsys.readouterr()
    assert command(["job", "eject", first.job_id, second.job_id, str(out)], context) == 0
    lines = [line.split("\t") for line in capsys.readouterr().out.splitlines()]
    assert [(job_id, verb) for job_id, verb, _path in lines] == [(first.job_id, "ejected"), (second.job_id, "ejected")]

    context = CLIContext("httk", destination.root)
    directories = [str(out / first.job_key), str(out / second.job_key)]
    assert command(["job", "adopt", "--placement", "adopted", *directories], context) == 0
    lines = [line.split("\t") for line in capsys.readouterr().out.splitlines()]
    assert [(job_id, verb) for job_id, verb, _kind, _path in lines] == [
        (first.job_id, "adopted"),
        (second.job_id, "adopted"),
    ]
    adopted = destination.find_marker_by_id(first.job_id)
    assert adopted is not None and str(adopted.placement) == "adopted"
    assert is_job_sealed(destination.payload_path(adopted.placement, adopted.job_key))
    assert not list(out.iterdir())


def test_a_bundle_that_left_before_recovery_is_simply_retired(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"
    transfer_id = str(uuid.uuid4())
    bundle = transfers._detach_job(
        source, marker.job_id, marker=marker, destination_workspace_id=None, transfer_id=transfer_id, eject_to=target
    )
    # The move happened, then the process died; the directory was adopted
    # elsewhere before the source ever recovered.
    os.rename(bundle, target)
    destination.adopt(target)
    source.recover_transfers()
    assert not (source.control / "transfers" / f"{transfer_id}.json").exists()
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    assert source.eject(other.job_id, tmp_path / "other").is_dir()


def test_a_stuck_ejection_does_not_block_other_jobs(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    source, _destination = _pair(tmp_path)
    stuck = source.submit(_payload(tmp_path / "payloads", "stuck"), "jobs")
    target = tmp_path / "taken"
    transfers._detach_job(
        source,
        stuck.job_id,
        marker=stuck,
        destination_workspace_id=None,
        transfer_id=str(uuid.uuid4()),
        eject_to=target,
    )
    target.mkdir()  # something else took the destination meanwhile
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    assert source.eject(other.job_id, tmp_path / "other").is_dir()
    assert "cannot finish ejecting job" in caplog.text
    # Freeing the destination lets the next ejection finish the stuck one too.
    target.rmdir()
    third = source.submit(_payload(tmp_path / "payloads", "third"), "jobs")
    source.eject(third.job_id, tmp_path / "third")
    assert validate_bundle(target)["job_id"] == stuck.job_id


def test_an_interrupted_adoption_finishes_even_after_the_job_moved_on(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    # Import as adopt does, then stop before removing the directory ...
    transfers._import_bundle(destination, loose)
    # ... and let a manager run the job, so its current state no longer names the transfer.
    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    finished = destination.find_marker_by_id(marker.job_id)
    assert finished is not None and finished.kind == "succeeded"
    assert "transfer" not in destination.read_state(finished)
    assert destination.adopt(loose).job_id == marker.job_id
    assert not loose.exists()


# ---------------------------------------------------------------------------
# Adoption moves the directory in
# ---------------------------------------------------------------------------


def test_adopt_on_one_filesystem_renames_the_directory_in(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    inode = (loose / "job.json").stat().st_ino
    adopted = destination.adopt(loose)
    assert not loose.exists()
    payload = destination.payload_path(adopted.placement, adopted.job_key)
    assert (payload / "job.json").stat().st_ino == inode
    assert not list((destination.control / "transfers" / "adopting").glob("*"))


def _crossing_tmp(monkeypatch: pytest.MonkeyPatch, workspace: Workspace) -> None:
    """Make the rename of a directory into *workspace*'s staging cross filesystems."""

    real_rename = os.rename

    def rename(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        if Path(dst).parent == workspace.control / "tmp" and Path(dst).name.startswith("import."):
            raise OSError(errno.EXDEV, "cross-device link")
        real_rename(src, dst)

    monkeypatch.setattr(transfers.os, "rename", rename)


def test_adopt_across_filesystems_copies_verifies_and_removes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    inode = (loose / "job.json").stat().st_ino
    _crossing_tmp(monkeypatch, destination)
    adopted = destination.adopt(loose)
    assert not loose.exists()
    payload = destination.payload_path(adopted.placement, adopted.job_key)
    assert (payload / "job.json").stat().st_ino != inode
    assert adopted.job_id == marker.job_id


def test_a_move_that_fails_verification_restores_the_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    real_digest = transfers._payload_digest
    monkeypatch.setattr(
        transfers, "_payload_digest", lambda path: "0" * 64 if path.name.startswith("import.") else real_digest(path)
    )
    with pytest.raises(FormatError, match="digest mismatch"):
        destination.adopt(loose)
    assert validate_bundle(loose)["job_id"] == marker.job_id
    assert destination.find_marker_by_id(marker.job_id) is None
    assert not list((destination.control / "tmp").glob("import.*"))
    assert not list((destination.control / "transfers" / "adopting").glob("*"))
    monkeypatch.setattr(transfers, "_payload_digest", real_digest)
    assert destination.adopt(loose).job_id == marker.job_id


def test_a_moved_job_whose_publication_was_interrupted_is_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")

    def crash(_self: Workspace, _source: Path, _destination: Path) -> None:
        raise RuntimeError("killed")

    monkeypatch.setattr(Workspace, "_publish_path", crash)
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    # The directory is gone and only the workspace staging holds the job now.
    assert not loose.exists() and list((destination.control / "tmp").glob("import.*"))
    destination.recover_transfers()
    adopted = destination.find_marker_by_id(marker.job_id)
    assert adopted is not None and adopted.kind == marker.kind
    assert not list((destination.control / "tmp").glob("import.*"))
    assert not list((destination.control / "transfers" / "adopting").glob("*"))


def test_garbage_collection_keeps_a_staged_adoption(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")

    def crash(_self: Workspace, _source: Path, _destination: Path) -> None:
        raise RuntimeError("killed")

    monkeypatch.setattr(Workspace, "_publish_path", crash)
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    (staged,) = (destination.control / "tmp").glob("import.*")
    old = time.time() - 3 * 24 * 60 * 60
    for entry in [staged, *staged.rglob("*")]:
        os.utime(entry, (old, old), follow_symlinks=False)

    report = destination.collect_garbage(categories=("tmp_entries",))
    assert staged.is_dir() and report.category("tmp_entries").removed == 0
    assert "kept staged adoption" in (report.category("tmp_entries").skip_reason or "")
    destination.recover_transfers()
    adopted = destination.find_marker_by_id(marker.job_id)
    assert adopted is not None and not staged.exists()


# ---------------------------------------------------------------------------
# Job trees leave and come back as one directory
# ---------------------------------------------------------------------------


def _tree(workspace: Workspace, root: Path) -> tuple[Marker, list[Marker], list[Marker]]:
    """Run a three-level tree to completion; return its root, children, and grandchildren."""

    root.mkdir(parents=True)
    (root / "run.py").write_text(_TREE_RUNNER, encoding="utf-8")
    reference = workspace.publish_runner(root / "run.py", name="tree/run.py")
    job = prepare_job_payload(
        root / "parent",
        JobSpec(
            name="Tree",
            workflow="tests.eject_tree",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="parent",
            initial_step="start",
            maximum_attempts_per_activation=1,
            parameters={"grand": True},
        ),
    )
    workspace.submit(root / "parent", "tree/parents")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    parent = workspace.find_marker_by_id(job.id)
    assert parent is not None and parent.kind == "succeeded"

    def at(placement: str) -> list[Marker]:
        return sorted(
            (marker for marker in workspace.scan_markers() if marker.placement.as_posix() == placement),
            key=lambda item: item.job_key,
        )

    children, grandchildren = at("tree/children"), at("tree/deeper")
    assert [marker.kind for marker in [*children, *grandchildren]] == ["succeeded"] * 4
    return parent, children, grandchildren


def _definition(workspace: Workspace, marker: Marker) -> tuple[Path, JobDefinition]:
    payload = workspace.payload_path(marker.placement, marker.job_key)
    return payload, JobDefinition.from_path(payload / "job.json")


def _open_ledgers(workspace: Workspace) -> list[dict[str, object]]:
    return [ledger for ledger in transfers._ledgers(workspace) if ledger.get("status") != "retired"]


def _assert_tree_arrived(workspace: Workspace, everyone: list[Marker]) -> None:
    """Every member is live at its old placement and state, and bound as before."""

    root, *members = everyone
    for original in everyone:
        arrived = workspace.find_marker_by_id(original.job_id)
        assert arrived is not None
        assert (arrived.placement, arrived.kind) == (original.placement, original.kind)
    assert len([marker for marker in workspace.scan_markers()]) == len(everyone)
    children = {marker.job_id for marker, _job in tree_children(workspace, *_definition(workspace, root))}
    assert children == {marker.job_id for marker in members if marker.placement.as_posix() == "tree/children"}
    for grandchild in (marker for marker in members if marker.placement.as_posix() == "tree/deeper"):
        parent = bound_parent(workspace, *_definition(workspace, grandchild))
        assert parent is not None and parent.job_id in children and parent.kind == "succeeded"


def test_a_job_tree_ejects_as_one_directory_and_adopts_back_whole(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    everyone = [root, *children, *grandchildren]
    seal_job(source, root)
    seal_job(source, children[0])
    seals = {
        marker.job_id: job_seal_path(source.payload_path(marker.placement, marker.job_key)).read_bytes()
        for marker in (root, children[0])
    }

    loose = source.eject(root.job_id, tmp_path / "loose")
    nested = loose / TRANSFER_DIRECTORY / "tree" / "tree"
    assert sorted(path.name for path in (nested / "children").iterdir()) == [marker.job_key for marker in children]
    assert sorted(path.name for path in (nested / "deeper").iterdir()) == [marker.job_key for marker in grandchildren]
    assert not list(source.scan_markers()) and not list(source.scan_markers(("transferring",)))
    for marker in everyone:
        assert not source.payload_path(marker.placement, marker.job_key).exists()
    assert not _open_ledgers(source)
    tree = validate_bundle(loose)["eject_tree"]
    assert [entry["job_id"] for entry in tree][:2] == [marker.job_id for marker in children]

    adopted = destination.adopt(loose)
    assert adopted.job_id == root.job_id and not loose.exists()
    _assert_tree_arrived(destination, everyone)
    for job_id, seal in seals.items():
        arrived = destination.find_marker_by_id(job_id)
        assert arrived is not None
        payload = destination.payload_path(arrived.placement, arrived.job_key)
        assert job_seal_path(payload).read_bytes() == seal and verify_job_seal(payload).valid


def test_a_tree_with_a_live_member_is_refused_before_anything_is_fenced(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    root = source.submit(_payload(tmp_path / "payloads", "root"), "jobs")
    # A hand-registered child bound to the root, left schedulable.
    child_payload = _payload(tmp_path / "payloads", "kid")
    definition = json.loads((child_payload / "job.json").read_text(encoding="utf-8"))
    definition["parent"] = {"job_id": root.job_id, "job_key": root.job_key, "placement": "jobs", "spawn_id": None}
    (child_payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    child_key = make_job_key(definition["id"], "kid")
    record_spawns(
        source.payload_path(root.placement, root.job_key),
        str(uuid.uuid4()),
        [{"job_key": child_key, "label": "kid", "placement": "jobs/kids"}],
        durable=False,
    )
    child = source.submit(child_payload, "jobs/kids")
    assert child.kind not in {"paused", "succeeded", "failed"}
    with pytest.raises(ValueError, match=f"its tree cannot leave yet: {child_key} is {child.kind}"):
        source.eject(root.job_id, tmp_path / "loose")
    assert not (tmp_path / "loose").exists() and not transfers._ledgers(source)
    for marker in (root, child):
        current = source.find_marker_by_id(marker.job_id)
        assert current is not None and current.kind == marker.kind


def _fence_root(source: Workspace, root: Marker, target: Path) -> tuple[str, Path, list[dict[str, object]]]:
    """Fence and seal an ejecting tree root exactly as eject does, and stop there."""

    tree = transfers._eject_tree_of(source, root, {})
    transfer_id = str(uuid.uuid4())
    bundle = transfers._detach_job(
        source,
        root.job_id,
        marker=root,
        destination_workspace_id=None,
        transfer_id=transfer_id,
        with_tree=True,
        eject_to=target,
        eject_tree=tree,
    )
    return transfer_id, bundle, tree


@pytest.mark.parametrize("fenced_members", [0, 1])
def test_an_interrupted_tree_ejection_is_finished_by_recovery(tmp_path: Path, fenced_members: int) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    target = tmp_path / "loose"
    transfer_id, bundle, tree = _fence_root(source, root, target)
    for entry in tree[:fenced_members]:
        transfers._detach_job(
            source,
            str(entry["job_id"]),
            destination_workspace_id=None,
            transfer_id=str(entry["transfer_id"]),
            with_tree=True,
            eject_to=transfers._nested_member(bundle, entry),
            eject_root=transfer_id,
        )
    assert len(list(source.scan_markers())) == 4 - fenced_members

    recovered = source.recover_transfers()
    assert [item["transfer_id"] for item in recovered if item["status"] == "ejected"] == [transfer_id]
    assert not list(source.scan_markers()) and not _open_ledgers(source) and not bundle.exists()
    destination.adopt(target)
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_a_tree_root_whose_move_failed_after_gathering_is_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    target = tmp_path / "loose"
    real_move = transfers._move_bundle_out

    def refuse_root(workspace: Workspace, bundle: Path, to: Path, *arguments: object) -> None:
        if to == target:
            raise OSError(errno.EIO, "interrupted")
        real_move(workspace, bundle, to, *arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(transfers, "_move_bundle_out", refuse_root)
    with pytest.raises(OSError, match="interrupted"):
        source.eject(root.job_id, target)
    monkeypatch.undo()
    # Every member is retired inside the root's directory, which is still in the
    # workspace; recovery must not mistake them for bundles of their own.
    assert not list(source.scan_markers())
    source.recover_transfers()
    assert not _open_ledgers(source)
    destination.adopt(target)
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_an_interrupted_tree_adoption_finishes_without_duplicates(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    for entry in validate_bundle(loose)["eject_tree"]:
        transfers._import_bundle(destination, transfers._nested_member(loose, entry), move=True)
    assert len(list(destination.scan_markers())) == 4
    assert destination.adopt(loose).job_id == root.job_id
    assert not loose.exists()
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_cli_ejects_a_tree_once_and_refuses_to_re_place_it(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, destination = _pair(tmp_path)
    root, _children, _grandchildren = _tree(source, tmp_path / "runner")
    context = CLIContext("httk", source.root)
    assert command(["job", "eject", root.job_id, str(tmp_path / "loose")], context) == 0
    lines = [line.split("\t") for line in capsys.readouterr().out.splitlines()]
    assert [(job_id, verb) for job_id, verb, _path in lines] == [(root.job_id, "ejected")]

    other = Workspace.initialize(tmp_path / "other")
    second_root, second_children, _ = _tree(other, tmp_path / "runner-2")
    out = tmp_path / "out"
    out.mkdir()
    context = CLIContext("httk", other.root)
    assert command(["job", "eject", second_root.job_id, second_children[0].job_id, str(out)], context) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"{second_root.job_id}\tejected\t{out / second_root.job_key}",
        f"{second_children[0].job_id}\tejected with its parent",
    ]

    context = CLIContext("httk", destination.root)
    assert command(["job", "adopt", "--placement", "x", str(tmp_path / "loose")], context) == 1
    assert "a job tree keeps its placements; adopt it without --placement" in capsys.readouterr().err
    assert (tmp_path / "loose").is_dir() and not list(destination.scan_markers())


# ---------------------------------------------------------------------------
# Review hardening: crash windows, expired receipts, refusals
# ---------------------------------------------------------------------------


def _crash_import_once(monkeypatch: pytest.MonkeyPatch, workspace: Workspace, window: str) -> None:
    """Kill the next moving import after its marker is published.

    Window ``A`` dies before the envelope is removed, window ``B`` after that
    but before the acknowledgement is written.
    """

    fired: list[bool] = []
    if window == "A":
        real_remove = transfers._remove_tree

        def remove(path: Path) -> None:
            if not fired and path.name == TRANSFER_DIRECTORY and workspace.root in path.parents:
                fired.append(True)
                raise RuntimeError("killed")
            real_remove(path)

        monkeypatch.setattr(transfers, "_remove_tree", remove)
    else:
        real_sign = transfers.sign_document

        def sign(document: dict[str, object]) -> dict[str, object]:
            if not fired:
                fired.append(True)
                raise RuntimeError("killed")
            return real_sign(document)

        monkeypatch.setattr(transfers, "sign_document", sign)


@pytest.mark.parametrize("window", ["A", "B"])
def test_a_single_adoption_killed_after_publication_is_settled_by_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, window: str
) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads", "a"), "jobs")
    other = source.submit(_payload(tmp_path / "payloads", "b"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    spare = source.eject(other.job_id, tmp_path / "spare")
    transfer_id = str(validate_bundle(loose)["transfer_id"])
    _crash_import_once(monkeypatch, destination, window)
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    live = destination.find_marker_by_id(marker.job_id)
    assert live is not None and not transfers._ack_path(destination, transfer_id).exists()
    # Recovery never raises on the leftover envelope, and settles it.
    destination.recover_transfers()
    destination.recover_transfers()
    payload = destination.payload_path(live.placement, live.job_key)
    assert not (payload / TRANSFER_DIRECTORY).exists()
    assert transfers._ack_path(destination, transfer_id).is_file()
    # The workspace goes on adopting normally.
    assert destination.adopt(spare).job_id == other.job_id


def test_a_leftover_envelope_without_an_intent_is_settled_by_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    transfer_id = str(validate_bundle(loose)["transfer_id"])
    _crash_import_once(monkeypatch, destination, "A")
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    transfers._adoption_intent_path(destination, transfer_id).unlink()
    destination.recover_transfers()
    live = destination.find_marker_by_id(marker.job_id)
    assert live is not None
    assert not (destination.payload_path(live.placement, live.job_key) / TRANSFER_DIRECTORY).exists()
    assert transfers._ack_path(destination, transfer_id).is_file()


@pytest.mark.parametrize(("window", "resume"), [("A", "adopt"), ("A", "recover"), ("B", "adopt"), ("B", "recover")])
def test_a_tree_adoption_killed_after_a_member_published_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, window: str, resume: str
) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    _crash_import_once(monkeypatch, destination, window)
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    if resume == "recover":
        destination.recover_transfers()
    destination.adopt(loose)
    assert not loose.exists()
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_an_interrupted_tree_adoption_resumes_after_its_receipts_expired(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    for entry in validate_bundle(loose)["eject_tree"]:
        transfers._import_bundle(destination, transfers._nested_member(loose, entry), move=True)
    old = time.time() - 3 * 24 * 60 * 60
    for name in ("acks", "imported"):
        for entry in (destination.control / "transfers" / name).glob("*.json"):
            os.utime(entry, (old, old))
    destination.collect_garbage(categories=("transfer_records",))
    assert not list((destination.control / "transfers" / "acks").glob("*.json"))
    destination.adopt(loose)
    assert not loose.exists()
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_a_tree_member_cannot_be_adopted_alone(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, _children, _grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    member = transfers._nested_member(loose, validate_bundle(loose)["eject_tree"][0])
    with pytest.raises(ValueError, match="member of an ejected job tree"):
        destination.adopt(member)
    assert member.is_dir() and not list(destination.scan_markers())


def test_retire_refuses_an_ejection_in_progress(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    transfer_id = str(uuid.uuid4())
    bundle = transfers._detach_job(
        source,
        marker.job_id,
        marker=marker,
        destination_workspace_id=None,
        transfer_id=transfer_id,
        eject_to=tmp_path / "loose",
    )
    with pytest.raises(ValueError, match="ejection in progress"):
        transfers.retire_transfers(source, [marker.job_id])
    with pytest.raises(WorkspaceCorruptionError):
        transfers._retire_sealed_bundle(source, transfer_id, provenance={})
    assert validate_bundle(bundle)["job_id"] == marker.job_id


def test_a_tree_colliding_with_the_workspace_is_refused_untouched(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, children, _grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    taken = destination.payload_path(PurePosixPath("tree/children"), children[1].job_key)
    taken.mkdir(parents=True)
    with pytest.raises(FileExistsError, match="is already taken"):
        destination.adopt(loose)
    assert not list(destination.scan_markers())
    for entry in validate_bundle(loose)["eject_tree"]:
        assert validate_bundle(transfers._nested_member(loose, entry))["job_id"] == entry["job_id"]


def test_a_stale_copy_of_a_tree_is_refused_untouched(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, _children, _grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    stale = tmp_path / "stale"
    shutil.copytree(loose, stale, symlinks=True)
    destination.adopt(loose)
    destination.eject(root.job_id, tmp_path / "again")
    with pytest.raises(ValueError, match="stale copy"):
        destination.adopt(stale)
    assert not list(destination.scan_markers())
    assert validate_bundle(stale)["job_id"] == root.job_id
    for entry in validate_bundle(stale)["eject_tree"]:
        assert transfers._nested_member(stale, entry).is_dir()


def test_a_tree_ejected_across_filesystems_carries_verified_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    target = tmp_path / "loose"
    real_rename = os.rename

    def rename(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        if Path(dst) == target and not Path(src).name.startswith(".loose.eject-"):
            raise OSError(errno.EXDEV, "cross-device link")
        real_rename(src, dst)

    monkeypatch.setattr(transfers.os, "rename", rename)
    loose = source.eject(root.job_id, target)
    monkeypatch.undo()
    for entry in validate_bundle(loose)["eject_tree"]:
        assert (
            validate_bundle(transfers._nested_member(loose, entry))["eject_root"]
            == validate_bundle(loose)["transfer_id"]
        )
    assert not list(source.scan_markers()) and not list(tmp_path.glob(".loose.eject-*"))
    destination.adopt(loose)
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_cli_ejects_a_child_named_before_its_root(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, _destination = _pair(tmp_path)
    root, children, _grandchildren = _tree(source, tmp_path / "runner")
    out = tmp_path / "out"
    out.mkdir()
    context = CLIContext("httk", source.root)
    assert command(["job", "eject", children[0].job_id, root.job_id, str(out)], context) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"{root.job_id}\tejected\t{out / root.job_key}",
        f"{children[0].job_id}\tejected with its parent",
    ]


def test_recovery_writes_no_ledger_for_a_half_published_adoption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    transfer_id = str(validate_bundle(loose)["transfer_id"])

    def crash(*_arguments: object) -> None:
        raise RuntimeError("killed")

    monkeypatch.setattr(Workspace, "_verified_marker_rename", crash)
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    # The payload is published with its envelope, but no marker names it yet.
    monkeypatch.setattr(transfers, "_finish_interrupted_adoptions", lambda _workspace: None)
    destination.recover_transfers()
    assert not transfers._ledger_path(destination, transfer_id).exists()
    monkeypatch.undo()
    destination.recover_transfers()
    adopted = destination.find_marker_by_id(marker.job_id)
    assert adopted is not None and adopted.kind == marker.kind
    assert not transfers._ledger_path(destination, transfer_id).exists()


def test_a_copy_of_a_tree_whose_member_moved_on_is_stale(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, _children, _grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    stale = tmp_path / "stale"
    shutil.copytree(loose, stale, symlinks=True)
    destination.adopt(loose)
    # One child leaves on its own; the root is still here by the same transfer.
    leaf = next(marker for marker in destination.scan_markers() if marker.placement.as_posix() == "tree/deeper")
    destination.detach_from_parent(leaf.job_id, operator=None)
    destination.eject(leaf.job_id, tmp_path / "leaf")
    before = sorted(marker.job_id for marker in destination.scan_markers())
    with pytest.raises(ValueError, match="stale copy"):
        destination.adopt(stale)
    assert sorted(marker.job_id for marker in destination.scan_markers()) == before
    for entry in validate_bundle(stale)["eject_tree"]:
        assert transfers._nested_member(stale, entry).is_dir()


def test_cli_ejects_a_selected_child_its_parent_did_not_carry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, _destination = _pair(tmp_path)
    root = source.submit(_payload(tmp_path / "payloads", "root"), "jobs")
    child_payload = _payload(tmp_path / "payloads", "kid")
    definition = json.loads((child_payload / "job.json").read_text(encoding="utf-8"))
    definition["parent"] = {"job_id": root.job_id, "job_key": root.job_key, "placement": "jobs", "spawn_id": None}
    (child_payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    child = source.submit(child_payload, "jobs/kids")
    out = tmp_path / "out"
    out.mkdir()
    context = CLIContext("httk", source.root)
    assert command(["job", "eject", child.job_id, root.job_id, str(out)], context) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"{root.job_id}\tejected\t{out / root.job_key}",
        f"{child.job_id}\tejected\t{out / child.job_key}",
    ]


def test_an_ejection_sealed_before_its_ledger_was_written_is_resumed(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"
    transfer_id = str(uuid.uuid4())
    transfers._detach_job(
        source, marker.job_id, marker=marker, destination_workspace_id=None, transfer_id=transfer_id, eject_to=target
    )
    # The crash window: the marker is in the envelope, but no ledger exists yet.
    transfers._ledger_path(source, transfer_id).unlink()
    source.recover_transfers()
    assert validate_bundle(target)["transfer_id"] == transfer_id
    assert not source.payload_path(marker.placement, marker.job_key).exists()
    assert not _open_ledgers(source)


def test_a_tree_root_sealed_before_its_ledger_was_written_is_resumed(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root, children, grandchildren = _tree(source, tmp_path / "runner")
    target = tmp_path / "loose"
    transfer_id, bundle, _tree_entries = _fence_root(source, root, target)
    transfers._ledger_path(source, transfer_id).unlink()
    source.recover_transfers()
    assert not bundle.exists() and not list(source.scan_markers()) and not _open_ledgers(source)
    destination.adopt(target)
    _assert_tree_arrived(destination, [root, *children, *grandchildren])


def test_adopt_accepts_the_exchange_staging_inbox_and_no_other_workspace_path(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    staging = transfers.exchange_staging(destination)
    for place in (staging / "inbox", staging / "inbox" / "nested", staging, destination.root / "elsewhere"):
        place.mkdir(parents=True, exist_ok=True)
    markers = [source.submit(_payload(tmp_path / "payloads", tag), "jobs") for tag in ("a", "b", "c", "d")]
    refused = [staging / "inbox" / "nested", staging, destination.root / "elsewhere"]
    for marker, place in zip(markers, refused, strict=False):
        moved = place / marker.job_key
        os.rename(source.eject(marker.job_id, tmp_path / marker.job_key), moved)
        with pytest.raises(ValueError, match="already inside the workspace"):
            destination.adopt(moved)
        assert moved.is_dir() and destination.find_marker_by_id(marker.job_id) is None
    staged = staging / "inbox" / "anyname"
    os.rename(source.eject(markers[3].job_id, tmp_path / "staged"), staged)
    assert destination.adopt(staged).job_id == markers[3].job_id and not staged.exists()


def test_eject_accepts_the_exchange_staging_outbox_and_no_other_workspace_path(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    staging = transfers.exchange_staging(source)
    for place in ("inbox", "outbox/rejected"):
        (staging / place).mkdir(parents=True)
    a, b, c = (source.submit(_payload(tmp_path / "payloads", tag), "jobs") for tag in ("a", "b", "c"))
    for target in (staging / "inbox", staging / "outbox" / "rejected", staging / "named"):
        with pytest.raises(ValueError, match="inside this workspace"):
            eject_job(source, c.job_id, target)
    assert source.eject(a.job_id, staging / "outbox") == staging / "outbox" / a.job_key
    assert source.eject(b.job_id, staging / "outbox" / "named") == staging / "outbox" / "named"
    assert validate_bundle(staging / "outbox" / "named")["job_id"] == b.job_id
    assert source.find_marker_by_id(a.job_id) is None and source.find_marker_by_id(c.job_id) is not None


def _loose(tmp_path: Path, tag: str = "job") -> tuple[Workspace, Path]:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads", tag), "jobs")
    return destination, source.eject(marker.job_id, tmp_path / "loose")


@pytest.mark.parametrize("where", ["files", "logs", TRANSFER_DIRECTORY])
def test_adopt_refuses_a_fifo_anywhere_without_opening_it(tmp_path: Path, where: str) -> None:
    destination, loose = _loose(tmp_path)
    (loose / where).mkdir(exist_ok=True)
    os.mkfifo(loose / where / "pipe")
    with pytest.raises(FormatError, match=f"special entry {where}/pipe"):
        destination.adopt(loose)
    assert (loose / where / "pipe").exists() and not list(destination.scan_markers())


@pytest.mark.parametrize(
    ("name", "target", "message"),
    [
        ("absolute", "/etc/passwd", "absolute symlink"),
        ("climbing", "../../outside", "escaping symlink"),
        # Lexically inside (files/back/.. is files), but files/back is the bundle root itself.
        ("composed", "back/..", "resolves outside"),
    ],
)
def test_adopt_refuses_symlinks_leaving_the_directory(tmp_path: Path, name: str, target: str, message: str) -> None:
    destination, loose = _loose(tmp_path)
    (loose / "logs").mkdir(exist_ok=True)
    (loose / "files" / "back").symlink_to("..")
    (loose / "logs" / name).symlink_to(target if name != "composed" else "../files/back/..")
    with pytest.raises(FormatError, match=message):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


def test_adopt_refuses_a_symlinked_job_directory(tmp_path: Path) -> None:
    destination, loose = _loose(tmp_path)
    (tmp_path / "link").symlink_to(loose)
    with pytest.raises(FormatError, match="is not a directory"):
        destination.adopt(tmp_path / "link")
    assert loose.is_dir() and not list(destination.scan_markers())


def test_adopt_accepts_a_contained_relative_symlink(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    # Submission copies a payload's links away, but a run may leave one in its workdir.
    workdir = source.payload_path(marker.placement, marker.job_key) / "run"
    workdir.mkdir()
    (workdir / "alias").symlink_to("../files/runner")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    adopted = destination.adopt(loose)
    arrived = destination.payload_path(adopted.placement, adopted.job_key) / "run" / "alias"
    assert os.readlink(arrived) == "../files/runner"
