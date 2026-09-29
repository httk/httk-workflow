"""Ejecting jobs to free-standing directories and adopting them into workspaces."""

import errno
import json
import os
import shutil
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import TaskManager, Workspace, transfers
from httk.workflow.errors import FormatError, SealedError
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
