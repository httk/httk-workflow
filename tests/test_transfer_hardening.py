"""What a detached transfer bundle pins, refuses, and recovers from."""

import ast
import json
import os
import uuid
from pathlib import Path

import pytest

from httk.workflow import FormatError, Workspace
from httk.workflow import transfers as transfers_module
from httk.workflow.transfers import (
    TRANSFER_DIRECTORY,
    TRANSFER_FORMAT_VERSION,
    TRANSFER_MANIFEST,
    _payload_digest,
    offer_transfers,
    validate_bundle,
)


def _payload(root: Path, *, tag: str = "test") -> tuple[Path, str]:
    """Write one minimal, valid payload and return it with its job UUID."""

    job_id = str(uuid.uuid4())
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 1,
                "id": job_id,
                "tag": tag,
                "name": tag,
                "workflow": "tests",
                "runner": {"path": "files/runner", "arguments": []},
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
    return payload, job_id


def _pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    source = Workspace.initialize(tmp_path / "source", extensions=["detached-transfer-v1"])
    destination = Workspace.initialize(tmp_path / "destination", extensions=["detached-transfer-v1"])
    return source, destination


# ---------------------------------------------------------------------------
# What the payload digest pins
# ---------------------------------------------------------------------------


def test_the_payload_digest_pins_the_executable_bit(tmp_path: Path) -> None:
    payload, _ = _payload(tmp_path)
    executable = _payload_digest(payload)
    (payload / "files" / "runner").chmod(0o644)
    plain = _payload_digest(payload)
    # A runner that arrives without its executable bit does not run, so losing
    # the bit has to be a digest mismatch rather than a silent success.
    assert plain != executable
    (payload / "files" / "runner").chmod(0o755)
    assert _payload_digest(payload) == executable


def test_the_payload_digest_pins_the_target_of_a_symlink(tmp_path: Path) -> None:
    payload, _ = _payload(tmp_path)
    (payload / "run").mkdir()
    link = payload / "run" / "inputs"
    link.symlink_to("../files")
    first = _payload_digest(payload)
    link.unlink()
    link.symlink_to("../job.json")
    # The link is hashed as its literal target, exactly as a signed project
    # manifest records one, so retargeting it moves the digest.
    assert _payload_digest(payload) != first
    link.unlink()
    link.symlink_to("../files")
    assert _payload_digest(payload) == first


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("/etc/passwd", "absolute symlink"),
        ("../../elsewhere", "escaping symlink"),
        ("../files/../../elsewhere", "escaping symlink"),
    ],
)
def test_a_symlink_that_leaves_the_payload_is_refused_by_name(tmp_path: Path, target: str, message: str) -> None:
    payload, _ = _payload(tmp_path)
    (payload / "files" / "outside").symlink_to(target)
    with pytest.raises(FormatError, match=message):
        _payload_digest(payload)


def test_a_bundle_sealed_under_the_previous_digest_rule_is_refused_by_version(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest_path = bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["format_version"] == TRANSFER_FORMAT_VERSION == 2
    manifest["format_version"] = 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    # An older bundle is named as an old bundle rather than reported as the
    # digest mismatch it would otherwise look like.
    with pytest.raises(FormatError, match="version 1 is not 2"):
        validate_bundle(bundle)


# ---------------------------------------------------------------------------
# What survives a transfer
# ---------------------------------------------------------------------------


def test_a_v1_style_relative_symlink_transfers_and_survives_the_import(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    marker = source.submit(payload, "jobs")
    live = source.payload_path(marker.placement, marker.job_key)
    # Exactly the shape the v1 compatibility backend plants in a live payload.
    legacy = live / "ht.task"
    legacy.mkdir()
    (legacy / "inputs").symlink_to("../files")

    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = json.loads((bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST).read_text(encoding="utf-8"))
    destination.import_bundle(bundle)

    imported = destination.find_marker_by_id(job_id)
    assert imported is not None
    arrived = destination.payload_path(imported.placement, imported.job_key)
    link = arrived / "ht.task" / "inputs"
    assert link.is_symlink() and os.readlink(link) == "../files"
    assert (link / "runner").is_file()
    assert (arrived / "files" / "runner").stat().st_mode & 0o111
    assert _payload_digest(arrived) == manifest["payload_sha256"]


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


def test_a_detach_interrupted_before_sealing_is_completed_by_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")

    def interrupt(*arguments: object, **keywords: object) -> Path:
        raise RuntimeError("simulated interruption between the transition and the seal")

    monkeypatch.setattr(transfers_module, "_seal_transferring", interrupt)
    with pytest.raises(RuntimeError):
        source.detach(job_id, destination_workspace_id=destination.workspace_id)
    # The job is fenced: it is no longer schedulable, and the only marker left
    # is the transferring one no bundle has been sealed around yet.
    assert source.find_marker_by_id(job_id) is None
    fenced = [marker for marker in source.scan_markers(("transferring",)) if marker.job_id == job_id]
    assert len(fenced) == 1
    monkeypatch.undo()

    recovered = source.recover_transfers()
    assert len(recovered) == 1 and recovered[0]["status"] == "sealed"
    bundle = Path(str(recovered[0]["bundle"]))
    assert validate_bundle(bundle)["job_id"] == job_id
    assert destination.import_bundle(bundle)["job_id"] == job_id


def test_a_sealed_but_unsent_bundle_is_offered_again(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = validate_bundle(bundle)

    # A crash between the seal and the copy loses nothing: the sealed bundle is
    # what a later offer reports, under the transfer UUID it was sealed with.
    offers = offer_transfers(
        source,
        destination_workspace_id=destination.workspace_id,
        states=("submitted",),
    )
    assert len(offers) == 1
    assert offers[0]["transfer_id"] == manifest["transfer_id"]
    assert offers[0]["bundle_path"] == str(bundle)
    assert offers[0]["job_id"] == job_id

    # Even a lost ledger is rebuilt from the bundle the workspace still holds.
    ledger = source.control / "transfers" / f"{manifest['transfer_id']}.json"
    ledger.unlink()
    assert offer_transfers(source, destination_workspace_id=destination.workspace_id, states=("submitted",)) == offers


def test_a_source_bundle_already_moved_aside_is_retired_without_a_second_move(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    acknowledgement = destination.import_bundle(bundle)
    transfer_id = str(acknowledgement["transfer_id"])

    # Simulate a crash between the rename and the ledger write.
    retired = source.control / "transfers" / "retired" / transfer_id / "bundle"
    retired.parent.mkdir(parents=True, exist_ok=True)
    os.rename(bundle, retired)
    ledger = source.control / "transfers" / f"{transfer_id}.json"
    assert json.loads(ledger.read_text(encoding="utf-8"))["status"] == "sealed"

    assert source.acknowledge_transfer(acknowledgement) == retired
    assert json.loads(ledger.read_text(encoding="utf-8"))["status"] == "retired"
    assert (retired / TRANSFER_DIRECTORY / TRANSFER_MANIFEST).is_file()


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_every_transfer_ledger_write_is_durability_aware() -> None:
    """No write of the transfer protocol may quietly ignore workspace durability."""

    tree = ast.parse(Path(str(transfers_module.__file__)).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "write_json_atomic"
    ]
    assert calls
    for call in calls:
        assert any(keyword.arg == "durable" for keyword in call.keywords), ast.unparse(call)
