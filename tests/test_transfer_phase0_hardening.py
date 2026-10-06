"""Phase 0 transfer hardening: bundle fields are validated before they become paths."""

import json
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from conftest import configure_identity
from httk.workflow import Workspace, transfers
from httk.workflow.errors import FormatError
from httk.workflow.hygiene import _check_tmp_leftovers
from httk.workflow.models import JobDefinition, make_job_key
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.transfers import TRANSFER_DIRECTORY, TRANSFER_MANIFEST, validate_bundle
from test_eject_adopt import _payload, _tree


def _pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    configure_identity()
    return Workspace.initialize(tmp_path / "source"), Workspace.initialize(tmp_path / "destination")


def _loose(tmp_path: Path) -> tuple[Workspace, Workspace, Path]:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    return source, destination, source.eject(marker.job_id, tmp_path / "loose")


def _addressed(tmp_path: Path) -> tuple[Workspace, Path]:
    """Return a destination and an addressed bundle staged in its incoming directory."""

    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    incoming = destination.control / "transfers" / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    staged = incoming / "bundle"
    shutil.copytree(bundle, staged, symlinks=True)
    return destination, staged


def _edit_manifest(bundle: Path, **changes: Any) -> None:
    path = bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.update(changes)
    path.write_text(json.dumps(manifest), encoding="utf-8")


def _snapshot(root: Path) -> list[str]:
    """List what sits directly beside the workspaces, where an escaped payload would land."""

    return sorted(path.name for path in root.iterdir())


# ---------------------------------------------------------------------------
# 1. job_key
# ---------------------------------------------------------------------------


def test_adopt_refuses_a_traversal_job_key(tmp_path: Path) -> None:
    _source, destination, loose = _loose(tmp_path)
    _edit_manifest(loose, job_key="../../escaped")
    before = _snapshot(tmp_path)
    with pytest.raises(FormatError, match="job key"):
        destination.adopt(loose)
    assert _snapshot(tmp_path) == before
    assert not (tmp_path / "escaped").exists()
    assert (loose / TRANSFER_DIRECTORY / TRANSFER_MANIFEST).is_file()


def test_import_bundle_refuses_a_traversal_job_key(tmp_path: Path) -> None:
    destination, bundle = _addressed(tmp_path)
    _edit_manifest(bundle, job_key="../../escaped")
    before = _snapshot(tmp_path)
    with pytest.raises(FormatError, match="job key"):
        destination.import_bundle(bundle)
    assert _snapshot(tmp_path) == before
    assert not list(destination.scan_markers())


def test_validate_bundle_refuses_a_job_key_of_another_job(tmp_path: Path) -> None:
    _source, destination, loose = _loose(tmp_path)
    _edit_manifest(loose, job_key=make_job_key(str(uuid.uuid4()), "job"))
    with pytest.raises(FormatError, match="job key"):
        validate_bundle(loose)
    with pytest.raises(FormatError, match="job key"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


@pytest.mark.parametrize("field", ["job_key", "destination_placement"])
def test_adopt_refuses_a_tree_member_that_differs_from_its_entry(tmp_path: Path, field: str) -> None:
    source, destination = _pair(tmp_path)
    root, children, _grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    entry = next(item for item in validate_bundle(loose)["eject_tree"] if item["job_id"] == children[0].job_id)
    member = transfers._nested_member(loose, entry)
    if field == "job_key":
        # Re-key the member consistently (marker included), so only the tree entry can tell.
        old = str(validate_bundle(member)["job_key"])
        new = make_job_key(children[0].job_id, "renamed")
        embedded = member / TRANSFER_DIRECTORY
        marker = next(path for path in embedded.iterdir() if path.name.startswith(old + "."))
        marker.rename(embedded / (new + marker.name.removeprefix(old)))
        _edit_manifest(member, job_key=new, sealed_marker=new + marker.name.removeprefix(old))
    else:
        _edit_manifest(member, destination_placement="somewhere/else")
    with pytest.raises(FormatError, match="does not belong to this ejected tree"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


# ---------------------------------------------------------------------------
# 2. sealed_marker
# ---------------------------------------------------------------------------


def test_a_sealed_marker_naming_a_path_is_refused(tmp_path: Path) -> None:
    _source, destination, loose = _loose(tmp_path)
    victim = tmp_path / "victim.txt"
    victim.write_text("keep me\n", encoding="utf-8")
    _edit_manifest(loose, sealed_marker="../../victim.txt")
    with pytest.raises(FormatError, match="sealed transfer marker"):
        destination.adopt(loose)
    assert victim.read_text(encoding="utf-8") == "keep me\n"
    assert loose.is_dir() and not list(destination.scan_markers())


def test_a_symlinked_sealed_marker_is_refused(tmp_path: Path) -> None:
    _source, destination, loose = _loose(tmp_path)
    victim = tmp_path / "victim.txt"
    victim.write_text("keep me\n", encoding="utf-8")
    manifest = validate_bundle(loose)
    sealed = loose / TRANSFER_DIRECTORY / str(manifest["sealed_marker"])
    sealed.unlink()
    sealed.symlink_to(victim)
    with pytest.raises(FormatError, match="sealed transfer marker"):
        validate_bundle(loose)
    with pytest.raises(FormatError):
        destination.adopt(loose)
    assert victim.read_text(encoding="utf-8") == "keep me\n"
    assert not list(destination.scan_markers())


def test_a_sealed_marker_of_another_job_is_refused(tmp_path: Path) -> None:
    _source, _destination, loose = _loose(tmp_path)
    manifest = validate_bundle(loose)
    other = f"{make_job_key(str(uuid.uuid4()), None)}.p500.g1.init"
    (loose / TRANSFER_DIRECTORY / other).write_text("", encoding="utf-8")
    _edit_manifest(loose, sealed_marker=other)
    assert manifest["sealed_marker"] != other
    with pytest.raises(FormatError, match="sealed transfer marker"):
        validate_bundle(loose)


# ---------------------------------------------------------------------------
# 3. unsafe-entry walk on receive and recovery
# ---------------------------------------------------------------------------


def test_import_bundles_refuses_a_hard_linked_file(tmp_path: Path) -> None:
    destination, bundle = _addressed(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("x\n", encoding="utf-8")
    (bundle / "logs").mkdir(exist_ok=True)
    os.link(outside, bundle / "logs" / "borrowed")
    with pytest.raises(FormatError, match="hard links are not accepted"):
        transfers.import_bundles(destination, [bundle])
    assert not list(destination.scan_markers())


def test_import_bundle_refuses_a_fifo(tmp_path: Path) -> None:
    destination, bundle = _addressed(tmp_path)
    (bundle / "logs").mkdir(exist_ok=True)
    os.mkfifo(bundle / "logs" / "pipe")
    with pytest.raises(FormatError, match="special entry"):
        destination.import_bundle(bundle)
    assert not list(destination.scan_markers())


def test_recovery_refuses_a_hard_link_planted_in_adoption_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _source, destination, loose = _loose(tmp_path)

    def crash(_self: Workspace, _source: Path, _destination: Path) -> None:
        raise RuntimeError("killed")

    monkeypatch.setattr(Workspace, "_publish_path", crash)
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    monkeypatch.undo()
    (staging,) = (destination.control / "tmp").glob("import.*")
    outside = tmp_path / "outside.txt"
    outside.write_text("x\n", encoding="utf-8")
    (staging / "logs").mkdir(exist_ok=True)
    os.link(outside, staging / "logs" / "borrowed")
    destination.recover_transfers()
    assert not list(destination.scan_markers())
    assert staging.is_dir()
    assert "hard-linked file" in caplog.text


# ---------------------------------------------------------------------------
# 4. sealing below a planted symlink
# ---------------------------------------------------------------------------


def _workspace_runner_job(workspace: Workspace, root: Path) -> Any:
    root.mkdir(parents=True)
    (root / "run.py").write_text("print('x')\n", encoding="utf-8")
    reference = workspace.publish_runner(root / "run.py", name="grouped/run.py")
    prepare_job_payload(
        root / "job",
        JobSpec(
            name="Runner job",
            workflow="tests.phase0",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="job",
            initial_step="start",
        ),
    )
    return workspace.submit(root / "job", "jobs")


def test_sealing_never_follows_a_symlink_a_job_planted_below_its_transfer_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = _pair(tmp_path)
    marker = _workspace_runner_job(source, tmp_path / "src")
    payload = source.payload_path(marker.placement, marker.job_key)
    outside = tmp_path / "outside"
    (outside / "grouped").mkdir(parents=True)
    victim = outside / "grouped" / "run.py"
    victim.write_text("precious\n", encoding="utf-8")
    real = transfers._transfer_directory

    def plant_after_acceptance(directory: Path, transfer_id: str) -> int:
        descriptor = real(directory, transfer_id)
        (directory / TRANSFER_DIRECTORY / "runners").symlink_to(outside)
        return descriptor

    monkeypatch.setattr(transfers, "_transfer_directory", plant_after_acceptance)
    with pytest.raises(FormatError, match="not a real directory"):
        source.eject(marker.job_id, tmp_path / "loose")
    assert victim.read_text(encoding="utf-8") == "precious\n"
    assert sorted(str(path.relative_to(outside)) for path in outside.rglob("*")) == ["grouped", "grouped/run.py"]
    assert payload.is_dir()


def test_sealing_refuses_a_foreign_transfer_directory(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    payload = source.payload_path(marker.placement, marker.job_key)
    planted = payload / TRANSFER_DIRECTORY
    planted.mkdir()
    (planted / TRANSFER_MANIFEST).write_text(json.dumps({"transfer_id": str(uuid.uuid4())}), encoding="utf-8")
    with pytest.raises(FormatError, match=r"\.httk-transfer"):
        source.eject(marker.job_id, tmp_path / "loose")


def test_a_workspace_runner_still_travels_with_an_ordinary_ejection(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = _workspace_runner_job(source, tmp_path / "src")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    carried = loose / TRANSFER_DIRECTORY / "runners" / "grouped" / "run.py"
    assert carried.is_file() and carried.stat().st_mode & 0o777 == 0o555
    adopted = destination.adopt(loose)
    definition = JobDefinition.from_path(destination.payload_path(adopted.placement, adopted.job_key) / "job.json")
    assert definition.runner_path.as_posix() == "grouped/run.py"


# ---------------------------------------------------------------------------
# 5. hygiene keeps a live adoption's staging
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("intent", [True, False])
def test_hygiene_repair_keeps_staging_of_a_live_adoption(tmp_path: Path, intent: bool) -> None:
    _source, destination = _pair(tmp_path)
    transfer_id = str(uuid.uuid4())
    staging = destination.control / "tmp" / f"import.{transfer_id}"
    staging.mkdir(parents=True)
    (staging / "job.json").write_text("{}", encoding="utf-8")
    if intent:
        adopting = destination.control / "transfers" / "adopting"
        adopting.mkdir(parents=True, exist_ok=True)
        (adopting / f"{transfer_id}.json").write_text("{}", encoding="utf-8")
    old = time.time() - 10 * 24 * 3600
    os.utime(staging, (old, old))
    _check_tmp_leftovers(destination.root, True)
    assert staging.exists() is intent


# ---------------------------------------------------------------------------
# Refusal and recovery behaviour of sealing
# ---------------------------------------------------------------------------


def test_an_empty_planted_transfer_directory_does_not_stop_an_ejection(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (source.payload_path(marker.placement, marker.job_key) / TRANSFER_DIRECTORY).mkdir()
    loose = source.eject(marker.job_id, tmp_path / "loose")
    assert validate_bundle(loose)["job_id"] == marker.job_id
    assert destination.adopt(loose).job_id == marker.job_id


def test_a_refused_foreign_transfer_directory_leaves_the_job_unfenced_and_others_unblocked(tmp_path: Path) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    planted = source.payload_path(marker.placement, marker.job_key) / TRANSFER_DIRECTORY
    planted.mkdir()
    (planted / TRANSFER_MANIFEST).write_text(json.dumps({"transfer_id": str(uuid.uuid4())}), encoding="utf-8")
    with pytest.raises(FormatError, match="another transfer"):
        source.eject(marker.job_id, tmp_path / "loose")
    kept = source.find_marker_by_id(marker.job_id)
    assert kept is not None and kept.kind == marker.kind
    assert not list(source.scan_markers(("transferring",)))
    assert validate_bundle(source.eject(other.job_id, tmp_path / "other"))["job_id"] == other.job_id
    source.recover_transfers()


def test_an_unsealable_transferring_job_blocks_neither_recovery_nor_other_ejections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    source, _destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    real = transfers._seal_transferring

    def refuse_first_job(workspace: Workspace, item: Any, state: Any) -> Path:
        if item.job_id == marker.job_id:
            raise FormatError("planted")
        return real(workspace, item, state)

    monkeypatch.setattr(transfers, "_seal_transferring", refuse_first_job)
    with pytest.raises(FormatError, match="planted"):
        source.eject(marker.job_id, tmp_path / "loose")
    assert [item.kind for item in source.scan_markers(("transferring",))] == ["transferring"]
    # The stuck job does not wedge recovery nor another job's ejection.
    source.recover_transfers()
    assert "cannot seal transferring job" in caplog.text
    assert validate_bundle(source.eject(other.job_id, tmp_path / "other"))["job_id"] == other.job_id
    monkeypatch.undo()
    source.recover_transfers()
    assert validate_bundle(tmp_path / "loose")["job_id"] == marker.job_id


def test_a_directory_runner_travels_through_eject_and_adopt(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    root = tmp_path / "src"
    (root / "tool" / "lib").mkdir(parents=True)
    (root / "tool" / "run").write_text("print('x')\n", encoding="utf-8")
    (root / "tool" / "run").chmod(0o755)
    (root / "tool" / "lib" / "helper.py").write_text("X = 1\n", encoding="utf-8")
    reference = source.publish_runner(root / "tool", name="grouped/tool")
    prepare_job_payload(
        root / "job",
        JobSpec(
            name="Dir runner",
            workflow="tests.phase0",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="job",
            initial_step="start",
        ),
    )
    marker = source.submit(root / "job", "jobs")
    loose = source.eject(marker.job_id, tmp_path / "loose")
    carried = loose / TRANSFER_DIRECTORY / "runners" / "grouped" / "tool"
    assert (carried / "lib" / "helper.py").read_text(encoding="utf-8") == "X = 1\n"
    assert os.access(carried / "run", os.X_OK)
    adopted = destination.adopt(loose)
    stored = destination.runner_store_path("grouped/tool")
    assert (stored / "lib" / "helper.py").is_file() and adopted.job_id == marker.job_id


def test_a_crash_right_after_the_manifest_is_written_is_resumed_by_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = _pair(tmp_path)
    marker = _workspace_runner_job(source, tmp_path / "src")
    real = transfers._write_manifest_at

    def crash_after(transfer_fd: int, manifest: Any, *, durable: bool) -> None:
        real(transfer_fd, manifest, durable=durable)
        raise RuntimeError("killed")

    monkeypatch.setattr(transfers, "_write_manifest_at", crash_after)
    with pytest.raises(RuntimeError):
        source.eject(marker.job_id, tmp_path / "loose")
    monkeypatch.undo()
    assert [item.kind for item in source.scan_markers(("transferring",))] == ["transferring"]
    source.recover_transfers()
    assert validate_bundle(tmp_path / "loose")["job_id"] == marker.job_id
