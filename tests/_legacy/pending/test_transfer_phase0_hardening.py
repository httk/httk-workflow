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
from httk.workflow import Workspace, _txn, transfers
from httk.workflow._bundle import verify_bundle
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
    with pytest.raises(FormatError, match="job_key .* does not carry the job id"):
        validate_bundle(loose)
    with pytest.raises(FormatError, match="job_key .* does not carry the job id"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


@pytest.mark.parametrize("field", ["job_key", "destination_placement"])
def test_adopt_refuses_a_tree_member_that_differs_from_its_entry(tmp_path: Path, field: str) -> None:
    source, destination = _pair(tmp_path)
    root, children, _grandchildren = _tree(source, tmp_path / "runner")
    loose = source.eject(root.job_id, tmp_path / "loose")
    entry = next(item for item in validate_bundle(loose)["members"] if item["job_id"] == children[0].job_id)
    tree = loose / TRANSFER_DIRECTORY / "tree"
    member = tree.joinpath(*entry["placement"].split("/"), entry["job_key"])
    if field == "job_key":
        # Re-key the member's directory, so only the manifest's member list can tell.
        member.rename(member.with_name(make_job_key(children[0].job_id, "renamed")))
    else:
        (tree / "somewhere" / "else").mkdir(parents=True)
        member.rename(tree / "somewhere" / "else" / member.name)
    with pytest.raises(FormatError, match="unexpected entry|lacks"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


# ---------------------------------------------------------------------------
# 2. sealed_marker
# ---------------------------------------------------------------------------


def test_a_marker_naming_a_path_is_refused(tmp_path: Path) -> None:
    """Markers are fixed names (``markers/<job_id>``): a manifest cannot name one anywhere else."""

    _source, destination, loose = _loose(tmp_path)
    victim = tmp_path / "victim.txt"
    victim.write_text("keep me\n", encoding="utf-8")
    _edit_manifest(loose, sealed_marker="../../victim.txt")
    with pytest.raises(FormatError, match="unknown members sealed_marker"):
        destination.adopt(loose)
    assert victim.read_text(encoding="utf-8") == "keep me\n"
    assert loose.is_dir() and not list(destination.scan_markers())


def test_a_symlinked_marker_is_refused(tmp_path: Path) -> None:
    _source, destination, loose = _loose(tmp_path)
    victim = tmp_path / "victim.txt"
    victim.write_text("keep me\n", encoding="utf-8")
    manifest = validate_bundle(loose)
    marker = loose / TRANSFER_DIRECTORY / "markers" / str(manifest["job_id"])
    marker.unlink()
    marker.symlink_to(victim)
    with pytest.raises(FormatError, match="symlink"):
        validate_bundle(loose)
    with pytest.raises(FormatError):
        destination.adopt(loose)
    assert victim.read_text(encoding="utf-8") == "keep me\n"
    assert not list(destination.scan_markers())


def test_a_marker_of_another_job_is_refused(tmp_path: Path) -> None:
    _source, _destination, loose = _loose(tmp_path)
    (loose / TRANSFER_DIRECTORY / "markers" / str(uuid.uuid4())).write_text("", encoding="utf-8")
    with pytest.raises(FormatError, match="unexpected marker"):
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
    [result] = transfers.import_bundles(destination, [bundle])
    assert result["status"] == "refused" and "hard links are not accepted" in str(result["reason"])
    # A refused bundle is kept where it was delivered, for the operator.
    assert bundle.is_dir()
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

    def killed(step: str) -> None:
        if step == "V2.claimed":
            _txn._HOOK = None
            raise RuntimeError("killed")

    _txn._HOOK = killed
    with pytest.raises(RuntimeError):
        destination.adopt(loose)
    (lineage,) = (destination.control / "tmp").glob("import.*")
    outside = tmp_path / "outside.txt"
    outside.write_text("x\n", encoding="utf-8")
    (lineage / "bundle" / "logs").mkdir(exist_ok=True)
    os.link(outside, lineage / "bundle" / "logs" / "borrowed")
    # The takeover runs the whole trust walk again before anything is published.
    monkeypatch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: True)
    destination.recover_transfers()
    assert not list(destination.scan_markers())
    assert "hard-linked file" in caplog.text
    # Refused, the bundle goes back where it came from.
    assert (loose / "logs" / "borrowed").is_file() and not list((destination.control / "tmp").glob("import.*"))


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
    """Adoption staging is only ever removed by the transfer protocol, never by hygiene (plan decision 16)."""

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
    assert staging.exists()


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
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    planted = source.payload_path(marker.placement, marker.job_key) / TRANSFER_DIRECTORY
    planted.mkdir()
    (planted / TRANSFER_MANIFEST).write_text(json.dumps({"transfer_id": str(uuid.uuid4())}), encoding="utf-8")
    with pytest.raises(FormatError, match="of its own"):
        source.eject(marker.job_id, tmp_path / "loose")
    kept = source.find_marker_by_id(marker.job_id)
    assert kept is not None and kept.kind == marker.kind
    assert not list(source.scan_markers(("transferring",)))
    ejected = source.eject(other.job_id, tmp_path / "other")
    assert verify_bundle(ejected, workspace=destination, source="adopt").manifest.job_id == other.job_id
    source.recover_transfers()


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
