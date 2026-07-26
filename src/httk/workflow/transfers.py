"""Crash-recoverable detached job transfer."""

import hashlib
import os
import shutil
import stat
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from ._util import read_json, sha256_file, utc_now, write_json_atomic
from .errors import FormatError, UnsupportedExtensionError, WorkspaceCorruptionError
from .journal import JournalWriter
from .models import (
    QUIESCENT_KINDS,
    STATE_KINDS,
    JobDefinition,
    Marker,
    canonical_uuid,
    is_payload_private,
    normalize_placement,
    validate_runner_path,
    validate_sha256,
)
from .workspace import WorkflowWorkspace

TRANSFER_DIRECTORY = ".httk-transfer"
TRANSFER_MANIFEST = "manifest.json"
TRANSFER_RUNNERS = "runners"


def _excluded_from_bundle(name: str) -> bool:
    """Report whether one top-level payload entry stays out of the digest.

    The transfer directory describes the bundle rather than the job, and the
    runner-private entries of a payload — attempt control directories and job
    state — are excluded from every payload digest, so a job that ran before it
    was detached digests exactly like one that never did.
    """

    return name == TRANSFER_DIRECTORY or is_payload_private(name)


def _payload_digest(payload: Path) -> str:
    digest = hashlib.sha256()
    entries = [
        item
        for item in payload.rglob("*")
        if item.relative_to(payload).parts and not _excluded_from_bundle(item.relative_to(payload).parts[0])
    ]
    for entry in sorted(entries, key=lambda item: item.relative_to(payload).as_posix()):
        relative = entry.relative_to(payload).as_posix().encode("utf-8")
        mode = entry.lstat().st_mode
        if stat.S_ISDIR(mode):
            digest.update(b"D\0" + relative + b"\0")
        elif stat.S_ISREG(mode):
            digest.update(b"F\0" + relative + b"\0" + sha256_file(entry).encode("ascii") + b"\0")
        else:
            raise FormatError(f"transfer payload rejects symlink or special entry: {entry}")
    return digest.hexdigest()


def _bundled_runners(workspace: WorkflowWorkspace, payload: Path, transfer_dir: Path) -> list[dict[str, str]]:
    """Copy the workspace runners one job references into its bundle.

    A detached job must remain runnable at its destination, so a runner it only
    references by name and digest travels with it. Payload runners are already
    inside the payload, and an installed runner is deployment state of the
    destination rather than of the job, so neither is bundled.
    """

    job = JobDefinition.from_path(payload / "job.json")
    if job.runner_source != "workspace" or job.runner_sha256 is None:
        return []
    relative = job.runner_path
    source = workspace.runner_store_path(relative)
    if not source.is_file():
        raise FormatError(f"referenced workspace runner is not published: {relative.as_posix()}")
    digest = sha256_file(source)
    if digest != job.runner_sha256:
        raise WorkspaceCorruptionError(
            f"workspace runner {relative.as_posix()} has digest {digest}, but the job pinned {job.runner_sha256}"
        )
    embedded = transfer_dir / TRANSFER_RUNNERS / Path(*relative.parts)
    embedded.parent.mkdir(parents=True, exist_ok=True)
    if not embedded.is_file() or sha256_file(embedded) != digest:
        shutil.copyfile(source, embedded)
        embedded.chmod(0o555)
    return [{"path": relative.as_posix(), "sha256": digest}]


def _install_bundled_runners(workspace: WorkflowWorkspace, bundle: Path, manifest: Mapping[str, Any]) -> None:
    """Install every runner a bundle carries into the destination store.

    Installation is content addressed and therefore idempotent: an entry whose
    digest already matches is skipped, and a name that already holds different
    content is a conflict rather than something to overwrite, because live jobs
    at the destination may already reference the stored digest.
    """

    for entry in _manifest_runners(manifest):
        relative = validate_runner_path(entry["path"], "workspace")
        digest = entry["sha256"]
        source = bundle / TRANSFER_DIRECTORY / TRANSFER_RUNNERS / Path(*relative.parts)
        target = workspace.runner_store_path(relative)
        if target.is_file():
            existing = sha256_file(target)
            if existing == digest:
                continue
            raise WorkspaceCorruptionError(
                f"destination workspace runner {relative.as_posix()} holds digest {existing}, "
                f"but the transfer carries {digest}"
            )
        if not source.is_file():
            raise FormatError(f"transfer bundle does not carry the runner it declares: {relative.as_posix()}")
        workspace.publish_runner(source, name=relative)


def _manifest_runners(manifest: Mapping[str, Any]) -> list[dict[str, str]]:
    """Validate and return the ``runners`` list of one transfer manifest."""

    raw = manifest.get("runners", [])
    if not isinstance(raw, list):
        raise FormatError("transfer manifest runners must be an array")
    result: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise FormatError("transfer manifest runner must be an object")
        relative = validate_runner_path(item.get("path"), "workspace")
        result.append({"path": relative.as_posix(), "sha256": validate_sha256(item.get("sha256"), "runner.sha256")})
    return result


def _ledger_path(workspace: WorkflowWorkspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / f"{transfer_id}.json"


def _all_markers(workspace: WorkflowWorkspace) -> list[Marker]:
    return list(workspace.scan_markers(STATE_KINDS))


def _unresolved_join_reference(workspace: WorkflowWorkspace, marker: Marker) -> bool:
    if marker.kind == "waiting":
        return True
    for waiting in workspace.scan_markers(("waiting",)):
        state = workspace.read_state(waiting)
        join = state.get("join")
        if not isinstance(join, Mapping):
            continue
        children = join.get("children", [])
        if not isinstance(children, list):
            continue
        for child in children:
            if isinstance(child, Mapping) and child.get("workspace_id") == workspace.workspace_id:
                if child.get("job_id") == marker.job_id:
                    return True
    return False


def _seal_transferring(workspace: WorkflowWorkspace, marker: Marker, state: Mapping[str, Any]) -> Path:
    payload = workspace.payload_path(marker.placement, marker.job_key)
    transfer_dir = payload / TRANSFER_DIRECTORY
    transfer_dir.mkdir(exist_ok=True)
    transfer_id = canonical_uuid(state.get("transfer_id"), "transfer_id")
    destination_workspace_id = canonical_uuid(state.get("destination_workspace_id"), "destination_workspace_id")
    prior = state.get("prior_state")
    if not isinstance(prior, Mapping):
        raise FormatError("transferring state has no prior_state object")
    prior_kind = str(state.get("prior_kind"))
    runners = _bundled_runners(workspace, payload, transfer_dir)
    manifest = {
        "format": "httk-workflow-detached-transfer",
        "format_version": 1,
        "extension": "detached-transfer-v1",
        "transfer_id": transfer_id,
        "source_workspace_id": workspace.workspace_id,
        "destination_workspace_id": destination_workspace_id,
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "source_placement": marker.placement.as_posix(),
        "destination_placement": str(state["destination_placement"]),
        "payload_sha256": _payload_digest(payload),
        "runners": runners,
        "prior_kind": prior_kind,
        "prior_state": dict(prior),
        "priority": marker.priority,
        "source_generation": marker.generation,
        "sealed_marker": marker.path.name,
    }
    manifest_path = transfer_dir / TRANSFER_MANIFEST
    if manifest_path.exists():
        existing = read_json(manifest_path)
        if existing != manifest:
            raise WorkspaceCorruptionError(f"conflicting transfer manifest: {manifest_path}")
    else:
        write_json_atomic(manifest_path, manifest, durable=workspace.durable)
    embedded = transfer_dir / marker.path.name
    if marker.path.exists():
        os.rename(marker.path, embedded)
    elif not embedded.is_file():
        raise WorkspaceCorruptionError("transfer marker exists in neither state tree nor sealed bundle")
    ledger = {**manifest, "status": "sealed", "bundle": str(payload), "updated_at": utc_now()}
    write_json_atomic(_ledger_path(workspace, transfer_id), ledger, durable=workspace.durable)
    return payload


def detach_job(
    workspace: WorkflowWorkspace,
    job_id: str,
    *,
    destination_workspace_id: str,
    destination_placement: str | PurePosixPath | None = None,
    transfer_id: str | None = None,
) -> Path:
    """Fence and seal one job, leaving no schedulable source marker."""

    if "detached-transfer-v1" not in workspace.extensions:
        raise UnsupportedExtensionError("workspace has not enabled detached-transfer-v1")
    destination_id = canonical_uuid(destination_workspace_id, "destination_workspace_id")
    identifier = str(uuid.uuid4()) if transfer_id is None else canonical_uuid(transfer_id, "transfer_id")
    existing = _ledger_path(workspace, identifier)
    if existing.is_file():
        ledger = read_json(existing)
        if ledger.get("destination_workspace_id") != destination_id:
            raise WorkspaceCorruptionError("transfer UUID was reused for a different destination")
        return Path(str(ledger["bundle"]))
    matches = [marker for marker in _all_markers(workspace) if marker.job_id == job_id]
    if len(matches) != 1:
        raise ValueError(f"job must have exactly one source marker: {job_id}")
    marker = matches[0]
    if marker.kind == "transferring":
        state = workspace.read_state(marker)
        if state.get("transfer_id") != identifier or state.get("destination_workspace_id") != destination_id:
            raise ValueError("job is already transferring under a different transfer")
        return _seal_transferring(workspace, marker, state)
    if marker.kind not in QUIESCENT_KINDS:
        raise ValueError(f"job is not quiescent and cannot transfer: {marker.kind}")
    if _unresolved_join_reference(workspace, marker):
        raise ValueError("job participates in an unresolved join and cannot transfer")
    target_placement = normalize_placement(destination_placement or marker.placement)
    prior_state = workspace.read_state(marker)
    with JournalWriter(workspace.control, durable=workspace.durable) as writer:
        transferring = workspace.transition(
            writer,
            marker,
            "transferring",
            {
                "transfer_id": identifier,
                "source_workspace_id": workspace.workspace_id,
                "destination_workspace_id": destination_id,
                "destination_placement": target_placement.as_posix(),
                "prior_kind": marker.kind,
                "prior_state": prior_state,
                "reason": "detached_transfer",
            },
        )
    return _seal_transferring(workspace, transferring, workspace.read_state(transferring))


def validate_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate a sealed bundle and return its manifest."""

    payload = Path(bundle).expanduser().resolve()
    manifest = read_json(payload / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
    if (
        manifest.get("format") != "httk-workflow-detached-transfer"
        or manifest.get("format_version") != 1
        or manifest.get("extension") != "detached-transfer-v1"
    ):
        raise FormatError("unsupported detached transfer manifest")
    canonical_uuid(manifest.get("transfer_id"), "transfer_id")
    canonical_uuid(manifest.get("source_workspace_id"), "source_workspace_id")
    canonical_uuid(manifest.get("destination_workspace_id"), "destination_workspace_id")
    canonical_uuid(manifest.get("job_id"), "job_id")
    normalize_placement(str(manifest.get("destination_placement")))
    marker_name = manifest.get("sealed_marker")
    if not isinstance(marker_name, str) or not (payload / TRANSFER_DIRECTORY / marker_name).is_file():
        raise FormatError("sealed transfer marker is absent")
    if _payload_digest(payload) != manifest.get("payload_sha256"):
        raise FormatError("detached transfer payload digest mismatch")
    # Bundled runners sit beside the manifest rather than inside the payload, so
    # each one is verified against its own declared digest.
    for entry in _manifest_runners(manifest):
        carried = payload / TRANSFER_DIRECTORY / TRANSFER_RUNNERS / Path(*PurePosixPath(entry["path"]).parts)
        if not carried.is_file():
            raise FormatError(f"transfer bundle does not carry the runner it declares: {entry['path']}")
        if sha256_file(carried) != entry["sha256"]:
            raise FormatError(f"bundled runner digest mismatch: {entry['path']}")
    return manifest


def _ack_path(workspace: WorkflowWorkspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / "acks" / f"{transfer_id}.json"


def import_bundle(workspace: WorkflowWorkspace, bundle: str | os.PathLike[str]) -> dict[str, object]:
    """Idempotently import a sealed bundle and publish its prior state."""

    if "detached-transfer-v1" not in workspace.extensions:
        raise UnsupportedExtensionError("destination workspace has not enabled detached-transfer-v1")
    source = Path(bundle).expanduser().resolve()
    manifest = validate_bundle(source)
    transfer_id = str(manifest["transfer_id"])
    digest = str(manifest["payload_sha256"])
    acknowledgement_path = _ack_path(workspace, transfer_id)
    if acknowledgement_path.is_file():
        existing_ack = read_json(acknowledgement_path)
        if existing_ack.get("payload_sha256") != digest:
            raise WorkspaceCorruptionError("transfer acknowledgement digest mismatch")
        return existing_ack
    if manifest["destination_workspace_id"] != workspace.workspace_id:
        raise ValueError("bundle names a different destination workspace")
    # Runners are installed before anything about the job is published, because
    # an imported job must never become schedulable without the runner it pins.
    _install_bundled_runners(workspace, source, manifest)
    duplicates = [marker for marker in _all_markers(workspace) if marker.job_id == manifest["job_id"]]
    if duplicates:
        if len(duplicates) != 1:
            raise WorkspaceCorruptionError(f"destination has multiple markers for job UUID {manifest['job_id']}")
        duplicate_state = workspace.read_state(duplicates[0])
        provenance = duplicate_state.get("transfer")
        if not isinstance(provenance, Mapping) or provenance.get("transfer_id") != transfer_id:
            raise FileExistsError(f"destination already contains job UUID {manifest['job_id']}")
        if provenance.get("payload_sha256") != digest:
            raise WorkspaceCorruptionError("imported marker transfer digest mismatch")
        duplicate_ack: dict[str, object] = {
            "format": "httk-workflow-transfer-acknowledgement",
            "format_version": 1,
            "transfer_id": transfer_id,
            "source_workspace_id": manifest["source_workspace_id"],
            "destination_workspace_id": workspace.workspace_id,
            "payload_sha256": digest,
            "job_id": manifest["job_id"],
            "job_key": manifest["job_key"],
            "placement": duplicate_state["placement"],
            "state": duplicates[0].kind,
            "acknowledged_at": utc_now(),
        }
        transfer_dir = workspace.payload_path(duplicates[0].placement, duplicates[0].job_key) / TRANSFER_DIRECTORY
        if transfer_dir.exists():
            shutil.rmtree(transfer_dir)
        write_json_atomic(acknowledgement_path, duplicate_ack, durable=workspace.durable)
        return duplicate_ack
    placement = normalize_placement(str(manifest["destination_placement"]))
    target = workspace.payload_path(placement, str(manifest["job_key"]))
    if target.exists():
        if validate_bundle(target).get("payload_sha256") != digest:
            raise FileExistsError(f"destination payload collision: {target}")
    else:
        staging = workspace.control / "tmp" / f"import.{transfer_id}"
        if staging.exists():
            if validate_bundle(staging).get("payload_sha256") != digest:
                raise WorkspaceCorruptionError("staged transfer digest mismatch")
        else:
            shutil.copytree(source, staging, symlinks=True)
        if _payload_digest(staging) != digest:
            raise FormatError("copied transfer payload digest mismatch")
        target.parent.mkdir(parents=True, exist_ok=True)
        workspace._publish_path(staging, target)
    transfer_dir = target / TRANSFER_DIRECTORY
    embedded = transfer_dir / str(manifest["sealed_marker"])
    prior = manifest.get("prior_state")
    if not isinstance(prior, Mapping):
        raise FormatError("transfer prior_state must be an object")
    prior_kind = str(manifest["prior_kind"])
    if prior_kind not in QUIESCENT_KINDS:
        raise FormatError(f"transfer prior state is not quiescent: {prior_kind}")
    generation = int(manifest["source_generation"]) + 1
    frame: dict[str, object] = {
        **{
            key: value
            for key, value in prior.items()
            if key
            not in {
                "workspace_id",
                "state_generation",
                "kind",
                "previous_record_ref",
                "created_at",
                "placement",
                "priority",
            }
        },
        "format": "httk-workflow-state",
        "format_version": 1,
        "workspace_id": workspace.workspace_id,
        "job_id": manifest["job_id"],
        "job_key": manifest["job_key"],
        "placement": placement.as_posix(),
        "state_generation": generation,
        "kind": prior_kind,
        "previous_record_ref": None,
        "created_at": utc_now(),
        "priority": int(manifest["priority"]),
        "transfer": {
            "transfer_id": transfer_id,
            "source_workspace_id": manifest["source_workspace_id"],
            "payload_sha256": digest,
        },
    }
    with JournalWriter(workspace.control, durable=workspace.durable) as writer:
        record_ref = writer.append(frame)
    destination = workspace.marker_path(
        prior_kind,
        placement,
        str(manifest["job_key"]),
        int(manifest["priority"]),
        generation,
        record_ref,
    )
    embedded_marker = Marker(
        "transferring",
        normalize_placement(str(manifest["source_placement"])),
        str(manifest["job_key"]),
        int(manifest["priority"]),
        int(manifest["source_generation"]),
        "init",
        embedded,
    )
    imported = workspace._verified_marker_rename(embedded_marker, destination)
    imported_record = {**manifest, "status": "imported", "marker": str(imported.path), "imported_at": utc_now()}
    write_json_atomic(workspace.control / "transfers" / "imported" / f"{transfer_id}.json", imported_record)
    shutil.rmtree(transfer_dir)
    acknowledgement: dict[str, object] = {
        "format": "httk-workflow-transfer-acknowledgement",
        "format_version": 1,
        "transfer_id": transfer_id,
        "source_workspace_id": manifest["source_workspace_id"],
        "destination_workspace_id": workspace.workspace_id,
        "payload_sha256": digest,
        "job_id": manifest["job_id"],
        "job_key": manifest["job_key"],
        "placement": placement.as_posix(),
        "state": prior_kind,
        "acknowledged_at": utc_now(),
    }
    write_json_atomic(acknowledgement_path, acknowledgement, durable=workspace.durable)
    return acknowledgement


def acknowledge_transfer(workspace: WorkflowWorkspace, acknowledgement: Mapping[str, object]) -> Path:
    """Validate an acknowledgement and retire the sealed source bundle."""

    if acknowledgement.get("format") != "httk-workflow-transfer-acknowledgement":
        raise FormatError("invalid transfer acknowledgement format")
    transfer_id = canonical_uuid(acknowledgement.get("transfer_id"), "transfer_id")
    ledger_path = _ledger_path(workspace, transfer_id)
    ledger = read_json(ledger_path)
    for name in ("source_workspace_id", "destination_workspace_id", "payload_sha256", "job_id", "job_key"):
        if acknowledgement.get(name) != ledger.get(name):
            raise FormatError(f"transfer acknowledgement disagrees on {name}")
    if ledger.get("status") == "retired":
        return Path(str(ledger["retired_bundle"]))
    bundle = Path(str(ledger["bundle"]))
    validate_bundle(bundle)
    retired = workspace.control / "transfers" / "retired" / transfer_id / "bundle"
    retired.parent.mkdir(parents=True, exist_ok=True)
    if retired.exists():
        if bundle.exists():
            raise WorkspaceCorruptionError("both active and retired source bundles exist")
    else:
        os.rename(bundle, retired)
    ledger.update(
        {
            "status": "retired",
            "retired_bundle": str(retired),
            "acknowledgement": dict(acknowledgement),
            "updated_at": utc_now(),
        }
    )
    write_json_atomic(ledger_path, ledger, durable=workspace.durable)
    return retired


def recover_transfers(workspace: WorkflowWorkspace) -> list[dict[str, object]]:
    """Finish source sealing and inventory every retained bundle."""

    results: list[dict[str, object]] = []
    for marker in list(workspace.scan_markers(("transferring",))):
        state = workspace.read_state(marker)
        bundle = _seal_transferring(workspace, marker, state)
        results.append({"transfer_id": state["transfer_id"], "status": "sealed", "bundle": str(bundle)})
    for manifest_path in workspace.root.rglob(f"{TRANSFER_DIRECTORY}/{TRANSFER_MANIFEST}"):
        if workspace.control in manifest_path.parents:
            continue
        bundle = manifest_path.parent.parent
        manifest = validate_bundle(bundle)
        ledger_path = _ledger_path(workspace, str(manifest["transfer_id"]))
        if not ledger_path.exists():
            write_json_atomic(
                ledger_path,
                {**manifest, "status": "sealed", "bundle": str(bundle), "updated_at": utc_now()},
                durable=workspace.durable,
            )
        results.append({"transfer_id": manifest["transfer_id"], "status": "sealed", "bundle": str(bundle)})
    unique = {(str(item["transfer_id"]), str(item["status"])): item for item in results}
    return list(unique.values())
