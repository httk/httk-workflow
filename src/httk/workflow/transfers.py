"""Crash-recoverable detached job transfer."""

import hashlib
import logging
import os
import shutil
import stat
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.digests import sha256_file, tree_digest
from httk.core.identity import identity_seed, sign_document, verify_document

from ._util import read_json, utc_now, write_json_atomic
from .errors import FormatError, WorkflowError, WorkspaceCorruptionError
from .models import (
    CORE_PROFILE,
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
from .seals import job_seal_path
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_OFFER_STATES",
    "TRANSFER_DIRECTORY",
    "TRANSFER_FORMAT",
    "TRANSFER_FORMAT_VERSION",
    "TRANSFER_MANIFEST",
    "TRANSFER_OFFER_FORMAT",
    "TRANSFER_RETIREMENT_FORMAT",
    "TRANSFER_RUNNERS",
    "TransferCandidate",
    "acknowledge_transfer",
    "detach_job",
    "discard_staged_bundle",
    "import_bundle",
    "offer_transfers",
    "recover_transfers",
    "retire_transfers",
    "select_transfer_jobs",
    "validate_bundle",
]

TRANSFER_DIRECTORY = ".httk-transfer"
TRANSFER_MANIFEST = "manifest.json"
TRANSFER_RUNNERS = "runners"

TRANSFER_FORMAT = "httk-workflow-detached-transfer"
#: Version 2 widened the payload digest: it now also pins the executable bit of
#: every regular file and the target of every symlink.
TRANSFER_FORMAT_VERSION = 2
#: Domain separation of the payload digest, so a digest computed by an older
#: rule can never collide with one computed by the current rule.
_PAYLOAD_DIGEST_DOMAIN = b"httk-workflow-transfer-payload-v2\0"

#: The format of what ``tasks offer`` prints and ``tasks fetch`` consumes.
TRANSFER_OFFER_FORMAT = "httk-workflow-transfer-offer"
TRANSFER_RETIREMENT_FORMAT = "httk-workflow-transfer-retirement"
#: The terminal states a results fetch collects unless told otherwise.
DEFAULT_OFFER_STATES = ("succeeded", "failed")


@dataclass(frozen=True)
class TransferCandidate:
    """One job a transfer offer or resume can actually inspect.

    :param job_id: Identify the job.
    :param job_key: Preserve the stable job key.
    :param prior_kind: State the job had before transfer.
    :param source_placement: Placement in the source workspace.
    :param bundle: Sealed payload, when this is a resumed transfer.
    :param marker: Live source marker, when this is a new offer.
    :param job: Readable immutable job definition, when available.
    :param manifest: Validated sealed-bundle manifest, when available.
    :param problem: Readability problem, if the candidate cannot be validated.
    """

    job_id: str
    job_key: str
    prior_kind: str
    source_placement: PurePosixPath
    bundle: Path | None
    marker: Marker | None
    job: JobDefinition | None
    manifest: Mapping[str, Any] | None = None
    problem: str | None = None


def _excluded_from_bundle(name: str) -> bool:
    """Report whether one top-level payload entry stays out of the digest.

    The transfer directory describes the bundle rather than the job, and the
    runner-private entries of a payload — attempt control directories and job
    state — are excluded from every payload digest, so a job that ran before it
    was detached digests exactly like one that never did.
    """

    return name == TRANSFER_DIRECTORY or is_payload_private(name)


def _contained_symlink_target(payload: Path, entry: Path) -> str:
    """Return the target of one payload symlink, refusing any that escapes.

    A symlink is transferred as its literal target string, exactly as a signed
    project manifest records one, because that is what makes the link mean the
    same thing at the destination. That only holds for a link that stays inside
    the payload: an absolute target names a path of the source machine, and a
    relative target climbing out of the payload resolves against whatever
    happens to sit beside the payload at the destination. Both are refused by
    name rather than transferred into a different meaning.
    """

    target = os.readlink(entry)
    if PurePosixPath(target).is_absolute():
        raise FormatError(
            f"transfer payload rejects the absolute symlink {entry.name} -> {target}: "
            f"an absolute target names a path of the source machine ({entry})"
        )
    parts = list(entry.parent.relative_to(payload).parts)
    for part in PurePosixPath(target).parts:
        if part in {"", "."}:
            continue
        if part != "..":
            parts.append(part)
            continue
        if not parts:
            raise FormatError(
                f"transfer payload rejects the escaping symlink {entry.name} -> {target}: "
                f"the target resolves outside the payload ({entry})"
            )
        parts.pop()
    return target


def _payload_digest(payload: Path) -> str:
    """Digest one payload tree: names, kinds, content, exec bits, and link targets.

    The executable bit is part of the digest because it is part of what a
    payload *is*: a runner or helper script that arrives without it does not
    run, so a transfer that dropped it would be a silent corruption rather than
    a detected one.
    """

    digest = hashlib.sha256()
    digest.update(_PAYLOAD_DIGEST_DOMAIN)
    entries = [
        item
        for item in payload.rglob("*")
        if item.relative_to(payload).parts and not _excluded_from_bundle(item.relative_to(payload).parts[0])
    ]
    for entry in sorted(entries, key=lambda item: item.relative_to(payload).as_posix()):
        relative = entry.relative_to(payload).as_posix().encode("utf-8")
        mode = entry.lstat().st_mode
        if stat.S_ISLNK(mode):
            target = _contained_symlink_target(payload, entry).encode("utf-8")
            digest.update(b"L\0" + relative + b"\0" + target + b"\0")
        elif stat.S_ISDIR(mode):
            digest.update(b"D\0" + relative + b"\0")
        elif stat.S_ISREG(mode):
            executable = b"x" if mode & 0o111 else b"-"
            digest.update(b"F\0" + relative + b"\0" + executable + b"\0" + sha256_file(entry).encode("ascii") + b"\0")
        else:
            raise FormatError(f"transfer payload rejects special entry: {entry}")
    return digest.hexdigest()


def _runner_digest(path: Path) -> str:
    """Digest one published runner file or tree with the workspace rule."""

    if path.is_symlink() or not (path.is_file() or path.is_dir()):
        raise FormatError(f"referenced workspace runner is not a regular file or tree: {path}")
    return sha256_file(path) if path.is_file() else tree_digest(path)


def _remove_tree(path: Path) -> None:
    """Remove a bundle tree even when a copied runner tree is read-only."""

    for entry in path.rglob("*"):
        if entry.is_symlink():
            continue
        entry.chmod(0o755 if entry.is_dir() else 0o644)
    path.chmod(0o755)
    shutil.rmtree(path)


def _bundled_runners(workspace: Workspace, payload: Path, transfer_dir: Path) -> list[dict[str, str]]:
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
    digest = _runner_digest(source)
    if digest != job.runner_sha256:
        raise WorkspaceCorruptionError(
            f"workspace runner {relative.as_posix()} has digest {digest}, but the job pinned {job.runner_sha256}"
        )
    embedded = transfer_dir / TRANSFER_RUNNERS / Path(*relative.parts)
    embedded.parent.mkdir(parents=True, exist_ok=True)
    if not embedded.is_symlink() and (embedded.is_file() or embedded.is_dir()):
        same_shape = source.is_file() == embedded.is_file()
        if same_shape and _runner_digest(embedded) == digest:
            return [{"path": relative.as_posix(), "sha256": digest}]
        if embedded.is_dir():
            _remove_tree(embedded)
        else:
            embedded.unlink()
    if source.is_dir():
        shutil.copytree(source, embedded)
    else:
        shutil.copyfile(source, embedded)
        embedded.chmod(0o555)
    return [{"path": relative.as_posix(), "sha256": digest}]


def _install_bundled_runners(workspace: Workspace, bundle: Path, manifest: Mapping[str, Any]) -> None:
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
        if target.is_file() or target.is_dir():
            existing = _runner_digest(target)
            if existing == digest:
                continue
            raise WorkspaceCorruptionError(
                f"destination workspace runner {relative.as_posix()} holds digest {existing}, "
                f"but the transfer carries {digest}"
            )
        if not source.is_file() and not source.is_dir():
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


def _bundled_seal(workspace: Workspace, marker: Marker, transfer_dir: Path) -> dict[str, str] | None:
    """Copy a detached job's seal into its bundle so the seal travels with it.

    A sealed payload must arrive at the destination still sealed by exactly the
    same signed document, so the seal is carried verbatim beside the manifest
    rather than re-signed. An unsealed job contributes nothing.
    """

    seal_source = job_seal_path(workspace, marker.job_key)
    if not seal_source.is_file():
        return None
    embedded = transfer_dir / "seal.json"
    embedded.write_bytes(seal_source.read_bytes())
    return {"path": "seal.json", "sha256": sha256_file(embedded)}


def _install_bundled_seal(workspace: Workspace, transfer_dir: Path, manifest: Mapping[str, Any]) -> None:
    """Restore a bundle's carried seal at the destination, refusing a conflict.

    The seal is copied byte for byte — never re-serialized — so its digest, and
    therefore any enclosing workspace or project seal that pins it, is preserved.
    A destination that already holds an identical seal is left untouched; one
    holding a different seal is corruption rather than something to overwrite.
    """

    entry = manifest.get("seal")
    if entry is None:
        return
    if not isinstance(entry, Mapping):
        raise FormatError("transfer manifest seal must be an object")
    carried = transfer_dir / str(entry["path"])
    if not carried.is_file():
        raise FormatError("transfer bundle does not carry the seal it declares")
    destination = job_seal_path(workspace, str(manifest["job_key"]))
    if destination.is_file():
        if sha256_file(destination) == str(entry["sha256"]):
            return
        raise WorkspaceCorruptionError(
            f"destination job seal for {manifest['job_key']} differs from the transferred seal"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".seal.{uuid.uuid4()}.tmp"
    staging.write_bytes(carried.read_bytes())
    os.replace(staging, destination)


def _ledger_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / f"{transfer_id}.json"


def _all_markers(workspace: Workspace) -> list[Marker]:
    return list(workspace.scan_markers(STATE_KINDS))


def _waiting_parent_map(workspace: Workspace) -> dict[str, set[str]]:
    """Map child job ids to waiting parents from one bounded scan."""

    parents: dict[str, set[str]] = {}
    for waiting in workspace.scan_markers(("waiting",)):
        state = workspace.read_state(waiting)
        join = state.get("join")
        if not isinstance(join, Mapping):
            continue
        children = join.get("children", [])
        if not isinstance(children, list):
            continue
        for child in children:
            if not isinstance(child, Mapping):
                continue
            if child.get("workspace_id") != workspace.workspace_id:
                continue
            child_id = child.get("job_id")
            if isinstance(child_id, str):
                parents.setdefault(child_id, set()).add(waiting.job_id)
    return parents


def _unresolved_join_reference(
    workspace: Workspace,
    marker: Marker,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
) -> bool:
    """Return whether *marker* is a child of an unresolved waiting join.

    The parent check intentionally scans only the ``waiting`` state subtree;
    it never enumerates all state kinds. Its cost is therefore bounded by the
    number of waiting markers, even when the workspace has a much larger job
    population.
    """

    if marker.kind == "waiting":
        return True
    parents = waiting_parent_map if waiting_parent_map is not None else _waiting_parent_map(workspace)
    return marker.job_id in parents


def _seal_transferring(workspace: Workspace, marker: Marker, state: Mapping[str, Any]) -> Path:
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
    seal = _bundled_seal(workspace, marker, transfer_dir)
    manifest = {
        "format": TRANSFER_FORMAT,
        "format_version": TRANSFER_FORMAT_VERSION,
        "core_profile": CORE_PROFILE,
        "transfer_id": transfer_id,
        "source_workspace_id": workspace.workspace_id,
        "destination_workspace_id": destination_workspace_id,
        "destination_remote": state.get("destination_remote"),
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "source_placement": marker.placement.as_posix(),
        "destination_placement": str(state["destination_placement"]),
        "payload_sha256": _payload_digest(payload),
        "runners": runners,
        "seal": seal,
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
    workspace: Workspace,
    job_id: str,
    *,
    marker: Marker | None = None,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
    destination_workspace_id: str,
    destination_remote: str | None = None,
    destination_placement: str | PurePosixPath | None = None,
    transfer_id: str | None = None,
) -> Path:
    """Fence and seal one job, leaving no schedulable source marker.

    :param workspace: Provide the source workspace.
    :param job_id: Identify the job to detach.
    :param marker: An already resolved source marker; avoids scanning all states.
    :param waiting_parent_map: Precomputed child-to-parent map for this transfer batch.
    :param destination_workspace_id: Identify the destination workspace.
    :param destination_remote: Preserve the destination's remote identifier.
    :param destination_placement: Override the destination placement.
    :param transfer_id: Reuse a transfer id when resuming sealing.
    :return: The sealed source bundle path.
    :raises ValueError: If the job is missing, active, joined, or already transferring incompatibly.
    """

    destination_id = canonical_uuid(destination_workspace_id, "destination_workspace_id")
    identifier = str(uuid.uuid4()) if transfer_id is None else canonical_uuid(transfer_id, "transfer_id")
    existing = _ledger_path(workspace, identifier)
    if existing.is_file():
        ledger = read_json(existing)
        if ledger.get("destination_workspace_id") != destination_id:
            raise WorkspaceCorruptionError("transfer UUID was reused for a different destination")
        return Path(str(ledger["bundle"]))
    if marker is None:
        matches = [candidate for candidate in _all_markers(workspace) if candidate.job_id == job_id]
        if len(matches) != 1:
            raise ValueError(f"job must have exactly one source marker: {job_id}")
        marker = matches[0]
    elif marker.job_id != job_id:
        raise ValueError(f"known source marker does not identify job: {job_id}")
    if marker.kind == "transferring":
        state = workspace.read_state(marker)
        if state.get("transfer_id") != identifier or state.get("destination_workspace_id") != destination_id:
            raise ValueError("job is already transferring under a different transfer")
        return _seal_transferring(workspace, marker, state)
    if marker.kind not in QUIESCENT_KINDS:
        raise ValueError(f"job is not quiescent and cannot transfer: {marker.kind}")
    if _unresolved_join_reference(workspace, marker, waiting_parent_map):
        raise ValueError("job participates in an unresolved join and cannot transfer")
    target_placement = normalize_placement(destination_placement or marker.placement)
    prior_state = workspace.read_state(marker)
    with workspace.open_journal_writer() as writer:
        transferring = workspace.transition(
            writer,
            marker,
            "transferring",
            {
                "transfer_id": identifier,
                "source_workspace_id": workspace.workspace_id,
                "destination_workspace_id": destination_id,
                "destination_remote": destination_remote,
                "destination_placement": target_placement.as_posix(),
                "prior_kind": marker.kind,
                "prior_state": prior_state,
                "reason": "detached_transfer",
            },
            # A sealed job may be transferred: its seal travels in the bundle and
            # is restored at the destination, so the marker move is not a mutation
            # the enforcement guard should refuse.
            allow_sealed=True,
        )
    return _seal_transferring(workspace, transferring, workspace.read_state(transferring))


def validate_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate a sealed bundle and return its manifest.

    :param bundle: Locate the sealed transfer bundle.
    :return: The validated transfer manifest.
    :raises httk.workflow.errors.FormatError: If the bundle format, digest, marker, or runner is invalid.
    """

    payload = Path(bundle).expanduser().resolve()
    manifest = read_json(payload / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
    if manifest.get("format") != TRANSFER_FORMAT or manifest.get("core_profile") != CORE_PROFILE:
        raise FormatError("unsupported detached transfer manifest")
    if manifest.get("format_version") != TRANSFER_FORMAT_VERSION:
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
        if not carried.is_file() and not carried.is_dir():
            raise FormatError(f"transfer bundle does not carry the runner it declares: {entry['path']}")
        if _runner_digest(carried) != entry["sha256"]:
            raise FormatError(f"bundled runner digest mismatch: {entry['path']}")
    seal_entry = manifest.get("seal")
    if seal_entry is not None:
        if not isinstance(seal_entry, Mapping):
            raise FormatError("transfer manifest seal must be an object")
        carried_seal = payload / TRANSFER_DIRECTORY / str(seal_entry.get("path"))
        if not carried_seal.is_file():
            raise FormatError("transfer bundle does not carry the seal it declares")
        if sha256_file(carried_seal) != seal_entry.get("sha256"):
            raise FormatError("bundled seal digest mismatch")
    return manifest


def _ack_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / "acks" / f"{transfer_id}.json"


def import_bundle(workspace: Workspace, bundle: str | os.PathLike[str]) -> dict[str, object]:
    """Idempotently import a sealed bundle and publish its prior state.

    Runners are installed and verified before the imported job becomes schedulable;
    the returned acknowledgement identifies the imported payload and transfer.

    :param workspace: Provide the destination workspace.
    :param bundle: Locate the sealed source bundle.
    :return: The destination acknowledgement.
    :raises httk.workflow.errors.FormatError: If the bundle or copied payload fails validation.
    :raises ValueError: If the bundle names another destination workspace.
    """

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
    # Signing the acknowledgement is refused for an ambiguous operator
    # identity (2+ configured identities and no default). Resolve it now,
    # before any state mutation, so that refusal cannot leave a runner
    # installed, a marker renamed, or the transfer tree removed.
    identity_seed()
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
        duplicate_ack: dict[str, object] = sign_document(
            {
                "format": "httk-workflow-transfer-acknowledgement",
                "format_version": 2,
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
        )
        transfer_dir = workspace.payload_path(duplicates[0].placement, duplicates[0].job_key) / TRANSFER_DIRECTORY
        if transfer_dir.exists():
            _install_bundled_seal(workspace, transfer_dir, manifest)
            _remove_tree(transfer_dir)
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
        "format_version": 2,
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
    with workspace.open_journal_writer() as writer:
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
    write_json_atomic(
        workspace.control / "transfers" / "imported" / f"{transfer_id}.json",
        imported_record,
        durable=workspace.durable,
    )
    _install_bundled_seal(workspace, transfer_dir, manifest)
    _remove_tree(transfer_dir)
    # The acknowledgement is what retires a sealed source, so it carries the
    # optional identity signature of whoever imported the bundle: the source can
    # then say which identity claimed the payload, not merely that somebody did.
    acknowledgement: dict[str, object] = sign_document(
        {
            "format": "httk-workflow-transfer-acknowledgement",
            "format_version": 2,
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
    )
    write_json_atomic(acknowledgement_path, acknowledgement, durable=workspace.durable)
    return acknowledgement


def _retire_sealed_bundle(
    workspace: Workspace,
    transfer_id: str,
    *,
    provenance: Mapping[str, object],
) -> Path:
    """Move one sealed source bundle aside and record the move in its ledger.

    Retirement is a rename and never a delete: the whole bundle lands under
    ``transfers/retired/`` intact, so a source is only ever fully retired or
    fully live and no interrupted retirement can leave a half-removed payload
    behind. Recognizing an already-moved bundle before validating one makes the
    step resumable across a crash between the rename and the ledger write.
    """

    ledger_path = _ledger_path(workspace, transfer_id)
    ledger = read_json(ledger_path)
    if ledger.get("status") == "retired":
        return Path(str(ledger["retired_bundle"]))
    bundle = Path(str(ledger["bundle"]))
    retired = workspace.control / "transfers" / "retired" / transfer_id / "bundle"
    if retired.exists():
        if bundle.exists():
            raise WorkspaceCorruptionError("both active and retired source bundles exist")
    else:
        validate_bundle(bundle)
        retired.parent.mkdir(parents=True, exist_ok=True)
        os.rename(bundle, retired)
    # The seal travelled to the destination with the bundle; once the source is
    # retired for good its own copy is an orphan, so drop it here and only here.
    job_seal_path(workspace, str(ledger["job_key"])).unlink(missing_ok=True)
    ledger.update({"status": "retired", "retired_bundle": str(retired), "updated_at": utc_now(), **provenance})
    write_json_atomic(ledger_path, ledger, durable=workspace.durable)
    return retired


def acknowledge_transfer(workspace: Workspace, acknowledgement: Mapping[str, object]) -> Path:
    """Validate an acknowledgement and retire the sealed source bundle.

    An acknowledgement that carries an identity signature must carry a valid
    one: retiring a source is irreversible enough that a damaged or forged
    attribution is refused rather than recorded. An acknowledgement with no
    signature is accepted exactly as before, so a destination without an
    identity key keeps working.

    :param workspace: Provide the source workspace.
    :param acknowledgement: Supply the destination acknowledgement.
    :return: The retired source bundle path.
    :raises httk.workflow.errors.FormatError: If the acknowledgement format, signature, or identity is invalid.
    """

    if acknowledgement.get("format") != "httk-workflow-transfer-acknowledgement":
        raise FormatError("invalid transfer acknowledgement format")
    signature = verify_document(acknowledgement)
    if signature.present and not signature.valid:
        raise FormatError(f"transfer acknowledgement signature is invalid: {signature.reason}")
    transfer_id = canonical_uuid(acknowledgement.get("transfer_id"), "transfer_id")
    ledger = read_json(_ledger_path(workspace, transfer_id))
    for name in ("source_workspace_id", "destination_workspace_id", "payload_sha256", "job_id", "job_key"):
        if acknowledgement.get(name) != ledger.get(name):
            raise FormatError(f"transfer acknowledgement disagrees on {name}")
    if signature.present:
        _LOGGER.info(
            "transfer %s was acknowledged by %s",
            transfer_id,
            signature.operator_key,
            extra={"event": "transfer_ack_verified", "transfer_id": transfer_id},
        )
    return _retire_sealed_bundle(workspace, transfer_id, provenance={"acknowledgement": dict(acknowledgement)})


def recover_transfers(workspace: Workspace) -> list[dict[str, object]]:
    """Finish source sealing and inventory every retained bundle.

    :param workspace: Provide the workspace whose transfers to recover.
    :return: The recovered and retained transfer records.
    """

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


# ---------------------------------------------------------------------------
# Offering finished work back to whoever sent it
# ---------------------------------------------------------------------------


def _ledgers(workspace: Workspace) -> list[dict[str, Any]]:
    """Read every transfer ledger of *workspace*, in a stable order."""

    directory = workspace.control / "transfers"
    return [read_json(path) for path in sorted(directory.glob("*.json"))] if directory.is_dir() else []


def _offer_record(ledger: Mapping[str, Any], bundle: Path) -> dict[str, object]:
    """Describe one sealed bundle the way ``tasks offer`` reports it."""

    return {
        "transfer_id": str(ledger["transfer_id"]),
        "job_id": str(ledger["job_id"]),
        "job_key": str(ledger["job_key"]),
        "state": str(ledger["prior_kind"]),
        "placement": str(ledger["destination_placement"]),
        "source_placement": str(ledger["source_placement"]),
        "payload_sha256": str(ledger["payload_sha256"]),
        "bundle_path": str(bundle),
    }


def select_transfer_jobs(
    workspace: Workspace,
    *,
    destination_workspace_id: str,
    states: Iterable[str] = DEFAULT_OFFER_STATES,
    placement: str | PurePosixPath | None = None,
    job_ids: Iterable[str] | None = None,
    destination_remote: str | None = None,
    include_transferring: bool = False,
    known_markers: Sequence[Marker] | None = None,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
) -> list[TransferCandidate]:
    """Select sealed and live jobs without changing transfer state.

    :param workspace: Provide the source workspace.
    :param destination_workspace_id: Select one destination's sealed ledgers.
    :param states: Select quiescent states eligible for offering.
    :param placement: Restrict source placements by normalized prefix.
    :param job_ids: Restrict selection to explicit job ids when supplied.
    :param destination_remote: Restrict sealed ledgers to one remote name.
    :param include_transferring: Include live interrupted-transfer markers for advisory checks.
    :param known_markers: Use these already-resolved live markers instead of scanning the workspace.
    :param waiting_parent_map: Reuse one waiting-parent map across a transfer batch.
    :return: Candidates in the same placement/key order as :func:`offer_transfers`.
    :raises ValueError: If the destination id or requested states are invalid.
    """

    destination_id = destination_workspace_id
    kinds = tuple(dict.fromkeys(states))
    if not kinds:
        raise ValueError("an offer needs at least one state kind")
    allowed_kinds = QUIESCENT_KINDS | ({"transferring"} if include_transferring else set())
    unusable = [kind for kind in kinds if kind not in allowed_kinds]
    if unusable:
        raise ValueError(f"only a quiescent job can be offered, so {', '.join(unusable)} cannot be")
    prefix = None if placement is None else normalize_placement(placement).parts
    selected_ids = None if job_ids is None else set(job_ids)
    candidates: list[TransferCandidate] = []
    offered_jobs: set[str] = set()
    parent_map = waiting_parent_map if waiting_parent_map is not None else _waiting_parent_map(workspace)
    for ledger in _ledgers(workspace):
        if ledger.get("status") != "sealed" or ledger.get("destination_workspace_id") != destination_id:
            continue
        if (
            destination_remote is not None
            and ledger.get("destination_remote", destination_remote) != destination_remote
        ):
            continue
        if ledger.get("prior_kind") not in kinds or str(ledger["job_id"]) in offered_jobs:
            continue
        if selected_ids is not None and str(ledger["job_id"]) not in selected_ids:
            continue
        source_placement = normalize_placement(str(ledger["source_placement"]))
        if prefix is not None and source_placement.parts[: len(prefix)] != prefix:
            continue
        bundle = Path(str(ledger["bundle"]))
        if not bundle.is_dir():
            continue
        manifest: Mapping[str, Any] | None = None
        problem: str | None = None
        try:
            manifest = read_json(bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
        except (FormatError, OSError) as exc:
            problem = str(exc)
        if manifest is not None:
            for field in (
                "transfer_id",
                "source_workspace_id",
                "destination_workspace_id",
                "job_id",
                "job_key",
                "source_placement",
                "destination_placement",
                "prior_kind",
            ):
                if field in ledger and manifest.get(field) != ledger[field]:
                    problem = f"sealed transfer manifest disagrees with its ledger on {field}"
                    break
        try:
            job = JobDefinition.from_path(bundle / "job.json")
        except (WorkflowError, OSError) as exc:
            job = None
            problem = str(exc)
        record = manifest if manifest is not None else ledger
        candidate_job_id = str(record.get("job_id", ledger["job_id"]))
        candidate_job_key = str(record.get("job_key", ledger["job_key"]))
        try:
            candidate_placement = normalize_placement(str(record.get("source_placement", ledger["source_placement"])))
        except ValueError as exc:
            candidate_placement = source_placement
            problem = str(exc)
        candidate_kind = str(record.get("prior_kind", ledger["prior_kind"]))
        candidates.append(
            TransferCandidate(
                candidate_job_id,
                candidate_job_key,
                candidate_kind,
                candidate_placement,
                bundle,
                None,
                job,
                manifest,
                problem,
            )
        )
        offered_jobs.add(str(ledger["job_id"]))
    marker_source = workspace.scan_markers(kinds) if known_markers is None else known_markers
    for marker in marker_source:
        if selected_ids is not None and marker.job_id not in selected_ids:
            continue
        prior_kind = marker.kind
        if marker.kind == "transferring":
            state = workspace.read_state(marker)
            if state.get("destination_workspace_id") != destination_id:
                continue
            if state.get("destination_remote") != destination_remote:
                continue
            if state.get("prior_kind") not in kinds:
                continue
            prior_kind = str(state["prior_kind"])
        if prefix is not None and marker.placement.parts[: len(prefix)] != prefix:
            continue
        if marker.job_id in offered_jobs or _unresolved_join_reference(workspace, marker, parent_map):
            continue
        try:
            job = workspace.load_job(marker)
            problem = None
        except (WorkflowError, OSError) as exc:
            job = None
            problem = str(exc)
        candidates.append(
            TransferCandidate(
                marker.job_id,
                marker.job_key,
                prior_kind,
                marker.placement,
                None,
                marker,
                job,
                None,
                problem,
            )
        )
        offered_jobs.add(marker.job_id)
    return sorted(candidates, key=lambda item: (item.source_placement.as_posix(), item.job_key))


def _offer_selection_errors(
    workspace: Workspace,
    requested_ids: set[str],
    candidates: Sequence[TransferCandidate],
    *,
    destination_workspace_id: str,
    states: Sequence[str],
    placement: str | PurePosixPath | None,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
) -> dict[str, str]:
    """Explain explicit ids that did not produce an offer candidate."""

    found = {candidate.job_id for candidate in candidates if not candidate.problem}
    missing = requested_ids - found
    if not missing:
        return {}
    prefix = None if placement is None else normalize_placement(placement).parts
    reasons: dict[str, str] = {}
    ledgers = _ledgers(workspace)
    for job_id in sorted(missing):
        marker = workspace.find_marker_by_id(job_id)
        if marker is not None:
            if marker.kind not in QUIESCENT_KINDS:
                reasons[job_id] = f"not quiescent (state: {marker.kind})"
            elif marker.kind not in states:
                reasons[job_id] = f"filtered by state (state: {marker.kind})"
            elif prefix is not None and marker.placement.parts[: len(prefix)] != prefix:
                reasons[job_id] = "filtered by placement"
            elif _unresolved_join_reference(workspace, marker, waiting_parent_map):
                reasons[job_id] = "blocked by an unresolved join"
            else:
                reasons[job_id] = "not eligible for offering"
            continue
        sealed = [
            ledger
            for ledger in ledgers
            if ledger.get("status") == "sealed"
            and ledger.get("destination_workspace_id") == destination_workspace_id
            and str(ledger.get("job_id")) == job_id
        ]
        if sealed:
            ledger = sealed[0]
            if ledger.get("prior_kind") not in states:
                reasons[job_id] = f"filtered by state (state: {ledger.get('prior_kind')})"
            elif prefix is not None and (
                normalize_placement(str(ledger["source_placement"])).parts[: len(prefix)] != prefix
            ):
                reasons[job_id] = "filtered by placement"
            else:
                reasons[job_id] = "sealed bundle is unavailable"
        else:
            reasons[job_id] = "not found"
    for candidate in candidates:
        if candidate.job_id in requested_ids and candidate.problem:
            reasons[candidate.job_id] = candidate.problem
    return reasons


def offer_transfers(
    workspace: Workspace,
    *,
    destination_workspace_id: str,
    states: Iterable[str] = DEFAULT_OFFER_STATES,
    placement: str | PurePosixPath | None = None,
    job_ids: Iterable[str] | None = None,
) -> list[dict[str, object]]:
    """Seal every finished job of *workspace* into a bundle for one destination.

    This is the far side of a results fetch: the remote that ran the work
    offers what stopped there, and the workspace that asked pulls each bundle
    and imports it. Offering is idempotent because a sealed bundle is reported
    from its ledger rather than sealed again, so the jobs a first call detached
    — which no longer have a schedulable marker — are exactly the jobs a second
    call re-offers, and an interrupted fetch resumes by simply asking again.

    A job that cannot leave right now is skipped rather than fatal: one still
    referenced by an unresolved join keeps the campaign it belongs to
    consistent, and reporting the rest lets the fetch make progress.

    :param workspace: Provide the source workspace.
    :param destination_workspace_id: Identify the destination workspace.
    :param states: Select quiescent states eligible for offering.
    :param placement: Restrict offers to this placement prefix.
    :param job_ids: Restrict the offer to these explicit job ids.
    :return: Offered transfer records in placement order.
    :raises ValueError: If the destination id or requested states are invalid.
    """

    destination_id = canonical_uuid(destination_workspace_id, "destination_workspace_id")
    kinds = tuple(dict.fromkeys(states))
    requested_ids = None if job_ids is None else set(job_ids)
    waiting_parent_map = _waiting_parent_map(workspace)
    if requested_ids is not None:
        # Validate and preflight before recovery can seal an interrupted job.
        select_transfer_jobs(
            workspace,
            destination_workspace_id=destination_id,
            states=kinds,
            job_ids=(),
            waiting_parent_map=waiting_parent_map,
        )
        precheck_candidates = select_transfer_jobs(
            workspace,
            destination_workspace_id=destination_id,
            states=(*kinds, "transferring"),
            placement=placement,
            job_ids=requested_ids,
            include_transferring=True,
            waiting_parent_map=waiting_parent_map,
        )
        errors = _offer_selection_errors(
            workspace,
            requested_ids,
            precheck_candidates,
            destination_workspace_id=destination_id,
            states=kinds,
            placement=placement,
            waiting_parent_map=waiting_parent_map,
        )
        if errors:
            details = "; ".join(f"{job_id}: {reason}" for job_id, reason in sorted(errors.items()))
            raise ValueError(f"requested transfer jobs are not all eligible: {details}")
    recover_transfers(workspace)
    offers: dict[str, dict[str, object]] = {}
    sealing_errors: list[str] = []
    candidates = select_transfer_jobs(
        workspace,
        destination_workspace_id=destination_id,
        states=kinds,
        placement=placement,
        job_ids=requested_ids,
        waiting_parent_map=waiting_parent_map,
    )
    if requested_ids is not None:
        errors = _offer_selection_errors(
            workspace,
            requested_ids,
            candidates,
            destination_workspace_id=destination_id,
            states=kinds,
            placement=placement,
            waiting_parent_map=waiting_parent_map,
        )
        if errors:
            details = "; ".join(f"{job_id}: {reason}" for job_id, reason in sorted(errors.items()))
            raise ValueError(f"requested transfer jobs are not all eligible: {details}")
    for candidate in candidates:
        if candidate.bundle is not None:
            if candidate.manifest is None:
                raise FormatError(candidate.problem or f"sealed transfer manifest is unreadable: {candidate.bundle}")
            offers[str(candidate.manifest["transfer_id"])] = _offer_record(candidate.manifest, candidate.bundle)
            continue
        try:
            assert candidate.marker is not None
            bundle = detach_job(
                workspace,
                candidate.marker.job_id,
                marker=candidate.marker,
                waiting_parent_map=waiting_parent_map,
                destination_workspace_id=destination_id,
            )
        except ValueError as exc:
            if requested_ids is not None:
                sealing_errors.append(f"{candidate.job_id}: {exc}")
                continue
            _LOGGER.warning(
                "not offering %s: %s",
                candidate.job_key,
                exc,
                extra={"event": "transfer_offer_skipped", "job_key": candidate.job_key},
            )
            continue
        manifest = read_json(bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
        offers[str(manifest["transfer_id"])] = _offer_record(manifest, bundle)
    if sealing_errors:
        details = "; ".join(sealing_errors)
        raise ValueError(
            "requested transfer jobs could not all be sealed: "
            f"{details}; already-sealed jobs remain sealed and a retry resumes them"
        )
    return sorted(offers.values(), key=lambda item: (str(item["placement"]), str(item["job_key"])))


def retire_transfers(
    workspace: Workspace,
    job_ids: Sequence[str],
    *,
    destination_workspace_id: str | None = None,
) -> list[dict[str, object]]:
    """Retire the sealed source bundle of every named job.

    A fetch retires at the source only once the destination holds an
    acknowledgement, so the identity of the job is all this side needs; naming
    the destination as well refuses to retire a bundle that was sealed for
    somebody else. Retirement moves the bundle rather than deleting it, and a
    bundle already retired is reported as such, so calling this twice is the
    same as calling it once.

    :param workspace: Provide the source workspace.
    :param job_ids: Identify the jobs whose bundles to retire.
    :param destination_workspace_id: Restrict retirement to one destination.
    :return: Retirement records for the named jobs.
    :raises ValueError: If a job id has no matching detached transfer.
    """

    destination_id = (
        None if destination_workspace_id is None else canonical_uuid(destination_workspace_id, "workspace_id")
    )
    ledgers = _ledgers(workspace)
    results: list[dict[str, object]] = []
    for job_id in job_ids:
        identifier = canonical_uuid(job_id, "job_id")
        matches = [
            ledger
            for ledger in ledgers
            if ledger.get("job_id") == identifier
            and (destination_id is None or ledger.get("destination_workspace_id") == destination_id)
        ]
        if not matches:
            raise ValueError(f"no detached transfer of this workspace names job: {identifier}")
        live = [ledger for ledger in matches if ledger.get("status") != "retired"]
        if len(live) > 1:
            raise WorkspaceCorruptionError(f"job {identifier} has several sealed transfers to retire")
        ledger = live[0] if live else matches[-1]
        transfer_id = str(ledger["transfer_id"])
        retired = _retire_sealed_bundle(workspace, transfer_id, provenance={"retired_by": "fetch"})
        results.append(
            {
                "transfer_id": transfer_id,
                "job_id": identifier,
                "job_key": str(ledger["job_key"]),
                "status": "retired",
                "retired_bundle": str(retired),
            }
        )
    return results


def discard_staged_bundle(workspace: Workspace, staging: Path) -> None:
    """Drop a staged incoming bundle whose payload the workspace now owns.

    The staging tree is renamed out of the incoming directory before it is
    removed, so an interrupted removal can never leave a partial bundle where a
    resumed fetch would find one and mistake it for the real thing.

    :param workspace: Provide the workspace owning the staging directory.
    :param staging: Locate the staged bundle to discard.
    """

    if not staging.exists():
        return
    consumed = workspace.control / "tmp" / f"consumed.{staging.name}"
    consumed.parent.mkdir(parents=True, exist_ok=True)
    if consumed.exists():
        _remove_tree(consumed)
    os.rename(staging, consumed)
    _remove_tree(consumed)
