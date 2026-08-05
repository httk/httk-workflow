"""Crash-recoverable detached job transfer."""

import hashlib
import logging
import os
import shutil
import stat
import uuid
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from ._util import read_json, sha256_file, utc_now, write_json_atomic
from .configuration import sign_document, verify_document
from .errors import FormatError, WorkspaceCorruptionError
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
    "acknowledge_transfer",
    "detach_job",
    "discard_staged_bundle",
    "import_bundle",
    "offer_transfers",
    "recover_transfers",
    "retire_transfers",
    "validate_bundle",
]

TRANSFER_DIRECTORY = ".httk-transfer"
TRANSFER_MANIFEST = "manifest.json"
TRANSFER_RUNNERS = "runners"

TRANSFER_FORMAT = "httk-workflow-detached-transfer"
#: Version 2 widened the payload digest: it now also pins the executable bit of
#: every regular file and the target of every symlink. A version 1 bundle is
#: refused by name rather than silently reported as a digest mismatch.
TRANSFER_FORMAT_VERSION = 2
#: Domain separation of the payload digest, so a digest computed by an older
#: rule can never collide with one computed by the current rule.
_PAYLOAD_DIGEST_DOMAIN = b"httk-workflow-transfer-payload-v2\0"

#: The format of what ``tasks offer`` prints and ``tasks fetch`` consumes.
TRANSFER_OFFER_FORMAT = "httk-workflow-transfer-offer"
TRANSFER_RETIREMENT_FORMAT = "httk-workflow-transfer-retirement"
#: The terminal states a results fetch collects unless told otherwise.
DEFAULT_OFFER_STATES = ("succeeded", "failed")


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


def _ledger_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / f"{transfer_id}.json"


def _all_markers(workspace: Workspace) -> list[Marker]:
    return list(workspace.scan_markers(STATE_KINDS))


def _unresolved_join_reference(workspace: Workspace, marker: Marker) -> bool:
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
            if (
                isinstance(child, Mapping)
                and child.get("workspace_id") == workspace.workspace_id
                and child.get("job_id") == marker.job_id
            ):
                return True
    return False


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
    destination_workspace_id: str,
    destination_remote: str | None = None,
    destination_placement: str | PurePosixPath | None = None,
    transfer_id: str | None = None,
) -> Path:
    """Fence and seal one job, leaving no schedulable source marker."""

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
        )
    return _seal_transferring(workspace, transferring, workspace.read_state(transferring))


def validate_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate a sealed bundle and return its manifest."""

    payload = Path(bundle).expanduser().resolve()
    manifest = read_json(payload / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
    if manifest.get("format") != TRANSFER_FORMAT or manifest.get("core_profile") != CORE_PROFILE:
        raise FormatError("unsupported detached transfer manifest")
    if manifest.get("format_version") != TRANSFER_FORMAT_VERSION:
        # Naming the version keeps an older bundle from being reported as the
        # digest mismatch it would otherwise look like: version 2 widened the
        # payload digest to cover executable bits and symlink targets.
        raise FormatError(
            f"detached transfer manifest version {manifest.get('format_version')!r} is not "
            f"{TRANSFER_FORMAT_VERSION}; the payload digest now pins executable bits and symlink "
            f"targets, so re-seal this bundle at its source: {payload}"
        )
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


def _ack_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / "acks" / f"{transfer_id}.json"


def import_bundle(workspace: Workspace, bundle: str | os.PathLike[str]) -> dict[str, object]:
    """Idempotently import a sealed bundle and publish its prior state."""

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
        duplicate_ack: dict[str, object] = sign_document(
            {
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
        )
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
    shutil.rmtree(transfer_dir)
    # The acknowledgement is what retires a sealed source, so it carries the
    # optional identity signature of whoever imported the bundle: the source can
    # then say which identity claimed the payload, not merely that somebody did.
    acknowledgement: dict[str, object] = sign_document(
        {
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


def offer_transfers(
    workspace: Workspace,
    *,
    destination_workspace_id: str,
    states: Iterable[str] = DEFAULT_OFFER_STATES,
    placement: str | PurePosixPath | None = None,
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
    """

    destination_id = canonical_uuid(destination_workspace_id, "destination_workspace_id")
    kinds = tuple(dict.fromkeys(states))
    if not kinds:
        raise ValueError("an offer needs at least one state kind")
    unusable = [kind for kind in kinds if kind not in QUIESCENT_KINDS]
    if unusable:
        raise ValueError(f"only a quiescent job can be offered, so {', '.join(unusable)} cannot be")
    prefix = None if placement is None else normalize_placement(placement).parts
    recover_transfers(workspace)
    offers: dict[str, dict[str, object]] = {}
    offered_jobs: set[str] = set()
    for ledger in _ledgers(workspace):
        if ledger.get("status") != "sealed" or ledger.get("destination_workspace_id") != destination_id:
            continue
        if ledger.get("prior_kind") not in kinds:
            continue
        source_placement = normalize_placement(str(ledger["source_placement"])).parts
        if prefix is not None and source_placement[: len(prefix)] != prefix:
            continue
        bundle = Path(str(ledger["bundle"]))
        if not bundle.is_dir():
            continue
        offers[str(ledger["transfer_id"])] = _offer_record(ledger, bundle)
        offered_jobs.add(str(ledger["job_id"]))
    for marker in list(workspace.scan_markers(kinds)):
        if prefix is not None and marker.placement.parts[: len(prefix)] != prefix:
            continue
        if marker.job_id in offered_jobs:
            continue
        try:
            bundle = detach_job(workspace, marker.job_id, destination_workspace_id=destination_id)
        except ValueError as exc:
            _LOGGER.warning(
                "not offering %s: %s",
                marker.job_key,
                exc,
                extra={"event": "transfer_offer_skipped", "job_key": marker.job_key},
            )
            continue
        manifest = read_json(bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
        offers[str(manifest["transfer_id"])] = _offer_record(manifest, bundle)
        offered_jobs.add(str(manifest["job_id"]))
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
    """

    if not staging.exists():
        return
    consumed = workspace.control / "tmp" / f"consumed.{staging.name}"
    consumed.parent.mkdir(parents=True, exist_ok=True)
    if consumed.exists():
        shutil.rmtree(consumed)
    os.rename(staging, consumed)
    shutil.rmtree(consumed)
