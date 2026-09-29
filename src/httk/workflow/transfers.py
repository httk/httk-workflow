"""Crash-recoverable detached job transfer, and ejecting and adopting jobs.

A detached bundle normally names the one workspace it is addressed to, and the
source retires it only on that destination's acknowledgement. An *ejected* job
is the same bundle addressed to no workspace (``destination_workspace_id`` is
null): :func:`eject_job` moves it out of its workspace to a free-standing job
directory and retires the source at once, and :func:`adopt_job` moves such a
directory into any workspace. A job tree travels as one such directory.
"""

import errno
import hashlib
import logging
import os
import shutil
import stat
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.digests import sha256_file, tree_digest
from httk.core.identity import identity_seed, sign_document, verify_document

from . import _transfer_receipts as receipts
from ._job_tree import bound_parent, descendant_ids, tree_children
from ._util import fsync_directory, fsync_tree, read_json, utc_now, write_json_atomic
from .errors import FormatError, WorkflowError, WorkspaceCorruptionError
from .journal import SEGMENT_HEADER, encode_record_ref, iter_record_chain, parse_record_ref, read_record
from .models import (
    CORE_PROFILE,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TERMINAL_KINDS,
    TRANSFER_DIRECTORY,
    JobDefinition,
    Marker,
    canonical_uuid,
    is_payload_private,
    normalize_placement,
    parse_job_key,
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
    "adopt_job",
    "detach_job",
    "discard_staged_bundle",
    "eject_job",
    "import_bundle",
    "offer_transfers",
    "recover_transfers",
    "retire_transfers",
    "select_transfer_jobs",
    "validate_bundle",
]

TRANSFER_MANIFEST = "manifest.json"
TRANSFER_RUNNERS = "runners"
#: Where an ejected tree root's transfer envelope carries its members.
_EJECTED_TREE = "tree"

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
    :param tree_root: The job id of the tree root this candidate travels with,
        when it is a member of a multi-job tree (the root names itself).
    :param tree_parent: The job id of this member's parent within its tree.
    :param tree_blocked: Whether the tree rules keep this candidate from leaving now;
        :attr:`problem` says why.
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
    tree_root: str | None = None
    tree_parent: str | None = None
    tree_blocked: bool = False


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


def _payload_seal_sha256(payload: Path) -> str | None:
    """Return the digest of the seal a payload carries, or ``None`` when unsealed.

    The seal lives inside the payload's ``.httk-job/``, which the payload digest
    excludes, so the manifest pins it separately: a bundle must arrive exactly as
    sealed, or exactly as unsealed, as it left.
    """

    path = job_seal_path(payload)
    return sha256_file(path) if path.is_file() else None


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


def _require_tree_boundary(workspace: Workspace, marker: Marker, *, with_tree: bool) -> None:
    """Refuse to fence a job that would split its tree."""

    payload = workspace.payload_path(marker.placement, marker.job_key)
    try:
        job = JobDefinition.from_path(payload / "job.json")
    except (WorkflowError, OSError) as exc:
        raise ValueError(f"job definition cannot be read to check its tree: {exc}") from exc
    parent = bound_parent(workspace, payload, job)
    if parent is not None:
        raise ValueError(
            f"job travels with its parent {parent.job_key}: transfer the parent, "
            "or make it independent with 'httk job detach'"
        )
    if not with_tree:
        try:
            children = tree_children(workspace, payload, job)
        except (WorkflowError, OSError) as exc:
            raise ValueError(f"its spawn records cannot be read: {exc}") from exc
        if children:
            raise ValueError(f"job has {len(children)} child job(s) that travel with it: transfer it as a tree")


def _seal_transferring(workspace: Workspace, marker: Marker, state: Mapping[str, Any]) -> Path:
    payload = workspace.payload_path(marker.placement, marker.job_key)
    transfer_dir = payload / TRANSFER_DIRECTORY
    transfer_dir.mkdir(exist_ok=True)
    transfer_id = canonical_uuid(state.get("transfer_id"), "transfer_id")
    destination_workspace_id = (
        None
        if state.get("destination_workspace_id") is None
        else canonical_uuid(state.get("destination_workspace_id"), "destination_workspace_id")
    )
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
        "seal_sha256": _payload_seal_sha256(payload),
        "prior_kind": prior_kind,
        "prior_state": dict(prior),
        "priority": marker.priority,
        "source_generation": marker.generation,
        "sealed_marker": marker.path.name,
    }
    if "transfer_sequence" in state:
        manifest["transfer_sequence"] = state["transfer_sequence"]
    if "transfer_epoch" in state:
        manifest["transfer_epoch"] = state["transfer_epoch"]
    for name in ("eject_tree", "eject_root"):
        # An ejected tree travels as one directory: the root lists its members,
        # and each member names the root it is nested in.
        if name in state:
            manifest[name] = state[name]
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
    if "eject_to" in state:
        # Where an ejection moves the bundle is source bookkeeping, not part of
        # the travelling manifest: the directory may be moved on again freely.
        ledger["eject_to"] = str(state["eject_to"])
    write_json_atomic(_ledger_path(workspace, transfer_id), ledger, durable=workspace.durable)
    if destination_workspace_id is not None:
        receipts.sealed(workspace, destination_workspace_id, transfer_id)
    return payload


@receipts.serialized
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
    with_tree: bool = False,
) -> Path:
    """Fence and seal one job, leaving no schedulable source marker.

    A spawned child that is still bound to its live parent never leaves on its
    own, and a parent leaves with its bound children only as a tree: the caller
    fences the root first (*with_tree*) and then each member, whose parent is by
    then transferring and so no longer binds it.

    :param workspace: Provide the source workspace.
    :param job_id: Identify the job to detach.
    :param marker: An already resolved source marker; avoids scanning all states.
    :param waiting_parent_map: Precomputed child-to-parent map for this transfer batch.
    :param destination_workspace_id: Identify the destination workspace.
    :param destination_remote: Preserve the destination's remote identifier.
    :param destination_placement: Override the destination placement.
    :param transfer_id: Reuse a transfer id when resuming sealing.
    :param with_tree: Fence a job whose bound children the caller transfers with it (the
        root or an intermediate member of a selected tree); the caller must fence every member.
    :return: The sealed source bundle path.
    :raises ValueError: If the job is missing, active, joined, bound to its parent, a parent
        transferred without its tree, or already transferring incompatibly.
    """

    return _detach_job(
        workspace,
        job_id,
        marker=marker,
        waiting_parent_map=waiting_parent_map,
        destination_workspace_id=canonical_uuid(destination_workspace_id, "destination_workspace_id"),
        destination_remote=destination_remote,
        destination_placement=destination_placement,
        transfer_id=transfer_id,
        with_tree=with_tree,
    )


def _detach_job(
    workspace: Workspace,
    job_id: str,
    *,
    marker: Marker | None = None,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
    destination_workspace_id: str | None,
    destination_remote: str | None = None,
    destination_placement: str | PurePosixPath | None = None,
    transfer_id: str | None = None,
    with_tree: bool = False,
    eject_to: Path | None = None,
    eject_tree: Sequence[Mapping[str, object]] | None = None,
    eject_root: str | None = None,
) -> Path:
    """Fence and seal one job as :func:`detach_job` does, or as an ejection.

    A null *destination_workspace_id* seals a bundle addressed to no workspace,
    which carries no replay sequence; *eject_to* then records where
    :func:`eject_job` moves it, so an interrupted ejection can be resumed.
    An ejected tree root records its members (*eject_tree*), and each member
    the transfer id of its root (*eject_root*).
    """

    destination_id = destination_workspace_id
    identifier = str(uuid.uuid4()) if transfer_id is None else canonical_uuid(transfer_id, "transfer_id")
    existing = _ledger_path(workspace, identifier)
    if existing.is_file():
        ledger = read_json(existing)
        if ledger.get("destination_workspace_id") != destination_id:
            raise WorkspaceCorruptionError("transfer UUID was reused for a different destination")
        return Path(str(ledger["bundle"]))
    if marker is None:
        marker = workspace.find_marker_by_id(job_id)
        if marker is None:
            interrupted = [item for item in workspace.scan_markers(("transferring",)) if item.job_id == job_id]
            if len(interrupted) != 1:
                raise ValueError(f"job must have exactly one source marker: {job_id}")
            marker = interrupted[0]
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
    _require_tree_boundary(workspace, marker, with_tree=with_tree)
    target_placement = normalize_placement(destination_placement or marker.placement)
    prior_state = workspace.read_state(marker)
    _finish_incoming_receipt(workspace, marker, prior_state)
    fields: dict[str, object] = {"transfer_id": identifier}
    if destination_id is not None:
        # Sequenced replay protection is kept per destination peer. An ejected
        # bundle has no peer; its adopter keeps an individual receipt instead.
        identifier, sequence, epoch = receipts.reserve(workspace, destination_id, job_id, identifier)
        fields = {"transfer_id": identifier, "transfer_sequence": sequence, "transfer_epoch": epoch}
    elif eject_to is not None:
        fields["eject_to"] = str(eject_to)
        if eject_tree is not None:
            fields["eject_tree"] = [dict(entry) for entry in eject_tree]
        if eject_root is not None:
            fields["eject_root"] = eject_root
    writer = workspace.open_journal_writer()
    try:
        with writer:
            if destination_id is not None:
                receipts.fencing(workspace, destination_id, identifier)
            transferring = workspace.transition(
                writer,
                marker,
                "transferring",
                {
                    **fields,
                    "source_workspace_id": workspace.workspace_id,
                    "destination_workspace_id": destination_id,
                    "destination_remote": destination_remote,
                    "destination_placement": target_placement.as_posix(),
                    "prior_kind": marker.kind,
                    "prior_state": prior_state,
                    "reason": "ejected" if destination_id is None else "detached_transfer",
                },
                # A sealed job may be transferred: its seal travels inside the payload,
                # so the marker move is not a mutation the enforcement guard should refuse.
                allow_sealed=True,
            )
    except BaseException:
        # A failed transition can leave a header-only operation writer. Use
        # the usual reference protection, including damaged/live markers.
        from .gc import collect_retired_journal

        directory = workspace.control / "journal" / writer.writer_id
        if (directory / "0.hwj").is_file() and (directory / "0.hwj").stat().st_size == len(SEGMENT_HEADER):
            reference = encode_record_ref(writer.writer_id, 0, len(SEGMENT_HEADER), 1, b"0" * 32)
            collect_retired_journal(workspace, [reference])
        raise
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
    if manifest.get("destination_workspace_id") is not None:
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
    if _payload_seal_sha256(payload) != manifest.get("seal_sha256"):
        raise FormatError("detached transfer payload seal does not match the manifest")
    return manifest


def _ack_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / "acks" / f"{transfer_id}.json"


def _prune_import_receipts(workspace: Workspace, transfer_id: str) -> None:
    """The durable sequence receipt replaces these per-transfer records."""

    for directory in ("acks", "imported"):
        path = workspace.control / "transfers" / directory / f"{transfer_id}.json"
        path.unlink(missing_ok=True)
        if workspace.durable and path.parent.exists():
            fsync_directory(path.parent)


def _finish_incoming_receipt(workspace: Workspace, marker: Marker, state: Mapping[str, Any]) -> None:
    """Finish a published import before allowing its job to leave again.

    A crashed receiver may have published the marker but not its compact
    receipt. Preserve that import fact even if the original sender has not yet
    retried. The state provenance is authoritative once its marker exists.
    """

    provenance = state.get("transfer")
    if not isinstance(provenance, Mapping) or receipts.sequence_of(provenance) is None:
        return
    transfer_dir = workspace.payload_path(marker.placement, marker.job_key) / TRANSFER_DIRECTORY
    if transfer_dir.exists():
        _remove_tree(transfer_dir)
    if not receipts.received(workspace, provenance):
        receipts.remember(workspace, provenance)
    _prune_import_receipts(workspace, str(provenance["transfer_id"]))


def _sequence_ack(workspace: Workspace, manifest: Mapping[str, Any]) -> dict[str, object]:
    """Reconstruct a receipt from an intact replay, even after the job moved on.

    Sequenced receipts intentionally have no per-job wall-clock timestamp: the
    durable range is the acknowledgement fact, and this signed envelope binds
    it to the replay's validated manifest. Source retirement still compares the
    complete envelope identity and digest against its own immutable ledger.
    """

    return sign_document(
        {
            "format": "httk-workflow-transfer-acknowledgement",
            "format_version": 2,
            "transfer_id": manifest["transfer_id"],
            "transfer_sequence": manifest["transfer_sequence"],
            **({"transfer_epoch": manifest["transfer_epoch"]} if "transfer_epoch" in manifest else {}),
            "source_workspace_id": manifest["source_workspace_id"],
            "destination_workspace_id": workspace.workspace_id,
            "payload_sha256": manifest["payload_sha256"],
            "job_id": manifest["job_id"],
            "job_key": manifest["job_key"],
            "placement": manifest["destination_placement"],
            "state": manifest["prior_kind"],
        }
    )


_UNKNOWN_MARKER = object()


@receipts.serialized
def import_bundle(workspace: Workspace, bundle: str | os.PathLike[str]) -> dict[str, object]:
    """Import one sealed bundle, or validate and acknowledge its replay."""

    return _import_one(workspace, bundle)


@receipts.serialized
def import_bundles(workspace: Workspace, bundles: Sequence[str | os.PathLike[str]]) -> list[dict[str, object]]:
    """Import a batch under one protocol lock and one destination identity scan."""

    if len(bundles) <= 1:
        return [_import_one(workspace, bundle) for bundle in bundles]
    markers: dict[str, Marker] = {}
    for marker in workspace.scan_markers(STATE_KINDS):
        if marker.job_id in markers:
            raise WorkspaceCorruptionError(f"destination has multiple markers for job UUID {marker.job_id}")
        markers[marker.job_id] = marker
    results = []
    seen: set[str] = set()
    for bundle in bundles:
        manifest = validate_bundle(bundle)
        job_id = str(manifest["job_id"])
        known = _UNKNOWN_MARKER if job_id in seen else markers.get(job_id)
        results.append(_import_one(workspace, bundle, known_marker=known))
        seen.add(job_id)
    return results


def _import_one(
    workspace: Workspace, bundle: str | os.PathLike[str], *, known_marker: object = _UNKNOWN_MARKER
) -> dict[str, object]:
    """Import once, recording a compact durable receipt before pruning metadata.

    Legacy bundles have no sequence and must retain their individual receipts.
    :param workspace: The destination workspace.
    :param bundle: The intact sealed bundle to import or replay.
    :param known_marker: Reuse a previously resolved destination marker.
    :return: A destination acknowledgement safe for source retirement.
    """

    manifest = validate_bundle(bundle)
    if manifest["destination_workspace_id"] != workspace.workspace_id:
        raise ValueError("bundle names a different destination workspace")
    marker = workspace.find_marker_by_id(str(manifest["job_id"])) if known_marker is _UNKNOWN_MARKER else known_marker
    assert marker is None or isinstance(marker, Marker)
    if receipts.sequence_of(manifest) is None:
        return _import_bundle(workspace, bundle, known_marker=marker)
    identity_seed()
    if receipts.received(workspace, manifest):
        if marker is not None:
            provenance = workspace.read_state(marker).get("transfer")
            if not isinstance(provenance, Mapping) or any(
                provenance.get(key) != manifest.get(key) for key in ("transfer_id", "payload_sha256", "transfer_epoch")
            ):
                raise WorkspaceCorruptionError("received sequence conflicts with destination job provenance")
        elif receipts.epoch_of(manifest) is None:
            # Pre-epoch compact ranges cannot distinguish a replay from a
            # reused allocator. Never fabricate an acknowledgement for them.
            raise WorkspaceCorruptionError(
                "legacy receipt has no epoch or live job; cannot safely acknowledge replay; "
                "after verifying destination delivery, retire it by job id from the source workspace: "
                f"httk workflow transfer retire . {manifest['job_id']}"
            )
    else:
        _import_bundle(workspace, bundle, known_marker=marker)
        receipts.remember(workspace, manifest)
    _prune_import_receipts(workspace, str(manifest["transfer_id"]))
    return _sequence_ack(workspace, manifest)


def _import_bundle(
    workspace: Workspace,
    bundle: str | os.PathLike[str],
    *,
    known_marker: object = _UNKNOWN_MARKER,
    placement: PurePosixPath | None = None,
    move: bool = False,
) -> dict[str, object]:
    """Idempotently import a sealed bundle and publish its prior state.

    Runners are installed and verified before the imported job becomes schedulable;
    the returned acknowledgement identifies the imported payload and transfer.
    A moving import (adoption) renames the bundle into the workspace rather than
    copying it, copying only across filesystems. An adoption intent record names
    the transfer first, so the job, which its directory then no longer holds, is
    still found and published by :func:`_finish_interrupted_adoptions`.

    :param workspace: Provide the destination workspace.
    :param bundle: Locate the sealed source bundle.
    :param known_marker: Reuse a previously resolved destination marker.
    :param placement: Place the job here instead of at the manifest's destination placement.
    :param move: Move the bundle in rather than copy it.
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
    if manifest["destination_workspace_id"] not in (None, workspace.workspace_id):
        raise ValueError("bundle names a different destination workspace")
    # Signing the acknowledgement is refused for an ambiguous operator
    # identity (2+ configured identities and no default). Resolve it now,
    # before any state mutation, so that refusal cannot leave a runner
    # installed, a marker renamed, or the transfer tree removed.
    identity_seed()
    # Runners are installed before anything about the job is published, because
    # an imported job must never become schedulable without the runner it pins.
    _install_bundled_runners(workspace, source, manifest)
    duplicate = (
        workspace.find_marker_by_id(str(manifest["job_id"])) if known_marker is _UNKNOWN_MARKER else known_marker
    )
    assert duplicate is None or isinstance(duplicate, Marker)
    if duplicate is not None:
        duplicate_state = workspace.read_state(duplicate)
        provenance = duplicate_state.get("transfer")
        if not isinstance(provenance, Mapping) or provenance.get("transfer_id") != transfer_id:
            raise FileExistsError(f"destination already contains job UUID {manifest['job_id']}")
        if provenance.get("payload_sha256") != digest:
            raise WorkspaceCorruptionError("imported marker transfer digest mismatch")
        return _acknowledge_arrival(workspace, duplicate, transfer_id, str(manifest["source_workspace_id"]), digest)
    if placement is None:
        placement = normalize_placement(str(manifest["destination_placement"]))
    target = workspace.payload_path(placement, str(manifest["job_key"]))
    if target.exists():
        if validate_bundle(target).get("payload_sha256") != digest:
            raise FileExistsError(f"destination payload collision: {target}")
    else:
        staging = _staging_path(workspace, transfer_id)
        moved = False
        if move and staging.exists():
            # The source verified above, so a leftover staging entry of this same
            # transfer (an interrupted cross-filesystem copy) is redundant.
            _remove_tree(staging)
        if staging.exists():
            if validate_bundle(staging).get("payload_sha256") != digest:
                raise WorkspaceCorruptionError("staged transfer digest mismatch")
        else:
            if move:
                write_json_atomic(
                    _adoption_intent_path(workspace, transfer_id),
                    {
                        "job_id": manifest["job_id"],
                        "job_key": manifest["job_key"],
                        "placement": placement.as_posix(),
                        "payload_sha256": digest,
                    },
                    durable=workspace.durable,
                )
                try:
                    os.rename(source, staging)
                    moved = True
                except OSError as exc:
                    if exc.errno != errno.EXDEV:
                        raise
            if not moved:
                shutil.copytree(source, staging, symlinks=True)
        try:
            if _payload_digest(staging) != digest:
                raise FormatError("copied transfer payload digest mismatch")
            if _payload_seal_sha256(staging) != manifest.get("seal_sha256"):
                raise FormatError("copied transfer payload seal mismatch")
        except BaseException:
            if moved:
                # Never strand the user's only copy in workspace staging.
                os.rename(staging, source)
                _adoption_intent_path(workspace, transfer_id).unlink(missing_ok=True)
            raise
        target.parent.mkdir(parents=True, exist_ok=True)
        workspace._publish_path(staging, target)
    if workspace.durable:
        fsync_tree(target)
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
            **({"transfer_sequence": manifest["transfer_sequence"]} if "transfer_sequence" in manifest else {}),
            **({"transfer_epoch": manifest["transfer_epoch"]} if "transfer_epoch" in manifest else {}),
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
    if workspace.durable:
        fsync_directory(workspace.control / "journal")
    imported_record = {**manifest, "status": "imported", "marker": str(imported.path), "imported_at": utc_now()}
    write_json_atomic(
        workspace.control / "transfers" / "imported" / f"{transfer_id}.json",
        imported_record,
        durable=workspace.durable,
    )
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
    _adoption_intent_path(workspace, transfer_id).unlink(missing_ok=True)
    return acknowledgement


def _acknowledge_arrival(
    workspace: Workspace, marker: Marker, transfer_id: str, source_workspace_id: str, digest: str
) -> dict[str, object]:
    """Finish an import whose job is already live here: drop its envelope, write its acknowledgement."""

    acknowledgement: dict[str, object] = sign_document(
        {
            "format": "httk-workflow-transfer-acknowledgement",
            "format_version": 2,
            "transfer_id": transfer_id,
            "source_workspace_id": source_workspace_id,
            "destination_workspace_id": workspace.workspace_id,
            "payload_sha256": digest,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "state": marker.kind,
            "acknowledged_at": utc_now(),
        }
    )
    transfer_dir = workspace.payload_path(marker.placement, marker.job_key) / TRANSFER_DIRECTORY
    if transfer_dir.exists():
        _remove_tree(transfer_dir)
    write_json_atomic(_ack_path(workspace, transfer_id), acknowledgement, durable=workspace.durable)
    _adoption_intent_path(workspace, transfer_id).unlink(missing_ok=True)
    return acknowledgement


def _staging_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "tmp" / f"import.{transfer_id}"


def _adoption_intent_path(workspace: Workspace, transfer_id: str) -> Path:
    return workspace.control / "transfers" / "adopting" / f"{transfer_id}.json"


def _finish_interrupted_adoptions(workspace: Workspace) -> None:
    """Finish every import an interrupted adoption already moved into the workspace.

    A moving import takes the job out of its directory before publishing it, so
    the directory can no longer resume it; the intent record says where the job
    is instead: in its staging entry, or at its payload path, still to be
    published, or already live by this transfer with only its envelope and
    acknowledgement left to settle. An intent is dropped only once none of these
    holds, which means the adoption never moved anything.

    :param workspace: The adopting workspace.
    """

    directory = workspace.control / "transfers" / "adopting"
    for path in sorted(directory.glob("*.json")) if directory.is_dir() else []:
        transfer_id = path.stem
        if not _ack_path(workspace, transfer_id).is_file():
            intent = read_json(path)
            placement = normalize_placement(str(intent["placement"]))
            staging = _staging_path(workspace, transfer_id)
            candidates = (staging, workspace.payload_path(placement, str(intent["job_key"])))
            held = next((item for item in candidates if _holds_bundle(item, transfer_id)), None)
            arrival = _arrival(workspace, str(intent["job_id"]))
            try:
                if held is not None:
                    _import_bundle(workspace, held, placement=placement)
                elif arrival is not None and arrival[1].get("transfer_id") == transfer_id:
                    marker, provenance = arrival
                    _acknowledge_arrival(
                        workspace,
                        marker,
                        transfer_id,
                        str(provenance["source_workspace_id"]),
                        str(provenance["payload_sha256"]),
                    )
                elif staging.exists():
                    raise FormatError(f"staged entry {staging} does not verify")
                else:
                    path.unlink(missing_ok=True)
                    continue
            except (WorkflowError, OSError, ValueError) as exc:
                _LOGGER.warning(
                    "cannot finish adopting job %s: %s",
                    intent["job_key"],
                    exc,
                    extra={"event": "adopt_pending", "transfer_id": transfer_id},
                )
                continue
            _LOGGER.info("finished an interrupted adoption of job %s", intent["job_key"])
        path.unlink(missing_ok=True)


def _retired_journal_refs(workspace: Workspace, ledger: Mapping[str, Any]) -> list[str]:
    """Remember one reference per source-chain segment before reclaiming any.

    Persisting these in the retired ledger lets a retry finish collection even
    if a crash already removed the segment linking to older source history.
    """

    references: dict[tuple[str, int], str] = {}
    reference = str(ledger.get("sealed_marker", "init")).rsplit(".", 1)[-1]
    for current, frame in iter_record_chain(workspace.control, reference, deadline_seconds=0):
        if frame is None:
            break
        writer_id, segment, *_ = parse_record_ref(current)
        references[(writer_id, segment)] = current
    return list(references.values())


def _reclaim_retired_transfer(workspace: Workspace, ledger: Mapping[str, Any]) -> None:
    """Reclaim only after the retired ledger is published; retries are safe."""

    if workspace.policy.retention.trash_days is None:
        return
    retired = Path(str(ledger["retired_bundle"]))
    if retired.exists():
        _remove_tree(retired)
    try:
        retired.parent.rmdir()
    except OSError:
        pass


def _remember_pending_journal(
    workspace: Workspace, references: Sequence[str], *, transfer_id: str | None = None
) -> None:
    """Keep shared-segment recovery once per segment, never once per job."""

    from .gc import collect_retired_journal
    from .journal import segment_path

    path = workspace.control / "transfers" / "protocol" / "journal.json"
    pending = read_json(path) if path.exists() else {}
    for reference in references:
        writer, segment, *_ = parse_record_ref(reference)
        pending[f"{writer}/{segment}"] = reference
    if not pending:
        return
    # Inventory publication precedes removal of the last per-job ledger. A
    # killed collector can always retry from this shared recovery inventory.
    write_json_atomic(path, pending, durable=workspace.durable)
    collect_retired_journal(workspace, list(pending.values()), transfer_id=transfer_id)
    if workspace.durable:
        for reference in pending.values():
            directory = segment_path(workspace.control, *parse_record_ref(reference)[:2]).parent
            if directory.exists():
                fsync_directory(directory)
        fsync_directory(workspace.control / "journal")
        retired_root = workspace.control / "transfers" / "retired"
        if retired_root.exists():
            fsync_directory(retired_root)
    pending = {
        key: ref for key, ref in pending.items() if segment_path(workspace.control, *parse_record_ref(ref)[:2]).exists()
    }
    if pending:
        write_json_atomic(path, pending, durable=workspace.durable)
    else:
        path.unlink(missing_ok=True)
        if workspace.durable:
            fsync_directory(path.parent)


def _retire_sealed_bundle(
    workspace: Workspace,
    transfer_id: str,
    *,
    provenance: Mapping[str, object],
    moved_out: bool = False,
) -> Path:
    """Atomically retire a sealed source, then reclaim its redundant history.

    The whole bundle is renamed before the retired ledger is durably written.
    Only then may deletion begin: interrupted deletion leaves a retired source,
    never a partially live bundle. Repeating retirement finishes cleanup. The
    returned path identifies the retired bundle even after its bytes are gone.
    An ejected bundle (*moved_out*) has already left the workspace as the job
    itself, so no retired copy is kept.
    """

    ledger_path = _ledger_path(workspace, transfer_id)
    if not ledger_path.exists():
        return workspace.control / "transfers" / "retired" / transfer_id / "bundle"
    ledger = read_json(ledger_path)
    if ledger.get("destination_workspace_id") is not None:
        receipts.sealed(workspace, str(ledger["destination_workspace_id"]), str(ledger["transfer_id"]))
    elif ledger.get("status") != "retired" and not moved_out:
        # An ejected bundle is the job itself, not a copy some destination holds.
        raise WorkspaceCorruptionError(f"refusing to retire ejection {transfer_id} whose job has not left")
    if ledger.get("status") != "retired":
        bundle = Path(str(ledger["bundle"]))
        retired = workspace.control / "transfers" / "retired" / transfer_id / "bundle"
        if moved_out:
            if bundle.exists():
                raise WorkspaceCorruptionError(f"ejected bundle is still inside the workspace: {bundle}")
        elif retired.exists():
            if bundle.exists():
                raise WorkspaceCorruptionError("both active and retired source bundles exist")
        else:
            validate_bundle(bundle)
            retired.parent.mkdir(parents=True, exist_ok=True)
            os.rename(bundle, retired)
        if workspace.durable and not moved_out:
            # Persist both sides of the rename and the newly created parents
            # before a ledger can durably declare the source retired.
            for directory in (bundle.parent, retired.parent, retired.parent.parent):
                fsync_directory(directory)
        ledger.update(
            {
                "status": "retired",
                "retired_bundle": str(retired),
                "retired_journal_refs": _retired_journal_refs(workspace, ledger),
                "updated_at": utc_now(),
                **provenance,
            }
        )
        write_json_atomic(ledger_path, ledger, durable=workspace.durable)
    elif "retired_journal_refs" not in ledger:
        # Older retired ledgers predate eager cleanup; inventory their history
        # before a retry can remove the links needed to find it.
        ledger["retired_journal_refs"] = _retired_journal_refs(workspace, ledger)
        write_json_atomic(ledger_path, ledger, durable=workspace.durable)
    _reclaim_retired_transfer(workspace, ledger)
    if workspace.policy.retention.trash_days is not None:
        _remember_pending_journal(workspace, ledger.get("retired_journal_refs", []), transfer_id=transfer_id)
        ledger_path.unlink(missing_ok=True)
        if workspace.durable:
            fsync_directory(ledger_path.parent)
    return Path(str(ledger["retired_bundle"]))


@receipts.serialized
def acknowledge_transfer(workspace: Workspace, acknowledgement: Mapping[str, object]) -> Path:
    """Validate one destination receipt and retire its exact source transfer."""

    return _acknowledge_transfer(workspace, acknowledgement)


@receipts.serialized
def acknowledge_transfers(workspace: Workspace, acknowledgements: Sequence[Mapping[str, object]]) -> list[Path]:
    """Retire a sweep using one journal-protection scan, rather than one per job."""

    from .gc import retirement_batch

    with retirement_batch(workspace):
        return [_acknowledge_transfer(workspace, acknowledgement) for acknowledgement in acknowledgements]


def _acknowledge_transfer(workspace: Workspace, acknowledgement: Mapping[str, object]) -> Path:
    """Validate an acknowledgement and retire the sealed source bundle.

    An acknowledgement that carries an identity signature must carry a valid
    one: retiring a source is irreversible enough that a damaged or forged
    attribution is refused rather than recorded. An acknowledgement with no
    signature is accepted exactly as before, so a destination without an
    identity key keeps working.

    :param workspace: Provide the source workspace.
    :param acknowledgement: Supply the destination acknowledgement.
    :return: The retired source bundle identity path (normally already removed).
    :raises httk.workflow.errors.FormatError: If the acknowledgement format, signature, or identity is invalid.
    """

    if acknowledgement.get("format") != "httk-workflow-transfer-acknowledgement":
        raise FormatError("invalid transfer acknowledgement format")
    signature = verify_document(acknowledgement)
    if signature.present and not signature.valid:
        raise FormatError(f"transfer acknowledgement signature is invalid: {signature.reason}")
    transfer_id = canonical_uuid(acknowledgement.get("transfer_id"), "transfer_id")
    ledger_path = _ledger_path(workspace, transfer_id)
    if not ledger_path.exists():
        # No ledger is also the terminal state after successful reclamation.
        # Do not inspect or mutate a newer incarnation of the same job.
        return workspace.control / "transfers" / "retired" / transfer_id / "bundle"
    ledger = read_json(ledger_path)
    if "transfer_sequence" in acknowledgement and acknowledgement["transfer_sequence"] != ledger.get(
        "transfer_sequence"
    ):
        raise FormatError("transfer acknowledgement disagrees on transfer_sequence")
    for name in (
        "source_workspace_id",
        "destination_workspace_id",
        "payload_sha256",
        "job_id",
        "job_key",
        "transfer_epoch",
    ):
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


@receipts.serialized
def recover_transfers(workspace: Workspace) -> list[dict[str, object]]:
    """Finish source sealing and inventory every retained bundle.

    :param workspace: Provide the workspace whose transfers to recover.
    :return: The recovered and retained transfer records.
    """

    results: list[dict[str, object]] = []
    _finish_interrupted_adoptions(workspace)
    for ledger in _ledgers(workspace):
        if ledger.get("status") == "retired":
            _retire_sealed_bundle(workspace, str(ledger["transfer_id"]), provenance={})
        elif ledger.get("destination_workspace_id") is not None:
            receipts.sealed(workspace, str(ledger["destination_workspace_id"]), str(ledger["transfer_id"]))
    for marker in list(workspace.scan_markers(("transferring",))):
        state = workspace.read_state(marker)
        bundle = _seal_transferring(workspace, marker, state)
        results.append({"transfer_id": state["transfer_id"], "status": "sealed", "bundle": str(bundle)})
    for manifest_path in workspace.root.rglob(f"{TRANSFER_DIRECTORY}/{TRANSFER_MANIFEST}"):
        if workspace.control in manifest_path.parents:
            continue
        bundle = manifest_path.parent.parent
        if TRANSFER_DIRECTORY in bundle.relative_to(workspace.root).parts:
            # A member already gathered into its ejected tree root's directory
            # has left as far as this workspace is concerned.
            continue
        try:
            manifest = validate_bundle(bundle)
        except (FormatError, OSError) as exc:
            _settle_leftover_envelope(workspace, bundle, exc)
            continue
        ledger_path = _ledger_path(workspace, str(manifest["transfer_id"]))
        if not ledger_path.exists() and manifest.get("destination_workspace_id") is None:
            # An unaddressed bundle without a ledger is either this workspace's
            # ejection, sealed just before its ledger was written (its fencing
            # frame records where it goes), or an adoption not yet published,
            # which its intent record resumes.
            ejection = _ejection_frame(workspace, manifest)
            if ejection is not None:
                write_json_atomic(
                    ledger_path,
                    {**manifest, "status": "sealed", "bundle": str(bundle), **ejection, "updated_at": utc_now()},
                    durable=workspace.durable,
                )
                results.append({"transfer_id": manifest["transfer_id"], "status": "sealed", "bundle": str(bundle)})
                continue
            _LOGGER.warning(
                "unaddressed job bundle %s has no transfer ledger; left for adoption recovery",
                bundle,
                extra={"event": "transfer_envelope_unaddressed", "transfer_id": manifest["transfer_id"]},
            )
            continue
        if not ledger_path.exists():
            write_json_atomic(
                ledger_path,
                {**manifest, "status": "sealed", "bundle": str(bundle), "updated_at": utc_now()},
                durable=workspace.durable,
            )
        results.append({"transfer_id": manifest["transfer_id"], "status": "sealed", "bundle": str(bundle)})
    for ejected in _finish_pending_ejections(workspace):
        results.append({"transfer_id": ejected["transfer_id"], "status": "ejected", "bundle": ejected["eject_to"]})
    unique = {(str(item["transfer_id"]), str(item["status"])): item for item in results}
    return list(unique.values())


def _ejection_frame(workspace: Workspace, manifest: Mapping[str, Any]) -> dict[str, str] | None:
    """Return the ledger-only ejection fields of a bundle this workspace sealed, if it is an ejection.

    :param workspace: The workspace the bundle is in.
    :param manifest: The bundle's validated manifest.
    :return: ``{"eject_to": ...}`` from the bundle's ``transferring`` frame, or ``None``.
    """

    if manifest.get("source_workspace_id") != workspace.workspace_id:
        return None
    try:
        frame = read_record(workspace.control, str(manifest["sealed_marker"]).rsplit(".", 1)[-1], deadline_seconds=0)
    except (WorkflowError, OSError, ValueError):
        return None
    if frame.get("kind") != "transferring" or frame.get("transfer_id") != manifest["transfer_id"]:
        return None
    if "eject_to" not in frame or frame.get("eject_root") != manifest.get("eject_root"):
        return None
    return {"eject_to": str(frame["eject_to"])}


def _settle_leftover_envelope(workspace: Workspace, bundle: Path, problem: Exception) -> None:
    """Handle a transfer envelope in the workspace that does not verify as a bundle.

    An import interrupted after its marker was published leaves the envelope of a
    job that is live here by that very transfer: it is removed and the
    acknowledgement completed. Anything else is reported and left alone.
    """

    transfer_id: str | None = None
    arrival = None
    try:
        manifest = read_json(bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST)
        transfer_id = canonical_uuid(manifest.get("transfer_id"), "transfer_id")
        arrival = _arrival(workspace, canonical_uuid(manifest.get("job_id"), "job_id"))
    except (WorkflowError, OSError, ValueError):
        pass
    if transfer_id is None or arrival is None or arrival[1].get("transfer_id") != transfer_id:
        _LOGGER.warning(
            "transfer envelope %s does not verify and is left in place: %s",
            bundle,
            problem,
            extra={"event": "transfer_envelope_invalid"},
        )
        return
    marker, provenance = arrival
    if workspace.payload_path(marker.placement, marker.job_key) != bundle:
        _LOGGER.warning("transfer envelope %s is not at its live job's payload; left in place", bundle)
        return
    if _ack_path(workspace, transfer_id).is_file():
        _remove_tree(bundle / TRANSFER_DIRECTORY)
    else:
        _acknowledge_arrival(
            workspace, marker, transfer_id, str(provenance["source_workspace_id"]), str(provenance["payload_sha256"])
        )


# ---------------------------------------------------------------------------
# Offering finished work back to whoever sent it
# ---------------------------------------------------------------------------


def _ledgers(workspace: Workspace) -> list[dict[str, Any]]:
    """Read every transfer ledger of *workspace*, in a stable order."""

    directory = workspace.control / "transfers"
    return [read_json(path) for path in sorted(directory.glob("*.json"))] if directory.is_dir() else []


def _offer_record(ledger: Mapping[str, Any], bundle: Path, tree_root: str | None = None) -> dict[str, object]:
    """Describe one sealed bundle the way ``tasks offer`` reports it.

    A member of a job tree also names its tree root, so the fetching side can
    tell a closure member it did not request from an unexpected offer.
    """

    record: dict[str, object] = {
        "transfer_id": str(ledger["transfer_id"]),
        "job_id": str(ledger["job_id"]),
        "job_key": str(ledger["job_key"]),
        "state": str(ledger["prior_kind"]),
        "placement": str(ledger["destination_placement"]),
        "source_placement": str(ledger["source_placement"]),
        "payload_sha256": str(ledger["payload_sha256"]),
        "bundle_path": str(bundle),
    }
    if tree_root is not None:
        record["tree_root"] = tree_root
    return record


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
    marker_source = list(workspace.scan_markers(kinds) if known_markers is None else known_markers)
    root_of: dict[str, str] = {}
    selected_ids = None
    if job_ids is not None:
        selected_ids, root_of = _with_descendants(workspace, set(job_ids), marker_source)
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
    return _rooted(_with_trees(workspace, candidates, parent_map), root_of)


def _with_descendants(
    workspace: Workspace, job_ids: set[str], markers: Sequence[Marker]
) -> tuple[set[str], dict[str, str]]:
    """Widen explicit job ids to their whole trees, in whatever state each member is.

    A selected root reaches its members through the spawn records in its payload,
    live or already sealed, so re-running an interrupted tree transfer picks up
    every member the interruption left behind: sealed ones through their ledgers,
    live ones through their markers. Roots are located among *markers*, the ones
    the selection scans anyway, so widening adds no workspace scan.

    :param workspace: The source workspace.
    :param job_ids: The explicitly requested job ids.
    :param markers: The live markers the selection scanned.
    :return: The widened ids, and each added descendant's requested root.
    """

    widened = set(job_ids)
    root_of: dict[str, str] = {}
    live = {marker.job_id: marker for marker in markers if marker.job_id in job_ids}
    sealed = {
        str(ledger.get("job_id")): ledger
        for ledger in _ledgers(workspace)
        if ledger.get("status") == "sealed" and str(ledger.get("job_id")) in job_ids
    }
    for job_id in sorted(job_ids):
        bundle = sealed.get(job_id, {}).get("bundle")
        if isinstance(bundle, str):
            payload = Path(bundle)
        elif job_id in live:
            payload = workspace.payload_path(live[job_id].placement, live[job_id].job_key)
        else:
            continue
        try:
            descendants = descendant_ids(workspace, payload, JobDefinition.from_path(payload / "job.json"))
        except (WorkflowError, OSError):
            continue
        for descendant in descendants - job_ids:
            root_of.setdefault(descendant, job_id)
        widened |= descendants
    return widened, root_of


def _rooted(candidates: list[TransferCandidate], root_of: Mapping[str, str]) -> list[TransferCandidate]:
    """Name the requested root every widened-in candidate travels with.

    A member an interrupted transfer left behind is sealed, or live but no longer
    bound (its parent is already transferring), so the tree pass cannot see its
    root; the widening that selected it can, and the offer carries that root.
    """

    if not root_of:
        return candidates
    rooted: list[TransferCandidate] = []
    for candidate in candidates:
        root = root_of.get(candidate.tree_root or candidate.job_id)
        rooted.append(candidate if root is None else replace(candidate, tree_root=root))
    return rooted


# A tree member other than its root must be one no manager can claim, so that
# nothing in the tree can start between the eligibility check and its fence.
_TREE_MEMBER_KINDS = TERMINAL_KINDS | {"paused"}


def _with_trees(
    workspace: Workspace,
    candidates: Sequence[TransferCandidate],
    waiting_parent_map: Mapping[str, set[str]],
) -> list[TransferCandidate]:
    """Group live candidates into whole trees, root first, and block split trees.

    A live candidate bound to its parent never leaves alone: it comes back as a
    member of its parent's tree when that parent is selected too, and is blocked
    otherwise. A selected parent brings every bound descendant, whatever the
    selection filters say, provided each is paused or terminal and outside any
    unresolved join; otherwise the whole tree is blocked.
    """

    ordered = sorted(candidates, key=lambda item: (item.source_placement.as_posix(), item.job_key))
    live = {
        item.job_id
        for item in ordered
        if item.marker is not None and item.marker.kind != "transferring" and item.job is not None and not item.problem
    }
    # First decide every tree, so a member is emitted exactly once: inside its
    # root's tree, never also as a blocked candidate of its own.
    parents: dict[str, Marker] = {}
    trees: dict[str, tuple[list[TransferCandidate], list[str]]] = {}
    for candidate in ordered:
        if candidate.job_id not in live:
            continue
        assert candidate.marker is not None and candidate.job is not None
        payload = workspace.payload_path(candidate.marker.placement, candidate.marker.job_key)
        parent = bound_parent(workspace, payload, candidate.job)
        if parent is not None:
            parents[candidate.job_id] = parent
            continue
        try:
            trees[candidate.job_id] = _tree_members(workspace, candidate, waiting_parent_map)
        except (WorkflowError, OSError) as exc:
            trees[candidate.job_id] = ([], [f"its spawn records cannot be read ({exc})"])
    included = {member.job_id for members, blockers in trees.values() if not blockers for member in members}
    blocked_parents = {job_id for job_id, (_members, blockers) in trees.items() if blockers}
    result: list[TransferCandidate] = []
    for candidate in ordered:
        if candidate.job_id not in live:
            result.append(candidate)
        elif candidate.job_id in included:
            continue
        elif candidate.job_id in parents:
            parent = parents[candidate.job_id]
            if parent.job_id in blocked_parents:
                problem = f"travels with its parent {parent.job_key}, whose tree cannot leave yet"
            elif parent.job_id in live:
                # The parent leaves but does not list it: it spawned before tree records existed.
                problem = (
                    f"travels with its parent {parent.job_key}, which has no record of it: transfer it once "
                    "the parent has left, or make it independent with 'httk job detach'"
                )
            else:
                problem = (
                    f"travels with its parent {parent.job_key}: transfer the parent, "
                    "or make it independent with 'httk job detach'"
                )
            result.append(replace(candidate, problem=problem, tree_blocked=True))
        else:
            members, blockers = trees[candidate.job_id]
            if blockers:
                reason = f"its tree cannot leave yet: {'; '.join(blockers)} (wait for them to end, or pause them)"
                result.append(replace(candidate, problem=reason, tree_blocked=True))
            elif members:
                result.append(replace(candidate, tree_root=candidate.job_id))
                result.extend(members)
            else:
                result.append(candidate)
    return result


def _tree_members(
    workspace: Workspace,
    root: TransferCandidate,
    waiting_parent_map: Mapping[str, set[str]],
) -> tuple[list[TransferCandidate], list[str]]:
    """Return the bound descendants of *root*, top-down, and what blocks them."""

    assert root.marker is not None and root.job is not None
    members: list[TransferCandidate] = []
    blockers: list[str] = []
    seen = {root.job_id}
    queue: list[tuple[Marker, JobDefinition]] = [(root.marker, root.job)]
    while queue:
        parent_marker, parent_job = queue.pop(0)
        parent_payload = workspace.payload_path(parent_marker.placement, parent_marker.job_key)
        for child_marker, child_job in tree_children(workspace, parent_payload, parent_job):
            if child_marker.job_id in seen:
                continue
            seen.add(child_marker.job_id)
            if child_marker.kind not in _TREE_MEMBER_KINDS:
                blockers.append(f"{child_marker.job_key} is {child_marker.kind}")
            elif _unresolved_join_reference(workspace, child_marker, waiting_parent_map):
                blockers.append(f"{child_marker.job_key} is in an unresolved join")
            members.append(
                TransferCandidate(
                    child_marker.job_id,
                    child_marker.job_key,
                    child_marker.kind,
                    child_marker.placement,
                    None,
                    child_marker,
                    child_job,
                    tree_root=root.job_id,
                    tree_parent=parent_job.id,
                )
            )
            queue.append((child_marker, child_job))
    return members, blockers


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
        # A missing exact UUID may already have completed and been pruned.
        # Live but ineligible jobs and broken sealed bundles still fail above.
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
    stranded: set[str] = set()
    for candidate in candidates:
        if candidate.bundle is not None:
            if candidate.manifest is None:
                raise FormatError(candidate.problem or f"sealed transfer manifest is unreadable: {candidate.bundle}")
            offers[str(candidate.manifest["transfer_id"])] = _offer_record(
                candidate.manifest, candidate.bundle, candidate.tree_root
            )
            continue
        if candidate.tree_blocked:
            _LOGGER.warning(
                "not offering %s: %s",
                candidate.job_key,
                candidate.problem,
                extra={"event": "transfer_offer_skipped", "job_key": candidate.job_key},
            )
            continue
        if candidate.tree_parent in stranded:
            # Its parent stayed behind, so it stays too, still bound to it.
            stranded.add(candidate.job_id)
            continue
        try:
            assert candidate.marker is not None
            bundle = detach_job(
                workspace,
                candidate.marker.job_id,
                marker=candidate.marker,
                waiting_parent_map=waiting_parent_map,
                destination_workspace_id=destination_id,
                with_tree=candidate.tree_root is not None,
            )
        except ValueError as exc:
            stranded.add(candidate.job_id)
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
        offers[str(manifest["transfer_id"])] = _offer_record(manifest, bundle, candidate.tree_root)
    if sealing_errors:
        details = "; ".join(sealing_errors)
        raise ValueError(
            "requested transfer jobs could not all be sealed: "
            f"{details}; already-sealed jobs remain sealed and a retry resumes them"
        )
    return sorted(offers.values(), key=lambda item: (str(item["placement"]), str(item["job_key"])))


@receipts.serialized
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
    somebody else. Retirement renames the bundle, publishes its retired ledger,
    then reclaims the bundle and unprotected source journal segments unless
    retention says to keep them. Repeated retirement also retries cleanup.

    :param workspace: Provide the source workspace.
    :param job_ids: Identify the jobs whose bundles to retire.
    :param destination_workspace_id: Restrict retirement to one destination.
    :return: Retirement records for the named jobs.
    :raises ValueError: If a job id has no matching detached transfer, or is being ejected.
    """

    destination_id = (
        None if destination_workspace_id is None else canonical_uuid(destination_workspace_id, "workspace_id")
    )
    ledgers = _ledgers(workspace)
    results: list[dict[str, object]] = []
    for job_id in job_ids:
        identifier = canonical_uuid(job_id, "job_id")
        named = [ledger for ledger in ledgers if ledger.get("job_id") == identifier]
        # An ejection is never retired by name: its bundle is the job itself
        # until the ejection has moved it out.
        matches = [
            ledger
            for ledger in named
            if ledger.get("destination_workspace_id") is not None
            and (destination_id is None or ledger.get("destination_workspace_id") == destination_id)
        ]
        if not matches and any(
            ledger.get("destination_workspace_id") is None and ledger.get("status") != "retired" for ledger in named
        ):
            raise ValueError(
                f"job {identifier} is not retirable: ejection in progress; "
                "finish it with `httk job eject` or transfer recovery"
            )
        if not matches:
            if workspace.find_marker_by_id(identifier) is not None:
                raise ValueError(f"no detached transfer of this workspace names job: {identifier}")
            continue
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


# ---------------------------------------------------------------------------
# Ejecting jobs to free-standing directories, and adopting them back
# ---------------------------------------------------------------------------


def _eject_destination(workspace: Workspace, target: str | os.PathLike[str], job_key: str) -> Path:
    """Resolve where one job is ejected to, the way ``mv`` resolves its target.

    An existing directory receives the job as ``<directory>/<job_key>``; any
    other path names the new job directory itself, whose parent must exist.
    The result is never inside a workspace, where the directory would read as
    that workspace's own sealed bundle.
    """

    path = Path(target).expanduser()
    if path.is_dir():
        path = path / job_key
    path = path.parent.resolve() / path.name
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"eject destination already exists: {path}")
    if not path.parent.is_dir():
        raise ValueError(f"eject destination parent is not a directory: {path.parent}")
    enclosing = Workspace.discover(path.parent)
    if enclosing is not None:
        relation = "this" if enclosing.resolve() == workspace.root.resolve() else "a"
        raise ValueError(
            f"eject destination {path} is inside {relation} workspace ({enclosing}); "
            "eject to a directory outside every workspace, then adopt it where it should go"
        )
    return path


def _holds_bundle(path: Path, transfer_id: str, payload_sha256: str | None = None) -> bool:
    """Report whether *path* is an intact copy of exactly this sealed bundle."""

    try:
        manifest = validate_bundle(path)
    except (FormatError, OSError):
        return False
    return manifest.get("transfer_id") == transfer_id and payload_sha256 in (None, manifest.get("payload_sha256"))


def _move_bundle_out(
    workspace: Workspace,
    bundle: Path,
    target: Path,
    transfer_id: str,
    digest: str,
    tree: Sequence[Mapping[str, Any]] = (),
) -> None:
    """Move a sealed bundle to *target*, by rename or by verified copy.

    Within one filesystem the move is a single rename. Across filesystems the
    bundle is copied to a hidden sibling of *target*, verified (with the *tree*
    members it carries), and renamed into place before the workspace copy is
    removed, so an interruption leaves either the bundle in the workspace or a
    complete copy at *target*, never neither.
    """

    try:
        os.rename(bundle, target)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
    else:
        if workspace.durable:
            fsync_directory(target.parent)
            fsync_directory(bundle.parent)
        return
    staging = target.parent / f".{target.name}.eject-{transfer_id}"
    if staging.exists():
        _remove_tree(staging)
    shutil.copytree(bundle, staging, symlinks=True)
    if not _holds_bundle(staging, transfer_id, digest) or not all(
        _holds_bundle(_nested_member(staging, entry), str(entry["transfer_id"])) for entry in tree
    ):
        _remove_tree(staging)
        raise FormatError(f"copied ejected bundle does not verify: {staging}")
    if workspace.durable:
        fsync_tree(staging)
    os.rename(staging, target)
    if workspace.durable:
        fsync_directory(target.parent)
    _remove_tree(bundle)


def _nested_member(root_bundle: Path, entry: Mapping[str, Any]) -> Path:
    """Return where an ejected tree root's directory carries one member.

    :param root_bundle: The root's bundle directory.
    :param entry: The member's ``eject_tree`` entry.
    :return: The member's bundle directory inside the root's transfer envelope.
    :raises httk.workflow.errors.FormatError: If the entry's placement or job key is unsafe.
    """

    job_key = str(entry.get("job_key"))
    if parse_job_key(job_key)[1] != entry.get("job_id"):
        raise FormatError(f"ejected tree entry names the job key of another job: {job_key}")
    placement = normalize_placement(str(entry.get("placement")))
    return root_bundle / TRANSFER_DIRECTORY / _EJECTED_TREE / Path(*placement.parts) / job_key


def _ejected_tree(manifest: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Return the member entries an ejected job's manifest lists (none for a single job)."""

    tree = manifest.get("eject_tree") or []
    if not isinstance(tree, list) or not all(isinstance(entry, Mapping) for entry in tree):
        raise FormatError("ejected job directory has a malformed eject_tree")
    return tree


def _gather_tree(workspace: Workspace, root: Mapping[str, Any]) -> None:
    """Fence every member of an ejected tree and move it into its root's directory.

    Members are handled top-down under the transfer ids the root reserved for
    them, so a resumed call finds each one wherever an interruption left it:
    still live (it is fenced now), sealed in the workspace (it is moved), or
    already inside the root's directory (only its retirement remains). The root
    is still in the workspace throughout, so the nested targets are too.
    """

    root_id = str(root["transfer_id"])
    root_bundle = Path(str(root["bundle"]))
    waiting: dict[str, set[str]] | None = None
    for entry in _ejected_tree(root):
        job_id, job_key = str(entry["job_id"]), str(entry["job_key"])
        member_id = canonical_uuid(entry.get("transfer_id"), "transfer_id")
        nested = _nested_member(root_bundle, entry)
        ledger_path = _ledger_path(workspace, member_id)
        if not ledger_path.is_file():
            marker = workspace.find_marker_by_id(job_id)
            if marker is None:
                if root_bundle.exists() and not _holds_bundle(nested, member_id):
                    raise WorkspaceCorruptionError(
                        f"ejected tree member {job_key} is neither in the workspace nor in its root's directory"
                    )
                continue  # it is inside the root's directory and already retired
            if waiting is None:
                waiting = _waiting_parent_map(workspace)
            try:
                _detach_job(
                    workspace,
                    job_id,
                    marker=marker,
                    waiting_parent_map=waiting,
                    destination_workspace_id=None,
                    transfer_id=member_id,
                    with_tree=True,
                    eject_to=nested,
                    eject_root=root_id,
                )
            except ValueError as exc:
                raise ValueError(
                    f"tree member {job_key} cannot leave now ({exc}); its root stays sealed, and the "
                    "ejection resumes once it can (`httk job eject` again, or any command that recovers transfers)"
                ) from exc
        member = read_json(ledger_path)
        if member.get("job_id") != job_id or member.get("eject_root") != root_id:
            raise WorkspaceCorruptionError(f"transfer ledger {member_id} is not the ejected tree member {job_key}")
        member_bundle = Path(str(member["bundle"]))
        if member.get("status") != "retired" and member_bundle.exists():
            if not root_bundle.is_dir():
                raise WorkspaceCorruptionError(f"ejected tree root left without its member {job_key}")
            digest = str(member["payload_sha256"])
            if _holds_bundle(nested, member_id, digest):
                _remove_tree(member_bundle)
            else:
                created = [parent for parent in nested.parents if not parent.exists()]
                nested.parent.mkdir(parents=True, exist_ok=True)
                if workspace.durable:
                    for directory in created:
                        fsync_directory(directory.parent)
                _move_bundle_out(workspace, member_bundle, nested, member_id, digest)
        _retire_sealed_bundle(workspace, member_id, provenance={"ejected_into": root_id}, moved_out=True)


def _finish_ejection(workspace: Workspace, ledger: Mapping[str, Any]) -> Path:
    """Move one sealed, unaddressed bundle to its recorded target and retire it.

    Every step is resumable from the ledger alone: the bundle is still in the
    workspace and is moved, or a complete copy is already at the target and the
    leftover workspace copy is dropped, or the bundle has left and only the
    retirement remains. A tree root first gathers its members into its own
    directory, so the whole tree leaves in one move.
    """

    transfer_id = str(ledger["transfer_id"])
    digest = str(ledger["payload_sha256"])
    bundle = Path(str(ledger["bundle"]))
    target = Path(str(ledger["eject_to"]))
    tree = _ejected_tree(ledger)
    if tree:
        _gather_tree(workspace, ledger)
    if bundle.exists():
        if _holds_bundle(target, transfer_id, digest):
            _remove_tree(bundle)
        elif target.exists() or target.is_symlink():
            raise FileExistsError(
                f"eject destination {target} was taken by something else; the job stays sealed in its "
                "workspace until that path is moved away and the ejection is resumed "
                "(`httk job eject` again, or any command that recovers transfers)"
            )
        else:
            _move_bundle_out(workspace, bundle, target, transfer_id, digest, tree)
    # A bundle is removed from the workspace only once a verified copy is at the
    # target, so a bundle that is gone has left; the directory may since have
    # been moved on or adopted, which is no concern of this workspace.
    _retire_sealed_bundle(workspace, transfer_id, provenance={"ejected_to": str(target)}, moved_out=True)
    return target


def _finish_pending_ejections(workspace: Workspace) -> list[dict[str, Any]]:
    """Resume every ejection an earlier process sealed but did not finish.

    Tree members are finished through their root, never on their own.

    :param workspace: The workspace the jobs are leaving.
    :return: The ledgers of the ejections this call finished.
    """

    for marker in list(workspace.scan_markers(("transferring",))):
        state = workspace.read_state(marker)
        if "eject_to" in state:
            _seal_transferring(workspace, marker, state)
    finished = []
    for ledger in _ledgers(workspace):
        if ledger.get("status") != "sealed" or "eject_to" not in ledger:
            continue
        if "eject_root" in ledger:
            if not _ledger_path(workspace, str(ledger["eject_root"])).exists() and Path(str(ledger["bundle"])).exists():
                # Its root has no record left to gather it by; moving it anywhere
                # would be a guess, so it stays sealed where it is.
                _LOGGER.warning(
                    "ejected tree member %s is still in the workspace, but its root's ejection has no record",
                    ledger.get("job_key"),
                    extra={"event": "eject_orphan_member", "transfer_id": ledger.get("transfer_id")},
                )
            continue
        try:
            _finish_ejection(workspace, ledger)
        except (WorkflowError, OSError, ValueError) as exc:
            # One stuck ejection (say, a destination taken meanwhile) must not
            # block every other job from leaving; it stays sealed and resumable.
            _LOGGER.warning(
                "cannot finish ejecting job %s: %s",
                ledger.get("job_key"),
                exc,
                extra={"event": "eject_pending", "transfer_id": ledger.get("transfer_id")},
            )
            continue
        finished.append(ledger)
    return finished


def _eject_tree_of(workspace: Workspace, marker: Marker, waiting: Mapping[str, set[str]]) -> list[dict[str, Any]]:
    """List the bound descendants an ejected job takes along, top-down, or refuse the tree.

    Each member's transfer id is reserved here, so a resumed ejection fences a
    member it finds still live under the same id and finds a sealed one by it.
    """

    try:
        job = JobDefinition.from_path(workspace.payload_path(marker.placement, marker.job_key) / "job.json")
        candidate = TransferCandidate(marker.job_id, marker.job_key, marker.kind, marker.placement, None, marker, job)
        members, blockers = _tree_members(workspace, candidate, waiting)
    except (WorkflowError, OSError) as exc:
        raise ValueError(f"job definition or spawn records cannot be read to check its tree: {exc}") from exc
    if blockers:
        raise ValueError(f"its tree cannot leave yet: {'; '.join(blockers)} (wait for them to end, or pause them)")
    return [
        {
            "job_id": member.job_id,
            "job_key": member.job_key,
            "placement": member.source_placement.as_posix(),
            "parent_job_id": member.tree_parent,
            "transfer_id": str(uuid.uuid4()),
        }
        for member in members
    ]


@receipts.serialized
def eject_job(
    workspace: Workspace,
    job_id: str,
    target: str | os.PathLike[str],
    *,
    marker: Marker | None = None,
) -> Path:
    """Move one quiescent job out of its workspace to a free-standing job directory.

    The job is fenced and sealed as a detached bundle addressed to no workspace,
    moved to *target*, and retired from the source, which keeps no copy. The
    directory is the whole job: its payload, seal, tree metadata, prior state,
    and any shared runner it pins. A job with bound children takes its whole
    tree along: every descendant, each of which must be paused or terminal, is
    sealed the same way and nested in the directory under
    ``.httk-transfer/tree/<placement>/<job_key>``. Any ejection an interrupted
    earlier call left unfinished is completed first.

    :param workspace: The workspace the job leaves.
    :param job_id: The job to eject.
    :param target: The new job directory, or an existing directory to eject into.
    :param marker: The already resolved job marker, when available.
    :return: The free-standing job directory.
    :raises ValueError: If the job or its tree cannot leave its workspace or the target is unusable.
    :raises FileExistsError: If the target job directory already exists.
    :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
    """

    _finish_pending_ejections(workspace)
    if marker is None:
        marker = workspace.find_marker_by_id(job_id)
        if marker is None:
            raise ValueError(f"no job {job_id} in this workspace")
    destination = _eject_destination(workspace, target, marker.job_key)
    waiting = _waiting_parent_map(workspace)
    tree = _eject_tree_of(workspace, marker, waiting)
    transfer_id = str(uuid.uuid4())
    _detach_job(
        workspace,
        job_id,
        marker=marker,
        waiting_parent_map=waiting,
        destination_workspace_id=None,
        transfer_id=transfer_id,
        with_tree=bool(tree),
        eject_to=destination,
        eject_tree=tree,
    )
    return _finish_ejection(workspace, read_json(_ledger_path(workspace, transfer_id)))


def _arrival(workspace: Workspace, job_id: str) -> tuple[Marker, Mapping[str, Any]] | None:
    """Return a live job's marker with the transfer provenance it arrived here by, if any.

    The import frame that starts the job's history here names the transfer it
    came by; later frames do not repeat it, so the history is walked to its
    start. An unreadable history answers ``None``, which keeps a directory in place.
    """

    marker = workspace.find_marker_by_id(job_id)
    if marker is None:
        return None
    first: Mapping[str, Any] | None = None
    for _reference, frame in iter_record_chain(workspace.control, marker.record_ref, deadline_seconds=0):
        if frame is None:
            return None
        first = frame
    provenance = None if first is None else first.get("transfer")
    return (marker, provenance) if isinstance(provenance, Mapping) else None


def _adopted_through(workspace: Workspace, job_id: str, transfer_id: str) -> bool:
    """Report whether this workspace's live copy of a job arrived by *transfer_id*.

    This, not the acknowledgement (which garbage collection expires), is what
    says an adoption already brought the job here.
    """

    arrival = _arrival(workspace, job_id)
    return arrival is not None and arrival[1].get("transfer_id") == transfer_id


def _adoption_needed(
    workspace: Workspace, bundle: Path, manifest: Mapping[str, Any], placement: PurePosixPath | None
) -> bool:
    """Decide, without changing anything, whether one verified bundle still has to be imported.

    A job already live here by this very transfer has arrived (an interrupted
    adoption); an acknowledged transfer whose job has since moved on makes the
    bundle a stale copy; and a job id or payload path this workspace already
    uses for something else is a collision.

    :param workspace: The adopting workspace.
    :param bundle: The verified bundle directory.
    :param manifest: Its validated manifest.
    :param placement: Where it is to be placed, when not at its recorded placement.
    :return: Whether the bundle still has to be imported.

    :raises ValueError: If the bundle is a stale copy.
    :raises FileExistsError: If its job or its payload path is already taken here.
    """

    transfer_id = str(manifest["transfer_id"])
    if _adopted_through(workspace, str(manifest["job_id"]), transfer_id):
        return False
    if _ack_path(workspace, transfer_id).is_file():
        raise ValueError(
            f"{bundle} was already adopted into this workspace once and its job has since moved on; "
            "it is a stale copy and was left in place"
        )
    if workspace.find_marker_by_id(str(manifest["job_id"])) is not None:
        raise FileExistsError(f"this workspace already holds job {manifest['job_id']}; {bundle} was left in place")
    target = workspace.payload_path(
        placement or normalize_placement(str(manifest["destination_placement"])), str(manifest["job_key"])
    )
    if (target.exists() or target.is_symlink()) and not _holds_bundle(target, transfer_id):
        raise FileExistsError(f"payload path {target} is already taken; {bundle} was left in place")
    return True


@receipts.serialized
def adopt_job(
    workspace: Workspace,
    directory: str | os.PathLike[str],
    *,
    placement: str | PurePosixPath | None = None,
) -> Marker:
    """Move one free-standing job directory into a workspace.

    The directory must be an ejected job (a bundle addressed to no workspace).
    It is verified, moved in (renamed on one filesystem, else copied, verified,
    and removed), imported exactly as a transfer is, and restored to the state it
    was ejected in. An ejected tree brings back every member nested in it, each
    at the placement it left from, so the parent/child bindings hold again. A
    copy of a directory whose job has already passed through this workspace is
    refused and left in place rather than resurrecting a stale job.

    :param workspace: The workspace the job joins.
    :param directory: The free-standing job directory.
    :param placement: Place the job here instead of where it was ejected from (not for a tree).
    :return: The adopted job's marker (a tree's root).
    :raises ValueError: If the directory is inside the workspace, addressed to a workspace, or stale,
        or a placement is given for a tree.
    :raises FileExistsError: If the workspace already holds this job or its payload path.
    :raises httk.workflow.errors.FormatError: If the directory or a tree member does not verify or is missing.
    """

    workspace._require_unsealed()
    _finish_interrupted_adoptions(workspace)
    source = Path(directory).expanduser().resolve()
    root = workspace.root.resolve()
    if source == root or root in source.parents:
        raise ValueError(f"{source} is already inside the workspace")
    manifest = validate_bundle(source)
    if manifest.get("destination_workspace_id") is not None:
        raise ValueError(
            f"{source} is a transfer bundle addressed to workspace {manifest['destination_workspace_id']}; "
            "move it with `httk job transfer` instead"
        )
    if manifest.get("eject_root") is not None:
        raise ValueError(f"{source} is a member of an ejected job tree; adopt the tree's root directory instead")
    transfer_id = str(manifest["transfer_id"])
    tree = _ejected_tree(manifest)
    if tree and placement is not None:
        raise ValueError("a job tree keeps its placements; adopt it without --placement")
    # Check every member before changing anything, so a refused tree is left
    # exactly as it was.
    pending: list[tuple[Path, PurePosixPath | None]] = []
    root_arrived = _adopted_through(workspace, str(manifest["job_id"]), transfer_id)
    for entry in tree:
        member_dir = _nested_member(source, entry)
        member_id = canonical_uuid(entry.get("transfer_id"), "transfer_id")
        if _adopted_through(workspace, str(entry["job_id"]), member_id):
            continue  # an interrupted adoption already brought it here
        if root_arrived:
            # The members of one ejected tree arrive before their root does.
            raise ValueError(
                f"{source} was already adopted into this workspace once and its tree has since moved on; "
                "it is a stale copy and was left in place"
            )
        if not member_dir.is_dir():
            raise FormatError(f"tree member {entry['job_key']} is missing from {source}")
        member = validate_bundle(member_dir)
        if (
            member["transfer_id"] != member_id
            or member["job_id"] != entry["job_id"]
            or member.get("destination_workspace_id") is not None
            or member.get("eject_root") != transfer_id
        ):
            raise FormatError(f"tree member {member_dir} does not belong to this ejected tree")
        if _adoption_needed(workspace, member_dir, member, None):
            pending.append((member_dir, None))
    root_placement = None if placement is None else normalize_placement(placement)
    if _adoption_needed(workspace, source, manifest, root_placement):
        pending.append((source, root_placement))
    # Members first: they are nested in the root's transfer envelope, which
    # importing the root removes.
    for bundle, target_placement in pending:
        _import_bundle(workspace, bundle, placement=target_placement, move=True)
    adopted = workspace.find_marker_by_id(str(manifest["job_id"]))
    if adopted is None:
        raise WorkspaceCorruptionError(f"adopted job {manifest['job_key']} has no marker; {source} was left in place")
    if source.exists():
        # Only a cross-filesystem copy, or an earlier interrupted adoption, leaves it.
        _remove_tree(source)
    if workspace.durable:
        fsync_directory(source.parent)
    return adopted
