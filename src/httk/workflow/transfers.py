"""Crash-recoverable detached job transfer, and ejecting and adopting jobs.

A detached bundle normally names the one workspace it is addressed to, and the
source retires it only on that destination's acknowledgement. An *ejected* job
is the same bundle addressed to no workspace (``destination_workspace_id`` is
null): :func:`eject_job` moves it out of its workspace to a free-standing job
directory and retires the source at once, and :func:`adopt_job` moves such a
directory into any workspace. A job tree travels as one such directory.
"""

import json
import logging
import os
import stat
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.identity import verify_document

from . import _txn
from ._adoption import AdoptionResult, Source, adopt, recover_lineages
from ._bundle import (
    _MANIFEST_BYTES,
    TRANSFER_FORMAT,
    TRANSFER_FORMAT_VERSION,
    TRANSFER_MANIFEST,
    TRANSFER_RUNNERS,
    BundleSource,
    check_bundle,
)
from ._job_tree import bound_parent, descendant_ids, tree_children
from ._receipts import CLOCK_SKEW_NS, FRESHNESS_WINDOW_NS
from ._sealing import (
    Commit,
    Transaction,
    _open_outbox,
    copy_out,
    eject_directory,
    exchange_outbox,
    fenced_under,
    outgoing_path,
    pending_outgoing,
    reclaim,
    recover,
    resume_copy_outs,
    retire,
    retired_path,
    seal,
    settle,
)
from .errors import FormatError, WorkflowError
from .models import (
    EXCHANGE_DIRECTORY,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TERMINAL_KINDS,
    TRANSFER_DIRECTORY,
    WORKSPACE_DIRECTORY,
    JobDefinition,
    Marker,
    _read_regular_file,
    canonical_uuid,
    normalize_placement,
    placement_text,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "CLOCK_SKEW_NS",
    "DEFAULT_OFFER_STATES",
    "FRESHNESS_WINDOW_NS",
    "TRANSFER_DIRECTORY",
    "TRANSFER_FORMAT",
    "TRANSFER_FORMAT_VERSION",
    "TRANSFER_MANIFEST",
    "TRANSFER_OFFER_FORMAT",
    "TRANSFER_RETIREMENT_FORMAT",
    "TRANSFER_RUNNERS",
    "TransferCandidate",
    "acknowledge_transfer",
    "acknowledge_transfers",
    "adopt_job",
    "detach_job",
    "eject_job",
    "import_bundle",
    "offer_transfers",
    "reclaim_transfer",
    "recover_interrupted_transfers",
    "recover_transfers",
    "resume_exports",
    "retire_transfers",
    "select_transfer_jobs",
    "validate_bundle",
]

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


def _read_manifest(bundle: Path) -> dict[str, Any]:
    """Read a bundle's transfer manifest, bounded and without following a symlink.

    Both ``.httk-transfer`` and its ``manifest.json`` must be real (a directory
    and a regular file of at most 1 MiB), so a manifest planted as a symlink,
    FIFO or oversized file is refused instead of followed, blocked on or loaded.

    :param bundle: The bundle directory.
    :return: The manifest object, not yet validated.
    :raises httk.workflow.errors.FormatError: If the manifest cannot be read or is not a JSON object.
    """

    transfer_dir = bundle / TRANSFER_DIRECTORY
    path = transfer_dir / TRANSFER_MANIFEST
    try:
        if not stat.S_ISDIR(os.lstat(transfer_dir).st_mode):
            raise FormatError(f"cannot read JSON object {path}: {TRANSFER_DIRECTORY} is not a directory")
        value = json.loads(_read_regular_file(path, _MANIFEST_BYTES).decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise FormatError(f"cannot read JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise FormatError(f"expected JSON object in {path}")
    return value


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


# ---------------------------------------------------------------------------
# Sealing: ejection, export and addressed transfer (the source side, plan 4.4 and 4.6)
# ---------------------------------------------------------------------------


def _transferring_root(workspace: Workspace, job_id: str) -> tuple[Marker, Transaction] | None:
    """Return a job's ``transferring`` marker and transaction when it is the root of one."""

    marker = workspace.find_marker_by_id(job_id, kinds=("transferring",))
    if marker is None:
        return None
    frame = workspace.read_state(marker)
    outgoing = frame.get("outgoing")
    if not isinstance(outgoing, Mapping) or outgoing.get("role") != "root":
        return None
    return marker, Transaction.from_root(marker, frame)


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
    """Seal one quiescent job for another workspace: an addressed transfer, committed to ``transfers/outgoing/<T>``.

    The job's marker stays ``transferring`` until the destination's
    acknowledgement retires the bundle (:func:`acknowledge_transfer`). A job
    that is already transferring to the same destination is resumed rather than
    sealed again, so an interrupted transfer finishes by asking again.

    A spawned child that is still bound to its live parent never leaves on its
    own, and a parent leaves with its bound children only as a tree: the caller
    seals the root first (*with_tree*) and then each member, whose parent is by
    then transferring and so no longer binds it.

    :param workspace: Provide the source workspace.
    :param job_id: Identify the job to detach.
    :param marker: An already resolved source marker; avoids a lookup.
    :param waiting_parent_map: Precomputed child-to-parent map for this transfer batch.
    :param destination_workspace_id: Identify the destination workspace.
    :param destination_remote: Preserve the destination's remote identifier.
    :param destination_placement: Override the destination placement.
    :param transfer_id: The transfer id of the transfer to resume: accepted only when the job is
        already transferring under it (a new transfer always mints its own id).
    :param with_tree: Seal a job whose bound children the caller transfers with it.
    :return: The sealed bundle, ``transfers/outgoing/<T>``.
    :raises ValueError: If the job is missing, active, joined, bound to its parent, a parent
        transferred without its tree, already transferring incompatibly, or the transfer aborted.
    """

    destination_id = canonical_uuid(destination_workspace_id, "destination_workspace_id")
    if marker is None:
        marker = workspace.find_marker_by_id(job_id, kinds=STATE_KINDS)
        if marker is None:
            raise ValueError(f"job must have exactly one source marker: {job_id}")
    elif marker.job_id != job_id:
        raise ValueError(f"known source marker does not identify job: {job_id}")
    if marker.kind == "transferring":
        found = _transferring_root(workspace, job_id)
        if (
            found is None
            or not found[1].addressed
            or found[1].destination_workspace_id != destination_id
            or (transfer_id is not None and found[1].transfer_id != canonical_uuid(transfer_id, "transfer_id"))
        ):
            raise ValueError("job is already transferring under a different transfer")
        txn = found[1]
        outcome = settle(workspace, txn.transfer_id, force=True)
        bundle = outgoing_path(workspace, txn.transfer_id)
        # A deferred abort ("pending") leaves the bundle deliverable until its window passes.
        if outcome in {"committed", "pending"} and os.path.lexists(bundle):
            return bundle
        raise ValueError(f"transfer {txn.transfer_id} of {marker.job_key} could not be resumed ({outcome})")
    if transfer_id is not None:
        raise ValueError(
            f"transfer {transfer_id} names no transfer of job {job_id} to resume; a new transfer mints its own id"
        )
    return seal(
        workspace,
        marker,
        Commit("outgoing"),
        destination_workspace_id=destination_id,
        destination_remote=destination_remote,
        destination_placement=destination_placement,
        waiting_parent_map=waiting_parent_map,
        with_tree=with_tree,
        reason="detached_transfer",
    )


def _ack_matches(manifest: Mapping[str, Any], acknowledgement: Mapping[str, object]) -> None:
    for name in ("source_workspace_id", "destination_workspace_id", "payload_sha256", "job_id", "job_key"):
        if acknowledgement.get(name) != manifest.get(name):
            raise FormatError(f"transfer acknowledgement disagrees on {name}")


def acknowledge_transfer(workspace: Workspace, acknowledgement: Mapping[str, object]) -> Path:
    """Validate one destination acknowledgement and retire its addressed bundle.

    The acknowledgement is checked against the bundle's own manifest
    (``transfers/outgoing/<T>/.httk-transfer/manifest.json``); then the bundle is
    renamed to ``transfers/retired/<T>``, and only after that positive result
    the root marker is removed and the transaction directory trashed. An
    acknowledgement whose bundle is in neither place is a repeat of one long
    retired, unless the job is still here: then the transfer was reclaimed (or
    is being reclaimed) and the destination holds a copy as well, which is
    reported as in doubt and never as a retirement. An acknowledgement that
    carries an identity signature must carry a valid one.

    :param workspace: Provide the source workspace.
    :param acknowledgement: Supply the destination acknowledgement.
    :return: The retired bundle path ``transfers/retired/<T>`` (which may already be collected).
    :raises httk.workflow.errors.FormatError: If the acknowledgement format, signature, or identity is invalid.
    :raises ValueError: If the transfer was reclaimed here, so the job may now exist in both workspaces.
    """

    if acknowledgement.get("format") != "httk-workflow-transfer-acknowledgement":
        raise FormatError("invalid transfer acknowledgement format")
    signature = verify_document(acknowledgement)
    if signature.present and not signature.valid:
        raise FormatError(f"transfer acknowledgement signature is invalid: {signature.reason}")
    transfer_id = canonical_uuid(acknowledgement.get("transfer_id"), "transfer_id")
    outgoing = outgoing_path(workspace, transfer_id)
    retired = retired_path(workspace, transfer_id)
    held = outgoing if os.path.lexists(outgoing) else retired if os.path.lexists(retired) else None
    if held is None:
        return _unretired(workspace, transfer_id, acknowledgement)
    _ack_matches(_read_manifest(held), acknowledgement)
    if signature.present:
        _LOGGER.info(
            "transfer %s was acknowledged by %s",
            transfer_id,
            signature.operator_key,
            extra={"event": "transfer_ack_verified", "transfer_id": transfer_id},
        )
    fenced = fenced_under(workspace, transfer_id)
    if fenced.root is None:
        if held == retired:
            # Retired and its marker removed, but the clean-up after it was interrupted.
            _txn.trash(
                eject_directory(workspace, transfer_id), control=workspace.control, holds_payload=_txn.holds_job_payload
            )
        return retired
    if retire(workspace, Transaction.from_root(*fenced.root)) is None:
        return _unretired(workspace, transfer_id, acknowledgement)
    return retired


def _unretired(workspace: Workspace, transfer_id: str, acknowledgement: Mapping[str, object]) -> Path:
    """An acknowledgement whose bundle is neither outgoing nor retired: long retired, or reclaimed (in doubt)."""

    retired = retired_path(workspace, transfer_id)
    job_id = canonical_uuid(acknowledgement.get("job_id"), "job_id")
    marker = workspace.find_marker_by_id(job_id, kinds=STATE_KINDS)
    if marker is None or os.path.lexists(retired):
        return retired
    _LOGGER.warning(
        "transfer %s was acknowledged, but job %s is still here (reclaimed): in doubt",
        transfer_id,
        marker.job_key,
        extra={"event": "transfer_ack_in_doubt", "transfer_id": transfer_id, "job_key": marker.job_key},
    )
    raise ValueError(
        f"transfer {transfer_id} was acknowledged by its destination, but job {marker.job_key} was reclaimed "
        "here: it may now exist in both workspaces; it is not retired"
    )


def acknowledge_transfers(workspace: Workspace, acknowledgements: Sequence[Mapping[str, object]]) -> list[Path]:
    """Retire every acknowledged bundle of a sweep.

    :param workspace: Provide the source workspace.
    :param acknowledgements: Supply the destination acknowledgements.
    :return: The retired bundle paths, in order.
    :raises httk.workflow.errors.FormatError: If an acknowledgement is invalid.
    :raises ValueError: If a transfer was reclaimed here (in doubt), after every other one was retired.
    """

    paths: list[Path] = []
    in_doubt: list[str] = []
    for acknowledgement in acknowledgements:
        try:
            paths.append(acknowledge_transfer(workspace, acknowledgement))
        except ValueError as exc:
            if isinstance(exc, FormatError):
                raise
            in_doubt.append(str(exc))
    if in_doubt:
        raise ValueError("; ".join(in_doubt))
    return paths


def retire_transfers(
    workspace: Workspace,
    job_ids: Sequence[str],
    *,
    destination_workspace_id: str | None = None,
) -> list[dict[str, object]]:
    """Retire the sealed addressed bundle of every named job, as an acknowledgement without a document.

    A fetch retires at the source only once the destination holds the job, so
    the identity of the job is all this side needs; naming the destination as
    well refuses to retire a bundle that was sealed for somebody else. A job
    with no transfer in progress any more is skipped (already retired).

    :param workspace: Provide the source workspace.
    :param job_ids: Identify the jobs whose bundles to retire.
    :param destination_workspace_id: Restrict retirement to one destination.
    :return: Retirement records for the named jobs.
    :raises ValueError: If a job is live here with no addressed transfer, is being ejected, or
        its bundle is not (or no longer) waiting in ``transfers/outgoing``.
    """

    destination_id = (
        None if destination_workspace_id is None else canonical_uuid(destination_workspace_id, "workspace_id")
    )
    results: list[dict[str, object]] = []
    for job_id in job_ids:
        identifier = canonical_uuid(job_id, "job_id")
        found = _transferring_root(workspace, identifier)
        if found is not None and not found[1].addressed:
            raise ValueError(
                f"job {identifier} is not retirable: ejection in progress; "
                "finish it with `httk job eject` or transfer recovery"
            )
        if found is None or (destination_id is not None and found[1].destination_workspace_id != destination_id):
            if workspace.find_marker_by_id(identifier, kinds=STATE_KINDS) is not None:
                raise ValueError(f"no detached transfer of this workspace names job: {identifier}")
            continue
        marker, txn = found
        retired = retire(workspace, txn)
        if retired is None:
            raise ValueError(
                f"transfer {txn.transfer_id} of {marker.job_key} has no sealed bundle waiting in "
                f"{outgoing_path(workspace, txn.transfer_id)}; it is not retired"
            )
        results.append(
            {
                "transfer_id": txn.transfer_id,
                "job_id": identifier,
                "job_key": marker.job_key,
                "status": "retired",
                "retired_bundle": str(retired),
            }
        )
    return results


def reclaim_transfer(workspace: Workspace, job_id: str) -> dict[str, object]:
    """Take back an addressed transfer that was never delivered: the job returns to its placement.

    Allowed only once the bundle can no longer be accepted by any destination
    (``sealed_at + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS`` has passed). A later
    transfer of the job mints a new transfer id.

    :param workspace: Provide the source workspace.
    :param job_id: Identify the transferring job.
    :return: The reclamation record.
    :raises ValueError: If the job has no addressed transfer in progress, the window has not passed,
        the acknowledgement retired it first, or the reclaim could not finish now.
    """

    identifier = canonical_uuid(job_id, "job_id")
    found = _transferring_root(workspace, identifier)
    if found is None or not found[1].addressed:
        raise ValueError(f"job {identifier} has no addressed transfer in progress to reclaim")
    marker, txn = found
    outcome = reclaim(workspace, txn)
    if outcome == "retired":
        raise ValueError(
            f"transfer {txn.transfer_id} of {marker.job_key} was acknowledged before it could be reclaimed; "
            "it stays retired"
        )
    if outcome != "aborted":
        raise ValueError(f"transfer {txn.transfer_id} of {marker.job_key} could not be reclaimed now ({outcome})")
    return {"transfer_id": txn.transfer_id, "job_id": identifier, "job_key": marker.job_key, "status": "reclaimed"}


def recover_transfers(workspace: Workspace, *, owner: str | None = None) -> list[dict[str, object]]:
    """Recover every interrupted transfer whose owner is gone (plan 4.7).

    Adoption lineages below ``tmp/`` are taken over and continued; sealing
    transactions are settled by the phase reader; orphaned transaction
    directories are swept. Every action is safe if a listing missed something.
    Lineages of exchange-inbox bundles are left to the exchange pass, which
    alone has the exchange's directories open.

    :param workspace: Provide the workspace whose transfers to recover.
    :param owner: The recovering actor's owner token (this process when omitted).
    :return: One record per lineage (``lineage``) or transaction (``transfer_id``) looked at.
    """

    return [*recover_lineages(workspace, owner=owner), *recover(workspace)]


def recover_interrupted_transfers(workspace: Workspace) -> None:
    """Finish every transfer an interrupted earlier call left unfinished.

    :param workspace: The workspace whose interrupted transfers to finish.
    """

    recover_transfers(workspace)


def _eject_commit(workspace: Workspace, target: str | os.PathLike[str], job_key: str) -> Commit:
    """Decide where an ejection commits: the exchange outbox, a directory on this filesystem, or an export."""

    absolute = Path(os.path.abspath(Path(target).expanduser()))
    outbox = exchange_outbox(workspace)
    if absolute == outbox:
        # The exchange pass names the outbox literally: nothing in client territory is resolved.
        return Commit("exchange", name=job_key)
    real_outbox = os.path.realpath(outbox)

    def is_outbox(path: Path) -> bool:
        return path == outbox or os.path.realpath(path) == real_outbox

    if is_outbox(absolute):
        return Commit("exchange", name=job_key)
    if not os.path.isdir(absolute) and is_outbox(absolute.parent):
        if absolute.name in {"", ".", ".."}:
            raise ValueError(f"eject destination {absolute} names no entry of the exchange outbox")
        return Commit("exchange", name=absolute.name)
    tmp_device = os.stat(workspace.control / "tmp").st_dev
    inbox = _inbox_target(absolute, job_key)
    if inbox is not None and os.stat(inbox[0]).st_dev == tmp_device:
        # A workspace's exchange inbox is client territory: the commit goes by
        # descriptor from that workspace's root, never through a resolved path.
        return Commit("inbox", path=inbox[0], name=inbox[1])
    destination = _eject_destination(workspace, target, job_key)
    return Commit("export" if os.stat(destination.parent).st_dev != tmp_device else "path", path=destination)


def _inbox_target(absolute: Path, job_key: str) -> tuple[Path, str] | None:
    """Return ``(workspace root, entry name)`` when *absolute* names an entry of a workspace's exchange inbox.

    The decision is lexical (``<root>/exchange/inbox[/<name>]`` with a workspace
    at ``<root>``), so a client that replaced ``exchange`` or ``inbox`` by a
    symlink cannot turn the target into a path elsewhere: the commit then opens
    the inbox without following it and refuses.
    """

    entry = absolute / job_key if absolute.name == "inbox" else absolute
    box = entry.parent
    if entry.name in {"", ".", ".."} or box.name != "inbox" or box.parent.name != EXCHANGE_DIRECTORY:
        return None
    if os.path.lexists(entry):
        return None  # an existing entry: resolved like any other target (and refused)
    root = box.parent.parent
    if not (root / WORKSPACE_DIRECTORY / "format.json").is_file():
        return None
    return root.resolve(), entry.name


def eject_job(
    workspace: Workspace,
    job_id: str,
    target: str | os.PathLike[str],
    *,
    marker: Marker | None = None,
    owner: str | None = None,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
) -> Path:
    """Move one quiescent job out of its workspace to a free-standing job directory.

    The job is sealed as a bundle addressed to no workspace and moved to
    *target* by one rename; the workspace keeps no copy. The directory is the
    whole job: its payload, seal, tree metadata, prior state, and any shared
    runner it pins. A job with bound children takes its whole tree along: every
    descendant, each of which must be paused or terminal, is carried in the
    directory under ``.httk-transfer/tree/<placement>/<job_key>``. A target on
    another filesystem is an *export*: the bundle is held below
    ``transfers/exports/<T>`` and copied out (``httk job eject --resume`` retries a
    copy-out that did not finish). Interrupted transfers whose owner is gone are
    recovered first.

    :param workspace: The workspace the job leaves.
    :param job_id: The job to eject.
    :param target: The new job directory, or an existing directory to eject into.
    :param marker: The already resolved job marker, when available.
    :param owner: The owner token recorded for the transaction (this process when omitted).
    :param waiting_parent_map: A precomputed child-to-parent map of the waiting joins.
    :return: The free-standing job directory.
    :raises ValueError: If the job or its tree cannot leave its workspace or the target is unusable.
    :raises FileExistsError: If the target job directory already exists.
    :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
    :raises httk.workflow.errors.FormatError: If the job or a tree member sits at a placement naming
        a job directory (:func:`~httk.workflow.protocol.check_job_placement`).
    """

    recover_transfers(workspace, owner=owner)
    if marker is None:
        marker = workspace.find_marker_by_id(job_id, kinds=STATE_KINDS)
        if marker is None:
            raise ValueError(f"no job {job_id} in this workspace")
    commit = _eject_commit(workspace, target, marker.job_key)
    location = seal(workspace, marker, commit, owner=owner, reason="ejected", waiting_parent_map=waiting_parent_map)
    if commit.kind == "export":
        return copy_out(workspace, location.parent.name, owner=owner)
    return location


def resume_exports(workspace: Workspace, *, owner: str | None = None) -> list[Path]:
    """Re-run every pending export copy-out: held exports, and copy-outs whose owner is gone.

    :param workspace: The workspace the jobs left.
    :param owner: The owner token (this process when omitted).
    :return: The destinations that now hold their job.
    """

    recover_transfers(workspace)
    return resume_copy_outs(workspace, owner=owner)


# ---------------------------------------------------------------------------
# Offering finished work back to whoever sent it
# ---------------------------------------------------------------------------


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
    for root_marker, root_frame, txn in pending_outgoing(workspace):
        if txn.destination_workspace_id != destination_id:
            continue
        if destination_remote is not None and txn.destination_remote not in (None, destination_remote):
            continue
        prior_kind = str(root_frame.get("prior_kind"))
        if prior_kind not in kinds or root_marker.job_id in offered_jobs:
            continue
        if selected_ids is not None and root_marker.job_id not in selected_ids:
            continue
        if prefix is not None and root_marker.placement.parts[: len(prefix)] != prefix:
            continue
        bundle = outgoing_path(workspace, txn.transfer_id)
        if not bundle.is_dir() or bundle.is_symlink():
            # Not committed (yet): its owner, or recovery once the owner is gone,
            # finishes it; until then it is not offered, and an explicit request for
            # the job reports the sealed bundle as unavailable.
            continue
        manifest: Mapping[str, Any] | None = None
        problem: str | None = None
        try:
            manifest = _read_manifest(bundle)
        except (FormatError, OSError) as exc:
            problem = str(exc)
        if manifest is not None:
            for field, expected in (
                ("transfer_id", txn.transfer_id),
                ("destination_workspace_id", txn.destination_workspace_id),
                ("job_id", root_marker.job_id),
                ("job_key", root_marker.job_key),
                ("prior_kind", prior_kind),
            ):
                if manifest.get(field) != expected:
                    problem = f"sealed transfer manifest disagrees with its transfer on {field}"
                    break
        try:
            job = JobDefinition.from_path(bundle / "job.json")
        except (WorkflowError, OSError) as exc:
            job = None
            problem = str(exc)
        candidates.append(
            TransferCandidate(
                root_marker.job_id,
                root_marker.job_key,
                prior_kind,
                root_marker.placement,
                bundle,
                None,
                job,
                manifest,
                problem,
            )
        )
        offered_jobs.add(root_marker.job_id)
    for marker in marker_source:
        if selected_ids is not None and marker.job_id not in selected_ids:
            continue
        prior_kind = marker.kind
        if marker.kind == "transferring":
            state = workspace.read_state(marker)
            outgoing = state.get("outgoing")
            if not isinstance(outgoing, Mapping) or outgoing.get("role") != "root":
                continue
            if outgoing.get("destination_workspace_id") != destination_id:
                continue
            if outgoing.get("destination_remote") != destination_remote:
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
    every member the interruption left behind: sealed ones in their outgoing
    bundles, live ones at their placements. Roots are located among *markers*,
    the ones the selection scans anyway, and the pending outgoing transfers.

    :param workspace: The source workspace.
    :param job_ids: The explicitly requested job ids.
    :param markers: The live markers the selection scanned.
    :return: The widened ids, and each added descendant's requested root.
    """

    widened = set(job_ids)
    root_of: dict[str, str] = {}
    live = {marker.job_id: marker for marker in markers if marker.job_id in job_ids}
    sealed: dict[str, Path] = {}
    sealed_by_key: dict[str, Path] = {}
    for marker, _frame, txn in pending_outgoing(workspace):
        bundle = outgoing_path(workspace, txn.transfer_id)
        if bundle.is_dir():
            sealed[marker.job_id] = bundle
            sealed_by_key[marker.job_key] = bundle

    def locate(placement: PurePosixPath, job_key: str) -> Path:
        home = workspace.payload_path(placement, job_key)
        return home if home.is_dir() or job_key not in sealed_by_key else sealed_by_key[job_key]

    for job_id in sorted(job_ids):
        if job_id in sealed:
            payload = sealed[job_id]
        elif job_id in live:
            payload = workspace.payload_path(live[job_id].placement, live[job_id].job_key)
        else:
            continue
        try:
            descendants = descendant_ids(
                workspace, payload, JobDefinition.from_path(payload / "job.json"), locate=locate
            )
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

    ordered = sorted(candidates, key=lambda item: (placement_text(item.source_placement), item.job_key))
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
    sealed_roots = {
        marker.job_id: (marker, frame)
        for marker, frame, txn in pending_outgoing(workspace)
        if txn.destination_workspace_id == destination_workspace_id
    }
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
        if job_id in sealed_roots:
            sealed_marker, sealed_frame = sealed_roots[job_id]
            if sealed_frame.get("prior_kind") not in states:
                reasons[job_id] = f"filtered by state (state: {sealed_frame.get('prior_kind')})"
            elif prefix is not None and sealed_marker.placement.parts[: len(prefix)] != prefix:
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
        manifest = _read_manifest(bundle)
        offers[str(manifest["transfer_id"])] = _offer_record(manifest, bundle, candidate.tree_root)
    if sealing_errors:
        details = "; ".join(sealing_errors)
        raise ValueError(
            "requested transfer jobs could not all be sealed: "
            f"{details}; already-sealed jobs remain sealed and a retry resumes them"
        )
    return sorted(offers.values(), key=lambda item: (str(item["placement"]), str(item["job_key"])))


# ---------------------------------------------------------------------------
# Ejecting jobs to free-standing directories, and adopting them back
# ---------------------------------------------------------------------------


def _eject_destination(workspace: Workspace, target: str | os.PathLike[str], job_key: str) -> Path:
    """Resolve where one job is ejected to, the way ``mv`` resolves its target.

    An existing directory receives the job as ``<directory>/<job_key>``; any
    other path names the new job directory itself, whose parent must exist.
    The result is never inside a workspace, except directly in a workspace's
    exchange inbox (``WORKSPACE/exchange/inbox/<name>``), where that
    workspace's managers adopt it; this workspace's exchange outbox is
    resolved before this by :func:`_eject_commit`.
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
    if enclosing is not None and path.parent != enclosing / EXCHANGE_DIRECTORY / "inbox":
        relation = "this" if enclosing.resolve() == workspace.root.resolve() else "a"
        raise ValueError(
            f"eject destination {path} is inside {relation} workspace ({enclosing}); "
            "eject to a directory outside every workspace, then adopt it where it should go"
        )
    return path


def validate_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Check one bundle's structure, digests and seals, and return its manifest.

    These are the checks of the bundle's own content that need no importing
    workspace; every import also checks the destination and its source's rules.

    :param bundle: The bundle (root payload) directory.
    :return: The manifest object.
    :raises httk.workflow.errors.FormatError: If the bundle's content is refused.
    :raises OSError: If the bundle cannot be read.
    """

    return check_bundle(Path(bundle)).as_mapping()


def _adopted_marker(workspace: Workspace, result: AdoptionResult, where: Path) -> Marker:
    """Turn one adoption result into the adopted root's marker, or raise what stopped it."""

    if result.status in {"imported", "replay"}:
        assert result.job_id is not None
        marker = workspace.find_marker_by_id(result.job_id, kinds=STATE_KINDS)
        if marker is not None:
            return marker
        raise ValueError(f"{where} was already adopted into this workspace once and its job has since moved on")
    if result.status == "refused":
        assert result.error is not None
        raise result.error
    if result.status == "lost":
        raise FileNotFoundError(f"{where}: {result.reason}")
    if result.status == "waiting":
        raise ValueError(
            f"{where} was taken in, but its adoption waits: {result.reason}; it finishes when transfer "
            "recovery next runs (a manager attaching, or `httk job adopt`/`httk job eject` again)"
        )
    raise ValueError(f"{where}: {result.status}: {result.reason}")


def _same_filesystem(workspace: Workspace, path: Path) -> bool:
    """Report whether *path* can be renamed into the workspace's ``tmp/`` (compared in this process)."""

    return os.lstat(path).st_dev == os.stat(workspace.control / "tmp").st_dev


def adopt_job(
    workspace: Workspace,
    directory: str | os.PathLike[str],
    *,
    placement: str | PurePosixPath | None = None,
    owner: str | None = None,
) -> Marker:
    """Move one free-standing job directory (an ejected bundle) into a workspace.

    The directory is claimed into a private lineage below ``tmp/`` (renamed on
    one filesystem, otherwise copied, and the source removed once the job has
    arrived), verified completely, and published: members first, then the
    root, each restored to the state it was ejected in. A held export of this
    workspace (``transfers/exports/<T>/<job_key>``, or one held in doubt at
    ``transfers/in-doubt/<T>/<job_key>``) is taken back the same way,
    and so is an entry of the workspace's exchange outbox, claimed by
    descriptor and verified as client content. A refused directory is put back
    where it was (a refused outbox entry is quarantined instead). A copy of a directory whose
    job already arrived is a replay; one whose job has since moved on is a
    stale copy and is refused.

    :param workspace: The workspace the job joins.
    :param directory: The free-standing job directory.
    :param placement: Place the job here instead of where it was ejected from (not for a tree).
    :param owner: The owner token recorded for the lineage (this process when omitted).
    :return: The adopted job's marker (a tree's root).
    :raises ValueError: If the directory is inside the workspace (other than a held export or an
        entry of the workspace's exchange outbox), an entry of its exchange inbox (the exchange pass
        adopts those), addressed to a workspace, stale, a placement is given for a tree, or the
        adoption has to wait.
    :raises FileExistsError: If the workspace already holds this job or its payload path.
    :raises FileNotFoundError: If the directory does not exist or another actor took it first.
    :raises httk.workflow.errors.FormatError: If the directory does not verify.
    """

    workspace._require_unsealed()
    absolute = Path(os.path.abspath(Path(directory).expanduser()))
    # The final component is not resolved: a symlinked job directory is refused, not followed.
    source = absolute.parent.resolve() / absolute.name
    root = workspace.root.resolve()
    exports = (workspace.control / "transfers" / "exports").resolve()
    in_doubt = (workspace.control / "transfers" / "in-doubt").resolve()
    exchange = root / EXCHANGE_DIRECTORY

    # Lexically ``<root>/exchange/<box>/<name>`` with a workspace at ``<root>``:
    # decided without resolving anything in client territory, so neither a
    # symlinked prefix nor a client-swapped box turns the entry into an
    # ordinary path elsewhere.
    lexical = absolute.parent
    lexical_root = lexical.parent.parent
    lexical_box = (
        lexical.name
        if lexical.parent.name == EXCHANGE_DIRECTORY
        and (lexical_root / WORKSPACE_DIRECTORY / "format.json").is_file()
        and lexical_root.resolve() == root
        else None
    )

    def in_exchange(box: str) -> bool:
        return lexical_box == box or exchange / box in {absolute.parent, source.parent}

    if in_exchange("inbox"):
        raise ValueError(
            f"{absolute} is in this workspace's exchange inbox, which the workspace's managers adopt as client "
            "content; leave it there for them (`httk job eject JOB WORKSPACE/exchange/inbox` is the local way in)"
        )
    if in_exchange("outbox"):
        return _adopt_from_outbox(workspace, exchange / "outbox" / absolute.name, placement=placement, owner=owner)
    kind: BundleSource = "adopt"
    if source.parent.parent in {exports, in_doubt}:
        # A held export, or one held in doubt: the operator takes it back.
        kind = "export"
    elif source == root or root in source.parents:
        raise ValueError(f"{source} is already inside the workspace")
    recover_transfers(workspace)
    copy = not _same_filesystem(workspace, source)
    result = adopt(
        workspace,
        Source(kind, source, copy=copy, remove_source=copy),
        placement=placement,
        owner=owner,
    )
    return _adopted_marker(workspace, result, source)


def _adopt_from_outbox(
    workspace: Workspace, entry: Path, *, placement: str | PurePosixPath | None, owner: str | None
) -> Marker:
    """Take back one entry of this workspace's exchange outbox, claimed by descriptor.

    The outbox is client territory: it is opened from the workspace root
    without following a symlink, the entry is claimed by its name relative to
    that descriptor, it is verified as client content (the exchange checks,
    no exchange origin), and a refused bundle is never renamed back by path:
    it stays in the lineage and is quarantined.
    """

    if entry.name in {"", ".", ".."} or entry.name == "rejected":
        raise ValueError(f"{entry} names no returned job of the exchange outbox")
    outbox = _open_outbox(workspace)
    try:
        recover_transfers(workspace)
        result = adopt(
            workspace,
            Source("exchange_outbox", entry, directory_fd=outbox),
            placement=placement,
            owner=owner,
        )
    finally:
        os.close(outbox)
    return _adopted_marker(workspace, result, entry)


def _import_results(workspace: Workspace, bundles: Sequence[str | os.PathLike[str]]) -> list[AdoptionResult]:
    incoming = (workspace.control / "transfers" / "incoming").resolve()
    results: list[AdoptionResult] = []
    for bundle in bundles:
        absolute = Path(os.path.abspath(Path(bundle).expanduser()))
        path = absolute.parent.resolve() / absolute.name
        try:
            # A bundle delivered into incoming/ is claimed; any other (a local
            # transfer's outgoing bundle, still the source's) is copied.
            result = adopt(workspace, Source("incoming", path, copy=path.parent != incoming))
        except (WorkflowError, OSError, ValueError) as exc:
            result = AdoptionResult("failed", reason=str(exc), error=exc)
        results.append(result)
    return results


def import_bundles(workspace: Workspace, bundles: Sequence[str | os.PathLike[str]]) -> list[dict[str, object]]:
    """Import addressed bundles, each through the adoption chain; one result never aborts the batch.

    A bundle in this workspace's ``transfers/incoming/`` is claimed by rename;
    any other path is copied (its owner keeps it until it is acknowledged).
    Each result's ``status`` is ``imported``, ``replay`` (it had arrived
    already), ``expired`` (outside its freshness window, discarded), ``refused``
    (kept where it was, with the reason), ``waiting`` or ``failed``; an imported
    or replayed bundle carries the ``acknowledgement`` its source retires it with.

    :param workspace: The destination workspace.
    :param bundles: The bundle directories.
    :return: One result per bundle, in order.
    """

    recover_transfers(workspace)
    return [result.as_mapping() for result in _import_results(workspace, bundles)]


def import_bundle(workspace: Workspace, bundle: str | os.PathLike[str]) -> dict[str, object]:
    """Import one addressed bundle, or acknowledge its replay.

    :param workspace: The destination workspace.
    :param bundle: The bundle directory.
    :return: The acknowledgement document.
    :raises ValueError: If the bundle expired or its import has to wait.
    :raises httk.workflow.errors.FormatError: If the bundle is refused for its content.
    :raises FileExistsError: If the workspace already holds its job or payload path.
    """

    recover_transfers(workspace)
    [result] = _import_results(workspace, [bundle])
    if result.acknowledgement is not None:
        return result.acknowledgement
    if result.error is not None:
        raise result.error
    raise ValueError(f"transfer bundle {bundle} was not imported ({result.status}): {result.reason}")
