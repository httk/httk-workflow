"""Moving jobs between workspaces: eject, adopt, held transfers, and the scratch reconcilers of eject and adopt.

Everything here composes :mod:`~httk.workflow._kernel` and :mod:`~httk.workflow._bundles`; every move and write
goes through :mod:`~httk.workflow._fs`.

**Eject** claims the tree members, builds the bundle in an ``eject`` scratch (``bundle.json`` first, recording
each member's ``from`` state and priority) and delivers it to ``<destination>/<root key>``; across filesystems
it copies to ``<destination>/.<name>.partial.<transfer id>`` and delivers that. An occupied destination rolls
the members back.

**Adopt** takes a bundle into an ``adopt`` scratch, validates it (the trust boundary), rekeys an untrusted one,
deduplicates against the workspace and publishes the members bottom-up.

**Crash-resume contract** of the reconcilers, registered with the kernel at import:

- ``eject``: if the destination carries this bundle's exact ``bundle.json`` (under the root key, or for a hold
  under the transfer id), the delivery happened and the scratch is discarded (roll forward). Otherwise every
  member still in the bundle is submitted back to its recorded state and priority (roll back). A destination
  that cannot be read keeps the scratch. Delivery is at least once: verify the destination before re-ejecting.
- ``adopt`` and ``adopt-untrusted`` (the trust is in the scratch's purpose, so it is known from the take on):
  ``source.json`` records where the bundle came from and where refusals go; without it (a crash right after the
  take) a refused bundle has nowhere to go and stays in the scratch.
  ``plan.json`` is the validated, deduplicated manifest: once it exists only publication remains, and a member
  directory missing from the bundle was published by the earlier run. Before it, validation, rekeying,
  deduplication and the exchange-name claim run again; each is idempotent.
"""

import dataclasses
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, cast

from httk.workflow import _fs, _kernel, _store
from httk.workflow._bundles import (
    MAX_MANIFEST_BYTES,
    BundleError,
    BundleManifest,
    build_bundle,
    exchange_names,
    read_rekeyed,
    rekey_untrusted,
    validate_bundle,
)
from httk.workflow._job import JobDefinition
from httk.workflow._kernel import OWNED, JobRef, OwnedJob, Owner
from httk.workflow._state import TERMINAL_STATES, StateDoc, encode_state, read_state_unowned
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.models import normalize_placement

if TYPE_CHECKING:  # pragma: no cover
    from httk.workflow.workspace import Workspace

__all__ = [
    "AdoptReport",
    "Busy",
    "EjectReport",
    "Hold",
    "adopt",
    "eject",
    "held",
    "hold",
    "reconcile_adopt",
    "reconcile_eject",
    "release_hold",
]

_LOGGER = logging.getLogger(__name__)

#: The states a tree member may be ejected from; the root may be in any unowned state.
_MEMBER_STATES = frozenset({*TERMINAL_STATES, "paused"})
_TRANSFER_ID = re.compile(r"[0-9a-f]{32}")
_BUNDLE = "bundle"
#: The scratch purposes of adoption: the trust travels in the name, atomically with the take.
_ADOPT = {False: "adopt", True: "adopt-untrusted"}
_SOURCE = "source.json"
_PLAN = "plan.json"
_PARTIAL = ".partial"
#: Names an ``adopt`` scratch holds besides the bundle; a bundle may not be named like one of them.
_RESERVED = frozenset({_SOURCE, _PLAN, "rekey.json", _PARTIAL})
_RECORD_TEMPORARY = re.compile(r"\.(source|plan|rekey)\.json\.[a-z2-7]{16}\.tmp")
_RECORD_LIMIT = 1 << 16


class Busy(WorkflowError):
    """A tree member could not be claimed: it is running, in a state that may not move, or was taken first.

    :param job_key: The blocking job.
    :param reason: Why it blocks.
    """

    def __init__(self, job_key: str, reason: str) -> None:
        super().__init__(f"{job_key} blocks the tree: {reason}")
        self.job_key = job_key


@dataclass(frozen=True)
class EjectReport:
    """What :func:`eject` delivered.

    :param destination: The delivered bundle directory.
    :param members: The job keys, top-down.
    :param transfer_id: The bundle's transfer id.
    """

    destination: Path
    members: tuple[str, ...]
    transfer_id: str


@dataclass(frozen=True)
class AdoptReport:
    """What :func:`adopt` published.

    :param published: The published jobs, bottom-up.
    :param already_adopted: Every member was already in the workspace; nothing was published.
    :param missing_workflows: Workflow ids the published jobs need that are not installed (a warning only).
    """

    published: tuple[JobRef, ...]
    already_adopted: bool
    missing_workflows: tuple[str, ...]


@dataclass(frozen=True)
class Hold:
    """A held bundle in ``transfers/outgoing/<transfer-id>/``.

    :param transfer_id: The bundle's transfer id, also its directory name.
    :param path: The bundle directory.
    :param manifest: Its manifest.
    """

    transfer_id: str
    path: Path
    manifest: BundleManifest


# -- eject ----------------------------------------------------------------------------------------------------------


def _tree(workspace: "Workspace", root: OwnedJob) -> list[JobRef]:
    # The children a parent published are recorded in its state.json and confirmed from the child's side: its
    # job.json names this parent and it is not detached. A recorded child that is gone is no member.
    found: list[JobRef] = []
    queue: list[tuple[str, StateDoc | None]] = [(root.job_id, root.read_state())]
    seen = {root.job_id}
    while queue:
        parent_id, doc = queue.pop(0)
        for child in doc.children if doc is not None else ():
            job_id = str(child.get("job_id"))
            if job_id in seen:
                continue
            ref = _kernel.locate(workspace, job_id, placement_hint=PurePosixPath(str(child.get("placement"))))
            if ref is None:
                continue
            if ref.state not in _MEMBER_STATES:
                running = "running or held by an owner" if ref.state == OWNED else ref.state
                raise Busy(ref.job_key, f"it is {running}; tree members must be terminal or paused")
            try:
                parent = JobDefinition.from_path(ref.path / "job.json").parent
            except FormatError as exc:
                raise Busy(ref.job_key, f"its job.json is unreadable: {exc}") from exc
            child_doc, damaged = read_state_unowned(ref.path / "state.json")
            if damaged:
                raise Busy(ref.job_key, "its state.json is damaged")
            detached = child_doc is not None and child_doc.detached is not None
            if parent is None or parent.get("job_id") != parent_id or detached:
                continue
            seen.add(job_id)
            found.append(ref)
            queue.append((job_id, child_doc))
    return found


def _claim_tree(workspace: "Workspace", owner: Owner, root: OwnedJob) -> list[OwnedJob]:
    refs = _tree(workspace, root)
    claimed: dict[str, OwnedJob] = {}
    # Sorted claims: two ejectors of overlapping trees cannot each hold a part forever.
    for ref in sorted(refs, key=lambda item: item.job_id):
        job = _kernel.claim(workspace, owner, ref)
        if job is None:
            for taken in claimed.values():
                taken.give_back()
            raise Busy(ref.job_key, "another actor moved it first")
        claimed[ref.job_id] = job
    return [claimed[ref.job_id] for ref in refs]


def _read_manifest(path: Path) -> tuple[bytes, BundleManifest] | None:
    # Our own bundle.json (trusted): bounded, regular, never a symlink.
    data = _fs.read_bounded(_fs.loc(path), MAX_MANIFEST_BYTES)
    return None if data is None else (data, BundleManifest.from_json(data))


def _deliver(workspace: "Workspace", bundle: Path, target: Path, token: bytes, transfer_id: str) -> bool:
    durable = workspace.durable

    def deliver(source: Path) -> bool:
        outcome = _fs.deliver(_fs.loc(source), _fs.loc(target), token_name="bundle.json", token=token, durable=durable)
        return outcome is _fs.Delivered.DONE

    try:
        return deliver(bundle)
    except _fs.CrossDevice:
        partial = target.with_name(f".{target.name}.partial.{transfer_id}")
        _fs.copy_tree(bundle, partial, durable=durable)
        if deliver(partial):
            return True
        _fs.discard(_fs.loc(partial), trash_dir=partial.parent, durable=durable)
        return False


def _roll_back(owner: Owner, bundle: Path, manifest: BundleManifest) -> list[JobRef]:
    # A member already submitted is absent from the bundle: that member is done.
    returned = []
    for member in reversed(manifest.members):
        directory = manifest.member_dir(bundle, member)
        if _fs.exists(_fs.loc(directory)):
            returned.append(
                _kernel.submit(owner.workspace, owner, directory, state=member.state, priority=member.priority)
            )
    return returned


def _eject(
    workspace: "Workspace", owner: Owner, root: OwnedJob, *, destination: Path, tree: bool, by_transfer_id: bool
) -> EjectReport:
    members = _claim_tree(workspace, owner, root) if tree else []
    try:
        for job in (root, *members):
            job.append_log("ejected", destination=str(destination))
        bundle = build_bundle(
            owner, [root, *members], source_workspace_id=workspace.workspace_id, destination_locator=str(destination)
        )
    except Exception:
        # A refusal moved nothing; the members go back, the root stays with the caller.
        for job in members:
            if owner.holds(job.ref):
                job.give_back()
        raise
    read = _read_manifest(bundle / "bundle.json")
    assert read is not None
    token, manifest = read
    target = destination / (manifest.transfer_id if by_transfer_id else root.job_key)
    if _deliver(workspace, bundle, target, token, manifest.transfer_id):
        owner.discard_scratch(bundle.parent)
        return EjectReport(target, tuple(member.job_key for member in manifest.members), manifest.transfer_id)
    _roll_back(owner, bundle, manifest)
    owner.discard_scratch(bundle.parent)
    raise WorkflowError(f"{target} is occupied; the jobs were returned to the states they were taken from")


def eject(workspace: "Workspace", owner: Owner, root: OwnedJob, *, destination: Path, tree: bool) -> EjectReport:
    """Move a claimed job, and with *tree* its descendants, out of the workspace into ``<destination>/<root key>``.

    Tree members are the root's non-detached descendants; each must be terminal or paused, and they are claimed
    in sorted job-UUID order. Once :func:`~httk.workflow._bundles.build_bundle` has run, the root's handle is
    retired whatever happens: on an occupied destination every member, the root included, is returned to the
    state and priority it was taken from.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The claimed, quiescent root, in any unowned state.
    :param destination: An absolute directory; created when missing.
    :param tree: Eject the root's descendants too.
    :return: Where the bundle went.
    :raises Busy: When a member cannot be claimed; the members claimed so far are given back, *root* stays held.
    :raises ValueError: For a relative destination.
    :raises httk.workflow.errors.WorkflowError: When the destination is occupied (the jobs were returned), or
        :class:`~httk.workflow._bundles.BundleError` when the jobs cannot form a bundle (*root* stays held).
    """

    destination = Path(destination)
    if not destination.is_absolute():
        raise ValueError(f"the eject destination must be absolute: {destination}")
    return _eject(workspace, owner, root, destination=destination, tree=tree, by_transfer_id=False)


def _carries(path: Path, data: bytes) -> bool:
    try:
        # The destination may be writable by others (the exchange outbox): never block on a planted FIFO.
        return _fs.read_bounded(_fs.loc(path), len(data), nonblock=True) == data
    except (_fs.UnsafePath, _fs.TooLarge, NotADirectoryError):
        return False


def reconcile_eject(owner: Owner, scratch: Path) -> bool:
    """The ``eject`` scratch reconciler: roll forward when the bundle was delivered, otherwise roll back.

    :param owner: The owner the scratch is named after.
    :param scratch: ``tmp/<owner-id>.eject.<token>/``.
    :return: ``True`` when resolved (the kernel then discards the scratch); ``False`` when the destination
        cannot be read, which keeps the scratch.
    """

    bundle = scratch / _BUNDLE
    read = _read_manifest(bundle / "bundle.json")
    if read is None:
        # build_bundle writes bundle.json before any member moves: nothing to return.
        return True
    data, manifest = read
    candidates = []
    if manifest.destination_locator is not None:
        destination = Path(manifest.destination_locator)
        # An eject delivers under the root's key, a hold under the transfer id; the bytes carry the transfer id.
        candidates = [destination / name for name in (manifest.members[0].job_key, manifest.transfer_id)]
    try:
        if any(_carries(candidate / "bundle.json", data) for candidate in candidates):
            return True
        partials = [path.with_name(f".{path.name}.partial.{manifest.transfer_id}") for path in candidates]
        for partial in partials:
            if _fs.exists(_fs.loc(partial)):
                # An undelivered cross-filesystem copy is ours alone; the members go back, so it goes too.
                _fs.discard(_fs.loc(partial), trash_dir=partial.parent, durable=owner.workspace.durable)
    except OSError as exc:
        _LOGGER.warning("keeping %s: cannot check its destination: %s", scratch, exc)
        return False
    returned = _roll_back(owner, bundle, manifest)
    _LOGGER.warning("rolled back the interrupted eject %s: %d jobs returned", manifest.transfer_id, len(returned))
    return True


# -- adopt ----------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Record:
    source: str | None
    refused_to: str | None
    copied: bool
    untrusted: bool  # from the scratch's purpose, not stored


def _write_record(scratch: Path, record: _Record, *, durable: bool) -> None:
    data = json.dumps({"source": record.source, "refused_to": record.refused_to, "copied": record.copied}).encode()
    _fs.write_file(_fs.loc(scratch / _SOURCE), data, durable=durable)


def _read_record(scratch: Path, *, untrusted: bool) -> _Record:
    data = _fs.read_bounded(_fs.loc(scratch / _SOURCE), _RECORD_LIMIT)
    if data is None:
        # Taken, then the adopter died before recording: a refused bundle has nowhere to go.
        return _Record(None, None, False, untrusted)
    value = json.loads(data)
    return _Record(value["source"], value["refused_to"], bool(value["copied"]), untrusted)


def _refuse(owner: Owner, scratch: Path, bundle: Path, record: _Record, reason: str) -> str:
    """Move a refused bundle away and discard the scratch, or keep it; return the complete message."""

    durable = owner.workspace.durable
    if record.copied:
        owner.discard_scratch(scratch)
        return f"bundle refused: {reason}; it was copied from another filesystem and its source is untouched"
    target = None
    if record.refused_to is not None:
        target = Path(record.refused_to) / f"{bundle.name}.{_fs.fresh_token()}"
    elif record.source is not None and not _fs.exists(_fs.loc(Path(record.source))):
        target = Path(record.source)
    if target is None:
        return f"bundle refused: {reason}; it stays in {scratch}"
    _fs.move_owned(_fs.loc(bundle), _fs.loc(target), durable=durable)
    owner.discard_scratch(scratch)
    return f"bundle refused: {reason}; it was moved to {target}"


def _validated(workspace: "Workspace", scratch: Path, bundle: Path, *, untrusted: bool) -> BundleManifest:
    if read_rekeyed(scratch) is not None:
        # An interrupted rekey: finish it, then the bundle is a trusted one (the _bundles contract).
        read = _read_manifest(bundle / "bundle.json")
        if read is None:
            raise BundleError(f"{bundle}/bundle.json is missing")
        manifest = rekey_untrusted(scratch, bundle, read[1], workspace_id=workspace.workspace_id)
        validate_bundle(bundle, untrusted=False)
        return manifest
    manifest = validate_bundle(bundle, untrusted=untrusted)
    if untrusted:
        manifest = rekey_untrusted(scratch, bundle, manifest, workspace_id=workspace.workspace_id)
    return manifest


def _presence(workspace: "Workspace", bundle: Path, manifest: BundleManifest, *, untrusted: bool) -> str | None:
    """``"all"`` when every member is here already, a refusal reason, or ``None`` to publish."""

    # ponytail: check-then-act; two operators adopting copies of one trusted bundle at once can duplicate it
    # (plan §13.2: fsck reports duplicate UUIDs). Exchange bundles are serialized by the exchange-name claim.
    ids = [member.job_id for member in manifest.members]
    placements = [member.placement for member in manifest.members]
    found = _kernel.locate_many(workspace, ids, placements=placements, settle=True)
    if len(found) == len(ids):
        return "all"
    if found:
        present = ", ".join(sorted(ref.job_key for ref in found.values()))
        return f"{len(found)} of its {len(ids)} jobs are already in this workspace: {present}"
    if untrusted:
        return None
    parent = JobDefinition.from_path(manifest.member_dir(bundle, manifest.members[0]) / "job.json").parent
    if parent is not None:
        hint = PurePosixPath(str(parent.get("placement")))
        ref = _kernel.locate(workspace, str(parent.get("job_id")), placement_hint=hint, settle=True)
        if ref is not None:
            return f"the root's parent {ref.job_key} is in this workspace; adopt the parent's whole tree instead"
    return None


def _finish(workspace: "Workspace", owner: Owner, scratch: Path, bundle: Path, record: _Record) -> AdoptReport | str:
    """Steps 2-6 of adoption on a taken bundle: the report, or the message of a refusal."""

    durable = workspace.durable
    read = _read_manifest(scratch / _PLAN)
    if read is None:
        try:
            manifest = _validated(workspace, scratch, bundle, untrusted=record.untrusted)
            presence = _presence(workspace, bundle, manifest, untrusted=record.untrusted)
        except BundleError as exc:
            return _refuse(owner, scratch, bundle, record, str(exc))
        if presence == "all":
            owner.discard_scratch(scratch)
            return AdoptReport((), True, ())
        if presence is not None:
            return _refuse(owner, scratch, bundle, record, presence)
        if record.untrusted:
            root = manifest.members[0]
            name = exchange_names(scratch)[root.job_id]
            claimed = _kernel.claim_exchange_name(workspace, owner, name, root.job_id, root.placement)
            # A claim fails when the name is indexed at another placement: the client resubmitted its job there. Its
            # rekeyed id derives from the name, so an indexed job that still exists is this one, already adopted.
            indexed = root.placement if claimed else _indexed_placement(workspace, name, root.job_id)
            if indexed is None:
                return _refuse(owner, scratch, bundle, record, f"the exchange name {name} is already in use")
            # The bundle's own placements were settled by _presence already.
            if _kernel.locate_many(workspace, [root.job_id], placements=[indexed], settle=not claimed):
                owner.discard_scratch(scratch)
                return AdoptReport((), True, ())
            if not claimed:
                return _refuse(
                    owner, scratch, bundle, record, f"the exchange name {name} is indexed at another placement"
                )
        _fs.write_file(_fs.loc(scratch / _PLAN), manifest.to_json(), durable=durable)
    else:
        manifest = read[1]
    names = exchange_names(scratch) if record.untrusted else {}
    published: list[JobRef] = []
    workflows: set[str] = set()
    for member in reversed(manifest.members):
        directory = manifest.member_dir(bundle, member)
        if not _fs.exists(_fs.loc(directory)):
            continue
        workflows.add(JobDefinition.from_path(directory / "job.json").workflow_id)
        if record.untrusted:
            doc = StateDoc.empty(member.job_id).updated(origin="exchange", exchange_name=names[member.job_id])
            _fs.write_file(_fs.loc(directory / "state.json"), encode_state(doc), durable=durable)
        published.append(_kernel.submit(workspace, owner, directory, state=member.state, priority=member.priority))
    owner.discard_scratch(scratch)
    missing = tuple(sorted(workflow for workflow in workflows if _installed(workspace, workflow) is None))
    return AdoptReport(tuple(published), False, missing)


def _indexed_placement(workspace: "Workspace", exchange_name: str, job_id: str) -> PurePosixPath | None:
    """The placement the exchange index records for *exchange_name*, if the entry names *job_id*."""

    path = workspace.control / "exchange-jobs" / exchange_name / "index.json"
    try:
        data = _fs.read_bounded(_fs.loc(path), _RECORD_LIMIT)
        value = None if data is None else json.loads(data)
        if not isinstance(value, dict) or value.get("job_id") != job_id:
            return None
        return normalize_placement(str(value["placement"]))
    except (WorkflowError, ValueError, KeyError):
        return None


def _installed(workspace: "Workspace", workflow_id: str) -> object:
    try:
        return _store.lookup(workspace, workflow_id)
    except ValueError:
        # An ambiguous name is not this id.
        return None


def adopt(
    workspace: "Workspace", owner: Owner, source: Path | _fs.Loc, *, untrusted: bool, refused_to: Path | None = None
) -> AdoptReport | None:
    """Take a bundle into the workspace: validate, rekey when untrusted, deduplicate and publish bottom-up.

    A bundle whose members are all present already is "already adopted"; one with only some present, or (when
    trusted) whose root's parent is here, is refused. An untrusted bundle's members get fresh ids, enter
    ``ready`` with ``origin: exchange`` and their client ids as ``exchange_name``, after the root's exchange
    name was claimed. A refused bundle moves to a fresh name below *refused_to*, else back to *source* when that
    is free, else it stays in the owner's scratch. Installation of the jobs' workflows is not checked.

    :param workspace: The workspace to adopt into.
    :param owner: The adopting owner.
    :param source: The bundle: an absolute path, or an anchored location in a directory others write.
    :param untrusted: Apply the exchange rules.
    :param refused_to: Where refused bundles go.
    :return: The report; ``None`` when another actor took *source* first.
    :raises httk.workflow._bundles.BundleError: When the bundle is refused; the message says where it went.
    """

    durable = workspace.durable
    src = source if isinstance(source, _fs.Loc) else _fs.loc(Path(source))
    name = src.name()
    if name in _RESERVED or _RECORD_TEMPORARY.fullmatch(name):
        raise BundleError(f"a bundle may not be named {name!r}")
    record = _Record(
        None if src.at is not None else str(src.path), None if refused_to is None else str(refused_to), False, untrusted
    )
    try:
        scratch = _kernel.take(workspace, owner, src, _ADOPT[untrusted])
    except _fs.CrossDevice:
        # ponytail: take() leaves its empty adopt scratch behind; close or recovery discards it.
        if src.at is not None:
            raise
        scratch = owner.scratch(_ADOPT[untrusted])
        record = dataclasses.replace(record, copied=True)
        _write_record(scratch, record, durable=durable)
        partial = scratch / _PARTIAL / name
        _fs.copy_tree(src.path, partial, durable=durable)
        # Complete only once renamed: the reconciler discards a scratch without the bundle.
        _fs.move_owned(_fs.loc(partial), _fs.loc(scratch / name), durable=durable)
        _LOGGER.warning("copied %s from another filesystem; the source stays in place", src.path)
    else:
        if scratch is None:
            return None
        _write_record(scratch, record, durable=durable)
    result = _finish(workspace, owner, scratch, scratch / name, record)
    if isinstance(result, str):
        raise BundleError(result)
    return result


def reconcile_adopt(owner: Owner, scratch: Path) -> bool:
    """The ``adopt`` scratch reconciler: resume adoption from validation (see the module docstring).

    :param owner: The owner the scratch is named after.
    :param scratch: ``tmp/<owner-id>.adopt.<token>/`` or ``tmp/<owner-id>.adopt-untrusted.<token>/``.
    :return: ``True`` when resolved; ``False`` when a refused bundle has nowhere to go and stays.
    """

    entries = [
        name for name in sorted(os.listdir(scratch)) if name not in _RESERVED and not _RECORD_TEMPORARY.fullmatch(name)
    ]
    if not entries:
        # An incomplete cross-filesystem copy (its source is untouched), or nothing was taken.
        return True
    if len(entries) > 1:
        _LOGGER.warning("keeping %s: it holds more than one bundle: %s", scratch, entries)
        return False
    workspace = cast("Workspace", owner.workspace)
    untrusted = scratch.name.split(".")[1] == _ADOPT[True]
    result = _finish(workspace, owner, scratch, scratch / entries[0], _read_record(scratch, untrusted=untrusted))
    if isinstance(result, str):
        _LOGGER.warning("while reconciling %s: %s", scratch, result)
        return not _fs.exists(_fs.loc(scratch))
    return True


# -- holds ----------------------------------------------------------------------------------------------------------


def _outgoing(workspace: _kernel.KernelWorkspace) -> Path:
    return workspace.control / "transfers" / "outgoing"


def hold(workspace: "Workspace", owner: Owner, root: OwnedJob, *, tree: bool) -> Path:
    """Eject into this workspace's own ``transfers/outgoing/<transfer-id>/``: the held bundle of a transfer.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The claimed, quiescent root.
    :param tree: Hold the root's descendants too.
    :return: The held bundle directory.
    :raises Busy: As :func:`eject`.
    """

    outgoing = _outgoing(workspace)
    _fs.make_dirs(outgoing, durable=workspace.durable)
    return _eject(workspace, owner, root, destination=outgoing, tree=tree, by_transfer_id=True).destination


def held(workspace: "Workspace") -> list[Hold]:
    """List the held bundles with their manifests.

    :param workspace: The workspace.
    :return: The holds, by transfer id.
    """

    outgoing = _outgoing(workspace)
    try:
        names = sorted(os.listdir(outgoing))
    except FileNotFoundError:
        return []
    holds = []
    for name in names:
        if _TRANSFER_ID.fullmatch(name) and (read := _read_manifest(outgoing / name / "bundle.json")) is not None:
            holds.append(Hold(name, outgoing / name, read[1]))
    return holds


def release_hold(workspace: "Workspace", owner: Owner, transfer_id: str) -> bool:
    """Discard a held bundle once its destination adopted it.

    :param workspace: The workspace.
    :param owner: The releasing owner.
    :param transfer_id: The hold's transfer id.
    :return: ``False`` when the hold is gone (another actor released or adopted it).
    :raises ValueError: For a malformed transfer id.
    """

    if not _TRANSFER_ID.fullmatch(transfer_id):
        raise ValueError(f"not a transfer id: {transfer_id!r}")
    scratch = _kernel.take(workspace, owner, _fs.loc(_outgoing(workspace) / transfer_id), "release")
    if scratch is None:
        return False
    owner.discard_scratch(scratch)
    return True


_kernel.register_reconciler("eject", reconcile_eject)
for _purpose in _ADOPT.values():
    _kernel.register_reconciler(_purpose, reconcile_adopt)
