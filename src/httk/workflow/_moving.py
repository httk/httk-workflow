"""Moving jobs between workspaces: eject, adopt, held transfers, and the scratch reconcilers of eject and adopt.

Everything here composes :mod:`~httk.workflow._kernel` and :mod:`~httk.workflow._bundles`; every move and write
goes through :mod:`~httk.workflow._fs`.

**Eject** claims the tree members, builds the bundle in an ``eject`` scratch (a ``hold`` scratch for a hold;
``bundle.json`` first, recording each member's ``from`` state and priority) and delivers it to
``<destination>/<root key>`` (a hold to ``transfers/outgoing/<transfer id>``); across filesystems it copies to
``<destination>/.<name>.partial.<transfer id>`` and delivers that. An occupied destination, a failed delivery or
an error after some members were extracted runs the reconciler's decision at once, which rolls the members back;
a refusal before the bundle is built gives every claimed job back. A delivered exchange root leaves the exchange
index (decided from the bundle root's ``state.json``).

**Adopt** takes a bundle into an ``adopt`` scratch, validates it (the trust boundary), rekeys an untrusted one,
deduplicates against the workspace and publishes the members bottom-up. A bundle already adopted is discarded
only when it is a duplicate delivery; an operator's bundle is refused back to its path, and a refused copy of a
hold (in ``transfers/incoming/``) is discarded. An untrusted bundle is first *isolated*: right after the take
it is copied, descriptor-anchored, to ``.partial/<name>`` in the scratch, and that copy replaces the taken
original, which a client holding open descriptors could still change. Every later step works on the copy.

**Crash-resume contract** of the reconcilers, registered with the kernel at import:

- ``eject`` and ``hold``: if the destination carries this bundle's exact ``bundle.json`` (an eject's under the
  root key in its recorded destination, a hold's under the transfer id in ``transfers/outgoing``), the delivery
  happened, an exchange root leaves the index, and the scratch is discarded (roll forward). Otherwise every
  member still in the bundle is submitted back to its recorded state and priority (roll back). A destination
  that cannot be read keeps the scratch. Delivery is at least once: verify the destination before re-ejecting.
- ``adopt`` and ``adopt-untrusted`` (the trust is in the scratch's purpose, so it is known from the take on):
  ``source.json`` records where the bundle came from, where refusals go and the adoption's nonce (kept in the
  exchange index entry it claims, so a rerun recognizes its own claim and another adopter of a copy does not);
  without it (a crash right after the take) the reconciler records a fresh one, and a refused bundle goes to
  ``exchange/outbox/rejected`` when untrusted and stays in the scratch when trusted. A refusal moves the bundle
  to a fresh ``<refused_to>/<unique>/<name>`` beside its ``reason.json``, every directory opened without
  following a symlink.
  Isolation of an untrusted bundle: until ``source.json`` records ``isolated``, the taken original is the bundle
  and any ``.partial`` is an interrupted copy, discarded and made again. Once it does, ``.partial/<name>`` (if
  present) is the complete copy: the original is discarded and the copy renamed into place.
  ``plan.json`` is the validated, deduplicated manifest: once it exists only publication remains, and a member
  directory missing from the bundle was published by the earlier run. Before it, validation, rekeying,
  deduplication and the exchange-name claim run again; each is idempotent.
"""

import dataclasses
import json
import logging
import os
import re
import stat
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Self, cast

from httk.workflow import _fs, _kernel, _store
from httk.workflow._bundles import (
    BundleError,
    BundleManifest,
    build_bundle,
    exchange_names,
    read_manifest,
    read_rekeyed,
    rekey_untrusted,
    validate_bundle,
)
from httk.workflow._job import JobDefinition
from httk.workflow._kernel import OWNED, JobRef, OwnedJob, Owner
from httk.workflow._state import TERMINAL_STATES, StateDoc, encode_state, read_state_unowned
from httk.workflow._util import json_bytes, utc_now
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.models import EXCHANGE_DIRECTORY, WORKSPACE_DIRECTORY, canonical_uuid, placement_text

if TYPE_CHECKING:  # pragma: no cover
    from httk.workflow.workspace import Workspace

__all__ = [
    "RESERVED_NAMES",
    "AdoptReport",
    "Busy",
    "EjectReport",
    "Hold",
    "adopt",
    "destination_problem",
    "eject",
    "held",
    "hold",
    "reconcile_adopt",
    "reconcile_eject",
    "release_hold",
    "tree_members",
]

_LOGGER = logging.getLogger(__name__)

#: The states a tree member may be ejected from; the root may be in any unowned state.
_MEMBER_STATES = frozenset({*TERMINAL_STATES, "paused"})
_TRANSFER_ID = re.compile(r"[0-9a-f]{32}")
_BUNDLE = "bundle"
#: The scratch purpose of a hold; an eject's is ``eject``.
_HOLD = "hold"
#: The scratch purposes of adoption: the trust travels in the name, atomically with the take.
_ADOPT = {False: "adopt", True: "adopt-untrusted"}
_SOURCE = "source.json"
_PLAN = "plan.json"
_PARTIAL = ".partial"
#: Names an ``adopt`` scratch holds besides the bundle; a bundle may not be named like one of them.
RESERVED_NAMES = frozenset({_SOURCE, _PLAN, "rekey.json", _PARTIAL})
_RECORD_TEMPORARY = re.compile(r"\.(source|plan|rekey)\.json\.[a-z2-7]{16}\.tmp")
_RECORD_LIMIT = 1 << 16
#: The format of the ``reason.json`` beside a refused bundle.
REJECTION_FORMAT = "httk-workspace-exchange-rejection"


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
    :param copied: The bundle was copied from another filesystem, so its source is untouched.
    """

    published: tuple[JobRef, ...]
    already_adopted: bool
    missing_workflows: tuple[str, ...]
    copied: bool = False


@dataclass(frozen=True)
class Hold:
    """A held bundle, ``transfers/outgoing/<transfer-id>/``, as :func:`held` lists it or a remote reports it.

    :param transfer_id: The bundle's transfer id, also its directory name.
    :param path: The bundle directory, on the host of the workspace holding it.
    :param members: The members' job keys, top-down; the first is the root.
    :param destination_locator: The transfer's destination as recorded, or ``None``.
    :param destination_workspace_id: The destination workspace's id as recorded, or ``None``.
    :param created_at: When the bundle was built, an ISO 8601 timestamp with a time zone.
    """

    transfer_id: str
    path: str
    members: tuple[str, ...]
    destination_locator: str | None
    destination_workspace_id: str | None
    created_at: str

    @classmethod
    def from_manifest(cls, path: Path, manifest: BundleManifest) -> Self:
        """Describe the held bundle at *path* from its manifest.

        :param path: The bundle directory.
        :param manifest: Its manifest.
        :return: The hold.
        """

        return cls(
            manifest.transfer_id,
            str(path),
            tuple(member.job_key for member in manifest.members),
            manifest.destination_locator,
            manifest.destination_workspace_id,
            manifest.created_at,
        )

    def as_mapping(self, now: float | None = None) -> dict[str, object]:
        """Return the hold as ``transfer status --json`` and ``job eject --hold --json`` print it.

        :param now: The time its age is measured against; the current time by default.
        :return: A fresh mapping.
        """

        created = datetime.fromisoformat(self.created_at).timestamp()
        return {
            "transfer_id": self.transfer_id,
            "path": self.path,
            "root": self.members[0],
            "members": list(self.members),
            "destination": self.destination_locator,
            "destination_workspace_id": self.destination_workspace_id,
            "created_at": self.created_at,
            "age_seconds": max(0, int((time.time() if now is None else now) - created)),
        }

    @classmethod
    def from_mapping(cls, value: object) -> Self:
        """Parse a hold another workspace reported (:meth:`as_mapping`); derived members are ignored.

        :param value: The decoded mapping.
        :return: The hold.
        :raises ValueError: For a malformed hold: a bad transfer id, a path not ending in it, no members, or a
            malformed destination, workspace id or timestamp.
        """

        if not isinstance(value, dict):
            raise ValueError("a hold must be a JSON object")
        transfer_id, path, members = value.get("transfer_id"), value.get("path"), value.get("members")
        locator, workspace_id, created_at = (
            value.get("destination"),
            value.get("destination_workspace_id"),
            value.get("created_at"),
        )
        if (
            not isinstance(transfer_id, str)
            or not _TRANSFER_ID.fullmatch(transfer_id)
            or not isinstance(path, str)
            or PurePosixPath(path).name != transfer_id
            or not isinstance(members, list)
            or not members
            or not all(isinstance(member, str) for member in members)
            or not (locator is None or isinstance(locator, str))
            or not isinstance(created_at, str)
        ):
            raise ValueError(f"a malformed hold: {value!r}")
        if workspace_id is not None:
            workspace_id = canonical_uuid(workspace_id, "destination_workspace_id")
        if datetime.fromisoformat(created_at).tzinfo is None:
            raise ValueError(f"a hold's created_at has no time zone: {created_at!r}")
        return cls(transfer_id, path, tuple(members), locator, workspace_id, created_at)


# -- eject ----------------------------------------------------------------------------------------------------------


def tree_members(workspace: _kernel.KernelWorkspace, root_id: str, root_doc: StateDoc | None) -> list[JobRef]:
    """List the descendants an eject of the root with *tree* moves, top-down (breadth first).

    The children a parent published are recorded in its ``state.json`` and confirmed from the child's side: its
    ``job.json`` names this parent and it is not detached. A recorded child is looked up across the visibility
    deadline before it counts as gone (and so as no member).

    :param workspace: The workspace.
    :param root_id: The root's job UUID.
    :param root_doc: The root's ``state.json``, or ``None``.
    :return: The members besides the root.
    :raises Busy: For a member neither terminal nor paused, or one whose ``job.json`` or ``state.json`` is
        unreadable.
    """

    found: list[JobRef] = []
    queue: list[tuple[str, StateDoc | None]] = [(root_id, root_doc)]
    seen = {root_id}
    while queue:
        parent_id, doc = queue.pop(0)
        for child in doc.children if doc is not None else ():
            job_id = str(child.get("job_id"))
            if job_id in seen:
                continue
            hint = PurePosixPath(str(child.get("placement")))
            ref = _kernel.locate(workspace, job_id, placement_hint=hint, settle=True)
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
    refs = tree_members(workspace, root.job_id, root.read_state())
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


def destination_problem(destination: Path) -> str | None:
    """Report why *destination* cannot receive an eject, before anything moves.

    It must be absolute, and either a real directory or a missing name whose parent is a real directory; a
    symlink is neither.

    :param destination: The eject destination.
    :return: The reason, or ``None`` when it is usable.
    """

    if not destination.is_absolute():
        return f"the eject destination must be absolute: {destination}"
    checked = destination
    try:
        try:
            mode = os.lstat(checked).st_mode
        except FileNotFoundError:
            checked = destination.parent
            mode = os.lstat(checked).st_mode
    except OSError as exc:
        return f"the eject destination {destination} is unusable: {exc}"
    return None if stat.S_ISDIR(mode) else f"{checked} is a symlink or not a directory"


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


def _exchange_root(bundle: Path, manifest: BundleManifest) -> tuple[str, str] | None:
    """The exchange name and job id of a bundle whose root is an exchange job, from the root's ``state.json``."""

    root = manifest.members[0]
    doc, _damaged = read_state_unowned(manifest.member_dir(bundle, root) / "state.json")
    if doc is None or doc.origin != "exchange" or doc.exchange_name is None:
        return None
    return doc.exchange_name, root.job_id


def _leave_index(owner: Owner, exchange: tuple[str, str] | None) -> None:
    # A delivered exchange root (returned, ejected or held) leaves the index; a child's entry is its root's.
    if exchange is not None:
        _kernel.drop_exchange_index(owner, *exchange)


def _settle(owner: Owner, scratch: Path) -> bool | None:
    """Resolve an ``eject`` or ``hold`` scratch: ``True`` when its bundle was delivered, ``False`` when every member
    still in it went back (or none was taken), ``None`` when the destination cannot be read (the scratch stays)."""

    bundle = scratch / _BUNDLE
    read = read_manifest(bundle / "bundle.json")
    if read is None:
        # build_bundle writes bundle.json before any member moves: nothing to return.
        return False
    data, manifest = read
    if scratch.name.split(".")[1] == _HOLD:
        candidates = [_outgoing(owner.workspace) / manifest.transfer_id]
    else:
        # An eject records its absolute destination (destination_problem) and delivers under the root's key.
        locator = manifest.destination_locator
        candidates = [] if locator is None else [Path(locator) / manifest.members[0].job_key]
    try:
        # The destination may be writable by others (the exchange outbox): never follow a symlink there.
        if any(_fs.carries(_fs.loc(candidate), "bundle.json", data) for candidate in candidates):
            _leave_index(owner, _exchange_root(bundle, manifest))
            return True
        partials = [path.with_name(f".{path.name}.partial.{manifest.transfer_id}") for path in candidates]
        for partial in partials:
            if _fs.exists(_fs.loc(partial)):
                # An undelivered cross-filesystem copy is ours alone; the members go back, so it goes too.
                _fs.discard(_fs.loc(partial), trash_dir=partial.parent, durable=owner.workspace.durable)
    except OSError as exc:
        _LOGGER.warning("keeping %s: cannot check its destination: %s", scratch, exc)
        return None
    returned = _roll_back(owner, bundle, manifest)
    _LOGGER.warning("rolled back the eject %s: %d jobs returned", manifest.transfer_id, len(returned))
    return False


def _eject(
    workspace: "Workspace",
    owner: Owner,
    root: OwnedJob,
    *,
    destination: Path,
    tree: bool,
    hold: bool,
    locator: str | None,
    destination_workspace_id: str | None = None,
) -> tuple[EjectReport, BundleManifest]:
    members: list[OwnedJob] = []
    scratch: Path | None = None
    try:
        if tree:
            members = _claim_tree(workspace, owner, root)
        elif children := tree_members(workspace, root.job_id, root.read_state()):
            # Ejecting the root alone would orphan the children present here: it is refused.
            raise Busy(children[0].job_key, "it is a child of the job; eject the whole tree")
        scratch = owner.scratch(_HOLD if hold else "eject")
        bundle = build_bundle(
            owner,
            [root, *members],
            source_workspace_id=workspace.workspace_id,
            destination_workspace_id=destination_workspace_id,
            destination_locator=locator,
            event=("ejected", {"destination": str(destination)}),
            scratch=scratch,
        )
    except Exception:
        # A refusal moved nothing: every job still held goes back, the root included. An error after some members
        # were extracted is settled now, never left in the scratch until this owner closes.
        for job in (root, *members):
            if owner.holds(job.ref):
                job.give_back()
        if scratch is not None and _settle(owner, scratch) is not None:
            owner.discard_scratch(scratch)
        raise
    read = read_manifest(bundle / "bundle.json")
    assert read is not None
    token, manifest = read
    target = destination / (manifest.transfer_id if hold else root.job_key)
    report = EjectReport(target, tuple(member.job_key for member in manifest.members), manifest.transfer_id)
    # Read in the scratch, before the bundle reaches a destination others may write.
    exchange = _exchange_root(bundle, manifest)
    error: Exception | None = None
    try:
        if _deliver(workspace, bundle, target, token, manifest.transfer_id):
            _leave_index(owner, exchange)
            owner.discard_scratch(scratch)
            return report, manifest
        problem = f"{target} is occupied"
    except Exception as exc:
        # Never left in the scratch until this owner closes: the reconciler's decision is taken now.
        problem, error = f"cannot deliver to {target}: {exc}", exc
    settled = _settle(owner, scratch)
    if settled is None:
        raise WorkflowError(f"{problem}; the bundle stays in {scratch} for the eject reconciler") from error
    owner.discard_scratch(scratch)
    if settled:
        return report, manifest  # the delivery happened after all
    raise WorkflowError(f"{problem}; the jobs were returned to the states they were taken from") from error


def eject(workspace: "Workspace", owner: Owner, root: OwnedJob, *, destination: Path, tree: bool) -> EjectReport:
    """Move a claimed job, and with *tree* its descendants, out of the workspace into ``<destination>/<root key>``.

    Tree members are the root's non-detached descendants (:func:`tree_members`); each must be terminal or paused,
    and they are claimed in sorted job-UUID order. Without *tree*, a root that has such descendants present is
    refused. *root*'s handle is retired whatever happens: a refusal, an occupied destination or a failed delivery
    returns every job, the root included, to the state and priority it was taken from.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The claimed, quiescent root, in any unowned state.
    :param destination: An absolute directory, or a missing name in one (:func:`destination_problem`).
    :param tree: Eject the root's descendants too.
    :return: Where the bundle went.
    :raises Busy: When a member cannot be claimed, or without *tree* when the root has descendants.
    :raises ValueError: For an unusable destination.
    :raises httk.workflow.errors.WorkflowError: When the destination is occupied or the delivery failed, or
        :class:`~httk.workflow._bundles.BundleError` when the jobs cannot form a bundle.
    """

    destination = Path(destination)
    if (problem := destination_problem(destination)) is not None:
        root.give_back()
        raise ValueError(problem)
    report, _manifest = _eject(
        workspace, owner, root, destination=destination, tree=tree, hold=False, locator=str(destination)
    )
    return report


def reconcile_eject(owner: Owner, scratch: Path) -> bool:
    """The ``eject`` and ``hold`` scratch reconciler: roll forward when the bundle was delivered, otherwise roll back.

    :param owner: The owner the scratch is named after.
    :param scratch: ``tmp/<owner-id>.eject.<token>/`` or ``tmp/<owner-id>.hold.<token>/``.
    :return: ``True`` when resolved (the kernel then discards the scratch); ``False`` when the destination
        cannot be read, which keeps the scratch.
    """

    return _settle(owner, scratch) is not None


# -- adopt ----------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Record:
    source: str | None
    refused_to: str | None
    copied: bool
    #: This adoption's own nonce, recorded in the exchange index entry it claims.
    nonce: str
    untrusted: bool  # from the scratch's purpose, not stored
    #: An untrusted bundle's complete copy exists: the taken original no longer counts (see the module docstring).
    isolated: bool = False


def _write_record(scratch: Path, record: _Record, *, durable: bool) -> None:
    fields = {
        "source": record.source,
        "refused_to": record.refused_to,
        "copied": record.copied,
        "nonce": record.nonce,
        "isolated": record.isolated,
    }
    _fs.write_file(_fs.loc(scratch / _SOURCE), json.dumps(fields).encode(), durable=durable)


def _rejected(workspace: _kernel.KernelWorkspace) -> Path:
    return workspace.root / EXCHANGE_DIRECTORY / "outbox" / "rejected"


def _read_record(workspace: _kernel.KernelWorkspace, scratch: Path, *, untrusted: bool) -> _Record:
    data = _fs.read_bounded(_fs.loc(scratch / _SOURCE), _RECORD_LIMIT)
    if data is None:
        # Taken, then the adopter died before recording, so before any claim: a fresh nonce is this adoption's.
        # A trusted bundle refused now has nowhere to go; an exchange bundle goes to the exchange's rejections.
        refused_to = str(_rejected(workspace)) if untrusted else None
        record = _Record(None, refused_to, False, _fs.fresh_token(), untrusted)
        _write_record(scratch, record, durable=workspace.durable)
        return record
    value = json.loads(data)
    return _Record(
        value["source"],
        value["refused_to"],
        bool(value["copied"]),
        str(value["nonce"]),
        untrusted,
        bool(value.get("isolated", False)),
    )


def _reject(workspace: _kernel.KernelWorkspace, bundle: Path, refused_to: Path, reason: str) -> Path:
    """Move a refused bundle to a fresh ``<refused_to>/<unique>/<name>``, beside its ``reason.json``.

    *refused_to* may be writable by a client (``exchange/outbox/rejected``): below the workspace root (or
    below its parent, outside the workspace) every directory is opened without following a symlink, and the
    files go in anchored at the fresh directory's descriptor.
    """

    durable, root = workspace.durable, workspace.root
    if refused_to.is_relative_to(root):
        anchor, relative = root, PurePosixPath(refused_to.relative_to(root))
    else:
        anchor, relative = refused_to.parent, PurePosixPath(refused_to.name)
    unique = _fs.fresh_token()
    directory = _fs.open_dir_under(anchor, relative / unique, create=True, mode=0o755, durable=durable)
    try:
        document = {
            "format": REJECTION_FORMAT,
            "format_version": 1,
            "name": bundle.name,
            "reason": reason,
            "rejected_at": utc_now(),
        }
        _fs.write_file(_fs.anchored(directory, "reason.json"), json_bytes(document) + b"\n", durable=durable)
        _fs.move_owned(_fs.loc(bundle), _fs.anchored(directory, bundle.name), durable=durable)
    finally:
        os.close(directory)
    return refused_to / unique / bundle.name


def _refuse(owner: Owner, scratch: Path, bundle: Path, record: _Record, reason: str) -> str:
    """Move a refused bundle away and discard the scratch, or keep it; return the complete message."""

    durable = owner.workspace.durable
    if record.copied:
        owner.discard_scratch(scratch)
        return f"bundle refused: {reason}; it was copied from another filesystem and its source is untouched"
    if record.refused_to is not None:
        try:
            target = _reject(owner.workspace, bundle, Path(record.refused_to), reason)
        except (_fs.UnsafePath, _fs.MoveFailed, OSError) as exc:
            # A client replaced the directory (a symlink): the bundle waits in the scratch for a later refusal.
            return f"bundle refused: {reason}; it stays in {scratch}, since {record.refused_to} cannot take it: {exc}"
    elif record.source is not None and Path(record.source).parent == owner.workspace.control / "transfers" / "incoming":
        # A remote transfer's landing is a copy of a hold by construction: the hold stays, the copy goes.
        owner.discard_scratch(scratch)
        return f"bundle refused: {reason}; it was a copy of a held bundle and was discarded (the hold stays)"
    elif record.source is not None and not _fs.exists(_fs.loc(Path(record.source))):
        _fs.move_owned(_fs.loc(bundle), _fs.loc(Path(record.source)), durable=durable)
        owner.discard_scratch(scratch)
        return f"bundle refused: {reason}; the bundle was left at {record.source}"
    else:
        return f"bundle refused: {reason}; it stays in {scratch}"
    owner.discard_scratch(scratch)
    return f"bundle refused: {reason}; it was moved to {target}"


def _validated(workspace: "Workspace", scratch: Path, bundle: Path, *, untrusted: bool) -> BundleManifest:
    if read_rekeyed(scratch) is not None:
        # An interrupted rekey: finish it, then the bundle is a trusted one (the _bundles contract).
        read = read_manifest(bundle / "bundle.json")
        if read is None:
            raise BundleError(f"{bundle}/bundle.json is missing")
        manifest = rekey_untrusted(scratch, bundle, read[1], workspace_id=workspace.workspace_id)
        validate_bundle(bundle, untrusted=False)
        return manifest
    manifest = validate_bundle(bundle, untrusted=untrusted)
    if untrusted:
        manifest = rekey_untrusted(scratch, bundle, manifest, workspace_id=workspace.workspace_id)
    return manifest


def _redelivered(workspace: "Workspace", record: _Record) -> bool:
    """Whether a bundle whose jobs are all here is a duplicate delivery that may be discarded (plan §9.3).

    That is an exchange bundle, a cross-filesystem copy (its source is untouched), or a bundle the transfer
    machinery placed: a hold or landing in some workspace's ``transfers/outgoing`` or ``transfers/incoming``, or
    in this workspace's own scratch (a pulled landing). Any other bundle is an operator's, never destroyed.
    """

    if record.untrusted or record.copied:
        return True
    if record.source is None:
        return False
    source = Path(record.source)
    transfers = {(WORKSPACE_DIRECTORY, "transfers", "outgoing"), (WORKSPACE_DIRECTORY, "transfers", "incoming")}
    return source.parent.parts[-3:] in transfers or source.is_relative_to(workspace.control / "tmp")


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
    read = read_manifest(scratch / _PLAN)
    if read is None:
        try:
            manifest = _validated(workspace, scratch, bundle, untrusted=record.untrusted)
            presence = _presence(workspace, bundle, manifest, untrusted=record.untrusted)
        except BundleError as exc:
            return _refuse(owner, scratch, bundle, record, str(exc))
        if presence == "all":
            if not _redelivered(workspace, record):
                return _refuse(owner, scratch, bundle, record, "already adopted")
            owner.discard_scratch(scratch)
            return AdoptReport((), True, (), record.copied)
        if presence is not None:
            return _refuse(owner, scratch, bundle, record, presence)
        if record.untrusted:
            root = manifest.members[0]
            name = exchange_names(scratch)[root.job_id]
            claim = _kernel.claim_exchange_name(workspace, owner, name, root.job_id, root.placement, nonce=record.nonce)
            if claim is _kernel.ExchangeClaim.OTHER:
                indexed = _kernel.exchange_index(workspace, name)
                if indexed is None or indexed[0] != root.job_id:
                    return _refuse(owner, scratch, bundle, record, f"the exchange name {name} is already in use")
                # Another adoption (of a copy of this bundle, or of the client's resubmission at another placement)
                # holds the name. The rekeyed id derives from the name: a job present there is this one.
                if _kernel.locate_many(workspace, [root.job_id], placements=[indexed[1]], settle=True):
                    owner.discard_scratch(scratch)
                    return AdoptReport((), True, (), record.copied)
                return _refuse(
                    owner,
                    scratch,
                    bundle,
                    record,
                    f"the exchange name {name} is claimed by another adoption whose job is not here",
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
            # A fresh client job has no state.json of its own: its children are recorded here, so the tree is the
            # one eject (and the exchange's return) moves, as for children spawned in the workspace.
            children = [
                {"job_id": child.job_id, "job_key": child.job_key, "placement": placement_text(child.placement)}
                for child in manifest.members
                if child.parent_job_id == member.job_id
            ]
            if children:
                doc = doc.with_children(children)
            _fs.write_file(_fs.loc(directory / "state.json"), encode_state(doc), durable=durable)
        published.append(_kernel.submit(workspace, owner, directory, state=member.state, priority=member.priority))
    owner.discard_scratch(scratch)
    missing = tuple(sorted(workflow for workflow in workflows if _installed(workspace, workflow) is None))
    return AdoptReport(tuple(published), False, missing, record.copied)


def _installed(workspace: "Workspace", workflow_id: str) -> object:
    try:
        return _store.lookup(workspace, workflow_id)
    except ValueError:
        # An ambiguous name is not this id.
        return None


def _isolate(owner: Owner, scratch: Path, name: str, record: _Record) -> _Record | str:
    """Replace a taken untrusted bundle by the adopter's own copy: the record, or the message of a refusal.

    A client may hold open descriptors into the bundle it handed in and rename or replace its entries at any
    time; the copy, made descriptor-anchored, is out of its reach, and every later step works on it.
    """

    durable = owner.workspace.durable
    staged = scratch / _PARTIAL
    if not record.isolated:
        if _fs.exists(_fs.loc(staged)):
            owner.discard_tree(staged)  # an interrupted copy
        try:
            _fs.copy_tree(scratch / name, staged / name, durable=durable, limits=_fs.DEFAULT_LIMITS)
        except (OSError, _fs.UnsafePath, _fs.UntrustedContentError) as exc:
            return _refuse(owner, scratch, scratch / name, record, f"the bundle cannot be copied: {exc}")
        record = dataclasses.replace(record, isolated=True)
        _write_record(scratch, record, durable=durable)
    if _fs.exists(_fs.loc(staged / name)):
        if _fs.exists(_fs.loc(scratch / name)):
            owner.discard_tree(scratch / name)  # the taken original
        _fs.move_owned(_fs.loc(staged / name), _fs.loc(scratch / name), durable=durable)
    _fs.remove_empty_dir(_fs.loc(staged))
    return record


def adopt(
    workspace: "Workspace", owner: Owner, source: Path | _fs.Loc, *, untrusted: bool, refused_to: Path | None = None
) -> AdoptReport | None:
    """Take a bundle into the workspace: validate, rekey when untrusted, deduplicate and publish bottom-up.

    A bundle whose members are all present already is "already adopted" and discarded when it is a duplicate
    delivery (an exchange bundle, a copy, or a transfer's hold or landing); any other such bundle (an operator's
    path) is refused and left in place. One with only some members present, or (when trusted) whose root's
    parent is here, is refused. An untrusted bundle's members get fresh ids, enter
    ``ready`` with ``origin: exchange`` and their client ids as ``exchange_name``, after the root's exchange
    name was claimed; first, right after the take, the bundle is replaced by the adopter's own copy (isolation,
    see the module docstring). A refused bundle moves to a fresh name below *refused_to*; a copy of a hold landed
    in ``transfers/incoming/`` is discarded; any other goes back to *source* when that is free, else it stays in
    the owner's scratch. Installation of the jobs' workflows is not checked.

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
    if name in RESERVED_NAMES or _RECORD_TEMPORARY.fullmatch(name):
        raise BundleError(f"a bundle may not be named {name!r}")
    record = _Record(
        None if src.at is not None else str(src.path),
        None if refused_to is None else str(refused_to),
        False,
        _fs.fresh_token(),
        untrusted,
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
        _fs.copy_tree(src.path, partial, durable=durable, limits=_fs.DEFAULT_LIMITS if untrusted else None)
        # Complete only once renamed: the reconciler discards a scratch without the bundle.
        _fs.move_owned(_fs.loc(partial), _fs.loc(scratch / name), durable=durable)
        _LOGGER.warning("copied %s from another filesystem; the source stays in place", src.path)
    else:
        if scratch is None:
            return None
        _write_record(scratch, record, durable=durable)
        if untrusted:
            isolated = _isolate(owner, scratch, name, record)
            if isinstance(isolated, str):
                raise BundleError(isolated)
            record = isolated
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

    workspace = cast("Workspace", owner.workspace)
    untrusted = scratch.name.split(".")[1] == _ADOPT[True]
    entries = [
        name
        for name in _kernel.list_names(scratch)
        if name not in RESERVED_NAMES and not _RECORD_TEMPORARY.fullmatch(name)
    ]
    if (
        not entries
        and untrusted
        and _fs.exists(_fs.loc(scratch / _SOURCE))
        and _read_record(workspace, scratch, untrusted=True).isolated
    ):
        # An isolated bundle whose original was discarded before its copy was renamed into place.
        entries = _kernel.list_names(scratch / _PARTIAL)
    if not entries:
        # An incomplete cross-filesystem copy (its source is untouched), or nothing was taken.
        return True
    if len(entries) > 1:
        _LOGGER.warning("keeping %s: it holds more than one bundle: %s", scratch, entries)
        return False
    record = _read_record(workspace, scratch, untrusted=untrusted)
    if untrusted and not record.copied:
        isolated = _isolate(owner, scratch, entries[0], record)
        if isinstance(isolated, str):
            _LOGGER.warning("while reconciling %s: %s", scratch, isolated)
            return not _fs.exists(_fs.loc(scratch))
        record = isolated
    result = _finish(workspace, owner, scratch, scratch / entries[0], record)
    if isinstance(result, str):
        _LOGGER.warning("while reconciling %s: %s", scratch, result)
        return not _fs.exists(_fs.loc(scratch))
    return True


# -- holds ----------------------------------------------------------------------------------------------------------


def _outgoing(workspace: _kernel.KernelWorkspace) -> Path:
    return workspace.control / "transfers" / "outgoing"


def hold(
    workspace: "Workspace",
    owner: Owner,
    root: OwnedJob,
    *,
    tree: bool,
    destination_locator: str | None = None,
    destination_workspace_id: str | None = None,
) -> Hold:
    """Eject into this workspace's own ``transfers/outgoing/<transfer-id>/``: the held bundle of a transfer.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The claimed, quiescent root.
    :param tree: Hold the root's descendants too.
    :param destination_locator: The transfer's destination, recorded in ``bundle.json`` for resumption.
    :param destination_workspace_id: The destination workspace's id, recorded in ``bundle.json``, so that a
        resumption adopts only into that workspace.
    :return: The hold.
    :raises Busy: As :func:`eject`, which also gives the other failures and their outcome: *root*'s handle is
        retired whatever happens.
    """

    report, manifest = _eject(
        workspace,
        owner,
        root,
        destination=_outgoing(workspace),
        tree=tree,
        hold=True,
        locator=destination_locator,
        destination_workspace_id=destination_workspace_id,
    )
    return Hold.from_manifest(report.destination, manifest)


def held(workspace: "Workspace") -> list[Hold]:
    """List the held bundles.

    :param workspace: The workspace.
    :return: The holds, by transfer id.
    """

    outgoing = _outgoing(workspace)
    holds = []
    for name in _kernel.list_names(outgoing):
        if _TRANSFER_ID.fullmatch(name) and (read := read_manifest(outgoing / name / "bundle.json")) is not None:
            holds.append(Hold.from_manifest(outgoing / name, read[1]))
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
_kernel.register_reconciler(_HOLD, reconcile_eject)
for _purpose in _ADOPT.values():
    _kernel.register_reconciler(_purpose, reconcile_adopt)
