"""Moving jobs between workspaces: eject, adopt, held transfers, and the scratch reconcilers of eject and adopt.

Everything here composes :mod:`~httk.workflow._kernel` and :mod:`~httk.workflow._bundles`; every move and write
goes through :mod:`~httk.workflow._fs`.

**Eject** claims the tree members, builds the bundle in an ``eject`` scratch (a ``hold`` scratch for a hold;
``bundle.json`` first, recording each member's ``from`` state and priority), records ``eject.json`` beside the
bundle (the target, the token, the transfer id and the exchange root, if any) and delivers the bundle to
``<destination>/<root key>`` (a hold to ``transfers/outgoing/<transfer id>``); across filesystems it copies to
``<destination>/.<name>.partial.<transfer id>`` and delivers that. A target below ``WORKSPACE/exchange``, which a
client writes, is reached through its parent opened component by component without following a symlink, and is
never copied to. An occupied destination, a failed delivery or an error after some members were extracted runs
the reconciler's decision at once, which rolls the members back; a refusal before the bundle is built gives every
claimed job back. A delivered exchange root leaves the exchange index (decided from the bundle root's
``state.json`` before delivery).

**Adopt** takes a bundle into an ``adopt`` scratch, validates it (the trust boundary), rekeys an untrusted one,
deduplicates against the workspace and publishes the members bottom-up. A bundle already adopted is discarded
only when it is a duplicate delivery; an operator's bundle is refused back to its path, and a refused copy of a
hold (in ``transfers/incoming/``) is discarded. An untrusted bundle is first *isolated*: right after the take
it is copied, descriptor-anchored, to ``.partial/<name>`` in the scratch, and that copy replaces the taken
original, which a client holding open descriptors could still change. Every later step works on the copy.
A trusted bundle whose members found here are all ``away`` copies kept by a transfer to the bundle's source
workspace replaces them: they are claimed and discarded before any member is published.

**Away copies**: releasing a hold with ``keep`` publishes its members into ``away`` (inert copies recording
where they went and the state they left); :func:`recover` returns such a tree to those states.

**Crash-resume contract** of the reconcilers, registered with the kernel at import:

- ``eject`` and ``hold``: without ``eject.json`` no delivery was attempted, and every member in the bundle is
  submitted back to its recorded state and priority (roll back). With it, the delivery happened when the bundle
  has left the scratch (only its owner moves it) or the recorded target carries the recorded ``bundle.json``
  bytes (a cross-filesystem copy): a recorded exchange root leaves the index and the scratch is discarded (roll
  forward). Otherwise any partial copy is discarded and the members go back. The scratch is never empty while a
  delivery is in doubt, so the reconciler always runs. A destination that cannot be read keeps the scratch.
  Delivery is at least once: verify the destination before re-ejecting.
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
  deduplication and the exchange-name claim run again; each is idempotent. ``replaces.json``, written just before
  ``plan.json``, names the away copies a trusted adoption replaces: a replay claims and discards those of the
  members still in the bundle that are still here (a copy already gone was discarded), then publishes. A copy
  claimed by an adopter that died is back in ``away`` once recovery returned it.
- ``release-keep``: every member still in the released hold is published into ``away`` again, bottom-up (one
  already published is absent from the bundle), recording the destination workspace id given to the release
  (kept beside the hold in the scratch) when the hold records none; then the scratch is discarded.
"""

import dataclasses
import json
import logging
import os
import re
import stat
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Self, cast

from httk.workflow import _fs, _kernel, _store
from httk.workflow._bundles import (
    MAX_MANIFEST_BYTES,
    BundleError,
    BundleManifest,
    build_bundle,
    exchange_names,
    read_manifest,
    read_rekeyed,
    rekey_untrusted,
    rekeyed_id,
    validate_bundle,
)
from httk.workflow._job import JobDefinition
from httk.workflow._kernel import OWNED, JobRef, OwnedJob, Owner, OwnerLost
from httk.workflow._manager_scheduling import bound_workflows, effective_workflow
from httk.workflow._state import (
    TERMINAL_STATES,
    UNOWNED_STATES,
    Release,
    StateDoc,
    encode_state,
    read_state_unowned,
)
from httk.workflow._util import json_bytes, utc_now
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.models import (
    EMBEDDED_WORKFLOW_PREFIX,
    EXCHANGE_DIRECTORY,
    WORKSPACE_DIRECTORY,
    canonical_uuid,
    make_job_key,
    normalize_placement,
    parse_job_key,
    placement_text,
)

if TYPE_CHECKING:  # pragma: no cover
    from httk.workflow.workspace import Workspace

__all__ = [
    "RESERVED_NAMES",
    "AdoptReport",
    "Busy",
    "EjectReport",
    "Hold",
    "adopt",
    "away_root",
    "destination_problem",
    "eject",
    "held",
    "hold",
    "reconcile_adopt",
    "reconcile_eject",
    "recover",
    "release_hold",
    "tree_members",
]

_LOGGER = logging.getLogger(__name__)

#: The states a tree member may be ejected from; the root may be in any unowned state.
_MEMBER_STATES = frozenset({*TERMINAL_STATES, "paused"})
_TRANSFER_ID = re.compile(r"[0-9a-f]{32}")
_BUNDLE = "bundle"
#: The delivery record of an ``eject`` or ``hold`` scratch, written before the delivery is attempted.
_DELIVERY = "eject.json"
#: The scratch purpose of a hold; an eject's is ``eject``.
_HOLD = "hold"
#: The scratch purposes of adoption: the trust travels in the name, atomically with the take.
_ADOPT = {False: "adopt", True: "adopt-untrusted"}
_SOURCE = "source.json"
_PLAN = "plan.json"
#: The away copies a trusted adoption replaces, ``{job_id: placement}``, written before ``plan.json``.
_REPLACES = "replaces.json"
_PARTIAL = ".partial"
#: Names an ``adopt`` scratch holds besides the bundle; a bundle may not be named like one of them.
RESERVED_NAMES = frozenset({_SOURCE, _PLAN, "rekey.json", _REPLACES, _PARTIAL})
_RECORD_TEMPORARY = re.compile(r"\.(source|plan|rekey|replaces)\.json\.[a-z2-7]{16}\.tmp")
#: The scratch purpose of a release that keeps the hold's members as away copies; a plain release's is
#: ``release`` (discardable).
_RELEASE_KEEP = "release-keep"
#: The file of a ``release-keep`` scratch that holds the destination workspace id given to the release.
_DESTINATION_ID_FILE = "destination-workspace-id"
_RECORD_LIMIT = 1 << 16
_DIGEST = re.compile(r"[0-9a-f]{64}")
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
    :param workflows: The workflows the members need (each one's effective workflow and the closure of its
        calls) as ``(id, store directory, tree_sha256)``, the last two ``None`` when the holding workspace has
        no installation of it; ``None`` when not reported (an older holding workspace).
    """

    transfer_id: str
    path: str
    members: tuple[str, ...]
    destination_locator: str | None
    destination_workspace_id: str | None
    created_at: str
    workflows: tuple[tuple[str, str | None, str | None], ...] | None = None

    @classmethod
    def from_manifest(
        cls,
        path: Path,
        manifest: BundleManifest,
        workflows: tuple[tuple[str, str | None, str | None], ...] | None = None,
    ) -> Self:
        """Describe the held bundle at *path* from its manifest.

        :param path: The bundle directory.
        :param manifest: Its manifest.
        :param workflows: The workflows its members need (see the class).
        :return: The hold.
        """

        return cls(
            manifest.transfer_id,
            str(path),
            tuple(member.job_key for member in manifest.members),
            manifest.destination_locator,
            manifest.destination_workspace_id,
            manifest.created_at,
            workflows,
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
            **(
                {}
                if self.workflows is None
                else {
                    "workflows": [
                        {"id": workflow_id, "directory": directory, "tree_sha256": digest}
                        for workflow_id, directory, digest in self.workflows
                    ]
                }
            ),
        }

    @classmethod
    def from_mapping(cls, value: object) -> Self:
        """Parse a hold another workspace reported (:meth:`as_mapping`); derived members are ignored.

        :param value: The decoded mapping.
        :return: The hold.
        :raises ValueError: For a malformed hold: a bad transfer id, a path not ending in it, no members, or a
            malformed destination, workspace id or timestamp. A malformed ``workflows`` member reads as ``None``.
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
        workflows = value.get("workflows")
        if workflows is not None:
            try:
                if not isinstance(workflows, list):
                    raise ValueError(f"not a list: {workflows!r}")
                workflows = tuple(_workflow_entry(entry) for entry in workflows)
            except ValueError as exc:
                # Not usable (a newer or broken report): the hold still moves, its workflows are not shipped.
                _LOGGER.debug("ignoring the workflows of hold %s: %s", transfer_id, exc)
                workflows = None
        return cls(transfer_id, path, tuple(members), locator, workspace_id, created_at, workflows)


def _workflow_entry(entry: object) -> tuple[str, str | None, str | None]:
    """Check one ``workflows`` entry of a reported hold: a store directory is one plain store name."""

    if not isinstance(entry, dict):
        raise ValueError(f"a malformed workflow of a hold: {entry!r}")
    workflow_id, directory, digest = entry.get("id"), entry.get("directory"), entry.get("tree_sha256")
    store_name = isinstance(directory, str) and (
        _store._INSTALLATION.fullmatch(directory) or _store._REPLACED.fullmatch(directory)
    )
    if isinstance(workflow_id, str) and workflow_id:
        if directory is None and digest is None:
            return workflow_id, None, None
        if store_name and isinstance(digest, str) and _DIGEST.fullmatch(digest):
            return workflow_id, cast(str, directory), digest
    raise ValueError(f"a malformed workflow of a hold: {entry!r}")


# -- eject ----------------------------------------------------------------------------------------------------------


def tree_members(
    workspace: _kernel.KernelWorkspace,
    root_id: str,
    root_doc: StateDoc | None,
    *,
    states: frozenset[str] = _MEMBER_STATES,
) -> list[JobRef]:
    """List the descendants an eject of the root with *tree* moves, top-down (breadth first).

    The children a parent published are recorded in its ``state.json`` and confirmed from the child's side: its
    ``job.json`` names this parent and it is not detached. A recorded child is looked up across the visibility
    deadline before it counts as gone (and so as no member).

    :param workspace: The workspace.
    :param root_id: The root's job UUID.
    :param root_doc: The root's ``state.json``, or ``None``.
    :param states: The states a member may be in: terminal or paused by default (an ``away`` or owned member
        blocks); :func:`recover` passes every unowned state.
    :return: The members besides the root.
    :raises Busy: For a member in none of *states*, or one whose ``job.json`` or ``state.json`` is unreadable.
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
            if ref.state not in states:
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


def _claim_tree(
    workspace: "Workspace", owner: Owner, root: OwnedJob, *, states: frozenset[str] = _MEMBER_STATES
) -> list[OwnedJob]:
    refs = tree_members(workspace, root.job_id, root.read_state(), states=states)
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


def _anchored_target(workspace: _kernel.KernelWorkspace, target: Path, *, create: bool) -> tuple[int | None, _fs.Loc]:
    """The location of a delivery target: anchored at its parent's descriptor when it lies below the exchange.

    ``WORKSPACE/exchange`` is client-writable, so every directory from the workspace root down to the parent is
    opened without following a symlink (and created when *create*); a target elsewhere is a plain location in
    a directory only trusted parties write.

    :param workspace: The ejecting workspace.
    :param target: The absolute target path.
    :param create: Create the missing exchange directories on the way.
    :return: The parent's descriptor (``None`` for a plain location), which the caller closes, and the location.
    :raises httk.workflow._fs.UnsafePath: When a directory on the way below the root is a symlink or not one.
    :raises FileNotFoundError: Without *create*, when a directory on the way is missing.
    """

    if not target.is_relative_to(workspace.root / EXCHANGE_DIRECTORY):
        return None, _fs.loc(target)
    relative = PurePosixPath(target.relative_to(workspace.root))
    descriptor = _fs.open_dir_under(
        workspace.root, relative.parent, create=create, mode=0o755, durable=workspace.durable
    )
    return descriptor, _fs.anchored(descriptor, relative.name)


def destination_problem(destination: Path, workspace: _kernel.KernelWorkspace | None = None) -> str | None:
    """Report why *destination* cannot receive an eject, before anything moves.

    It must be absolute without ``..``, and either a real directory or a missing name whose parent is a real
    directory; a symlink is neither. Below *workspace*'s exchange directory, every directory from the workspace
    root down is opened (and created) without following a symlink, as the delivery does; a destination that
    reaches the exchange only through a symlink is refused, since anchoring is decided on the lexical path.

    :param destination: The eject destination.
    :param workspace: The ejecting workspace.
    :return: The reason, or ``None`` when it is usable.
    """

    if not destination.is_absolute() or ".." in destination.parts:
        return f"the eject destination must be absolute and without '..': {destination}"
    exchange = None if workspace is None else workspace.root / EXCHANGE_DIRECTORY
    if workspace is not None and exchange is not None and destination.is_relative_to(exchange):
        try:
            descriptor, _target = _anchored_target(workspace, destination / "probe", create=True)
        except (OSError, _fs.UnsafePath, ValueError) as exc:
            return f"the eject destination {destination} is unusable: {exc}"
        if descriptor is not None:
            os.close(descriptor)
        return None
    if exchange is not None and Path(os.path.realpath(destination)).is_relative_to(exchange):
        # Anchoring is decided on the lexical path: one that reaches the exchange through a symlink is refused.
        return f"the eject destination {destination} leads into {exchange} through a symlink; name it directly"
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
    descriptor, destination = _anchored_target(workspace, target, create=True)

    def deliver(source: Path) -> bool:
        outcome = _fs.deliver(_fs.loc(source), destination, token_name="bundle.json", token=token, durable=durable)
        return outcome is _fs.Delivered.DONE

    try:
        return deliver(bundle)
    except _fs.CrossDevice:
        if descriptor is not None:
            # Enabling the exchange proved it renames to and from tmp/: nothing is ever copied into it.
            raise
        partial = target.with_name(f".{target.name}.partial.{transfer_id}")
        _fs.copy_tree(bundle, partial, durable=durable)
        if deliver(partial):
            return True
        _fs.discard(_fs.loc(partial), trash_dir=partial.parent, durable=durable)
        return False
    finally:
        if descriptor is not None:
            os.close(descriptor)


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


type _Exchange = tuple[str, str, str | None]


def _exchange_root(workspace: _kernel.KernelWorkspace, bundle: Path, manifest: BundleManifest) -> _Exchange | None:
    """The exchange name, job id and index entry's adoption nonce of a bundle whose root is an exchange job, from
    the root's ``state.json`` and the index."""

    root = manifest.members[0]
    doc, _damaged = read_state_unowned(manifest.member_dir(bundle, root) / "state.json")
    if doc is None or doc.origin != "exchange" or doc.exchange_name is None:
        return None
    indexed = _kernel.exchange_index(workspace, doc.exchange_name)
    nonce = indexed[2] if indexed is not None and indexed[0] == root.job_id else None
    return doc.exchange_name, root.job_id, nonce


def _leave_index(owner: Owner, exchange: _Exchange | None) -> None:
    # A delivered exchange root (returned, ejected or held) leaves the index; a child's entry is its root's. The
    # nonce keeps a resubmission's entry (same rekeyed id, another adoption) from going with the earlier one.
    if exchange is not None:
        _kernel.drop_exchange_index(owner, *exchange)


@dataclass(frozen=True)
class _Delivery:
    """``eject.json``: what deciding a delivery needs once the bundle may have left the scratch."""

    target: Path
    token: bytes
    transfer_id: str
    exchange: _Exchange | None

    def encode(self) -> bytes:
        exchange = None if self.exchange is None else list(self.exchange)
        fields = {
            "target": str(self.target),
            "token": self.token.decode("utf-8"),
            "transfer_id": self.transfer_id,
            "exchange": exchange,
        }
        return json.dumps(fields).encode()

    @classmethod
    def read(cls, scratch: Path) -> Self | None:
        """Read *scratch*'s record.

        :param scratch: The ``eject`` or ``hold`` scratch.
        :return: The record, or ``None`` when it has none.
        :raises ValueError: For a record that is not a regular file of the expected shape (a torn write).
        """

        try:
            data = _fs.read_bounded(_fs.loc(scratch / _DELIVERY), _RECORD_LIMIT)
            if data is None:
                return None
            value = json.loads(data)
        except (_fs.UnsafePath, _fs.TooLarge) as exc:
            raise ValueError(str(exc)) from exc
        fields = value if isinstance(value, dict) else {}
        target, token, transfer_id = fields.get("target"), fields.get("token"), fields.get("transfer_id")
        exchange = fields.get("exchange", ())
        if not (isinstance(target, str) and isinstance(token, str) and isinstance(transfer_id, str)):
            raise ValueError(f"malformed {_DELIVERY}")
        if exchange is None:
            return cls(Path(target), token.encode("utf-8"), transfer_id, None)
        match exchange:
            case [str(name), str(job_id), str() | None as nonce]:
                return cls(Path(target), token.encode("utf-8"), transfer_id, (name, job_id, nonce))
            case _:
                raise ValueError(f"malformed {_DELIVERY}")


def _carried(workspace: _kernel.KernelWorkspace, target: Path, token: bytes) -> bool:
    """Whether *target* carries *token*, never following a symlink in a client-writable directory on the way."""

    try:
        descriptor, destination = _anchored_target(workspace, target, create=False)
    except (FileNotFoundError, _fs.UnsafePath):
        return False  # an exchange directory on the way is gone or replaced: nothing is delivered there
    try:
        return _fs.carries(destination, "bundle.json", token)
    finally:
        if descriptor is not None:
            os.close(descriptor)


def delivered_exchange_job(scratch: Path) -> str | None:
    """The exchange job an ``eject`` or ``hold`` scratch is delivering, from its ``eject.json``.

    :param scratch: The scratch.
    :return: The recorded exchange root's job id, or ``None`` when the scratch records none.
    :raises ValueError: For a torn or malformed record.
    """

    delivery = _Delivery.read(scratch)
    return None if delivery is None or delivery.exchange is None else delivery.exchange[1]


def _settle(owner: Owner, scratch: Path) -> bool | None:
    """Resolve an ``eject`` or ``hold`` scratch: ``True`` when its bundle was delivered, ``False`` when every member
    still in it went back (or none was taken), ``None`` when the destination cannot be read or a torn ``eject.json``
    leaves an empty bundle undecided (the scratch stays); :class:`OwnerLost` when a recoverer took the scratch."""

    bundle = scratch / _BUNDLE
    try:
        delivery, torn = _Delivery.read(scratch), False
    except ValueError as exc:
        # A torn record decides nothing: the members still in the bundle go back.
        _LOGGER.warning("ignoring the unreadable %s in %s: %s", _DELIVERY, scratch, exc)
        delivery, torn = None, True
    if delivery is not None:
        target = delivery.target
        try:
            carried = _carried(owner.workspace, target, delivery.token)
            if carried or not _fs.exists(_fs.loc(bundle)):
                if not carried:
                    # Only this owner moves bundle/ out of its scratch, but a recoverer takes the whole scratch:
                    # looked at again after bundle/ was seen gone, a record still here means it was delivered.
                    if not _fs.exists(_fs.loc(scratch / _DELIVERY)):
                        raise OwnerLost(f"the scratch {scratch} was taken by a recoverer")
                    _LOGGER.warning("the eject %s left %s, which does not carry it now", delivery.transfer_id, target)
                _leave_index(owner, delivery.exchange)
                return True
            partial = target.with_name(f".{target.name}.partial.{delivery.transfer_id}")
            plain = not target.is_relative_to(owner.workspace.root / EXCHANGE_DIRECTORY)
            if plain and _fs.exists(_fs.loc(partial)):
                # An undelivered cross-filesystem copy is ours alone; the members go back, so it goes too.
                _fs.discard(_fs.loc(partial), trash_dir=partial.parent, durable=owner.workspace.durable)
        except OSError as exc:
            _LOGGER.warning("keeping %s: cannot check its destination: %s", scratch, exc)
            return None
    read = read_manifest(bundle / "bundle.json")
    if read is None:
        # build_bundle writes bundle.json before any member moves: nothing to return. After a torn record the
        # bundle may have been delivered: the scratch stays for the operator.
        return None if torn else False
    returned = _roll_back(owner, bundle, read[1])
    _LOGGER.warning("rolled back the eject %s: %d jobs returned", read[1].transfer_id, len(returned))
    return False


def _exchange_inbox(destination: Path) -> bool:
    """Whether *destination* is an exchange's ``inbox``, whose untrusted adoption rebuilds every ``state.json``."""

    return destination.name == "inbox" and (destination.parent / "exchange.json").is_file()


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
    owner_key: str | None = None,
) -> tuple[EjectReport, BundleManifest]:
    members: list[OwnedJob] = []
    scratch: Path | None = None
    try:
        if root.from_state == "away":
            raise ValueError(f"{root.job_key} is away; recover it first")
        if tree:
            members = _claim_tree(workspace, owner, root)
        elif children := tree_members(workspace, root.job_id, root.read_state()):
            # Ejecting the root alone would orphan the children present here: it is refused.
            raise Busy(children[0].job_key, "it is a child of the job; eject the whole tree")
        if _exchange_inbox(destination) and (
            switched := next(
                (job for job in (root, *members) if (doc := job.read_state()) is not None and doc.workflow_override),
                None,
            )
        ):
            raise ValueError(
                f"{switched.job_key} runs another workflow than its job.json names (set_workflow), which the "
                "exchange's adoption would drop with the rest of state.json; run it here, or submit a new job of "
                "that workflow to the exchange"
            )
        if _exchange_inbox(destination) and (
            unlocked := next(
                (job for job in (root, *members) if (doc := job.read_state()) is not None and doc.unlocked),
                None,
            )
        ):
            raise ValueError(
                f"{unlocked.job_key} is unlocked (debug mode), which the exchange's adoption would drop with the rest "
                "of state.json; run it here, or submit a new job to the exchange"
            )
        scratch = owner.scratch(_HOLD if hold else "eject")
        bundle = build_bundle(
            owner,
            [root, *members],
            source_workspace_id=workspace.workspace_id,
            destination_workspace_id=destination_workspace_id,
            destination_locator=locator,
            owner_key=owner_key,
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
    # Read in the scratch, before the bundle reaches a destination others may write.
    exchange = _exchange_root(workspace, bundle, manifest)
    name = root.job_key
    if exchange is not None and rekeyed_id(workspace.workspace_id, exchange[0]) == root.job_id:
        # A job this workspace adopted from its exchange goes back under the client's name.
        name = make_job_key(exchange[0], parse_job_key(root.job_key)[0])
    target = destination / (manifest.transfer_id if hold else name)
    report = EjectReport(target, tuple(member.job_key for member in manifest.members), manifest.transfer_id)
    error: Exception | None = None
    try:
        # Recorded first: a crash after the delivery leaves a scratch whose reconciler can still decide it.
        delivery = _Delivery(target, token, manifest.transfer_id, exchange)
        _fs.write_file(_fs.loc(scratch / _DELIVERY), delivery.encode(), durable=workspace.durable)
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


def eject(
    workspace: "Workspace",
    owner: Owner,
    root: OwnedJob,
    *,
    destination: Path,
    tree: bool,
    owner_key: str | None = None,
) -> EjectReport:
    """Move a claimed job, and with *tree* its descendants, out of the workspace into ``<destination>/<name>``.

    The name is the client's job key (``<tag>--<exchange name>``) for an exchange job this workspace adopted,
    otherwise the root's job key.

    Tree members are the root's non-detached descendants (:func:`tree_members`); each must be terminal or paused,
    and they are claimed in sorted job-UUID order. Without *tree*, a root that has such descendants present is
    refused. *root*'s handle is retired whatever happens: a refusal, an occupied destination or a failed delivery
    returns every job, the root included, to the state and priority it was taken from.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The claimed, quiescent root, in any unowned state.
    :param destination: An absolute directory, or a missing name in one (:func:`destination_problem`); below
        the exchange, every directory on the way is opened without following a symlink.
    :param tree: Eject the root's descendants too.
    :param owner_key: The optional key that may control the adopted root through the workspace daemon.
    :return: Where the bundle went.
    :raises Busy: When a member cannot be claimed, or without *tree* when the root has descendants.
    :raises ValueError: For an unusable destination, or a root claimed from ``away``.
    :raises httk.workflow.errors.WorkflowError: When the destination is occupied or the delivery failed, or
        :class:`~httk.workflow._bundles.BundleError` when the jobs cannot form a bundle.
    """

    destination = Path(destination)
    if (problem := destination_problem(destination, workspace)) is not None:
        root.give_back()
        raise ValueError(problem)
    report, _manifest = _eject(
        workspace,
        owner,
        root,
        destination=destination,
        tree=tree,
        hold=False,
        locator=str(destination),
        owner_key=owner_key,
    )
    return report


def reconcile_eject(owner: Owner, scratch: Path) -> bool:
    """The ``eject`` and ``hold`` scratch reconciler: roll forward when the bundle was delivered, otherwise roll back.

    :param owner: The owner the scratch is named after.
    :param scratch: ``tmp/<owner-id>.eject.<token>/`` or ``tmp/<owner-id>.hold.<token>/``.
    :return: ``True`` when resolved (the kernel then discards the scratch); ``False`` when the destination
        cannot be read, which keeps the scratch.
    :raises httk.workflow._kernel.OwnerLost: When a recoverer took the scratch meanwhile.
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
    elif record.source is not None and Path(record.source).resolve().is_relative_to(
        (owner.workspace.control / "transfers" / "incoming").resolve()
    ):
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
    machinery placed: a hold or landing in some workspace's ``transfers/outgoing`` or ``transfers/incoming``, a
    landing anywhere below this workspace's own ``transfers/incoming`` (a batch's ``incoming/<token>/<id>``), or
    in this workspace's own scratch (a pulled landing). Any other bundle is an operator's, never destroyed.
    """

    if record.untrusted or record.copied:
        return True
    if record.source is None:
        return False
    source = Path(record.source)
    transfers = {(WORKSPACE_DIRECTORY, "transfers", "outgoing"), (WORKSPACE_DIRECTORY, "transfers", "incoming")}
    return (
        source.parent.parts[-3:] in transfers
        or source.resolve().parent.is_relative_to((workspace.control / "transfers" / "incoming").resolve())
        or source.is_relative_to(workspace.control / "tmp")
    )


def settle_absent(workspace: "Workspace", bundles: Sequence[Path]) -> frozenset[str]:
    """Settle once which members of trusted *bundles*, and which of their roots' parents, are not in *workspace*.

    One wait of the visibility deadline for a batch of adoptions, instead of one for each: :func:`adopt` given
    the result settles none of these ids again, while a fresh listing still sees every job published since (by
    an adoption of the batch, say). A bundle that cannot be read contributes nothing; its adoption reports it.

    :param workspace: The workspace the bundles are adopted into.
    :param bundles: The bundle directories, before any of them is adopted.
    :return: The job UUIDs absent across the visibility deadline.
    """

    wanted: dict[str, PurePosixPath] = {}
    for bundle in bundles:
        try:
            read = read_manifest(bundle / "bundle.json")
            if read is None:
                continue
            manifest = read[1]
            wanted.update((member.job_id, member.placement) for member in manifest.members)
            parent = JobDefinition.from_path(manifest.member_dir(bundle, manifest.members[0]) / "job.json").parent
            if parent is not None:
                parent_id = canonical_uuid(str(parent.get("job_id")), "parent job_id")
                wanted[parent_id] = normalize_placement(str(parent.get("placement")))
        except (BundleError, FormatError, OSError, ValueError):
            continue
    found = _kernel.locate_many(workspace, list(wanted), placements=list(wanted.values()), settle=True)
    return frozenset(wanted.keys() - found.keys())


def _presence(
    workspace: "Workspace",
    bundle: Path,
    manifest: BundleManifest,
    *,
    untrusted: bool,
    absent: frozenset[str] | None = None,
) -> tuple[str | None, dict[str, JobRef]]:
    """``"all"`` when every member is here already, a refusal reason, or ``None`` to publish, with the away copies
    the publication replaces (by job UUID).

    A trusted bundle replaces away copies of its members when every member found here is one, kept by a
    transfer to the workspace the bundle comes from; any other hit keeps the rules of :func:`adopt`. Ids in
    *absent* (from :func:`settle_absent`) are looked up afresh but not settled again.
    """

    # ponytail: check-then-act; two operators adopting copies of one trusted bundle at once can duplicate it
    # (plan §13.2: fsck reports duplicate UUIDs). Exchange bundles are serialized by the exchange-name claim.
    ids = [member.job_id for member in manifest.members]
    placements = [member.placement for member in manifest.members]
    found = _kernel.locate_many(workspace, ids, placements=placements, settle=absent is None)
    if absent is not None and (unsettled := [job_id for job_id in ids if job_id not in found and job_id not in absent]):
        found |= _kernel.locate_many(workspace, unsettled, placements=placements, settle=True)
    copies = {job_id: ref for job_id, ref in found.items() if ref.state == "away" and not untrusted}
    if found and len(copies) == len(found):
        source = manifest.source_workspace_id
        for ref in copies.values():
            doc, _damaged = read_state_unowned(ref.path / "state.json")
            away = None if doc is None else doc.away
            if away is None:
                return f"{ref.job_key} is away without a record of where it went; delete the copy first", {}
            if away["workspace_id"] != source:
                reason = f"{ref.job_key} is away at {away['locator']}, but this bundle comes from workspace {source}"
                return f"{reason}; recover or delete the copy first", {}
        found = {}
    else:
        copies = {}
        if not untrusted and (late := _late_return(found.values(), manifest.source_workspace_id)) is not None:
            return late, {}
    if len(found) == len(ids):
        return "all", {}
    if found:
        present = ", ".join(sorted(ref.job_key for ref in found.values()))
        return f"{len(found)} of its {len(ids)} jobs are already in this workspace: {present}", {}
    if untrusted:
        return None, {}
    parent = JobDefinition.from_path(manifest.member_dir(bundle, manifest.members[0]) / "job.json").parent
    if parent is not None:
        hint = PurePosixPath(str(parent.get("placement")))
        parent_id = str(parent.get("job_id"))
        settle = absent is None or parent_id not in absent
        parent_ref = _kernel.locate(workspace, parent_id, placement_hint=hint, settle=settle)
        if parent_ref is not None:
            reason = (
                f"the root's parent {parent_ref.job_key} is in this workspace; adopt the parent's whole tree instead"
            )
            return reason, {}
    return None, copies


def _late_return(found: Iterable[JobRef], source: str) -> str | None:
    """The refusal of a bundle one of whose present (unowned) members was recovered here from a transfer to
    *source*: the bundle is that job's late return, never an already-adopted duplicate to discard."""

    for ref in found:
        if ref.state == OWNED:
            continue  # being worked on: today's rules
        doc, _damaged = read_state_unowned(ref.path / "state.json")
        if doc is not None and doc.recovered is not None and doc.recovered["workspace_id"] == source:
            return (
                f"{ref.job_key} was recovered here after it was pushed to workspace {source}; this bundle is its "
                "late return. Delete one of the two, or adopt the bundle into another workspace"
            )
    return None


def _read_replaces(scratch: Path) -> dict[str, str]:
    """The ``replaces.json`` of an ``adopt`` scratch; empty when it has none."""

    data = _fs.read_bounded(_fs.loc(scratch / _REPLACES), MAX_MANIFEST_BYTES)
    return {} if data is None else {str(key): str(value) for key, value in json.loads(data).items()}


def _discard_copies(workspace: "Workspace", owner: Owner, replaces: dict[str, str]) -> str | None:
    """Claim every away copy an adoption replaces, then discard each (the ``delete`` primitive), all before any
    member is published.

    A copy no longer found was discarded by an earlier run. Nothing is discarded unless every copy found is
    claimed from ``away``: otherwise the claimed ones go back and the reason is returned.
    """

    found = _kernel.locate_many(
        workspace, list(replaces), placements=[PurePosixPath(item) for item in replaces.values()], settle=True
    )
    claimed: list[OwnedJob] = []
    problem = None
    for job_id in sorted(found):
        ref = found[job_id]
        if ref.state != "away":
            problem = f"{ref.job_key} is {ref.state} here, no longer away"
            break
        if (job := _kernel.claim(workspace, owner, ref)) is None:
            problem = f"{ref.job_key} was taken by another actor"
            break
        claimed.append(job)
    try:
        for job in claimed:
            if problem is not None:
                job.give_back()
            else:
                job.discard()
    except Exception:
        for job in claimed:
            if owner.holds(job.ref):
                job.give_back()
        raise
    return problem


def _finish(
    workspace: "Workspace",
    owner: Owner,
    scratch: Path,
    bundle: Path,
    record: _Record,
    absent: frozenset[str] | None = None,
) -> AdoptReport | str:
    """Steps 2-6 of adoption on a taken bundle: the report, or the message of a refusal."""

    durable = workspace.durable
    read = read_manifest(scratch / _PLAN)
    if read is None:
        try:
            manifest = _validated(workspace, scratch, bundle, untrusted=record.untrusted)
            presence, copies = _presence(workspace, bundle, manifest, untrusted=record.untrusted, absent=absent)
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
            claim = _kernel.claim_exchange_name(
                workspace,
                owner,
                name,
                root.job_id,
                root.placement,
                nonce=record.nonce,
                owner_key=manifest.owner_key,
            )
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
        if copies:
            # Before the plan: a replay that finds the plan finds the copies it replaces too.
            replaces = {job_id: placement_text(ref.placement or PurePosixPath()) for job_id, ref in copies.items()}
            _fs.write_file(_fs.loc(scratch / _REPLACES), json_bytes(replaces), durable=durable)
        _fs.write_file(_fs.loc(scratch / _PLAN), manifest.to_json(), durable=durable)
    else:
        manifest = read[1]
    if not record.untrusted and (replaces := _read_replaces(scratch)):
        # A member already published (absent from the bundle) is here under its id: its copy went before it.
        unpublished = {
            member.job_id for member in manifest.members if _fs.exists(_fs.loc(manifest.member_dir(bundle, member)))
        }
        pending = {job_id: placement for job_id, placement in replaces.items() if job_id in unpublished}
        if pending and (problem := _discard_copies(workspace, owner, pending)) is not None:
            return _refuse(owner, scratch, bundle, record, problem)
    names = exchange_names(scratch) if record.untrusted else {}
    source_id = manifest.source_workspace_id
    published: list[JobRef] = []
    workflows: set[str] = set()
    bound: set[str] = set()
    for member in reversed(manifest.members):
        directory = manifest.member_dir(bundle, member)
        if not _fs.exists(_fs.loc(directory)):
            continue
        # A trusted bundle keeps state.json, and with it a set_workflow override: the job runs that workflow.
        kept = None if record.untrusted else read_state_unowned(directory / "state.json")[0]
        job = JobDefinition.from_path(directory / "job.json")
        workflows.add(effective_workflow(job, kept)[0])
        bound.update(bound_workflows(job))
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
        elif kept is not None and kept.recovered is not None and kept.recovered["workspace_id"] == source_id:
            # This arrival from the workspace it was recovered from supersedes the recovery: a duplicate delivery of
            # it is already adopted, never a late return.
            kept = kept.updated(recovered=None)
            _fs.write_file(_fs.loc(directory / "state.json"), encode_state(kept), durable=durable)
        published.append(_kernel.submit(workspace, owner, directory, state=member.state, priority=member.priority))
    owner.discard_scratch(scratch)
    # An embedded workflow travels in its job's workflow/; its fixed calls are among the bound workflows.
    missing = {
        workflow
        for workflow in workflows
        if not workflow.startswith(EMBEDDED_WORKFLOW_PREFIX) and _installed(workspace, workflow) is None
    }
    # Reported only, never installed: an untrusted binding blocks its job until an operator installs.
    missing.update(
        reference for reference, found in _store.resolve_calls(workspace, sorted(bound)).items() if not found
    )
    return AdoptReport(tuple(published), False, tuple(sorted(missing)), record.copied)


def _installed(workspace: "Workspace", workflow_id: str) -> object:
    return _store.lookup(workspace, workflow_id)


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
    workspace: "Workspace",
    owner: Owner,
    source: Path | _fs.Loc,
    *,
    untrusted: bool,
    refused_to: Path | None = None,
    absent: frozenset[str] | None = None,
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
    :param absent: For a trusted bundle of a batch: :func:`settle_absent` of the batch, taken before any of it
        was adopted, so that this adoption does not wait out the visibility deadline again.
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
    result = _finish(workspace, owner, scratch, scratch / name, record, None if untrusted else absent)
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
    workflows: bool = False,
) -> Hold:
    """Eject into this workspace's own ``transfers/outgoing/<transfer-id>/``: the held bundle of a transfer.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The claimed, quiescent root.
    :param tree: Hold the root's descendants too.
    :param destination_locator: The transfer's destination, recorded in ``bundle.json`` for resumption.
    :param destination_workspace_id: The destination workspace's id, recorded in ``bundle.json``, so that a
        resumption adopts only into that workspace.
    :param workflows: Also report the workflows its members need (:attr:`Hold.workflows`).
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
    needed = _needed_workflows(workspace, report.destination, manifest) if workflows else None
    return Hold.from_manifest(report.destination, manifest, needed)


def _needed_workflows(
    workspace: _kernel.KernelWorkspace, bundle: Path, manifest: BundleManifest
) -> tuple[tuple[str, str | None, str | None], ...]:
    """The workflows a held bundle's members need, as :attr:`Hold.workflows` reports them, by id."""

    roots: set[str] = set()
    for member in manifest.members:
        directory = manifest.member_dir(bundle, member)
        try:
            job = JobDefinition.from_path(directory / "job.json")
        except ValueError as exc:
            _LOGGER.debug("no workflow of the held job %s: %s", member.job_key, exc)
            continue
        if job.embedded is None:  # an embedded workflow travels in the job's workflow/, never in the store
            roots.add(effective_workflow(job, read_state_unowned(directory / "state.json")[0])[0])
        roots.update(bound_workflows(job))
    needed: dict[str, tuple[str, str | None, str | None]] = {}
    for reference, found in _store.resolve_calls(workspace, sorted(roots)).items():
        if found is None:
            needed.setdefault(reference, (reference, None, None))
        else:
            digest = found.record.get("tree_sha256")
            needed[found.id] = (found.id, found.directory.name, digest if isinstance(digest, str) else None)
    # A record without a digest is reported as not installed: nothing comparable could be shipped.
    return tuple(entry if entry[2] is not None else (entry[0], None, None) for _, entry in sorted(needed.items()))


def held(workspace: "Workspace", *, workflows: bool = False) -> list[Hold]:
    """List the held bundles.

    :param workspace: The workspace.
    :param workflows: Also report the workflows each hold's members need (:attr:`Hold.workflows`), read from
        their ``job.json`` and ``state.json`` and resolved in this workspace's store.
    :return: The holds, by transfer id.
    """

    outgoing = _outgoing(workspace)
    holds = []
    for name in _kernel.list_names(outgoing):
        if _TRANSFER_ID.fullmatch(name) and (read := read_manifest(outgoing / name / "bundle.json")) is not None:
            bundle = outgoing / name
            needed = _needed_workflows(workspace, bundle, read[1]) if workflows else None
            holds.append(Hold.from_manifest(bundle, read[1], needed))
    return holds


def release_hold(
    workspace: "Workspace",
    owner: Owner,
    transfer_id: str,
    *,
    keep: bool = False,
    destination_workspace_id: str | None = None,
) -> bool:
    """Discard a held bundle once its destination adopted it, or with *keep* keep its members as away copies.

    A kept member is published bottom-up into ``away`` at the priority it was held with, its ``state.json``
    recording ``away``: the destination (workspace id and locator), the transfer id, and the state and priority
    it left (see :func:`recover`). The ``release-keep`` scratch reconciler finishes an interrupted release. A hold
    that records no destination workspace id (:attr:`Hold.destination_workspace_id`) keeps its copies for
    *destination_workspace_id* (the id the adopting workspace reported); without either it keeps nothing, as
    without *keep*, and a warning says so.

    :param workspace: The workspace.
    :param owner: The releasing owner.
    :param transfer_id: The hold's transfer id.
    :param keep: Keep the members as ``away`` copies instead of discarding them.
    :param destination_workspace_id: With *keep*, the destination workspace's canonical id for a hold that
        records none.
    :return: ``False`` when the hold is gone (another actor released or adopted it).
    :raises ValueError: For a malformed transfer id or destination workspace id.
    """

    if not _TRANSFER_ID.fullmatch(transfer_id):
        raise ValueError(f"not a transfer id: {transfer_id!r}")
    if destination_workspace_id is not None:
        destination_workspace_id = canonical_uuid(destination_workspace_id, "destination_workspace_id")
    purpose = _RELEASE_KEEP if keep else "release"
    scratch = _kernel.take(workspace, owner, _fs.loc(_outgoing(workspace) / transfer_id), purpose)
    if scratch is None:
        return False
    if keep:
        if destination_workspace_id is not None:
            # Beside the hold, for the reconciler that finishes an interrupted release (a death between the take
            # and this write leaves only the hold's own record).
            _fs.write_file(
                _fs.loc(scratch / _DESTINATION_ID_FILE),
                destination_workspace_id.encode("ascii"),
                durable=workspace.durable,
            )
        _keep_away(owner, scratch / transfer_id, destination_workspace_id)
    owner.discard_scratch(scratch)
    return True


def _keep_away(owner: Owner, bundle: Path, destination_workspace_id: str | None = None) -> None:
    """Publish every member still in a released hold into ``away``, bottom-up; a published one is absent.

    The copies record the hold's destination workspace id, else *destination_workspace_id*.
    """

    read = read_manifest(bundle / "bundle.json")
    if read is None:
        return  # build_bundle writes bundle.json first: a hold without it never held a member
    manifest = read[1]
    workspace_id = manifest.destination_workspace_id or destination_workspace_id
    if workspace_id is None:
        # A copy must know which workspace's bundle replaces it, and recover which one's late return to refuse.
        _LOGGER.warning(
            "kept no away copies of transfer %s: its hold records no destination workspace id", manifest.transfer_id
        )
        return
    for member in reversed(manifest.members):
        directory = manifest.member_dir(bundle, member)
        if not _fs.exists(_fs.loc(directory)):
            continue
        # A held member's state.json was readable when it was claimed; should it not be now, the copy still stays.
        doc = read_state_unowned(directory / "state.json")[0] or StateDoc.empty(member.job_id)
        away = {
            "workspace_id": workspace_id,
            "locator": manifest.destination_locator or "?",
            "transfer_id": manifest.transfer_id,
            "state": member.state,
            "priority": member.priority,
            "at": utc_now(),
        }
        # release_to names away: a later claim from away must not see the hold's state as a pending release.
        doc = doc.updated(away=away, release_to={"state": "away", "priority": member.priority}).with_history(
            "away", **{"from": member.state, "transfer_id": manifest.transfer_id, "destination": away["locator"]}
        )
        _fs.write_file(_fs.loc(directory / "state.json"), encode_state(doc), durable=owner.workspace.durable)
        _kernel.submit(owner.workspace, owner, directory, state="away", priority=member.priority)


def reconcile_release_keep(owner: Owner, scratch: Path) -> bool:
    """The ``release-keep`` scratch reconciler: publish the members still in the released hold into ``away``.

    :param owner: The owner the scratch is named after.
    :param scratch: ``tmp/<owner-id>.release-keep.<token>/``.
    :return: ``True``: the scratch is then discarded.
    """

    destination_workspace_id = None
    try:
        recorded = (scratch / _DESTINATION_ID_FILE).read_text(encoding="ascii")
        destination_workspace_id = canonical_uuid(recorded.strip(), "destination_workspace_id")
    except FileNotFoundError:
        pass
    except (OSError, ValueError, WorkflowError) as exc:
        _LOGGER.warning("ignored the unreadable destination workspace id of %s: %s", scratch, exc)
    for name in _kernel.list_names(scratch):
        if _TRANSFER_ID.fullmatch(name):
            _keep_away(owner, scratch / name, destination_workspace_id)
    return True


# -- away copies ----------------------------------------------------------------------------------------------------


def away_root(workspace: _kernel.KernelWorkspace, ref: JobRef) -> JobRef:
    """Return the root of the away tree *ref* belongs to: its furthest ancestor that stayed behind from the same
    transfer (``away`` with the same ``away.transfer_id``), or *ref* itself.

    :param workspace: The workspace.
    :param ref: A job (unowned, or owned by the caller).
    :return: The root, as last observed; *ref* when it has no away record or its parent is not in its tree.
    """

    doc = read_state_unowned(ref.path / "state.json")[0]
    transfer = None if doc is None or doc.away is None else doc.away["transfer_id"]
    current, seen = ref, {ref.job_id}
    while transfer is not None:
        try:
            parent = JobDefinition.from_path(current.path / "job.json").parent
        except FormatError:
            break
        if parent is None or (parent_id := str(parent.get("job_id"))) in seen:
            break
        hint = PurePosixPath(str(parent.get("placement")))
        found = _kernel.locate(workspace, parent_id, placement_hint=hint)
        # A parent claimed from away (a request being served, say) is still in the tree.
        if found is None or "away" not in (found.state, found.from_state):
            break
        doc = read_state_unowned(found.path / "state.json")[0]
        if doc is None or doc.away is None or doc.away["transfer_id"] != transfer:
            break
        current = found
        seen.add(parent_id)
    return current


def recover(workspace: "Workspace", owner: Owner, root: OwnedJob) -> list[JobRef]:
    """Return a claimed away job and its tree to the states and priorities they left (``job recover``).

    The members are the root's descendants in any unowned state (:func:`tree_members`), claimed in sorted
    job-UUID order. They are released bottom-up: an away member to its ``away.state`` and ``away.priority``,
    ``away`` cleared and a ``recovered`` history event added; any other member (one recovered by an earlier,
    interrupted run) goes back unchanged, which applies a pending release. A root whose record is gone but whose
    release is pending (a recovery interrupted after the root's ``state.json`` was written) is finished so.
    Nothing checks whether the job still runs where it went. *root*'s handle is retired whatever happens: a
    refusal gives every claimed job back.

    :param workspace: The workspace.
    :param owner: The owner holding *root*.
    :param root: The root, claimed from ``away``.
    :return: The released jobs, bottom-up (the root last).
    :raises ValueError: When *root* was not claimed from ``away``, has no away record, or belongs to the away
        tree of an ancestor (which is to be recovered instead).
    :raises Busy: When a member cannot be claimed.
    """

    try:
        if root.from_state != "away":
            raise ValueError(f"{root.job_key} is {root.from_state}, not away")
        doc = root.read_state()
        # A pending release is an interrupted recovery (the record already cleared): it is finished.
        if (doc is None or doc.away is None) and root.pending_release() is None:
            raise ValueError(f"{root.job_key} has no away record of the state it left; delete it instead")
        if (top := away_root(workspace, root.ref)).job_id != root.job_id:
            raise ValueError(f"{root.job_key} is a member of the away tree of {top.job_key}; recover {top.job_key}")
        members = _claim_tree(workspace, owner, root, states=frozenset(UNOWNED_STATES))
    except Exception:
        root.give_back()
        raise
    jobs = [root, *members]
    released: list[JobRef] = []
    try:
        for job in reversed(jobs):
            doc = job.read_state()
            away = None if doc is None else doc.away
            if doc is None or away is None or job.from_state != "away":
                if job.from_state == "away" and job.pending_release() is None:
                    _LOGGER.warning("%s is away without an away record; it stays away", job.job_key)
                released.append(job.give_back())  # a pending release (an interrupted recovery) is applied
                continue
            state, priority = str(away["state"]), cast(int, away["priority"])
            recovered = {"workspace_id": away["workspace_id"], "transfer_id": away["transfer_id"], "at": utc_now()}
            doc = doc.updated(away=None, recovered=recovered).with_history(
                "recovered", **{"from": "away", "to": state, "transfer_id": away["transfer_id"]}
            )
            released.append(job.release(doc, Release(state, priority)))
    except Exception:
        for job in jobs:
            if owner.holds(job.ref):
                job.give_back()
        raise
    return released


_kernel.register_reconciler("eject", reconcile_eject)
_kernel.register_reconciler(_HOLD, reconcile_eject)
_kernel.register_reconciler(_RELEASE_KEEP, reconcile_release_keep)
for _purpose in _ADOPT.values():
    _kernel.register_reconciler(_purpose, reconcile_adopt)
