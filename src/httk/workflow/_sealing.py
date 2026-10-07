"""The sealing transaction (plan 4.4): ejection, export and addressed transfer, without any lock.

A transaction *T* moves a job, or a whole tree, out of ``jobs/`` through a
private directory ``tmp/eject.<T>`` (``E``) to one committed location:

====  ==============================================================  =================================
Step  What happens                                                    Mechanism
====  ==============================================================  =================================
S0    pre-check: kinds, joins, tree, claims, devices, target           read only (plus 13.1 receipts)
S1    ``E`` is born with ``envelope.partial/{markers,runners,tree}``   :func:`~httk.workflow._txn.birth`
S2    every member is fenced (``transferring``), in job-id order       single-source marker renames
S3    the root is fenced, recording the whole transaction              single-source marker rename
S4    manifest and runners linked in; member payloads moved in         ``link_new``, single-source renames
S5    the root payload moved to ``E/payload``, the envelope into it    single-source renames
S6    commit: ``E/payload`` renamed to the target                      one rename
S7    markers removed, ``E`` trashed (ejection and export only)        unlink, :func:`~httk.workflow._txn.trash`
====  ==============================================================  =================================

Abort is decided by renaming ``E`` to ``tmp/abort.<T>`` (``A``); after that no
forward step can succeed (every forward move names a path inside ``E``), and
the abort steps move every payload back and unfence the markers. Any actor
that finds a marker ``transferring`` under *T* reads the phase from fixed names
(:func:`settle`) and continues the transaction forward or backward; recovery
does so only when the recorded owner is evidently gone.

The committed location is the target directory of an ejection (or an entry of
the exchange outbox, by descriptor), ``transfers/exports/<T>/<job_key>`` for an
export to another filesystem (copied out by :func:`copy_out`), or
``transfers/outgoing/<T>`` for an addressed transfer, whose markers stay
``transferring`` until the destination's acknowledgement.
"""

import errno
import json
import logging
import os
import shutil
import stat
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePosixPath
from typing import Any

from . import _txn
from ._bundle import (
    TRANSFER_DIRECTORY,
    TRANSFER_MANIFEST,
    TRANSFER_MARKERS,
    TRANSFER_RUNNERS,
    TRANSFER_TREE,
    BundleManifest,
    BundleMember,
    BundleRunner,
    _payload_digest,
    _payload_seal_sha256,
    _runner_digest,
    verify_bundle,
)
from ._job_tree import _markers_at, is_detached, spawned_children
from ._receipts import CLOCK_SKEW_NS, FRESHNESS_WINDOW_NS, record_carried_receipt
from ._util import fsync_directory, fsync_tree, json_bytes
from .errors import FormatError, TransitionLostError, WorkflowError, WorkspaceCorruptionError
from .models import (
    EXCHANGE_DIRECTORY,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TERMINAL_KINDS,
    JobDefinition,
    Marker,
    _read_regular_file,
    canonical_uuid,
    check_job_placement,
    normalize_placement,
    parse_job_key,
    parse_placement_text,
    placement_text,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

ENVELOPE_PARTIAL = "envelope.partial"
ENVELOPE = "envelope"
PAYLOAD = "payload"
#: The record an export directory carries beside the held bundle: where the copy-out goes.
COPY_TO = "copy-to.json"
#: The publication witness a copy-out owner links into its staging directory before X5.
PUBLISHING = "publishing"
#: The record beside a held bundle in ``transfers/in-doubt/<T>/`` whose copy-out may have published it.
IN_DOUBT = "in-doubt.json"
#: How long an ``eject.<T>`` no marker names, or a ``birth.*``, is left alone by the orphan sweep.
ORPHAN_SECONDS = 24 * 3600
#: Transfers decided aborted after their commit that this process already reported at warning level.
_IN_DOUBT_REPORTED: set[str] = set()
#: The operator's reclaim authorization inside ``A``: only with it does an abort take ``outgoing/<T>`` back.
RECLAIM = "reclaim"
#: Tree members are terminal or paused, so nothing in the tree can start between S0 and its fence.
EJECT_MEMBER_KINDS = TERMINAL_KINDS | {"paused"}
#: The state-frame members an unfence does not restore: the frame header the transition writes itself.
_FRAME_HEADER = frozenset(
    {
        "format",
        "format_version",
        "workspace_id",
        "job_id",
        "job_key",
        "placement",
        "state_generation",
        "kind",
        "previous_record_ref",
        "created_at",
        "priority",
        "outgoing",
        "prior_kind",
        "prior_state",
    }
)
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
_COMMIT_REFUSALS = frozenset({errno.EXDEV, errno.ENOTEMPTY, errno.EEXIST, errno.ENOTDIR, errno.EISDIR})


class _PhaseChanged(Exception):
    """Another actor moved the transaction on: read the phase again."""


class _Abort(Exception):
    """The transaction cannot commit: abort it."""


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------


def eject_directory(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``tmp/eject.<T>``, the live transaction directory.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :return: The path.
    """

    return workspace.control / "tmp" / f"eject.{transfer_id}"


def abort_directory(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``tmp/abort.<T>``, the transaction directory once abort was decided.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :return: The path.
    """

    return workspace.control / "tmp" / f"abort.{transfer_id}"


def outgoing_path(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``transfers/outgoing/<T>``, where a sealed addressed bundle waits for its acknowledgement.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :return: The path.
    """

    return workspace.control / "transfers" / "outgoing" / transfer_id


def retired_path(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``transfers/retired/<T>``, an acknowledged addressed bundle kept ``trash_days``.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :return: The path.
    """

    return workspace.control / "transfers" / "retired" / transfer_id


def in_doubt_directory(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``transfers/in-doubt/<T>``: an export whose copy-out may already have published it.

    Ordinary copy-out only ever claims ``exports/<T>/<key>``, so a bundle held
    here is never copied out again; ``httk job adopt`` of it takes it back.

    :param workspace: The source workspace.
    :param transfer_id: The transaction id.
    :return: The directory path.
    """

    return workspace.control / "transfers" / "in-doubt" / transfer_id


def exports_directory(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``transfers/exports/<T>``, which holds an exported bundle until it is copied out.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :return: The path.
    """

    return workspace.control / "transfers" / "exports" / transfer_id


def exchange_outbox(workspace: Workspace) -> Path:
    """Return the exchange outbox an ejection may commit into by descriptor.

    :param workspace: The source workspace.
    :return: ``WORKSPACE/exchange/outbox``.
    """

    return workspace.root / EXCHANGE_DIRECTORY / "outbox"


def _lexists(path: Path) -> bool:
    try:
        os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return False
    return True


def _is_directory(path: Path) -> bool:
    try:
        return stat.S_ISDIR(os.lstat(path).st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False


def _sync(workspace: Workspace, *directories: Path) -> None:
    if not workspace.durable:
        return
    for directory in dict.fromkeys(directories):
        try:
            fsync_directory(directory)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# The transaction record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TxnJob:
    """One job of a transaction: where it lives and where it hangs in the tree.

    :param job_id: The job id.
    :param job_key: The job key.
    :param placement: The job's placement in this workspace.
    :param parent_job_id: The member's parent (the root or an earlier member), ``None`` for the root.
    """

    job_id: str
    job_key: str
    placement: PurePosixPath
    parent_job_id: str | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the frame form of a member.

        :return: The ``members`` entry of the root's ``outgoing``.
        """

        return {
            "job_id": self.job_id,
            "job_key": self.job_key,
            "placement": placement_text(self.placement),
            "parent_job_id": self.parent_job_id,
        }

    @classmethod
    def from_mapping(cls, value: object) -> "TxnJob":
        """Parse one ``members`` entry of a root frame.

        :param value: The entry.
        :return: The member.
        :raises httk.workflow.errors.FormatError: If the entry is malformed.
        """

        if not isinstance(value, Mapping):
            raise FormatError("transaction member must be an object")
        job_id = canonical_uuid(value.get("job_id"), "member job_id")
        job_key = value.get("job_key")
        if not isinstance(job_key, str) or parse_job_key(job_key)[1] != job_id:
            raise FormatError("transaction member job_key does not carry its job id")
        parent = value.get("parent_job_id")
        return cls(
            job_id,
            job_key,
            parse_placement_text(value.get("placement")),
            None if parent is None else canonical_uuid(parent, "member parent_job_id"),
        )


@dataclass(frozen=True)
class Commit:
    """Where a transaction commits.

    :param kind: ``"path"`` (an ejection to a directory on this filesystem),
        ``"exchange"`` (an ejection into the exchange outbox, by descriptor),
        ``"inbox"`` (an ejection into a workspace's exchange inbox, by descriptor),
        ``"export"`` (an ejection to another filesystem, held below
        ``transfers/exports/<T>`` and copied out) or ``"outgoing"`` (an
        addressed transfer).
    :param path: The final directory, for ``"path"`` and ``"export"``; the
        receiving workspace's root, for ``"inbox"``.
    :param name: The outbox or inbox entry name, for ``"exchange"`` and ``"inbox"``.
    """

    kind: str
    path: Path | None = None
    name: str | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the frame form.

        :return: The ``target`` member of the root's ``outgoing``.
        """

        value: dict[str, object] = {"kind": self.kind}
        if self.path is not None:
            value["path"] = str(self.path)
        if self.name is not None:
            value["name"] = self.name
        return value

    @classmethod
    def from_mapping(cls, value: object) -> "Commit":
        """Parse the ``target`` member of a root frame.

        :param value: The member.
        :return: The commit target.
        :raises httk.workflow.errors.FormatError: If the member is malformed.
        """

        if not isinstance(value, Mapping) or value.get("kind") not in {
            "path",
            "exchange",
            "inbox",
            "export",
            "outgoing",
        }:
            raise FormatError("transaction target is malformed")
        kind = str(value["kind"])
        path = value.get("path")
        name = value.get("name")
        if kind in {"path", "export", "inbox"} and (not isinstance(path, str) or not os.path.isabs(path)):
            raise FormatError("transaction target needs an absolute path")
        if kind in {"exchange", "inbox"} and (
            not isinstance(name, str) or not name or "/" in name or name in {".", ".."}
        ):
            raise FormatError("transaction target needs an outbox entry name")
        return cls(kind, Path(path) if isinstance(path, str) else None, name if isinstance(name, str) else None)


@dataclass(frozen=True)
class Transaction:
    """Everything the root's fencing frame records about one sealing transaction.

    :param transfer_id: The transaction id *T*.
    :param root: The root job.
    :param members: The tree members, top-down.
    :param commit: Where the transaction commits.
    :param destination_workspace_id: The addressed workspace, or ``None`` for an ejection.
    :param destination_remote: The destination's remote name, when addressed through one.
    :param destination_placement: Where the destination publishes the root.
    :param owner: The owner token of the actor that started it.
    :param started_at: When it started, in integer UTC nanoseconds.
    :param sealed_at: When the root was fenced, in integer UTC nanoseconds.
    :param payload_inode: The root payload's ``st_ino`` when it was fenced.
    """

    transfer_id: str
    root: TxnJob
    members: tuple[TxnJob, ...]
    commit: Commit
    destination_workspace_id: str | None
    destination_remote: str | None
    destination_placement: PurePosixPath
    owner: str
    started_at: int
    sealed_at: int = 0
    payload_inode: int = 0

    @property
    def addressed(self) -> bool:
        """Return whether the transaction is an addressed transfer."""

        return self.commit.kind == "outgoing"

    def root_outgoing(self) -> dict[str, object]:
        """Return the ``outgoing`` member of the root's fencing frame.

        :return: The member.
        """

        return {
            "transfer_id": self.transfer_id,
            "role": "root",
            "members": [member.as_mapping() for member in self.members],
            "destination_workspace_id": self.destination_workspace_id,
            "destination_remote": self.destination_remote,
            "destination_placement": placement_text(self.destination_placement),
            "target": self.commit.as_mapping(),
            "owner": self.owner,
            "started_at": self.started_at,
            "sealed_at": self.sealed_at,
            "payload_inode": self.payload_inode,
        }

    def member_outgoing(self) -> dict[str, object]:
        """Return the ``outgoing`` member of a member's fencing frame.

        :return: The member.
        """

        return {
            "transfer_id": self.transfer_id,
            "role": "member",
            "root_job_id": self.root.job_id,
            "root_placement": placement_text(self.root.placement),
            "root_job_key": self.root.job_key,
            "owner": self.owner,
            "started_at": self.started_at,
        }

    @classmethod
    def from_root(cls, marker: Marker, frame: Mapping[str, Any]) -> "Transaction":
        """Read the transaction from the root's fencing frame.

        :param marker: The root's ``transferring`` marker.
        :param frame: Its state frame.
        :return: The transaction.
        :raises httk.workflow.errors.FormatError: If the frame does not record a root.
        """

        outgoing = frame.get("outgoing")
        if not isinstance(outgoing, Mapping) or outgoing.get("role") != "root":
            raise FormatError(f"transferring frame of {marker.job_key} records no transaction root")
        members_raw = outgoing.get("members")
        if not isinstance(members_raw, list):
            raise FormatError("transaction members must be an array")
        destination = outgoing.get("destination_workspace_id")
        remote = outgoing.get("destination_remote")
        numbers = [outgoing.get(name) for name in ("started_at", "sealed_at", "payload_inode")]
        if any(isinstance(value, bool) or not isinstance(value, int) for value in numbers):
            raise FormatError("transaction times and inode must be integers")
        owner = outgoing.get("owner")
        if not isinstance(owner, str):
            raise FormatError("transaction owner must be a string")
        return cls(
            transfer_id=canonical_uuid(outgoing.get("transfer_id"), "transfer_id"),
            root=TxnJob(marker.job_id, marker.job_key, marker.placement),
            members=tuple(TxnJob.from_mapping(item) for item in members_raw),
            commit=Commit.from_mapping(outgoing.get("target")),
            destination_workspace_id=None if destination is None else canonical_uuid(destination, "destination"),
            destination_remote=remote if isinstance(remote, str) else None,
            destination_placement=parse_placement_text(outgoing.get("destination_placement")),
            owner=owner,
            started_at=int(numbers[0]),  # type: ignore[arg-type]
            sealed_at=int(numbers[1]),  # type: ignore[arg-type]
            payload_inode=int(numbers[2]),  # type: ignore[arg-type]
        )

    @property
    def jobs(self) -> tuple[TxnJob, ...]:
        """Return the root, then the members top-down."""

        return (self.root, *self.members)


@dataclass
class Fenced:
    """The markers ``transferring`` under one transaction, with their frames.

    :param transfer_id: The transaction.
    :param root: The root's marker and frame, once the root is fenced.
    :param members: Each fenced member's marker and frame, by job id.
    """

    transfer_id: str
    root: tuple[Marker, dict[str, Any]] | None = None
    members: dict[str, tuple[Marker, dict[str, Any]]] = field(default_factory=dict)

    def all(self) -> list[tuple[Marker, dict[str, Any]]]:
        """Return every fenced marker, the members in job-id order first, the root last.

        :return: The markers and frames.
        """

        result = [self.members[job_id] for job_id in sorted(self.members)]
        return result + ([self.root] if self.root is not None else [])

    def owner(self) -> tuple[str, int] | None:
        """Return the recorded owner and start time, from the root or else a member.

        :return: The owner token and start time, or ``None`` when no frame records them.
        """

        for _marker, frame in ([self.root] if self.root is not None else []) + self.all():
            outgoing = frame.get("outgoing")
            if isinstance(outgoing, Mapping):
                owner, started = outgoing.get("owner"), outgoing.get("started_at")
                if isinstance(owner, str) and isinstance(started, int) and not isinstance(started, bool):
                    return owner, started
        return None

    def root_identity(self) -> TxnJob | None:
        """Return the root's identity, from its own marker or from a member's frame.

        :return: The root, or ``None`` when nothing records it.
        """

        if self.root is not None:
            marker = self.root[0]
            return TxnJob(marker.job_id, marker.job_key, marker.placement)
        for _marker, frame in self.all():
            outgoing = frame.get("outgoing")
            if not isinstance(outgoing, Mapping):
                continue
            try:
                return TxnJob(
                    canonical_uuid(outgoing.get("root_job_id"), "root_job_id"),
                    str(outgoing.get("root_job_key")),
                    parse_placement_text(outgoing.get("root_placement")),
                )
            except (FormatError, ValueError):
                continue
        return None


def _outgoing_of(frame: Mapping[str, Any]) -> Mapping[str, Any] | None:
    outgoing = frame.get("outgoing")
    return outgoing if isinstance(outgoing, Mapping) and isinstance(outgoing.get("transfer_id"), str) else None


def fenced_transactions(workspace: Workspace) -> dict[str, Fenced]:
    """Group every ``transferring`` marker of the workspace by the transaction it is fenced under.

    A marker whose frame cannot be read, or records no transaction, is reported and left out.

    :param workspace: The source workspace.
    :return: The fenced markers by transaction id.
    """

    groups: dict[str, Fenced] = {}
    for marker in workspace.scan_markers(("transferring",)):
        try:
            frame = workspace.read_state(marker)
        except (WorkflowError, OSError) as exc:
            _LOGGER.warning(
                "cannot read the transferring frame of %s: %s",
                marker.job_key,
                exc,
                extra={"event": "transfer_frame_unreadable", "job_key": marker.job_key},
            )
            continue
        outgoing = _outgoing_of(frame)
        if outgoing is None:
            _LOGGER.warning(
                "transferring job %s records no transaction",
                marker.job_key,
                extra={"event": "transfer_frame_unknown", "job_key": marker.job_key},
            )
            continue
        transfer_id = str(outgoing["transfer_id"])
        group = groups.setdefault(transfer_id, Fenced(transfer_id, None, {}))
        if outgoing.get("role") == "root":
            group.root = (marker, frame)
        else:
            group.members[marker.job_id] = (marker, frame)
    return groups


def fenced_under(workspace: Workspace, transfer_id: str) -> Fenced:
    """Return the markers ``transferring`` under one transaction.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :return: The fenced markers (possibly none).
    """

    return fenced_transactions(workspace).get(transfer_id) or Fenced(transfer_id, None, {})


# ---------------------------------------------------------------------------
# S0: what may leave
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Member:
    marker: Marker
    job: JobDefinition
    parent_job_id: str


def ejection_members(workspace: Workspace, root: Marker, waiting: Mapping[str, set[str]]) -> list[_Member]:
    """Return the bound descendants an ejected job takes along, top-down, or refuse the tree.

    Children are found from the spawn records and confirmed from the child side,
    over every marker kind: a member that is already ``transferring`` (under
    another transaction) blocks the tree, as does one that is not paused or
    terminal or that takes part in an unresolved join.

    :param workspace: The source workspace.
    :param root: The root's marker.
    :param waiting: The waiting-parent map of the workspace.
    :return: The members, top-down.
    :raises ValueError: If the tree cannot leave now.
    :raises httk.workflow.errors.FormatError: If a member's placement names a job directory.
    """

    from .transfers import _unresolved_join_reference

    try:
        root_job = JobDefinition.from_path(workspace.payload_path(root.placement, root.job_key) / "job.json")
    except (WorkflowError, OSError) as exc:
        raise ValueError(f"job definition cannot be read to check its tree: {exc}") from exc
    members: list[_Member] = []
    blockers: list[str] = []
    seen = {root.job_id}
    queue: list[tuple[Marker, JobDefinition]] = [(root, root_job)]
    while queue:
        parent_marker, parent_job = queue.pop(0)
        payload = workspace.payload_path(parent_marker.placement, parent_marker.job_key)
        try:
            entries = spawned_children(payload)
        except (WorkflowError, OSError) as exc:
            raise ValueError(f"its spawn records cannot be read: {exc}") from exc
        live: dict[tuple[str, str], Marker] = {}
        for placement in dict.fromkeys(placement_text(normalize_placement(str(e["placement"]))) for e in entries):
            try:
                live.update(((placement, m.job_key), m) for m in _markers_at(workspace, placement, STATE_KINDS))
            except (FormatError, ValueError):
                continue
        for entry in entries:
            child_marker = live.get(
                (placement_text(normalize_placement(str(entry["placement"]))), str(entry["job_key"]))
            )
            if child_marker is None or child_marker.job_id != entry["job_id"] or child_marker.job_id in seen:
                continue
            if child_marker.kind == "transferring":
                blockers.append(f"{child_marker.job_key} is transferring")
                seen.add(child_marker.job_id)
                continue
            child_payload = workspace.payload_path(child_marker.placement, child_marker.job_key)
            try:
                child = JobDefinition.from_path(child_payload / "job.json")
            except (FormatError, OSError):
                continue
            recorded = child.parent
            if recorded is None or recorded.get("job_id") != parent_job.id:
                continue
            if entry["spawn_id"] is not None and recorded.get("spawn_id") != entry["spawn_id"]:
                continue
            if is_detached(child_payload):
                continue
            seen.add(child_marker.job_id)
            if child_marker.kind not in EJECT_MEMBER_KINDS:
                blockers.append(f"{child_marker.job_key} is {child_marker.kind}")
            elif _unresolved_join_reference(workspace, child_marker, waiting):
                blockers.append(f"{child_marker.job_key} is in an unresolved join")
            check_job_placement(child_marker.placement)
            members.append(_Member(child_marker, child, parent_job.id))
            queue.append((child_marker, child))
    if blockers:
        raise ValueError(f"its tree cannot leave yet: {'; '.join(blockers)} (wait for them to end, or pause them)")
    return members


def _open_box(root: Path, box: str) -> int:
    """Open ``<root>/exchange/<box>`` by descriptor, never following a symlink below *root*.

    The exchange is client territory: a client that replaced ``exchange`` or
    the box by a symlink must not steer a commit anywhere else.

    :param root: The workspace root whose exchange holds the box.
    :param box: ``"inbox"`` or ``"outbox"``.
    :return: The box's directory descriptor.
    :raises ValueError: If ``exchange`` or the box is not a real directory.
    """

    descriptor = os.open(root, _DIRECTORY_FLAGS)
    try:
        for part in (EXCHANGE_DIRECTORY, box):
            try:
                inner = os.open(part, _DIRECTORY_FLAGS | os.O_NOFOLLOW, dir_fd=descriptor)
            except OSError as exc:
                raise ValueError(
                    f"the exchange {box} {root / EXCHANGE_DIRECTORY / box} is not a real directory ({exc.strerror})"
                ) from exc
            os.close(descriptor)
            descriptor = inner
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _open_outbox(workspace: Workspace) -> int:
    """Open this workspace's exchange outbox by descriptor, never following a symlink below its root."""

    return _open_box(workspace.root, "outbox")


def _box_of(workspace: Workspace, commit: Commit) -> tuple[Path, str] | None:
    """Return the workspace root and box an exchange commit goes into, or ``None`` for another kind."""

    if commit.kind == "exchange":
        return workspace.root, "outbox"
    if commit.kind == "inbox":
        assert commit.path is not None
        return commit.path, "inbox"
    return None


def _check_target(workspace: Workspace, commit: Commit, job_key: str) -> None:
    """Require the commit target to be absent (S0), by descriptor for an exchange box."""

    box = _box_of(workspace, commit)
    if box is not None:
        assert commit.name is not None
        directory = _open_box(*box)
        try:
            os.lstat(commit.name, dir_fd=directory)
        except FileNotFoundError:
            return
        finally:
            os.close(directory)
        raise FileExistsError(f"eject destination already exists: {box[0] / EXCHANGE_DIRECTORY / box[1] / commit.name}")
    if commit.kind in {"path", "export"}:
        assert commit.path is not None
        if _lexists(commit.path):
            raise FileExistsError(f"eject destination already exists: {commit.path}")


def _precheck(
    workspace: Workspace,
    root: Marker,
    commit: Commit,
    *,
    waiting: Mapping[str, set[str]],
    with_tree: bool,
    destination_placement: PurePosixPath,
    now: int,
) -> list[_Member]:
    """S0: decide that the root (and its tree) may leave, without changing anything but receipts."""

    from .transfers import _require_tree_boundary, _unresolved_join_reference

    if root.kind not in QUIESCENT_KINDS:
        if root.kind == "transferring":
            raise ValueError("job is already transferring")
        raise ValueError(f"job is not quiescent and cannot transfer: {root.kind}")
    if _unresolved_join_reference(workspace, root, waiting):
        raise ValueError("job participates in an unresolved join and cannot transfer")
    check_job_placement(root.placement)
    check_job_placement(destination_placement)
    if commit.kind == "outgoing":
        _require_tree_boundary(workspace, root, with_tree=with_tree)
        members: list[_Member] = []
    else:
        _require_tree_boundary(workspace, root, with_tree=True)
        members = ejection_members(workspace, root, waiting)
    claims = workspace.control / "transfers" / "adopting"
    tmp_device = os.stat(workspace.control / "tmp").st_dev
    for marker in (root, *(member.marker for member in members)):
        if _lexists(claims / marker.job_id):
            raise ValueError(f"job {marker.job_key} is still being adopted here; transfer it once that finishes")
        payload = workspace.payload_path(marker.placement, marker.job_key)
        try:
            information = os.lstat(payload)
        except FileNotFoundError:
            raise ValueError(f"job {marker.job_key} has no payload at {payload}") from None
        if not stat.S_ISDIR(information.st_mode):
            raise FormatError(f"job {marker.job_key} payload {payload} is not a real directory")
        if information.st_dev != tmp_device:
            raise ValueError(
                f"job {marker.job_key} payload {payload} is on another filesystem than the workspace control "
                "directory, so it cannot be moved by rename"
            )
        envelope = payload / TRANSFER_DIRECTORY
        try:
            envelope_mode = os.lstat(envelope).st_mode
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISDIR(envelope_mode):
                raise FormatError(f"cannot seal {payload}: its {TRANSFER_DIRECTORY} is a symlink or not a directory")
            if os.listdir(envelope):
                raise FormatError(f"cannot seal {payload}: it holds a {TRANSFER_DIRECTORY} of its own")
    _check_target(workspace, commit, root.job_key)
    # 13.1: a job that arrived by an addressed transfer still inside its window
    # keeps the fact that it was received here once it leaves again.
    for marker in (root, *(member.marker for member in members)):
        record_carried_receipt(workspace, marker.job_id, workspace.read_state(marker).get("transfer"), now)
    return members


# ---------------------------------------------------------------------------
# S1-S3: birth and fences
# ---------------------------------------------------------------------------


def _runner_parents(workspace: Workspace, jobs: Sequence[TxnJob]) -> set[PurePosixPath]:
    """S0: the store directories of every workspace runner the jobs pin, for the skeleton.

    Every directory inside ``E`` is created by the birth, so no later step ever
    creates one by a path that could re-create ``E`` after an abort. A job whose
    ``job.json`` cannot be read here is left to S4, which aborts on it. A runner
    directory that only a ``job.json`` changed after S0 names is created at S4 by
    non-recursive ``mkdir`` below these, which also fails once ``E`` is gone.
    """

    parents: set[PurePosixPath] = set()
    for job in jobs:
        try:
            definition = JobDefinition.from_path(_member_home(workspace, job) / "job.json")
        except (FormatError, OSError):
            continue
        if definition.runner_source == "workspace" and definition.runner_sha256 is not None:
            parents.add(PurePosixPath(*definition.runner_path.parts[:-1]))
    return parents


def _skeleton(
    members: Sequence[TxnJob], jobs: Sequence[TxnJob], runner_parents: Iterable[PurePosixPath] = ()
) -> Callable[[Path], None]:
    def build(directory: Path) -> None:
        partial = directory / ENVELOPE_PARTIAL
        for name in (TRANSFER_MARKERS, TRANSFER_RUNNERS, TRANSFER_TREE):
            (partial / name).mkdir(parents=True)
        for job in jobs:
            os.close(os.open(partial / TRANSFER_MARKERS / job.job_id, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644))
        for member in members:
            (partial / TRANSFER_TREE).joinpath(*member.placement.parts).mkdir(parents=True, exist_ok=True)
        for parent in runner_parents:
            (partial / TRANSFER_RUNNERS).joinpath(*parent.parts).mkdir(parents=True, exist_ok=True)

    return build


def _fence(workspace: Workspace, marker: Marker, outgoing: Mapping[str, object], *, reason: str) -> Marker:
    """Move one quiescent marker to ``transferring``, recording its prior state."""

    prior_state = workspace.read_state(marker)
    with workspace.open_journal_writer() as writer:
        return workspace.transition(
            writer,
            marker,
            "transferring",
            {
                "outgoing": dict(outgoing),
                "prior_kind": marker.kind,
                "prior_state": prior_state,
                "reason": reason,
            },
            # A sealed job may leave: its seal travels inside the payload.
            allow_sealed=True,
        )


def _unfence(workspace: Workspace, marker: Marker, frame: Mapping[str, Any]) -> Marker:
    """Move one ``transferring`` marker back to its prior kind with its prior members."""

    prior_kind = frame.get("prior_kind")
    prior_state = frame.get("prior_state")
    if prior_kind not in QUIESCENT_KINDS or not isinstance(prior_state, Mapping):
        raise WorkspaceCorruptionError(f"transferring frame of {marker.job_key} records no prior state")
    restored = {name: value for name, value in prior_state.items() if name not in _FRAME_HEADER}
    with workspace.open_journal_writer() as writer:
        return workspace.transition(writer, marker, str(prior_kind), restored, allow_sealed=True)


# ---------------------------------------------------------------------------
# S4-S7: the forward steps
# ---------------------------------------------------------------------------


def _check_identity(path: Path, txn: Transaction) -> None:
    """The secondary ``st_ino`` check of a root payload location; a mismatch stops and reports."""

    if txn.payload_inode and os.lstat(path).st_ino != txn.payload_inode:
        raise WorkspaceCorruptionError(
            f"transfer {txn.transfer_id}: {path} is not the payload that was fenced "
            f"(inode {os.lstat(path).st_ino}, recorded {txn.payload_inode}); stopping, nothing was moved"
        )


def _member_home(workspace: Workspace, job: TxnJob) -> Path:
    return workspace.payload_path(job.placement, job.job_key)


def _tree_relative(job: TxnJob) -> PurePosixPath:
    return PurePosixPath(TRANSFER_TREE, *job.placement.parts, job.job_key)


def _job_runner(workspace: Workspace, payload: Path) -> BundleRunner | None:
    """Return the workspace runner one payload pins, verified against the store."""

    job = JobDefinition.from_path(payload / "job.json")
    if job.runner_source != "workspace" or job.runner_sha256 is None:
        return None
    source = workspace.runner_store_path(job.runner_path)
    digest = _runner_digest(source)
    if digest != job.runner_sha256:
        raise _Abort(
            f"workspace runner {job.runner_path.as_posix()} has digest {digest}, but the job pinned {job.runner_sha256}"
        )
    return BundleRunner(job.runner_path, digest)


def _build_manifest(
    workspace: Workspace, txn: Transaction, fenced: Fenced, locations: Mapping[str, Path]
) -> BundleManifest:
    """Compute the deterministic manifest from the fenced frames and the payloads where they are now."""

    if fenced.root is None:
        raise _PhaseChanged()
    root_marker, root_frame = fenced.root
    members_by_id = fenced.members
    runners: dict[PurePosixPath, BundleRunner] = {}
    try:
        for job in txn.jobs:
            runner = _job_runner(workspace, locations[job.job_id])
            if runner is not None and runners.setdefault(runner.path, runner) != runner:
                raise _Abort(f"the tree pins runner {runner.path.as_posix()} with two digests")
        bundle_members = []
        for member in txn.members:
            if member.job_id not in members_by_id:
                raise WorkspaceCorruptionError(f"transfer {txn.transfer_id}: member {member.job_key} is not fenced")
            marker, frame = members_by_id[member.job_id]
            assert member.parent_job_id is not None
            bundle_members.append(
                BundleMember(
                    job_id=member.job_id,
                    job_key=member.job_key,
                    placement=member.placement,
                    parent_job_id=member.parent_job_id,
                    payload_sha256=_payload_digest(locations[member.job_id]),
                    seal_sha256=_payload_seal_sha256(locations[member.job_id]),
                    prior_kind=str(frame["prior_kind"]),
                    prior_state=dict(frame["prior_state"]),
                    priority=marker.priority,
                    source_generation=marker.generation,
                )
            )
        return BundleManifest(
            transfer_id=txn.transfer_id,
            source_workspace_id=workspace.workspace_id,
            destination_workspace_id=txn.destination_workspace_id,
            destination_remote=txn.destination_remote,
            destination_placement=txn.destination_placement,
            sealed_at=txn.sealed_at,
            job_id=txn.root.job_id,
            job_key=txn.root.job_key,
            source_placement=txn.root.placement,
            payload_sha256=_payload_digest(locations[txn.root.job_id]),
            seal_sha256=_payload_seal_sha256(locations[txn.root.job_id]),
            runners=tuple(sorted(runners.values(), key=lambda runner: runner.path.as_posix())),
            prior_kind=str(root_frame["prior_kind"]),
            prior_state=dict(root_frame["prior_state"]),
            priority=root_marker.priority,
            source_generation=root_marker.generation,
            members=tuple(bundle_members),
        )
    except (FormatError, OSError) as exc:
        # A payload that cannot be digested (a special file, an escaping
        # symlink) or read can never be sealed: the transaction aborts.
        raise _Abort(f"the job cannot be sealed: {exc}") from exc


def _manifest_bytes(manifest: BundleManifest) -> bytes:
    return json_bytes(manifest.as_mapping()) + b"\n"


def _embed_runner(workspace: Workspace, partial: Path, runner: BundleRunner) -> None:
    """Place one store runner below ``envelope.partial/runners`` write-if-absent-else-verify.

    Nothing here creates a directory inside ``E`` by a path that could re-create
    ``E`` once another actor renamed it to ``A``: the birth made the runner
    directories known at S0, a missing one is made by a single non-recursive
    ``mkdir`` below an existing one, and a directory runner is built under
    ``tmp/birth.*`` and moved in by one rename. Each of those fails with
    ``ENOENT`` once ``E`` is gone, which is a phase change.
    """

    source = workspace.runner_store_path(runner.path)
    name = runner.path.parts[-1]
    try:
        parent = partial / TRANSFER_RUNNERS
        for part in runner.path.parts[:-1]:
            parent = parent / part
            try:
                os.mkdir(parent, 0o755)
            except FileExistsError:
                pass
        target = parent / name
        if source.is_dir():
            if not _lexists(target):

                def build(directory: Path) -> None:
                    shutil.copytree(source, directory, symlinks=True, dirs_exist_ok=True)
                    # The store keeps runner trees read-only; moving a directory
                    # to another parent needs write permission on it.
                    os.chmod(directory, stat.S_IMODE(os.lstat(directory).st_mode) | stat.S_IWUSR)

                # False means the name was there already; the digest below decides.
                _txn.birth(workspace.control / "tmp", name, build, durable=workspace.durable, parent=parent)
        else:
            # False means the name was there already; the digest below decides whether it is right.
            _txn.link_new(parent, name, source.read_bytes(), mode=0o555, durable=workspace.durable)
        digest = _runner_digest(target)
    except FileNotFoundError as exc:
        if not _lexists(partial):
            raise _PhaseChanged() from exc
        raise
    if digest != runner.sha256:
        raise _Abort(f"the bundled runner {runner.path.as_posix()} does not match the store")


def _clean_temporaries(envelope: Path, manifest: BundleManifest | None) -> None:
    """Remove :func:`~httk.workflow._txn.link_new` leftovers from the envelope and its runner directories."""

    def clean(directory: Path) -> None:
        try:
            names = os.listdir(directory)
        except FileNotFoundError:
            return
        for name in names:
            if _txn.is_link_temporary(name):
                _txn.remove_tree(directory / name)

    clean(envelope)
    leaves = set() if manifest is None else {runner.path for runner in manifest.runners}
    prefixes = {leaf.parents[index] for leaf in leaves for index in range(len(leaf.parts) - 1)}
    clean(envelope / TRANSFER_RUNNERS)
    for prefix in sorted(prefixes, key=lambda item: item.as_posix()):
        clean((envelope / TRANSFER_RUNNERS).joinpath(*prefix.parts))


def _read_envelope_manifest(envelope: Path) -> BundleManifest | None:
    try:
        return BundleManifest.from_mapping(json.loads(_read_regular_file(envelope / TRANSFER_MANIFEST, 1 << 20)))
    except (OSError, ValueError, FormatError):
        return None


def _prepare(workspace: Workspace, txn: Transaction) -> None:
    """S4: link the manifest and runners in, move every member payload in, complete the envelope."""

    eject = eject_directory(workspace, txn.transfer_id)
    partial = eject / ENVELOPE_PARTIAL
    fenced = fenced_under(workspace, txn.transfer_id)
    locations: dict[str, Path] = {}
    for job in txn.jobs:
        home = _member_home(workspace, job)
        inside = partial.joinpath(*_tree_relative(job).parts)
        locations[job.job_id] = home if job is txn.root or _lexists(home) else inside
    manifest = _build_manifest(workspace, txn, fenced, locations)
    data = _manifest_bytes(manifest)
    _txn._hook("S4.manifest")
    if not _txn.link_new(partial, TRANSFER_MANIFEST, data, durable=workspace.durable):
        try:
            existing = _read_regular_file(partial / TRANSFER_MANIFEST, 1 << 20)
        except OSError as exc:
            if not _lexists(partial):
                raise _PhaseChanged() from exc
            raise
        if existing != data:
            raise _Abort(
                "the job changed while it was being sealed (its manifest no longer matches); "
                "a lingering attempt may still be writing into it"
            )
    for runner in manifest.runners:
        _txn._hook("S4.runner")
        _embed_runner(workspace, partial, runner)
    _txn._hook("S4.runners")
    for member in txn.members:
        _txn._hook("S4.member")
        home = _member_home(workspace, member)
        inside = partial.joinpath(*_tree_relative(member).parts)
        if _lexists(home):
            if not _txn.rename_verified(home, inside) and not _lexists(inside):
                raise _PhaseChanged()
        elif not _lexists(inside):
            raise _PhaseChanged()
    _sync(workspace, partial / TRANSFER_TREE, workspace.jobs)
    _txn._hook("S4.envelope")
    if not _txn.rename_verified(partial, eject / ENVELOPE) and not _lexists(eject / ENVELOPE):
        raise _PhaseChanged()
    _sync(workspace, eject)


def _commit(workspace: Workspace, txn: Transaction) -> Path:
    """S6: one rename of ``E/payload`` to the committed location."""

    eject = eject_directory(workspace, txn.transfer_id)
    source = eject / PAYLOAD
    commit = txn.commit
    try:
        box = _box_of(workspace, commit)
        if box is not None:
            assert commit.name is not None
            location = committed_location(workspace, txn)
            try:
                directory = _open_box(*box)
            except ValueError as exc:
                raise _Abort(str(exc)) from exc
            tmp = os.open(workspace.control / "tmp", _DIRECTORY_FLAGS)
            try:
                if _probe_at(commit.name, directory) and _lexists(source):
                    raise _Abort(f"eject destination {location} was taken meanwhile")
                moved = _txn.rename_verified(
                    f"eject.{txn.transfer_id}/{PAYLOAD}", commit.name, src_dir_fd=tmp, dst_dir_fd=directory
                )
                if moved and workspace.durable:
                    os.fsync(directory)
            finally:
                os.close(tmp)
                os.close(directory)
        else:
            if commit.kind == "path":
                assert commit.path is not None
                location = commit.path
            elif commit.kind == "export":
                location = _born_exports(workspace, txn) / txn.root.job_key
            else:
                location = outgoing_path(workspace, txn.transfer_id)
                location.parent.mkdir(parents=True, exist_ok=True)
            if _lexists(location) and _lexists(source):
                raise _Abort(f"eject destination {location} was taken meanwhile")
            moved = _txn.rename_verified(source, location)
            if moved:
                _sync(workspace, location.parent)
    except OSError as exc:
        if exc.errno in _COMMIT_REFUSALS:
            raise _Abort(f"the commit to its target failed: {exc}") from exc
        raise
    if not moved:
        # The source is gone: committed by another actor, or aborted meanwhile.
        _txn._hook("phase.recheck")
        if not _lexists(eject):
            raise _PhaseChanged()
    _sync(workspace, eject)
    return location


def _probe_at(name: str, directory: int) -> bool:
    try:
        os.lstat(name, dir_fd=directory)
    except FileNotFoundError:
        return False
    return True


def _born_exports(workspace: Workspace, txn: Transaction) -> Path:
    """Create ``transfers/exports/<T>`` with its ``copy-to.json``, born complete by rename."""

    assert txn.commit.path is not None
    exports = exports_directory(workspace, txn.transfer_id)
    record = json_bytes(
        {"destination": str(txn.commit.path), "job_key": txn.root.job_key, "transfer_id": txn.transfer_id}
    )

    def build(directory: Path) -> None:
        (directory / COPY_TO).write_bytes(record + b"\n")

    exports.parent.mkdir(parents=True, exist_ok=True)
    tmp = workspace.control / "tmp"
    if not _lexists(exports):
        staging = tmp / f"birth.{uuid.uuid4().hex}"
        os.mkdir(staging, 0o755)
        try:
            build(staging)
            if workspace.durable:
                fsync_tree(staging)
            try:
                _txn.rename_verified(staging, exports)
            except OSError as exc:
                if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST}:
                    raise
        finally:
            _txn.remove_tree(staging)
        _sync(workspace, exports.parent)
    if _read_regular_file(exports / COPY_TO, 1 << 16) != record + b"\n":
        raise _Abort(f"{exports} records another copy-out")
    return exports


def committed_location(workspace: Workspace, txn: Transaction) -> Path:
    """Return the nominal committed location of a transaction.

    :param workspace: The source workspace.
    :param txn: The transaction.
    :return: The target directory, outbox entry, export entry or outgoing bundle.
    """

    commit = txn.commit
    if commit.kind == "path":
        assert commit.path is not None
        return commit.path
    if commit.kind == "exchange":
        assert commit.name is not None
        return exchange_outbox(workspace) / commit.name
    if commit.kind == "inbox":
        assert commit.path is not None and commit.name is not None
        return commit.path / EXCHANGE_DIRECTORY / "inbox" / commit.name
    if commit.kind == "export":
        return exports_directory(workspace, txn.transfer_id) / txn.root.job_key
    return outgoing_path(workspace, txn.transfer_id)


def _remove_markers(workspace: Workspace, fenced: Fenced, *, root: bool) -> None:
    """Unlink the members' markers, then (with *root*) the root's: only after a positively observed commit."""

    entries = [fenced.members[job_id] for job_id in sorted(fenced.members)]
    if root and fenced.root is not None:
        entries.append(fenced.root)
    for marker, _frame in entries:
        _txn._hook("S7.marker")
        try:
            os.unlink(marker.path)
        except FileNotFoundError:
            pass
        workspace._index_forget(marker.job_id)
        _sync(workspace, marker.path.parent)


def _clean_up(workspace: Workspace, transfer_id: str, directory: Path) -> None:
    """S7: remove the markers of a committed ejection or export, then trash its transaction directory."""

    _remove_markers(workspace, fenced_under(workspace, transfer_id), root=True)
    _txn._hook("S7.trash")
    _txn.trash(directory, control=workspace.control, holds_payload=_txn.holds_job_payload)


def _forward(workspace: Workspace, txn: Transaction) -> Path:
    """Continue one transaction forward from wherever it is, through S7. Every step is repeatable.

    Every forward move names a path inside ``E``, so once another actor renamed
    ``E`` to ``A`` a move fails; ``E`` can never reappear, so a failure with
    ``E`` gone means the phase changed rather than an error.
    """

    eject = eject_directory(workspace, txn.transfer_id)
    try:
        location = _forward_moves(workspace, txn)
    except OSError as exc:
        if not _lexists(eject):
            raise _PhaseChanged() from exc
        raise
    if txn.addressed:
        outgoing, retired = outgoing_path(workspace, txn.transfer_id), retired_path(workspace, txn.transfer_id)
        if not _lexists(outgoing) and _lexists(retired):
            # Acknowledged, but the clean-up after it was interrupted.
            _finish_retirement(workspace, txn)
            return retired
        return location
    _txn._hook("S7.cleanup")
    _clean_up(workspace, txn.transfer_id, eject)
    return location


def _forward_moves(workspace: Workspace, txn: Transaction) -> Path:
    """S4-S6 from wherever the transaction is; return the committed location."""

    eject = eject_directory(workspace, txn.transfer_id)
    home = _member_home(workspace, txn.root)
    if _lexists(home):
        _check_identity(home, txn)
        if _lexists(eject / ENVELOPE_PARTIAL):
            _prepare(workspace, txn)
        if not _lexists(eject / ENVELOPE):
            raise _PhaseChanged()
        _txn._hook("S5.detach")
        if not _txn.rename_verified(home, eject / PAYLOAD) and not _lexists(eject / PAYLOAD):
            raise _PhaseChanged()
        _sync(workspace, home.parent, eject)
    payload = eject / PAYLOAD
    _txn._hook("phase.read")
    if _lexists(payload):
        _check_identity(payload, txn)
        envelope = eject / ENVELOPE
        if _lexists(envelope):
            _clean_temporaries(envelope, _read_envelope_manifest(envelope))
            _txn._hook("S5.envelope")
            try:
                moved = _txn.rename_verified(envelope, payload / TRANSFER_DIRECTORY)
            except OSError as exc:
                if exc.errno in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOTDIR}:
                    raise _Abort(f"the job planted a {TRANSFER_DIRECTORY} of its own while it was sealed") from exc
                raise
            if not moved and not _lexists(payload / TRANSFER_DIRECTORY):
                raise _PhaseChanged()
            _sync(workspace, eject, payload)
        _txn._hook("S6.commit")
        return _commit(workspace, txn)
    held = [outgoing_path(workspace, txn.transfer_id), retired_path(workspace, txn.transfer_id)]
    held.append(exports_directory(workspace, txn.transfer_id) / txn.root.job_key)
    if not any(_lexists(path) for path in held):
        # Not at its placement, not in E/payload, not at a trusted committed
        # location: committed to client territory, unless E was renamed
        # meanwhile (E can never reappear, so a present E proves the commit).
        # The target itself proves nothing either way: anybody may create a
        # path in client territory, and its user may already have moved the job on.
        _txn._hook("phase.recheck")
        if not _lexists(eject):
            raise _PhaseChanged()
        if txn.commit.kind == "path" and txn.commit.path is not None and not _lexists(txn.commit.path):
            _LOGGER.info(
                "transfer %s committed to %s, which was moved on meanwhile",
                txn.transfer_id,
                txn.commit.path,
                extra={"event": "transfer_target_moved", "transfer_id": txn.transfer_id},
            )
    return committed_location(workspace, txn)


# ---------------------------------------------------------------------------
# Abort
# ---------------------------------------------------------------------------


def _job_directories(tree: Path) -> Iterator[tuple[PurePosixPath, str, Path]]:
    """Yield ``(placement, job_key, path)`` of every job directory below an envelope's ``tree/``."""

    pending = [(tree, PurePosixPath())]
    while pending:
        directory, placement = pending.pop()
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except (FileNotFoundError, NotADirectoryError):
            continue
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False):
                continue
            try:
                parse_job_key(entry.name)
            except FormatError:
                pending.append((Path(entry.path), placement / entry.name))
                continue
            yield placement, entry.name, Path(entry.path)


def _move_home(workspace: Workspace, source: Path, placement: PurePosixPath, job_key: str) -> None:
    home = workspace.payload_path(placement, job_key)
    if _lexists(home):
        if not _lexists(source):
            return  # another aborter moved it home first
        raise WorkspaceCorruptionError(
            f"cannot move {source} back: {home} is taken; both are left in place for an operator"
        )
    workspace.ensure_directory(home.parent)
    _txn.rename_verified(source, home)
    _sync(workspace, home.parent, source.parent)


def _abort_steps(workspace: Workspace, transfer_id: str, fenced: Fenced) -> str:
    """Abort steps (1)-(6): every payload of *T* back to its placement, then unfence, then trash ``A``.

    A committed addressed bundle is taken back from ``outgoing/<T>`` only with
    an operator's explicit reclaim authorization ``A/reclaim`` (:func:`reclaim`).
    The end of its freshness window proves only that no destination may accept
    it any more, not that none did (design 13.1: an in-doubt transfer is never
    resolved automatically). Without the authorization the bundle stays where
    it is, deliverable, retirable and reported, and the result is ``"committed"``.
    """

    abort = abort_directory(workspace, transfer_id)
    txn = None if fenced.root is None else Transaction.from_root(*fenced.root)
    root = fenced.root_identity()
    # (1) An addressed bundle being reclaimed comes back into the private directory first.
    outgoing = outgoing_path(workspace, transfer_id)
    if txn is not None and txn.addressed and _lexists(outgoing):
        if not _lexists(abort / RECLAIM):
            # Reported once per process; every recovery pass meets it again.
            level = logging.DEBUG if transfer_id in _IN_DOUBT_REPORTED else logging.WARNING
            _IN_DOUBT_REPORTED.add(transfer_id)
            _LOGGER.log(
                level,
                "transfer %s was decided aborted after it committed; its bundle stays in %s until it is "
                "acknowledged, retired, or reclaimed by an operator (`httk workflow transfer reclaim`)",
                transfer_id,
                outgoing,
                extra={"event": "transfer_abort_in_doubt", "transfer_id": transfer_id},
            )
            return "committed"
        _txn._hook("abort.reclaim")
        _txn.rename_verified(outgoing, abort / PAYLOAD)
        _sync(workspace, outgoing.parent, abort)
    # (2) The inverse of S5, while the payload is still private.
    carried = abort / PAYLOAD / TRANSFER_DIRECTORY
    if _lexists(carried) and not _lexists(abort / ENVELOPE) and not _lexists(abort / ENVELOPE_PARTIAL):
        _txn._hook("abort.envelope")
        _txn.rename_verified(carried, abort / ENVELOPE)
    # (3) Members back to their placements.
    for name in (ENVELOPE_PARTIAL, ENVELOPE):
        for placement, job_key, path in list(_job_directories(abort / name / TRANSFER_TREE)):
            _txn._hook("abort.member")
            _move_home(workspace, path, placement, job_key)
    # (4) The root back to its placement.
    if _lexists(abort / PAYLOAD):
        if root is None:
            raise WorkspaceCorruptionError(f"transfer {transfer_id}: {abort / PAYLOAD} names no root to return to")
        if txn is not None:
            _check_identity(abort / PAYLOAD, txn)
        _txn._hook("abort.root")
        _move_home(workspace, abort / PAYLOAD, root.placement, root.job_key)
    # (5) Unfence, members first, once every payload of T is back.
    if (
        txn is not None
        and txn.addressed
        and not _lexists(abort / PAYLOAD)
        and not _is_directory(_member_home(workspace, txn.root))
        and _lexists(retired_path(workspace, transfer_id))
    ):
        # The acknowledgement retired the bundle before this abort could take it back.
        _LOGGER.warning(
            "transfer %s was acknowledged before it could be reclaimed; it stays retired",
            transfer_id,
            extra={"event": "transfer_reclaim_lost", "transfer_id": transfer_id},
        )
        _remove_markers(workspace, fenced, root=True)
        _txn.trash(abort, control=workspace.control, holds_payload=_txn.holds_job_payload)
        return "retired"
    for marker, _frame in fenced.all():
        if not _is_directory(workspace.payload_path(marker.placement, marker.job_key)):
            raise WorkspaceCorruptionError(
                f"transfer {transfer_id}: job {marker.job_key} is not back at its placement; left fenced"
            )
    for marker, frame in fenced.all():
        _txn._hook("abort.unfence")
        try:
            _unfence(workspace, marker, frame)
        except TransitionLostError:
            continue
    # (6) Discard the abort directory, never a payload.
    _txn._hook("abort.trash")
    _txn.trash(abort, control=workspace.control, holds_payload=_txn.holds_job_payload)
    _LOGGER.info(
        "aborted transfer %s; its jobs are back at their placements",
        transfer_id,
        extra={"event": "transfer_aborted", "transfer_id": transfer_id},
    )
    return "aborted"


def _decide_abort(workspace: Workspace, transfer_id: str) -> bool:
    """Rename ``E`` to ``A``: the single decision that the transaction aborts; ``False`` when it was not ``E``."""

    _txn._hook("abort.decide")
    return _txn.rename_verified(eject_directory(workspace, transfer_id), abort_directory(workspace, transfer_id))


# ---------------------------------------------------------------------------
# The phase reader
# ---------------------------------------------------------------------------


def _owner_gone(workspace: Workspace, fenced: Fenced, now: int) -> bool:
    recorded = fenced.owner()
    if recorded is None:
        return True
    owner, started = recorded
    try:
        return _txn.owner_gone(
            workspace.control, owner, since=started, now=now, lease_seconds=workspace.policy.lease_seconds
        )
    except FormatError:
        return True


def settle(workspace: Workspace, transfer_id: str, *, force: bool = False, now: int | None = None) -> str:
    """Read the phase of one transaction from fixed names and move it on (the phase reader).

    * ``E`` present (forward): a fenced root continues forward through S7 (or
      waits for the acknowledgement of an addressed transfer); without a fenced
      root the owner died before S3, and the transaction aborts.
    * ``A`` present (abort): the abort steps run, unless the root payload is in
      none of ``outgoing/<T>``, ``A/payload`` or its placement — then the commit
      (or acknowledgement) happened before the abort decision, and only the
      clean-up runs, never an unfence.
    * neither: no payload of *T* can move any more; a marker whose payload is at
      its placement is unfenced, an addressed root whose bundle is retired is
      removed, anything else is reported.

    Forward and abort work is throttled by the owner: it runs only when *force*
    is set (the owner itself, an operator) or the recorded owner is evidently gone.

    :param workspace: The source workspace.
    :param transfer_id: The transaction.
    :param force: Act whatever the owner's liveness.
    :param now: The current time in integer UTC nanoseconds (for the owner rule).
    :return: ``"none"`` (nothing fenced), ``"pending"`` (owner alive), ``"committed"``
        (an addressed bundle waits for its acknowledgement), ``"ejected"``, ``"aborted"``,
        ``"retired"``, ``"unfenced"`` or ``"stuck"`` (reported); ``"committed"`` also for an
        addressed bundle decided aborted after its commit but not reclaimed by an operator.
    :raises httk.workflow.errors.WorkspaceCorruptionError: If an identity check fails.
    """

    clock = time.time_ns() if now is None else now
    for _attempt in range(16):
        fenced = fenced_under(workspace, transfer_id)
        eject = eject_directory(workspace, transfer_id)
        abort = abort_directory(workspace, transfer_id)
        if not fenced.all():
            if _lexists(abort):
                # Decided aborted and nothing fenced (any more): only the directory is left.
                _txn.trash(abort, control=workspace.control, holds_payload=_txn.holds_job_payload)
            return "none"
        if _lexists(eject):
            if not force and not _owner_gone(workspace, fenced, clock):
                return "pending"
            if fenced.root is None:
                _decide_abort(workspace, transfer_id)
                continue
            forward = Transaction.from_root(*fenced.root)
            try:
                _forward(workspace, forward)
            except _PhaseChanged:
                continue
            except _Abort as exc:
                _LOGGER.warning(
                    "transfer %s aborts: %s",
                    transfer_id,
                    exc,
                    extra={"event": "transfer_abort", "transfer_id": transfer_id},
                )
                _decide_abort(workspace, transfer_id)
                continue
            return "committed" if forward.addressed else "ejected"
        if _lexists(abort):
            if not force and not _owner_gone(workspace, fenced, clock):
                return "pending"
            root = fenced.root_identity()
            txn = None if fenced.root is None else Transaction.from_root(*fenced.root)
            # Probed in the order a payload moves (outgoing/<T> -> A/payload -> home):
            # one that moves on between two probes is still found at a later one.
            held_back = []
            if txn is not None and txn.addressed:
                held_back.append(outgoing_path(workspace, transfer_id))
            held_back.append(abort / PAYLOAD)
            if root is not None:
                held_back.append(workspace.payload_path(root.placement, root.job_key))
            present = False
            for path in held_back:
                _txn._hook("phase.probe")
                if _lexists(path):
                    present = True
                    break
            if root is None or present:
                return _abort_steps(workspace, transfer_id, fenced)
            # The commit (or the acknowledgement) came first: clean up, never unfence.
            _remove_markers(workspace, fenced, root=True)
            _txn.trash(abort, control=workspace.control, holds_payload=_txn.holds_job_payload)
            return "retired" if txn is not None and txn.addressed else "ejected"
        return _settle_neither(workspace, transfer_id, fenced)
    _LOGGER.error(
        "transfer %s keeps changing phase; left for a later recovery",
        transfer_id,
        extra={"event": "transfer_unsettled", "transfer_id": transfer_id},
    )
    return "stuck"


def _settle_neither(workspace: Workspace, transfer_id: str, fenced: Fenced) -> str:
    """Neither ``E`` nor ``A``: unfence stale fences whose payload is home; report anything else.

    An addressed root whose bundle is at ``outgoing/<T>`` is committed and waits
    for its acknowledgement (``"committed"``); :func:`reclaim` can still take it back.
    """

    txn = None if fenced.root is None else Transaction.from_root(*fenced.root)
    outcome = "unfenced"
    for marker, frame in fenced.all():
        if _is_directory(workspace.payload_path(marker.placement, marker.job_key)):
            try:
                _unfence(workspace, marker, frame)
            except TransitionLostError:
                pass
            continue
        root = txn is not None and txn.addressed and marker.job_id == txn.root.job_id
        if root and _lexists(retired_path(workspace, transfer_id)):
            _remove_markers(workspace, Fenced(transfer_id, fenced.root, {}), root=True)
            outcome = "retired"
            continue
        if root and _lexists(outgoing_path(workspace, transfer_id)):
            # Committed and deliverable (its transaction directory was discarded
            # meanwhile): it waits for its acknowledgement, or a reclaim.
            if outcome == "unfenced":
                outcome = "committed"
            continue
        _LOGGER.error(
            "job %s is transferring under %s, but neither its transaction directory nor its payload exists",
            marker.job_key,
            transfer_id,
            extra={"event": "transfer_orphan_marker", "transfer_id": transfer_id, "job_key": marker.job_key},
        )
        outcome = "stuck"
    return outcome


# ---------------------------------------------------------------------------
# The transaction, start to end
# ---------------------------------------------------------------------------


def seal(
    workspace: Workspace,
    root: Marker,
    commit: Commit,
    *,
    transfer_id: str | None = None,
    destination_workspace_id: str | None = None,
    destination_remote: str | None = None,
    destination_placement: str | PurePosixPath | None = None,
    waiting_parent_map: Mapping[str, set[str]] | None = None,
    with_tree: bool = False,
    owner: str | None = None,
    reason: str = "transfer",
) -> Path:
    """Run one sealing transaction S0-S7 and return where it committed.

    :param workspace: The source workspace.
    :param root: The root job's current marker.
    :param commit: Where the transaction commits.
    :param transfer_id: The transaction id (a fresh one when omitted).
    :param destination_workspace_id: The addressed workspace (``"outgoing"`` commits only).
    :param destination_remote: The destination's remote name.
    :param destination_placement: Where the destination publishes the root (default: its placement).
    :param waiting_parent_map: A precomputed waiting-parent map.
    :param with_tree: For an addressed transfer, seal a job whose bound children follow it.
    :param owner: The owner token (this CLI process when omitted).
    :param reason: The ``reason`` recorded in the fencing frames.
    :return: The committed location.
    :raises ValueError: If the job or its tree cannot leave, or the transaction aborted.
    :raises FileExistsError: If the target already exists.
    :raises httk.workflow.errors.FormatError: If a placement names a job directory or a payload holds its own envelope.
    """

    from .transfers import _waiting_parent_map

    workspace._require_unsealed()
    identifier = str(uuid.uuid4()) if transfer_id is None else canonical_uuid(transfer_id, "transfer_id")
    placement = normalize_placement(root.placement if destination_placement is None else destination_placement)
    waiting = _waiting_parent_map(workspace) if waiting_parent_map is None else waiting_parent_map
    now = time.time_ns()
    members = _precheck(
        workspace, root, commit, waiting=waiting, with_tree=with_tree, destination_placement=placement, now=now
    )
    txn = Transaction(
        transfer_id=identifier,
        root=TxnJob(root.job_id, root.job_key, root.placement),
        members=tuple(
            TxnJob(member.marker.job_id, member.marker.job_key, member.marker.placement, member.parent_job_id)
            for member in members
        ),
        commit=commit,
        destination_workspace_id=destination_workspace_id,
        destination_remote=destination_remote,
        destination_placement=placement,
        owner=_txn.owner_token() if owner is None else owner,
        started_at=now,
    )
    _txn._hook("S0.checked")
    # S1
    if not _txn.birth(
        workspace.control / "tmp",
        f"eject.{identifier}",
        _skeleton(txn.members, txn.jobs, _runner_parents(workspace, txn.jobs)),
        durable=workspace.durable,
    ):
        raise ValueError(f"transfer {identifier} already exists")
    _txn._hook("S1.born")
    try:
        # S2: members in job-id order, so two ejectors of one tree meet at the first.
        for member in sorted(members, key=lambda item: item.marker.job_id):
            _txn._hook("S2.member")
            _fence(workspace, member.marker, txn.member_outgoing(), reason=reason)
        # S3: the root, recording the whole transaction and its payload's inode.
        _txn._hook("S3.root")
        home = workspace.payload_path(root.placement, root.job_key)
        txn = replace(txn, sealed_at=time.time_ns(), payload_inode=os.lstat(home).st_ino)
        _fence(workspace, root, txn.root_outgoing(), reason=reason)
    except (TransitionLostError, WorkflowError, OSError) as exc:
        _LOGGER.info(
            "transfer %s could not fence its jobs (%s); aborting it",
            identifier,
            exc,
            extra={"event": "transfer_fence_lost", "transfer_id": identifier},
        )
        _decide_abort(workspace, identifier)
        settle(workspace, identifier, force=True)
        raise ValueError(
            f"job {root.job_key} or a member of its tree changed while it was being fenced: {exc}"
        ) from exc
    _txn._hook("S3.fenced")
    try:
        return _forward(workspace, txn)
    except _PhaseChanged:
        outcome = settle(workspace, identifier, force=True)
    except _Abort as exc:
        _LOGGER.warning(
            "transfer %s aborts: %s", identifier, exc, extra={"event": "transfer_abort", "transfer_id": identifier}
        )
        _decide_abort(workspace, identifier)
        settle(workspace, identifier, force=True)
        raise ValueError(f"transfer of {root.job_key} was aborted ({exc}); the job is back at its placement") from exc
    if outcome in {"ejected", "committed"}:
        return committed_location(workspace, txn)
    if outcome == "aborted":
        raise ValueError(f"transfer of {root.job_key} was aborted; the job is back at its placement")
    raise ValueError(f"transfer {identifier} of {root.job_key} did not complete ({outcome}); recovery finishes it")


# ---------------------------------------------------------------------------
# Recovery and the orphan sweep (4.7 steps 2 and 3)
# ---------------------------------------------------------------------------


def recover(workspace: Workspace, *, now: int | None = None) -> list[dict[str, object]]:
    """Settle every transaction whose owner is gone, then sweep orphaned transaction directories.

    :param workspace: The source workspace.
    :param now: The current time in integer UTC nanoseconds.
    :return: One ``{transfer_id, status}`` record per transaction looked at.
    """

    clock = time.time_ns() if now is None else now
    results: list[dict[str, object]] = []
    groups = fenced_transactions(workspace)
    for transfer_id in sorted(groups):
        try:
            status = settle(workspace, transfer_id, now=clock)
        except (WorkflowError, OSError) as exc:
            _LOGGER.error(
                "cannot recover transfer %s: %s",
                transfer_id,
                exc,
                extra={"event": "transfer_recovery_failed", "transfer_id": transfer_id},
            )
            status = "failed"
        results.append({"transfer_id": transfer_id, "status": status})
    _sweep(workspace, set(groups), clock)
    return results


def _sweep(workspace: Workspace, fenced: set[str], now: int) -> None:
    """The orphan sweep: every action is safe even if the marker listing missed something."""

    tmp = workspace.control / "tmp"
    try:
        names = sorted(os.listdir(tmp))
    except FileNotFoundError:
        return
    cutoff = now / 1e9 - ORPHAN_SECONDS
    for name in names:
        kind, _, transfer_id = name.partition(".")
        path = tmp / name
        try:
            committed = kind in {"eject", "abort"} and any(
                _lexists(held)
                for held in (outgoing_path(workspace, transfer_id), exports_directory(workspace, transfer_id))
            )
            if committed and transfer_id not in fenced:
                # Committed to a trusted location (by fixed name): never an orphan,
                # whatever the marker listing missed; its own phase reader finishes it.
                continue
            if kind == "eject" and transfer_id not in fenced:
                if os.lstat(path).st_mtime < cutoff:
                    # An abort is always safe; a missed marker can only cause a spurious one.
                    _txn.rename_verified(path, tmp / f"abort.{transfer_id}")
                    _LOGGER.warning(
                        "aborted orphaned transfer directory %s",
                        path,
                        extra={"event": "transfer_orphan_aborted", "transfer_id": transfer_id},
                    )
            elif kind == "abort" and transfer_id not in fenced or kind == "birth" and os.lstat(path).st_mtime < cutoff:
                _txn.trash(path, control=workspace.control, holds_payload=_txn.holds_job_payload)
        except FileNotFoundError:
            continue
        except OSError as exc:
            _LOGGER.warning("cannot sweep %s: %s", path, exc, extra={"event": "transfer_sweep_failed"})


# ---------------------------------------------------------------------------
# Addressed transfers after commit (4.6)
# ---------------------------------------------------------------------------


def retire(workspace: Workspace, txn: Transaction) -> Path | None:
    """Retire an acknowledged addressed bundle: ``outgoing/<T>`` to ``retired/<T>``, then clean up.

    The root marker is removed and ``E`` trashed only after the rename was
    positively observed (or ``retired/<T>`` is there). With neither
    ``outgoing/<T>`` nor ``retired/<T>`` present nothing is decided: the bundle
    may be in flight to ``A/payload`` (a reclaim), or back at its placement.

    :param workspace: The source workspace.
    :param txn: The addressed transaction.
    :return: ``retired/<T>``, or ``None`` when nothing could be retired now.
    """

    outgoing = outgoing_path(workspace, txn.transfer_id)
    retired = retired_path(workspace, txn.transfer_id)
    retired.parent.mkdir(parents=True, exist_ok=True)
    _txn._hook("ack.retire")
    moved = _txn.rename_verified(outgoing, retired)
    if moved:
        _sync(workspace, outgoing.parent, retired.parent)
    elif not _lexists(retired):
        if _lexists(_member_home(workspace, txn.root)):
            _LOGGER.warning(
                "transfer %s was acknowledged, but its job is back at its placement (reclaimed): in doubt",
                txn.transfer_id,
                extra={"event": "transfer_ack_in_doubt", "transfer_id": txn.transfer_id},
            )
        return None
    _finish_retirement(workspace, txn)
    return retired


def _finish_retirement(workspace: Workspace, txn: Transaction) -> None:
    """After ``retired/<T>`` is observed: remove the root marker, then trash ``E``."""

    fenced = fenced_under(workspace, txn.transfer_id)
    if fenced.root is not None and fenced.root[0].job_id == txn.root.job_id:
        _remove_markers(workspace, Fenced(txn.transfer_id, fenced.root, {}), root=True)
    _txn._hook("ack.trash")
    _txn.trash(
        eject_directory(workspace, txn.transfer_id), control=workspace.control, holds_payload=_txn.holds_job_payload
    )


def reclaim(workspace: Workspace, txn: Transaction, *, now: int | None = None) -> str:
    """Take an undelivered addressed bundle back: abort it once it can no longer be accepted anywhere.

    This is the operator's explicit decision that the bundle was not delivered
    (design 13.1): nothing else ever takes a committed bundle back. The abort is
    decided by renaming ``E`` to ``A``; when neither exists any more (the
    transaction directory was discarded while the bundle waited in
    ``outgoing/<T>``), an empty ``A`` is born instead, since no forward step can
    run without ``E``. The authorization is then recorded durably as
    ``A/reclaim``, and the abort steps (also a later recovery's, after a crash)
    move ``outgoing/<T>`` back by one single-source rename, which an
    acknowledgement's rename to ``retired/<T>`` arbitrates.

    :param workspace: The source workspace.
    :param txn: The addressed transaction.
    :param now: The current time in integer UTC nanoseconds.
    :return: The phase reader's outcome: ``"aborted"`` when the job is back, ``"retired"``
        when the acknowledgement won, anything else when it could not finish now.
    :raises ValueError: If the bundle may still be accepted (before ``sealed_at + W + S``).
    """

    clock = time.time_ns() if now is None else now
    deadline = txn.sealed_at + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS
    if clock <= deadline:
        raise ValueError(
            f"transfer {txn.transfer_id} may still be delivered until {deadline} ns; "
            "retire it if the destination has it, or reclaim it after that"
        )
    if (
        not _decide_abort(workspace, txn.transfer_id)
        and not _lexists(abort_directory(workspace, txn.transfer_id))
        and _lexists(outgoing_path(workspace, txn.transfer_id))
    ):
        _txn.birth(
            workspace.control / "tmp", f"abort.{txn.transfer_id}", lambda _directory: None, durable=workspace.durable
        )
    _txn._hook("reclaim.authorize")
    authorization = json_bytes({"reclaimed_at": clock, "transfer_id": txn.transfer_id}) + b"\n"
    try:
        _txn.link_new(abort_directory(workspace, txn.transfer_id), RECLAIM, authorization, durable=workspace.durable)
    except FileNotFoundError:
        pass  # A was discarded meanwhile (the acknowledgement won): the phase reader says so
    # The phase reader runs the abort steps (moving outgoing/<T> back first), or
    # only the clean-up when an acknowledgement retired the bundle before the decision.
    return settle(workspace, txn.transfer_id, force=True, now=clock)


# ---------------------------------------------------------------------------
# Export copy-out (4.6, X1-X6)
# ---------------------------------------------------------------------------


def _copy_record(exports: Path) -> dict[str, str]:
    value = json.loads(_read_regular_file(exports / COPY_TO, 1 << 16))
    if not isinstance(value, dict) or not all(isinstance(value.get(k), str) for k in ("destination", "job_key")):
        raise FormatError(f"{exports / COPY_TO} is malformed")
    return {"destination": value["destination"], "job_key": value["job_key"]}


def _holds_transfer(workspace: Workspace, path: Path, transfer_id: str, payload_sha256: str | None) -> bool:
    try:
        verified = verify_bundle(path, workspace=workspace, source="export")
    except (FormatError, OSError):
        return False
    return verified.transfer_id == transfer_id and payload_sha256 in (None, verified.manifest.payload_sha256)


def _bundle_digest(bundle: Path) -> str | None:
    manifest = _read_envelope_manifest(bundle / TRANSFER_DIRECTORY)
    return None if manifest is None else manifest.payload_sha256


def copy_out(workspace: Workspace, transfer_id: str, *, owner: str | None = None) -> Path:
    """Copy one held export to its destination on the other filesystem (X1-X6), or finish a taken-over one.

    :param workspace: The source workspace.
    :param transfer_id: The export's transaction id.
    :param owner: The owner token (this CLI process when omitted).
    :return: The destination directory.
    :raises FileExistsError: If the destination is taken by something else; the bundle stays held.
    :raises ValueError: If another actor took the held bundle (an adoption, another copy-out, or a
        recovery that found an earlier copy-out may have delivered it).
    :raises httk.workflow.errors.FormatError: If the copy does not verify; the bundle stays held.
    """

    token = _txn.owner_token() if owner is None else owner
    exports = exports_directory(workspace, transfer_id)
    record = _copy_record(exports)
    held = exports / record["job_key"]
    now = time.time_ns()
    staging = workspace.control / "tmp" / f"export.{token}.{_txn.format_ns(now)}.{transfer_id}"
    # X1
    os.mkdir(staging, 0o755)
    _txn.link_new(staging, COPY_TO, json_bytes(record) + b"\n", durable=workspace.durable)
    _txn._hook("X1.staged")
    # X2
    if not _txn.rename_verified(held, staging / "bundle"):
        _txn.trash(staging, control=workspace.control, holds_payload=_txn.holds_job_payload)
        raise ValueError(f"the exported job {record['job_key']} was taken by another actor")
    _sync(workspace, exports, staging)
    _txn._hook("X2.claimed")
    return _finish_copy_out(workspace, staging, transfer_id, record)


def _in_doubt_message(held: Path, record: Mapping[str, str]) -> str:
    return (
        f"the exported job {record['job_key']} may already have been delivered to {record['destination']} by an "
        f"interrupted copy-out; it stays held in {held}: remove that copy, or take it back with `httk job adopt {held}`"
    )


def _finish_copy_out(workspace: Workspace, staging: Path, transfer_id: str, record: Mapping[str, str]) -> Path:
    """X3-X6 from a staging directory that holds the claimed bundle.

    Before the publishing rename (X5) the owner links the witness
    ``publishing`` into its staging directory. That name is the fence: once a
    takeover renamed the staging directory the link fails and the owner stops,
    and a later owner that finds the witness never publishes again (the outcome
    is in doubt, :func:`resume_copy_outs`). The publication itself renames a
    temporary outside the workspace, which no takeover can fence.
    """

    destination = Path(record["destination"])
    bundle = staging / "bundle"
    digest = _bundle_digest(bundle)
    token = staging.name.removeprefix("export.").rsplit(".", 1)[0]
    temporary = _export_temporary(destination, token)
    # X3
    if _lexists(temporary):
        _txn.remove_tree(temporary)
    shutil.copytree(bundle, temporary, symlinks=True)
    if workspace.durable:
        fsync_tree(temporary)
    _txn._hook("X3.copied")
    # X4
    if not _holds_transfer(workspace, temporary, transfer_id, digest):
        _txn.remove_tree(temporary)
        raise FormatError(f"the copy of exported job {record['job_key']} does not verify; it stays held")
    _txn._hook("X4.verified")
    # X5: the witness first, fenced by the staging name; then the publishing rename.
    try:
        witnessed = _txn.link_new(
            staging,
            PUBLISHING,
            json_bytes({"destination": str(destination), "temporary": str(temporary)}) + b"\n",
            durable=workspace.durable,
        )
    except FileNotFoundError:
        witnessed = False
    if not witnessed:
        _txn.remove_tree(temporary)
        raise ValueError(f"the copy-out of exported job {record['job_key']} was taken over by another actor")
    _txn._hook("X5.witnessed")
    try:
        moved = _txn.rename_verified(temporary, destination)
    except OSError as exc:
        if exc.errno not in _COMMIT_REFUSALS - {errno.EXDEV}:
            raise
        _txn.remove_tree(temporary)
        if not _holds_transfer(workspace, destination, transfer_id, digest):
            exports = exports_directory(workspace, transfer_id)
            _txn.rename_verified(bundle, exports / record["job_key"])
            _txn.trash(staging, control=workspace.control, holds_payload=_txn.holds_job_payload)
            raise FileExistsError(
                f"export destination {destination} was taken meanwhile; the job stays held in {exports}"
            ) from exc
        moved = True
    if not moved:
        # The temporary vanished: a takeover removed it and found this owner's witness.
        raise ValueError(f"the copy-out of exported job {record['job_key']} was taken over by another actor")
    _sync(workspace, destination.parent)
    _txn._hook("X5.published")
    # X6
    _txn.trash(staging, control=workspace.control, holds_payload=None)
    exports = exports_directory(workspace, transfer_id)
    if _lexists(exports) and not _lexists(exports / record["job_key"]):
        _txn.trash(exports, control=workspace.control, holds_payload=_txn.holds_job_payload)
    return destination


def resume_copy_outs(workspace: Workspace, *, owner: str | None = None, now: int | None = None) -> list[Path]:
    """Re-run every pending copy-out: held exports, and copy-out staging whose owner is gone.

    :param workspace: The source workspace.
    :param owner: The owner token (this CLI process when omitted).
    :param now: The current time in integer UTC nanoseconds.
    :return: The destinations that now hold their job.
    """

    token = _txn.owner_token() if owner is None else owner
    clock = time.time_ns() if now is None else now
    done: list[Path] = []
    tmp = workspace.control / "tmp"
    stagings: dict[str, Path] = {}
    try:
        names = sorted(os.listdir(tmp))
    except FileNotFoundError:
        names = []
    for name in names:
        if not name.startswith("export."):
            continue
        parts = name.split(".")
        if len(parts) != 4:
            continue
        _, staged_owner, started, transfer_id = parts
        stagings[transfer_id] = tmp / name
        try:
            since = _txn.parse_ns(started)
            gone = _txn.owner_gone(
                workspace.control, staged_owner, since=since, now=clock, lease_seconds=workspace.policy.lease_seconds
            )
        except FormatError:
            gone = True
        if not gone:
            continue
        mine = tmp / f"export.{token}.{_txn.format_ns(clock)}.{transfer_id}"
        if not _txn.rename_verified(tmp / name, mine):
            continue
        stagings[transfer_id] = mine
        try:
            record = _copy_record(mine)
            destination = Path(record["destination"])
            witness = _read_witness(mine)
            # The copy a previous owner may still publish: the one its witness names
            # (carried along by every takeover rename of the staging directory), else
            # the previous owner's by its exact name. It is renamed away first, so a
            # stale owner's publishing rename and this removal cannot both happen.
            _txn._hook("copyout.cleanup")
            _discard_temporary(
                Path(witness["temporary"])
                if witness is not None
                else _export_temporary(destination, f"{staged_owner}.{started}")
            )
            if _lexists(mine / "bundle"):
                if _holds_transfer(workspace, destination, transfer_id, _bundle_digest(mine / "bundle")):
                    # Published before the owner died (X5): only X6 is left.
                    _txn.trash(mine, control=workspace.control, holds_payload=None)
                    exports = exports_directory(workspace, transfer_id)
                    if _lexists(exports) and not _lexists(exports / record["job_key"]):
                        _txn.trash(exports, control=workspace.control, holds_payload=_txn.holds_job_payload)
                    done.append(destination)
                elif witness is not None or _lexists(mine / PUBLISHING):
                    # The previous owner may have published, and the destination may have
                    # been taken away since: copying again could deliver the job twice.
                    _hold_in_doubt(workspace, mine, transfer_id, record)
                else:
                    done.append(_finish_copy_out(workspace, mine, transfer_id, record))
            else:
                _txn.trash(mine, control=workspace.control, holds_payload=_txn.holds_job_payload)
        except (WorkflowError, OSError, ValueError) as exc:
            _LOGGER.warning("cannot resume copy-out %s: %s", transfer_id, exc, extra={"event": "export_pending"})
    exports_root = workspace.control / "transfers" / "exports"
    try:
        held = sorted(os.listdir(exports_root))
    except FileNotFoundError:
        held = []
    for transfer_id in held:
        exports = exports_root / transfer_id
        try:
            record = _copy_record(exports)
        except (OSError, ValueError, FormatError) as exc:
            _LOGGER.warning("cannot read export %s: %s", exports, exc, extra={"event": "export_pending"})
            continue
        if _lexists(exports / record["job_key"]):
            try:
                done.append(copy_out(workspace, transfer_id, owner=token))
            except (WorkflowError, OSError, ValueError) as exc:
                _LOGGER.warning("cannot copy out export %s: %s", transfer_id, exc, extra={"event": "export_pending"})
        elif transfer_id not in stagings and not _lexists(eject_directory(workspace, transfer_id)):
            # Copied out (or adopted back) already: only the record is left.
            _txn.trash(exports, control=workspace.control, holds_payload=_txn.holds_job_payload)
    for entry in _in_doubt_entries(workspace):
        if not entry["present"]:
            # The operator resolved it (removed the held copy, or adopted it back).
            _txn.trash(
                in_doubt_directory(workspace, entry["transfer_id"]),
                control=workspace.control,
                holds_payload=_txn.holds_job_payload,
            )
    return done


def _read_witness(staging: Path) -> dict[str, str] | None:
    """Read a staging directory's ``publishing`` witness; an unreadable one still counts as present."""

    try:
        value = json.loads(_read_regular_file(staging / PUBLISHING, 1 << 16))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise WorkspaceCorruptionError(f"{staging / PUBLISHING} cannot be read: {exc}") from exc
    if not isinstance(value, dict) or not all(isinstance(value.get(k), str) for k in ("destination", "temporary")):
        raise WorkspaceCorruptionError(f"{staging / PUBLISHING} is malformed")
    return {"destination": value["destination"], "temporary": value["temporary"]}


def _export_temporary(destination: Path, token: str) -> Path:
    """The external temporary of the copy-out whose staging directory carries *token* (``<owner>.<ns>``).

    A sibling of the destination named independently of the destination's
    (client-chosen) name, so it stays well below ``NAME_MAX`` whatever that is.
    """

    return destination.parent / f".httk-export.{token}"


def _discard_temporary(temporary: Path) -> None:
    """Remove a copy-out's external temporary: renamed to a unique name first, then removed.

    The rename is single-source against the previous owner's publishing rename
    of the same temporary: exactly one of them moves it, so a removal can never
    run under a copy that is being published.
    """

    cleanup = temporary.with_name(f".httk-export-cleanup-{uuid.uuid4().hex}")
    try:
        moved = _txn.rename_verified(temporary, cleanup)
    except FileNotFoundError:
        return  # its directory is gone: nothing to discard
    if moved:
        _txn.remove_tree(cleanup)


def _hold_in_doubt(workspace: Workspace, staging: Path, transfer_id: str, record: Mapping[str, str]) -> None:
    """Move an ambiguous copy-out's bundle to ``transfers/in-doubt/<T>/<key>`` with its record, and report it.

    That path is never claimed by an ordinary copy-out, so no pending or later
    copy-out can publish the bundle again.
    """

    directory = in_doubt_directory(workspace, transfer_id)
    directory.parent.mkdir(parents=True, exist_ok=True)
    document = json_bytes({**record, "transfer_id": transfer_id, "recorded_at": time.time_ns()}) + b"\n"

    def build(path: Path) -> None:
        (path / IN_DOUBT).write_bytes(document)

    _txn.birth(workspace.control / "tmp", transfer_id, build, durable=workspace.durable, parent=directory.parent)
    held = directory / record["job_key"]
    _txn.rename_verified(staging / "bundle", held)
    _sync(workspace, directory, staging)
    _txn.trash(staging, control=workspace.control, holds_payload=_txn.holds_job_payload)
    exports = exports_directory(workspace, transfer_id)
    if _lexists(exports) and not _lexists(exports / record["job_key"]):
        _txn.trash(exports, control=workspace.control, holds_payload=_txn.holds_job_payload)
    _LOGGER.warning(_in_doubt_message(held, record), extra={"event": "export_in_doubt", "transfer_id": transfer_id})


def _in_doubt_entries(workspace: Workspace) -> list[dict[str, Any]]:
    root = workspace.control / "transfers" / "in-doubt"
    try:
        names = sorted(os.listdir(root))
    except FileNotFoundError:
        return []
    result: list[dict[str, Any]] = []
    for transfer_id in names:
        directory = root / transfer_id
        try:
            value = json.loads(_read_regular_file(directory / IN_DOUBT, 1 << 16))
            record = {"destination": str(value["destination"]), "job_key": str(value["job_key"])}
        except (OSError, ValueError, TypeError, KeyError) as exc:
            _LOGGER.warning("cannot read %s: %s", directory, exc, extra={"event": "export_pending"})
            continue
        held = directory / record["job_key"]
        result.append({"transfer_id": transfer_id, **record, "held": str(held), "present": _lexists(held)})
    return result


def exports_in_doubt(workspace: Workspace) -> list[dict[str, str]]:
    """Return every export whose earlier copy-out may already have delivered it (never copied out again).

    :param workspace: The source workspace.
    :return: One ``{transfer_id, job_key, destination, held}`` record per held bundle in doubt.
    """

    return [
        {name: str(entry[name]) for name in ("transfer_id", "destination", "job_key", "held")}
        for entry in _in_doubt_entries(workspace)
        if entry["present"]
    ]


def pending_outgoing(workspace: Workspace) -> list[tuple[Marker, dict[str, Any], Transaction]]:
    """Return every addressed transaction root: the jobs this workspace sealed for another one.

    :param workspace: The source workspace.
    :return: The root marker, its frame and its transaction, per transfer.
    """

    result = []
    for transfer_id, group in sorted(fenced_transactions(workspace).items()):
        if group.root is None:
            continue
        try:
            txn = Transaction.from_root(*group.root)
        except FormatError as exc:
            _LOGGER.warning("transfer %s: %s", transfer_id, exc, extra={"event": "transfer_frame_unknown"})
            continue
        if txn.addressed:
            result.append((group.root[0], group.root[1], txn))
    return result
