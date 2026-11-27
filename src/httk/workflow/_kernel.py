"""The job kernel: every cross-actor protocol rule of a workspace, on top of :mod:`httk.workflow._fs`.

It owns the names (``<job-key>~p<NNN>~<t>[~<from>]``), owners and their
fail-stop, claims, the release rule, recovery of dead owners, the request
files, scratch directories and their reconcilers, the exchange index and the shared-memory sweep. Layer modules decide *what* to do
with an :class:`OwnedJob`; this module decides how a job changes hands.

Every rename and removal goes through :mod:`~httk.workflow._fs`: contested
moves through ``move_once``, moves only the caller can make through
``move_owned``, and names created at most once through ``publish_dir``. No
decision here depends on a clock; time only paces visibility retries and dates
records.
"""

import contextlib
import contextvars
import dataclasses
import enum
import errno
import itertools
import json
import logging
import os
import re
import stat
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path, PurePosixPath
from types import MappingProxyType, TracebackType
from typing import Literal, NamedTuple, Protocol, Self

from httk.workflow import _death, _fs
from httk.workflow._job import MAX_JOB_BYTES
from httk.workflow._state import (
    MAX_STATE_BYTES,
    TERMINAL_STATES,
    UNOWNED_STATES,
    Release,
    StateDoc,
    decode_state,
    encode_state,
)
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.models import (
    canonical_uuid,
    make_job_key,
    normalize_placement,
    parse_job_key,
    placement_text,
    validate_label,
)

__all__ = [
    "DISCARDABLE_PURPOSES",
    "OWNED",
    "TERMINAL_STATES",
    "UNOWNED_STATES",
    "ExchangeClaim",
    "FirstScan",
    "JobHeader",
    "JobName",
    "JobRef",
    "KernelWorkspace",
    "ListingCache",
    "LocationIndex",
    "OwnedJob",
    "Owner",
    "OwnerDeclaredDead",
    "OwnerLost",
    "OwnerRecord",
    "RecoveryReport",
    "RecoverySealFailed",
    "Release",
    "ReleasedJobError",
    "StepRefused",
    "attest_dead",
    "check_placement",
    "claim",
    "claim_exchange_name",
    "drop_exchange_index",
    "exchange_index",
    "exchange_owner_key",
    "format_job_name",
    "list_jobs",
    "list_names",
    "list_owners",
    "listing_progress",
    "locate",
    "locate_many",
    "parse_job_name",
    "post_request",
    "probe_owner",
    "prune_empty_placements",
    "reconcile_scratch",
    "recover",
    "recoverable_owners",
    "register_owner",
    "register_reconciler",
    "request_file",
    "scan_final",
    "scan_first",
    "stage_request",
    "submit",
    "sweep_unrecorded_shm",
    "take",
]

_LOGGER = logging.getLogger(__name__)

#: The state of a job directory below ``jobs/owned/<owner-id>/``.
OWNED = "owned"
_OWNER_KINDS = ("manager", "cli", "daemon")
_ADVERTISED = frozenset({"pools", "capabilities", "prefixes", "resources", "end_time", "drain_start"})
_OWNER_ID = re.compile(r"[0-9a-f]{32}")
_TOKEN = re.compile(r"[a-z2-7]{16}")
_PRIORITY = re.compile(r"p([0-9]{3})")
_PURPOSE = re.compile(r"[a-z][a-z0-9-]{0,31}")
_SCRATCH = re.compile(r"([0-9a-f]{32})\.([a-z][a-z0-9-]{0,31})\.([a-z2-7]{16})")
_LAUNCH_N = re.compile(r"0|[0-9a-f]{32}")
_SHM_NAME = re.compile(r"httk-([0-9a-f]{32})")
_OWNER_TEMPORARY = re.compile(r"\.(owner|heartbeat|dead)\.json\.[a-z2-7]{16}\.tmp")
_STATE_TEMPORARY = re.compile(r"\.state\.json\.[a-z2-7]{16}\.tmp")
_LOG_TEMPORARY = re.compile(r"\..+\.[a-z2-7]{16}\.tmp")
_TRUSTED_NAMES = frozenset({"job.json", "state.json", "logs"})
_LOG_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
_EVIDENCE_KEYS = frozenset({"subject", "rule", "detail"})
#: A job may break its own trusted files (``chmod 000``) or its disk may fail: such a read is damage.
_DAMAGE_ERRNOS = frozenset({errno.EACCES, errno.EPERM, errno.EIO})
_RECORD_LIMIT = 1 << 20
_ROUNDS = 8
_SETTLE_STEP = 0.1
_SETTLE_CAP = 1.0
_NO_PLACEMENT = PurePosixPath()


class KernelWorkspace(Protocol):
    """What the kernel needs of a workspace: its directories, durability and visibility deadline.

    The members are read-only properties so that frozen values (and plain attributes) satisfy it.
    """

    @property
    def root(self) -> Path:
        """The workspace root."""
        ...

    @property
    def control(self) -> Path:
        """``<root>/.httk-workspace``."""
        ...

    @property
    def jobs(self) -> Path:
        """``<root>/jobs``."""
        ...

    @property
    def durable(self) -> bool:
        """Whether every move and write is fsynced."""
        ...

    @property
    def visibility_deadline(self) -> float:
        """Seconds a metadata change may take to become visible to every node."""
        ...


class OwnerLost(WorkflowError):
    """This process's ownership was revoked: the caller must fail-stop without touching any job."""


class OwnerDeclaredDead(OwnerLost):
    """This owner carries a tombstone, or its ``owner.json`` is missing or names another owner."""


class ReleasedJobError(WorkflowError):
    """An :class:`OwnedJob` handle was used after its job was released or discarded."""


class RecoverySealFailed(_fs.SyncFailed):
    """The seal recovery makes before anything else failed (plan WORKFLOW_LOCAL_COMMANDS amendment 27): nothing was
    returned, reconciled or removed, so the dead owner stays recoverable and the next probe retries. Any other error of
    a recovery, after it began to move things, propagates as it always has."""


class StepRefused(WorkflowError):
    """A batched step met what only the serial path handles (a planted ``logs/`` or ``runlog.jsonl``); the job is
    served serially instead (plan WORKFLOW_LOCAL_COMMANDS amendment 26)."""


#: The two kinds of owner obligation (plan WORKFLOW_LOCAL_COMMANDS amendment 26): ``owner`` gates the removal of
#: ``owner.json`` (moves out of ``owned/<owner-id>/``, their destinations, the trash, ``tmp/``); ``exposure`` gates
#: one job's exposure to a successor (its state replace, its request unlinks).
type ObligationKind = Literal["owner", "exposure"]


@dataclass
class _Obligation:
    """One directory that may hold an entry change no seal has covered yet.

    :param kind: Its kind.
    :param directory: The directory.
    :param job: The handle whose step it is, or ``None``.
    :param identity: The ``(st_dev, st_ino)`` of the descriptor the owner retains of a shared directory for the seal
        (the directory found at the path when the obligation was registered), or ``None`` to seal by path.
    :param finished: The owner's finish count once the mutation finished (however it ended), else ``None``.
    """

    kind: ObligationKind
    directory: Path
    job: "OwnedJob | None"
    identity: tuple[int, int] | None
    finished: int | None = None


#: Written durably beside ``owner.json`` before an owner's first batched mutation (plan WORKFLOW_LOCAL_COMMANDS
#: amendment 27): only the recovery of such an owner seals every placement directory. A dot name, so that ``gc``
#: removes it with the tombstone; it never ends in ``.tmp``, so that the sweep of write temporaries leaves it.
_BATCHED_MARKER = ".batched"


def _io_sync(workspace: "KernelWorkspace") -> object:
    # The io.sync setting as stored now (a workspace that has no settings, as the kernel's test doubles, is auto).
    reader = getattr(workspace, "io_sync", None)
    return reader() if callable(reader) else "auto"


def _io_workers(workspace: "KernelWorkspace") -> int:
    # The io.workers setting (1 for a workspace that has no settings).
    workers = getattr(workspace, "io_workers", 1)
    return workers if isinstance(workers, int) and not isinstance(workers, bool) and workers > 0 else 1


class JobName(NamedTuple):
    """The parsed name of a job directory.

    :param job_key: ``[<tag>--]<uuid>``.
    :param job_id: The job UUID.
    :param priority: ``0``–``999``.
    :param token: The fresh token of the move that created the name.
    :param from_state: The state an owned job was claimed from; ``None`` for an unowned name.
    """

    job_key: str
    job_id: str
    priority: int
    token: str
    from_state: str | None


def format_job_name(job_key: str, priority: int, token: str, from_state: str | None = None) -> str:
    """Return ``<job-key>~p<NNN>~<t>``, or ``<job-key>~p<NNN>~<t>~<from>`` for an owned job.

    :param job_key: The job key.
    :param priority: ``0``–``999``.
    :param token: A fresh token from :func:`httk.workflow._fs.fresh_token`.
    :param from_state: The unowned state an owned job was claimed from.
    :return: The directory name.
    :raises ValueError: If a component is malformed.
    """

    parse_job_key(job_key)
    if type(priority) is not int or not 0 <= priority <= 999:
        raise ValueError(f"priority must be an integer 0-999: {priority!r}")
    if not _TOKEN.fullmatch(token):
        raise ValueError(f"not a fresh token: {token!r}")
    if from_state is not None and from_state not in UNOWNED_STATES:
        raise ValueError(f"not an unowned state: {from_state!r}")
    name = f"{job_key}~p{priority:03d}~{token}"
    return name if from_state is None else f"{name}~{from_state}"


def parse_job_name(name: str) -> JobName:
    """Parse a job directory name of either grammar.

    :param name: The directory name.
    :return: Its components.
    :raises httk.workflow.errors.FormatError: If the name follows neither grammar.
    """

    # "~" never occurs in a job key (tags are [a-z0-9._-]), so splitting is unambiguous.
    parts = name.split("~")
    if len(parts) not in (3, 4):
        raise FormatError(f"not a job directory name: {name!r}")
    _, job_id = parse_job_key(parts[0])
    priority = _PRIORITY.fullmatch(parts[1])
    from_state = parts[3] if len(parts) == 4 else None
    if (
        priority is None
        or not _TOKEN.fullmatch(parts[2])
        or (from_state is not None and from_state not in UNOWNED_STATES)
    ):
        raise FormatError(f"not a job directory name: {name!r}")
    return JobName(parts[0], job_id, int(priority[1]), parts[2], from_state)


@dataclass(frozen=True)
class JobRef:
    """A job directory as last observed: a hint until a claim wins it.

    :param state: One of :data:`~httk.workflow._state.UNOWNED_STATES`, or :data:`~httk.workflow._kernel.OWNED`.
    :param placement: The placement of an unowned job; ``None`` for an owned one (read it from ``job.json``).
    :param job_key: The job key.
    :param job_id: The job UUID.
    :param priority: The priority in the name.
    :param token: The name's fresh token.
    :param owner_id: The owner of an owned job.
    :param from_state: The state an owned job was claimed from.
    :param path: The job directory.
    """

    state: str
    placement: PurePosixPath | None
    job_key: str
    job_id: str
    priority: int
    token: str
    owner_id: str | None
    from_state: str | None
    path: Path

    @classmethod
    def from_path(
        cls, path: Path, *, state: str, placement: PurePosixPath | None = None, owner_id: str | None = None
    ) -> Self:
        """Build the reference of a job directory from its name and location.

        :param path: The job directory.
        :param state: Its state.
        :param placement: Its placement (unowned jobs only).
        :param owner_id: Its owner (owned jobs only).
        :return: The reference.
        :raises httk.workflow.errors.FormatError: If the name does not fit the state.
        """

        name = parse_job_name(path.name)
        if state == OWNED:
            valid = owner_id is not None and _OWNER_ID.fullmatch(owner_id) and name.from_state and placement is None
        else:
            valid = state in UNOWNED_STATES and name.from_state is None and placement is not None and not owner_id
        if not valid:
            raise FormatError(f"{path} is not a job directory name of state {state!r}")
        return cls(
            state, placement, name.job_key, name.job_id, name.priority, name.token, owner_id, name.from_state, path
        )

    @property
    def cursor(self) -> str:
        """The ``start`` value of :func:`~httk.workflow._kernel.list_jobs` that resumes after this job."""

        return ((self.placement or PurePosixPath()) / self.path.name).as_posix()


@dataclass(frozen=True)
class JobHeader:
    """The members of ``job.json`` the kernel reads.

    :param job_id: The job UUID.
    :param job_key: The job key.
    :param tag: The job tag.
    :param placement: The authoritative, immutable placement.
    :param priority: The submitted priority.
    """

    job_id: str
    job_key: str
    tag: str | None
    placement: PurePosixPath
    priority: int


@dataclass(frozen=True)
class OwnerRecord:
    """One entry of ``owners/``.

    :param owner_id: The owner id.
    :param path: ``owners/<owner-id>/``.
    :param record: ``owner.json``, or ``None`` when absent or unreadable.
    :param tombstone: ``dead.json``, or ``None`` when absent or unreadable.
    """

    owner_id: str
    path: Path
    record: Mapping[str, object] | None
    tombstone: Mapping[str, object] | None


@dataclass(frozen=True)
class RecoveryReport:
    """What :func:`recover` did.

    :param dead_owner_id: The recovered owner.
    :param returned: Jobs this call returned to their ``from`` state.
    :param quarantined: Job directories this call moved to quarantine (unreadable ``job.json``).
    :param scratch: Scratch directories this call took and reconciled.
    :param kept_scratch: Taken scratch directories whose reconciler could not finish.
    :param launches_discarded: Launch directories this call discarded.
    """

    dead_owner_id: str
    returned: tuple[JobRef, ...]
    quarantined: tuple[Path, ...]
    scratch: tuple[Path, ...]
    kept_scratch: tuple[Path, ...]
    launches_discarded: int


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _pause(seconds: float) -> None:
    # Visibility pacing only; never an input to a decision.
    time.sleep(seconds)


#: What :func:`listing_progress` was given in this context: told once after each directory listed.
_LISTED: contextvars.ContextVar[Callable[[], object] | None] = contextvars.ContextVar("httk_listed", default=None)


@contextlib.contextmanager
def listing_progress(tell: Callable[[], object]) -> Iterator[None]:
    """Call *tell* once after each directory :func:`list_names` (or a walk of placements) lists in this context, and
    in the worker threads of :mod:`httk.workflow._pool` it starts (which run in a copy of it; other threads do not
    see it): a transfer session's far side reports it as progress, so that a slow listing of a large placement is not
    taken for a stalled request.

    :param tell: Called with no arguments, after the listing (a missing directory counts as listed); an exception it
        raises is ignored, so that it never changes a listing's result.
    :return: A context manager.
    """

    token = _LISTED.set(tell)
    try:
        yield
    finally:
        _LISTED.reset(token)


def _listed() -> None:
    if (tell := _LISTED.get()) is not None:
        with contextlib.suppress(Exception):  # an observer never masks the listing's result
            tell()


def list_names(directory: Path) -> list[str]:
    """List a directory's entry names, sorted; a missing directory (or a non-directory) has none.

    :param directory: The directory.
    :return: The names.
    """

    try:
        return sorted(os.listdir(directory))
    except (FileNotFoundError, NotADirectoryError):
        return []
    finally:
        _listed()


def _subdirectories(directory: Path) -> list[str]:
    try:
        with os.scandir(directory) as listing:
            return sorted(entry.name for entry in listing if entry.is_dir(follow_symlinks=False))
    except (FileNotFoundError, NotADirectoryError):
        return []
    finally:
        _listed()


def _read_bytes(path: Path, limit: int) -> bytes | None:
    # Non-blocking: a FIFO planted in a job-written path must never stall the caller.
    try:
        return _fs.read_bounded(_fs.loc(path), limit, nonblock=True)
    except OSError as exc:
        # FormatError routes it to every layer's damage handling instead of crashing the reader.
        if exc.errno in _DAMAGE_ERRNOS:
            raise FormatError(f"{path} is unreadable: {exc}") from exc
        raise


def _read_json(path: Path, limit: int) -> dict[str, object] | None:
    data = _read_bytes(path, limit)
    if data is None:
        return None
    try:
        value = json.loads(data)
    except ValueError as exc:
        raise FormatError(f"{path} is not JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise FormatError(f"{path} is not a JSON object")
    return value


def _read_json_quietly(path: Path) -> dict[str, object] | None:
    try:
        return _read_json(path, _RECORD_LIMIT)
    except (WorkflowError, OSError):
        return None


def _read_header(directory: Path, expect_key: str | None = None) -> JobHeader:
    value = _read_json(directory / "job.json", MAX_JOB_BYTES)
    if value is None:
        raise FormatError(f"{directory} has no job.json")
    if value.get("format") != "httk-workflow-job" or value.get("format_version") != 3:
        raise FormatError(f"{directory}/job.json is not httk-workflow-job version 3")
    job_id = canonical_uuid(value.get("id"), "job.json id")
    raw_tag = value.get("tag")
    tag = None if raw_tag is None else validate_label(raw_tag, "job.json tag")
    text, priority = value.get("placement"), value.get("priority")
    if not isinstance(text, str):
        raise FormatError(f"{directory}/job.json has no placement")
    placement = normalize_placement(text)
    # Listings tell job directories from placement directories by "~", so a placement may not use it.
    if placement_text(placement) != text or "~" in text:
        raise FormatError(f"{directory}/job.json placement is not canonical: {text!r}")
    if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= 999:
        raise FormatError(f"{directory}/job.json priority must be an integer 0-999")
    job_key = make_job_key(job_id, tag)
    if expect_key is not None and job_key != expect_key:
        raise FormatError(f"{directory}/job.json names {job_key}, not {expect_key}")
    return JobHeader(job_id, job_key, tag, placement, priority)


def _lstat_mode(path: Path, directory: int | None = None) -> int | None:
    try:
        return os.lstat(path, dir_fd=directory).st_mode
    except FileNotFoundError:
        return None


def _check_owner_id(owner_id: str) -> str:
    if not isinstance(owner_id, str) or not _OWNER_ID.fullmatch(owner_id):
        raise ValueError(f"not an owner id (32 lowercase hex digits): {owner_id!r}")
    return owner_id


def _owner_dir(workspace: KernelWorkspace, owner_id: str) -> Path:
    return workspace.control / "owners" / owner_id


def _owned_dir(workspace: KernelWorkspace, owner_id: str) -> Path:
    return workspace.jobs / OWNED / owner_id


def _state_dir(workspace: KernelWorkspace, state: str, placement: PurePosixPath = _NO_PLACEMENT) -> Path:
    return workspace.jobs / state / placement


def check_placement(workspace: KernelWorkspace, state: str, placement: PurePosixPath) -> None:
    """Check that every existing component of ``jobs/<state>/<placement>`` is a real directory (§3.1).

    :func:`~httk.workflow._fs.make_dirs` checks only the deepest existing ancestor, so a symlinked component
    whose target already holds the rest would be followed out of ``jobs/``.

    :param workspace: The workspace.
    :param state: The state directory.
    :param placement: The placement below it.
    :raises httk.workflow._fs.UnsafePath: When a component is a symlink or not a directory.
    """

    root = _state_dir(workspace, state)
    for depth in range(len(placement.parts) + 1):
        current = root.joinpath(*placement.parts[:depth])
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(mode):
            raise _fs.UnsafePath(f"placement component {current} is a symlink or not a directory", "not_directory")


def _tmp(workspace: KernelWorkspace) -> Path:
    return workspace.control / "tmp"


def _requests_dir(workspace: KernelWorkspace) -> Path:
    return workspace.control / "requests"


def _exchange_entry(workspace: KernelWorkspace, exchange_name: str) -> Path:
    return workspace.control / "exchange-jobs" / canonical_uuid(exchange_name, "exchange_name")


# -- owners -------------------------------------------------------------------------------------------------------


def register_owner(
    workspace: KernelWorkspace,
    *,
    kind: str,
    label: str | None,
    allocation: Mapping[str, object] | None,
    advertised: Mapping[str, object],
    execution_scope: _death.ProcessIdentity | None = None,
    launch_id: str | None = None,
) -> "Owner":
    """Create ``owners/<owner-id>/`` and its ``owner.json``, before the owner claims anything.

    :param workspace: The workspace.
    :param kind: ``"manager"``, ``"cli"`` or ``"daemon"``.
    :param label: A human-readable label, or ``None``.
    :param allocation: The recorded allocation (``RecordedAllocation.as_json()``), or ``None``.
    :param advertised: Any of ``pools``, ``capabilities``, ``prefixes``, ``resources``, ``end_time`` and
        ``drain_start`` (epoch seconds, or ``None``).
    :param execution_scope: The identity of this owner's full execution scope, or ``None`` to identify this process.
    :param launch_id: The launch id the owner's launcher gave it (``HTTK_LAUNCH_ID``), which names its launcher
        output ``logs/launchers/<launch id>.*``, or ``None`` when no launcher started it.
    :return: The new owner.
    :raises ValueError: For an unknown kind or advertised member.
    """

    if kind not in _OWNER_KINDS:
        raise ValueError(f"owner kind must be one of {_OWNER_KINDS}: {kind!r}")
    if unknown := set(advertised) - _ADVERTISED:
        raise ValueError(f"unknown advertised owner members: {sorted(unknown)}")
    owner_id = uuid.uuid4().hex
    identity = _death.process_identity() if execution_scope is None else execution_scope
    record = {
        "format": "httk-workflow-owner",
        "format_version": 1,
        "owner_id": owner_id,
        "kind": kind,
        "label": label,
        "hostname": identity.hostname,
        "boot_id": identity.boot_id,
        "pid": identity.pid,
        "process_start_ticks": identity.start_ticks,
        "started_at": _now(),
        "allocation": None if allocation is None else dict(allocation),
        "launch_id": launch_id,
        **advertised,
    }
    path = _owner_dir(workspace, owner_id)
    _fs.make_dirs(path, durable=workspace.durable)
    _fs.write_file(_fs.loc(path / "owner.json"), _encode(record), durable=workspace.durable)
    return Owner(workspace, owner_id, record)


def list_owners(workspace: KernelWorkspace) -> list[OwnerRecord]:
    """List ``owners/`` with each owner's record and tombstone, tolerating unreadable files.

    :param workspace: The workspace.
    :return: The owners, sorted by id.
    """

    root = workspace.control / "owners"
    return [
        OwnerRecord(
            name,
            root / name,
            _read_json_quietly(root / name / "owner.json"),
            _read_json_quietly(root / name / "dead.json"),
        )
        for name in list_names(root)
        if _OWNER_ID.fullmatch(name)
    ]


def attest_dead(
    workspace: KernelWorkspace,
    owner_id: str,
    *,
    by: str,
    evidence: Sequence[Mapping[str, str]],
    operator: str | None = None,
    reason: str | None = None,
) -> None:
    """Write the tombstone ``owners/<owner-id>/dead.json``; dead stays dead, so any writer may repeat it.

    :param workspace: The workspace.
    :param owner_id: The dead owner.
    :param by: ``"probe"`` or ``"operator"``.
    :param evidence: ``{subject, rule, detail}`` strings.
    :param operator: The attesting operator.
    :param reason: The operator's reason.
    :raises ValueError: For a malformed owner id, ``by`` or evidence item.
    """

    _check_owner_id(owner_id)
    if by not in ("probe", "operator"):
        raise ValueError(f"by must be 'probe' or 'operator': {by!r}")
    items = [dict(item) for item in evidence]
    if any(set(item) != _EVIDENCE_KEYS or not all(isinstance(v, str) for v in item.values()) for item in items):
        raise ValueError("evidence items must be {subject, rule, detail} strings")
    tombstone: dict[str, object] = {
        "format": "httk-workflow-tombstone",
        "format_version": 1,
        "owner_id": owner_id,
        "declared_at": _now(),
        "by": by,
        "evidence": items,
    }
    tombstone |= {key: value for key, value in (("operator", operator), ("reason", reason)) if value is not None}
    path = _owner_dir(workspace, owner_id)
    # An operator may attest an owner whose directory is gone but whose owned/<id>/ remains.
    _fs.make_dirs(path, durable=workspace.durable)
    try:
        _fs.write_file(_fs.loc(path / "dead.json"), _encode(tombstone), durable=workspace.durable)
    except FileNotFoundError:
        # Recovery, which needs dead.json, removes the write temporaries beside it: a concurrent attester's that
        # wrote it first and recovered the owner took this one's. Dead stays dead, so the tombstone is there; its
        # details (by, evidence, an operator's reason) are the rival's, and this attester's are not written.
        if not _fs.exists(_fs.loc(path / "dead.json")):
            raise


def _names_process(record_path: Path, here: _death.ProcessIdentity) -> bool:
    record = _read_json_quietly(record_path)
    if record is None:
        return False
    recorded = (record.get("hostname"), record.get("boot_id"), record.get("pid"), record.get("process_start_ticks"))
    return recorded == (here.hostname, here.boot_id, here.pid, here.start_ticks)


def probe_owner(workspace: KernelWorkspace, owner_id: str, *, scheduler: _death.SchedulerQueries) -> _death.Liveness:
    """Run the death proof on another process's owner and write its tombstone when it is proven dead.

    :mod:`~httk.workflow._death` only reads; this is where its ``DEAD`` becomes
    ``dead.json`` (``by="probe"``, with the proof's evidence), which
    :func:`recover` requires.

    :param workspace: The workspace.
    :param owner_id: The owner to probe.
    :param scheduler: The paced scheduler queries.
    :return: ``DEAD`` once a tombstone exists; otherwise the proof's ``ALIVE`` or ``UNKNOWN``.
    :raises ValueError: For a malformed id, or an owner whose ``owner.json`` names this very process.
    """

    _check_owner_id(owner_id)
    path, here = _owner_dir(workspace, owner_id), _death.process_identity()
    if _names_process(path / "owner.json", here):
        raise ValueError(f"owner {owner_id} belongs to this process; an owner cannot probe itself")
    tombstone, record = _fs.loc(path / "dead.json"), _fs.loc(path / "owner.json")
    if _fs.exists(tombstone):
        return _death.Liveness.DEAD
    verdict, evidence = _death.probe(
        path, visibility_deadline=workspace.visibility_deadline, scheduler=scheduler, here=here, sleep=_pause
    )
    if verdict is not _death.Liveness.DEAD or _fs.exists(tombstone):
        return verdict
    if not _fs.exists(record):
        # Its owner.json went away before its process did: a clean close finished; nothing to recover (§6 step 1).
        return _death.Liveness.UNKNOWN
    attest_dead(workspace, owner_id, by="probe", evidence=[dataclasses.asdict(item) for item in evidence])
    return _death.Liveness.DEAD


class Owner:
    """A registered owner: the only actor that may write inside the jobs below ``owned/<owner-id>/``.

    :param workspace: The workspace.
    :param owner_id: The owner id.
    :param record: The ``owner.json`` content.
    """

    def __init__(self, workspace: KernelWorkspace, owner_id: str, record: Mapping[str, object]) -> None:
        self.workspace = workspace
        self.owner_id = _check_owner_id(owner_id)
        self.record: Mapping[str, object] = MappingProxyType(dict(record))
        self.path = _owner_dir(workspace, owner_id)
        # The lock guards _held, _building, _retired_while_building, _trash and _flights for the threads of one
        # command (httk.workflow._pool); no filesystem call runs under it.
        self._lock = threading.Lock()
        self._held: dict[Path, OwnedJob] = {}
        # Paths whose handle adopt_owned is building outside the lock (how many builders), and those of them whose
        # held handle was retired meanwhile: such a build must not publish a handle. Both empty when idle.
        self._building: dict[Path, int] = {}
        self._retired_while_building: set[Path] = set()
        self._trash: Path | None = None
        # tmp/ has an entry change not yet made durable: a non-durable discard's rename out of it, or the trash's own
        # rename whose fsync has not succeeded yet. Cleared only by a successful fsync of tmp/; close seals it
        # before owner.json goes (plan WORKFLOW_TRANSFER_SESSION §7.11, T6a).
        self._unsealed = False
        self._flights = 0
        # Obligations (plan WORKFLOW_LOCAL_COMMANDS §4.10, amendments 10, 16, 26): directories a batched step may
        # have changed without an fsync, registered before the mutation and cleared only by a seal that started
        # after it finished. The serial paths register none. Guarded by the lock, as is the latch below.
        self._obligations: dict[int, _Obligation] = {}
        self._obligation_ids = itertools.count()
        self._finishes = 0
        # A descriptor of each shared directory an obligation names, by the identity of the directory opened, with
        # how many obligations hold it.
        self._retained: dict[tuple[int, int], list[int]] = {}
        # Set once a batch holds a job with an obligation (or explicitly): __exit__ then gives nothing back.
        self._latched = False
        # Whether this owner wrote its batched marker (amendment 27).
        self._batched = False

    def __enter__(self) -> Self:
        """Return the owner."""

        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Close on a normal exit; on an interrupt, return quiescent jobs first; otherwise touch nothing.

        While a pooled operation is still in flight (:meth:`enter_flight`) nothing is touched either: that is a
        fail-stop, leaving every owned job for recovery. On an exception or interrupt, obligations are sealed first
        (:meth:`seal_obligations`); when that fails, or while the owner is :attr:`latched`, nothing is given back
        and ``owner.json`` stays (fail-stop; plan WORKFLOW_LOCAL_COMMANDS amendment 16).

        :param exc_type: The exception type, if any.
        :param exc: The exception, if any.
        :param traceback: The traceback, if any.
        """

        with self._lock:
            flights, held, obliged = self._flights, list(self._held.values()), bool(self._obligations)
        if flights:
            _LOGGER.warning(
                "owner %s exits with %d operations in flight; its jobs are left for recovery", self.owner_id, flights
            )
            return
        if exc_type is None:
            self.close()
            return
        if obliged:
            try:
                self.seal_obligations()
            except Exception:
                _LOGGER.warning(
                    "owner %s could not seal its obligations; its jobs are left for recovery",
                    self.owner_id,
                    exc_info=True,
                )
                return
        with self._lock:
            latched = self._latched
        if latched:
            _LOGGER.warning("owner %s exits latched; its jobs are left for recovery", self.owner_id)
            return
        if not issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
            # Fail-stop (§5.2): an error leaves every owned job for recovery once this process is proven dead.
            return
        try:
            for job in held:
                if job._attempt is None:
                    job.give_back()
            self.close()
        except Exception:
            _LOGGER.warning("owner %s could not return its jobs on interrupt", self.owner_id, exc_info=True)

    def enter_flight(self) -> None:
        """Count one operation of a pooled batch as running; :meth:`__exit__` fail-stops until it leaves."""

        with self._lock:
            self._flights += 1

    def leave_flight(self) -> None:
        """Count one operation counted by :meth:`enter_flight` as finished."""

        with self._lock:
            self._flights -= 1

    def check_alive(self) -> None:
        """Fail-stop check: this owner has no tombstone and its ``owner.json`` names it.

        :raises OwnerDeclaredDead: When this owner was declared dead or its record is gone or foreign.
        """

        if _fs.exists(_fs.loc(self.path / "dead.json")):
            raise OwnerDeclaredDead(f"owner {self.owner_id} carries a tombstone")
        try:
            record = _read_json(self.path / "owner.json", _RECORD_LIMIT)
        except (WorkflowError, OSError) as exc:
            raise OwnerDeclaredDead(f"owner {self.owner_id} has an unreadable owner.json") from exc
        if record is None or record.get("owner_id") != self.owner_id:
            raise OwnerDeclaredDead(f"owner {self.owner_id} has no owner.json of its own")

    def heartbeat(self) -> None:
        """Write the informational ``heartbeat.json``."""

        data = _encode({"owner_id": self.owner_id, "updated_at": _now()})
        _fs.write_file(_fs.loc(self.path / "heartbeat.json"), data, durable=self.workspace.durable)

    def scratch(self, purpose: str) -> Path:
        """Create a scratch directory ``tmp/<owner-id>.<purpose>.<token>/`` only this owner may move.

        :param purpose: ``[a-z][a-z0-9-]*``; it selects the reconciler (§5.4).
        :return: The new directory.
        :raises ValueError: For a malformed purpose.
        """

        if not _PURPOSE.fullmatch(purpose):
            raise ValueError(f"not a scratch purpose: {purpose!r}")
        path = _tmp(self.workspace) / f"{self.owner_id}.{purpose}.{_fs.fresh_token()}"
        durable = self.workspace.durable
        _fs.make_dirs(path.parent, durable=durable)
        # The token is fresh, so this creates the leaf; a recovering owner must find it after a crash.
        _fs.make_dirs(path, durable=durable, mode=0o700)
        return path

    def creation_scratch(self, purpose: str) -> Path:
        """Create a scratch directory as :meth:`scratch` does, but leave its entry in ``tmp/`` to the caller's sync.

        Only for job creation, whose batch commit makes ``tmp/`` durable (a ``syncfs``, or one ``fsync`` of
        ``tmp/`` under the strategy ``fsync``) before anything in the scratch is named complete (plan
        WORKFLOW_TRANSFER_SESSION §7.11, T6a): until then a crash loses only a copy. Never for a scratch that
        receives the only copy of something (a job's moved inputs, a moved payload): those use :meth:`scratch`.

        :param purpose: ``[a-z][a-z0-9-]*``; it selects the reconciler (§5.4).
        :return: The new directory.
        :raises ValueError: For a malformed purpose.
        """

        if not _PURPOSE.fullmatch(purpose):
            raise ValueError(f"not a scratch purpose: {purpose!r}")
        path = _tmp(self.workspace) / f"{self.owner_id}.{purpose}.{_fs.fresh_token()}"
        _fs.make_dirs(path.parent, durable=self.workspace.durable)
        _fs.make_dirs(path, durable=False, mode=0o700)
        return path

    def launch_dir(self, attempt_id: str, n: str) -> Path:
        """Create ``owners/<owner-id>/launches/<attempt>.<n>/`` for one launch's records.

        :param attempt_id: The attempt UUID.
        :param n: ``"0"`` for the attempt runner, or a launch request id (32 hex digits).
        :return: The new directory.
        :raises ValueError: For a malformed attempt id or *n*.
        :raises FileExistsError: When the directory already exists.
        """

        path = self._launch_path(attempt_id, n)
        # Owner-private, so observe-then-create is exclusive; durable, since the death proof reads these records.
        if _fs.exists(_fs.loc(path)):
            raise FileExistsError(errno.EEXIST, "launch directory exists", str(path))
        _fs.make_dirs(path, durable=self.workspace.durable)
        return path

    def remove_launch(self, attempt_id: str, n: str) -> None:
        """Discard one launch directory once its launch is reaped.

        :param attempt_id: The attempt UUID.
        :param n: The launch's ``n``.
        """

        self.discard_tree(self._launch_path(attempt_id, n))

    def discard_scratch(self, path: Path, *, durable: bool = True) -> None:
        """Discard one of this owner's own scratch directories; an absent one is already discarded.

        :param path: ``tmp/<owner-id>.<purpose>.<token>/`` of this owner.
        :param durable: Make its move out of ``tmp/`` durable (in a durable workspace). Without, a crash may bring
            the scratch back, possibly partly removed, for its reconciler or hygiene: only for the scratch of a step
            that completed (a hold's delivery, an adoption's publication, a release's take; plan
            WORKFLOW_TRANSFER_SESSION §7.11, T6a). :meth:`close` seals such moves with its trash's durable discard
            before the owner record goes.
        :raises ValueError: When *path* is not a scratch directory of this owner.
        """

        parts = _SCRATCH.fullmatch(path.name)
        if parts is None or parts[1] != self.owner_id or path.parent != _tmp(self.workspace):
            raise ValueError(f"{path} is not a scratch directory of owner {self.owner_id}")
        if _fs.exists(_fs.loc(path)):
            self.discard_tree(path, durable=durable)

    def holds(self, ref: JobRef) -> bool:
        """Whether this process holds a handle for *ref*, as opposed to a job left in ``owned/<owner-id>/``.

        :param ref: A reference from :meth:`owned`.
        :return: ``True`` while a handle from :meth:`adopt_owned` or :func:`claim` is unreleased.
        """

        with self._lock:
            return ref.path in self._held

    def owned(self) -> list[JobRef]:
        """List ``owned/<owner-id>/``: every job this owner holds on disk, held by this process or not.

        :return: The owned references.
        """

        return _owned_refs(_owned_dir(self.workspace, self.owner_id), self.owner_id)

    def adopt_owned(self, ref: JobRef) -> "OwnedJob":
        """Return the handle of a job in ``owned/<owner-id>/``; the existing one if this process holds it.

        :param ref: A reference from :meth:`owned`.
        :return: The handle.
        :raises ValueError: If *ref* is not owned by this owner.
        :raises OwnerLost: If the job is no longer there.
        :raises httk.workflow.errors.FormatError: If its ``job.json`` is unreadable; the job stays owned.
        :raises httk.workflow._fs.UnsafePath: If its ``job.json`` is not a regular file; the job stays owned.
        """

        if ref.state != OWNED or ref.owner_id != self.owner_id:
            raise ValueError(f"{ref.path} is not owned by {self.owner_id}")
        # One handle per job and process: the quiescence bookkeeping must not be forked.
        path = ref.path
        with self._lock:
            held = self._held.get(path)
            if held is None:
                self._building[path] = self._building.get(path, 0) + 1
        if held is not None:
            return held
        try:
            if not _fs.exists(_fs.loc(path)):
                raise OwnerLost(f"{path} left owned/{self.owner_id}/")
            job = OwnedJob(self, ref)  # reads job.json: built outside the lock, inserted unless another thread won
            with self._lock:
                if path in self._retired_while_building:
                    # Another thread's handle of this job was released or extracted meanwhile.
                    raise OwnerLost(f"{path} left owned/{self.owner_id}/")
                return self._held.setdefault(path, job)
        finally:
            with self._lock:
                if builders := self._building.pop(path) - 1:
                    self._building[path] = builders
                else:
                    self._retired_while_building.discard(path)

    def close(self) -> None:
        """Seal the obligations, return unheld owned jobs, reconcile scratch and remove this owner's directories
        when nothing is left.

        The obligations are sealed first (:meth:`seal_obligations`), before anything else is touched.

        :raises httk.workflow.errors.WorkflowError: While this process holds jobs, has operations in flight or
            obligations whose mutation has not finished, or when owned jobs remain.
        :raises OSError: When the seal fails (:class:`~httk.workflow._fs.SyncFailed`); ``owner.json`` stays.
        """

        self.seal_obligations()
        with self._lock:
            holding, flights, unfinished = len(self._held), self._flights, len(self._obligations)
        if holding or flights:
            raise WorkflowError(f"owner {self.owner_id} still holds {holding} jobs, {flights} operations in flight")
        if unfinished:
            raise WorkflowError(f"owner {self.owner_id} has {unfinished} obligations whose mutation has not finished")
        # §5.2 self-healing: jobs in owned/<self>/ this process never held go back unchanged; nothing launches.
        for ref in self.owned():
            self.adopt_owned(ref).give_back()
        if self.owned():
            raise WorkflowError(f"owner {self.owner_id} still owns jobs after self-healing")
        workspace = self.workspace
        with self._lock:
            trash = self._trash
        kept = [path for path in _own_scratch(workspace, self.owner_id, trash) if not reconcile_scratch(self, path)]
        _fs.remove_empty_dir(_fs.loc(_owned_dir(workspace, self.owner_id)))
        launches = self.path / "launches"
        # The trash goes before owner.json: an owner-named scratch must never outlive its owner's record.
        with self._lock:
            trash = self._trash
        if trash is not None:
            reconcile_scratch(self, trash)
        with self._lock:
            unsealed = self._unsealed
        if unsealed and workspace.durable:
            # A non-durable move out of tmp/ that no fsync of tmp/ has sealed yet (an earlier close failed after the
            # trash's rename, say): sealed here, whether or not the moved entry still exists.
            _seal_tmp(workspace)
            with self._lock:
                self._unsealed = False
        if not kept and not list_names(launches):
            # Whether or not this process knows it wrote one (a marker whose directory fsync failed is there too).
            _remove_batched_marker(self.path, durable=workspace.durable)
            _fs.remove_file(_fs.loc(self.path / "owner.json"), durable=workspace.durable)
            _fs.remove_file(_fs.loc(self.path / "heartbeat.json"), durable=workspace.durable)
            _fs.remove_empty_dir(_fs.loc(launches))
            _fs.remove_empty_dir(_fs.loc(self.path))

    def _launch_path(self, attempt_id: str, n: str) -> Path:
        canonical_uuid(attempt_id, "attempt_id")
        if not _LAUNCH_N.fullmatch(n):
            raise ValueError(f"launch n must be '0' or 32 lowercase hex digits: {n!r}")
        return self.path / "launches" / f"{attempt_id}.{n}"

    def discard_tree(self, path: Path | _fs.Loc, *, durable: bool = True) -> None:
        """Remove a directory tree only this owner can move, through its trash scratch.

        The tree is moved into this owner's ``trash`` scratch first, so a crash mid-removal leaves an owner-named
        scratch that close or recovery reconciles; the trash scratch itself goes through the ``tmp`` root. Single
        files use :func:`httk.workflow._fs.remove_file`. The composition of :meth:`trash_tree` and
        :func:`httk.workflow._fs.remove_trashed`.

        :param path: The tree, such as an entry inside one of this owner's scratch directories.
        :param durable: Make the move into the trash durable (in a durable workspace); see :meth:`discard_scratch`. A
            move that is not durable leaves ``tmp/`` to be sealed by :meth:`close` before the owner record goes.
        """

        _fs.remove_trashed(self.trash_tree(path, durable=durable))

    def trash_tree(
        self,
        path: Path | _fs.Loc,
        *,
        durable: bool = True,
        commit: _fs.Commit | None = None,
        job: "OwnedJob | None" = None,
    ) -> _fs.Loc:
        """Move a directory tree only this owner can move into its trash scratch: the first step of
        :meth:`discard_tree`, after which :func:`httk.workflow._fs.remove_trashed` removes it.

        :param path: The tree.
        :param durable: As for :meth:`discard_tree`.
        :param commit: Record the move's directories in this collector instead of fsyncing them, after registering
            them as ``owner`` obligations (:meth:`obliged`); ``tmp/`` is then left for :meth:`close` to seal too.
        :param job: The handle whose step this is (it latches the owner), with *commit*.
        :return: The tree's trash name.
        :raises ValueError: For an anchored *path* with *commit*.
        """

        target = path if isinstance(path, _fs.Loc) else _fs.loc(path)
        if commit is not None and target.at is not None:
            # Its directory is a descriptor no obligation could retain by identity: batched trashing takes paths.
            raise ValueError(f"a batched trash move takes a path, not an anchored location: {target.path}")
        with self._lock:
            trash = self._trash
            itself = target.at is None and target.path == trash
            if itself:
                self._trash = None
        if itself:
            trash_dir = target.path.parent
        elif trash is None or not _fs.exists(_fs.loc(trash)):
            # ponytail: threads that find no trash at once each create one; close (or recovery) discards the
            # extra ones as ordinary discardable scratch. One shared creation would need the lock across _fs.
            trash_dir = self.scratch("trash")
            with self._lock:
                if self._trash == trash:
                    self._trash = trash_dir
        else:
            trash_dir = trash
        durable = self.workspace.durable and durable
        if self.workspace.durable and (itself or not durable or commit is not None):
            # Unsealed from the rename on: the trash's own rename is sealed only once its fsync of tmp/ succeeded.
            with self._lock:
                self._unsealed = True
        if commit is None:
            trashed = _fs.trash(target, trash_dir=trash_dir, durable=durable)
        else:
            with self.obliged([target.path.parent, trash_dir], kind="owner", job=job):
                trashed = _fs.trash(target, trash_dir=trash_dir, durable=False, commit=commit)
        if itself and durable and commit is None:
            # That fsync of tmp/ also sealed every earlier non-durable move out of it.
            with self._lock:
                self._unsealed = False
        return trashed

    # -- obligations (plan WORKFLOW_LOCAL_COMMANDS §4.10, amendments 10, 16, 20, 26) ------------------------------

    @property
    def latched(self) -> bool:
        """Whether a batch holds (or held) a job with an obligation: :meth:`__exit__` then gives nothing back."""

        with self._lock:
            return self._latched

    def latch(self) -> None:
        """Set the fail-stop latch: from now on an exceptional exit gives nothing back and keeps ``owner.json``."""

        with self._lock:
            self._latched = True

    def unlatch(self) -> None:
        """Clear the latch once the batch's cleanup has decided every held job, sealing every obligation first.

        :raises httk.workflow.errors.WorkflowError: While an obligation's mutation has not finished; the latch stays.
        :raises OSError: When the seal fails; the latch stays.
        """

        self.seal_obligations()
        with self._lock:
            if self._obligations:
                raise WorkflowError(f"owner {self.owner_id} has obligations whose mutation has not finished")
            self._latched = False

    @contextlib.contextmanager
    def obliged(
        self, directories: Iterable[Path], *, kind: ObligationKind, job: "OwnedJob | None" = None
    ) -> Iterator[None]:
        """Register one obligation per directory before a batched mutation; mark them finished after it.

        Conservatively, each says "this directory may have an unsealed change" from before the mutation on; it is
        marked finished however the mutation ended (it may have happened even when it raised), and only a seal
        that starts after that clears it (:meth:`seal_obligations`, :meth:`commit`, or a serial step's own fsync of
        the directory). A shared directory (any outside *job*'s directory) is sealed through a descriptor retained
        from now on where it exists, by the identity of the directory opened; a job-private one by path. An
        obligation of a handle latches the owner. The first registration writes the owner's batched marker
        (plan WORKFLOW_LOCAL_COMMANDS amendment 27) durably first. In a workspace that is not durable nothing is
        registered.

        :param directories: The directories whose entries the mutation may change.
        :param kind: ``owner`` or ``exposure``.
        :param job: The handle whose step it is, or ``None``.
        :return: A context manager around the mutation.
        :raises ValueError: For an unknown kind.
        """

        if kind not in ("owner", "exposure"):
            raise ValueError(f"an obligation is of kind owner or exposure, not {kind!r}")
        ids: list[int] = []
        if self.workspace.durable:
            self.mark_batched()
            paths = list(dict.fromkeys(Path(item) for item in directories))
            identities = {
                path: self._retain(path) if job is None or not path.is_relative_to(job.path) else None for path in paths
            }
            with self._lock:
                for path in paths:
                    ids.append(identifier := next(self._obligation_ids))
                    self._obligations[identifier] = _Obligation(kind, path, job, identities[path])
                if job is not None:
                    self._latched = True
        try:
            yield
        finally:
            with self._lock:
                self._finishes += bool(ids)
                for identifier in ids:
                    if (obligation := self._obligations.get(identifier)) is not None:
                        obligation.finished = self._finishes

    def mark_batched(self) -> None:
        """Write this owner's batched marker durably, once, before its first batched mutation (plan
        WORKFLOW_LOCAL_COMMANDS amendment 27): the recovery of a dead owner so marked also seals every placement
        directory. Nothing in a workspace that is not durable.
        """

        with self._lock:
            if self._batched or not self.workspace.durable:
                return
        record = _encode({"owner_id": self.owner_id, "batched_at": _now()})
        _fs.write_file(_fs.loc(self.path / _BATCHED_MARKER), record, durable=True)
        with self._lock:
            self._batched = True

    def _retain(self, path: Path) -> tuple[int, int] | None:
        # Keep a descriptor of the directory at *path* for the seal, counted by its identity; None when it cannot be
        # opened (it is then sealed by path, whose seal fails on anything but absence).
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError:
            return None
        try:
            status = os.fstat(descriptor)
        except OSError:
            os.close(descriptor)
            return None
        identity = (status.st_dev, status.st_ino)
        with self._lock:
            if (entry := self._retained.get(identity)) is None:
                self._retained[identity] = [descriptor, 1]
                return identity
            entry[1] += 1
        os.close(descriptor)
        return identity

    def _finished_obligations(self) -> tuple[list[int], dict[object, Path | int]]:
        # The obligations whose mutation has finished, and the distinct directories to seal for them: a duplicate of
        # each retained descriptor (by identity), which the caller closes, or a path (by path).
        targets: dict[object, Path | int] = {}
        try:
            with self._lock:
                ids = [identifier for identifier, item in self._obligations.items() if item.finished is not None]
                for identifier in ids:
                    obligation = self._obligations[identifier]
                    if obligation.identity is None:
                        targets.setdefault(obligation.directory, obligation.directory)
                    elif obligation.identity not in targets:
                        targets[obligation.identity] = os.dup(self._retained[obligation.identity][0])
        except BaseException:
            _close_descriptors(targets.values())
            raise
        return ids, targets

    def _clear(self, ids: Iterable[int]) -> None:
        # Drop sealed obligations; a retained descriptor no obligation holds any more is closed.
        closing = []
        with self._lock:
            for identifier in ids:
                obligation = self._obligations.pop(identifier, None)
                if obligation is None or obligation.identity is None:
                    continue
                entry = self._retained[obligation.identity]
                entry[1] -= 1
                if not entry[1]:
                    closing.append(self._retained.pop(obligation.identity)[0])
        _close_descriptors(closing)

    def _mark(self) -> int:
        # The finish count now: a serial step's fsync that starts after this seals what had finished by then.
        with self._lock:
            return self._finishes

    def _sealed_by(self, directories: Iterable[Path], mark: int) -> None:
        # A serial step fsynced *directories* (by path) after *mark*: the obligations there that had finished by
        # then are sealed. One retained by descriptor only while the path still names the directory it retained.
        # HEAD's _fsync_parent skips a parent a pruner removed; clearing then is sound for the same reason it is
        # there: an rmdir succeeds only on an empty directory, so every entry a step of ours put there has left it by
        # a later owner's move, which that owner makes durable; nothing of ours is left in it to seal.
        with self._lock:
            if not self._obligations:
                return
        wanted: dict[Path, tuple[int, int] | None] = {}
        for directory in directories:
            try:
                status = os.lstat(directory)
                wanted[directory] = (status.st_dev, status.st_ino)
            except OSError:
                wanted[directory] = None
        with self._lock:
            ids = [
                identifier
                for identifier, item in self._obligations.items()
                if item.finished is not None
                and item.finished <= mark
                and item.directory in wanted
                and (item.identity is None or item.identity == wanted[item.directory])
            ]
        self._clear(ids)

    def pinned(self, job: "OwnedJob") -> bool:
        """Whether *job* has an ``exposure`` obligation no seal has cleared (an unsealed earlier-phase step).

        :param job: The handle.
        :return: Whether it may not leave ``owned/`` yet.
        """

        with self._lock:
            return any(item.kind == "exposure" and item.job is job for item in self._obligations.values())

    def seal_obligations(self) -> None:
        """Fsync the directory of every obligation whose mutation has finished, then clear them.

        Each distinct directory once: through its retained descriptor (one per directory identity), or by path
        (gone: its nearest existing ancestor; :func:`httk.workflow._fs.seal_directory`). All or nothing: nothing is
        cleared when an fsync fails.

        :raises OSError: When an fsync fails (:class:`~httk.workflow._fs.SyncFailed` for a seal by path).
        :raises httk.workflow._fs.UnsafePath: When a directory sealed by path is a symlink or not a directory.
        """

        ids, targets = self._finished_obligations()
        if not ids:
            return
        try:
            for target in targets.values():
                if isinstance(target, int):
                    os.fsync(target)
                else:
                    _fs.seal_directory(target)
        finally:
            _close_descriptors(targets.values())
        self._clear(ids)

    def commit(self, commit: _fs.Commit) -> None:
        """Run *commit*'s commit point with every finished obligation's directory recorded in it, then clear those
        obligations: a phase's commit point that also seals the owner's obligations.

        :param commit: The entered collector of the phase.
        :raises OSError: When the commit fails (:class:`~httk.workflow._fs.SyncFailed`); nothing is cleared.
        """

        ids, targets = self._finished_obligations()
        try:
            for target in targets.values():
                commit.directory(target)
        finally:
            _close_descriptors(targets.values())
        commit.commit()
        self._clear(ids)


def _close_descriptors(items: Iterable[object]) -> None:
    for item in items:
        if isinstance(item, int):
            os.close(item)


def _owned_refs(directory: Path, owner_id: str) -> list[JobRef]:
    refs = []
    for name in list_names(directory):
        try:
            refs.append(JobRef.from_path(directory / name, state=OWNED, owner_id=owner_id))
        except FormatError:
            _LOGGER.debug("skipping %s: not an owned job name", directory / name)
    return refs


def _own_scratch(workspace: KernelWorkspace, owner_id: str, trash: Path | None) -> list[Path]:
    tmp = _tmp(workspace)
    return [
        tmp / name
        for name in list_names(tmp)
        if (parts := _SCRATCH.fullmatch(name)) and parts[1] == owner_id and tmp / name != trash
    ]


# -- owned jobs ---------------------------------------------------------------------------------------------------


class OwnedJob:
    """The handle of a job this process won: every write to a job goes through one.

    :param owner: The owner.
    :param ref: The job's reference below ``owned/<owner-id>/``.
    :raises httk.workflow.errors.FormatError: If ``job.json`` is missing, malformed or names another job.
    :raises httk.workflow._fs.UnsafePath: If ``job.json`` is a symlink or not a regular file.
    """

    def __init__(self, owner: Owner, ref: JobRef) -> None:
        if ref.from_state is None:
            raise ValueError(f"{ref.path} is not an owned job")
        self.owner = owner
        self.ref = ref
        self.path = ref.path
        self.job_id = ref.job_id
        self.job_key = ref.job_key
        self.from_state: str = ref.from_state
        self.from_priority = ref.priority
        self._attempt: str | None = None
        self._retired: set[str] = set()
        self._released = False
        # Where move_to_trash moved the job, and whether the handle was retired as discarded (retire_discarded): only
        # then does it unlink the job's request files without holding the job.
        self._trashed: _fs.Loc | None = None
        self._discarded = False
        # Where a release or give-back moved the job out of owned/ (None until then, and after discard or extract).
        self.moved_to: JobRef | None = None
        # Read while quiescent (at claim or adoption): a later attempt may rewrite or replace job.json.
        self._header = _read_header(self.path, self.job_key)

    def header(self) -> JobHeader:
        """Return the kernel's view of ``job.json``, read when this handle was created.

        :return: The header.
        """

        self._live()
        return self._header

    def placement(self) -> PurePosixPath:
        """Return the authoritative placement, from ``job.json`` as read at claim or adoption."""

        return self.header().placement

    def read_state(self) -> StateDoc | None:
        """Return ``state.json``, or ``None`` before the job's first write.

        Callers treat both errors below as a damaged ``state.json``.

        :return: The document.
        :raises httk.workflow.errors.FormatError: If the document is malformed, too large, unreadable (``EACCES``,
            ``EPERM``, ``EIO``) or names another job.
        :raises httk.workflow._fs.UnsafePath: If ``state.json`` is a symlink or not a regular file.
        """

        self._live()
        data = _read_bytes(self.path / "state.json", MAX_STATE_BYTES)
        if data is None:
            return None
        doc = decode_state(data)
        if doc.job_id != self.job_id:
            raise FormatError(f"{self.path}/state.json names job {doc.job_id}")
        return doc

    def write_state(self, doc: StateDoc) -> None:
        """Replace ``state.json``; applied request ids whose files this handle deleted are dropped.

        The composition of :meth:`stage_state` and :meth:`replace_state`, each fsyncing inline.

        :param doc: The document.
        :raises OwnerLost: When the job directory is gone.
        :raises ValueError: When *doc* names another job.
        """

        self.replace_state(self.stage_state(doc))

    def stage_state(self, doc: StateDoc, *, commit: _fs.Commit | None = None) -> _fs.Staged:
        """Write ``.state.json.<t>.tmp`` beside ``state.json``: the first step of :meth:`write_state` (plan
        WORKFLOW_LOCAL_COMMANDS §5 amendment 14).

        Applied request ids whose files this handle deleted are dropped from *doc* first.

        :param doc: The document.
        :param commit: Record the temporary's writing descriptor in this collector instead of fsyncing it.
        :return: The staged temporary, for :meth:`replace_state`.
        :raises OwnerLost: When the job directory is gone.
        :raises ValueError: When *doc* names another job.
        """

        self._present()
        if doc.job_id != self.job_id:
            raise ValueError(f"state of job {doc.job_id} written to {self.job_id}")
        if retired := self._retired.intersection(doc.applied_requests):
            # §5.3: ids whose files are gone need no skipping any more.
            applied = tuple(item for item in doc.applied_requests if item not in retired)
            doc = dataclasses.replace(doc, applied_requests=applied)
        target = _fs.loc(self.path / "state.json")
        if commit is not None:
            self.owner.mark_batched()
        try:
            return _fs.stage_file(target, encode_state(doc), durable=self.owner.workspace.durable, commit=commit)
        except FileNotFoundError:
            self._present()
            raise

    def replace_state(self, staged: _fs.Staged, *, commit: _fs.Commit | None = None) -> None:
        """Rename a staged ``state.json`` temporary onto ``state.json``: the second step of :meth:`write_state`.

        With *commit*, the job directory is an ``exposure`` obligation of this handle from before the rename until
        a seal clears it (:meth:`Owner.obliged`), and is recorded in *commit* instead of fsynced.

        :param staged: What :meth:`stage_state` returned.
        :param commit: The phase's collector, or ``None`` to fsync inline.
        :raises OwnerLost: When the job directory is gone.
        """

        self._present()
        owner, durable = self.owner, self.owner.workspace.durable
        mark = owner._mark()
        try:
            if commit is None:
                _fs.replace_staged(staged, durable=durable)
            else:
                with owner.obliged([self.path], kind="exposure", job=self):
                    _fs.replace_staged(staged, durable=False, commit=commit)
        except FileNotFoundError:
            self._present()
            raise
        if commit is None and durable:
            owner._sealed_by([self.path], mark)

    def append_log(self, event: str, /, **detail: object) -> None:
        """Append one line to ``logs/runlog.jsonl``.

        :param event: The event name.
        :param **detail: Further JSON members of the line.
        :raises OwnerLost: When the job directory is gone.
        :raises ValueError: For an empty event or a detail that shadows a fixed member.
        """

        self.append_log_line(event, detail)

    def append_log_line(self, event: str, detail: Mapping[str, object], *, commit: _fs.Commit | None = None) -> None:
        """Append one line to ``logs/runlog.jsonl``: the step of :meth:`append_log` (plan WORKFLOW_LOCAL_COMMANDS §5
        amendment 14).

        Without *commit* it is :meth:`append_log` (a planted ``logs`` or ``runlog.jsonl`` is removed and recreated,
        the line fsynced on its writing descriptor). With *commit*, a planted entry refuses the step
        (:class:`StepRefused`, amendment 26); ``logs/`` and the run log are created where missing, and the writing
        descriptor and the directories that gained an entry are recorded in *commit* instead of fsynced.

        :param event: The event name.
        :param detail: Further JSON members of the line.
        :param commit: The phase's collector, or ``None`` to fsync inline.
        :raises OwnerLost: When the job directory is gone.
        :raises ValueError: For an empty event or a detail that shadows a fixed member.
        :raises StepRefused: With *commit*, for a planted ``logs`` or ``runlog.jsonl``.
        """

        self._present()
        if not event or {"at", "event", "owner_id"} & detail.keys():
            raise ValueError("a run-log line needs an event and may not override at/event/owner_id")
        line = _encode({"at": _now(), "event": event, "owner_id": self.owner.owner_id, **detail}) + b"\n"
        if commit is not None:
            self.owner.mark_batched()
        descriptor = self.open_log("runlog.jsonl") if commit is None else self._open_log_step(commit)
        try:
            _fs.write_all(descriptor, line)
            if commit is not None:
                commit.file(descriptor)
            elif self.owner.workspace.durable:
                os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _open_log_step(self, commit: _fs.Commit) -> int:
        # open_log("runlog.jsonl") for a batched step: a planted entry refuses it, and what gained an entry (the job
        # directory where logs/ was created, logs/ where the run log was) is recorded in the collector.
        self._present()
        logs = self.path / "logs"
        mode = _lstat_mode(logs)
        if mode is not None and not stat.S_ISDIR(mode):
            raise StepRefused(f"{logs} is not a real directory: the job is served serially")
        directory = _fs.open_dir_under(self.path, "logs", create=True, mode=0o777, durable=False)
        try:
            if mode is None:
                commit.directory(self.path)
            existing = _lstat_mode(Path("runlog.jsonl"), directory)
            if existing is not None and not stat.S_ISREG(existing):
                raise StepRefused(f"{logs}/runlog.jsonl is not a regular file: the job is served serially")
            descriptor = _fs.open_append(_fs.anchored(directory, "runlog.jsonl"), durable=False)
            if existing is None:
                try:
                    commit.directory(directory)
                except BaseException:
                    os.close(descriptor)
                    raise
            return descriptor
        finally:
            os.close(directory)

    def open_log(self, name: str) -> int:
        """Open ``logs/<name>`` for appending, after making ``logs/`` and the file trusted, plain entries.

        A symlink or non-directory at ``logs`` and a non-regular entry at ``logs/<name>`` (planted by a job)
        are removed, never followed, and recreated; nothing there can block the call.

        :param name: The file name, ``[a-z0-9][a-z0-9._-]*``.
        :return: An ``O_WRONLY|O_APPEND`` descriptor of a regular file; the caller closes it.
        :raises ValueError: For a malformed name.
        :raises OwnerLost: When the job directory is gone.
        """

        if not _LOG_NAME.fullmatch(name):
            raise ValueError(f"not a log file name: {name!r}")
        self._present()
        durable = self.owner.workspace.durable
        logs = self.path / "logs"
        # §3.3: logs/ is trusted. Anything but a real directory there was planted, and is removed unfollowed.
        if (mode := _lstat_mode(logs)) is not None and not stat.S_ISDIR(mode):
            _fs.remove_file(_fs.loc(logs), durable=durable)
            mode = None
        # Every later step is anchored at this descriptor, so a swapped logs/ cannot redirect it; anchored at the
        # job directory, so a job a recoverer moved away is never recreated here.
        directory = _fs.open_dir_under(self.path, "logs", create=True, mode=0o777, durable=durable)
        try:
            target = _fs.anchored(directory, name)
            mode = _lstat_mode(Path(name), directory)
            if mode is not None and stat.S_ISDIR(mode):
                self.owner.discard_tree(target)
            elif mode is not None and not stat.S_ISREG(mode):
                _fs.remove_file(target, durable=durable)
            return _fs.open_append(target, durable=durable)
        finally:
            os.close(directory)

    def begin_attempt(self, attempt_id: str) -> None:
        """Record that an attempt's processes may run from now on (called right before the gate opens).

        :param attempt_id: The attempt UUID.
        :raises httk.workflow.errors.WorkflowError: If another attempt has not ended.
        """

        self._live()
        if self._attempt is not None:
            raise WorkflowError(f"{self.job_key}: attempt {self._attempt} has not ended")
        self._attempt = canonical_uuid(attempt_id, "attempt_id")

    def end_attempt(self, attempt_id: str) -> None:
        """Record that the attempt and every launch it made were reaped.

        :param attempt_id: The attempt UUID.
        :raises httk.workflow.errors.WorkflowError: If *attempt_id* is not the live attempt.
        """

        self._live()
        if self._attempt != attempt_id:
            raise WorkflowError(f"{self.job_key}: attempt {attempt_id} is not the live attempt")
        self._attempt = None

    def require_quiescent(self) -> None:
        """Refuse while an attempt this process began has not ended (§1 P5).

        :raises httk.workflow.errors.WorkflowError: While an attempt is live.
        """

        if self._attempt is not None:
            raise WorkflowError(f"{self.job_key}: attempt {self._attempt} is still live")

    def request_files(self, *, listing: Iterable[str] | None = None) -> list[Path]:
        """List this job's pending request files; delete leftover files of applied requests.

        :param listing: The names of one listing of ``requests/`` taken after this job's claim, the job held ever
            since (the retirement evidence of plan WORKFLOW_LOCAL_COMMANDS amendment 2), instead of listing it now.
        :return: The pending request files, sorted.
        """

        self._live()
        doc = self.read_state()
        applied = set(doc.applied_requests) if doc is not None else set()
        directory, pending, listed = _requests_dir(self.owner.workspace), [], set()
        for name in list_names(directory) if listing is None else sorted(listing):
            request_id = _request_id(name, self.job_id)
            if request_id is None:
                continue
            listed.add(request_id)
            if request_id in applied:
                # §3.3: an applied request's leftover file is deleted on sight and never re-applied.
                _fs.remove_file(_fs.loc(directory / name), durable=self.owner.workspace.durable)
                self._retired.add(request_id)
            else:
                pending.append(directory / name)
        # An applied id without a file was deleted before this claim (a stale listing that still shows the
        # file takes the branch above): it needs no skipping, so the next write_state drops it.
        self._retired |= applied - listed
        return pending

    def pending_release(self) -> Release | None:
        """Return the recorded release if it is not yet done (it differs from the claimed state and priority).

        :return: The pending release, or ``None``.
        """

        return self._pending(self.read_state())

    def release(self, doc: StateDoc, release: Release) -> JobRef:
        """The release rule: record the release, delete applied requests, move to ``jobs/<state>/<placement>/``.

        The composition of :meth:`write_state`, :meth:`unlink_requests` and :meth:`move_out`, each fsyncing inline.

        :param doc: The document to record (``release_to`` and ``applied_requests`` are added).
        :param release: Where to and which requests were applied.
        :return: The released job's new reference.
        :raises OwnerLost: When the job directory is gone.
        """

        self._live()
        self.require_quiescent()
        self._present()
        placement = self.placement()
        self.write_state(doc.with_release(release))
        self.unlink_requests(f"{self.job_id}.{request_id}.json" for request_id in release.applied_requests)
        return self._move_out(release.state, placement, release.priority)

    def unlink_requests(self, names: Iterable[str], *, commit: _fs.Commit | None = None) -> None:
        """Unlink request files of this job from ``requests/``: a step of :meth:`release` and :meth:`discard` (plan
        WORKFLOW_LOCAL_COMMANDS §5 amendment 14).

        While the handle holds the job it must still be there (a recovered job's requests are its successor's);
        once :meth:`retire_discarded` retired it, the requests of the deleted job are unlinked without it. With
        *commit*, ``requests/`` is an obligation from before the first unlink (``exposure`` of this handle while it
        holds the job, ``owner`` after a discard) and is recorded in *commit* instead of fsynced.

        :param names: The file names, ``<job-uuid>.<request-uuid>.json``, of this job.
        :param commit: The phase's collector, or ``None`` to fsync ``requests/`` after each unlink.
        :raises OwnerLost: When the held job directory is gone.
        :raises ReleasedJobError: When the handle was retired otherwise than by :meth:`retire_discarded`.
        """

        held = not self._discarded
        if held:
            self._live()
            self._present()
        names = list(names)
        if not names:
            return
        owner, durable = self.owner, self.owner.workspace.durable
        directory = _requests_dir(owner.workspace)
        if commit is None:
            mark = owner._mark()
            for name in names:
                _fs.remove_file(_fs.loc(directory / name), durable=durable)
            if durable:
                owner._sealed_by([directory], mark)
            return
        with owner.obliged([directory], kind="exposure" if held else "owner", job=self if held else None):
            for name in names:
                _fs.remove_file(_fs.loc(directory / name), durable=False, commit=commit)

    def already_applied(self, request_id: str) -> bool:
        """Whether *request_id* is among the applied requests of the held, authoritative ``state.json`` (plan
        WORKFLOW_LOCAL_COMMANDS §5 amendment 21): an applied leftover is never applied again.

        :param request_id: The request UUID.
        :return: Whether the job's state records it as applied.
        :raises httk.workflow.errors.FormatError: If ``state.json`` is damaged (see :meth:`read_state`).
        :raises httk.workflow._fs.UnsafePath: If ``state.json`` is not a regular file.
        :raises OwnerLost: When the job directory is gone.
        """

        self._present()
        request_id = canonical_uuid(request_id, "request_id")
        doc = self.read_state()
        if doc is None:
            self._present()  # no state.json because the job is gone is not "never applied"
            return False
        return request_id in doc.applied_requests

    def move_to_trash(self, *, commit: _fs.Commit | None = None) -> _fs.Loc:
        """Move the quiescent job into the owner's trash scratch: the step of :meth:`discard` that removes it from
        ``owned/`` (plan WORKFLOW_LOCAL_COMMANDS §5 amendment 14); the handle stays until the caller retires it.

        :param commit: Record the move's directories in this collector instead of fsyncing them (``owner``
            obligations of this handle).
        :return: The job's trash name, for :func:`httk.workflow._fs.remove_trashed`.
        :raises OwnerLost: When the job directory is gone.
        :raises httk.workflow.errors.WorkflowError: While an attempt runs, or the handle has an unsealed step.
        """

        self._live()
        self.require_quiescent()
        self._unpinned()
        self._present()
        self._trashed = self.owner.trash_tree(self.path, commit=commit, job=self if commit is not None else None)
        return self._trashed

    @property
    def trashed(self) -> _fs.Loc | None:
        """Where :meth:`move_to_trash` moved the job, or ``None`` before it returned."""

        return self._trashed

    def retire_discarded(self) -> None:
        """Retire the handle of a job :meth:`move_to_trash` moved into the trash: the job is deleted, and only its
        request files remain to be unlinked (:meth:`unlink_requests`).

        :raises httk.workflow.errors.WorkflowError: When the job was not moved into the trash by this handle.
        """

        if self._trashed is None:
            raise WorkflowError(f"{self.job_key} was not moved into the trash: it is not discarded")
        self._discarded = True
        self._retire()

    def discard(self) -> None:
        """Delete the job (the ``delete`` request), its exchange index entry and its request files.

        The composition of the exchange index drop, :meth:`move_to_trash`, the tree removal and
        :meth:`unlink_requests`, each fsyncing inline.

        :raises OwnerLost: When the job directory is gone.
        """

        self._live()
        self.require_quiescent()
        try:
            doc = self.read_state()
        except (FormatError, _fs.UnsafePath):
            # A delete must always apply: a damaged state.json is treated as not an exchange job.
            _LOGGER.warning("deleting %s with an unreadable state.json", self.path, exc_info=True)
            doc = None
        if doc is not None and doc.origin == "exchange" and doc.exchange_name is not None:
            # §8.5: the index goes first; a crash before the job goes leaves the delete request pending. The job is
            # held here, so no other adoption of its id can hold the entry: its current nonce is this job's.
            indexed = exchange_index(self.owner.workspace, doc.exchange_name)
            if indexed is not None:
                drop_exchange_index(self.owner, doc.exchange_name, self.job_id, indexed[2])
        _fs.remove_trashed(self.move_to_trash())
        self.retire_discarded()
        directory = _requests_dir(self.owner.workspace)
        self.unlink_requests(name for name in list_names(directory) if _request_id(name, self.job_id) is not None)

    def extract(self, destination: Path) -> None:
        """Move the quiescent job into a bundle being built in this owner's scratch, and retire the handle.

        Besides release, discard and recovery, this is the only way an owned job leaves ``owned/``. Nothing is
        written to the job: the bundle's ``bundle.json`` already records its ``from`` state and priority.

        :param destination: A fresh path strictly below one of this owner's scratch directories,
            ``tmp/<owner-id>.<purpose>.<token>/``, whose parent exists.
        :raises httk.workflow.errors.WorkflowError: While an attempt runs, after the handle was released, or while
            it has a step no seal has made durable yet.
        :raises ValueError: When *destination* is not below this owner's scratch, exists, or has no parent.
        :raises OwnerLost: When the job directory or the destination's parent is gone.
        """

        self._live()
        self.require_quiescent()
        self._unpinned()
        destination = Path(destination)
        tmp = _tmp(self.owner.workspace)
        # Lexical: the scratch is owner-private, so a path below its name cannot lead anywhere else.
        relative = destination.relative_to(tmp) if destination.is_relative_to(tmp) else None
        if (
            relative is None
            or len(relative.parts) < 2
            or ".." in relative.parts
            or (scratch := _SCRATCH.fullmatch(relative.parts[0])) is None
            or scratch[1] != self.owner.owner_id
        ):
            raise ValueError(f"{destination} is not below a scratch directory of owner {self.owner.owner_id}")
        if _fs.exists(_fs.loc(destination)):
            raise ValueError(f"{destination} already exists")
        try:
            parent_is_dir = stat.S_ISDIR(os.lstat(destination.parent).st_mode)
        except (FileNotFoundError, NotADirectoryError):
            parent_is_dir = False
        if not parent_is_dir:
            raise ValueError(f"the parent of {destination} is not a directory")
        # move_owned decides by the source alone, so a vanished source must be caught first.
        self._present()
        try:
            # Never recreate the parent: a recoverer may have taken the scratch since the check above.
            _fs.move_owned(
                _fs.loc(self.path), _fs.loc(destination), durable=self.owner.workspace.durable, create_parents=False
            )
        except _fs.MoveFailed as exc:
            if not _fs.exists(_fs.loc(destination.parent)):
                raise OwnerLost(f"{destination.parent} is gone: owner {self.owner.owner_id} was recovered") from exc
            raise
        self._retire()

    def discard_subtree(self, relative: str | PurePosixPath) -> bool:
        """Remove one entry below the job directory; a symlink is removed, never followed.

        Quiescence is the caller's concern: the manager removes ``attempts/<A>`` after :meth:`end_attempt`.

        :param relative: A normalized relative POSIX path below the job, outside ``job.json``, ``state.json``
            and ``logs``.
        :return: ``False`` when the entry is absent.
        :raises ValueError: For a malformed path or a trusted name.
        :raises httk.workflow._fs.UnsafePath: When a parent component is a symlink or not a directory.
        :raises OwnerLost: When the job directory is gone.
        """

        self._live()
        self._present()
        path = PurePosixPath(relative)
        if (
            path.is_absolute()
            or not path.parts
            or ".." in path.parts
            or str(relative) != path.as_posix()
            or path.parts[0] in _TRUSTED_NAMES
        ):
            raise ValueError(f"not a removable path below the job: {relative!r}")
        # The parents may be job-written: each must be a real directory, so nothing outside the job is reached.
        modes: list[int] = []
        for depth in range(1, len(path.parts) + 1):
            try:
                modes.append(os.lstat(self.path.joinpath(*path.parts[:depth])).st_mode)
            except (FileNotFoundError, NotADirectoryError):
                return False
            if depth < len(path.parts) and not stat.S_ISDIR(modes[-1]):
                raise _fs.UnsafePath(
                    f"{self.path.joinpath(*path.parts[:depth])} is a symlink or not a directory", "not_directory"
                )
        target = self.path.joinpath(*path.parts)
        if stat.S_ISDIR(modes[-1]) or stat.S_ISREG(modes[-1]):
            self.owner.discard_tree(target)
        else:
            _fs.remove_file(_fs.loc(target), durable=self.owner.workspace.durable)
        return True

    def remove_leftover_temporaries(self) -> None:
        """Remove write temporaries a crashed writer left beside ``state.json`` and in ``logs/``."""

        self._live()
        for directory, pattern in ((self.path, _STATE_TEMPORARY), (self.path / "logs", _LOG_TEMPORARY)):
            for name in list_names(directory):
                if not pattern.fullmatch(name):
                    continue
                try:
                    mode = os.lstat(directory / name).st_mode
                except FileNotFoundError:
                    continue
                # A writer leaves a regular file; anything else was planted by the job and must not wedge us.
                if stat.S_ISREG(mode) or stat.S_ISLNK(mode):
                    _fs.remove_file(_fs.loc(directory / name), durable=self.owner.workspace.durable)
                else:
                    _LOGGER.warning("leaving %s: not a write temporary", directory / name)

    def _live(self) -> None:
        if self._released:
            raise ReleasedJobError(f"{self.job_key} was already released from {self.path}")

    def _present(self) -> None:
        self._live()
        # Best effort (§5.2): the tombstone, seen by the next check_alive, is the guarantee.
        if not _fs.exists(_fs.loc(self.path)):
            raise OwnerLost(f"{self.path} is gone: owner {self.owner.owner_id} was recovered")

    def _pending(self, doc: StateDoc | None) -> Release | None:
        target = None if doc is None else doc.release_to
        if target is None or (target.state, target.priority) == (self.from_state, self.from_priority):
            return None
        return target

    def _unpinned(self) -> None:
        # The fail-stop latch (plan WORKFLOW_LOCAL_COMMANDS amendment 16): a job leaves owned/ only once every step
        # of its earlier phases is sealed.
        if self.owner.pinned(self):
            raise WorkflowError(f"{self.job_key} has a step no seal has made durable yet: it stays held")

    def _move_out(self, state: str, placement: PurePosixPath, priority: int) -> JobRef:
        return self.move_out(state, placement, priority)

    def move_out(
        self, state: str, placement: PurePosixPath, priority: int, *, commit: _fs.Commit | None = None
    ) -> JobRef:
        """Move the job out of ``owned/`` to ``jobs/<state>/<placement>/`` under a fresh name and retire the
        handle: the last step of :meth:`release` and :meth:`give_back` (plan WORKFLOW_LOCAL_COMMANDS §5 amendment 14).

        With *commit*, ``owned/<owner-id>/``, the destination and every ancestor of it the move may create are
        ``owner`` obligations of this handle from before the move, recorded in *commit* instead of fsynced.

        :param state: The unowned state.
        :param placement: The job's placement.
        :param priority: The priority of the new name.
        :param commit: The phase's collector, or ``None`` to fsync inline.
        :return: The new reference.
        :raises OwnerLost: When the job directory is gone.
        :raises httk.workflow.errors.WorkflowError: While an attempt runs, or the handle has an unsealed step of an
            earlier phase.
        :raises httk.workflow._fs.UnsafePath: When a placement component is a symlink or not a directory.
        """

        self.require_quiescent()
        self._unpinned()
        owner, durable = self.owner, self.owner.workspace.durable
        target = _state_dir(owner.workspace, state, placement) / format_job_name(
            self.job_key, priority, _fs.fresh_token()
        )
        # §5.3 step 5: move_owned decides by the source alone, so a vanished source must be caught first.
        self._present()
        check_placement(owner.workspace, state, placement)
        if commit is None:
            mark = owner._mark()
            _fs.move_owned(_fs.loc(self.path), _fs.loc(target), durable=durable)
            if durable:
                owner._sealed_by([self.path.parent, target.parent], mark)
        else:
            changed = [self.path.parent, *_entry_directories(target.parent)]
            with owner.obliged(changed, kind="owner", job=self):
                _fs.move_owned(_fs.loc(self.path), _fs.loc(target), durable=False, commit=commit)
        self._retire()
        self.moved_to = JobRef.from_path(target, state=state, placement=placement)
        return self.moved_to

    def give_back(self) -> JobRef:
        """Move the job back to where it was claimed from, unchanged, exactly as recovery would.

        A pending release is applied instead. There is no state write: a release would clear an unfinished
        commit intent, which the next claimant's reconcile must still see (§5.4).

        :return: The reference after the move.
        :raises httk.workflow.errors.WorkflowError: While an attempt runs, if the job is no longer owned, or while
            the handle has a step no seal has made durable yet (the fail-stop latch).
        """

        self.require_quiescent()
        self._unpinned()
        doc = self.read_state()
        if (pending := self._pending(doc)) is not None and doc is not None:
            return self.release(doc, pending)
        return self._move_out(self.from_state, self.placement(), self.from_priority)

    def _retire(self) -> None:
        self._released = True
        with self.owner._lock:
            if self.path in self.owner._building:
                self.owner._retired_while_building.add(self.path)
            if self.owner._held.get(self.path) is self:
                del self.owner._held[self.path]


def _entry_directories(directory: Path) -> list[Path]:
    # The directories whose entries a move into *directory* may change: it and each missing ancestor (created on
    # the way), down from the deepest existing ancestor, which gains the first created one.
    found = [directory]
    current = directory
    while _lstat_mode(current) is None and current != current.parent:
        current = current.parent
        found.append(current)
    return found[::-1]


def _request_id(name: str, job_id: str) -> str | None:
    if not name.startswith(f"{job_id}.") or not name.endswith(".json"):
        return None
    try:
        return canonical_uuid(name[len(job_id) + 1 : -len(".json")], "request id")
    except FormatError:
        return None


def drop_exchange_index(owner: Owner, exchange_name: str, job_id: str, nonce: str | None) -> None:
    """Discard the exchange index entry of *exchange_name*, but only while it indexes *job_id* by *nonce*.

    :param owner: The owner whose trash receives the entry.
    :param exchange_name: The exchange name (the client's job UUID).
    :param job_id: The job the entry must index (an exchange root leaving the workspace).
    :param nonce: The adoption nonce the entry must carry (``None`` for an entry without one): a resubmission
        rekeys to the same job id under another adoption, whose entry is never removed for the earlier one.
    """

    entry = _exchange_entry(owner.workspace, exchange_name)
    index = _read_json_quietly(entry / "index.json")
    # Never remove an entry that indexes another job, or this job for another adoption.
    if index is not None and index.get("job_id") == job_id and index.get("adoption_nonce") == nonce:
        owner.discard_tree(entry)


# -- jobs -----------------------------------------------------------------------------------------------------------


def _disjoint_prefixes(prefixes: Sequence[PurePosixPath]) -> list[PurePosixPath]:
    kept: list[PurePosixPath] = []
    for prefix in sorted({normalize_placement(item) for item in prefixes}, key=lambda item: item.parts):
        if not any(prefix.is_relative_to(other) for other in kept):
            kept.append(prefix)
    return kept


def _walk(workspace: KernelWorkspace, state: str, placement: PurePosixPath, after: tuple[str, ...]) -> Iterator[JobRef]:
    # A sorted depth-first walk yields jobs in the order of their path parts, so a cursor resumes it.
    directory = _state_dir(workspace, state, placement)
    for name in _subdirectories(directory):
        parts = (placement / name).parts
        if parts < after[: len(parts)]:
            continue
        if "~" not in name:
            yield from _walk(workspace, state, placement / name, after)
            continue
        try:
            ref = JobRef.from_path(directory / name, state=state, placement=placement)
        except FormatError:
            _LOGGER.debug("skipping %s: not a job directory name", directory / name)
            continue
        if parts > after:
            yield ref


def list_jobs(
    workspace: KernelWorkspace,
    state: str,
    *,
    prefixes: Sequence[PurePosixPath] = (),
    limit: int | None = None,
    start: str | None = None,
) -> Iterator[JobRef]:
    """Stream the jobs of one unowned state in placement order; every reference is only a hint.

    :param workspace: The workspace.
    :param state: One of :data:`UNOWNED_STATES`.
    :param prefixes: Placement prefixes to restrict the walk to; all placements when empty.
    :param limit: The most references to yield.
    :param start: A :attr:`JobRef.cursor` to resume after.
    :return: The references.
    :raises ValueError: For an owned or unknown state.
    """

    if state not in UNOWNED_STATES:
        raise ValueError(f"not an unowned state: {state!r}")
    after = PurePosixPath(start).parts if start else ()
    roots = _disjoint_prefixes(prefixes) or [_NO_PLACEMENT]
    return itertools.islice((ref for root in roots for ref in _walk(workspace, state, root, after)), limit)


class ListingCache:
    """The directory listings of one pass, keyed by the listed ``(state, placement)`` or ``owned/<id>`` directory."""

    def __init__(self) -> None:
        self._listings: dict[Path, tuple[str, ...]] = {}

    def names(self, directory: Path) -> tuple[str, ...]:
        """Return the sorted names in *directory*, listing it at most once per cache.

        :param directory: The directory.
        :return: Its names; empty when it does not exist.
        """

        listing = self._listings.get(directory)
        if listing is None:
            listing = self._listings[directory] = tuple(list_names(directory))
        return listing


def _scan(
    workspace: KernelWorkspace,
    wanted: set[str],
    placements: Sequence[PurePosixPath],
    include_owned: bool,
    exhaustive: bool,
    cache: ListingCache,
    *,
    final: bool = False,
) -> dict[str, JobRef]:
    found: dict[str, JobRef] = {}

    def match(ref: JobRef) -> None:
        # Within a final scan, a later observation replaces an earlier one: the job has moved since. This is best
        # effort: references are hints, and a duplicate UUID or a late-showing removal from owned/ can still yield
        # a stale reference, as before.
        if ref.job_id in wanted and (final or ref.job_id not in found):
            found[ref.job_id] = ref

    def match_names(directory: Path, names: Sequence[str], build: Callable[[Path], JobRef]) -> None:
        for name in names:
            # The UUID ends the job key (the text before the first "~"): parse only the names that could match.
            if name.partition("~")[0][-36:] in wanted:
                try:
                    match(build(directory / name))
                except FormatError:
                    continue

    def match_owned(listings: ListingCache) -> None:
        root = workspace.jobs / OWNED
        for owner_id in listings.names(root):
            if _OWNER_ID.fullmatch(owner_id):
                directory = root / owner_id
                match_names(directory, list_names(directory), partial(JobRef.from_path, state=OWNED, owner_id=owner_id))

    # The final scan lists owned/ before and after the unowned states: a job claimed or released during it is seen.
    if include_owned and final:
        match_owned(cache)
    for state in UNOWNED_STATES:
        if final and wanted <= found.keys():
            return found
        if exhaustive:
            for ref in list_jobs(workspace, state):
                match(ref)
            continue
        for placement in placements:
            directory = _state_dir(workspace, state, placement)
            match_names(directory, cache.names(directory), partial(JobRef.from_path, state=state, placement=placement))
    if include_owned and not (final and wanted <= found.keys()):
        match_owned(ListingCache() if final else cache)
    return found


class FirstScan(NamedTuple):
    """The first scan of a settled lookup (rule I6), which :func:`scan_final` completes once its deadline has passed.

    :param wanted: The job UUIDs looked for.
    :param placements: The placements listed in each unowned state (with *include_owned* and *exhaustive*, the
        search's coverage).
    :param include_owned: Whether each ``owned/<owner-id>/`` was listed.
    :param exhaustive: Whether every placement was walked.
    :param found: The references the first scan found, by job UUID.
    :param deadline: The :func:`time.monotonic` time from which the final scan may start: the end of the first scan
        plus the workspace's visibility deadline.
    :param final: The first scan was a final one already (a deadline of zero): nothing is scanned again.
    """

    wanted: frozenset[str]
    placements: tuple[PurePosixPath, ...]
    include_owned: bool
    exhaustive: bool
    found: Mapping[str, JobRef]
    deadline: float
    final: bool

    def remaining(self) -> float:
        """Return the seconds left until the final scan may start (zero once the deadline has passed)."""

        return 0.0 if self.final else max(0.0, self.deadline - time.monotonic())


def _first_scan(
    workspace: KernelWorkspace,
    wanted: set[str],
    placements: Sequence[PurePosixPath],
    *,
    include_owned: bool,
    settle: bool,
    exhaustive: bool = False,
    cache: ListingCache | None = None,
) -> FirstScan:
    # The deadline runs from the end of the first scan: a move made during it is visible to the final one.
    final = settle and workspace.visibility_deadline <= 0
    found = _scan(workspace, wanted, placements, include_owned, exhaustive, cache or ListingCache(), final=final)
    deadline = time.monotonic() + workspace.visibility_deadline
    return FirstScan(frozenset(wanted), tuple(placements), include_owned, exhaustive, found, deadline, final)


def scan_first(
    workspace: KernelWorkspace,
    job_ids: Sequence[str],
    *,
    placements: Sequence[PurePosixPath],
    include_owned: bool = True,
) -> FirstScan:
    """Take the first scan of a settled lookup of many jobs; :func:`scan_final` completes it after its deadline.

    Together they are :func:`locate_many` with ``settle=True`` and ``poll=False`` (jobs expected absent), split so
    that the wait between them can be spent elsewhere.

    :param workspace: The workspace.
    :param job_ids: The job UUIDs.
    :param placements: The placements to list in each unowned state.
    :param include_owned: Also list each ``owned/<owner-id>/``.
    :return: The first scan, with what it found and its deadline.
    """

    wanted = {canonical_uuid(job_id, "job_id") for job_id in job_ids}
    normalized = sorted({normalize_placement(item) for item in placements}, key=lambda item: item.parts)
    return _first_scan(workspace, wanted, normalized, include_owned=include_owned, settle=True)


def scan_final(workspace: KernelWorkspace, first: FirstScan, *, observe: bool = False) -> dict[str, JobRef]:
    """Complete the settled lookup *first* began: wait until its deadline, then list the jobs it missed once more,
    in a final scan (``owned/`` before and after the unowned states, fresh listings).

    Observations accumulate: a job the first scan found counts as found, even when the final scan would not see it
    (it moved since; a fresh lookup re-settles such a miss, see :func:`httk.workflow._moving._presence`). When the
    first scan found every job, nothing is listed again.

    :param workspace: The workspace.
    :param first: The first scan (:func:`scan_first`).
    :param observe: Something happened since the first scan that the jobs it missed must be observed after (a
        landing's deferred commit; plan WORKFLOW_TRANSFER_SESSION §7.11, T6c-lite): they are listed once more even
        when the first scan was final already (a deadline of zero).
    :return: The references either scan found, by job UUID (a later observation replaces an earlier one).
    """

    found = dict(first.found)
    missing = first.wanted - found.keys()
    if not missing or (first.final and not observe):
        return found
    while (remaining := first.deadline - time.monotonic()) > 0:
        _pause(remaining)
    scan = _scan(
        workspace, set(missing), first.placements, first.include_owned, first.exhaustive, ListingCache(), final=True
    )
    found |= scan
    return found


def _settled(
    workspace: KernelWorkspace,
    wanted: set[str],
    placements: Sequence[PurePosixPath],
    *,
    include_owned: bool,
    settle: bool,
    exhaustive: bool = False,
    cache: ListingCache | None = None,
    poll: bool = True,
) -> dict[str, JobRef]:
    # I6: absence is concluded only from a listing started after the visibility deadline (the final scan). Polling
    # backs off from _SETTLE_STEP to _SETTLE_CAP; without polling, the misses wait out the deadline once.
    first = _first_scan(
        workspace, wanted, placements, include_owned=include_owned, settle=settle, exhaustive=exhaustive, cache=cache
    )
    if settle and not poll:
        return scan_final(workspace, first)
    found, deadline, final = dict(first.found), first.deadline, first.final
    step = _SETTLE_STEP
    while settle and not final and (missing := wanted - found.keys()):
        if (remaining := deadline - time.monotonic()) > 0:
            _pause(min(step, remaining))
            step = min(2 * step, _SETTLE_CAP)
        final = time.monotonic() >= deadline
        found |= _scan(workspace, missing, placements, include_owned, exhaustive, ListingCache(), final=final)
    return found


def locate(
    workspace: KernelWorkspace,
    job_id: str,
    *,
    placement_hint: PurePosixPath | None,
    include_owned: bool = True,
    settle: bool = False,
    exhaustive: bool = False,
    cache: ListingCache | None = None,
    poll: bool = True,
) -> JobRef | None:
    """Find a job by UUID at its placement in the unowned states, and optionally in every ``owned/<id>/``.

    :param workspace: The workspace.
    :param job_id: The job UUID.
    :param placement_hint: The placement to list; ``None`` lists no unowned state unless *exhaustive*.
    :param include_owned: Also list each ``owned/<owner-id>/``.
    :param settle: Repeat a miss across the visibility deadline before answering ``None``.
    :param exhaustive: Walk every placement (interactive use only).
    :param cache: Listings shared by one pass (not used by the settle retries).
    :param poll: Retry a settled miss with a backing-off pause (a job expected to appear); ``False`` waits out
        the visibility deadline once and lists again (a job expected absent).
    :return: The reference, or ``None``.
    """

    canonical_uuid(job_id, "job_id")
    placements = () if placement_hint is None else (normalize_placement(placement_hint),)
    found = _settled(
        workspace,
        {job_id},
        placements,
        include_owned=include_owned,
        settle=settle,
        exhaustive=exhaustive,
        cache=cache,
        poll=poll,
    )
    return found.get(job_id)


def locate_many(
    workspace: KernelWorkspace,
    job_ids: Sequence[str],
    *,
    placements: Sequence[PurePosixPath],
    settle: bool,
    cache: ListingCache | None = None,
    include_owned: bool = True,
    poll: bool = True,
) -> dict[str, JobRef]:
    """Find many jobs with one listing pass, settled once for all misses.

    :param workspace: The workspace.
    :param job_ids: The job UUIDs.
    :param placements: The placements to list in each unowned state.
    :param settle: Repeat the misses across one visibility deadline.
    :param cache: Listings shared by one pass.
    :param include_owned: Also list each ``owned/<owner-id>/``.
    :param poll: Retry settled misses with a backing-off pause (jobs expected to appear); ``False`` waits out the
        visibility deadline once and lists the misses again (jobs expected absent).
    :return: The references found, by job UUID.
    """

    wanted = {canonical_uuid(job_id, "job_id") for job_id in job_ids}
    normalized = sorted({normalize_placement(item) for item in placements}, key=lambda item: item.parts)
    return _settled(workspace, wanted, normalized, include_owned=include_owned, settle=settle, cache=cache, poll=poll)


def _every_job(workspace: KernelWorkspace) -> Iterator[JobRef]:
    # What an exhaustive locate walks: every unowned state in order, then each owned/<owner-id>/.
    for state in UNOWNED_STATES:
        yield from list_jobs(workspace, state)
    root = workspace.jobs / OWNED
    for owner_id in list_names(root):
        if _OWNER_ID.fullmatch(owner_id):
            yield from _owned_refs(root / owner_id, owner_id)


def _is_directory(path: Path) -> bool:
    try:
        status = _fs.lstat(_fs.loc(path))
    except OSError:
        return False
    return status is not None and stat.S_ISDIR(status.st_mode)


class LocationIndex:
    """Where each job was seen by one listing of a command, kept current with the command's own moves.

    The listing is taken once, when first needed, so the lookups of a many-job command cost one walk of the
    workspace instead of one walk a job (plan WORKFLOW_LOCAL_COMMANDS §2 L1.1, §4.1). Every entry is a hint: an
    unowned entry answers :meth:`locate` only after an ``lstat`` finds its directory still there, and a stale, owned
    or missing entry is answered by :func:`locate` itself, for that job alone, whose answer then replaces the entry.
    A job this command moves is recorded where it went (:meth:`moved`), or forgotten when that is not known.

    :param workspace: The workspace.
    :param listing: Produces the command's one listing, called at most once; by default every job of every unowned
        state, then of every owner, as an exhaustive :func:`locate` walks them.
    :param complete: Whether *listing* lists every job (a command restricted to some states lists only those; the
        jobs outside them are located by the fallback when asked for).
    """

    def __init__(
        self,
        workspace: KernelWorkspace,
        listing: Callable[[], Iterable[JobRef]] | None = None,
        *,
        complete: bool = True,
    ) -> None:
        self.workspace = workspace
        self._listing = listing
        #: Whether the listing lists every job of the workspace.
        self.complete = complete
        self._refs: list[JobRef] | None = None
        self._listed: dict[str, JobRef] = {}
        # A later observation of this command, which overrides the listing: None when the job's place is unknown.
        self._observed: dict[str, JobRef | None] = {}

    def refs(self) -> list[JobRef]:
        """Return the listing in its own order, taking it now if it has not been taken yet.

        :return: The references, as listed (never updated by later observations).
        """

        if self._refs is None:
            listing = self._listing() if self._listing is not None else _every_job(self.workspace)
            self._refs = list(listing)
            for ref in self._refs:
                # The first observation wins, as in an exhaustive locate's walk.
                self._listed.setdefault(ref.job_id, ref)
        return self._refs

    def add(self, refs: Iterable[JobRef]) -> None:
        """Take the entries of another listing of this command too, where the index has none for a job.

        :param refs: The references, as listed (each still only a hint).
        """

        for ref in refs:
            if ref.job_id not in self._observed:
                self._listed.setdefault(ref.job_id, ref)

    def _entry(self, job_id: str) -> JobRef | None:
        if job_id in self._observed:
            return self._observed[job_id]
        self.refs()
        return self._listed.get(job_id)

    def locate(
        self,
        job_id: str,
        *,
        placement_hint: PurePosixPath | None,
        include_owned: bool = True,
        exhaustive: bool = False,
        cache: ListingCache | None = None,
    ) -> JobRef | None:
        """Answer :func:`locate` with these arguments from the index where an ``lstat`` confirms the entry.

        An unowned entry is used when it is where the call would look (any placement when *exhaustive*, else the
        placement *placement_hint* names) and its directory is still there; anything else calls :func:`locate`.

        :param job_id: The job UUID.
        :param placement_hint: As for :func:`locate`.
        :param include_owned: As for :func:`locate`.
        :param exhaustive: As for :func:`locate`.
        :param cache: As for :func:`locate`, used only by a fallback.
        :return: The reference, or ``None``.
        """

        job_id = canonical_uuid(job_id, "job_id")
        entry = self._entry(job_id)
        if (
            entry is not None
            and entry.state != OWNED
            and (exhaustive or (placement_hint is not None and entry.placement == normalize_placement(placement_hint)))
            and _is_directory(entry.path)
        ):
            return entry
        ref = locate(
            self.workspace,
            job_id,
            placement_hint=placement_hint,
            include_owned=include_owned,
            exhaustive=exhaustive,
            cache=cache,
        )
        if ref is not None or exhaustive:
            self._observed[job_id] = ref
        return ref

    def hint(self, job_id: str) -> JobRef | None:
        """Return the entry of a job as listed, or as this command last observed it, without confirming it.

        :param job_id: The job UUID.
        :return: The reference (only a hint: the job may have moved since), or ``None``.
        """

        return self._entry(canonical_uuid(job_id, "job_id"))

    def present(self, job_id: str) -> bool:
        """Report whether the job exists anywhere, as an exhaustive :func:`locate` that finds it would.

        An entry, owned or not, whose directory an ``lstat`` still finds is the job; otherwise the walk decides.

        :param job_id: The job UUID.
        :return: Whether the job was found.
        """

        entry = self._entry(canonical_uuid(job_id, "job_id"))
        if entry is not None and _is_directory(entry.path):
            return True
        return self.locate(job_id, placement_hint=None, exhaustive=True) is not None

    def moved(self, job_id: str, handle: "OwnedJob | None") -> None:
        """Record where this command moved a job: where *handle* released it to, else nowhere known.

        :param job_id: The job UUID.
        :param handle: The job's handle, or ``None`` when the command did not win it.
        """

        self._observed[job_id] = None if handle is None else handle.moved_to


def submit(
    workspace: KernelWorkspace, owner: Owner, staging: Path, *, state: str = "ready", priority: int | None = None
) -> JobRef:
    """Publish a complete payload under a fresh name in ``jobs/<state>/<placement>/``.

    :param workspace: The workspace.
    :param owner: The owner of *staging* (its scratch, or an outcome of its quiescent job).
    :param staging: The payload; its ``job.json`` gives the key, placement and priority.
    :param state: The unowned state to publish into.
    :param priority: The priority to publish under instead of ``job.json``'s (a ``set_priority`` request
        changes only the name, so a returned or moved job carries its priority separately).
    :return: The published reference.
    :raises OwnerLost: When *staging* is gone (recovery took this owner's scratch).
    :raises ValueError: For an owned or unknown state, or a priority outside ``0``-``999``.
    """

    if state not in UNOWNED_STATES:
        raise ValueError(f"not an unowned state: {state!r}")
    # §5.3: move_owned is right because the staging is private; its absence therefore means it was taken.
    if not _fs.exists(_fs.loc(staging)):
        raise OwnerLost(f"{staging} of owner {owner.owner_id} is gone")
    header = _read_header(staging)
    target = _state_dir(workspace, state, header.placement) / format_job_name(
        header.job_key, header.priority if priority is None else priority, _fs.fresh_token()
    )
    check_placement(workspace, state, header.placement)
    _fs.move_owned(_fs.loc(staging), _fs.loc(target), durable=workspace.durable)
    return JobRef.from_path(target, state=state, placement=header.placement)


def _prune_placement(workspace: KernelWorkspace, state: str, placement: PurePosixPath) -> None:
    # Best effort, deepest first, never jobs/<state>/ itself: any actor may rmdir an empty placement directory, and
    # a concurrent move into a pruned parent recreates it and retries (§4.4). Any failure just stops pruning.
    for depth in range(len(placement.parts), 0, -1):
        try:
            if not _fs.remove_empty_dir(_fs.loc(_state_dir(workspace, state, PurePosixPath(*placement.parts[:depth])))):
                return
        except OSError:
            return


def claim(
    workspace: KernelWorkspace, owner: Owner, ref: JobRef, *, commit: _fs.Commit | None = None
) -> OwnedJob | None:
    """Try to take an unowned job into ``owned/<owner-id>/``.

    A won claim from ``ready/`` then removes the placement directories it emptied, never ``jobs/ready/`` itself.

    With *commit* it is the batched claim step (plan WORKFLOW_LOCAL_COMMANDS §5 amendment 14): both directories
    (and that of a created ``owned/<owner-id>/``) are ``owner`` obligations from before the move, recorded in
    *commit* instead of fsynced, and the pruning is left to the caller (amendment 26: after the release moves).

    :param workspace: The workspace.
    :param owner: The claiming owner.
    :param ref: An unowned reference.
    :param commit: The phase's collector, or ``None`` to fsync inline.
    :return: The quiescent handle, or ``None`` when another actor moved the job first.
    :raises ValueError: For an owned reference (I3).
    :raises httk.workflow.errors.FormatError: If the won job's ``job.json`` is unreadable; the job stays owned.
    :raises httk.workflow._fs.UnsafePath: If the won job's ``job.json`` is not a regular file; it stays owned.
    """

    if ref.state not in UNOWNED_STATES:
        raise ValueError(f"only an unowned job can be claimed: {ref.path}")
    name = format_job_name(ref.job_key, ref.priority, _fs.fresh_token(), ref.state)
    target = _owned_dir(workspace, owner.owner_id) / name
    # §5.3 settle=0: a win misjudged as lost stays in owned/<self>/, where self-healing finds it (§5.4).
    if commit is None:
        mark = owner._mark()
        moved = _fs.move_once(_fs.loc(ref.path), _fs.loc(target), durable=workspace.durable)
        if moved is _fs.Moved.WON and workspace.durable:
            owner._sealed_by([ref.path.parent, target.parent], mark)
    else:
        with owner.obliged([ref.path.parent, *_entry_directories(target.parent)], kind="owner"):
            moved = _fs.move_once(_fs.loc(ref.path), _fs.loc(target), durable=False, commit=commit)
    if moved is _fs.Moved.LOST:
        return None
    if commit is None and ref.state == "ready" and ref.placement is not None:
        _prune_placement(workspace, ref.state, ref.placement)
    # A job claimed from an unowned state has nothing running: recovery returns jobs only after DEAD.
    return owner.adopt_owned(JobRef.from_path(target, state=OWNED, owner_id=owner.owner_id))


# -- recovery and scratch ------------------------------------------------------------------------------------------

_RECONCILERS: dict[str, Callable[[Owner, Path], bool]] = {}

#: Scratch purposes whose content is always safe to discard: nothing in them is the only copy of a job.
DISCARDABLE_PURPOSES = frozenset({"trash", "build", "landing", "release", "claim"})


def register_reconciler(purpose: str, function: Callable[[Owner, Path], bool]) -> None:
    """Register the reconciler of a scratch purpose (``eject`` and ``adopt`` are registered by ``_moving``).

    :param purpose: The scratch purpose.
    :param function: Called with the owner and its scratch; ``True`` when finished, ``False`` to keep it.
    :raises ValueError: For a malformed purpose.
    """

    if not _PURPOSE.fullmatch(purpose):
        raise ValueError(f"not a scratch purpose: {purpose!r}")
    _RECONCILERS[purpose] = function


def reconcile_scratch(owner: Owner, path: Path) -> bool:
    """Finish or discard one of the owner's scratch directories (used by recovery and :meth:`Owner.close`).

    An empty scratch is discarded without calling its reconciler: a purpose whose reconciler must run after a
    step that empties the scratch (an eject's delivery) records what it needs there first (``eject.json``).

    :param owner: The owner the scratch is named after.
    :param path: ``tmp/<owner-id>.<purpose>.<token>/``.
    :return: ``False`` when the scratch is kept: its reconciler could not finish, or its purpose has none
        registered in this process (it may hold the only copy of a job).
    :raises ValueError: When *path* is not this owner's scratch.
    """

    parts = _SCRATCH.fullmatch(path.name)
    if parts is None or parts[1] != owner.owner_id or path.parent != _tmp(owner.workspace):
        raise ValueError(f"{path} is not a scratch directory of owner {owner.owner_id}")
    purpose = parts[2]
    # §5.4: an empty scratch and a discardable purpose are discarded; anything else needs its reconciler.
    if purpose not in DISCARDABLE_PURPOSES and list_names(path):
        reconciler = _RECONCILERS.get(purpose)
        if reconciler is None:
            _LOGGER.error(
                "keeping scratch %s: no reconciler of purpose %r is registered in this process", path, purpose
            )
            return False
        if not reconciler(owner, path):
            return False
    if _fs.exists(_fs.loc(path)):
        owner.discard_tree(path)
    return True


def recover(workspace: KernelWorkspace, owner: Owner, dead_owner_id: str) -> RecoveryReport:
    """Return a tombstoned owner's jobs to their ``from`` states and take over its scratch (§5.4).

    Concurrent recoverers are safe: every move is contested and every removal
    is an ``rmdir`` or the removal of a private copy. ``dead.json`` stays. In a
    durable workspace ``tmp/`` is fsynced before the dead owner's record goes:
    it may have died between a rename out of ``tmp/`` and the fsync sealing it.
    Before anything else, a durable workspace is sealed conservatively (plan
    WORKFLOW_LOCAL_COMMANDS amendments 15 and 27): the dead owner may have died
    between any rename or unlink of its and the commit point sealing it. A seal
    that fails fails the recovery: nothing is returned, reconciled or removed.

    :param workspace: The workspace.
    :param owner: The recovering owner.
    :param dead_owner_id: The owner to recover.
    :return: What this call did.
    :raises httk.workflow.errors.WorkflowError: Without a tombstone, or when entries keep appearing (an
        :class:`~httk.workflow._fs.UnsafePath` when a directory to seal is a symlink or not a directory).
    :raises RecoverySealFailed: When the recovery seal fails, before anything was moved: the recovery did not
        finish, and the owner stays recoverable.
    :raises OSError: When a later step fails (a move's fsync after its rename, say), as before L3a.
    :raises ValueError: For a malformed id or recovering oneself.
    """

    _check_owner_id(dead_owner_id)
    if dead_owner_id == owner.owner_id:
        raise ValueError("an owner cannot recover itself")
    dead_dir = _owner_dir(workspace, dead_owner_id)
    if not _fs.exists(_fs.loc(dead_dir / "dead.json")):
        raise WorkflowError(f"owner {dead_owner_id} has no tombstone; recovery requires dead.json")
    owned_dir, launches = _owned_dir(workspace, dead_owner_id), dead_dir / "launches"
    if workspace.durable:
        _seal_recovery(workspace, dead_owner_id)
    returned: list[JobRef] = []
    quarantined: list[Path] = []
    taken: list[Path] = []
    kept: list[Path] = []
    discarded = 0
    for _ in range(_ROUNDS):
        _return_jobs(workspace, owned_dir, dead_owner_id, returned, quarantined)
        _take_scratch(owner, dead_owner_id, taken, kept)
        for name in list_names(launches):
            owner.discard_tree(launches / name)
            discarded += 1
        # §5.4 step 5: settle once, re-list, and remove by rmdir only; never discard owned/<dead>/.
        _pause(workspace.visibility_deadline)
        if list_names(owned_dir) or list_names(launches) or _dead_scratch(workspace, dead_owner_id):
            continue
        if not _fs.remove_empty_dir(_fs.loc(owned_dir)) and _fs.exists(_fs.loc(owned_dir)):
            continue
        _fs.remove_empty_dir(_fs.loc(launches))
        if workspace.durable:
            # The dead owner may have died between a rename out of tmp/ (its trash's, or a non-durable discard) and
            # the fsync that seals it, leaving no scratch here to take: tmp/ is sealed before its record goes.
            _seal_tmp(workspace)
        _remove_batched_marker(dead_dir, durable=workspace.durable)
        _fs.remove_file(_fs.loc(dead_dir / "owner.json"), durable=workspace.durable)
        _fs.remove_file(_fs.loc(dead_dir / "heartbeat.json"), durable=workspace.durable)
        # Write temporaries of a writer that died (the owner, or a prober attesting) go too; dead.json stays.
        for name in list_names(dead_dir):
            if _OWNER_TEMPORARY.fullmatch(name):
                _fs.remove_file(_fs.loc(dead_dir / name), durable=workspace.durable)
        return RecoveryReport(dead_owner_id, tuple(returned), tuple(quarantined), tuple(taken), tuple(kept), discarded)
    raise WorkflowError(f"owner {dead_owner_id} kept acquiring entries during {_ROUNDS} recovery rounds")


def recoverable_owners(workspace: KernelWorkspace) -> list[str]:
    """List the owners recovery must consider: every owner with a record, and every tombstoned one left with work.

    A recovered owner keeps only ``dead.json``; a falsely attested owner that woke up may still claim into
    ``owned/<id>/`` (or leave a launch or scratch) after its recovery, which this list makes recoverable again.

    :param workspace: The workspace.
    :return: Owner ids, sorted.
    """

    scratch_owners = {parts[1] for name in list_names(_tmp(workspace)) if (parts := _SCRATCH.fullmatch(name))}
    return [
        item.owner_id
        for item in list_owners(workspace)
        if item.record is not None
        or (
            _fs.exists(_fs.loc(item.path / "dead.json"))
            and (
                list_names(_owned_dir(workspace, item.owner_id))
                or list_names(item.path / "launches")
                or item.owner_id in scratch_owners
            )
        )
    ]


def _return_jobs(
    workspace: KernelWorkspace, owned_dir: Path, dead_owner_id: str, returned: list[JobRef], quarantined: list[Path]
) -> None:
    durable = workspace.durable
    for name in list_names(owned_dir):
        path = owned_dir / name
        try:
            ref = JobRef.from_path(path, state=OWNED, owner_id=dead_owner_id)
            header = _read_header(path, ref.job_key)
            assert ref.from_state is not None
            check_placement(workspace, ref.from_state, header.placement)
        except (WorkflowError, OSError):
            if not _fs.exists(_fs.loc(path)):
                # A concurrent recoverer moved it after the listing: its job.json was not unreadable.
                continue
            # §5.4 step 1: without a readable, usable placement the job goes to quarantine, contested like any
            # move, so one bad job never stops the recovery of the others.
            target = workspace.control / "quarantine" / f"{int(time.time())}-{_fs.fresh_token()}" / "entry"
            if _fs.move_once(_fs.loc(path), _fs.loc(target), durable=durable) is _fs.Moved.WON:
                quarantined.append(target)
            else:
                # The losing move created only its fresh parent; nothing else can be in it.
                _fs.remove_empty_dir(_fs.loc(target.parent))
            continue
        target = _state_dir(workspace, ref.from_state, header.placement) / format_job_name(
            ref.job_key, ref.priority, _fs.fresh_token()
        )
        # WON and LOST are both fine: LOST means a concurrent recoverer returned it. No job passes scratch.
        if _fs.move_once(_fs.loc(path), _fs.loc(target), durable=durable) is _fs.Moved.WON:
            returned.append(JobRef.from_path(target, state=ref.from_state, placement=header.placement))


def _dead_scratch(workspace: KernelWorkspace, dead_owner_id: str) -> list[str]:
    return [
        name
        for name in list_names(_tmp(workspace))
        if (parts := _SCRATCH.fullmatch(name)) and parts[1] == dead_owner_id
    ]


def _seal_recovery(workspace: KernelWorkspace, dead_owner_id: str) -> None:
    """Seal every entry change the dead owner may have left unsealed, before recovery exposes, reconciles or removes
    anything (plan WORKFLOW_LOCAL_COMMANDS amendment 27).

    For every dead owner: ``requests/``, ``owned/<dead>/``, each job directory held there (a batched state replace
    may be unsealed in it), each of its ``tmp/<dead>.*`` scratch directories and ``tmp/``, at O(held jobs). Only for
    one whose batched marker exists, also each unowned state's directory and every placement directory below it,
    at O(placements): a serial owner seals its moves inline. One ``syncfs`` per filesystem where the protocol
    predicate holds (:func:`httk.workflow._fs.seal_directories`). Every directory is inspected strictly: an error
    other than absence fails the seal, and with it this recovery.

    :param workspace: The workspace.
    :param dead_owner_id: The dead owner.
    :raises RecoverySealFailed: When the seal fails or a directory cannot be inspected.
    :raises httk.workflow._fs.UnsafePath: When a directory to seal is a symlink or not a directory.
    """

    try:
        _seal_recovery_directories(workspace, dead_owner_id)
    except OSError as exc:
        raise RecoverySealFailed(exc.errno, f"the recovery seal of owner {dead_owner_id} failed: {exc}") from exc


def _seal_recovery_directories(workspace: KernelWorkspace, dead_owner_id: str) -> None:
    owned, tmp = _owned_dir(workspace, dead_owner_id), _tmp(workspace)
    directories = [_requests_dir(workspace), owned]
    directories += [owned / name for name in list_names(owned) if _strict_directory(owned / name)]
    directories += [tmp / name for name in _dead_scratch(workspace, dead_owner_id) if _strict_directory(tmp / name)]
    directories.append(tmp)
    if _strict_exists(_owner_dir(workspace, dead_owner_id) / _BATCHED_MARKER):
        for state in UNOWNED_STATES:
            root = _state_dir(workspace, state)
            if _strict_directory(root):
                directories += [root, *sorted(_placements_bottom_up(root))]
    _fs.seal_directories(directories, _io_sync(workspace), workers=_io_workers(workspace))


def _remove_batched_marker(owner_dir: Path, *, durable: bool) -> None:
    # The batched marker goes with the owner record (close, recovery); an absent one costs nothing (no fsync).
    marker = _fs.loc(owner_dir / _BATCHED_MARKER)
    if _fs.exists(marker):
        _fs.remove_file(marker, durable=durable)


def _strict_status(path: Path) -> os.stat_result | None:
    # lstat that answers None only for absence; any other error is a seal error, never "not there".
    try:
        return os.lstat(path)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ESTALE):
            return None
        raise _fs.SyncFailed(exc.errno, f"cannot inspect {path} to seal it: {exc.strerror}") from exc


def _strict_directory(path: Path) -> bool:
    status = _strict_status(path)
    return status is not None and stat.S_ISDIR(status.st_mode)


def _strict_exists(path: Path) -> bool:
    return _strict_status(path) is not None


def _seal_tmp(workspace: KernelWorkspace) -> None:
    """Fsync the control ``tmp/``, making every rename into or out of it durable; nothing when it does not exist."""

    try:
        descriptor = _fs.open_dir(_tmp(workspace))
    except FileNotFoundError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _take_scratch(owner: Owner, dead_owner_id: str, taken: list[Path], kept: list[Path]) -> None:
    tmp = _tmp(owner.workspace)
    for name in _dead_scratch(owner.workspace, dead_owner_id):
        purpose = name.split(".")[1]
        # A fresh token, so the destination is fresh by construction whatever else the recoverer holds.
        target = tmp / f"{owner.owner_id}.{purpose}.{_fs.fresh_token()}"
        # §5.4 step 2: renaming to the recoverer's name makes exactly one recoverer reconcile it.
        if _fs.move_once(_fs.loc(tmp / name), _fs.loc(target), durable=owner.workspace.durable) is _fs.Moved.WON:
            (taken if reconcile_scratch(owner, target) else kept).append(target)


def take(workspace: KernelWorkspace, owner: Owner, src: _fs.Loc, purpose: str) -> Path | None:
    """Win an entry several actors may grab by moving it into a fresh scratch of this owner.

    :param workspace: The workspace.
    :param owner: The taking owner.
    :param src: The contested entry (anchored for a client-writable directory).
    :param purpose: The scratch purpose.
    :return: The scratch holding the entry under its own name, or ``None`` when another actor took it.
    :raises OwnerLost: When a recoverer took the new scratch before the move.
    """

    scratch = owner.scratch(purpose)
    target = _fs.loc(scratch / src.name())
    try:
        # Never recreate the scratch: a recoverer may have taken it.
        if _fs.move_once(src, target, durable=workspace.durable, create_parents=False) is _fs.Moved.WON:
            return scratch
    except _fs.MoveFailed as exc:
        if not _fs.exists(_fs.loc(scratch)):
            raise OwnerLost(f"{scratch} is gone: owner {owner.owner_id} was recovered") from exc
        raise
    if _fs.remove_empty_dir(_fs.loc(scratch)) or not _fs.exists(_fs.loc(scratch)):
        # Removed, or already taken by a recoverer: either way the entry is not ours.
        return None
    # A won move not yet visible leaves the scratch non-empty: then it is ours after all.
    return scratch


# -- the exchange index ---------------------------------------------------------------------------------------------


class ExchangeClaim(enum.Enum):
    """The outcome of :func:`claim_exchange_name`."""

    #: This call created the entry.
    WON = "won"
    #: The entry carries the caller's adoption nonce: an earlier run of the same adoption created it.
    SAME = "same"
    #: Another adoption (or another job) holds the name.
    OTHER = "other"


def claim_exchange_name(
    workspace: KernelWorkspace,
    owner: Owner,
    exchange_name: str,
    job_id: str,
    placement: PurePosixPath,
    *,
    nonce: str,
    owner_key: str | None = None,
) -> ExchangeClaim:
    """Create the exchange index entry ``exchange-jobs/<name>/`` at most once, before the job is submitted.

    :param workspace: The workspace.
    :param owner: The adopting owner.
    :param exchange_name: The client's name for the job (a UUID).
    :param job_id: The job UUID.
    :param placement: The job's placement.
    :param nonce: The adoption's own nonce, kept in its scratch, so that a reconciler rerun of the same adoption
        recognizes its entry and a concurrent adopter of another copy does not.
    :param owner_key: The optional key that owns control of the adopted root; SAME also requires it to match.
    :return: Whether the entry was created here, by an earlier run of this adoption, or by another one.
    """

    job_id = canonical_uuid(job_id, "job_id")
    entry, durable = _exchange_entry(workspace, exchange_name), workspace.durable
    staging = owner.scratch("claim")
    publish_nonce = f"{owner.owner_id}.{_fs.fresh_token()}".encode()
    _fs.write_file(_fs.loc(staging / ".nonce"), publish_nonce, durable=durable)
    index: dict[str, object] = {
        "job_id": job_id,
        "placement": placement_text(normalize_placement(placement)),
        "adoption_nonce": nonce,
    }
    if owner_key is not None:
        index["owner_key"] = owner_key
    _fs.write_file(_fs.loc(staging / "index.json"), _encode(index), durable=durable)
    # publish_dir removes the staging whether it won or lost.
    if _fs.publish_dir(_fs.loc(staging), _fs.loc(entry), nonce=publish_nonce, durable=durable):
        return ExchangeClaim.WON
    value = _read_json_quietly(entry / "index.json")
    existing = _parse_exchange_index(value)
    key_matches = value is not None and value.get("owner_key") == owner_key
    return (
        ExchangeClaim.SAME
        if existing == (job_id, normalize_placement(placement), nonce) and key_matches
        else ExchangeClaim.OTHER
    )


def exchange_index(workspace: KernelWorkspace, exchange_name: str) -> tuple[str, PurePosixPath, str | None] | None:
    """Read the exchange index entry of *exchange_name*.

    :param workspace: The workspace.
    :param exchange_name: The client's name for the job.
    :return: The indexed job UUID, its placement and the adoption nonce (``None`` when the entry has none), or
        ``None`` when there is no readable entry.
    """

    value = _read_json_quietly(_exchange_entry(workspace, exchange_name) / "index.json")
    return _parse_exchange_index(value)


def _parse_exchange_index(value: dict[str, object] | None) -> tuple[str, PurePosixPath, str | None] | None:
    """Parse the public fields of one already-read exchange index entry."""

    if value is None:
        return None
    try:
        job_id = canonical_uuid(value.get("job_id"), "job_id")
        placement = normalize_placement(str(value["placement"]))
    except (FormatError, ValueError, KeyError):
        return None
    nonce = value.get("adoption_nonce")
    return job_id, placement, nonce if isinstance(nonce, str) else None


def exchange_owner_key(workspace: KernelWorkspace, exchange_name: str, *, job_id: str, nonce: str) -> str | None:
    """The key that submitted the exchange root *job_id* under adoption *nonce*, or ``None``.

    :param workspace: The workspace.
    :param exchange_name: The client's name for the job.
    :param job_id: The adopted root's job UUID.
    :param nonce: The adoption nonce recorded in the trusted index.
    :return: The indexed key when the entry matches, or ``None``.
    """

    value = _read_json_quietly(_exchange_entry(workspace, exchange_name) / "index.json")
    indexed = _parse_exchange_index(value)
    try:
        expected_job_id = canonical_uuid(job_id, "job_id")
    except (FormatError, ValueError):
        return None
    if indexed is None or indexed[0] != expected_job_id or indexed[2] != nonce or value is None:
        return None
    key = value.get("owner_key")
    return key if isinstance(key, str) else None


# -- requests, placements and shared memory -------------------------------------------------------------------------


def request_file(workspace: KernelWorkspace, doc: Mapping[str, object]) -> Path:
    """Return the file a request document is posted as, ``requests/<job-uuid>.<request-uuid>.json``.

    :param workspace: The workspace.
    :param doc: The request document; its ``job_id`` and ``request_id`` name the file.
    :return: The request file.
    """

    job_id = canonical_uuid(doc.get("job_id"), "job_id")
    request_id = canonical_uuid(doc.get("request_id"), "request_id")
    return _requests_dir(workspace) / f"{job_id}.{request_id}.json"


def post_request(workspace: KernelWorkspace, doc: Mapping[str, object]) -> Path:
    """Write a request onto its unique name ``requests/<job-uuid>.<request-uuid>.json``.

    :param workspace: The workspace.
    :param doc: The request document; its ``job_id`` and ``request_id`` name the file.
    :return: The request file.
    """

    path = request_file(workspace, doc)
    _fs.make_dirs(path.parent, durable=workspace.durable)
    # A unique name, or a deterministic id with equivalent content (§8.5): write_file is idempotent.
    _fs.write_file(_fs.loc(path), _encode(dict(doc)), durable=workspace.durable)
    return path


def stage_request(workspace: KernelWorkspace, doc: Mapping[str, object], *, commit: _fs.Commit) -> _fs.Staged:
    """Write a request beside its unique name, the first step of a batched :func:`post_request` (plan
    WORKFLOW_LOCAL_COMMANDS §2 L3, phase A): :func:`httk.workflow._fs.replace_staged` then publishes it.

    :param workspace: The workspace; ``requests/`` exists.
    :param doc: The request document; its ``job_id`` and ``request_id`` name the file.
    :param commit: The phase's collector, which records the temporary's writing descriptor instead of fsyncing it.
    :return: The staged temporary; it is removed when this raises.
    """

    return _fs.stage_file(_fs.loc(request_file(workspace, doc)), _encode(dict(doc)), durable=False, commit=commit)


def _placements_bottom_up(directory: Path) -> list[Path]:
    found: list[Path] = []
    for name in _subdirectories(directory):
        # A name with "~" is a job directory (or a damaged one): never a placement, never entered.
        if "~" not in name:
            found += _placements_bottom_up(directory / name)
            found.append(directory / name)
    return found


def prune_empty_placements(workspace: KernelWorkspace, *, budget: int) -> int:
    """Remove empty placement directories of the unowned states, one ``rmdir`` attempt each, children first.

    :param workspace: The workspace.
    :param budget: The most ``rmdir`` attempts.
    :return: The number of directories removed.
    """

    attempts = removed = 0
    for state in UNOWNED_STATES:
        for directory in _placements_bottom_up(_state_dir(workspace, state)):
            if attempts >= budget:
                return removed
            attempts += 1
            # A concurrent move into a pruned parent recreates it and retries (§4.4).
            removed += _fs.remove_empty_dir(_fs.loc(directory))
    return removed


def _recorded_shm_tokens(workspace: KernelWorkspace) -> set[str] | None:
    tokens: set[str] = set()
    owners = workspace.control / "owners"
    for owner_id in list_names(owners):
        launches = owners / owner_id / "launches"
        for name in list_names(launches):
            try:
                record = _read_json(launches / name / "launch.json", _RECORD_LIMIT)
            except (WorkflowError, OSError):
                record = {"token": None}
            if record is None:
                # A runner launch (n = 0) has no launch.json and no shared memory.
                continue
            if not isinstance(token := record.get("token"), str):
                # A record that cannot be read names an unknown token: sweep nothing.
                _LOGGER.warning("unreadable launch record %s; not sweeping shared memory", launches / name)
                return None
            tokens.add(token)
    return tokens


def sweep_unrecorded_shm(workspace: KernelWorkspace, shm_root: Path) -> int:
    """Remove ``httk-<token>/`` shared-memory directories that no launch record names (§7.9).

    :param workspace: The workspace.
    :param shm_root: The node-local shared-memory root.
    :return: The number of directories removed.
    """

    # Candidates first: a directory listed now existed before both record listings below.
    candidates = [name for name in _subdirectories(shm_root) if _SHM_NAME.fullmatch(name)]
    if not candidates:
        return 0
    first = _recorded_shm_tokens(workspace)
    _pause(workspace.visibility_deadline)
    second = _recorded_shm_tokens(workspace)
    if first is None or second is None:
        return 0
    removed = 0
    for name in candidates:
        if name[len("httk-") :] not in first | second:
            _fs.discard(_fs.loc(shm_root / name), trash_dir=shm_root, durable=False)
            removed += 1
    return removed
