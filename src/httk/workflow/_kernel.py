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

import dataclasses
import itertools
import json
import logging
import os
import re
import stat
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType, TracebackType
from typing import NamedTuple, Protocol, Self

from httk.workflow import _death, _fs
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
    "OWNED",
    "TERMINAL_STATES",
    "UNOWNED_STATES",
    "JobHeader",
    "JobName",
    "JobRef",
    "KernelWorkspace",
    "ListingCache",
    "OwnedJob",
    "Owner",
    "OwnerDeclaredDead",
    "OwnerLost",
    "OwnerRecord",
    "RecoveryReport",
    "Release",
    "ReleasedJobError",
    "attest_dead",
    "claim",
    "claim_exchange_name",
    "exchange_translation_applied",
    "format_job_name",
    "list_jobs",
    "list_owners",
    "locate",
    "locate_many",
    "parse_job_name",
    "post_request",
    "probe_owner",
    "prune_empty_placements",
    "reconcile_scratch",
    "record_exchange_translation",
    "recover",
    "register_owner",
    "register_reconciler",
    "submit",
    "sweep_unrecorded_shm",
    "take",
]

_LOGGER = logging.getLogger(__name__)

#: The state of a job directory below ``jobs/owned/<owner-id>/``.
OWNED = "owned"
_OWNER_KINDS = ("manager", "cli", "daemon")
_ADVERTISED = frozenset({"pools", "capabilities", "prefixes", "resources"})
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
_EVIDENCE_KEYS = frozenset({"subject", "rule", "detail"})
_JOB_LIMIT = 8 << 20
_RECORD_LIMIT = 1 << 20
_ROUNDS = 8
_SETTLE_STEP = 0.1
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

    :param state: One of :data:`UNOWNED_STATES`, or :data:`OWNED`.
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
        """The ``start`` value of :func:`list_jobs` that resumes after this job."""

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


def _names(directory: Path) -> list[str]:
    try:
        return sorted(os.listdir(directory))
    except (FileNotFoundError, NotADirectoryError):
        return []


def _subdirectories(directory: Path) -> list[str]:
    try:
        with os.scandir(directory) as listing:
            return sorted(entry.name for entry in listing if entry.is_dir(follow_symlinks=False))
    except (FileNotFoundError, NotADirectoryError):
        return []


def _read_json(path: Path, limit: int) -> dict[str, object] | None:
    data = _fs.read_bounded(_fs.loc(path), limit)
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
    value = _read_json(directory / "job.json", _JOB_LIMIT)
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


def _check_placement(workspace: KernelWorkspace, state: str, placement: PurePosixPath) -> None:
    # §3.1: placement directories are real directories. _fs checks only the deepest existing ancestor, so a
    # symlinked component whose target already holds the rest would be followed out of jobs/.
    root = _state_dir(workspace, state)
    for depth in range(len(placement.parts) + 1):
        current = root.joinpath(*placement.parts[:depth])
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(mode):
            raise _fs.UnsafePath(f"placement component {current} is a symlink or not a directory")


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
) -> "Owner":
    """Create ``owners/<owner-id>/`` and its ``owner.json``, before the owner claims anything.

    :param workspace: The workspace.
    :param kind: ``"manager"``, ``"cli"`` or ``"daemon"``.
    :param label: A human-readable label, or ``None``.
    :param allocation: The recorded allocation (``RecordedAllocation.as_json()``), or ``None``.
    :param advertised: Any of ``pools``, ``capabilities``, ``prefixes`` and ``resources``.
    :return: The new owner.
    :raises ValueError: For an unknown kind or advertised member.
    """

    if kind not in _OWNER_KINDS:
        raise ValueError(f"owner kind must be one of {_OWNER_KINDS}: {kind!r}")
    if unknown := set(advertised) - _ADVERTISED:
        raise ValueError(f"unknown advertised owner members: {sorted(unknown)}")
    owner_id, identity = uuid.uuid4().hex, _death.process_identity()
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
        **advertised,
    }
    path = _owner_dir(workspace, owner_id)
    os.makedirs(path.parent, exist_ok=True)
    os.mkdir(path)
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
        for name in _names(root)
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
    os.makedirs(path, exist_ok=True)
    _fs.write_file(_fs.loc(path / "dead.json"), _encode(tombstone), durable=workspace.durable)


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
        self._held: dict[Path, OwnedJob] = {}
        self._trash: Path | None = None

    def __enter__(self) -> Self:
        """Return the owner."""

        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Close on a normal exit; on an interrupt, return quiescent jobs first; otherwise touch nothing.

        :param exc_type: The exception type, if any.
        :param exc: The exception, if any.
        :param traceback: The traceback, if any.
        """

        if exc_type is None:
            self.close()
            return
        if not issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
            # Fail-stop (§5.2): an error leaves every owned job for recovery once this process is proven dead.
            return
        try:
            for job in list(self._held.values()):
                if job._attempt is None:
                    job._return()
            self.close()
        except Exception:
            _LOGGER.warning("owner %s could not return its jobs on interrupt", self.owner_id, exc_info=True)

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
        os.makedirs(path.parent, exist_ok=True)
        os.mkdir(path, 0o700)
        return path

    def launch_dir(self, attempt_id: str, n: str) -> Path:
        """Create ``owners/<owner-id>/launches/<attempt>.<n>/`` for one launch's records.

        :param attempt_id: The attempt UUID.
        :param n: ``"0"`` for the attempt runner, or a launch request id (32 hex digits).
        :return: The new directory.
        :raises ValueError: For a malformed attempt id or *n*.
        """

        path = self._launch_path(attempt_id, n)
        os.makedirs(path.parent, exist_ok=True)
        os.mkdir(path)
        return path

    def remove_launch(self, attempt_id: str, n: str) -> None:
        """Discard one launch directory once its launch is reaped.

        :param attempt_id: The attempt UUID.
        :param n: The launch's ``n``.
        """

        self._discard(self._launch_path(attempt_id, n))

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
        """

        if ref.state != OWNED or ref.owner_id != self.owner_id:
            raise ValueError(f"{ref.path} is not owned by {self.owner_id}")
        # One handle per job and process: the quiescence bookkeeping must not be forked.
        if (held := self._held.get(ref.path)) is not None:
            return held
        if not _fs.exists(_fs.loc(ref.path)):
            raise OwnerLost(f"{ref.path} left owned/{self.owner_id}/")
        job = self._held[ref.path] = OwnedJob(self, ref)
        return job

    def close(self) -> None:
        """Return unheld owned jobs, reconcile scratch and remove this owner's directories when nothing is left.

        :raises httk.workflow.errors.WorkflowError: While this process holds jobs, or when owned jobs remain.
        """

        if self._held:
            raise WorkflowError(f"owner {self.owner_id} still holds {len(self._held)} jobs")
        # §5.2 self-healing: jobs in owned/<self>/ this process never held go back unchanged; nothing launches.
        for ref in self.owned():
            self.adopt_owned(ref)._return()
        if self.owned():
            raise WorkflowError(f"owner {self.owner_id} still owns jobs after self-healing")
        workspace = self.workspace
        kept = [
            path for path in _own_scratch(workspace, self.owner_id, self._trash) if not reconcile_scratch(self, path)
        ]
        _fs.remove_empty_dir(_fs.loc(_owned_dir(workspace, self.owner_id)))
        launches = self.path / "launches"
        # The trash goes before owner.json: an owner-named scratch must never outlive its owner's record.
        if self._trash is not None:
            reconcile_scratch(self, self._trash)
        if not kept and not _names(launches):
            _fs.remove_file(_fs.loc(self.path / "owner.json"), durable=workspace.durable)
            _fs.remove_file(_fs.loc(self.path / "heartbeat.json"), durable=workspace.durable)
            _fs.remove_empty_dir(_fs.loc(launches))
            _fs.remove_empty_dir(_fs.loc(self.path))

    def _launch_path(self, attempt_id: str, n: str) -> Path:
        canonical_uuid(attempt_id, "attempt_id")
        if not _LAUNCH_N.fullmatch(n):
            raise ValueError(f"launch n must be '0' or 32 lowercase hex digits: {n!r}")
        return self.path / "launches" / f"{attempt_id}.{n}"

    def _discard(self, path: Path) -> None:
        # Directories only (single files use _fs.remove_file). A removal moves the tree into this owner's trash scratch first, so a crash mid-removal leaves an
        # owner-named scratch that close or recovery reconciles. The trash itself goes to the tmp root.
        if path == self._trash:
            self._trash, trash_dir = None, path.parent
        else:
            if self._trash is None or not _fs.exists(_fs.loc(self._trash)):
                self._trash = self.scratch("trash")
            trash_dir = self._trash
        _fs.discard(_fs.loc(path), trash_dir=trash_dir, durable=self.workspace.durable)


def _owned_refs(directory: Path, owner_id: str) -> list[JobRef]:
    refs = []
    for name in _names(directory):
        try:
            refs.append(JobRef.from_path(directory / name, state=OWNED, owner_id=owner_id))
        except FormatError:
            _LOGGER.debug("skipping %s: not an owned job name", directory / name)
    return refs


def _own_scratch(workspace: KernelWorkspace, owner_id: str, trash: Path | None) -> list[Path]:
    tmp = _tmp(workspace)
    return [
        tmp / name
        for name in _names(tmp)
        if (parts := _SCRATCH.fullmatch(name)) and parts[1] == owner_id and tmp / name != trash
    ]


# -- owned jobs ---------------------------------------------------------------------------------------------------


class OwnedJob:
    """The handle of a job this process won: every write to a job goes through one.

    :param owner: The owner.
    :param ref: The job's reference below ``owned/<owner-id>/``.
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
        self._header: JobHeader | None = None
        self._attempt: str | None = None
        self._retired: set[str] = set()
        self._released = False

    def header(self) -> JobHeader:
        """Return the kernel's view of ``job.json`` (cached; it is immutable).

        :return: The header.
        :raises httk.workflow.errors.FormatError: If ``job.json`` is missing, malformed or names another job.
        """

        self._live()
        if self._header is None:
            self._header = _read_header(self.path, self.job_key)
        return self._header

    def placement(self) -> PurePosixPath:
        """Return the authoritative placement from ``job.json``."""

        return self.header().placement

    def read_state(self) -> StateDoc | None:
        """Return ``state.json``, or ``None`` before the job's first write.

        :return: The document.
        :raises httk.workflow.errors.FormatError: If the document is malformed or names another job.
        """

        self._live()
        data = _fs.read_bounded(_fs.loc(self.path / "state.json"), MAX_STATE_BYTES)
        if data is None:
            return None
        doc = decode_state(data)
        if doc.job_id != self.job_id:
            raise FormatError(f"{self.path}/state.json names job {doc.job_id}")
        return doc

    def write_state(self, doc: StateDoc) -> None:
        """Replace ``state.json``; applied request ids whose files this handle deleted are dropped.

        :param doc: The document.
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
        try:
            _fs.write_file(_fs.loc(self.path / "state.json"), encode_state(doc), durable=self.owner.workspace.durable)
        except FileNotFoundError:
            self._present()
            raise

    def append_log(self, event: str, /, **detail: object) -> None:
        """Append one line to ``logs/runlog.jsonl``.

        :param event: The event name.
        :param **detail: Further JSON members of the line.
        :raises OwnerLost: When the job directory is gone.
        :raises ValueError: For an empty event or a detail that shadows a fixed member.
        """

        self._present()
        if not event or {"at", "event", "owner_id"} & detail.keys():
            raise ValueError("a run-log line needs an event and may not override at/event/owner_id")
        line = _encode({"at": _now(), "event": event, "owner_id": self.owner.owner_id, **detail}) + b"\n"
        logs = self.path / "logs"
        os.makedirs(logs, exist_ok=True)
        _fs.append_file(_fs.loc(logs / "runlog.jsonl"), line, durable=self.owner.workspace.durable)

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

    def request_files(self) -> list[Path]:
        """List this job's pending request files; delete leftover files of applied requests.

        :return: The pending request files, sorted.
        """

        self._live()
        doc = self.read_state()
        applied = set(doc.applied_requests) if doc is not None else set()
        directory, pending, listed = _requests_dir(self.owner.workspace), [], set()
        for name in _names(directory):
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
        directory = _requests_dir(self.owner.workspace)
        for request_id in release.applied_requests:
            _fs.remove_file(
                _fs.loc(directory / f"{self.job_id}.{request_id}.json"), durable=self.owner.workspace.durable
            )
        return self._move_out(release.state, placement, release.priority)

    def discard(self) -> None:
        """Delete the job (the ``delete`` request), its exchange index entry and its request files.

        :raises OwnerLost: When the job directory is gone.
        """

        self._live()
        self.require_quiescent()
        try:
            doc = self.read_state()
        except FormatError:
            # A delete must always apply: an unreadable state.json is treated as not an exchange job.
            _LOGGER.warning("deleting %s with an unreadable state.json", self.path, exc_info=True)
            doc = None
        if doc is not None and doc.origin == "exchange" and doc.exchange_name is not None:
            # §8.5: the index goes first; a crash before the job goes leaves the delete request pending.
            _drop_exchange_index(self.owner, doc.exchange_name, self.job_id)
        self._present()
        self.owner._discard(self.path)
        self._retire()
        directory = _requests_dir(self.owner.workspace)
        for name in _names(directory):
            if _request_id(name, self.job_id) is not None:
                _fs.remove_file(_fs.loc(directory / name), durable=self.owner.workspace.durable)

    def remove_leftover_temporaries(self) -> None:
        """Remove write temporaries a crashed writer left beside ``state.json`` and in ``logs/``."""

        self._live()
        for directory, pattern in ((self.path, _STATE_TEMPORARY), (self.path / "logs", _LOG_TEMPORARY)):
            for name in _names(directory):
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

    def _move_out(self, state: str, placement: PurePosixPath, priority: int) -> JobRef:
        target = _state_dir(self.owner.workspace, state, placement) / format_job_name(
            self.job_key, priority, _fs.fresh_token()
        )
        # §5.3 step 5: move_owned decides by the source alone, so a vanished source must be caught first.
        self._present()
        _check_placement(self.owner.workspace, state, placement)
        _fs.move_owned(_fs.loc(self.path), _fs.loc(target), durable=self.owner.workspace.durable)
        self._retire()
        return JobRef.from_path(target, state=state, placement=placement)

    def _return(self) -> JobRef:
        # Back to from_state without a state write, exactly as recovery would: with_release would clear an
        # unfinished commit intent, which the next claimant's reconcile must still see (§5.4, note §5.4).
        self.require_quiescent()
        doc = self.read_state()
        if (pending := self._pending(doc)) is not None and doc is not None:
            return self.release(doc, pending)
        return self._move_out(self.from_state, self.placement(), self.from_priority)

    def _retire(self) -> None:
        self._released = True
        self.owner._held.pop(self.path, None)


def _request_id(name: str, job_id: str) -> str | None:
    if not name.startswith(f"{job_id}.") or not name.endswith(".json"):
        return None
    try:
        return canonical_uuid(name[len(job_id) + 1 : -len(".json")], "request id")
    except FormatError:
        return None


def _drop_exchange_index(owner: Owner, exchange_name: str, job_id: str) -> None:
    entry = _exchange_entry(owner.workspace, exchange_name)
    index = _read_json_quietly(entry / "index.json")
    # Never remove an entry that indexes another job.
    if index is not None and index.get("job_id") == job_id:
        owner._discard(entry)


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
            listing = self._listings[directory] = tuple(_names(directory))
        return listing


def _scan(
    workspace: KernelWorkspace,
    wanted: set[str],
    placements: Sequence[PurePosixPath],
    include_owned: bool,
    exhaustive: bool,
    cache: ListingCache,
) -> dict[str, JobRef]:
    found: dict[str, JobRef] = {}

    def match(ref: JobRef) -> None:
        if ref.job_id in wanted:
            found.setdefault(ref.job_id, ref)

    for state in UNOWNED_STATES:
        if exhaustive:
            for ref in list_jobs(workspace, state):
                match(ref)
            continue
        for placement in placements:
            directory = _state_dir(workspace, state, placement)
            for name in cache.names(directory):
                try:
                    match(JobRef.from_path(directory / name, state=state, placement=placement))
                except FormatError:
                    continue
    if include_owned:
        root = workspace.jobs / OWNED
        for owner_id in cache.names(root):
            if _OWNER_ID.fullmatch(owner_id):
                for ref in _owned_refs(root / owner_id, owner_id):
                    match(ref)
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
) -> dict[str, JobRef]:
    found = _scan(workspace, wanted, placements, include_owned, exhaustive, cache or ListingCache())
    if not settle:
        return found
    # I6: absence is concluded only from fresh listings spanning the visibility deadline.
    deadline = time.monotonic() + workspace.visibility_deadline
    while (missing := wanted - found.keys()) and (remaining := deadline - time.monotonic()) > 0:
        _pause(min(_SETTLE_STEP, remaining))
        found |= _scan(workspace, missing, placements, include_owned, exhaustive, ListingCache())
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
) -> JobRef | None:
    """Find a job by UUID at its placement in the unowned states, and optionally in every ``owned/<id>/``.

    :param workspace: The workspace.
    :param job_id: The job UUID.
    :param placement_hint: The placement to list; ``None`` lists no unowned state unless *exhaustive*.
    :param include_owned: Also list each ``owned/<owner-id>/``.
    :param settle: Repeat a miss across the visibility deadline before answering ``None``.
    :param exhaustive: Walk every placement (interactive use only).
    :param cache: Listings shared by one pass (not used by the settle retries).
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
    )
    return found.get(job_id)


def locate_many(
    workspace: KernelWorkspace,
    job_ids: Sequence[str],
    *,
    placements: Sequence[PurePosixPath],
    settle: bool,
    cache: ListingCache | None = None,
) -> dict[str, JobRef]:
    """Find many jobs with one listing pass, settled once for all misses (owned jobs included).

    :param workspace: The workspace.
    :param job_ids: The job UUIDs.
    :param placements: The placements to list in each unowned state.
    :param settle: Repeat the misses across one visibility deadline.
    :param cache: Listings shared by one pass.
    :return: The references found, by job UUID.
    """

    wanted = {canonical_uuid(job_id, "job_id") for job_id in job_ids}
    normalized = sorted({normalize_placement(item) for item in placements}, key=lambda item: item.parts)
    return _settled(workspace, wanted, normalized, include_owned=True, settle=settle, cache=cache)


def submit(workspace: KernelWorkspace, owner: Owner, staging: Path, *, state: str = "ready") -> JobRef:
    """Publish a complete payload under a fresh name in ``jobs/<state>/<placement>/``.

    :param workspace: The workspace.
    :param owner: The owner of *staging* (its scratch, or an outcome of its quiescent job).
    :param staging: The payload; its ``job.json`` gives the key, placement and priority.
    :param state: The unowned state to publish into.
    :return: The published reference.
    :raises OwnerLost: When *staging* is gone (recovery took this owner's scratch).
    :raises ValueError: For an owned or unknown state.
    """

    if state not in UNOWNED_STATES:
        raise ValueError(f"not an unowned state: {state!r}")
    # §5.3: move_owned is right because the staging is private; its absence therefore means it was taken.
    if not _fs.exists(_fs.loc(staging)):
        raise OwnerLost(f"{staging} of owner {owner.owner_id} is gone")
    header = _read_header(staging)
    target = _state_dir(workspace, state, header.placement) / format_job_name(
        header.job_key, header.priority, _fs.fresh_token()
    )
    _check_placement(workspace, state, header.placement)
    _fs.move_owned(_fs.loc(staging), _fs.loc(target), durable=workspace.durable)
    return JobRef.from_path(target, state=state, placement=header.placement)


def claim(workspace: KernelWorkspace, owner: Owner, ref: JobRef) -> OwnedJob | None:
    """Try to take an unowned job into ``owned/<owner-id>/``.

    :param workspace: The workspace.
    :param owner: The claiming owner.
    :param ref: An unowned reference.
    :return: The quiescent handle, or ``None`` when another actor moved the job first.
    :raises ValueError: For an owned reference (I3).
    """

    if ref.state not in UNOWNED_STATES:
        raise ValueError(f"only an unowned job can be claimed: {ref.path}")
    name = format_job_name(ref.job_key, ref.priority, _fs.fresh_token(), ref.state)
    target = _owned_dir(workspace, owner.owner_id) / name
    # §5.3 settle=0: a win misjudged as lost stays in owned/<self>/, where self-healing finds it (§5.4).
    if _fs.move_once(_fs.loc(ref.path), _fs.loc(target), durable=workspace.durable) is _fs.Moved.LOST:
        return None
    # A job claimed from an unowned state has nothing running: recovery returns jobs only after DEAD.
    return owner.adopt_owned(JobRef.from_path(target, state=OWNED, owner_id=owner.owner_id))


# -- recovery and scratch ------------------------------------------------------------------------------------------

_RECONCILERS: dict[str, Callable[[Owner, Path], bool]] = {}


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

    :param owner: The owner the scratch is named after.
    :param path: ``tmp/<owner-id>.<purpose>.<token>/``.
    :return: ``False`` when the reconciler could not finish and the scratch is kept.
    :raises ValueError: When *path* is not this owner's scratch.
    """

    parts = _SCRATCH.fullmatch(path.name)
    if parts is None or parts[1] != owner.owner_id or path.parent != _tmp(owner.workspace):
        raise ValueError(f"{path} is not a scratch directory of owner {owner.owner_id}")
    reconciler = _RECONCILERS.get(parts[2])
    # §5.4: an empty scratch, and build, copy, trash and unknown purposes, are discarded.
    if reconciler is not None and _names(path) and not reconciler(owner, path):
        return False
    if _fs.exists(_fs.loc(path)):
        owner._discard(path)
    return True


def recover(workspace: KernelWorkspace, owner: Owner, dead_owner_id: str) -> RecoveryReport:
    """Return a tombstoned owner's jobs to their ``from`` states and take over its scratch (§5.4).

    Concurrent recoverers are safe: every move is contested and every removal
    is an ``rmdir`` or the removal of a private copy. ``dead.json`` stays.

    :param workspace: The workspace.
    :param owner: The recovering owner.
    :param dead_owner_id: The owner to recover.
    :return: What this call did.
    :raises httk.workflow.errors.WorkflowError: Without a tombstone, or when entries keep appearing.
    :raises ValueError: For a malformed id or recovering oneself.
    """

    _check_owner_id(dead_owner_id)
    if dead_owner_id == owner.owner_id:
        raise ValueError("an owner cannot recover itself")
    dead_dir = _owner_dir(workspace, dead_owner_id)
    if not _fs.exists(_fs.loc(dead_dir / "dead.json")):
        raise WorkflowError(f"owner {dead_owner_id} has no tombstone; recovery requires dead.json")
    owned_dir, launches = _owned_dir(workspace, dead_owner_id), dead_dir / "launches"
    returned: list[JobRef] = []
    quarantined: list[Path] = []
    taken: list[Path] = []
    kept: list[Path] = []
    discarded = 0
    for _ in range(_ROUNDS):
        _return_jobs(workspace, owned_dir, dead_owner_id, returned, quarantined)
        _take_scratch(owner, dead_owner_id, taken, kept)
        for name in _names(launches):
            owner._discard(launches / name)
            discarded += 1
        # §5.4 step 5: settle once, re-list, and remove by rmdir only; never discard owned/<dead>/.
        _pause(workspace.visibility_deadline)
        if _names(owned_dir) or _names(launches) or _dead_scratch(workspace, dead_owner_id):
            continue
        if not _fs.remove_empty_dir(_fs.loc(owned_dir)) and _fs.exists(_fs.loc(owned_dir)):
            continue
        _fs.remove_empty_dir(_fs.loc(launches))
        _fs.remove_file(_fs.loc(dead_dir / "owner.json"), durable=workspace.durable)
        _fs.remove_file(_fs.loc(dead_dir / "heartbeat.json"), durable=workspace.durable)
        # Write temporaries of a writer that died (the owner, or a prober attesting) go too; dead.json stays.
        for name in _names(dead_dir):
            if _OWNER_TEMPORARY.fullmatch(name):
                _fs.remove_file(_fs.loc(dead_dir / name), durable=workspace.durable)
        return RecoveryReport(dead_owner_id, tuple(returned), tuple(quarantined), tuple(taken), tuple(kept), discarded)
    raise WorkflowError(f"owner {dead_owner_id} kept acquiring entries during {_ROUNDS} recovery rounds")


def _return_jobs(
    workspace: KernelWorkspace, owned_dir: Path, dead_owner_id: str, returned: list[JobRef], quarantined: list[Path]
) -> None:
    durable = workspace.durable
    for name in _names(owned_dir):
        path = owned_dir / name
        try:
            ref = JobRef.from_path(path, state=OWNED, owner_id=dead_owner_id)
            header = _read_header(path, ref.job_key)
            assert ref.from_state is not None
            _check_placement(workspace, ref.from_state, header.placement)
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
        name for name in _names(_tmp(workspace)) if (parts := _SCRATCH.fullmatch(name)) and parts[1] == dead_owner_id
    ]


def _take_scratch(owner: Owner, dead_owner_id: str, taken: list[Path], kept: list[Path]) -> None:
    tmp = _tmp(owner.workspace)
    for name in _dead_scratch(owner.workspace, dead_owner_id):
        target = tmp / f"{owner.owner_id}{name[len(dead_owner_id) :]}"
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
    """

    scratch = owner.scratch(purpose)
    target = _fs.loc(scratch / src.name())
    if _fs.move_once(src, target, durable=workspace.durable) is _fs.Moved.WON:
        return scratch
    # A won move not yet visible leaves the scratch non-empty: then it is ours after all.
    return None if _fs.remove_empty_dir(_fs.loc(scratch)) else scratch


# -- the exchange index ---------------------------------------------------------------------------------------------


def claim_exchange_name(
    workspace: KernelWorkspace, owner: Owner, exchange_name: str, job_id: str, placement: PurePosixPath
) -> bool:
    """Create the exchange index entry ``exchange-jobs/<name>/`` at most once, before the job is submitted.

    :param workspace: The workspace.
    :param owner: The adopting owner.
    :param exchange_name: The client's name for the job (a UUID).
    :param job_id: The job UUID.
    :param placement: The job's placement.
    :return: Whether the entry now indexes *job_id* (created here, or by an earlier run for the same job).
    """

    job_id = canonical_uuid(job_id, "job_id")
    entry, durable = _exchange_entry(workspace, exchange_name), workspace.durable
    staging = owner.scratch("claim")
    nonce = f"{owner.owner_id}.{_fs.fresh_token()}".encode()
    _fs.write_file(_fs.loc(staging / ".nonce"), nonce, durable=durable)
    index = {"job_id": job_id, "placement": placement_text(normalize_placement(placement))}
    _fs.write_file(_fs.loc(staging / "index.json"), _encode(index), durable=durable)
    os.mkdir(staging / "translated")
    # publish_dir removes the staging whether it won or lost.
    if _fs.publish_dir(_fs.loc(staging), _fs.loc(entry), nonce=nonce, durable=durable):
        return True
    # §13.2: an adopt reconciler rerun after its adopter died finds its own job's entry; that is not a duplicate.
    existing = _read_json_quietly(entry / "index.json")
    return existing is not None and existing.get("job_id") == job_id


def record_exchange_translation(workspace: KernelWorkspace, exchange_name: str, request_id: str) -> None:
    """Record an applied exchange request id in the index entry (idempotent).

    :param workspace: The workspace.
    :param exchange_name: The client's name for the job.
    :param request_id: The exchange request id.
    :raises httk.workflow.errors.WorkflowError: When the index entry does not exist.
    """

    entry = _exchange_entry(workspace, exchange_name)
    if not _fs.exists(_fs.loc(entry)):
        raise WorkflowError(f"no exchange index entry {entry}")
    target = entry / "translated" / canonical_uuid(request_id, "request_id")
    _fs.write_file(_fs.loc(target), b"", durable=workspace.durable)


def exchange_translation_applied(workspace: KernelWorkspace, exchange_name: str, request_id: str) -> bool:
    """Report whether an exchange request id was recorded as applied.

    :param workspace: The workspace.
    :param exchange_name: The client's name for the job.
    :param request_id: The exchange request id.
    :return: Whether ``translated/<request-id>`` exists.
    """

    entry = _exchange_entry(workspace, exchange_name)
    return _fs.exists(_fs.loc(entry / "translated" / canonical_uuid(request_id, "request_id")))


# -- requests, placements and shared memory -------------------------------------------------------------------------


def post_request(workspace: KernelWorkspace, doc: Mapping[str, object]) -> Path:
    """Write a request onto its unique name ``requests/<job-uuid>.<request-uuid>.json``.

    :param workspace: The workspace.
    :param doc: The request document; its ``job_id`` and ``request_id`` name the file.
    :return: The request file.
    """

    job_id = canonical_uuid(doc.get("job_id"), "job_id")
    request_id = canonical_uuid(doc.get("request_id"), "request_id")
    directory = _requests_dir(workspace)
    os.makedirs(directory, exist_ok=True)
    path = directory / f"{job_id}.{request_id}.json"
    # A unique name, or a deterministic id with equivalent content (§8.5): write_file is idempotent.
    _fs.write_file(_fs.loc(path), _encode(dict(doc)), durable=workspace.durable)
    return path


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
    for owner_id in _names(owners):
        launches = owners / owner_id / "launches"
        for name in _names(launches):
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
