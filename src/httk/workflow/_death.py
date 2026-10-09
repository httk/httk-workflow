"""The death proof: whether an owner's whole execution scope has provably ended.

A read-only oracle. :func:`probe` reads an owner's ``owner.json``, its
``dead.json`` tombstone and its launch records below ``launches/``, and answers
:attr:`Liveness.ALIVE`, :attr:`Liveness.DEAD` or :attr:`Liveness.UNKNOWN` with
the evidence. It writes nothing; the kernel writes the tombstone.

Proof comes only from this host's process table (host name, boot id, pid and
start ticks) or from the batch scheduler (:class:`SchedulerQueries`). No clock
is consulted: neither a heartbeat age nor a recorded allocation end time that
has passed is evidence of death.
"""

import enum
import json
import os
import socket
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import _fs
from ._allocation import RecordedAllocation, Run, allocation_ended, can_ask
from .errors import WorkflowError

_OWNER_FORMAT = "httk-workflow-owner"
_OWNER_FILE = "owner.json"
_TOMBSTONE_FILE = "dead.json"
_LAUNCHES_DIRECTORY = "launches"
_PROCESS_FILE = "process.json"
_RECORD_BYTES = 65536
_PROC_BYTES = 4096
_BOOT_ID = Path("/proc/sys/kernel/random/boot_id")


class Liveness(enum.Enum):
    """What the death proof concludes about an owner."""

    ALIVE = "alive"
    DEAD = "dead"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Evidence:
    """One finding of the death proof.

    :param subject: What the finding is about, such as ``owner <id>`` or ``launch <attempt>.<n>``.
    :param rule: The rule that applied, such as ``tombstone`` or ``allocation_ended``.
    :param detail: A human-readable account of the observation.
    """

    subject: str
    rule: str
    detail: str


@dataclass(frozen=True)
class ProcessIdentity:
    """A process as a record names it: enough to tell it from a later process with the same pid.

    :param hostname: The host the process runs on.
    :param boot_id: That host's boot id, or ``None`` when unknown.
    :param pid: The process id, or the process group id for a group.
    :param start_ticks: When the process started, in clock ticks after boot, or ``None`` when unknown.
    """

    hostname: str
    boot_id: str | None
    pid: int
    start_ticks: int | None


def _read_proc(path: Path) -> bytes | None:
    try:
        return _fs.read_bounded(_fs.loc(path), _PROC_BYTES)
    except (OSError, WorkflowError):
        return None


def _boot_id() -> str | None:
    data = _read_proc(_BOOT_ID)
    text = None if data is None else data.decode("ascii", errors="replace").strip()
    return text or None


def _start_ticks(pid: int) -> int | None:
    data = _read_proc(Path(f"/proc/{pid}/stat"))
    if data is None:
        return None
    try:
        # The command name in field 2 may contain spaces and parentheses; field 3 follows the last ")".
        return int(data[data.rindex(b")") + 2 :].split()[19])
    except (ValueError, IndexError):
        return None


def process_identity(pid: int | None = None) -> ProcessIdentity:
    """Return the identity of a process on this host.

    :param pid: The process id; this process when ``None``.
    :return: This host's name and boot id with the pid and its start ticks (``None`` when unreadable).
    """

    pid = os.getpid() if pid is None else pid
    return ProcessIdentity(socket.gethostname(), _boot_id(), pid, _start_ticks(pid))


def process_gone(recorded: ProcessIdentity, *, here: ProcessIdentity, group: bool = False) -> bool | None:
    """Decide from this host's process table whether a recorded process or process group is gone.

    On the recorded host, a different boot id proves it gone, as does a pid (or
    process group) that no longer exists, or a different process under the pid:
    the kernel does not reuse a pid while a process group of that id exists, so
    a different leader proves the recorded group empty.

    :param recorded: The recorded process; for a group, its ``pid`` is the
        process group id and its ``start_ticks`` the group leader's.
    :param here: This host's identity (its ``hostname`` and ``boot_id`` are compared).
    :param group: Whether *recorded* names a process group rather than a process.
    :return: ``True`` when provably gone, ``False`` when it exists here, ``None``
        for another host or a pid that is not positive.
    """

    if recorded.hostname != here.hostname or recorded.pid <= 0:
        return None
    if recorded.boot_id is not None and here.boot_id is not None and recorded.boot_id != here.boot_id:
        return True
    if recorded.start_ticks is not None:
        started = _start_ticks(recorded.pid)
        if started is not None and started != recorded.start_ticks:
            return True
    try:
        if group:
            os.killpg(recorded.pid, 0)
        else:
            os.kill(recorded.pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        # EPERM: it exists, owned by someone else.
        return False
    return False


class SchedulerQueries:
    """Asks whether recorded allocations have ended, pacing repeated questions.

    Answers are remembered per recorded allocation for *cache_seconds*. That is
    query pacing, not a correctness input.

    :param run: The command runner the scheduler or site probe is asked with.
    :param timeout: Seconds one query may take.
    :param cache_seconds: How long an answer is reused.
    :param clock: The monotonic clock the cache ages by.
    """

    def __init__(
        self,
        *,
        run: Run = subprocess.run,
        timeout: float = 30.0,
        cache_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._run = run
        self._timeout = timeout
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._answers: dict[str, tuple[float, bool | None]] = {}

    def ended(self, recorded: RecordedAllocation) -> bool | None:
        """Return whether a recorded allocation has ended, as the scheduler confirms.

        :param recorded: The allocation a record names.
        :return: ``True`` when the end is confirmed, ``False`` while it is active,
            ``None`` when nobody can tell or the answering client is not installed here.
        """

        now = self._clock()
        for key in [key for key, (asked, _) in self._answers.items() if now - asked >= self._cache_seconds]:
            del self._answers[key]
        key = json.dumps(recorded.as_json(), sort_keys=True)
        if key not in self._answers:
            answer = allocation_ended(recorded, run=self._run, timeout=self._timeout) if can_ask(recorded) else None
            self._answers[key] = (now, answer)
        return self._answers[key][1]


def _read_json(target: _fs.Loc) -> dict[str, Any] | None:
    """Read one bounded JSON object: ``None`` when absent, :class:`ValueError` when unusable."""

    try:
        data = _fs.read_bounded(target, _RECORD_BYTES)
    except (OSError, WorkflowError) as exc:
        raise ValueError(f"cannot read {target.path}: {exc}") from exc
    if data is None:
        return None
    try:
        value = json.loads(data)
    except (ValueError, RecursionError) as exc:
        raise ValueError(f"{target.path} is not JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{target.path} is not a JSON object")
    return value


def _identity(value: dict[str, Any]) -> ProcessIdentity:
    """Return the process a record names by ``hostname``, ``boot_id``, ``pid`` and ``process_start_ticks``."""

    hostname, boot_id = value.get("hostname"), value.get("boot_id")
    pid, ticks = value.get("pid"), value.get("process_start_ticks")
    if not isinstance(hostname, str) or not hostname:
        raise ValueError("hostname must be a non-empty string")
    if boot_id is not None and (not isinstance(boot_id, str) or not boot_id):
        raise ValueError("boot_id must be a non-empty string or null")
    if type(pid) is not int or pid <= 0:
        raise ValueError("pid must be a positive integer")
    if ticks is not None and (type(ticks) is not int or ticks < 0):
        raise ValueError("process_start_ticks must be a non-negative integer or null")
    return ProcessIdentity(hostname, boot_id, pid, ticks)


def _owner(value: dict[str, Any], owner_id: str) -> tuple[ProcessIdentity, RecordedAllocation | None]:
    """Validate an ``owner.json`` document: its process and its allocation."""

    version = value.get("format_version")
    if value.get("format") != _OWNER_FORMAT or type(version) is not int or version != 1:
        raise ValueError(f"owner.json is not {_OWNER_FORMAT} format_version 1")
    if value.get("owner_id") != owner_id:
        raise ValueError(f"owner.json does not name owner {owner_id}")
    return _identity(value), RecordedAllocation.from_record(value.get("allocation"))


def _process_evidence(subject: str, identity: ProcessIdentity, gone: bool | None) -> Evidence:
    what = f"pid {identity.pid} on {identity.hostname} (boot {identity.boot_id}, start ticks {identity.start_ticks})"
    if gone is True:
        return Evidence(subject, "process_gone", f"{what} is gone")
    if gone is False:
        return Evidence(subject, "process_alive", f"{what} is running on this host")
    return Evidence(subject, "process_unprovable", f"{what} cannot be observed from this host")


def _allocation_evidence(subject: str, allocation: RecordedAllocation, ended: bool | None) -> Evidence:
    what = f"{allocation.kind} allocation {dict(allocation.identity or {})} (probe {allocation.probe})"
    if ended is True:
        return Evidence(subject, "allocation_ended", f"{what} has ended")
    if ended is False:
        return Evidence(subject, "allocation_active", f"{what} is active")
    return Evidence(subject, "allocation_unknown", f"{what} cannot be asked about from this host")


def _launch(
    launches: Path, name: str, scheduler: SchedulerQueries, here: ProcessIdentity
) -> tuple[bool, tuple[Evidence, ...]]:
    """Decide whether one launch directory provably describes no running process."""

    subject = f"launch {name}"
    try:
        descriptor = _fs.open_dir(launches / name)
    except FileNotFoundError:
        return True, (Evidence(subject, "removed", "the launch directory was removed after reaping"),)
    except (OSError, WorkflowError) as exc:
        return False, (Evidence(subject, "malformed_record", f"cannot open the launch directory: {exc}"),)
    try:
        value = _read_json(_fs.anchored(descriptor, _PROCESS_FILE))
    except ValueError as exc:
        return False, (Evidence(subject, "malformed_record", str(exc)),)
    finally:
        os.close(descriptor)
    if value is None:
        # No process of a launch runs before its process.json is durable, and a dead owner writes none.
        return True, (Evidence(subject, "not_started", "no process.json: the launch gate never opened"),)
    try:
        leader = _identity(value)
        pgid, local = value.get("pgid"), value.get("ranks_local_only")
        if type(pgid) is not int or pgid <= 0:
            raise ValueError("pgid must be a positive integer")
        if not isinstance(local, bool):
            raise ValueError("ranks_local_only must be a boolean")
    except ValueError as exc:
        return False, (Evidence(subject, "malformed_record", f"process.json: {exc}"),)
    allocation = RecordedAllocation.from_record(value.get("allocation"))
    evidence: list[Evidence] = []
    gone: bool | None = None
    if local:
        # The recorded start ticks are the leader's: they identify the group only when the leader leads it.
        ticks = leader.start_ticks if leader.pid == pgid else None
        group = ProcessIdentity(leader.hostname, leader.boot_id, pgid, ticks)
        gone = process_gone(group, here=here, group=True)
        evidence.append(_process_evidence(subject, group, gone))
        if gone is not None:
            # A group running here outranks a scheduler answer that may be stale or wrong.
            return gone, tuple(evidence)
    else:
        evidence.append(Evidence(subject, "remote_ranks", "ranks may run on other hosts than the launching one"))
    if allocation is not None:
        ended = scheduler.ended(allocation)
        evidence.append(_allocation_evidence(subject, allocation, ended))
        if ended is True:
            return True, (evidence[-1],)
    return False, tuple(evidence)


def probe(
    owner_dir: Path,
    *,
    visibility_deadline: float,
    scheduler: SchedulerQueries,
    here: ProcessIdentity,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[Liveness, tuple[Evidence, ...]]:
    """Decide whether an owner and every launch it started have provably ended.

    1. A ``dead.json`` tombstone means dead; neither a tombstone nor
       ``owner.json`` means unknown, and so does a malformed ``owner.json``.
    2. The owner is dead when its process is provably gone on this host, or
       when its process cannot be observed here and its recorded allocation
       has provably ended. An active allocation never makes a dead process
       alive, and a process running here outranks a scheduler answer that may
       be stale or wrong.
    3. An owner that is not dead is alive when its process runs on this host,
       and otherwise unknown.
    4. For a dead owner, ``launches/`` is listed after *visibility_deadline*
       (a dead owner adds none, so the listing is complete). A launch whose
       ranks ran only on the launching host is decided by that process group
       when it can be observed here (gone is dead, running is not dead, whatever
       the scheduler answers); otherwise it is dead when its allocation has
       ended. A launch is also dead when it has no ``process.json`` (its gate
       never opened) or when its directory was removed. A malformed or
       unreadable record is not dead.
    5. The owner is dead only when every launch is; otherwise unknown.

    :param owner_dir: The owner's directory, ``.httk-workspace/owners/<owner-id>``.
    :param visibility_deadline: Seconds after which another host's writes are visible here.
    :param scheduler: The paced scheduler queries.
    :param here: This host's identity (see :func:`process_identity`).
    :param sleep: Waits out *visibility_deadline*; injectable for tests.
    :return: The conclusion and its evidence: all of it for ``DEAD``, otherwise
        what decided it or what blocks the proof.
    """

    subject = f"owner {owner_dir.name}"
    owner_file = _fs.loc(owner_dir / _OWNER_FILE)
    problem: str | None = None
    parsed: tuple[ProcessIdentity, RecordedAllocation | None] | None = None
    try:
        value = _read_json(owner_file)
        parsed = None if value is None else _owner(value, owner_dir.name)
    except ValueError as exc:
        problem = str(exc)
    # Read after owner.json: the tombstone is written before a recovery removes owner.json.
    try:
        tombstone = _fs.exists(_fs.loc(owner_dir / _TOMBSTONE_FILE))
    except OSError as exc:
        return Liveness.UNKNOWN, (Evidence(subject, "malformed_owner", f"cannot observe dead.json: {exc}"),)
    if tombstone:
        return Liveness.DEAD, (Evidence(subject, "tombstone", f"{owner_dir / _TOMBSTONE_FILE} exists"),)
    if problem is not None:
        return Liveness.UNKNOWN, (Evidence(subject, "malformed_owner", problem),)
    if parsed is None:
        return Liveness.UNKNOWN, (Evidence(subject, "no_record", "neither owner.json nor dead.json exists"),)

    identity, allocation = parsed
    gone = process_gone(identity, here=here)
    evidence = [_process_evidence(subject, identity, gone)]
    ended: bool | None = None
    if allocation is not None:
        ended = scheduler.ended(allocation)
        evidence.append(_allocation_evidence(subject, allocation, ended))
    if not (gone is True or (gone is None and ended is True)):
        return (Liveness.ALIVE if gone is False else Liveness.UNKNOWN), tuple(evidence)

    sleep(visibility_deadline)
    launches = owner_dir / _LAUNCHES_DIRECTORY
    try:
        descriptor = _fs.open_dir(launches)
    except FileNotFoundError:
        names: list[str] = []
    except (OSError, WorkflowError) as exc:
        blocker = Evidence(f"{subject} launches", "malformed_record", f"cannot open {launches}: {exc}")
        return Liveness.UNKNOWN, (*evidence, blocker)
    else:
        try:
            names = sorted(os.listdir(descriptor))
        except OSError as exc:
            blocker = Evidence(f"{subject} launches", "malformed_record", f"cannot list {launches}: {exc}")
            return Liveness.UNKNOWN, (*evidence, blocker)
        finally:
            os.close(descriptor)
    blockers: list[Evidence] = []
    for name in names:
        dead, found = _launch(launches, name, scheduler, here)
        (evidence if dead else blockers).extend(found)
    if blockers:
        return Liveness.UNKNOWN, (*evidence, *blockers)
    return Liveness.DEAD, tuple(evidence)
