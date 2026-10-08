"""Manager side of confined launches: admit, start, supervise and record the launches of confined attempts.

A confined attempt's ``HTTK_WORKFLOW_LAUNCH`` is the launch client (:mod:`httk.workflow._launch_client`),
which publishes a request in ``launch/`` of the attempt control directory. The manager answers it here:

* **Admission.** A request is started only for a live attempt that is neither fenced, cancelling, timed
  out, drained, reaped nor published, whose admission was not closed by an uncertain launch, with a client
  lock file (in the attempt's launch-lock directory, see below) whose lock is still held, and only one
  launch at a time per attempt; further requests wait in arrival order. Every other request gets a
  ``refused`` status.
* **Liveness lock.** The client's ``flock`` lives on host tmpfs, never on the shared workspace filesystem:
  for an attempt with the launch client the manager creates ``<confine.shm_root>/httk-launch-<attempt_id>/``
  (exclusively, owner and mode checked, see :func:`httk.workflow._confine.create_launch_locks`), binds it
  into the sandbox at the identical path and exports it as ``HTTK_WORKFLOW_LAUNCH_LOCKS``. The client
  creates ``ID.lock`` there; the manager opens it relative to its own directory descriptor and probes it
  with ``LOCK_EX|LOCK_NB``: held means alive, acquired means the client is gone, even after ``SIGKILL``.
  Requests, statuses and output stay in ``launch/`` of the attempt control directory. The manager removes
  the directory when it drops the attempt (:func:`forget`); the directory of a crashed manager stays on its
  node until reboot.
* **Start.** The manager writes the trusted launch directory
  ``<workspace>/.httk-workspace/managers/<manager_id>/launches/<attempt_id>.<request_id>/`` (``nodefile``
  rendered from the placement kept in memory, ``launch.json`` with a random launch token and, after the
  start, ``process.json``), creates ``ID.stdout``
  and ``ID.stderr`` exclusively, and starts the launch template rendered in memory around the rank helper
  in a new session. It never interprets the request's command.
* **Supervision.** A launch that exits gets an ``exited`` status. A stop marker, a dead client, or an
  attempt that stops being live stops the launch: ``SIGTERM`` to its process group, ``SIGKILL`` after
  ``cancel_grace_seconds``, a ``stopped`` status once the group is gone, and an ``uncertain`` status (with
  admission closed for the attempt) when it is still not gone a further grace later.

The attempt keeps its placement, its control tree and its marker until every launch is reaped; the manager
checks :func:`unreaped` before it treats an attempt as finished and :func:`holds_commit` before it commits.

Every other decision that needs the launches of an attempt to have ended (a commit takeover, writer death for
an attempt takeover, verified cancellation) asks :func:`launch_end_evidence`, which walks the launch records
of every manager and decides each record by one ladder of rules.
"""

import contextlib
import errno
import fcntl
import json
import logging
import math
import os
import secrets
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from . import _allocation
from ._allocation import RecordedAllocation
from ._confine import ConfineSettings, remove_launch_locks
from ._jobdir import JobDirectory, JobDirectoryError
from ._launch_protocol import (
    LAUNCH_DIRECTORY,
    MAX_ERROR_BYTES,
    MAX_REQUEST_BYTES,
    MAX_TRUSTED_BYTES,
    LaunchConfinement,
    LaunchStatus,
    TrustedLaunch,
    check_request_id,
    decode_request,
    decode_trusted,
    encode_status,
    encode_trusted,
    lock_name,
    read_bounded,
    request_name,
    request_relative_path,
    status_name,
    stderr_name,
    stdout_name,
    stop_name,
    trusted_name,
)
from ._manager_binding import Placement, nodefile_lines, render_launch
from ._util import timestamp_seconds, utc_now
from .errors import FormatError, WorkflowError
from .models import Marker, placement_text

_LOGGER = logging.getLogger("httk.workflow.manager")

#: The directory below a manager's own directory that holds its trusted launch directories.
LAUNCHES_DIRECTORY = "launches"
#: The trusted launch description in a trusted launch directory, as the rank helper reads it. The rank
#: helper is not imported here: ``python -m`` would then find it imported with the package.
LAUNCH_FILE = "launch.json"
#: The process record a started launch leaves in its trusted launch directory.
PROCESS_FILE = "process.json"
#: The nodefile of a trusted launch directory.
NODEFILE = "nodefile"
#: The shortest interval between two scans of the attempts' launch directories, in seconds.
SCAN_INTERVAL = 1.0
#: The most entries one scan of one attempt's launch directory visits.
MAXIMUM_ENTRIES = 4096
#: The most requests one scan of one attempt starts or refuses.
MAXIMUM_DECISIONS = 32
#: The most entries a search for the recorded launches of an attempt visits.
MAXIMUM_RECORD_ENTRIES = 65536
#: Seconds after its allocation's recorded end a launch counts as ended: Slurm's ``KillWait`` and clock
#: skew between hosts.
LAUNCH_END_GRACE = 300.0
#: Seconds one answer to whether an allocation ended is reused, so a tick never asks twice.
ALLOCATION_ANSWER_SECONDS = 60.0
#: Seconds one question whether an allocation ended may take.
ALLOCATION_QUERY_TIMEOUT = 30.0
#: Seconds beyond :data:`LAUNCH_END_GRACE` after which a recorded allocation end counts although the
#: allocation's scheduler, installed here, cannot answer: one failing ``squeue`` is no evidence.
SCHEDULER_UNAVAILABLE_SECONDS = 3600.0
#: The rules by which :func:`launch_end_evidence` counts a recorded launch as ended.
LAUNCH_END_RULES = frozenset(
    {"not_started", "malformed_record", "process_group_gone", "allocation_end_passed", "scheduler_confirmed_ended"}
)
_BOOT_ID = Path("/proc/sys/kernel/random/boot_id")
_RECORD_BYTES = 4096
#: The largest process record: the allocation member carries an identity and the probe path.
_PROCESS_RECORD_BYTES = 65536
_REQUEST_SUFFIX = ".request.json"
_STREAM_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_LOCK_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


def client_prefix() -> str:
    """Return the ``HTTK_WORKFLOW_LAUNCH`` value of a confined attempt that has a launch prefix.

    :return: The shell-quoted launch client command of this interpreter.
    """

    return shlex.join([sys.executable, "-m", "httk.workflow._launch_client"])


@dataclass(frozen=True, slots=True)
class LaunchContext:
    """What every launch of one confined attempt is rendered from, fixed when the attempt starts.

    Nothing here is read back from the job-writable attempt control directory.

    :param placement: The attempt's placement.
    :param template: The effective ``manager.launch_template``, or ``None`` for the scheduler default.
    :param kind: The allocation kind.
    :param gpus_present: Whether the allocation has GPUs.
    :param cpus_per_proc: The allocation's CPUs per processor slot.
    :param mem: The attempt's ``mem`` requirement, or ``None``.
    :param mpi: The effective ``manager.launch_mpi`` plugin of the scheduler default, or ``None``.
    :param confinement: The rank sandbox settings.
    """

    placement: Placement
    template: str | None
    kind: str
    gpus_present: bool
    cpus_per_proc: int
    mem: int | None
    mpi: str | None
    confinement: LaunchConfinement


def launch_confinement(settings: ConfineSettings, *, block_userns: bool) -> LaunchConfinement:
    """Return the rank sandbox settings of an attempt's effective confinement settings.

    :param settings: The validated settings, in ``bwrap`` mode.
    :param block_userns: Whether sandboxes block nested user namespaces (the probe result).
    :return: The rank sandbox settings.
    :raises ValueError: If Bubblewrap is unknown or a setting is outside the launch protocol.
    """

    if settings.bwrap is None:
        raise ValueError("confined launches need Bubblewrap; set confine.bwrap")
    return LaunchConfinement(
        bwrap=settings.bwrap,
        block_userns=block_userns,
        readonly_paths=settings.readonly_paths,
        devices=settings.devices,
        pmix_roots=settings.pmix_roots,
        shm_root=settings.shm_root,
        environment=settings.environment,
        block_mpi_spawn=settings.block_mpi_spawn,
    )


@dataclass(slots=True)
class ConfinedLaunch:
    """One started launch.

    :param request_id: The request identifier.
    :param process: The launch template process, the leader of its own session and process group.
    :param trusted: The trusted launch directory.
    :param exit_code: The leader's exit status once observed, ``128+N`` for signal ``N``.
    :param stop_reason: Why the manager stops the launch, or ``None`` while it is not being stopped.
    :param term_at: The monotonic time ``SIGTERM`` was sent to the group, or ``None``.
    :param kill_at: The monotonic time ``SIGKILL`` was sent to the group, or ``None``.
    :param uncertain: Whether an ``uncertain`` status was decided because the group outlived ``SIGKILL``.
    :param reaped: Whether the leader is reaped and its process group is gone.
    :param pending: A final status not yet written, retried every tick.
    """

    request_id: str
    process: subprocess.Popen[bytes]
    trusted: Path
    exit_code: int | None = None
    stop_reason: str | None = None
    term_at: float | None = None
    kill_at: float | None = None
    uncertain: bool = False
    reaped: bool = False
    pending: LaunchStatus | None = None


@dataclass(slots=True)
class AttemptLaunches:
    """The launch state of one local confined attempt.

    :param control_name: The attempt control directory relative to the job directory.
    :param context: What launches are rendered from, or ``None`` when the attempt has no launch prefix
        (every request is then refused).
    :param launches: The started launches by request identifier.
    :param closed: Why admission is closed for good, or ``None``.
    :param launch_locks: The descriptor (owned by this state, closed by :func:`forget`) and host path of the
        attempt's launch-lock directory on node-local shared memory, or ``None`` without one.
    """

    control_name: str
    context: LaunchContext | None
    launches: dict[str, ConfinedLaunch] = field(default_factory=dict)
    closed: str | None = None
    launch_locks: tuple[int, Path] | None = None


def _state(attempt: Any) -> AttemptLaunches | None:
    state = attempt.launches
    return state if isinstance(state, AttemptLaunches) else None


def unreaped(attempt: Any) -> bool:
    """Return whether a local attempt has a launch that is not reaped yet.

    :param attempt: The manager's running attempt.
    :return: Whether any of its launches still has a live leader or process group.
    """

    state = _state(attempt)
    return state is not None and any(not launch.reaped for launch in state.launches.values())


def holds_commit(manager: Any, marker: Marker) -> bool:
    """Return whether a committing job must wait for the launches of its local attempt.

    :param manager: The task manager.
    :param marker: The committing job's marker.
    :return: Whether this manager tracks an attempt of the job with an unreaped launch.
    """

    return any(
        attempt.marker.job_key == marker.job_key and attempt.marker.placement == marker.placement and unreaped(attempt)
        for attempt in manager._running.values()
    )


def _fields(attempt: Any, request_id: str, **extra: object) -> dict[str, object]:
    return {"attempt_id": attempt.attempt_id, "request_id": request_id, **extra}


def _status_code(returncode: int) -> int:
    return min(max(128 - returncode if returncode < 0 else returncode, 0), 255)


def _error(text: str) -> str:
    data = text.encode("utf-8", errors="replace")[:MAX_ERROR_BYTES]
    return data.decode("utf-8", errors="ignore")


def _group_alive(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def _boot_id() -> str | None:
    """Return this host's boot id, or ``None`` when it cannot be read."""

    try:
        return _BOOT_ID.read_text(encoding="ascii").strip() or None
    except (OSError, ValueError):
        return None


def _start_time(pid: int) -> int | None:
    """Return when a process started, in clock ticks after boot (``/proc/<pid>/stat`` field 22), or ``None``."""

    try:
        data = Path(f"/proc/{pid}/stat").read_bytes()
        # The command name in field 2 may contain spaces and parentheses; field 3 follows the last ")".
        return int(data[data.rindex(b")") + 2 :].split()[19])
    except (OSError, ValueError, IndexError):
        return None


def _recorded_group_alive(process_group: int, process_start: int | None, boot_id: str | None) -> bool:
    """Return whether a recorded process group may still be alive on this host.

    A recorded boot id or leader start time that no longer matches means the group is gone: the kernel
    does not reuse a pid while a process group of that id exists, so a different process under the
    leader's pid proves the recorded group empty. A record without them is judged by the group alone.
    """

    current_boot = _boot_id()
    if boot_id is not None and current_boot is not None and boot_id != current_boot:
        return False
    if process_start is not None:
        started = _start_time(process_group)
        if started is not None and started != process_start:
            return False
    return _group_alive(process_group)


def _stop(manager: Any, attempt: Any, launch: ConfinedLaunch, reason: str, now: float) -> None:
    if launch.reaped:
        return
    if launch.stop_reason is None:
        launch.stop_reason = reason
        _LOGGER.info(
            "stopping launch %s of attempt %s: %s",
            launch.request_id,
            attempt.attempt_id,
            reason,
            extra=manager._event("launch_stopping", attempt.marker, **_fields(attempt, launch.request_id)),
        )
    if launch.term_at is None:
        launch.term_at = now
        manager._terminate_process(launch.process.pid)


def stop_all(manager: Any, attempt: Any, reason: str) -> None:
    """Start stopping every unreaped launch of an attempt: ``SIGTERM`` to each launch's process group.

    :param manager: The task manager.
    :param attempt: The manager's running attempt.
    :param reason: Why the launches are stopped, recorded in their ``stopped`` status.
    """

    state = _state(attempt)
    if state is None:
        return
    now = time.monotonic()
    for launch in state.launches.values():
        _stop(manager, attempt, launch, reason, now)


def signal_all(manager: Any, attempt: Any, signal_number: int, reason: str) -> None:
    """Send *signal_number* to the process group of every unreaped launch of an attempt.

    A drain uses this beside signalling the attempt itself; the launches are then stopped launches.

    :param manager: The task manager.
    :param attempt: The manager's running attempt.
    :param signal_number: The signal.
    :param reason: Why the launches are stopped.
    """

    state = _state(attempt)
    if state is None:
        return
    now = time.monotonic()
    for launch in state.launches.values():
        if launch.reaped:
            continue
        _stop(manager, attempt, launch, reason, now)
        if signal_number == signal.SIGKILL and launch.kill_at is None:
            launch.kill_at = now
        if signal_number != signal.SIGTERM or launch.term_at != now:
            manager._terminate_process(launch.process.pid, signal_number)


def _remove_trusted(launch: ConfinedLaunch) -> bool:
    """Remove a reaped launch's trusted directory, which is takeover evidence only while the launch may run."""

    try:
        shutil.rmtree(launch.trusted)
    except FileNotFoundError:
        pass
    except OSError as exc:
        _LOGGER.warning("cannot remove the trusted launch directory %s: %s", launch.trusted, exc)
        return False
    return True


def forget(manager: Any, attempt: Any) -> None:
    """Remove the trusted launch directories of a dropped attempt's reaped launches.

    :param manager: The task manager.
    :param attempt: The attempt the manager stopped tracking.
    """

    state = _state(attempt)
    if state is None:
        return
    for launch in state.launches.values():
        if launch.reaped:
            _remove_trusted(launch)
    if state.launch_locks is not None:
        descriptor, path = state.launch_locks
        state.launch_locks = None
        os.close(descriptor)
        remove_launch_locks(path.parent, attempt.attempt_id)


def _escalate(manager: Any, attempt: Any, state: AttemptLaunches, launch: ConfinedLaunch, now: float) -> None:
    grace = manager.cancel_grace_seconds
    if launch.term_at is None:
        launch.term_at = now
        manager._terminate_process(launch.process.pid)
    elif launch.kill_at is None:
        if now >= launch.term_at + grace:
            launch.kill_at = now
            _LOGGER.warning(
                "launch %s of attempt %s outlived the %.1fs grace after SIGTERM; killing its process group",
                launch.request_id,
                attempt.attempt_id,
                grace,
                extra=manager._event("launch_kill", attempt.marker, **_fields(attempt, launch.request_id)),
            )
            manager._terminate_process(launch.process.pid, signal.SIGKILL)
    elif not launch.uncertain and now >= launch.kill_at + grace:
        launch.uncertain = True
        reason = f"the launch process group {launch.process.pid} was not reaped {grace:.1f}s after SIGKILL"
        launch.pending = LaunchStatus(launch.request_id, "uncertain", error=_error(reason))
        state.closed = f"an earlier launch ({launch.request_id}) could not be confirmed stopped"
        _LOGGER.error(
            "launch %s of attempt %s of %s is uncertain: %s; the attempt admits no further launches",
            launch.request_id,
            attempt.attempt_id,
            attempt.marker.job_key,
            reason,
            extra=manager._event("launch_uncertain", attempt.marker, **_fields(attempt, launch.request_id)),
        )


def _open_launch_directory(manager: Any, attempt: Any, state: AttemptLaunches) -> JobDirectory:
    with manager._job_directory(attempt.marker) as job_dir:
        return job_dir.directory(f"{state.control_name}/{LAUNCH_DIRECTORY}")


def _open_client_lock(state: AttemptLaunches, request_id: str) -> int | None:
    """Open the client's lock file in the attempt's launch-lock directory, or return ``None`` if it is not one."""

    if state.launch_locks is None:
        return None
    try:
        descriptor = os.open(lock_name(request_id), _LOCK_FLAGS, dir_fd=state.launch_locks[0])
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ELOOP, errno.ENXIO, errno.ENOTDIR):
            return None
        raise
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        return None
    return descriptor


def _client_alive(state: AttemptLaunches, request_id: str) -> bool:
    """Return whether the client holds its lock: a missing or unlocked file means the client is gone."""

    descriptor = _open_client_lock(state, request_id)
    if descriptor is None:
        return False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return True
            raise
        return False
    finally:
        os.close(descriptor)


def _client_stop_reason(manager: Any, attempt: Any, state: AttemptLaunches, launch: ConfinedLaunch) -> str | None:
    try:
        with _open_launch_directory(manager, attempt, state) as directory:
            if directory.stat(stop_name(launch.request_id)) is not None:
                return "the client asked to stop the launch"
            alive = _client_alive(state, launch.request_id)
    except (FileNotFoundError, JobDirectoryError):
        return "the attempt's launch directory is gone or was replaced"
    except (FormatError, OSError) as exc:
        _LOGGER.debug("cannot check the client of launch %s: %s", launch.request_id, exc)
        return None
    return None if alive else "the client is gone"


def _poll_launch(manager: Any, attempt: Any, state: AttemptLaunches, launch: ConfinedLaunch, now: float) -> bool:
    if launch.reaped:
        return False
    returncode = launch.process.poll()
    if returncode is not None:
        if launch.exit_code is None:
            launch.exit_code = _status_code(returncode)
        if not _group_alive(launch.process.pid):
            launch.reaped = True
            fields = _fields(attempt, launch.request_id, exit_code=launch.exit_code)
            if launch.uncertain:
                _LOGGER.warning(
                    "uncertain launch %s of attempt %s was reaped at last",
                    launch.request_id,
                    attempt.attempt_id,
                    extra=manager._event("launch_stopped", attempt.marker, uncertain=True, **fields),
                )
            elif launch.stop_reason is None:
                launch.pending = LaunchStatus(launch.request_id, "exited", launch.exit_code)
                _LOGGER.info(
                    "launch %s of attempt %s exited with status %d",
                    launch.request_id,
                    attempt.attempt_id,
                    launch.exit_code,
                    extra=manager._event("launch_finished", attempt.marker, **fields),
                )
            else:
                launch.pending = LaunchStatus(
                    launch.request_id, "stopped", launch.exit_code, error=_error(launch.stop_reason)
                )
                _LOGGER.info(
                    "launch %s of attempt %s stopped (%s)",
                    launch.request_id,
                    attempt.attempt_id,
                    launch.stop_reason,
                    extra=manager._event("launch_stopped", attempt.marker, **fields),
                )
            return True
        # The leader exited while processes of its group remain: they are stopped like a launch.
        _escalate(manager, attempt, state, launch, now)
        return False
    if launch.stop_reason is None:
        reason = _client_stop_reason(manager, attempt, state, launch)
        if reason is not None:
            _stop(manager, attempt, launch, reason, now)
    if launch.stop_reason is not None:
        _escalate(manager, attempt, state, launch, now)
    return False


def _write_status(manager: Any, attempt: Any, state: AttemptLaunches, status: LaunchStatus) -> bool:
    try:
        with _open_launch_directory(manager, attempt, state) as directory:
            directory.write_atomic(status_name(status.request_id), encode_status(status))
    except (FormatError, OSError, ValueError) as exc:
        manager._report_anomaly(
            f"launch_status:{attempt.attempt_id}:{status.request_id}",
            f"cannot write the {status.state} status of launch {status.request_id} of {attempt.marker.job_key}: "
            f"{exc}; retrying",
            manager._event("launch_status_error", attempt.marker, **_fields(attempt, status.request_id)),
            level=logging.WARNING,
        )
        return False
    manager._reported.pop(f"launch_status:{attempt.attempt_id}:{status.request_id}", None)
    return True


def _attempt_stop_reason(manager: Any, attempt: Any) -> str | None:
    if attempt.cancelling:
        return "the attempt is being cancelled"
    if attempt.fenced:
        return "the attempt's outcome is committed"
    if attempt.timed_out:
        return "the attempt exceeded its maxtime"
    if attempt.interrupted or manager._draining:
        return "the manager is draining"
    if attempt.sweep_kill_at is not None:
        return "the attempt no longer owns a running marker"
    if attempt.reaped or attempt.process.poll() is not None:
        return "the attempt process exited"
    return None


def _pending_requests(manager: Any, attempt: Any, directory: JobDirectory, state: AttemptLaunches) -> list[str]:
    """Return the request identifiers without a status that were not started, in arrival order."""

    names: set[str] = set()
    requests: list[tuple[int, str]] = []
    with os.scandir(directory.fd) as entries:
        for count, entry in enumerate(entries):
            if count >= MAXIMUM_ENTRIES:
                manager._report_anomaly(
                    f"launch_entries:{attempt.attempt_id}",
                    f"the launch directory of attempt {attempt.attempt_id} of {attempt.marker.job_key} has more "
                    f"than {MAXIMUM_ENTRIES} entries; only the first are considered",
                    manager._event("launch_directory_full", attempt.marker, attempt_id=attempt.attempt_id),
                    level=logging.WARNING,
                )
                break
            name = entry.name
            if name.startswith("."):
                continue
            names.add(name)
            if not name.endswith(_REQUEST_SUFFIX):
                continue
            request_id = name.removesuffix(_REQUEST_SUFFIX)
            try:
                check_request_id(request_id)
                arrived = entry.stat(follow_symlinks=False).st_mtime_ns
            except (ValueError, OSError):
                continue
            requests.append((arrived, request_id))
    return [
        request_id
        for _arrived, request_id in sorted(requests)
        if request_id not in state.launches and status_name(request_id) not in names
    ]


def _refuse(manager: Any, attempt: Any, directory: JobDirectory, request_id: str, reason: str) -> None:
    status = LaunchStatus(request_id, "refused", error=_error(reason))
    try:
        directory.write_atomic(status_name(request_id), encode_status(status))
    except (FormatError, OSError) as exc:
        _LOGGER.warning(
            "cannot refuse launch request %s of attempt %s: %s",
            request_id,
            attempt.attempt_id,
            exc,
            extra=manager._event("launch_status_error", attempt.marker, **_fields(attempt, request_id)),
        )
        return
    _LOGGER.warning(
        "refused launch request %s of attempt %s of %s: %s",
        request_id,
        attempt.attempt_id,
        attempt.marker.job_key,
        reason,
        extra=manager._event("launch_refused", attempt.marker, **_fields(attempt, request_id, reason=reason)),
    )


#: The shell gate in front of a launch: it execs its arguments only after one line arrives on stdin (exit 125 on EOF).
_LAUNCH_GATE = ("/bin/sh", "-c", 'IFS= read -r _ || exit 125; exec "$@" </dev/null', "httk-launch-gate")


def _write_new(directory: Path, name: str, data: bytes) -> None:
    descriptor = os.open(directory / name, _STREAM_FLAGS, 0o600)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(descriptor, view) :]
    finally:
        os.close(descriptor)


def _write_durable(directory: Path, name: str, data: bytes) -> None:
    """Write a new file so that a crash leaves either nothing or all of it: temporary, fsync, rename, fsync."""

    temporary = f".{name}.tmp"
    _write_new(directory, temporary, data)
    try:
        descriptor = os.open(directory / temporary, os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.rename(directory / temporary, directory / name)
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(directory / temporary)
        raise


def _launch_environment() -> dict[str, str]:
    return {name: value for name, value in os.environ.items() if not name.startswith("HTTK_WORKFLOW_")}


def _start(manager: Any, attempt: Any, state: AttemptLaunches, directory: JobDirectory, request_id: str) -> str | None:
    """Start one admitted request, returning ``None``, or why it is refused."""

    context = state.context
    if context is None:
        return "the attempt has no launch binding; HTTK_WORKFLOW_LAUNCH is not available to it"
    try:
        lock = _open_client_lock(state, request_id)
    except OSError as exc:
        return f"cannot open the client lock: {exc}"
    if lock is None:
        return "the request has no client lock file"
    os.close(lock)
    try:
        request = decode_request(directory.read(request_name(request_id), MAX_REQUEST_BYTES))
    except (FormatError, OSError, ValueError) as exc:
        return f"the request is invalid: {exc}"
    if request.request_id != request_id:
        return "the request names another request identifier than its file"
    if request.attempt_id != attempt.attempt_id:
        return "the request names another attempt"
    if directory.stat(stop_name(request_id)) is not None:
        return "the client asked to stop before the launch started"
    try:
        alive = _client_alive(state, request_id)
    except OSError as exc:
        return f"cannot check the client lock: {exc}"
    if not alive:
        return "the client is gone"
    streams: list[int] = []
    trusted = manager.manager_directory / LAUNCHES_DIRECTORY / trusted_name(attempt.attempt_id, request_id)
    created = False
    try:
        try:
            for name in (stdout_name(request_id), stderr_name(request_id)):
                streams.append(os.open(name, _STREAM_FLAGS, 0o600, dir_fd=directory.fd))
        except FileExistsError:
            return "the launch output files already exist; only the manager creates them"
        except OSError as exc:
            return f"cannot create the launch output files: {exc}"
        try:
            trusted.parent.mkdir(mode=0o700, exist_ok=True)
            trusted.mkdir(mode=0o700)
            created = True
            _write_new(trusted, NODEFILE, "".join(f"{host}\n" for host in nodefile_lines(context.placement)).encode())
            marker = attempt.marker
            _write_new(
                trusted,
                LAUNCH_FILE,
                encode_trusted(
                    TrustedLaunch(
                        request_id=request_id,
                        attempt_id=attempt.attempt_id,
                        workspace_id=manager.workspace.workspace_id,
                        workspace_root=manager.workspace.root,
                        placement=placement_text(marker.placement),
                        job_key=marker.job_key,
                        request=request_relative_path(attempt.attempt_id, request_id),
                        confine=context.confinement,
                        python=Path(sys.executable),
                        token=secrets.token_hex(16),
                    )
                ),
            )
            prefix = render_launch(
                context.placement,
                kind=context.kind,
                template=context.template,
                nodefile=str(trusted / NODEFILE),
                gpus_present=context.gpus_present,
                cpus_per_proc=context.cpus_per_proc,
                mem=context.mem,
                mpi=context.mpi,
            )
            if not prefix:
                raise ValueError("the launch template renders no launch prefix")
            # The gate reads one line from its stdin (the pipe) before exec, so the ranks start only once the
            # manager has made process.json durable; the launch then gets /dev/null as before. A plain pipe end
            # as stdin needs no descriptor number: dash cannot redirect descriptors above 9, and pass_fds
            # cannot renumber one.
            gate_read, gate_write = os.pipe()
            try:
                process = subprocess.Popen(
                    [
                        *_LAUNCH_GATE,
                        *prefix,
                        sys.executable,
                        "-I",
                        "-m",
                        "httk.workflow._confine_rank",
                        "--launch-dir",
                        str(trusted),
                    ],
                    stdin=gate_read,
                    stdout=streams[0],
                    stderr=streams[1],
                    start_new_session=True,
                    env=_launch_environment(),
                    cwd="/",
                )
            except BaseException:
                os.close(gate_write)
                raise
            finally:
                os.close(gate_read)
        except FileExistsError:
            return f"the trusted launch directory {trusted} already exists"
        except (OSError, ValueError) as exc:
            if created:
                shutil.rmtree(trusted, ignore_errors=True)
            return f"cannot start the launch: {exc}"
    finally:
        for descriptor in streams:
            os.close(descriptor)
    launch = ConfinedLaunch(request_id, process, trusted)
    state.launches[request_id] = launch
    _LOGGER.info(
        "started launch %s of attempt %s of %s as process group %d",
        request_id,
        attempt.attempt_id,
        attempt.marker.job_key,
        process.pid,
        extra=manager._event("launch_started", attempt.marker, **_fields(attempt, request_id, pid=process.pid)),
    )
    allocation = manager.allocation
    record: dict[str, object] = {
        "pid": process.pid,
        "hostname": manager.hostname,
        "attempt_id": attempt.attempt_id,
        "allocation": None if allocation is None else RecordedAllocation.from_allocation(allocation).as_json(),
    }
    # The leader's start time and the boot tell a reused pid from the launch.
    process_start, boot_id = _start_time(process.pid), _boot_id()
    if process_start is not None and boot_id is not None:
        record.update(process_start=process_start, boot_id=boot_id)
    try:
        _write_durable(trusted, PROCESS_FILE, json.dumps({**record, "started_at": utc_now()}).encode())
        # A manager frozen since admission may wake after a successor took the attempt over, judging this
        # record by the gate: only an attempt that still owns its running marker releases it.
        if _still_running(manager, attempt):
            os.write(gate_write, b"\n")
        else:
            _stop(manager, attempt, launch, "the attempt no longer owns its running marker", time.monotonic())
    except OSError as exc:
        # Closing the gate unopened makes the launch exit 125 without starting anything; stop it anyway.
        _stop(manager, attempt, launch, f"cannot record the launch process: {exc}", time.monotonic())
    finally:
        os.close(gate_write)
    return None


def _still_running(manager: Any, attempt: Any) -> bool:
    """Return whether an attempt still owns a running marker: the one it started with, or after a same-kind move.

    The exact marker path is checked first; only when it is gone is the job's current marker looked up,
    which must be ``running`` and name this attempt. Anything that cannot be observed releases nothing.
    """

    try:
        os.lstat(attempt.marker.path)
    except FileNotFoundError:
        pass
    except OSError:
        return False
    else:
        return True
    try:
        current = manager.workspace.find_marker_at(attempt.marker.job_key, attempt.marker.placement)
        return (
            current is not None
            and current.kind == "running"
            and manager._read_frame(current).attempt_id == attempt.attempt_id
        )
    except (WorkflowError, OSError):
        return False


def _admission_refusal(manager: Any, attempt: Any, state: AttemptLaunches, control: JobDirectory) -> str | None:
    reason = _attempt_stop_reason(manager, attempt)
    if reason is not None:
        return reason
    if state.closed is not None:
        return state.closed
    try:
        published = control.exists_dir("outcome.ready")
    except (FormatError, OSError):
        published = True
    if published:
        return "the attempt published its outcome"
    return None


def _scan(manager: Any, attempt: Any, state: AttemptLaunches) -> bool:
    changed = False
    try:
        with (
            manager._job_directory(attempt.marker) as job_dir,
            job_dir.directory(state.control_name) as control,
        ):
            try:
                directory = control.directory(LAUNCH_DIRECTORY)
            except FileNotFoundError:
                return False
            with directory:
                pending = _pending_requests(manager, attempt, directory, state)
                refusal = _admission_refusal(manager, attempt, state, control)
                if refusal is not None:
                    stop_all(manager, attempt, refusal)
                for decided, request_id in enumerate(pending):
                    if decided >= MAXIMUM_DECISIONS:
                        break
                    if refusal is None and any(
                        not launch.reaped or launch.pending is not None for launch in state.launches.values()
                    ):
                        # One launch at a time: the rest wait in arrival order for its final status.
                        break
                    reason = refusal or _start(manager, attempt, state, directory, request_id)
                    if reason is not None:
                        _refuse(manager, attempt, directory, request_id, reason)
                    changed = True
    except FileNotFoundError:
        return changed
    except (FormatError, OSError) as exc:
        manager._report_anomaly(
            f"launch_scan:{attempt.attempt_id}",
            f"cannot read the launch requests of attempt {attempt.attempt_id} of {attempt.marker.job_key}: {exc}",
            manager._event("launch_scan_error", attempt.marker, attempt_id=attempt.attempt_id),
            level=logging.WARNING,
        )
    return changed


def supervise(manager: Any) -> bool:
    """Supervise the launches of every local confined attempt: one pass of a manager tick.

    Launch exits, stop markers, client deaths and attempt-level stops are handled on every call; the
    launch directories are scanned for new requests at most once per :data:`SCAN_INTERVAL`.

    :param manager: The task manager.
    :return: Whether a launch was started, refused or finished.
    """

    now = time.monotonic()
    scan = now - manager._last_launch_scan >= SCAN_INTERVAL
    if scan:
        manager._last_launch_scan = now
    changed = False
    for attempt in list(manager._running.values()):
        state = _state(attempt)
        if state is None:
            continue
        reason = _attempt_stop_reason(manager, attempt)
        if reason is not None:
            stop_all(manager, attempt, reason)
        for launch in list(state.launches.values()):
            changed |= _poll_launch(manager, attempt, state, launch, now)
            if launch.pending is not None and _write_status(manager, attempt, state, launch.pending):
                launch.pending = None
            if launch.reaped and launch.pending is None and _remove_trusted(launch):
                # Reaped and answered: the launch is no longer evidence and holds no admission slot; its
                # status file marks the request as done.
                del state.launches[launch.request_id]
        if scan:
            changed |= _scan(manager, attempt, state)
    return changed


@dataclass(frozen=True, slots=True)
class _Record:
    """What one trusted launch directory says.

    :param kind: ``process`` (a readable process record), ``description`` (only a valid ``launch.json``),
        ``garbage`` (gone, not a directory, or only malformed manager files) or ``unknown`` (an I/O error,
        which never proves anything).
    :param attempt_id: The recorded attempt, for ``process`` and ``description``.
    :param host: The recorded host, for ``process``.
    :param process_group: The recorded process group, for ``process``.
    :param allocation: The recorded allocation, for a ``process`` record that names a usable one.
    :param process_start: The launch leader's recorded start time, in clock ticks after boot.
    :param boot_id: The recorded boot id of the launching host.
    """

    kind: Literal["process", "description", "garbage", "unknown"]
    attempt_id: str | None = None
    host: str | None = None
    process_group: int | None = None
    allocation: RecordedAllocation | None = None
    process_start: int | None = None
    boot_id: str | None = None


_GARBAGE = _Record("garbage")
_UNKNOWN = _Record("unknown")
_ABSENT_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR, errno.ELOOP})


def _read_record(launches_fd: int, name: str) -> _Record:
    """Read one trusted launch directory, telling garbage from what an I/O error hides."""

    try:
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=launches_fd)
    except OSError as exc:
        return _GARBAGE if exc.errno in _ABSENT_ERRNOS else _UNKNOWN
    try:
        try:
            data = read_bounded(descriptor, PROCESS_FILE, _PROCESS_RECORD_BYTES)
        except FileNotFoundError:
            data = None
        except ValueError:
            return _GARBAGE
        except OSError:
            return _UNKNOWN
        if data is not None:
            try:
                value = json.loads(data)
            except (ValueError, RecursionError):
                return _GARBAGE
            if not isinstance(value, Mapping):
                return _GARBAGE
            attempt_id, host, pid = value.get("attempt_id"), value.get("hostname"), value.get("pid")
            if not (isinstance(attempt_id, str) and isinstance(host, str) and type(pid) is int and pid > 0):
                return _GARBAGE
            # An older record has neither member; an unusable allocation only proves less, as does a
            # leader identity that is not recorded.
            process_start, boot_id = value.get("process_start"), value.get("boot_id")
            return _Record(
                "process",
                attempt_id,
                host,
                pid,
                RecordedAllocation.from_record(value.get("allocation")),
                process_start if type(process_start) is int and process_start >= 0 else None,
                boot_id if isinstance(boot_id, str) and boot_id else None,
            )
        # A manager that died between the start and the process record leaves only launch.json. The launch
        # process waits at its gate until process.json is durable, so no rank of that launch ever ran.
        try:
            launch = decode_trusted(read_bounded(descriptor, LAUNCH_FILE, MAX_TRUSTED_BYTES))
        except (FileNotFoundError, ValueError, RecursionError):
            return _GARBAGE
        except OSError:
            return _UNKNOWN
        return _Record("description", launch.attempt_id)
    finally:
        os.close(descriptor)


def _heartbeat_age(manager_dir: Path, now: float) -> float | None:
    """Return how long ago a manager heartbeated: infinite without a heartbeat, ``None`` when unreadable."""

    try:
        descriptor = os.open(manager_dir, _DIRECTORY_FLAGS)
    except OSError as exc:
        return math.inf if exc.errno in _ABSENT_ERRNOS else None
    try:
        data = read_bounded(descriptor, "heartbeat.json", _RECORD_BYTES)
    except FileNotFoundError:
        return math.inf
    except ValueError:
        # Not a regular file or oversized: no heartbeat a manager wrote.
        return math.inf
    except OSError:
        return None
    finally:
        os.close(descriptor)
    try:
        value = json.loads(data)
        return now - timestamp_seconds(str(value["updated_at"]))
    except (ValueError, KeyError, TypeError, RecursionError):
        return math.inf


def _expired(manager_dir: Path, grace_seconds: float, now: float) -> bool:
    age = _heartbeat_age(manager_dir, now)
    return age is not None and age >= grace_seconds


def _gone_here(record: _Record, hostname: str) -> bool:
    """Return whether a process record names this host and its process group is gone."""

    return (
        record.kind == "process"
        and record.host == hostname
        and record.process_group is not None
        and not _recorded_group_alive(record.process_group, record.process_start, record.boot_id)
    )


def dead_records(
    manager_dir: Path,
    *,
    hostname: str,
    grace_seconds: float,
    now: float,
    own_identity: Mapping[str, str] | None = None,
) -> list[str]:
    """Return the trusted launch directories of an expired manager that provably describe no live launch.

    A manager is expired when it has not heartbeated for *grace_seconds* or has no heartbeat; a heartbeat
    that cannot be read proves nothing. Of an expired manager, a record is dead when its process group
    is gone on this host, or when it holds no process record (only ``launch.json``, or only malformed
    manager files, or nothing). A record an I/O error hides is never dead. Garbage collection calls
    this for manager directories it already found aged and not live.

    A launch with only ``launch.json`` never hides a started launch: the launch process waits at a gate
    until the manager has made ``process.json`` durable, and only the manager writes it after starting
    the process, so ranks run only once the record exists.

    A gone group does not prove a step of another queryable allocation gone (its ranks run outside the
    group): such a record is dead only once its allocation's end time and :data:`LAUNCH_END_GRACE` have
    passed, and is otherwise left to :func:`launch_end_evidence`, which asks the scheduler and prunes it.
    Garbage collection never asks a scheduler.

    :param manager_dir: The manager's directory below ``managers/``.
    :param hostname: This host's name, the only host whose process groups can be proven gone.
    :param grace_seconds: How long the manager must have been silent: its lease times the takeover grace factor.
    :param now: The current epoch second.
    :param own_identity: The caller's own allocation identity, whose steps a gone group does end, or ``None``.
    :return: The names below ``launches/`` that can be removed.
    """

    if not _expired(manager_dir, grace_seconds, now):
        return []
    try:
        launches_fd = os.open(manager_dir / LAUNCHES_DIRECTORY, _DIRECTORY_FLAGS)
    except OSError:
        return []
    try:
        with os.scandir(launches_fd) as entries:
            names = [entry.name for _index, entry in zip(range(MAXIMUM_RECORD_ENTRIES), entries, strict=False)]
        dead: list[str] = []
        for name in names:
            record = _read_record(launches_fd, name)
            allocation = record.allocation
            if (
                allocation is not None
                and allocation.queryable
                and dict(allocation.identity or {}) != dict(own_identity or {})
                and not (allocation.end_time is not None and allocation.end_time + LAUNCH_END_GRACE < now)
            ):
                continue
            if record.kind in ("description", "garbage") or _gone_here(record, hostname):
                dead.append(name)
        return dead
    except OSError:
        return []
    finally:
        os.close(launches_fd)


def _remove_record(launches_fd: int, name: str) -> None:
    try:
        if stat.S_ISDIR(os.stat(name, dir_fd=launches_fd, follow_symlinks=False).st_mode):
            shutil.rmtree(name, dir_fd=launches_fd)
        else:
            os.unlink(name, dir_fd=launches_fd)
    except FileNotFoundError:
        pass
    except OSError as exc:
        _LOGGER.warning("cannot remove the dead trusted launch record %s: %s", name, exc)


@dataclass(frozen=True, slots=True)
class RecordedLaunch:
    """One launch recorded for an attempt in some manager's ``launches/``.

    :param name: The record below ``managers/``: ``<manager_id>/<attempt_id>.<request_id>``.
    :param manager_id: The directory name of the manager that wrote the record.
    :param kind: ``process`` (a process record), ``description`` (only ``launch.json``) or ``garbage``
        (malformed manager files, or gone while it was read).
    :param host: The recorded host, for ``process``.
    :param process_group: The recorded process group, for ``process``.
    :param allocation: The recorded allocation, for a ``process`` record that names a usable one.
    :param process_start: The launch leader's recorded start time, in clock ticks after boot.
    :param boot_id: The recorded boot id of the launching host.
    """

    name: str
    manager_id: str
    kind: Literal["process", "description", "garbage"]
    host: str | None = None
    process_group: int | None = None
    allocation: RecordedAllocation | None = None
    process_start: int | None = None
    boot_id: str | None = None


def _scan_records(manager: Any) -> dict[str, list[RecordedLaunch]] | None:
    """Read every launch record of the managers of this manager's uid, by attempt; ``None`` when unreadable."""

    found: dict[str, list[RecordedLaunch]] = {}
    visited = 0
    try:
        managers = os.scandir(manager.workspace.control / "managers")
    except FileNotFoundError:
        return found
    except OSError:
        return None
    with managers:
        for manager_entry in managers:
            visited += 1
            if visited > MAXIMUM_RECORD_ENTRIES:
                return None
            manager_dir = Path(manager_entry.path)
            try:
                if manager_entry.stat(follow_symlinks=False).st_uid != manager.uid:
                    continue
                launches_fd = os.open(manager_dir / LAUNCHES_DIRECTORY, _DIRECTORY_FLAGS)
            except OSError as exc:
                if exc.errno in _ABSENT_ERRNOS:
                    continue
                return None
            try:
                with os.scandir(launches_fd) as entries:
                    names = [
                        entry.name for _index, entry in zip(range(MAXIMUM_RECORD_ENTRIES + 1), entries, strict=False)
                    ]
                for name in names:
                    visited += 1
                    if visited > MAXIMUM_RECORD_ENTRIES:
                        return None
                    record = _read_record(launches_fd, name)
                    if record.kind == "unknown":
                        return None
                    # A malformed record belongs to the attempt its name starts with.
                    attempt_id = record.attempt_id if record.kind != "garbage" else name.partition(".")[0]
                    found.setdefault(attempt_id or "", []).append(
                        RecordedLaunch(
                            f"{manager_entry.name}/{name}",
                            manager_entry.name,
                            record.kind,
                            record.host,
                            record.process_group,
                            record.allocation,
                            record.process_start,
                            record.boot_id,
                        )
                    )
            except OSError:
                return None
            finally:
                os.close(launches_fd)
    return found


def recorded_launches(manager: Any, attempt_id: str) -> list[RecordedLaunch] | None:
    """Return every launch any manager recorded for an attempt.

    A record belongs to the attempt its process record or launch description names; a malformed record
    belongs to the attempt its name starts with. Only managers of this manager's uid are searched: only they
    start attempts of the jobs this manager serves, and another user's ``launches/`` is not readable. The
    records are read once per manager tick (``manager._launch_records``, cleared with the liveness cache).

    :param manager: The task manager.
    :param attempt_id: The attempt identifier.
    :return: The records, or ``None`` when a record cannot be read or the records exceed
        :data:`MAXIMUM_RECORD_ENTRIES`.
    """

    if manager._launch_records is None:
        manager._launch_records = (_scan_records(manager),)
    scanned = manager._launch_records[0]
    return None if scanned is None else list(scanned.get(attempt_id, ()))


def _prune(manager: Any, launch: RecordedLaunch, expired: dict[str, bool]) -> None:
    """Remove the process record of an ended launch of another expired manager, which is no evidence any more."""

    if launch.manager_id == manager.manager_id:
        return
    manager_dir = manager.workspace.control / "managers" / launch.manager_id
    if launch.manager_id not in expired:
        grace = manager.lease_seconds * manager.takeover_grace_factor
        expired[launch.manager_id] = _expired(manager_dir, grace, time.time())
    if not expired[launch.manager_id]:
        return
    try:
        launches_fd = os.open(manager_dir / LAUNCHES_DIRECTORY, _DIRECTORY_FLAGS)
    except OSError:
        return
    try:
        _remove_record(launches_fd, launch.name.partition("/")[2])
    finally:
        os.close(launches_fd)


@dataclass(frozen=True, slots=True)
class LaunchEnd:
    """The rule that decided one recorded launch.

    :param record: The record below ``managers/``, or ``None`` when the records could not be read.
    :param rule: One of :data:`LAUNCH_END_RULES` for an ended launch; otherwise ``launch_starting``,
        ``malformed_record_live``, ``launch_running_here``, ``allocation_active``, ``scheduler_unavailable``,
        ``launch_end_unprovable`` or ``launch_records_unreadable``.
    :param host: The recorded host, when the record names one.
    """

    record: str | None
    rule: str
    host: str | None = None

    def as_json(self) -> dict[str, object]:
        """Return the evidence as a state-frame object.

        :return: ``record`` and ``rule``, and ``host`` when known.
        """

        return {"record": self.record, "rule": self.rule, **({"host": self.host} if self.host is not None else {})}


@dataclass(frozen=True, slots=True)
class LaunchEndEvidence:
    """Whether every launch recorded for an attempt has ended (see :func:`launch_end_evidence`).

    :param records: Why each ended launch counts as ended, in record order.
    :param blocked: Every launch not proven ended and why, in record order.
    """

    records: tuple[LaunchEnd, ...]
    blocked: tuple[LaunchEnd, ...] = ()

    @property
    def pending(self) -> LaunchEnd | None:
        """Return the first launch not proven ended, or ``None`` when every one is."""

        return self.blocked[0] if self.blocked else None

    @property
    def ended(self) -> bool:
        """Return whether every recorded launch is proven ended."""

        return not self.blocked

    def as_json(self) -> list[dict[str, object]]:
        """Return the evidence of every ended launch as the ``launch_end_evidence`` state-frame member.

        :return: One object per record.
        """

        return [record.as_json() for record in self.records]


def _stop_recorded(manager: Any, launch: RecordedLaunch, process_group: int) -> None:
    """Stop a live launch recorded on this host: ``SIGTERM`` first, ``SIGKILL`` after the cancellation grace."""

    now = time.monotonic()
    signalled = manager._launch_signals.get(launch.name)
    if signalled is None:
        manager._launch_signals[launch.name] = (now, False)
        _LOGGER.warning(
            "stopping launch %s, recorded on this host as process group %d: its attempt is being taken over "
            "or cancelled",
            launch.name,
            process_group,
            extra=manager._event("recorded_launch_stopping", record=launch.name, process_group=process_group),
        )
        manager._terminate_process(process_group)
    elif not signalled[1] and now >= signalled[0] + manager.cancel_grace_seconds:
        manager._launch_signals[launch.name] = (signalled[0], True)
        manager._terminate_process(process_group, signal.SIGKILL)


def _allocation_answer(manager: Any, allocation: RecordedAllocation) -> bool | None:
    """Ask whether a queryable allocation ended, at most once a minute per allocation.

    :param manager: The task manager, whose memory holds the answers.
    :param allocation: The recorded allocation.
    :return: The answer: ``None`` when the scheduler cannot tell.
    """

    now = time.monotonic()
    answers: dict[str, tuple[float, bool | None]] = manager._allocation_answers
    for key in [key for key, (asked, _) in answers.items() if now - asked >= ALLOCATION_ANSWER_SECONDS]:
        del answers[key]
    key = json.dumps(allocation.as_json(), sort_keys=True)
    if key not in answers:
        answers[key] = (now, _allocation.allocation_ended(allocation, timeout=ALLOCATION_QUERY_TIMEOUT))
    return answers[key][1]


def _foreign_step(manager: Any, allocation: RecordedAllocation | None) -> bool:
    """Return whether a launch ran in a queryable allocation other than this manager's own.

    Ranks of such a step (``srun``, or a site launcher's ``mpiexec`` under PBS) run under the scheduler's
    or launcher's daemons, outside the launch's process group, so its group being gone here does not
    prove them gone.
    """

    if allocation is None or not allocation.queryable:
        return False
    own = manager.allocation
    return own is None or own.identity is None or dict(own.identity) != dict(allocation.identity or {})


def _recorder_gone(manager: Any, launch: RecordedLaunch, owner: tuple[str | None, float] | None) -> bool:
    """Return whether the manager that wrote a record is gone by the shared liveness evidence.

    The manager a state frame names is judged with that frame's lease, as everywhere else; any other
    with this manager's.
    """

    lease = owner[1] if owner is not None and owner[0] == launch.manager_id else manager.lease_seconds
    return manager._owner_gone_evidence(launch.manager_id, lease_seconds=lease) is not None


def _launch_end(manager: Any, launch: RecordedLaunch, owner: tuple[str | None, float] | None) -> LaunchEnd:
    """Decide one recorded launch by the first rule of the ladder that applies."""

    name = launch.name
    if launch.kind == "description":
        # The launch waits at its gate until process.json is durable, which only its manager writes, right
        # after the start: once that manager is gone, no rank of the launch ever ran.
        return LaunchEnd(name, "not_started" if _recorder_gone(manager, launch, owner) else "launch_starting")
    if launch.kind == "garbage":
        # A live manager may be writing it, or reading it may have met a write in progress.
        return LaunchEnd(
            name, "malformed_record" if _recorder_gone(manager, launch, owner) else "malformed_record_live"
        )
    host, process_group, allocation = launch.host, launch.process_group, launch.allocation
    if host == manager.hostname and process_group is not None:
        if _recorded_group_alive(process_group, launch.process_start, launch.boot_id):
            # A live manager stops its own launches; only an abandoned one is stopped here.
            if _recorder_gone(manager, launch, owner):
                _stop_recorded(manager, launch, process_group)
            return LaunchEnd(name, "launch_running_here", host)
        manager._launch_signals.pop(name, None)
        if not _foreign_step(manager, allocation):
            return LaunchEnd(name, "process_group_gone", host)
    if allocation is None:
        return LaunchEnd(name, "launch_end_unprovable", host)
    ended_ago = None if allocation.end_time is None else time.time() - allocation.end_time - LAUNCH_END_GRACE
    if _allocation.can_ask(allocation):
        # The scheduler outranks the recorded end: an administrator may have extended the time limit.
        answer = _allocation_answer(manager, allocation)
        if answer is True:
            return LaunchEnd(name, "scheduler_confirmed_ended", host)
        if answer is False:
            return LaunchEnd(name, "allocation_active", host)
        # One failing query is no evidence; a scheduler silent for that long after the end is.
        if ended_ago is not None and ended_ago > SCHEDULER_UNAVAILABLE_SECONDS:
            return LaunchEnd(name, "allocation_end_passed", host)
        if ended_ago is not None and ended_ago > 0:
            return LaunchEnd(name, "scheduler_unavailable", host)
    elif ended_ago is not None and ended_ago > 0:
        return LaunchEnd(name, "allocation_end_passed", host)
    return LaunchEnd(name, "launch_end_unprovable", host)


def launch_end_evidence(
    manager: Any, attempt_id: str, *, owner: str | None = None, lease_seconds: float | None = None
) -> LaunchEndEvidence:
    """Decide whether every launch recorded for an attempt in any manager's ``launches/`` has ended.

    Each record is decided by the first rule that applies. Of a record whose manager is gone by
    :meth:`~httk.workflow.TaskManager._owner_gone_evidence`, only ``launch.json`` means ``not_started`` (the
    gate lets no rank run before ``process.json`` is durable) and malformed files mean ``malformed_record``;
    of a manager that may be live, both are pending (``launch_starting``, ``malformed_record_live``). A record
    of this host whose process group is alive (judged with its leader's start time and boot id) is pending
    ``launch_running_here``, and the group gets ``SIGTERM``, then ``SIGKILL`` once the cancellation grace has
    passed, but only when its manager is gone. A gone group is ``process_group_gone``, unless the launch was
    a step of another maintained scheduler allocation than this manager's, which needs the evidence below.
    Then the scheduler or site probe of a queryable allocation whose client is installed here: ended
    (``scheduler_confirmed_ended``) or active (pending ``allocation_active``), asked at most every
    :data:`ALLOCATION_ANSWER_SECONDS`. When it cannot tell, a recorded allocation end counts
    (``allocation_end_passed``) once :data:`LAUNCH_END_GRACE` plus :data:`SCHEDULER_UNAVAILABLE_SECONDS`
    have passed since (pending ``scheduler_unavailable`` before); for an allocation that cannot be asked
    here, once :data:`LAUNCH_END_GRACE` has passed. Otherwise pending ``launch_end_unprovable``. Records that cannot be read, or too many of them, are pending
    ``launch_records_unreadable``.

    The process record of an ended launch of another expired manager is removed on the way.

    :param manager: The task manager.
    :param attempt_id: The attempt identifier.
    :param owner: The manager the attempt's state frame names, judged with *lease_seconds* when it wrote a
        record.
    :param lease_seconds: The lease of that frame; this manager's when omitted.
    :return: The evidence.
    """

    launches = recorded_launches(manager, attempt_id)
    if launches is None:
        return LaunchEndEvidence((), (LaunchEnd(None, "launch_records_unreadable"),))
    frame_owner = (owner, manager.lease_seconds if lease_seconds is None else lease_seconds)
    decided = [_launch_end(manager, launch, frame_owner) for launch in launches]
    expired: dict[str, bool] = {}
    for launch, end in zip(launches, decided, strict=True):
        if launch.kind == "process" and end.rule in LAUNCH_END_RULES:
            _prune(manager, launch, expired)
    return LaunchEndEvidence(
        tuple(end for end in decided if end.rule in LAUNCH_END_RULES),
        tuple(end for end in decided if end.rule not in LAUNCH_END_RULES),
    )


def signal_recorded(manager: Any, attempt_id: str, signal_number: int) -> None:
    """Signal the process group of every launch recorded for an attempt on this host.

    :param manager: The task manager.
    :param attempt_id: The attempt identifier.
    :param signal_number: The signal.
    """

    for launch in recorded_launches(manager, attempt_id) or ():
        if (
            launch.host == manager.hostname
            and launch.process_group is not None
            and _recorded_group_alive(launch.process_group, launch.process_start, launch.boot_id)
        ):
            manager._terminate_process(launch.process_group, signal_number)
