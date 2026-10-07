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
from .errors import FormatError
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
_RECORD_BYTES = 4096
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
    record = {"pid": process.pid, "hostname": manager.hostname, "attempt_id": attempt.attempt_id}
    try:
        _write_durable(trusted, PROCESS_FILE, json.dumps({**record, "started_at": utc_now()}).encode())
        os.write(gate_write, b"\n")
    except OSError as exc:
        # Closing the gate unopened makes the launch exit 125 without starting anything; stop it anyway.
        _stop(manager, attempt, launch, f"cannot record the launch process: {exc}", time.monotonic())
    finally:
        os.close(gate_write)
    return None


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
    """

    kind: Literal["process", "description", "garbage", "unknown"]
    attempt_id: str | None = None
    host: str | None = None
    process_group: int | None = None


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
            data = read_bounded(descriptor, PROCESS_FILE, _RECORD_BYTES)
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
            if isinstance(attempt_id, str) and isinstance(host, str) and type(pid) is int and pid > 0:
                return _Record("process", attempt_id, host, pid)
            return _GARBAGE
        # A manager that died between the start and the process record leaves only launch.json; that
        # launch may run, with no process group anyone can prove gone.
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
        and not _group_alive(record.process_group)
    )


def dead_records(manager_dir: Path, *, hostname: str, grace_seconds: float, now: float) -> list[str]:
    """Return the trusted launch directories of an expired manager that provably describe no live launch.

    A manager is expired when it has not heartbeated for *grace_seconds* or has no heartbeat; a heartbeat
    that cannot be read proves nothing. Of an expired manager, a record is dead when its process group
    is gone on this host, or when it holds no process record (only ``launch.json``, or only malformed
    manager files, or nothing). A record an I/O error hides is never dead. Garbage collection calls
    this for manager directories it already found aged and not live.

    A launch with only ``launch.json`` never hides a started launch: the launch process waits at a gate
    until the manager has made ``process.json`` durable, and only the manager writes it after starting
    the process, so ranks run only once the record exists.

    :param manager_dir: The manager's directory below ``managers/``.
    :param hostname: This host's name, the only host whose process groups can be proven gone.
    :param grace_seconds: How long the manager must have been silent: its lease times the takeover grace factor.
    :param now: The current epoch second.
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


def recorded_launches(manager: Any, attempt_id: str) -> list[tuple[str | None, int | None]] | None:
    """Return the host and process group of every launch any manager recorded for an attempt.

    Process records of other expired managers (silent for their lease times the takeover grace factor)
    whose process group is provably gone on this host are removed on the way. Records without a process
    record are left to garbage collection; records that cannot be read make the answer unprovable.

    :param manager: The task manager.
    :param attempt_id: The attempt identifier.
    :return: ``(host, process_group)`` pairs, ``None`` members when only the launch description
        survived, or ``None`` when a record cannot be read or the records exceed :data:`MAXIMUM_RECORD_ENTRIES`.
    """

    found: list[tuple[str | None, int | None]] = []
    visited = 0
    now = time.time()
    grace = manager.lease_seconds * manager.takeover_grace_factor
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
                expired = bool(names) and manager_entry.name != manager.manager_id and _expired(manager_dir, grace, now)
                for name in names:
                    visited += 1
                    if visited > MAXIMUM_RECORD_ENTRIES:
                        return None
                    record = _read_record(launches_fd, name)
                    if record.kind == "unknown":
                        return None
                    if expired and _gone_here(record, manager.hostname):
                        _remove_record(launches_fd, name)
                        continue
                    if record.attempt_id == attempt_id:
                        found.append((record.host, record.process_group))
            except OSError:
                return None
            finally:
                os.close(launches_fd)
    return found


def recorded_launches_dead(manager: Any, attempt_id: str) -> bool:
    """Return whether every launch recorded for an attempt is provably gone on this host.

    :param manager: The task manager.
    :param attempt_id: The attempt identifier.
    :return: ``False`` when a recorded launch is on another host, has no recorded process group, still
        has a process group here, or the records cannot be read.
    """

    records = recorded_launches(manager, attempt_id)
    if records is None:
        return False
    for host, process_group in records:
        if host != manager.hostname or process_group is None:
            return False
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            continue
        except OSError:
            return False
        return False
    return True


def signal_recorded(manager: Any, attempt_id: str, signal_number: int) -> None:
    """Signal the process group of every launch recorded for an attempt on this host.

    :param manager: The task manager.
    :param attempt_id: The attempt identifier.
    :param signal_number: The signal.
    """

    for host, process_group in recorded_launches(manager, attempt_id) or ():
        if host == manager.hostname and process_group is not None:
            manager._terminate_process(process_group, signal_number)
