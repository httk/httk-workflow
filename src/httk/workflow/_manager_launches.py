"""Manager side of confined launches: admit, start, supervise and record the launches of confined attempts.

A confined attempt's ``HTTK_WORKFLOW_LAUNCH`` is the launch client (:mod:`httk.workflow._launch_client`),
which publishes a request in ``launch/`` of the attempt directory. The manager answers it here:

* **Admission.** A request is started only for a live attempt that is neither cancelled, timed out,
  drained nor exited, has not published its outcome, and whose admission was not closed by an uncertain
  launch, and only one launch at a time per attempt; further requests wait in arrival order. Every other
  request gets a ``refused`` status.
* **Start.** The manager creates its launch directory ``owners/<owner-id>/launches/<attempt_id>.<request_id>/``
  (:meth:`httk.workflow._kernel.Owner.launch_dir`) with the ``nodefile`` rendered from the placement kept in
  memory and ``launch.json`` (a random launch token), creates ``ID.stdout`` and ``ID.stderr`` exclusively,
  and starts the launch template rendered in memory around the rank helper in a new session, behind a gate.
  ``process.json`` (the death proof's record, see :mod:`httk.workflow._death`) is written before the gate
  opens, and the gate opens only while the attempt is this owner's live attempt and the owner is alive. It
  never interprets the request's command.
* **Supervision.** A launch that exits gets an ``exited`` status. The client's stop marker, or the attempt
  stopping being live, stops the launch: ``SIGTERM`` to its process group, ``SIGKILL`` after
  ``cancel_grace_seconds``, a ``stopped`` status once the group is gone, and an ``uncertain`` status (with
  admission closed for the attempt) when it is still not gone a further grace later. A client killed with
  ``SIGKILL`` while its attempt continues leaves its launch running until the attempt ends (note §11).
* **Reap.** Once a launch's process group is gone, the manager removes the launch's shared-memory directory
  on this node and, once its status is written, its launch directory. Ranks on other nodes leave theirs to
  the record-based sweep of later rank helpers (:func:`httk.workflow._kernel.sweep_unrecorded_shm`).

The attempt keeps its placement and stays unfinished until every launch is reaped: the manager checks
:func:`unreaped` before it treats an attempt as ended.
"""

import logging
import os
import secrets
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import _death, _fs, _kernel
from ._allocation import RecordedAllocation
from ._attempt_process import process_group_alive, terminate_process
from ._confine import ConfineSettings
from ._jobdir import JobDirectory, JobDirectoryError
from ._launch_protocol import (
    LAUNCH_DIRECTORY,
    MAX_ERROR_BYTES,
    MAX_REQUEST_BYTES,
    LaunchConfinement,
    LaunchStatus,
    TrustedLaunch,
    check_request_id,
    decode_request,
    encode_status,
    encode_trusted,
    request_name,
    request_relative_path,
    status_name,
    stderr_name,
    stdout_name,
    stop_name,
)
from ._manager_binding import Placement, nodefile_lines, render_launch
from ._util import json_bytes, utc_now
from .errors import FormatError, WorkflowError
from .models import placement_text

_LOGGER = logging.getLogger("httk.workflow.manager")

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
_REQUEST_SUFFIX = ".request.json"


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
    :param trusted: The launch directory below this owner's ``launches/``.
    :param token: The launch token naming the per-launch shared-memory directory.
    :param shm_root: The node-local shared-memory root the ranks use.
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
    token: str
    shm_root: Path
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
    """

    control_name: str
    context: LaunchContext | None
    launches: dict[str, ConfinedLaunch] = field(default_factory=dict)
    closed: str | None = None


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


def _fields(attempt: Any, request_id: str, **extra: object) -> dict[str, object]:
    return {"attempt_id": attempt.attempt_id, "request_id": request_id, **extra}


def _status_code(returncode: int) -> int:
    return min(max(128 - returncode if returncode < 0 else returncode, 0), 255)


def _error(text: str) -> str:
    data = text.encode("utf-8", errors="replace")[:MAX_ERROR_BYTES]
    return data.decode("utf-8", errors="ignore")


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
            extra=manager._event("launch_stopping", attempt.owned, **_fields(attempt, launch.request_id)),
        )
    if launch.term_at is None:
        launch.term_at = now
        terminate_process(launch.process.pid)


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
            terminate_process(launch.process.pid, signal_number)


def _remove_trusted(manager: Any, attempt: Any, launch: ConfinedLaunch) -> bool:
    """Remove a reaped launch's directory: its record is evidence for the death proof only while it may run."""

    try:
        manager.owner.remove_launch(attempt.attempt_id, launch.request_id)
    except (OSError, WorkflowError) as exc:
        _LOGGER.warning("cannot remove the launch directory %s: %s", launch.trusted, exc)
        return False
    return True


def _remove_shared_memory(launch: ConfinedLaunch) -> None:
    """Remove a reaped launch's shared-memory directory on this node; other nodes' go by the rank helpers' sweep."""

    try:
        _fs.discard(_fs.loc(launch.shm_root / f"httk-{launch.token}"), trash_dir=launch.shm_root, durable=False)
    except (OSError, WorkflowError) as exc:
        _LOGGER.warning("cannot remove the shared memory of launch %s: %s", launch.request_id, exc)


def forget(manager: Any, attempt: Any) -> None:
    """Remove the launch directories of a finished attempt's reaped launches.

    :param manager: The task manager.
    :param attempt: The attempt the manager stopped tracking.
    """

    state = _state(attempt)
    if state is None:
        return
    for launch in state.launches.values():
        if launch.reaped:
            _remove_trusted(manager, attempt, launch)


def _escalate(manager: Any, attempt: Any, state: AttemptLaunches, launch: ConfinedLaunch, now: float) -> None:
    grace = manager.cancel_grace_seconds
    if launch.term_at is None:
        launch.term_at = now
        terminate_process(launch.process.pid)
    elif launch.kill_at is None:
        if now >= launch.term_at + grace:
            launch.kill_at = now
            _LOGGER.warning(
                "launch %s of attempt %s outlived the %.1fs grace after SIGTERM; killing its process group",
                launch.request_id,
                attempt.attempt_id,
                grace,
                extra=manager._event("launch_kill", attempt.owned, **_fields(attempt, launch.request_id)),
            )
            terminate_process(launch.process.pid, signal.SIGKILL)
    elif not launch.uncertain and now >= launch.kill_at + grace:
        launch.uncertain = True
        reason = f"the launch process group {launch.process.pid} was not reaped {grace:.1f}s after SIGKILL"
        launch.pending = LaunchStatus(launch.request_id, "uncertain", error=_error(reason))
        state.closed = f"an earlier launch ({launch.request_id}) could not be confirmed stopped"
        _LOGGER.error(
            "launch %s of attempt %s of %s is uncertain: %s; the attempt admits no further launches",
            launch.request_id,
            attempt.attempt_id,
            attempt.owned.job_key,
            reason,
            extra=manager._event("launch_uncertain", attempt.owned, **_fields(attempt, launch.request_id)),
        )


def _open_launch_directory(attempt: Any, state: AttemptLaunches) -> JobDirectory:
    # The owned job directory is this owner's; everything below it is job-written and opened without following.
    with JobDirectory.at(attempt.owned.path) as job_dir:
        return job_dir.directory(f"{state.control_name}/{LAUNCH_DIRECTORY}")


def _client_stop_reason(attempt: Any, state: AttemptLaunches, launch: ConfinedLaunch) -> str | None:
    try:
        with _open_launch_directory(attempt, state) as directory:
            if directory.stat(stop_name(launch.request_id)) is not None:
                return "the client asked to stop the launch"
    except (FileNotFoundError, JobDirectoryError):
        return "the attempt's launch directory is gone or was replaced"
    except (FormatError, OSError) as exc:
        _LOGGER.debug("cannot check the stop marker of launch %s: %s", launch.request_id, exc)
    return None


def _poll_launch(manager: Any, attempt: Any, state: AttemptLaunches, launch: ConfinedLaunch, now: float) -> bool:
    if launch.reaped:
        return False
    returncode = launch.process.poll()
    if returncode is not None:
        if launch.exit_code is None:
            launch.exit_code = _status_code(returncode)
        if not process_group_alive(launch.process.pid):
            launch.reaped = True
            _remove_shared_memory(launch)
            fields = _fields(attempt, launch.request_id, exit_code=launch.exit_code)
            if launch.uncertain:
                _LOGGER.warning(
                    "uncertain launch %s of attempt %s was reaped at last",
                    launch.request_id,
                    attempt.attempt_id,
                    extra=manager._event("launch_stopped", attempt.owned, uncertain=True, **fields),
                )
            elif launch.stop_reason is None:
                launch.pending = LaunchStatus(launch.request_id, "exited", launch.exit_code)
                _LOGGER.info(
                    "launch %s of attempt %s exited with status %d",
                    launch.request_id,
                    attempt.attempt_id,
                    launch.exit_code,
                    extra=manager._event("launch_finished", attempt.owned, **fields),
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
                    extra=manager._event("launch_stopped", attempt.owned, **fields),
                )
            return True
        # The leader exited while processes of its group remain: they are stopped like a launch.
        _escalate(manager, attempt, state, launch, now)
        return False
    if launch.stop_reason is None:
        reason = _client_stop_reason(attempt, state, launch)
        if reason is not None:
            _stop(manager, attempt, launch, reason, now)
    if launch.stop_reason is not None:
        _escalate(manager, attempt, state, launch, now)
    return False


def _write_status(manager: Any, attempt: Any, state: AttemptLaunches, status: LaunchStatus) -> bool:
    try:
        with _open_launch_directory(attempt, state) as directory:
            directory.write_atomic(status_name(status.request_id), encode_status(status))
    except (FormatError, OSError, ValueError) as exc:
        manager._report_anomaly(
            f"launch_status:{attempt.attempt_id}:{status.request_id}",
            f"cannot write the {status.state} status of launch {status.request_id} of {attempt.owned.job_key}: "
            f"{exc}; retrying",
            manager._event("launch_status_error", attempt.owned, **_fields(attempt, status.request_id)),
            level=logging.WARNING,
        )
        return False
    manager._reported.pop(f"launch_status:{attempt.attempt_id}:{status.request_id}", None)
    return True


def _attempt_stop_reason(manager: Any, attempt: Any) -> str | None:
    if attempt.cancel is not None:
        return "the attempt is being cancelled"
    if attempt.timed_out:
        return "the attempt exceeded its maxtime"
    if attempt.interrupted or manager._draining:
        return "the manager is draining"
    if attempt.process.poll() is not None:
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
                    f"the launch directory of attempt {attempt.attempt_id} of {attempt.owned.job_key} has more "
                    f"than {MAXIMUM_ENTRIES} entries; only the first are considered",
                    manager._event("launch_directory_full", attempt.owned, attempt_id=attempt.attempt_id),
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
            extra=manager._event("launch_status_error", attempt.owned, **_fields(attempt, request_id)),
        )
        return
    _LOGGER.warning(
        "refused launch request %s of attempt %s of %s: %s",
        request_id,
        attempt.attempt_id,
        attempt.owned.job_key,
        reason,
        extra=manager._event("launch_refused", attempt.owned, **_fields(attempt, request_id, reason=reason)),
    )


#: The shell gate in front of a launch: it execs its arguments only after one line arrives on stdin (exit 125 on EOF).
_LAUNCH_GATE = ("/bin/sh", "-c", 'IFS= read -r _ || exit 125; exec "$@" </dev/null', "httk-launch-gate")


def _launch_environment() -> dict[str, str]:
    return {name: value for name, value in os.environ.items() if not name.startswith("HTTK_WORKFLOW_")}


def write_process_record(
    manager: Any, directory: Path, attempt_id: str, n: str, pid: int, *, ranks_local_only: bool
) -> None:
    """Write ``process.json`` of one launch for the death proof (plan §3.6), before its gate opens.

    :param manager: The task manager.
    :param directory: The launch directory from :meth:`httk.workflow._kernel.Owner.launch_dir`.
    :param attempt_id: The attempt.
    :param n: ``"0"`` for the attempt runner, or the launch request identifier.
    :param pid: The launch's leader, which leads its own process group.
    :param ranks_local_only: Whether every process of the launch is in that group on this host.
    """

    identity = _death.process_identity(pid)
    allocation = manager.allocation
    record = {
        "attempt_id": attempt_id,
        "n": n,
        "pid": identity.pid,
        "pgid": pid,
        "hostname": identity.hostname,
        "boot_id": identity.boot_id,
        "process_start_ticks": identity.start_ticks,
        "started_at": utc_now(),
        "allocation": None if allocation is None else RecordedAllocation.from_allocation(allocation).as_json(),
        "ranks_local_only": ranks_local_only,
    }
    _fs.write_file(_fs.loc(directory / PROCESS_FILE), json_bytes(record), durable=manager.workspace.durable)


def _start(manager: Any, attempt: Any, state: AttemptLaunches, directory: JobDirectory, request_id: str) -> str | None:
    """Start one admitted request, returning ``None``, or why it is refused."""

    context = state.context
    if context is None:
        return "the attempt has no launch binding; HTTK_WORKFLOW_LAUNCH is not available to it"
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
    streams: list[int] = []
    token = secrets.token_hex(16)
    owned = attempt.owned
    try:
        try:
            for name in (stdout_name(request_id), stderr_name(request_id)):
                streams.append(_fs.create_exclusive(_fs.anchored(directory.fd, name), durable=False))
        except FileExistsError:
            return "the launch output files already exist; only the manager creates them"
        except OSError as exc:
            return f"cannot create the launch output files: {exc}"
        try:
            trusted = manager.owner.launch_dir(attempt.attempt_id, request_id)
        except FileExistsError:
            return f"the launch directory of request {request_id} already exists"
        except (OSError, ValueError) as exc:
            return f"cannot create the launch directory: {exc}"
        try:
            durable = manager.workspace.durable
            nodefile = "".join(f"{host}\n" for host in nodefile_lines(context.placement)).encode()
            _fs.write_file(_fs.loc(trusted / NODEFILE), nodefile, durable=durable)
            description = TrustedLaunch(
                request_id=request_id,
                attempt_id=attempt.attempt_id,
                workspace_id=manager.workspace.workspace_id,
                workspace_root=manager.workspace.root,
                placement=placement_text(owned.placement()),
                job_key=owned.job_key,
                request=request_relative_path(attempt.attempt_id, request_id),
                confine=context.confinement,
                python=Path(sys.executable),
                token=token,
            )
            _fs.write_file(_fs.loc(trusted / LAUNCH_FILE), encode_trusted(description), durable=durable)
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
        except (OSError, ValueError, WorkflowError) as exc:
            _remove_launch_quietly(manager, attempt.attempt_id, request_id)
            return f"cannot start the launch: {exc}"
    finally:
        for descriptor in streams:
            os.close(descriptor)
    launch = ConfinedLaunch(request_id, process, trusted, token, context.confinement.shm_root)
    state.launches[request_id] = launch
    _LOGGER.info(
        "started launch %s of attempt %s of %s as process group %d",
        request_id,
        attempt.attempt_id,
        owned.job_key,
        process.pid,
        extra=manager._event("launch_started", owned, **_fields(attempt, request_id, pid=process.pid)),
    )
    # Only a launch template run on this host alone keeps every rank in the leader's process group; a scheduler
    # step (srun) starts its ranks outside it.
    local = context.template is not None and manager._local_share(context.placement) is not None
    try:
        write_process_record(manager, trusted, attempt.attempt_id, request_id, process.pid, ranks_local_only=local)
        # A manager frozen since admission may wake after its attempt ended or its owner was declared dead:
        # only this owner's live attempt opens the gate.
        if _still_live(manager, attempt):
            os.write(gate_write, b"\n")
        else:
            _stop(manager, attempt, launch, "the attempt is no longer this owner's live attempt", time.monotonic())
    except (OSError, WorkflowError) as exc:
        # Closing the gate unopened makes the launch exit 125 without starting anything; stop it anyway.
        _stop(manager, attempt, launch, f"cannot record the launch process: {exc}", time.monotonic())
    finally:
        os.close(gate_write)
    return None


def _remove_launch_quietly(manager: Any, attempt_id: str, request_id: str) -> None:
    try:
        manager.owner.remove_launch(attempt_id, request_id)
    except (OSError, WorkflowError) as exc:
        _LOGGER.warning("cannot remove the launch directory of request %s: %s", request_id, exc)


def _still_live(manager: Any, attempt: Any) -> bool:
    """Return whether the attempt is this manager's held, live attempt and its owner is not declared dead."""

    try:
        attempt.owned.require_quiescent()
    except WorkflowError:
        pass
    else:
        # Quiescent: the attempt ended (or never began), so nothing of it may start now.
        return False
    try:
        manager.owner.check_alive()
    except _kernel.OwnerLost:
        return False
    return manager._running.get(attempt.attempt_id) is attempt


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
            JobDirectory.at(attempt.owned.path) as job_dir,
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
            f"cannot read the launch requests of attempt {attempt.attempt_id} of {attempt.owned.job_key}: {exc}",
            manager._event("launch_scan_error", attempt.owned, attempt_id=attempt.attempt_id),
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
            if launch.reaped and launch.pending is None and _remove_trusted(manager, attempt, launch):
                # Reaped and answered: the launch is no longer evidence and holds no admission slot; its
                # status file marks the request as done.
                del state.launches[launch.request_id]
        if scan:
            changed |= _scan(manager, attempt, state)
    return changed
