"""Filesystem workflow task manager for the current core profile."""

import contextlib
import dataclasses
import logging
import math
import os
import re
import shlex
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import FrameType
from typing import Any, Literal, Self, cast

from httk.core.digests import tree_digest

from . import (
    _confine,
    _manager_cancellation,
    _manager_commit,
    _manager_joins,
    _manager_launches,
    _manager_requests,
    _manager_runners,
    _manager_scheduling,
)
from ._allocation import (
    GPU_HIDING_VARIABLES,
    Allocation,
    Node,
    bind_cpus_setting,
    format_cpulist,
    parse_cpulist,
)
from ._durations import format_duration
from ._exchange_staging import ENROLLMENT_MARKER, exchange_pass
from ._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from ._launch_protocol import LaunchConfinement
from ._manager_binding import (
    Inventory,
    NodeShare,
    Placement,
    assign,
    describe,
    nodefile_lines,
    release,
    render_launch,
)
from ._manager_launches import AttemptLaunches, LaunchContext
from ._sandbox import BWRAP_USERNS_BLOCK, PreparedSandbox
from ._util import (
    interpreter_first_path,
    json_bytes,
    read_json,
    timestamp_seconds,
    utc_now,
    write_json_atomic,
)
from .codes import code_environment
from .errors import (
    ConfinementUnavailableError,
    FormatError,
    RunnerResolutionError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .executors import AttemptLaunch, PathRunnerExecutor, RunnerExecutor
from .gc import ALWAYS_SAFE_CATEGORIES
from .journal import SEGMENT_HEADER, parse_record_ref
from .manifests import read_maintenance_lock
from .models import (
    _MARKER_PATTERN as MARKER_PATTERN,
)
from .models import (
    _MAXIMUM_JOB_DOCUMENT_BYTES as MAXIMUM_JOB_DOCUMENT_BYTES,
)
from .models import (
    ATTEMPTS_DIRECTORY,
    CARRIED_STATE_MEMBERS,
    CORE_PROFILE,
    LOGS_DIRECTORY,
    STATE_KINDS,
    TERMINAL_KINDS,
    JobDefinition,
    Marker,
    StateFrame,
    canonical_uuid,
    check_job_placement,
    normalize_placement,
    validate_attempt_control,
    validate_capacity,
    validate_process,
)
from .workspace import DISCOVERY_HEARTBEAT_STRIDE, MarkerStream, Workspace

_LOGGER = logging.getLogger(__name__)
_DRAIN_SIGNALS = (signal.SIGTERM, signal.SIGINT)
DEFAULT_RUNNER_MODULES = ("httk.workflow",)
#: The largest fraction of its own lease a manager will go without
#: heartbeating, whatever heartbeat interval it was configured with. A manager
#: whose interval exceeds its lease would expire its own claims.
_MAXIMUM_HEARTBEAT_LEASE_FRACTION = 1.0 / 3.0
#: How many markers of one kind a single tick processes before deferring the
#: rest to the next tick. Every bounded pass resumes where it stopped, so a
#: workspace larger than the bound is served round-robin rather than starved.
DEFAULT_MAXIMUM_PASS_MARKERS = 256
#: How many directory entries a single bounded pass visits before deferring the
#: rest to the next tick, whether or not it has filled its marker budget. It
#: bounds the cost of discovery itself — a tick can neither materialize nor even
#: walk an unbounded tree — while the marker budget bounds the work that
#: discovery feeds. A pass resumes its walk where it stopped on the next tick.
DEFAULT_DISCOVERY_BUDGET = 4096
#: The multiple of the lease a takeover waits for when nothing else proves that
#: the previous attempt has stopped. It is deliberately larger than one lease:
#: an expired lease alone only says that a manager is slow.
DEFAULT_TAKEOVER_GRACE_FACTOR = 2.0
#: How long a cancelled attempt has to exit after ``SIGTERM`` before the
#: process group is killed.
DEFAULT_CANCEL_GRACE_SECONDS = 10.0
#: Fractions of the lease at which a long tick is reported as a warning and as
#: an error. A tick that spends its whole lease scanning invites a peer to take
#: over jobs this manager is still running.
_TICK_WARNING_FRACTION = 0.5
_TICK_ERROR_FRACTION = 0.9
#: What a `cancelling` frame keeps beyond the carried activation members. Any
#: manager may have to finish a cancellation another one started, including one
#: that died holding it, so the frame names the attempt, where its control
#: directory is, and who was running it.
_CANCELLING_MEMBERS = (
    *CARRIED_STATE_MEMBERS,
    "attempt_control",
    "manager_id",
    "writer_id",
    "lease_seconds",
    "workdir",
    "started_at",
    "process",
    "operator",
    "operator_key",
    "operator_reason",
    "request_id",
)
_ENVIRONMENT_MARKER = ".httk-environment-resolution.json"
#: How long a failed Bubblewrap probe stands before the manager probes again.
CONFINE_REPROBE_SECONDS = 60.0
#: The most entries a check for jobs nested below a job directory visits per state kind.
_NESTED_SCAN_ENTRIES = 4096
_ENROLLED_MESSAGE = (
    "this workspace is enrolled with a workspace daemon (.httk-workspace/exchange/enrollment.json exists), so "
    "every manager "
    "on it must confine its attempts: set manager.confine=bwrap as a workspace setting or pin it with "
    "--setting manager.confine=bwrap"
)


class _ConfinementBlocked(Exception):
    """A host or operator condition prevents confined attempts; the message names it."""


@dataclass(frozen=True, slots=True)
class _Confinement:
    """The checked confinement of attempts started now.

    :param settings: The validated settings, or ``None`` for unconfined attempts.
    :param block_userns: Whether sandboxes block nested user namespaces.
    :param launch: The rank sandbox settings of confined launches, or ``None`` when unconfined.
    """

    settings: _confine.ConfineSettings | None
    block_userns: bool
    launch: LaunchConfinement | None


def _write_marker(descriptor: int, line: str, job_key: str) -> None:
    """Write one evidence marker, retaining launch progress on ordinary errors."""

    try:
        os.write(descriptor, ("\n" + line).encode("utf-8", errors="backslashreplace"))
    except Exception as exc:
        _LOGGER.warning("cannot append an evidence marker for %s: %s", job_key, exc)


def _append_log_line(job_dir: JobDirectory, line: str, *, job_key: str) -> None:
    """Append one complete evidence line to the job's stdio chronicle.

    The chronicle is opened through the job directory without following a
    symlink or blocking on a FIFO the job may have planted; such a chronicle is
    skipped with a warning, never written through.
    """

    descriptor = -1
    try:
        with job_dir.directory(LOGS_DIRECTORY, create=True) as logs:
            descriptor = logs.open_append("stdio.out")
        _write_marker(descriptor, line, job_key)
    except Exception as exc:
        _LOGGER.warning("cannot append the stdio chronicle for %s: %s", job_key, exc)
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except Exception as exc:
                _LOGGER.warning("cannot close the stdio chronicle for %s: %s", job_key, exc)


def _attempt_outcome_action(job_dir: JobDirectory, control_name: str) -> str:
    """Return an attempt's published action, or ``none`` while it is absent or unreadable."""

    try:
        outcome = job_dir.read_json(f"{control_name}/outcome.ready/outcome.json", CONTROL_DOCUMENT_LIMIT)
    except (FormatError, OSError):
        return "none"
    action = outcome.get("action")
    return action if isinstance(action, str) else "none"


def _append_attempt_event(logs: JobDirectory, record: Mapping[str, object], job_key: str) -> None:
    """Append the manager's attempt evidence without affecting launch progress.

    The line is exactly what :meth:`~httk.workflow.runtime_builders.RunLog.append_record`
    writes, appended through the pinned log directory with one write.
    """

    try:
        logs.append("runlog.jsonl", json_bytes(record) + b"\n")
    except Exception as exc:
        _LOGGER.warning("cannot append the attempt runlog event for %s: %s", job_key, exc)


def _setting_variable_name(key: str) -> str:
    """Return the environment variable synthesized for one setting name."""

    return "HTTK_" + key.upper().replace(".", "_")


@dataclass(frozen=True)
class WorkCensus:
    """What one manager's scan found, tagged by why each job is or is not its work.

    ``ready_blocked`` groups the ready and unregisterable-submitted jobs this
    manager cannot progress by the requirement it lacks — ``executor``, ``pool``,
    ``capability``, ``requirements`` (an unmet ``requires`` entry of the job, checked in
    this manager's environment), ``resources``, ``time`` (a ``mintime`` beyond the
    time left before this manager's drain start, or ``drain_point`` once it has passed), or
    ``confinement`` (every claimable job while a host or operator condition holds back
    confined attempts) — mapping each requirement to the count of jobs it would
    turn away. Every such job is attributed to exactly one requirement, so the
    grouped counts sum to :attr:`ready_blocked_total`.

    :param succeeded: Terminal jobs that succeeded.
    :param failed: Terminal jobs that failed.
    :param ready_claimable: Ready jobs this manager could claim right now.
    :param ready_blocked: Requirement kind to requirement to blocked job count.
    :param waiting: Jobs waiting on their join children.
    :param paused: Jobs paused for an operator.
    :param actionable_count: Jobs this manager can still make progress on.
    :param unreadable: Committing or cancelling jobs whose definition cannot be read.
    """

    succeeded: int
    failed: int
    ready_claimable: int
    ready_blocked: Mapping[str, Mapping[str, int]]
    waiting: int
    paused: int
    actionable_count: int
    unreadable: int = 0

    @property
    def actionable(self) -> bool:
        """Whether this manager still has work it can make progress on.

        :return: Whether any counted job is this manager's to progress.
        """

        return self.actionable_count > 0

    @property
    def ready_blocked_total(self) -> int:
        """The number of jobs no requirement of this manager can claim here.

        :return: The total blocked job count.
        """

        return sum(sum(group.values()) for group in self.ready_blocked.values())

    def _blocked_groups(self) -> list[str]:
        groups: list[str] = []
        for kind in ("executor", "pool", "capability", "requirements", "calls", "resources", "time", "confinement"):
            for name, count in sorted(self.ready_blocked.get(kind, {}).items()):
                label = {
                    "resources": f"resource={name}",
                    "requirements": f"requires {name}",
                    "calls": name,
                    "time": "past the drain point" if name == "drain_point" else f"{name} beyond the time left",
                    "confinement": "confinement unavailable",
                }.get(kind, f"{kind}={name}")
                groups.append(f"{label}: {count}")
        return groups

    def summary_line(self) -> str:
        """Render the one-line idle summary an operator reads on exit.

        :return: The idle summary line.
        """

        total = self.ready_blocked_total
        blocked = f"{total} not claimable here"
        if total:
            blocked += f" ({', '.join(self._blocked_groups())})"
        line = (
            "idle: "
            f"{self.succeeded} succeeded, {self.failed} failed, {blocked}, "
            f"{self.waiting} waiting on children, {self.paused} paused"
        )
        if self.unreadable:
            line += f", {self.unreadable} with an unreadable definition"
        return line

    def time_advice(self) -> str | None:
        """Explain the ready jobs held back by this manager's allocation end.

        :return: The advice, or ``None`` when no job is held back for time.
        """

        timed = self.ready_blocked.get("time", {})
        parts: list[str] = []
        if timed.get("mintime"):
            parts.append(
                f"{timed['mintime']} ready job(s) need more time than this manager has left; start a manager "
                "with a longer allocation (slurm.time_limit / --time-limit) or lower the jobs' mintime"
            )
        if timed.get("drain_point"):
            parts.append(
                f"{timed['drain_point']} ready job(s) wait because this manager is past its drain point; "
                "start a manager with a fresh allocation"
            )
        return "; ".join(parts) or None

    def mismatch_advice(self) -> str | None:
        """Name the requirements blocked jobs need that this manager lacks.

        :return: The mismatch advice, or ``None`` when nothing is blocked.
        """

        pools = sorted(self.ready_blocked.get("pool", {}))
        capabilities = sorted(self.ready_blocked.get("capability", {}))
        executors = sorted(self.ready_blocked.get("executor", {}))
        resources = sorted(self.ready_blocked.get("resources", {}))
        requirements = sorted(self.ready_blocked.get("requirements", {}))
        calls = sorted(self.ready_blocked.get("calls", {}))
        time_advice = self.time_advice()
        timed = time_advice is not None
        if not (pools or capabilities or executors or resources or requirements or calls):
            return time_advice
        if resources and not (pools or capabilities or executors or requirements or calls or timed):
            count = sum(self.ready_blocked["resources"].values())
            names = ", ".join(f"`{name}`" for name in resources)
            resource_flags = " ".join(f"--worker-resource {name} COUNT" for name in resources)
            return (
                f"{count} ready job(s) need resource {names} beyond this manager's capacity; "
                f"start a manager with {resource_flags}, or pass --idle to keep serving"
            )
        lacks: list[str] = []
        remedies: list[str] = []
        flags: list[str] = []
        if pools:
            lacks.append("pool(s) " + ",".join(pools))
            flags += [f"--pool {name}" for name in pools]
        if capabilities:
            lacks.append("capability(ies) " + ",".join(capabilities))
            flags += [f"--capability {name}" for name in capabilities]
        if resources:
            lacks.append("resource(s) " + ",".join(resources))
            flags += [f"--worker-resource {name} COUNT" for name in resources]
        if flags:
            remedies.append("start a manager with " + " ".join(flags))
        # An executor is installed, not passed as a flag, so it gets its own
        # remedy clause rather than being dropped when a pool or capability also
        # mismatches.
        if executors:
            lacks.append("executor(s) " + ",".join(executors))
            remedies.append("run a manager that has executor(s) " + ",".join(executors) + " installed")
        if requirements:
            lacks.append("the job requirement(s) " + "; ".join(requirements))
            remedies.append(
                "install the required distribution versions in this manager's environment and restart the manager"
            )
        if calls:
            lacks.append("the called workflow(s) " + "; ".join(calls))
            remedies.append(
                "install or build the called workflows on this machine as each problem says "
                "(a running manager notices within a minute)"
            )
        remedy = "; ".join(remedies) if remedies else "start a manager that serves them"
        advice = (
            f"{self.ready_blocked_total - sum(self.ready_blocked.get('time', {}).values())} job(s) cannot be "
            f"claimed here because this manager does not serve {'; '.join(lacks)}; {remedy}, "
            "or pass --idle to keep serving"
        )
        return advice if time_advice is None else f"{advice}; {time_advice}"

    def timeout_message(self, seconds: float) -> str:
        """Render the not-idle advice, naming mismatches when there are any.

        :param seconds: The idle timeout that elapsed.
        :return: The not-idle advice line.
        """

        base = f"workspace is not idle after {seconds:.0f}s"
        parts: list[str] = []
        advice = self.mismatch_advice()
        if advice is not None:
            parts.append(advice)
        if self.unreadable:
            parts.append(
                f"{self.unreadable} job(s) have an unreadable definition — repair them with 'httk workspace fsck'"
            )
        if not parts:
            parts.append(
                "jobs are still running or claimable — rerun, raise --idle-timeout, or pass --idle to keep serving"
            )
        return f"{base}; {'; '.join(parts)}"


class NotIdleError(TimeoutError):
    """A manager did not become idle within its timeout.

    It carries the :class:`~httk.workflow.manager.WorkCensus` of the final
    scan so a caller can turn the failure into advice that names the actual
    pool, capability, or executor mismatches rather than a generic hint. It
    subclasses :class:`TimeoutError`, so existing ``except TimeoutError``
    callers keep working.

    :param census: The work census of the manager's last scan.
    """

    def __init__(self, census: WorkCensus) -> None:
        super().__init__("workflow manager did not become idle")
        self.census = census


@dataclass
class RunningAttempt:
    """Track one locally running job attempt.

    :param marker: Identify the claimed job marker.
    :param process: Track the launched process group.
    :param attempt_id: Identify the running attempt.
    :param resources: Reserve these resources while the attempt is running.
    :param outcome_action: Remember the published action if control cleanup wins
        the race with process reaping.
    :param cleanup_pending: Mark a non-terminal commit waiting for process reap.
    :param cleanup_request: Retain the commit transition needed at process reap.
    :param reaped: Record that this manager knows the process return code.
    :param fenced: Mark an attempt whose marker ownership is already resolved.
    :param cancelling: Mark an attempt currently being cancelled.
    :param owner_uid: Record the operating-system owner when known.
    :param started: Record the monotonic launch time the ``maxtime`` is measured from.
    :param maxtime: Stop the attempt after this many seconds, or never when ``None``.
    :param timeout_kill_at: Escalate a timed-out attempt to ``SIGKILL`` at this
        monotonic time, or ``None`` once escalated or before a timeout.
    :param timed_out: Mark an attempt this manager stopped for exceeding its ``maxtime``.
    :param interrupted: Mark an attempt this manager signalled while draining.
    :param placement: The nodes and slots this attempt was given, or ``None``
        when the manager has no node inventory; returned to the inventory when
        the attempt stops being tracked.
    :param sweep_kill_at: Escalate an untracked (fenced or orphaned) attempt that
        outlives its ``SIGTERM`` to ``SIGKILL`` at this monotonic time, or
        ``None`` before the first ``SIGTERM``.
    :param confined: Mark an attempt started inside the attempt sandbox.
    :param launches: The confined launches of a confined attempt, or ``None``
        for an unconfined one; the attempt stays tracked until all are reaped.
    """

    marker: Marker
    process: subprocess.Popen[bytes]
    control: Path
    attempt_id: str
    resources: Mapping[str, int]
    started: float
    maxtime: int | None
    outcome_action: str | None = None
    cleanup_pending: bool = False
    cleanup_request: tuple[Marker, StateFrame, Marker] | None = None
    reaped: bool = False
    # Set once this attempt's outcome has been committed or its marker has been
    # fenced: the process may still be exiting, and reaping it is then routine
    # rather than the discovery of an orphan.
    fenced: bool = False
    # Set while a cancellation is stopping this attempt. It stays tracked until
    # its exit has been verified, because the verification is exactly what the
    # cancelled state has to record.
    cancelling: bool = False
    owner_uid: int | None = None
    timeout_kill_at: float | None = None
    timed_out: bool = False
    interrupted: bool = False
    placement: Placement | None = None
    sweep_kill_at: float | None = None
    confined: bool = False
    launches: AttemptLaunches | None = None

    def __repr__(self) -> str:
        return f"RunningAttempt(attempt_id={self.attempt_id!r}, pid={self.process.pid})"


class TaskManager:
    """Execute and recover jobs in one workflow workspace.

    :param workspace: Attach the manager to this workspace.
    :param pools: Accept jobs assigned to these pools.
    :param capabilities: Advertise these execution capabilities.
    :param resources: Advertise these integer resource capacities.
    :param maximum_workers: Limit the number of local attempts.
    :param lease_seconds: Override the workspace claim lease.
    :param heartbeat_interval: Set the requested manager heartbeat interval.
    :param unsafe_persistent_takeover: Permit takeover based on persistent evidence.
    :param unsafe_isolated_takeover: Permit takeover based on isolated evidence.
    :param takeover_grace_factor: Multiply the lease to determine takeover grace.
    :param executors: Add runner executors to the built-in executor.
    :param allowed_executors: Restrict jobs to these installed executors.
    :param accept_any_pool: Accept jobs without requiring a configured pool match.
    :param join_grace_seconds: Wait this long for unresolved join children.
    :param cancel_grace_seconds: Wait this long after cancellation before killing.
    :param maximum_pass_markers: Bound markers processed in one scheduling pass.
    :param discovery_budget: Bound entries visited in one scheduling pass.
    :param placement_prefixes: Restrict scheduling to these placement subtrees.
    :param runner_search_paths: Search these locations for installed runners.
    :param runner_modules: Search these module prefixes for packaged runners.
    :param gc_interval: Run background collection at this interval when supplied.
    :param on_attached: Call this after the manager directory and heartbeat are
        published, before startup collection runs.
    :param end_time: The epoch second this manager's allocation ends, or ``None`` when unknown.
    :param deadline_margin: Stop claiming work this many seconds before *end_time*.
    :param allocation: The probed allocation this manager runs inside, or ``None``;
        recorded in ``manager.json``. Its capacity and end time are already folded
        into *resources* and *end_time* by the caller.
    :param exchange: Run the workspace-daemon exchange pass (adopt staged job
        directories, eject finished jobs, publish status) at the start of every tick.
    :param setting_overrides: Pinned ``manager.confine``, ``manager.launch_template``,
        ``manager.bind_cpus`` and ``confine.*`` settings that win over the
        workspace settings for this manager's lifetime.
    :raises ValueError: If a manager limit, a pinned setting or the effective
        confinement settings are invalid, or executor configuration conflicts.
    :raises httk.workflow.errors.UnsupportedExtensionError: If the workspace profile is not writable by this manager.
    :raises httk.workflow.errors.ConfinementUnavailableError: If the effective
        ``manager.confine`` is ``bwrap`` and Bubblewrap cannot build the attempt
        sandbox here, or the workspace is enrolled with a workspace daemon and the
        effective ``manager.confine`` is not ``bwrap``.
    """

    def __init__(
        self,
        workspace: Workspace,
        *,
        pools: Sequence[str] = ("default",),
        capabilities: Sequence[str] = (),
        resources: Mapping[str, int] | None = None,
        maximum_workers: int = 1,
        lease_seconds: float | None = None,
        heartbeat_interval: float = 30.0,
        unsafe_persistent_takeover: bool = False,
        unsafe_isolated_takeover: bool = False,
        takeover_grace_factor: float = DEFAULT_TAKEOVER_GRACE_FACTOR,
        executors: Sequence[RunnerExecutor] = (),
        allowed_executors: Sequence[str] | None = None,
        accept_any_pool: bool = False,
        join_grace_seconds: float = 3600.0,
        cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
        maximum_pass_markers: int = DEFAULT_MAXIMUM_PASS_MARKERS,
        discovery_budget: int = DEFAULT_DISCOVERY_BUDGET,
        placement_prefixes: Sequence[str] = (),
        runner_search_paths: Iterable[str | os.PathLike[str]] = (),
        runner_modules: Iterable[str] = DEFAULT_RUNNER_MODULES,
        gc_interval: float | None = None,
        on_attached: Callable[[str], None] | None = None,
        end_time: float | None = None,
        deadline_margin: float = 120.0,
        allocation: Allocation | None = None,
        exchange: bool = False,
        setting_overrides: Mapping[str, str] | None = None,
    ) -> None:
        if maximum_workers < 1:
            raise ValueError("maximum_workers must be positive")
        try:
            if resources is None:
                resources = {} if allocation is None else allocation.capacity()
            validated_resources = validate_capacity(resources, "manager.resources")
        except FormatError as exc:
            raise ValueError(str(exc)) from exc
        if discovery_budget < 1:
            raise ValueError("discovery_budget must be positive")
        if gc_interval is not None and gc_interval <= 0.0:
            raise ValueError("gc_interval must be positive")
        if join_grace_seconds < 0:
            raise ValueError("join_grace_seconds cannot be negative")
        if cancel_grace_seconds < 0:
            raise ValueError("cancel_grace_seconds cannot be negative")
        if maximum_pass_markers < 1:
            raise ValueError("maximum_pass_markers must be positive")
        if takeover_grace_factor < 1.0:
            raise ValueError("takeover_grace_factor cannot be shorter than one lease")
        if end_time is not None and not (math.isfinite(end_time) and end_time > 0):
            raise ValueError("end_time must be a finite positive epoch second")
        if not (math.isfinite(deadline_margin) and deadline_margin >= 0):
            raise ValueError("deadline_margin cannot be negative")
        if workspace.core_profile != CORE_PROFILE:
            # Serving a workspace means writing it, so an older profile is
            # refused here as well as at attach time.
            raise UnsupportedExtensionError(
                f"cannot serve a {workspace.core_profile!r} workspace: this manager writes {CORE_PROFILE!r}"
            )
        overrides = dict(setting_overrides or {})
        for key, value in overrides.items():
            if not isinstance(key, str) or not _confine.is_override_key(key) or not isinstance(value, str):
                raise ValueError(
                    f"a pinned setting must be manager.confine, manager.launch_template, manager.bind_cpus "
                    f"or confine.* with a string value: {key!r}"
                )
        self.workspace = workspace
        #: Pinned settings that win over the workspace settings for this manager's lifetime.
        self.setting_overrides: dict[str, str] = overrides
        # The functional Bubblewrap probe per probed sandbox (executable,
        # network isolation, user-namespace block): whether attempts block
        # nested user namespaces, or why confinement is unavailable, and when
        # it was probed. A manager started confined probes before it attaches,
        # so an unusable host refuses the start; a workspace switched to bwrap
        # later is probed at its next claim pass, and a failed probe is
        # repeated at most every CONFINE_REPROBE_SECONDS.
        self._bwrap_probes: dict[tuple[Path, bool, tuple[str, ...]], tuple[bool | str, float]] = {}
        # The confinement settings last validated, keyed by the confinement
        # keys of the effective settings, or why they are invalid.
        self._confine_checked: tuple[tuple[tuple[str, str], ...], _confine.ConfineSettings | str] | None = None
        self._confinement(self._effective_settings(), at_start=True)
        self.uid = os.getuid()
        # Ordered roots for jobs whose runner.source is installed, plus the
        # module prefixes the reserved pkg: form may name. Both are deployment
        # policy of this manager and never taken from a job.
        self.runner_search_paths: tuple[Path, ...] = tuple(Path(item).expanduser() for item in runner_search_paths)
        self.runner_modules: tuple[str, ...] = tuple(runner_modules)
        self.pools = frozenset(pools)
        self.capabilities = frozenset(capabilities)
        self.resources = validated_resources
        self.maximum_workers = maximum_workers
        self.end_time = end_time
        self.allocation = allocation
        # Per-node free capacity when the allocation lists its nodes; the
        # placed labels are then scheduled by it instead of by counters.
        self._inventory = Inventory.from_allocation(allocation, validated_resources)
        if self._inventory is None and allocation is not None and allocation.nodes:
            _LOGGER.warning(
                "the manager capacity exceeds the allocation's nodes; scheduling by counts only, without placement"
            )
        if self._inventory is not None:
            # Advertise what the inventory can place, such as only the kept
            # nodes' procs under --worker-resource nodes 1.
            totals = self._inventory.empty().free()
            for label in self._inventory.labels & self.resources.keys():
                self.resources[label] = totals[label]
        # Placements of attempts whose launch is in progress, by attempt id.
        self._unlaunched: dict[str, Placement] = {}
        # The epoch second this manager stops claiming work that cannot fit, or
        # None when its allocation end is unknown.
        self.drain_start = None if end_time is None else end_time - deadline_margin
        # A lease is workspace policy unless this manager overrides it, so two
        # managers of one workspace expire each other's claims consistently.
        self.lease_seconds = workspace.policy.lease_seconds if lease_seconds is None else lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.unsafe_persistent_takeover = unsafe_persistent_takeover
        self.unsafe_isolated_takeover = unsafe_isolated_takeover
        self.takeover_grace_factor = takeover_grace_factor
        self.join_grace_seconds = join_grace_seconds
        self.cancel_grace_seconds = cancel_grace_seconds
        self.maximum_pass_markers = maximum_pass_markers
        self.discovery_budget = discovery_budget
        # Placement subtrees this manager restricts every scheduling scan to, as
        # deployment policy like pools and capabilities. An empty assignment is
        # the whole workspace. Overlapping assignments stay safe on the
        # rename-claim; disjoint ones simply stop two managers scanning each
        # other's trees. The values are project-owned placement semantics; the
        # engine only validates and filters on them.
        self.placement_prefixes: tuple[PurePosixPath, ...] = tuple(
            normalize_placement(prefix) for prefix in placement_prefixes
        )
        # Background collection is off unless a deployment asks for it. It is a
        # housekeeping timer of this manager, never part of a scheduling
        # decision: it runs at the end of a tick, after every pass has decided
        # what to do, and at most once per interval.
        self.gc_interval = gc_interval
        self._last_gc = 0.0
        self.exchange = exchange
        executors = [PathRunnerExecutor(), *executors]
        self.executors = {executor.name: executor for executor in executors}
        if len(self.executors) != len(executors):
            raise ValueError("runner executor names must be unique")
        self.allowed_executors = (
            frozenset(self.executors) if allowed_executors is None else frozenset(allowed_executors)
        )
        unknown_allowed = self.allowed_executors - self.executors.keys()
        if unknown_allowed:
            raise ValueError(f"allowed runner executors are not installed: {', '.join(sorted(unknown_allowed))}")
        self.accept_any_pool = accept_any_pool
        self.manager_id = str(uuid.uuid4())
        self.hostname = socket.gethostname()
        self.writer = workspace.open_journal_writer()
        self._manager_dir = workspace.control / "managers" / self.manager_id
        self._manager_dir.mkdir(parents=True, exist_ok=False)
        self._running: dict[str, RunningAttempt] = {}
        # The monotonic time of the last scan of the confined attempts' launch
        # directories; scans are rate limited, launch supervision is not.
        self._last_launch_scan = -math.inf
        # Attempts reaped just before their running marker became committing.
        # This distinguishes a local process exit from an inherited commit;
        # entries live only until the commit cleanup decision is made.
        self._reaped_attempts: set[str] = set()
        # Running markers whose ownership could not be checked this pass are
        # preserved from orphan sweeping until the next pass can retry them.
        self._indeterminate_ownership: set[str] = set()
        # Anomaly keys whose committing-wedge sidecar has already been written,
        # so a permanently stuck commit records its error into the attempt
        # control directory once rather than on every poll.
        self._commit_wedge_recorded: set[str] = set()
        # Bounded pass name -> its streaming walker. Each keeps a per-root cursor
        # and rotation in memory so the next tick resumes where this one stopped
        # and no placement subtree starves; nothing is written to disk.
        self._streams: dict[str, MarkerStream] = {}
        # Attempt id -> the monotonic instant after which a cancelled attempt
        # that has not exited is killed.
        self._cancel_kill_at: dict[str, float] = {}
        # Attempt ids whose unverifiable cancellation has already been recorded,
        # so a foreign-host cancellation warns and journals once, not per tick.
        self._cancel_unverified: set[str] = set()
        # Request file names this manager cannot act on but another manager
        # may, remembered so they are not read again on every tick.
        self._deferred_requests: set[str] = set()
        self._last_heartbeat = 0.0
        # Repeating anomaly key -> last reported text, so a permanently broken
        # job is reported loudly once instead of once per poll interval.
        self._reported: dict[str, str] = {}
        self._reset_drain()
        self._closed = False
        write_json_atomic(
            self._manager_dir / "manager.json",
            {
                "format": "httk-workflow-manager",
                "format_version": 2,
                "manager_id": self.manager_id,
                "writer_id": self.writer.writer_id,
                "hostname": self.hostname,
                "pid": os.getpid(),
                "uid": self.uid,
                "pools": sorted(self.pools),
                "capabilities": sorted(self.capabilities),
                "placement_prefixes": [prefix.as_posix() for prefix in self.placement_prefixes],
                "executors": sorted(self.allowed_executors),
                "runner_search_paths": [str(path) for path in self.runner_search_paths],
                "runner_modules": list(self.runner_modules),
                "accept_any_pool": self.accept_any_pool,
                "resources": dict(self.resources),
                "end_time": self.end_time,
                "drain_start": self.drain_start,
                "allocation": None if allocation is None else allocation.kind,
                "nodes": [] if allocation is None else [node.host for node in allocation.nodes],
                "started_at": utc_now(),
            },
            durable=workspace.durable,
        )
        self.heartbeat(force=True)
        if on_attached is not None:
            on_attached(self.manager_id)
        _LOGGER.info(
            "manager %s attached to workspace %s as %s pools=%s capabilities=%s executors=%s workers=%d",
            self.manager_id,
            self.workspace.workspace_id,
            self.hostname,
            ",".join(sorted(self.pools)) or "-",
            ",".join(sorted(self.capabilities)) or "-",
            ",".join(sorted(self.allowed_executors)),
            self.maximum_workers,
            extra=self._event("manager_started", workspace=str(self.workspace.root)),
        )
        if self.drain_start is not None and self.drain_start <= time.time():
            _LOGGER.warning(
                "the allocation's drain point passed %.0f s before this manager started; it will claim nothing",
                time.time() - self.drain_start,
                extra=self._event("drain_point_passed", end_time=self.end_time),
            )
        for name in self.allowed_executors:
            try:
                self.executors[name].reconcile(self.workspace)
            except (WorkflowError, OSError) as exc:
                # Executor views are derived and must never prevent the manager
                # from attaching to authoritative marker state.
                _LOGGER.warning(
                    "runner executor %s could not reconcile its derived view: %s",
                    name,
                    exc,
                    extra=self._event("executor_error", executor=name),
                )
                continue
        self._warn_unmatched_placement_prefixes()
        self._collect_garbage("startup", categories=ALWAYS_SAFE_CATEGORIES)

    def __repr__(self) -> str:
        return f"TaskManager(workspace={self.workspace!r}, pools={tuple(sorted(self.pools))!r})"

    def _warn_unmatched_placement_prefixes(self) -> None:
        """Warn once for each configured prefix that matches no state subtree.

        A prefix that names nothing may be a typo, or simply a manager started
        before its jobs are submitted. The wording covers both honestly — the
        manager will serve that subtree once work arrives there — while still
        surfacing the common typo as one diagnosable line per empty prefix.
        """

        for prefix in self.placement_prefixes:
            # ponytail: short-circuit on the first marker below the prefix; a
            # populated subtree costs one directory read, an empty one a full
            # (bounded, one-time) walk.
            try:
                found = next(iter(self.workspace.walk_markers(roots=(prefix,))), None)
            except (WorkflowError, OSError) as exc:
                _LOGGER.debug("cannot check placement prefix %s: %s", prefix.as_posix(), exc)
                continue
            if found is None:
                _LOGGER.warning(
                    "placement prefix %s currently matches no job in this workspace; this manager will serve that "
                    "subtree when work arrives there, and claim nothing until then — check it if this is unexpected",
                    prefix.as_posix(),
                    extra=self._event("placement_prefix_empty", placement_prefix=prefix.as_posix()),
                )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        """Close local tracking and clean up after a clean manager exit."""

        if self._closed:
            return
        clean = not self._running
        if clean:
            try:
                clean = not self._has_live_owned_marker()
            except Exception as exc:
                _LOGGER.warning("cannot verify manager-owned live markers: %s", exc)
                clean = False
        if clean:
            self._collect_garbage("shutdown")
        self._running.clear()
        try:
            self.writer.close()
        finally:
            self._closed = True
            if clean:
                self._remove_empty_journal_writer()
                self._remove_manager_directory()

    def _collect_garbage(self, phase: str, *, categories: Sequence[str] | None = None) -> None:
        """Collect workspace garbage, without affecting manager service."""

        try:
            report = self.workspace.collect_garbage(
                categories=categories,
                journal_writer=self.writer,
            )
        except Exception as exc:
            _LOGGER.warning(
                "garbage collection at %s failed: %s",
                phase,
                exc,
                extra=self._event("always_safe_gc_error", phase=phase),
            )
            return
        _LOGGER.info(
            "garbage collection at %s removed %d entries and about %d bytes",
            phase,
            report.removed,
            report.bytes_reclaimed,
            extra=self._event(
                "always_safe_gc_completed",
                phase=phase,
                removed=report.removed,
                bytes_reclaimed=report.bytes_reclaimed,
            ),
        )

    def _has_live_owned_marker(
        self, kinds: tuple[str, ...] = ("claimed", "running", "committing", "cancelling")
    ) -> bool:
        """Return whether this manager still owns a state marker of one of *kinds*."""

        for marker in self.workspace.scan_markers(kinds):
            try:
                state = self.workspace.read_state(marker)
            except (WorkflowError, OSError):
                return True
            if state.get("manager_id") == self.manager_id:
                return True
        return False

    def _remove_manager_directory(self) -> None:
        """Remove this manager's metadata directory after its writer closes."""

        try:
            (self._manager_dir / _manager_launches.LAUNCHES_DIRECTORY).rmdir()
        except FileNotFoundError:
            pass
        except OSError as exc:
            # A trusted launch directory left behind may record a live launch:
            # it is takeover evidence, so the whole manager record stays.
            _LOGGER.warning("manager directory %s keeps launch records: %s", self._manager_dir, exc)
            return
        for name in ("heartbeat.json", "manager.json"):
            try:
                (self._manager_dir / name).unlink(missing_ok=True)
            except OSError as exc:
                _LOGGER.warning("cannot remove manager metadata %s: %s", name, exc)
                return
        try:
            self._manager_dir.rmdir()
        except OSError as exc:
            _LOGGER.debug("manager directory %s remains after clean exit: %s", self._manager_dir, exc)

    def _remove_empty_journal_writer(self) -> None:
        """Remove this manager's writer directory when it has no frames."""

        writer_dir = self.workspace.control / "journal" / self.writer.writer_id
        if self._writer_has_marker_reference():
            return
        try:
            entries = list(writer_dir.iterdir())
        except OSError as exc:
            _LOGGER.debug("cannot inspect empty journal writer %s: %s", writer_dir, exc)
            return
        if not entries:
            return
        for entry in entries:
            try:
                if not entry.is_file() or entry.stat().st_size != len(SEGMENT_HEADER):
                    return
            except OSError:
                return
        try:
            for entry in entries:
                entry.unlink()
            writer_dir.rmdir()
        except OSError as exc:
            _LOGGER.debug("journal writer %s remains after clean exit: %s", writer_dir, exc)

    def _writer_has_marker_reference(self) -> bool:
        """Return whether a current marker names this writer."""

        for marker in self.workspace.scan_markers(STATE_KINDS):
            try:
                writer_id, _segment, _offset, _length, _checksum = parse_record_ref(marker.record_ref)
            except (FormatError, ValueError):
                continue
            if writer_id == self.writer.writer_id:
                return True
        return False

    def _write_attempt_end(self, attempt: RunningAttempt, return_code: int) -> None:
        """Append the end marker for a locally reaped attempt."""

        try:
            with self._job_directory(attempt.marker) as job_dir:
                action = attempt.outcome_action or _attempt_outcome_action(
                    job_dir, f"{ATTEMPTS_DIRECTORY}/{attempt.attempt_id}"
                )
                _append_log_line(
                    job_dir,
                    f"=== httk attempt {attempt.attempt_id} ended {utc_now()} exit {return_code} outcome {action}\n",
                    job_key=attempt.marker.job_key,
                )
        except Exception as exc:
            _LOGGER.warning("cannot append the end marker for %s: %s", attempt.marker.job_key, exc)

    def _finish_attempt_cleanup(self, attempt: RunningAttempt) -> None:
        """Remove a committed attempt control after its process was reaped."""

        request = attempt.cleanup_request
        if attempt.cleanup_pending and request is not None:
            _manager_commit._remove_committed_attempt_control(self, *request)

    @property
    def manager_directory(self) -> Path:
        """Return this manager's own directory below ``managers/``.

        :return: The manager directory path.
        """

        return self._manager_dir

    def _event(self, event: str, marker: Marker | None = None, **fields: object) -> dict[str, object]:
        """Return structured logging fields describing one manager event."""

        data: dict[str, object] = {"event": event, "manager_id": self.manager_id}
        if marker is not None:
            data.update(
                {
                    "job_key": marker.job_key,
                    "job_id": marker.job_id,
                    "placement": marker.placement.as_posix(),
                    "kind": marker.kind,
                    "generation": marker.generation,
                }
            )
        data.update(fields)
        return data

    def _report_anomaly(
        self,
        key: str,
        text: str,
        fields: Mapping[str, object],
        *,
        level: int = logging.ERROR,
    ) -> None:
        """Report a possibly repeating anomaly loudly once, then quietly."""

        if self._reported.get(key) == text:
            _LOGGER.debug("%s (unchanged)", text, extra=dict(fields))
            # A commit anomaly that repeats unchanged is a wedge, not a
            # transient: the first pass reports it loudly, and once it recurs
            # its text is persisted where 'job why' can surface it.
            if key.startswith("resume_committing:"):
                self._record_commit_wedge(key, text, fields)
            return
        self._reported[key] = text
        _LOGGER.log(level, "%s", text, extra=dict(fields))

    def _record_commit_wedge(self, key: str, text: str, fields: Mapping[str, object]) -> None:
        """Persist a repeating commit anomaly into the newest attempt control dir."""

        if key in self._commit_wedge_recorded:
            return
        job_key = fields.get("job_key")
        placement = fields.get("placement")
        if not isinstance(job_key, str) or not isinstance(placement, str):
            return
        try:
            marker = self.workspace.find_marker_at(job_key, normalize_placement(placement))
            if marker is None or marker.kind != "committing":
                return
            control_name = self._attempt_control_name(self._read_frame(marker))
            with self._job_directory(marker) as job_dir, job_dir.directory(control_name, create=True) as control:
                control.write_atomic(
                    "commit-wedge.json",
                    json_bytes(
                        {
                            "format": "httk-workflow-commit-wedge",
                            "format_version": 2,
                            "error": text,
                            "manager_id": self.manager_id,
                            "recorded_at": utc_now(),
                        }
                    )
                    + b"\n",
                    durable=self.workspace.durable,
                )
        except (FormatError, WorkflowError, OSError) as exc:
            _LOGGER.debug("cannot record the commit wedge of %s: %s", job_key, exc)
            return
        self._commit_wedge_recorded.add(key)

    @property
    def heartbeat_period(self) -> float:
        """Return how long this manager may actually go without heartbeating.

        A configured interval longer than the lease it claims work under would
        let a manager expire its own claims, so the interval is capped at a
        fraction of the lease however it was configured.

        :return: The effective heartbeat interval.
        """

        if self.lease_seconds <= 0.0:
            return self.heartbeat_interval
        return min(self.heartbeat_interval, self.lease_seconds * _MAXIMUM_HEARTBEAT_LEASE_FRACTION)

    def heartbeat(self, *, force: bool = False) -> None:
        """Publish a manager heartbeat when the effective interval has elapsed.

        :param force: Publish immediately instead of honoring the interval.
        """

        now = time.monotonic()
        if not force and now - self._last_heartbeat < self.heartbeat_period:
            return
        write_json_atomic(
            self._manager_dir / "heartbeat.json",
            {"manager_id": self.manager_id, "updated_at": utc_now()},
            durable=self.workspace.durable,
        )
        self._last_heartbeat = now

    def _pace(self) -> None:
        """Take one heartbeat opportunity between units of scanning work.

        Every pass calls this once per marker. The write itself is rate limited
        by :attr:`heartbeat_period`, so pacing costs one clock reading per
        marker — and buys the guarantee that a workspace too large to scan
        inside one lease can no longer make a peer conclude that this manager
        has died while it is working perfectly normally.
        """

        self.heartbeat()

    def _owns(self, marker: Marker) -> bool | None:
        """Check kernel-enforced same-user provenance, not authentication.

        The conjunction proves that this manager's uid owns the marker, payload
        directory, and job definition; it deliberately does not authenticate
        the contents.
        """
        try:
            marker_stat = marker.path.lstat()
        except FileNotFoundError:
            _LOGGER.debug("skipping job %s: marker no longer exists", marker.job_key)
            return False
        except OSError as exc:
            _LOGGER.debug("deferring job %s: ownership check is indeterminate: %s", marker.job_key, exc)
            return None
        try:
            if not stat.S_ISREG(marker_stat.st_mode):
                _LOGGER.debug("skipping job %s: marker is not an owned regular file", marker.job_key)
                return False
            if marker_stat.st_uid != self.uid:
                _LOGGER.debug(
                    "skipping job %s: marker is owned by uid %d, manager runs uid %d",
                    marker.job_key,
                    marker_stat.st_uid,
                    self.uid,
                )
                return False
            payload = self.workspace.payload_path(marker.placement, marker.job_key)
            payload_stat = payload.lstat()
            if stat.S_ISLNK(payload_stat.st_mode) or not stat.S_ISDIR(payload_stat.st_mode):
                _LOGGER.debug(
                    "skipping job %s: payload path is not an owned directory (marker/payload ownership mismatch)",
                    marker.job_key,
                )
                return False
            job_path = payload / "job.json"
            job_stat = job_path.lstat()
            # A symlink or special file this uid planted as job.json in its own
            # payload is not foreign: the job is this manager's, and loading its
            # refused definition fails it instead of hiding it forever.
            if payload_stat.st_uid != marker_stat.st_uid or job_stat.st_uid != marker_stat.st_uid:
                _LOGGER.debug(
                    "skipping job %s: marker, payload, and job.json ownership mismatch",
                    marker.job_key,
                )
                return False
            return True
        except FileNotFoundError as exc:
            _LOGGER.debug("deferring job %s: ownership check is indeterminate: %s", marker.job_key, exc)
            return None
        except OSError as exc:
            _LOGGER.debug("deferring job %s: ownership check is indeterminate: %s", marker.job_key, exc)
            return None

    @staticmethod
    def _request_owner(path: Path) -> int:
        return path.lstat().st_uid

    def _window(self, pass_name: str, kind: str) -> list[Marker]:
        """Return the *kind* markers *pass_name* processes this tick.

        Discovery is streaming and bounded: the pass walks its assigned
        placement subtrees with :class:`MarkerStream`, visiting at most
        ``discovery_budget`` directory entries and collecting at most
        ``maximum_pass_markers`` markers, heartbeating from inside the walk. The
        walker resumes where it stopped on the next tick and rotates its roots,
        so a workspace larger than one tick's budget is served round-robin and
        nothing starves — without ever materializing or globally sorting the
        tree. No terminal kind is ever a *kind* here, so a scheduling scan never
        opens the succeeded, failed, or cancelled trees.
        """

        stream = self._streams.get(pass_name)
        if stream is None:
            stream = MarkerStream(self.workspace, kind, prefixes=self.placement_prefixes)
            self._streams[pass_name] = stream
        return [
            marker
            for marker in stream.advance(
                processing_budget=self.maximum_pass_markers,
                discovery_budget=self.discovery_budget,
                heartbeat=self.heartbeat,
                heartbeat_every=DISCOVERY_HEARTBEAT_STRIDE,
            )
            if self._owns(marker) is True
        ]

    def _walk(self, kinds: Sequence[str]) -> Iterable[Marker]:
        """Stream every marker of *kinds* this manager may schedule, exhaustively.

        A few passes must see all of their kind at once — polling running
        attempts to reap orphans, recovering abandoned claims, and the idleness
        probe — so they cannot be windowed. They still stream through the same
        scandir walker, restricted to the assigned placement subtrees and
        heartbeating from inside the walk, so a long exhaustive pass never holds
        its heartbeat and never touches a placement outside its assignment.
        """

        self._indeterminate_ownership.clear()
        for marker in self.workspace.walk_markers(kinds, roots=self.placement_prefixes, heartbeat=self.heartbeat):
            ownership = self._owns(marker)
            if ownership is None:
                self._indeterminate_ownership.add(marker.job_key)
            elif ownership is True:
                yield marker

    def _report_tick_duration(self, seconds: float) -> None:
        """Report a tick that spent a dangerous fraction of the lease."""

        if self.lease_seconds <= 0.0 or seconds < self.lease_seconds * _TICK_WARNING_FRACTION:
            return
        level = logging.ERROR if seconds >= self.lease_seconds * _TICK_ERROR_FRACTION else logging.WARNING
        _LOGGER.log(
            level,
            "one scheduling tick took %.1fs of a %.1fs lease; lower the scan cost or raise lease_seconds "
            "before a peer takes over work this manager is still running",
            seconds,
            self.lease_seconds,
            extra=self._event("tick_slow", seconds=seconds, lease_seconds=self.lease_seconds),
        )

    def tick(self) -> bool:
        """Perform one nonblocking scheduling and recovery pass.

        :return: Whether the pass changed or launched workflow state.
        """

        started = time.monotonic()
        self.heartbeat()
        changed = False
        if self.exchange:
            # First, so an adopted job registers and is claimed in this same tick.
            exchange_pass(self.workspace, now=time.time())
            self.heartbeat()
        for step in (
            self._handle_requests,
            self._register_submissions,
            # Cancellation runs before the running pass so that the attempts it
            # is terminating on purpose are never mistaken for orphans.
            self._process_cancelling,
            self._resume_committing,
            self._evaluate_joins,
            self._enforce_deadlines,
            self._poll_running,
            # Right after the running pass, so a launch is stopped in the tick
            # that observed its attempt stop.
            self._supervise_launches,
            self._recover_abandoned_claims,
        ):
            changed |= step()
            self.heartbeat()
        try:
            return self._claim_pass(changed)
        finally:
            self._collect_garbage_if_due()
            self.heartbeat()
            self._report_tick_duration(time.monotonic() - started)

    def _collect_garbage_if_due(self) -> None:
        """Run one background collection when the configured interval elapsed.

        Collection is housekeeping, so it happens after every pass of the tick
        has made its decisions and never between the observation of a marker and
        the transition based on it. It is rate limited to once per
        ``gc_interval``, it obeys the workspace's own ``policy.retention`` like
        every other collection, and a failure is reported rather than allowed to
        stop the manager: nothing scheduling depends on it.
        """

        if self.gc_interval is None:
            return
        now = time.monotonic()
        if self._last_gc and now - self._last_gc < self.gc_interval:
            return
        self._last_gc = now
        try:
            report = self.workspace.collect_garbage(journal_writer=self.writer)
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                "gc",
                f"background collection of {self.workspace.root} failed: {exc}",
                self._event("gc_error"),
                level=logging.WARNING,
            )
            return
        self._reported.pop("gc", None)
        _LOGGER.info(
            "background collection removed %d entries and about %d bytes",
            report.removed,
            report.bytes_reclaimed,
            extra=self._event(
                "gc_completed",
                removed=report.removed,
                bytes_reclaimed=report.bytes_reclaimed,
                skipped=list(report.skipped),
            ),
        )

    def _claim_pass(self, changed: bool) -> bool:
        """Claim and launch eligible work within this manager's worker budget."""

        if not self._draining and len(self._running) < self.maximum_workers and self._confinement_blocked():
            # A host or operator condition is not the jobs' fault: they stay
            # ready rather than failing one by one.
            return changed
        return _manager_scheduling.claim_pass(self, changed, _LOGGER)

    def _maintenance_paused(self) -> bool:
        """Report whether a live maintenance lock forbids launching work."""

        lock = read_maintenance_lock(self.workspace)
        if lock is None:
            self._reported.pop("maintenance", None)
            return False
        if lock.is_stale():
            self._report_anomaly(
                "maintenance",
                f"ignoring a stale maintenance lock held by {lock.describe()}; clear it with 'httk workspace unlock'",
                self._event("maintenance_lock_stale", lock=str(lock.path)),
                level=logging.WARNING,
            )
            return False
        self._report_anomaly(
            "maintenance",
            f"launching is paused by the maintenance lock held by {lock.describe()}",
            self._event("maintenance_lock_held", lock=str(lock.path)),
            level=logging.INFO,
        )
        return True

    def _executor_for(self, job: JobDefinition) -> RunnerExecutor | None:
        if job.runner_executor not in self.allowed_executors:
            return None
        return self.executors.get(job.runner_executor)

    def _transition(
        self,
        marker: Marker,
        kind: str,
        updates: StateFrame,
        *,
        priority: int | None = None,
    ) -> Marker:
        pause_member_present = updates.has("pause_requested")
        pause_requested = updates.pause_requested
        current: StateFrame | None = None
        if kind in {"ready", "waiting", "paused"} and not pause_member_present:
            try:
                current = self._read_frame(marker)
            except (WorkflowError, OSError):
                current = None
            if current is not None and not pause_member_present:
                pause_member_present = current.has("pause_requested")
                pause_requested = current.pause_requested
        if kind in {"ready", "waiting"} and pause_member_present:
            retained = {name: value for name, value in updates.members.items() if name != "pause_requested"}
            if pause_requested is not None:
                if marker.kind == "committing" and "process" not in retained:
                    raise FormatError("a deferred pause from committing must retain its process identity")
                updates = StateFrame.replace(
                    StateFrame(retained),
                    operator=pause_requested.get("operator"),
                    operator_reason=pause_requested.get("reason"),
                    request_id=pause_requested.get("request_id"),
                    reason="operator_pause_deferred",
                )
                kind = "paused"
            else:
                updates = StateFrame(retained)
        elif kind == "paused" and pause_member_present:
            retained = {name: value for name, value in updates.members.items() if name != "pause_requested"}
            updates = StateFrame(retained)
            if pause_requested is not None:
                updates = StateFrame.replace(
                    updates,
                    operator=pause_requested.get("operator"),
                    operator_reason=pause_requested.get("reason"),
                    request_id=pause_requested.get("request_id"),
                    reason="operator_pause_deferred",
                )
        elif kind in {*TERMINAL_KINDS, "cancelling"} and updates.has("pause_requested"):
            updates = StateFrame({name: value for name, value in updates.members.items() if name != "pause_requested"})
        moved = self.workspace.transition(self.writer, marker, kind, updates.as_mapping(), priority=priority)
        _LOGGER.info(
            "job %s moved from %s to %s (reason %s)",
            moved.job_key,
            marker.kind,
            kind,
            updates.reason or "-",
            extra=self._event("transition", moved, previous_kind=marker.kind, reason=updates.reason),
        )
        try:
            job = self.workspace.load_job(moved)
            executor = self._executor_for(job)
            if executor is not None:
                executor.marker_changed(self.workspace, moved)
        except (WorkflowError, OSError) as exc:
            # Executor views are recoverable derivatives. The committed marker
            # transition must remain successful even if refreshing one fails.
            _LOGGER.warning(
                "runner executor view for %s could not be refreshed: %s",
                moved.job_key,
                exc,
                extra=self._event("executor_error", moved),
            )
        return moved

    def serve(
        self,
        *,
        poll_interval: float = 1.0,
        drain_timeout: float = 30.0,
        drain_grace_seconds: float = 10.0,
    ) -> None:
        """Run until interrupted, draining running attempts on a stop signal.

        A first ``SIGTERM`` or ``SIGINT`` — what a batch system sends at
        walltime — stops claiming new work, terminates the local attempts, and
        keeps ticking so their outcomes are committed. A second signal exits at
        once. The drain is process-local: everything an interrupted attempt
        needs is already recorded by the transitions it produces, and any
        attempt left behind is recovered from its expired lease.

        :param poll_interval: Wait this long between scheduling passes.
        :param drain_timeout: Stop draining after this much time.
        :param drain_grace_seconds: Kill attempts after this much drain grace.
        """

        previous: dict[int, Any] = {}
        self._reset_drain()
        for number in _DRAIN_SIGNALS:
            try:
                previous[number] = signal.signal(number, self._request_drain)
            except ValueError:
                # Only the main thread may install handlers; an embedded
                # manager still serves, it just cannot drain on a signal.
                _LOGGER.warning("cannot install a drain handler for signal %d outside the main thread", number)
        try:
            self._serve_loop(
                poll_interval=poll_interval,
                drain_timeout=drain_timeout,
                drain_grace_seconds=drain_grace_seconds,
            )
        except KeyboardInterrupt:
            _LOGGER.info("interrupted; stopping without a drain", extra=self._event("interrupted"))
        finally:
            for installed, handler in previous.items():
                # None means a handler not installed from Python, which cannot be restored.
                if handler is not None:
                    signal.signal(installed, handler)
            self._draining = False

    def _reset_drain(self) -> None:
        """Forget any earlier drain before a manager loop starts."""

        self._draining = False
        self._drain_signals = 0
        # Why the last drain started, or None while no drain has started.
        self._drain_reason: str | None = None
        self._drain_deadline: float | None = None
        self._drain_kill_at: float | None = None

    @property
    def running_attempts(self) -> int:
        """The number of local attempts this manager still tracks."""

        return len(self._running)

    @property
    def drained(self) -> str | None:
        """Why the last manager loop drained (``"signal"`` or ``"deadline"``), or ``None``."""

        return self._drain_reason

    def _request_drain(self, number: int, frame: FrameType | None) -> None:
        """Record one drain request from a signal handler."""

        self._drain_signals += 1
        self._draining = True

    def _deadline_reached(self) -> bool:
        """Return whether this manager's drain start has passed."""

        return self.drain_start is not None and time.time() >= self.drain_start

    def _drain_begin(self, *, drain_timeout: float, drain_grace_seconds: float) -> bool:
        """Start a due drain before one tick, returning whether the loop must stop.

        Both manager loops share this state machine: the allocation's drain start
        or a stop signal starts the drain, a second signal ends it at once.
        """

        end_time = self.end_time
        if not self._draining and end_time is not None and self._deadline_reached():
            self._draining = True
            self._drain_reason = "deadline"
            _LOGGER.info(
                "drain point reached with %s left before the allocation ends; draining",
                format_duration(max(0, int(end_time - time.time()))),
                extra=self._event("drain_deadline", end_time=end_time),
            )
        # A signal during a drain the deadline started is already a second request.
        if self._drain_signals >= (1 if self._drain_reason == "deadline" else 2):
            _LOGGER.warning(
                "%s: killing %d running attempt(s) and exiting",
                "stop signal during the deadline drain" if self._drain_reason == "deadline" else "second stop signal",
                len(self._running),
                extra=self._event("drain_forced", attempts=len(self._running)),
            )
            self._signal_running_attempts(signal.SIGKILL)
            return True
        if self._draining and self._drain_deadline is None:
            self._drain_reason = self._drain_reason or "signal"
            now = time.monotonic()
            self._drain_deadline = now + drain_timeout
            self._drain_kill_at = now + drain_grace_seconds
            _LOGGER.info(
                "draining: terminating %d running attempt(s) with a %.0fs timeout",
                len(self._running),
                drain_timeout,
                extra=self._event("drain_started", attempts=len(self._running), reason=self._drain_reason),
            )
            self._signal_running_attempts(signal.SIGTERM)
        return False

    def _drain_after_tick(self) -> bool:
        """Advance a started drain after one tick, returning whether the loop must stop."""

        if not self._draining or self._drain_deadline is None:
            return False
        now = time.monotonic()
        # An outcome already committed may still sit in this manager's committing marker.
        if not self._running and not self._has_live_owned_marker(("committing",)):
            _LOGGER.info("drain complete: no local attempt remains", extra=self._event("drain_complete"))
            return True
        if now >= self._drain_deadline:
            _LOGGER.warning(
                "drain timeout expired with %d attempt(s) unreaped%s; leaving them to lease recovery",
                len(self._running),
                "" if self._running else " and a committing marker of this manager pending",
                extra=self._event("drain_timeout", attempts=len(self._running)),
            )
            self._signal_running_attempts(signal.SIGKILL)
            return True
        if self._drain_kill_at is not None and now >= self._drain_kill_at:
            _LOGGER.warning(
                "drain grace expired: killing %d running attempt(s)",
                len(self._running),
                extra=self._event("drain_kill", attempts=len(self._running)),
            )
            self._signal_running_attempts(signal.SIGKILL)
            self._drain_kill_at = None
        return False

    def _serve_loop(
        self,
        *,
        poll_interval: float,
        drain_timeout: float,
        drain_grace_seconds: float,
    ) -> None:
        while not self._drain_begin(drain_timeout=drain_timeout, drain_grace_seconds=drain_grace_seconds):
            self.tick()
            if self._drain_after_tick():
                return
            time.sleep(min(poll_interval, 0.25) if self._draining else poll_interval)

    def _signal_running_attempts(self, signal_number: int) -> int:
        """Signal every local attempt process group and report how many."""

        signalled = 0
        for attempt in list(self._running.values()):
            # Launches are stopped with their attempt, also one whose own process already exited.
            _manager_launches.signal_all(self, attempt, signal_number, "the manager is draining")
            if attempt.process.poll() is not None:
                continue
            self._terminate_process(attempt.process.pid, signal_number)
            # Only a drain signals every attempt, and an attempt it stops
            # without an outcome is lost to the manager, not failed by itself.
            attempt.interrupted = True
            signalled += 1
            _LOGGER.info(
                "sent signal %d to attempt %s process group %d",
                signal_number,
                attempt.attempt_id,
                attempt.process.pid,
                extra=self._event(
                    "attempt_signalled",
                    attempt.marker,
                    attempt_id=attempt.attempt_id,
                    signal=signal_number,
                ),
            )
        return signalled

    def run_until_idle(
        self,
        *,
        timeout: float = 60.0,
        poll_interval: float = 0.02,
        drain_timeout: float = 30.0,
        drain_grace_seconds: float = 10.0,
    ) -> WorkCensus:
        """Run until no local process or claimable marker remains, and report it.

        A job this manager cannot progress — one whose pool, capability, or
        executor it does not serve, or one waiting on children or paused for an
        operator — does not keep it awake: it is counted in the returned census
        instead. The census is what the caller prints as the idle summary.

        A ``SIGTERM`` (not ``SIGINT``) or reaching the allocation's drain start
        drains the manager as :meth:`serve` does and then returns the census; a
        ``SIGTERM`` during a deadline drain, or a second one during a signal
        drain, kills the local attempts and returns at once. When the allocation
        end time is known, running attempts keep extending *timeout*: the
        deadline drain bounds them, and the timeout still ends a manager that
        makes no progress with nothing running.

        :param timeout: Stop waiting after this many seconds.
        :param poll_interval: Wait this long between scheduling passes.
        :param drain_timeout: Stop draining after this much time.
        :param drain_grace_seconds: Kill attempts after this much drain grace.
        :return: The work census of the settled workspace.
        :raises httk.workflow.manager.NotIdleError: If the manager does not become idle
            before the timeout, extended by running attempts when the allocation end is known.
        """

        self._reset_drain()
        previous: Any = None
        try:
            previous = signal.signal(signal.SIGTERM, self._request_drain)
        except ValueError:
            _LOGGER.warning("cannot install a drain handler for signal %d outside the main thread", signal.SIGTERM)
        if self.end_time is not None:
            _LOGGER.debug("running attempts extend the %.0fs idle timeout up to the allocation's drain point", timeout)
        try:
            deadline = time.monotonic() + timeout
            quiet_passes = 0
            while True:
                if self._drain_begin(drain_timeout=drain_timeout, drain_grace_seconds=drain_grace_seconds):
                    return self._work_census()
                if self.end_time is not None and self._running:
                    # The drain point bounds running attempts; the timeout
                    # still ends a manager making no progress with none.
                    deadline = time.monotonic() + timeout
                if not self._draining and time.monotonic() >= deadline:
                    raise NotIdleError(self._work_census())
                changed = self.tick()
                if self._draining:
                    if self._drain_after_tick():
                        return self._work_census()
                    time.sleep(min(poll_interval, 0.25))
                    continue
                if changed or self._running:
                    quiet_passes = 0
                    time.sleep(poll_interval)
                    continue
                census = self._work_census()
                if census.actionable:
                    quiet_passes = 0
                else:
                    quiet_passes += 1
                    if quiet_passes >= 2:
                        return census
                time.sleep(poll_interval)
        finally:
            # None means no handler was installed here, or one not installed from Python.
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)
            self._draining = False

    def _work_census(self) -> WorkCensus:
        census = _manager_scheduling.work_census(self)
        if not census.ready_claimable or self._draining or self._confinement_blocked() is None:
            return census
        # Held back by a host or operator condition, the ready jobs are not
        # this manager's work until it clears, and are reported as such.
        return dataclasses.replace(
            census,
            ready_claimable=0,
            ready_blocked={**census.ready_blocked, "confinement": {"manager.confine": census.ready_claimable}},
            actionable_count=census.actionable_count - census.ready_claimable,
        )

    def _load_job_and_state(self, marker: Marker, pass_name: str) -> tuple[JobDefinition, StateFrame] | None:
        """Load one job and its state frame, skipping and reporting damage.

        A job whose ``job.json`` or state frame cannot be read is a local
        defect of that job. Reporting it and continuing keeps one damaged job
        from stopping every other job in the workspace. Core-v1 leaves the
        repair of such a payload to an operator, so nothing is moved: the
        authoritative marker stays exactly where it is.
        """

        try:
            job = self.workspace.load_job(marker)
        except FormatError as exc:
            refusal = self._refused_job_document(marker)
            if refusal is not None:
                # A job.json the job replaced by a symlink, FIFO or oversized
                # file never becomes readable again: fail the job instead of
                # skipping it forever.
                self._fail_unloadable_job(marker, f"{refusal}: {exc}")
                return None
            self._report_anomaly(
                f"{pass_name}:{marker.job_key}",
                f"skipping {marker.kind} job {marker.job_key} during {pass_name}: {exc}",
                self._event("job_unusable", marker, pass_name=pass_name),
            )
            return None
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"{pass_name}:{marker.job_key}",
                f"skipping {marker.kind} job {marker.job_key} during {pass_name}: {exc}",
                self._event("job_unusable", marker, pass_name=pass_name),
            )
            return None
        try:
            state = StateFrame.from_mapping(self.workspace.read_state(marker))
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"{pass_name}:{marker.job_key}",
                f"skipping {marker.kind} job {marker.job_key} during {pass_name}: {exc}",
                self._event("job_unusable", marker, pass_name=pass_name),
            )
            return None
        self._reported.pop(f"{pass_name}:{marker.job_key}", None)
        return job, state

    def _refused_job_document(self, marker: Marker) -> str | None:
        """Say why a job's ``job.json`` is refused for good, or ``None`` when it may be transient.

        Only what the bounded no-follow read refuses by kind counts: a symlink,
        a FIFO or other special file, or a document over the size bound. A
        missing file (a race with removal or transfer) or a malformed document
        keeps being reported and skipped as before.
        """

        try:
            with self._job_directory(marker) as job_dir:
                information = job_dir.stat("job.json")
        except (FormatError, OSError):
            return None
        if information is None:
            return None
        if stat.S_ISLNK(information.st_mode):
            return "job.json is a symlink"
        if not stat.S_ISREG(information.st_mode):
            return "job.json is not a regular file"
        if information.st_size > MAXIMUM_JOB_DOCUMENT_BYTES:
            return f"job.json is larger than {MAXIMUM_JOB_DOCUMENT_BYTES} bytes"
        return None

    def _fail_unloadable_job(self, marker: Marker, message: str) -> None:
        """Record ``protocol_error`` for a job whose definition is refused, without its definition.

        A ready, waiting or paused job is failed by any manager. A claimed,
        running or committing one is failed only by its own manager, or once
        its manager's lease has expired; a local attempt of it stays tracked, so
        the orphan sweep stops and reaps its process.
        """

        anomaly = f"unloadable:{marker.job_key}"
        try:
            state = self._read_frame(marker)
            if marker.kind in {"claimed", "running", "committing"} and state.manager_id != self.manager_id:
                lease_seconds = self.lease_seconds if state.lease_seconds is None else state.lease_seconds
                if self._manager_alive(state.manager_id, lease_seconds=lease_seconds):
                    self._report_anomaly(
                        anomaly,
                        f"leaving {marker.kind} job {marker.job_key} to its manager: {message}",
                        self._event("job_unusable", marker),
                    )
                    return
            if marker.kind not in {"ready", "claimed", "running", "committing", "waiting", "paused"}:
                self._report_anomaly(
                    anomaly,
                    f"cannot fail {marker.kind} job {marker.job_key}: {message}",
                    self._event("job_unusable", marker),
                )
                return
            failed = StateFrame.replace(
                state.carried(), failure=self._failure("protocol_error", message), reason="protocol_error"
            )
            if "process" in state.members:
                failed = StateFrame.replace(failed, process=state.members["process"])
            _LOGGER.error(
                "failing %s: %s", marker.job_key, message, extra=self._event("job_definition_refused", marker)
            )
            self._transition(marker, "failed", failed)
        except TransitionLostError:
            _LOGGER.debug("failure record for %s was lost to another actor", marker.job_key)
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                anomaly,
                f"cannot record the protocol_error failure of {marker.job_key}: {exc}",
                self._event("failure_error", marker, failure_code="protocol_error"),
            )

    def _read_frame(self, marker: Marker) -> StateFrame:
        """Return the typed state frame one marker references."""

        return StateFrame.from_mapping(self.workspace.read_state(marker))

    def _register_submissions(self) -> bool:
        return _manager_scheduling.register_submissions(self)

    def _eligible_ready(self) -> list[Marker]:
        """Return eligible markers for compatibility with private callers."""

        return [marker for marker, _ in self._eligible_ready_with_requirements()]

    def _eligible_ready_with_requirements(self) -> list[tuple[Marker, dict[str, int]]]:
        """Return eligible markers paired with their effective requirements."""

        return _manager_scheduling.eligible_ready(self)

    def _available_resources(self) -> dict[str, int]:
        """Return manager capacity after reservations of local attempts."""

        available = _manager_scheduling.available_resources(self.resources, self._running.values())
        if self._inventory is not None:
            available.update({name: value for name, value in self._inventory.free().items() if name in available})
        return available

    def _release_placement(self, placement: Placement | None) -> None:
        """Return one attempt's placement to the inventory."""

        if placement is not None and self._inventory is not None:
            release(self._inventory, placement)

    def _drop_running(self, attempt_id: str) -> RunningAttempt | None:
        """Stop tracking one local attempt, returning its placement to the inventory.

        Callers drop an attempt only once its launches are reaped; their trusted
        launch directories, kept until now as takeover evidence, are removed.
        """

        local = self._running.pop(attempt_id, None)
        if local is not None:
            self._release_placement(local.placement)
            _manager_launches.forget(self, local)
        return local

    def _claim_and_launch(self, marker: Marker) -> bool:
        """Claim one ready job and launch its attempt, reporting local faults."""

        ownership = self._owns(marker)
        if ownership is not True:
            if ownership is None:
                return False
            try:
                marker.path.lstat()
            except FileNotFoundError:
                # Let the verified transition report a stale candidate as a
                # lost race; a missing marker is not an ownership refusal.
                pass
            else:
                return False
        loaded = self._load_job_and_state(marker, "claim")
        if loaded is None:
            return False
        job, state = loaded
        if state.has("pause_requested"):
            self._transition(marker, "ready", StateFrame.replace(state.carried(), reason="pause_requested"))
            return True
        attempt_ordinal = (state.attempt_ordinal or 0) + 1
        total_attempts = (state.total_attempts or 0) + 1
        budget_failure = self._attempt_budget_failure(job, attempt_ordinal, total_attempts)
        if budget_failure is not None:
            self._transition(
                marker,
                "failed",
                StateFrame.replace(
                    state.carried(),
                    failure=self._failure("budget_exhausted", budget_failure),
                    reason="budget_exhausted",
                ),
            )
            return True
        attempt_id = str(uuid.uuid4())
        # The carried ``resources`` keep only the dynamic requirement an outcome
        # declared. The effective requirement, with this manager's fair share,
        # is recorded as the non-carried ``reservation``, so a retry or released
        # claim is not pinned to this manager's capacity.
        requirement = _manager_scheduling.effective_requirement(
            job, state, self.resources, self.maximum_workers, whole_nodes=self._inventory is not None
        )
        claimed = self._transition(
            marker,
            "claimed",
            StateFrame.replace(
                state.carried(),
                manager_id=self.manager_id,
                writer_id=self.writer.writer_id,
                claim_id=str(uuid.uuid4()),
                attempt_id=attempt_id,
                attempt_control=f"{ATTEMPTS_DIRECTORY}/{attempt_id}",
                attempt_ordinal=attempt_ordinal,
                total_attempts=total_attempts,
                reservation=dict(requirement),
                lease_seconds=self.lease_seconds,
                matched_pool=job.claim_pool,
                matched_capabilities=sorted(job.required_capabilities),
                reason=state.reason or "claim",
            ),
        )
        _LOGGER.info(
            "claimed %s for attempt %s (ordinal %d, total %d, pool %s)",
            claimed.job_key,
            attempt_id,
            attempt_ordinal,
            total_attempts,
            job.claim_pool,
            extra=self._event("claim", claimed, attempt_id=attempt_id, pool=job.claim_pool),
        )
        if self._maintenance_paused():
            # The lock appeared between eligibility and the claim. Releasing the
            # claim keeps the job runnable instead of wedging it for this
            # manager's lifetime.
            self._release_claim(claimed, "maintenance_lock")
            return True
        try:
            self._launch_claimed(claimed, job, state)
        except _ConfinementBlocked as exc:
            self._release_claim(claimed, "confinement_unavailable")
            self._report_confinement_blocked(str(exc))
        except TransitionLostError:
            raise
        except (WorkflowError, OSError) as exc:
            self._fail_attempt_preparation(claimed, job, exc)
        finally:
            # A launch that did not start a tracked attempt returns its placement.
            self._release_placement(self._unlaunched.pop(attempt_id, None))
        return True

    def _release_claim(self, marker: Marker, reason: str, state: StateFrame | None = None) -> None:
        """Return one claimed job to ready without consuming its budget."""

        if state is None:
            try:
                state = self._read_frame(marker)
            except (WorkflowError, OSError) as exc:
                self._report_anomaly(
                    f"release:{marker.job_key}",
                    f"cannot release claim on {marker.job_key}: {exc}",
                    self._event("release_error", marker, reason=reason),
                )
                return
        try:
            self._transition(
                marker,
                "ready",
                StateFrame.replace(
                    state.carried(),
                    reason=reason,
                    attempt_ordinal=max(0, (state.attempt_ordinal if state.attempt_ordinal is not None else 1) - 1),
                    total_attempts=max(0, (state.total_attempts if state.total_attempts is not None else 1) - 1),
                ),
            )
        except TransitionLostError:
            _LOGGER.debug("release of %s was lost to another actor", marker.job_key)

    def _fail_attempt_preparation(self, marker: Marker, job: JobDefinition, exc: Exception) -> None:
        """Fail one claimed job whose attempt could not be prepared."""

        if isinstance(exc, RunnerResolutionError):
            code = exc.code
            message = str(exc)
        else:
            code = "protocol_error" if isinstance(exc, FormatError) else "process_failure"
            message = f"cannot prepare attempt: {exc}"
        _LOGGER.error(
            "cannot prepare an attempt for %s: %s",
            marker.job_key,
            exc,
            extra=self._event("launch_error", marker, failure_code=code),
        )
        self._handle_attempt_failure(marker, job, code, message)

    def _resolve_shared_runner(self, job: JobDefinition) -> Path:
        return _manager_runners.resolve_shared_runner(self, job)

    def _resolve_package_runner(self, module: str, resource: PurePosixPath) -> Path:
        return _manager_runners.resolve_package_runner(module, resource, self.runner_modules)

    @staticmethod
    def _contained(root: Path, parts: Sequence[str]) -> Path | None:
        return _manager_runners.contained(root, parts)

    def _launch_claimed(
        self,
        marker: Marker,
        job: JobDefinition,
        previous_state: StateFrame,
    ) -> None:
        # Every directory and file descriptor the launch pins in the job
        # directory is closed when the launch returns, however it returns.
        with contextlib.ExitStack() as handles:
            self._launch_attempt(handles, marker, job, previous_state)

    def _launch_attempt(
        self,
        handles: contextlib.ExitStack,
        marker: Marker,
        job: JobDefinition,
        previous_state: StateFrame,
    ) -> None:
        """Prepare and launch one claimed attempt through the pinned job directory.

        The attempt container, the attempt-control directory, the workdir, the
        logs and the payload runner are all reached through descriptors opened
        without following links, so a job that planted a symlink, FIFO or other
        special file on one of them fails with ``protocol_error`` instead of
        redirecting a manager write or blocking the manager.
        """

        claimed_state = self._read_frame(marker)
        try:
            launch_job = self.workspace.load_job(marker)
        except (WorkflowError, OSError) as exc:
            raise RunnerResolutionError(
                "payload.tampered", f"cannot re-validate job.json for {marker.job_key}: {exc}"
            ) from exc
        if launch_job.digest != job.digest:
            raise RunnerResolutionError(
                "payload.tampered",
                f"job.json digest changed for {marker.job_key} after claim",
            )
        executor = self._executor_for(job)
        if executor is None:
            raise FormatError(f"runner executor is unavailable: {job.runner_executor}")
        attempt_id = claimed_state.attempt_id
        control_name = claimed_state.attempt_control
        if attempt_id is None or control_name is None:
            raise FormatError("a claimed frame must name its attempt and attempt control directory")
        # The manager decides by its effective settings (workspace settings with
        # its pinned overrides), and the runner sees the same mapping. A host or
        # operator confinement condition releases the claim before anything is
        # created for the attempt.
        settings = self._effective_settings()
        checked = self._confinement(settings)
        confinement = checked.settings
        confined = confinement is not None
        block_userns = checked.block_userns
        # Same manager and inputs as the claim, so this equals the claimed
        # frame's ``reservation``.
        requirement = _manager_scheduling.effective_requirement(
            job, claimed_state, self.resources, self.maximum_workers, whole_nodes=self._inventory is not None
        )
        placement = None
        if self._inventory is not None:
            placement = assign(self._inventory, requirement)
            if placement is None:
                # The claim pass checked the fit, so only a changed inventory gets here.
                self._release_claim(marker, "resources_changed", claimed_state)
                return
            self._unlaunched[attempt_id] = placement
        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        control = payload / control_name
        job_dir = handles.enter_context(self._job_directory(marker))
        # The attempt container is created when missing, but the attempt's own
        # control directory must be new: a pre-existing one was not made here.
        control_dir = handles.enter_context(job_dir.directory(control_name, create=True, exclusive=True))
        runner = payload.joinpath(*job.runner_path.parts)
        verified: _manager_runners.VerifiedRunner | None = None
        if job.workdir_mode == "persistent":
            workdir_name = job.workdir_path
            workdir_reused = job_dir.exists_dir(workdir_name)
        else:
            workdir_name = job.workdir_path.parent / f"{job.workdir_path.name}.{attempt_id}"
            workdir_reused = False
        # A symlinked workdir, or a symlinked component above it, is refused.
        job_dir.directory(workdir_name, create=True).close()
        workdir = payload.joinpath(*workdir_name.parts)
        if confined:
            self._check_confinement_start(marker)
        workflow_prelude = self.workspace.read_workflow_preludes().get(job.workflow, "")
        deadline = self._attempt_deadline(requirement)
        binding, binding_environment, pin = (
            (None, {}, None)
            if placement is None
            else self._attempt_binding(placement, control, settings, requirement.get("mem"), control_dir=control_dir)
        )
        launch_context: LaunchContext | None = None
        if confined and "HTTK_WORKFLOW_LAUNCH" in binding_environment:
            # A rendered prefix would start ranks outside the sandbox: a
            # confined attempt gets the launch client instead, and the manager
            # renders each launch from this in-memory context, never from the
            # job-writable nodefile or binding.json.
            assert placement is not None and self.allocation is not None and checked.launch is not None
            template = settings.get("manager.launch_template")
            launch_context = LaunchContext(
                placement=placement,
                template=template if isinstance(template, str) else None,
                kind=self.allocation.kind,
                gpus_present=self.resources.get("gpus", 0) > 0,
                cpus_per_proc=self.allocation.cpus_per_proc,
                mem=requirement.get("mem"),
                confinement=checked.launch,
            )
            binding_environment["HTTK_WORKFLOW_LAUNCH"] = _manager_launches.client_prefix()
        context = {
            "format": "httk-workflow-attempt-context",
            "format_version": 2,
            "workspace_id": self.workspace.workspace_id,
            "job_id": job.id,
            "job_key": job.job_key,
            "placement": marker.placement.as_posix(),
            "payload": str(payload.resolve()),
            "step": claimed_state.step,
            "activation_id": claimed_state.activation_id,
            "activation_ordinal": claimed_state.activation_ordinal,
            "attempt_id": attempt_id,
            "attempt_ordinal": claimed_state.attempt_ordinal,
            "total_attempts": claimed_state.total_attempts,
            "is_restart": (claimed_state.attempt_ordinal or 0) > 1,
            "is_unclean_restart": previous_state.unclean_restart,
            "attempt_reason": claimed_state.reason or "claim",
            "previous_attempt_id": previous_state.attempt_id,
            "activation_reason": previous_state.reason,
            "workdir_mode": job.workdir_mode,
            "workdir_reused": workdir_reused,
            "unsafe_persistent_takeover": previous_state.unsafe_persistent_takeover,
            "data_generation": claimed_state.data_generation,
            # The workspace durability mode, so every artifact the runner
            # publishes is synchronized to the same standard as the marker and
            # journal that will reference it.
            "durable": self.workspace.durable,
            # The workspace application settings, snapshotted at claim time, so a
            # runner resolves a.setting("code.command") without the operator
            # re-exporting it for every job. This is the workspace layer of the
            # parameters → environment → workspace → default resolution.
            "settings": settings,
            "resources": dict(requirement),
            **({} if deadline is None else {"deadline": deadline}),
            **({} if binding is None else {"binding": binding}),
            "join": claimed_state.join_summary,
            # The enriched, labeled observations of this activation's join, or an
            # empty array when the activation follows no join. ``join`` keeps the
            # summary exactly as earlier profiles published it.
            "children": self._context_children(claimed_state.join_summary),
        }
        context_value = json_bytes(context)
        if len(context_value) >= 100_000:
            raise FormatError(
                "attempt context exceeds the 100000-byte environment limit"
                + ("" if binding is None else f"; its binding of {len(binding['nodes'])} nodes is too large")
            )
        context_json = context_value.decode("utf-8")
        environment = os.environ.copy()
        environment.pop("HTTK_WORKFLOW_RUNNER_ARTIFACTS", None)
        environment.pop("HTTK_WORKFLOW_RUNNER_ROOT", None)
        environment.pop("HTTK_WORKFLOW_DEADLINE", None)
        if deadline is not None:
            environment["HTTK_WORKFLOW_DEADLINE"] = str(deadline)
        for variable in ("HTTK_WORKFLOW_NODELIST", "HTTK_WORKFLOW_NODEFILE", "HTTK_WORKFLOW_LAUNCH"):
            environment.pop(variable, None)
        environment.update(binding_environment)
        environment.update(
            {
                # A runner's ``#!/usr/bin/env python3`` finds this interpreter, the
                # one the job's ``requires`` were checked in at claim time.
                "PATH": interpreter_first_path(os.environ.get("PATH")),
                "HTTK_WORKFLOW_CONTEXT": context_json,
                "HTTK_WORKFLOW_CONTROL_DIR": str(control),
                "HTTK_WORKFLOW_WORKSPACE_DIR": str(self.workspace.root),
                "HTTK_WORKFLOW_JOB_DIR": str(payload),
                "HTTK_WORKFLOW_WORKDIR": str(workdir),
                "HTTK_WORKFLOW_IS_RESTART": "1" if context["is_restart"] else "0",
                "HTTK_WORKFLOW_UNCLEAN_RESTART": "1" if context["is_unclean_restart"] else "0",
                "HTTK_WORKFLOW_DURABLE": "1" if self.workspace.durable else "0",
                "HTTK_WORKFLOW_ATTEMPT_REASON": str(context["attempt_reason"]),
                "HTTK_WORKFLOW_STEP": str(context["step"]),
                "HTTK_WORKFLOW_PYTHON": sys.executable,
                "HTTK_WORKFLOW_BASH_API": str(Path(__file__).with_name("languages") / "bash" / "httk-workflow.sh"),
                "HTTK_WORKFLOW_LANGUAGES_DIR": str(Path(__file__).with_name("languages")),
                "HTTK_WORKFLOW_PERL_API": str(Path(__file__).with_name("languages") / "perl"),
            }
        )
        environment.update(code_environment())
        if job.data_mode == "transactional":
            environment["HTTK_WORKFLOW_DATA_DIR"] = str(payload / "data")
        declared_environment = job.environment.get("declared", {})
        consumed_variables: set[str] = set()
        if isinstance(declared_environment, Mapping):
            consumed_variables = {
                _setting_variable_name(setting)
                for name, metadata in declared_environment.items()
                if isinstance(metadata, Mapping)
                for setting in (metadata.get("setting", name),)
                if isinstance(setting, str)
            }
        for key in sorted(settings):
            value = settings[key]
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                continue
            variable = _setting_variable_name(key)
            if variable in consumed_variables:
                continue
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", variable) is None:
                _LOGGER.warning("setting %s has an invalid environment variable name; not exported", key)
                continue
            if variable.startswith("HTTK_WORKFLOW_"):
                _LOGGER.warning(
                    "setting %s shadows the reserved HTTK_WORKFLOW_ namespace; not exported",
                    key,
                )
                continue
            environment.setdefault(variable, str(value))
        if job.runner_source != "payload":
            verified = _manager_runners.verify_runner(self, job)
            runner = Path(f"/dev/fd/{verified.fd}") if verified.fd is not None else verified.path
            environment["HTTK_WORKFLOW_RUNNER_ROOT"] = str(verified.root)
            if verified.artifacts is not None:
                environment["HTTK_WORKFLOW_RUNNER_ARTIFACTS"] = str(verified.artifacts)
        stdio_fd = -1
        gate_read = -1
        gate_write = -1
        process: subprocess.Popen[bytes] | None = None
        running: Marker | None = None
        payload_runner_sha256: str | None = None
        sandbox: PreparedSandbox | None = None
        try:
            logs = handles.enter_context(job_dir.directory(LOGS_DIRECTORY, create=True))
            stdio_fd = logs.open_append("stdio.out")
            if job.runner_source == "payload":
                # The manager hashes the payload runner through a no-follow,
                # non-blocking descriptor; the attempt still executes it by its
                # path, so ``$0`` and ``__file__`` name the real runner.
                payload_runner_sha256 = self._hash_payload_runner(job_dir, job)
            start_marker = (
                f"=== httk attempt {attempt_id} step {context['step']} ordinal {context['attempt_ordinal']} "
                f"started {utc_now()}\n"
            )
            _write_marker(stdio_fd, start_marker, marker.job_key)
            runner_command = list(
                executor.command(
                    AttemptLaunch(
                        job=job,
                        marker=marker,
                        payload=payload,
                        workdir=workdir,
                        control=control,
                        context=context,
                        runner=runner,
                        workflow_prelude=workflow_prelude,
                        command=verified.command if verified is not None else None,
                        control_writer=lambda name, data: self._write_control_file(control_dir, name, data),
                    )
                )
            )
            if not runner_command:
                raise FormatError(f"runner executor {job.runner_executor!r} returned an empty command")
            gate_read, gate_write = os.pipe()
            runner_sha256: str | None
            if verified is not None:
                runner_sha256 = verified.sha256
            else:
                runner_sha256 = job.runner_sha256 or payload_runner_sha256
            _append_attempt_event(
                logs,
                {
                    "format": "httk-workflow-runlog-event",
                    "format_version": 2,
                    "timestamp": utc_now(),
                    "kind": "attempt",
                    "message": f"attempt {attempt_id} step {context['step']} launched",
                    "attempt_id": attempt_id,
                    "activation_id": claimed_state.activation_id,
                    "step": context["step"],
                    "runner_source": job.runner_source,
                    "runner_path": str(
                        verified.path if verified is not None else payload.joinpath(*job.runner_path.parts)
                    ),
                    "runner_sha256": runner_sha256,
                    **(
                        {"runner_command": list(verified.command)}
                        if verified is not None and verified.command is not None
                        else {}
                    ),
                    "files": [],
                },
                marker.job_key,
            )
            if confinement is not None:
                # The filtered environment is the launcher's env, which Bubblewrap
                # passes through to the sandboxed command; it never appears on
                # Bubblewrap's world-readable argv.
                environment = _confine.filtered_attempt_environment(environment)
                sandbox = self._prepare_sandbox(confinement, job_dir, workdir, environment, block_userns)
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).with_name("_launcher.py")),
                    str(gate_read),
                    "--",
                    # The gate stays outside the sandbox: the launcher execs
                    # Bubblewrap, which keeps the launcher's process group.
                    *(sandbox.argv if sandbox is not None else ()),
                    *runner_command,
                ],
                cwd=workdir,
                env=environment,
                # A confined attempt gets no terminal input to inject into.
                stdin=subprocess.DEVNULL if confined else None,
                stdout=stdio_fd,
                stderr=stdio_fd,
                start_new_session=True,
                pass_fds=(
                    gate_read,
                    *([verified.fd] if verified and verified.fd is not None else []),
                    *(sandbox.descriptors if sandbox is not None else ()),
                ),
            )
            if sandbox is not None:
                sandbox.close()
            os.close(stdio_fd)
            stdio_fd = -1
            os.close(gate_read)
            gate_read = -1
            running = self._transition(
                marker,
                "running",
                StateFrame.replace(
                    claimed_state.carried(),
                    manager_id=self.manager_id,
                    writer_id=self.writer.writer_id,
                    attempt_id=attempt_id,
                    reservation=dict(requirement),
                    lease_seconds=self.lease_seconds,
                    started_at=utc_now(),
                    workdir=str(workdir.relative_to(payload)),
                    attempt_control=control_name,
                    process={
                        "pid": process.pid,
                        "process_group": process.pid,
                        "hostname": self.hostname,
                        "launched_at": utc_now(),
                    },
                    reason="launched",
                ),
            )
            if pin is not None:
                # The launcher is blocked on the gate, so the runner it execs
                # inherits this mask.
                try:
                    os.sched_setaffinity(process.pid, pin)
                except (AttributeError, OSError) as exc:
                    _LOGGER.warning(
                        "cannot pin attempt %s to CPUs %s: %s",
                        attempt_id,
                        format_cpulist(pin),
                        exc,
                        extra=self._event("attempt_pin_failed", running, attempt_id=attempt_id),
                    )
            os.write(gate_write, b"R")
            started = time.monotonic()
            if verified is not None and verified.fd is not None:
                os.close(verified.fd)
                verified = _manager_runners.VerifiedRunner(
                    verified.path, verified.root, verified.sha256, None, verified.artifacts
                )
        except Exception as exc:
            if gate_read >= 0:
                os.close(gate_read)
                gate_read = -1
            if gate_write >= 0:
                os.close(gate_write)
                gate_write = -1
            if process is not None:
                # The gate is closed, so the launcher observes end-of-file and
                # exits, but an unreaped launcher must never be left behind.
                self._reap_launcher(process)
            reason = str(exc).replace("\n", "\\n")
            _append_log_line(
                job_dir,
                f"=== httk attempt {attempt_id} ended {utc_now()} launch-failed {reason}\n",
                job_key=marker.job_key,
            )
            if isinstance(exc, TransitionLostError):
                raise
            _LOGGER.error(
                "cannot launch an attempt for %s: %s",
                marker.job_key,
                exc,
                extra=self._event("launch_error", marker, attempt_id=attempt_id),
            )
            failure_code = "protocol_error" if isinstance(exc, FormatError) else "process_failure"
            self._handle_attempt_failure(running or marker, job, failure_code, f"cannot launch runner: {exc}")
            return
        finally:
            if stdio_fd >= 0:
                os.close(stdio_fd)
            if gate_read >= 0:
                os.close(gate_read)
            if gate_write >= 0:
                os.close(gate_write)
            if verified is not None and verified.fd is not None:
                os.close(verified.fd)
            if sandbox is not None:
                sandbox.close()
        assert process is not None
        assert running is not None
        self._running[attempt_id] = RunningAttempt(
            running,
            process,
            control,
            attempt_id,
            dict(requirement),
            started,
            requirement.get("maxtime"),
            owner_uid=self.uid,
            placement=self._unlaunched.pop(attempt_id, None),
            confined=confined,
            launches=AttemptLaunches(control_name, launch_context) if confined else None,
        )
        launch_fields: dict[str, object] = {"attempt_id": attempt_id, "pid": process.pid, "step": context["step"]}
        if confined:
            launch_fields["confined"] = True
        if job.runner_source == "payload":
            # A payload-source runner lives in the mutable payload, so record the
            # payload digest at launch: it makes post-hoc mutation of the runner
            # at least visible in the journal.
            # ponytail: full payload tree digest per payload launch; cache or cap
            # if payload launches ever dominate the manager's cost.
            payload_digest = self._launch_payload_digest(running)
            if payload_digest is not None:
                launch_fields["payload_digest"] = payload_digest
        _LOGGER.info(
            "launched attempt %s for %s as pid %d in %s",
            attempt_id,
            running.job_key,
            process.pid,
            workdir,
            extra=self._event("launch", running, **launch_fields),
        )

    def _local_share(self, placement: Placement) -> tuple[NodeShare, Node] | None:
        """Return a placement's only share and its node when that is this manager's host, else ``None``.

        The node is local when its probe marked it so or its host name matches,
        exactly or up to the first dot.
        """

        if len(placement.nodes) != 1 or self._inventory is None:
            return None
        share = placement.nodes[0]
        node = next(free.node for free in self._inventory.nodes if free.node.host == share.host)
        same = share.host == self.hostname or share.host.split(".")[0] == self.hostname.split(".")[0]
        return (share, node) if node.local or same else None

    def _attempt_binding(
        self,
        placement: Placement,
        control: Path,
        settings: Mapping[str, Any],
        mem: int | None,
        *,
        control_dir: JobDirectory | None = None,
    ) -> tuple[dict[str, Any], dict[str, str], set[int] | None]:
        """Write the attempt's nodefile and ``binding.json``; return its context ``binding``, environment and pin CPUs.

        Both files are written through *control_dir*, the pinned attempt-control
        directory, when the launch supplies it; otherwise *control* is opened as
        the anchor.
        """

        with contextlib.ExitStack() as handles:
            if control_dir is None:
                control_dir = handles.enter_context(JobDirectory.at(control))
            return self._write_attempt_binding(placement, control, control_dir, settings, mem)

    def _write_attempt_binding(
        self,
        placement: Placement,
        control: Path,
        control_dir: JobDirectory,
        settings: Mapping[str, Any],
        mem: int | None,
    ) -> tuple[dict[str, Any], dict[str, str], set[int] | None]:
        nodefile = control / "nodefile"
        control_dir.write_atomic(
            "nodefile", "".join(f"{host}\n" for host in nodefile_lines(placement)).encode("utf-8"), mode=0o666
        )
        template = settings.get("manager.launch_template")
        if template is not None and not isinstance(template, str):
            raise FormatError("workspace setting manager.launch_template must be a string")
        assert self.allocation is not None
        try:
            launch = render_launch(
                placement,
                kind=self.allocation.kind,
                template=template,
                nodefile=str(nodefile),
                gpus_present=self.resources.get("gpus", 0) > 0,
                cpus_per_proc=self.allocation.cpus_per_proc,
                mem=mem,
            )
        except ValueError as exc:
            source = "workspace setting manager.launch_template" if template is not None else "scheduler launch"
            raise FormatError(f"{source}: {exc}") from exc
        nodes = describe(placement)
        full: dict[str, Any] = {"nodes": nodes, "nodefile": str(nodefile)}
        if launch is not None:
            full["launch"] = launch
        # The per-slot cpulists and GPU ids can be large, so the context keeps
        # only the counts and points at the full binding.
        control_dir.write_atomic("binding.json", json_bytes(full) + b"\n")
        binding = {
            **full,
            "nodes": [{key: value for key, value in node.items() if key not in ("cpus", "gpu_ids")} for node in nodes],
            "file": str(control / "binding.json"),
        }
        environment = {
            "HTTK_WORKFLOW_NODELIST": ",".join(node["host"] for node in nodes),
            "HTTK_WORKFLOW_NODEFILE": str(nodefile),
        }
        if launch is not None:
            environment["HTTK_WORKFLOW_LAUNCH"] = shlex.join(launch)
        # Device identity holds only for a runner executed here; an srun step
        # chooses its own CPUs and GPUs.
        local = self._local_share(placement)
        pin: set[int] | None = None
        if local is not None:
            share, node = local
            if share.gpus and share.gpu_ids and placement.gpu_variable:
                environment[placement.gpu_variable] = ",".join(share.gpu_ids)
            elif not share.gpus and node.gpu_ids and node.gpu_variable in GPU_HIDING_VARIABLES:
                # An attempt given no GPUs must not see those given to others.
                environment[node.gpu_variable] = ""
            if share.cpu_slots and bind_cpus_setting(settings):
                pin = set[int]().union(*(parse_cpulist(slot) for slot in share.cpu_slots))
        return binding, environment, pin

    def _attempt_deadline(self, requirement: Mapping[str, int]) -> int | None:
        """Return the epoch second an attempt launched now must finish by.

        It is the earlier of its ``maxtime`` deadline and this manager's drain start.
        """

        candidates: list[int] = []
        if "maxtime" in requirement:
            candidates.append(int(time.time()) + requirement["maxtime"])
        if self.drain_start is not None:
            candidates.append(int(self.drain_start))
        return min(candidates, default=None)

    def _enforce_deadlines(self) -> bool:
        """Stop every local attempt that has run longer than its ``maxtime``.

        A timed-out attempt gets ``SIGTERM`` and, after the cancel grace, one
        ``SIGKILL``. It is neither cancelled nor fenced: its exit is reaped by the
        running pass like any other, which commits an outcome the runner still
        published and otherwise fails the attempt with ``timeout``.
        """

        if self._draining:
            # The drain is already stopping every attempt on its own clock.
            return False
        changed = False
        now = time.monotonic()
        for local in self._running.values():
            if local.maxtime is None or local.reaped or local.cancelling or local.fenced:
                continue
            if not local.timed_out:
                if now < local.started + local.maxtime:
                    continue
                if local.process.poll() is not None:
                    # A delayed manager tick must not turn an observed normal
                    # exit into a timeout and select the wrong retry policy.
                    continue
                local.timed_out = True
                local.timeout_kill_at = now + self.cancel_grace_seconds
                if local.process.poll() is None:
                    self._terminate_process(local.process.pid)
                _manager_launches.stop_all(self, local, "the attempt exceeded its maxtime")
                _LOGGER.warning(
                    "attempt %s of %s exceeded its maxtime %s; terminating it",
                    local.attempt_id,
                    local.marker.job_key,
                    format_duration(local.maxtime),
                    extra=self._event(
                        "attempt_timeout", local.marker, attempt_id=local.attempt_id, maxtime=local.maxtime
                    ),
                )
                changed = True
            elif local.timeout_kill_at is not None and now >= local.timeout_kill_at:
                local.timeout_kill_at = None
                if local.process.poll() is None:
                    self._terminate_process(local.process.pid, signal.SIGKILL)
                    _LOGGER.warning(
                        "attempt %s of %s outlived the %.1fs grace after its timeout; killing it",
                        local.attempt_id,
                        local.marker.job_key,
                        self.cancel_grace_seconds,
                        extra=self._event("attempt_timeout_kill", local.marker, attempt_id=local.attempt_id),
                    )
        return changed

    @staticmethod
    def _context_children(join_summary: object) -> list[dict[str, object]]:
        return _manager_commit.context_children(join_summary)

    def _reap_launcher(self, process: subprocess.Popen[bytes], *, grace_seconds: float = 5.0) -> None:
        """Terminate and reap a launcher whose attempt was never committed."""

        if process.poll() is None:
            self._terminate_process(process.pid)
        try:
            process.wait(timeout=grace_seconds)
            return
        except subprocess.TimeoutExpired:
            self._terminate_process(process.pid, signal.SIGKILL)
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            _LOGGER.warning("abandoned launcher pid %d could not be reaped", process.pid)

    def _poll_running(self) -> bool:
        changed = False
        current_by_attempt: dict[str, Marker] = {}
        # Job keys whose running state could not be read this pass. Their local
        # attempts must be preserved: an unreadable marker is not evidence that
        # the attempt it describes has disappeared.
        unreadable: set[str] = set()
        for marker in list(self._walk(("running",))):
            self._pace()
            loaded = self._load_job_and_state(marker, "poll_running")
            if loaded is None:
                unreadable.add(marker.job_key)
                continue
            job, state = loaded
            if self._executor_for(job) is None:
                _LOGGER.debug(
                    "skipping running job %s: runner executor %s is not served here",
                    marker.job_key,
                    job.runner_executor,
                )
                continue
            try:
                changed |= self._poll_one_running(marker, job, state, current_by_attempt)
            except FormatError as exc:
                # A frame whose attempt identity cannot be used, or an attempt
                # directory the job tampered with (JobDirectoryError, which also
                # covers a tree a digest cannot describe), is a protocol
                # violation of that job, recorded against it alone rather than
                # allowed to stop the pass.
                self._handle_attempt_failure(marker, job, "protocol_error", f"running state is unusable: {exc}")
                changed = True
        unreadable.update(self._indeterminate_ownership)
        self._sweep_untracked_attempts(current_by_attempt, unreadable)
        return changed

    def _poll_one_running(
        self,
        marker: Marker,
        job: JobDefinition,
        state: StateFrame,
        current_by_attempt: dict[str, Marker],
    ) -> bool:
        """Observe one running job's outcome, process, and lease."""

        attempt_id = state.attempt_id or ""
        current_by_attempt[attempt_id] = marker
        outcome_path = self._published_outcome(marker, state)
        if outcome_path is not None:
            return self._commit_published_outcome(marker, job, state, outcome_path)
        local = self._running.get(attempt_id)
        if local is not None:
            return_code = local.process.poll()
            if return_code is None:
                return False
            if _manager_launches.unreaped(local):
                # The attempt is not finished while a launch of it runs: its
                # exit is handled once every launch is reaped.
                _manager_launches.stop_all(self, local, "the attempt process exited")
                return False
            self._write_attempt_end(local, return_code)
            local.reaped = True
            _LOGGER.info(
                "attempt %s of %s exited with status %d",
                attempt_id,
                marker.job_key,
                return_code,
                extra=self._event("attempt_exit", marker, attempt_id=attempt_id, exit_status=return_code),
            )
            outcome_path = self._published_outcome(marker, state)
            if outcome_path is not None:
                self._reaped_attempts.add(attempt_id)
                self._commit_published_outcome(marker, job, state, outcome_path)
            elif local.timed_out and local.maxtime is not None:
                self._handle_attempt_failure(
                    marker,
                    job,
                    "timeout",
                    f"attempt exceeded its maxtime {format_duration(local.maxtime)}",
                    exit_status=return_code,
                )
            elif local.interrupted:
                self._handle_attempt_failure(
                    marker,
                    job,
                    "lease_lost",
                    f"the manager drained before the attempt finished ({self._drain_reason or 'signal'})",
                    exit_status=return_code,
                    unclean=True,
                )
            else:
                code = "protocol_error" if return_code == 0 else "process_failure"
                self._handle_attempt_failure(
                    marker,
                    job,
                    code,
                    f"runner exited with status {return_code} without an outcome",
                    exit_status=return_code,
                )
            self._finish_attempt_cleanup(local)
            self._drop_running(attempt_id)
            return True
        lease_seconds = self.lease_seconds if state.lease_seconds is None else state.lease_seconds
        if self._manager_alive(state.manager_id, lease_seconds=lease_seconds):
            return False
        evidence = self._takeover_evidence(marker, job, state, lease_seconds=lease_seconds)
        if evidence is None:
            return False
        _LOGGER.warning(
            "taking over %s: the lease of manager %s expired (%s)",
            marker.job_key,
            state.manager_id or "-",
            evidence.get("evidence"),
            extra=self._event("lease_takeover", marker, previous_manager=state.manager_id, **evidence),
        )
        self._handle_attempt_failure(
            marker,
            job,
            "lease_lost",
            "owning manager heartbeat expired",
            unclean=True,
            takeover_evidence=evidence,
        )
        return True

    def _takeover_evidence(
        self,
        marker: Marker,
        job: JobDefinition,
        state: StateFrame,
        *,
        lease_seconds: float,
    ) -> dict[str, object] | None:
        """Return why the previous attempt may be replaced, or ``None``.

        An expired lease says that a manager stopped heartbeating, which is not
        the same as an attempt having stopped. Replacing a live attempt costs a
        second allocation for one activation whichever workdir mode it uses, so
        both modes ask for the same evidence: the writing process is provably
        gone, or the lease has been silent for a configured multiple of itself.
        A persistent workdir is stricter still — a second writer would corrupt
        the shared directory rather than merely waste a node — so only proof
        that the writer is gone, or an explicitly unsafe site policy, admits it.
        """

        age = self._heartbeat_age(state.manager_id)
        grace = lease_seconds * self.takeover_grace_factor
        if self._attempt_writer_dead(state):
            return {"evidence": "writer_process_dead", "heartbeat_age_seconds": age}
        if job.workdir_mode == "persistent":
            if self.unsafe_persistent_takeover:
                return {"evidence": "unsafe_persistent_takeover", "heartbeat_age_seconds": age, "unsafe": True}
            writer_host = self._recorded_writer_host(state)
            where = f" on host {writer_host}" if writer_host and writer_host != self.hostname else ""
            self._report_anomaly(
                f"persistent_takeover:{marker.job_key}",
                f"leaving persistent-workdir job {marker.job_key} to its writer{where}: an expired lease alone "
                "cannot prove the writer stopped, and a second writer would corrupt the shared directory. "
                f"Run a manager on that host, or pass --unsafe-persistent-takeover to override.",
                self._event(
                    "persistent_takeover_deferred",
                    marker,
                    previous_manager=state.manager_id,
                    writer_host=writer_host,
                ),
                level=logging.INFO,
            )
            return None
        if self.unsafe_isolated_takeover:
            return {"evidence": "unsafe_isolated_takeover", "heartbeat_age_seconds": age, "unsafe": True}
        if age is None:
            # No manager record at all: nothing is heartbeating this attempt and
            # nothing ever will, which is exactly what the grace waits for.
            return {"evidence": "manager_record_absent", "heartbeat_age_seconds": None}
        if age >= grace:
            return {
                "evidence": "lease_grace_expired",
                "heartbeat_age_seconds": age,
                "grace_seconds": grace,
                "takeover_grace_factor": self.takeover_grace_factor,
            }
        self._report_anomaly(
            f"takeover:{marker.job_key}",
            f"not taking over {marker.job_key}: manager {state.manager_id or '-'} last heartbeated "
            f"{age:.0f}s ago, short of the {grace:.0f}s takeover grace",
            self._event("takeover_deferred", marker, previous_manager=state.manager_id, heartbeat_age_seconds=age),
            level=logging.INFO,
        )
        return None

    def _sweep_untracked_attempts(self, current_by_attempt: Mapping[str, Marker], unreadable: set[str]) -> None:
        """Reap every local attempt that no running marker names any more.

        An attempt whose outcome was committed, or whose marker was fenced by a
        cancellation, is expected to be here: its marker has moved on by design
        and the process is finishing or already gone. Only an attempt that is
        none of those is a genuine orphan, and only that case is loud.
        """

        for attempt_id, local in list(self._running.items()):
            if attempt_id in current_by_attempt or local.marker.job_key in unreadable:
                continue
            # owner_uid is stamped with self.uid at launch, so this differs only
            # when the uid test seam was reassigned after the attempt started.
            if local.owner_uid is not None and local.owner_uid != self.uid:
                _LOGGER.debug(
                    "skipping sweep of attempt %s of %s: attempt belongs to another user",
                    attempt_id,
                    local.marker.job_key,
                )
                continue
            if local.cancelling:
                # A cancellation owns this attempt until it has proven that the
                # process is gone, which is what its cancelled frame records.
                continue
            exited = local.process.poll() is not None
            if local.fenced:
                if not exited and local.sweep_kill_at is None:
                    _LOGGER.debug(
                        "terminating fenced attempt %s of %s after its outcome was committed",
                        attempt_id,
                        local.marker.job_key,
                    )
                elif exited:
                    _LOGGER.debug("reaped fenced attempt %s of %s", attempt_id, local.marker.job_key)
            elif local.sweep_kill_at is None:
                _LOGGER.warning(
                    "attempt %s of %s no longer owns a running marker; terminating it",
                    attempt_id,
                    local.marker.job_key,
                    extra=self._event("attempt_orphaned", local.marker, attempt_id=attempt_id),
                )
            if not exited:
                self._stop_untracked_attempt(local)
            _manager_launches.stop_all(
                self,
                local,
                "the attempt's outcome is committed" if local.fenced else "the attempt no longer owns a running marker",
            )
            return_code = local.process.poll()
            if return_code is None or _manager_launches.unreaped(local):
                # A signal sent successfully is not proof that the process has
                # exited. Keep tracking it until poll() supplies its returncode
                # and every launch of it is reaped; in particular, never clean a
                # control tree before that point.
                continue
            self._write_attempt_end(local, return_code)
            local.reaped = True
            self._finish_attempt_cleanup(local)
            self._drop_running(attempt_id)

    def _stop_untracked_attempt(self, local: RunningAttempt) -> None:
        """Signal an attempt no running marker names: ``SIGTERM``, then ``SIGKILL`` after the grace.

        A fenced attempt has published its outcome and may keep running, and
        writing its job directory, for as long as it likes after ``SIGTERM``;
        once ``cancel_grace_seconds`` have passed since the first ``SIGTERM``
        its process group is killed.
        """

        now = time.monotonic()
        if local.sweep_kill_at is None:
            local.sweep_kill_at = now + self.cancel_grace_seconds
            self._terminate_process(local.process.pid)
        elif now >= local.sweep_kill_at:
            _LOGGER.warning(
                "attempt %s of %s outlived the %.1fs grace after SIGTERM; killing its process group",
                local.attempt_id,
                local.marker.job_key,
                self.cancel_grace_seconds,
                extra=self._event("attempt_sweep_kill", local.marker, attempt_id=local.attempt_id),
            )
            # One kill is enough; the attempt stays tracked until it is reaped.
            local.sweep_kill_at = math.inf
            self._terminate_process(local.process.pid, signal.SIGKILL)

    def _commit_published_outcome(
        self,
        marker: Marker,
        job: JobDefinition,
        state: StateFrame,
        outcome_path: Path,
    ) -> bool:
        """Move one job with a published outcome into committing."""

        try:
            if not self._environment_log_ready(marker, state):
                return False
            local = self._running.get(state.attempt_id or "")
            if local is not None:
                with self._open_attempt_control(marker, state) as control:
                    action = control.read_json("outcome.ready/outcome.json", CONTROL_DOCUMENT_LIMIT).get("action")
                if isinstance(action, str):
                    local.outcome_action = action
            self._begin_commit(marker, state, outcome_path)
        except TransitionLostError:
            return True
        except FormatError as exc:
            # A malformed or tampered outcome, or an undescribable child bundle
            # (JobDirectoryError from a digest), is a protocol violation of the
            # runner, never a reason to stop the manager.
            self._handle_attempt_failure(marker, job, "protocol_error", f"published outcome is unusable: {exc}")
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"commit:{marker.job_key}",
                f"cannot begin the commit of {marker.job_key}: {exc}",
                self._event("commit_error", marker),
            )
        return True

    def _environment_log_ready(self, marker: Marker, state: StateFrame) -> bool:
        """Report whether a published outcome may be committed this tick.

        :param marker: The running job whose outcome is published.
        :param state: Its running state frame.
        :return: Whether the commit may begin now.
        :raises JobDirectoryError: If the environment-resolution marker is a
            symlink, special file, or oversized.
        """

        with self._open_attempt_control(marker, state) as control:
            information = control.stat(_ENVIRONMENT_MARKER)
            if information is None:
                return True
            if not stat.S_ISREG(information.st_mode):
                raise JobDirectoryError(f"{control.path / _ENVIRONMENT_MARKER} is not a regular file")
            try:
                recorded = control.read_json(_ENVIRONMENT_MARKER, CONTROL_DOCUMENT_LIMIT)
            except JobDirectoryError:
                raise
            except (FormatError, OSError):
                return True
            if recorded.get("status") != "resolved" or not recorded.get("log_pending"):
                return True
            deadline = recorded.get("log_deadline")
            deadline_expired = (
                isinstance(deadline, (int, float)) and not isinstance(deadline, bool) and time.time() >= deadline
            )
            if not self._attempt_writer_dead(state) and not deadline_expired:
                return False
            return self._reconcile_environment_log_absence(control)

    def _reconcile_environment_log_absence(self, control: JobDirectory) -> bool:
        """Clear pending logging after writer death or its persisted grace."""

        # Re-read immediately before the atomic update so a runner that won the
        # race to clear the handshake is never overwritten by stale state.
        try:
            recorded = control.read_json(_ENVIRONMENT_MARKER, CONTROL_DOCUMENT_LIMIT)
        except JobDirectoryError:
            raise
        except (FormatError, OSError):
            return True
        if recorded.get("status") != "resolved" or not recorded.get("log_pending"):
            return True
        recorded["log_pending"] = False
        recorded["log_absent"] = True
        control.write_atomic(_ENVIRONMENT_MARKER, json_bytes(recorded) + b"\n", durable=self.workspace.durable)
        return True

    def _effective_settings(self) -> dict[str, Any]:
        """Return the settings this manager decides by: the workspace's, with its pinned overrides applied.

        Workspace values are read live; pinned keys are fixed for the manager's
        lifetime. Nothing a job carries — its parameters, declared environment
        or spawned children — enters this mapping.

        :return: The effective settings.
        """

        return {**self.workspace.read_settings(), **self.setting_overrides}

    @staticmethod
    def _confine_mode(settings: Mapping[str, Any]) -> Literal["none", "bwrap"]:
        """Return the effective ``manager.confine`` mode without validating the ``confine.*`` settings.

        :param settings: The effective settings.
        :return: ``none`` (the default) or ``bwrap``.
        :raises ValueError: If ``manager.confine`` has another value.
        """

        raw = settings.get("manager.confine")
        if raw is None or raw == "none":
            return "none"
        if raw == "bwrap":
            return "bwrap"
        raise ValueError(f"setting manager.confine must be none or bwrap: {raw!r}")

    def _enrolled(self) -> bool:
        """Return whether the workspace is enrolled with a workspace daemon.

        Only daemon setup writes the enrollment marker; the exchange staging
        directory itself is also created by any ``--exchange`` manager and
        proves nothing. Anything at the marker's name counts, so a replaced
        marker fails closed.
        """

        return os.path.lexists(self.workspace.control / "exchange" / ENROLLMENT_MARKER)

    def _confinement(self, settings: Mapping[str, Any], *, at_start: bool = False) -> _Confinement:
        """Check how attempts started now are confined, validating and probing only what changed.

        ``manager.confine`` is parsed first; the ``confine.*`` settings are
        validated only in ``bwrap`` mode, and again only when they change. An
        enrolled workspace requires ``bwrap``.

        :param settings: The effective settings.
        :param at_start: Whether this is the manager's start, which refuses a
            condition instead of reporting it.
        :return: The checked confinement.
        :raises ValueError: At start, if a confinement setting is invalid.
        :raises httk.workflow.errors.ConfinementUnavailableError: At start, if
            Bubblewrap is unusable or an enrolled workspace is not confined.
        :raises _ConfinementBlocked: After start, for any of those conditions.
        """

        try:
            mode = self._confine_mode(settings)
        except ValueError as exc:
            if at_start:
                raise
            raise _ConfinementBlocked(f"invalid confinement setting: {exc}") from exc
        if mode == "none":
            if self._enrolled():
                if at_start:
                    raise ConfinementUnavailableError(f"refusing to start: {_ENROLLED_MESSAGE}")
                raise _ConfinementBlocked(_ENROLLED_MESSAGE)
            return _Confinement(None, False, None)
        key = tuple(
            sorted(
                (name, repr(value))
                for name, value in settings.items()
                if name == "manager.confine" or name.startswith(_confine.CONFINE_PREFIX)
            )
        )
        if self._confine_checked is None or self._confine_checked[0] != key:
            try:
                validated: _confine.ConfineSettings | str = _confine.confine_settings(settings)
            except ValueError as exc:
                if at_start:
                    raise
                validated = f"invalid confinement setting: {exc}"
            self._confine_checked = (key, validated)
        validated = self._confine_checked[1]
        if isinstance(validated, str):
            raise _ConfinementBlocked(validated)
        block_userns = self._bwrap_block_userns(validated, at_start=at_start)
        try:
            launch = _manager_launches.launch_confinement(validated, block_userns=block_userns)
        except ValueError as exc:
            if at_start:
                raise
            raise _ConfinementBlocked(f"invalid confinement setting for confined launches: {exc}") from exc
        return _Confinement(validated, block_userns, launch)

    def _bwrap_block_userns(self, confinement: _confine.ConfineSettings, *, at_start: bool = False) -> bool:
        """Probe the attempt sandbox once and return whether attempts block nested user namespaces.

        Results are cached per Bubblewrap executable, network isolation and
        user-namespace block options; a failed probe is repeated at most every
        :data:`CONFINE_REPROBE_SECONDS`.

        :param confinement: The effective confinement settings, in ``bwrap`` mode.
        :param at_start: Whether this is the manager's start, where an unusable
            Bubblewrap refuses the start instead of holding back claims.
        :return: Whether attempts block nested user namespaces.
        :raises httk.workflow.errors.ConfinementUnavailableError: At start, if Bubblewrap is unusable.
        :raises _ConfinementBlocked: After start, if Bubblewrap is unusable.
        """

        key = (confinement.bwrap or Path(), confinement.isolate_network, tuple(BWRAP_USERNS_BLOCK))
        cached = self._bwrap_probes.get(key)
        now = time.monotonic()
        if cached is None or (isinstance(cached[0], str) and now - cached[1] >= CONFINE_REPROBE_SECONDS):
            result: bool | str
            try:
                result = _confine.probe_bwrap(confinement)
            except ConfinementUnavailableError as exc:
                if at_start:
                    raise
                result = str(exc)
            cached = (result, now)
            self._bwrap_probes[key] = cached
            if isinstance(result, bool):
                _LOGGER.info(
                    "attempts are confined with Bubblewrap %s%s",
                    confinement.bwrap,
                    "" if result else " without blocking nested user namespaces",
                )
        if isinstance(cached[0], str):
            raise _ConfinementBlocked(f"Bubblewrap is unusable (manager.confine=bwrap): {cached[0]}")
        return cached[0]

    def _report_confinement_blocked(self, reason: str) -> None:
        """Report once that claims are held back by a confinement condition."""

        self._report_anomaly(
            "confinement",
            f"not claiming work until confinement is available: {reason}",
            self._event("confinement_unavailable", reason=reason),
        )

    def _confinement_blocked(self) -> str | None:
        """Return why attempts cannot be started with the effective confinement now, reporting it, or ``None``."""

        try:
            self._confinement(self._effective_settings())
        except _ConfinementBlocked as exc:
            reason = str(exc)
        except (WorkflowError, OSError) as exc:
            reason = f"cannot read the effective settings: {exc}"
        else:
            if self._reported.pop("confinement", None) is not None:
                _LOGGER.info(
                    "confinement is available again; claiming work", extra=self._event("confinement_available")
                )
            return None
        self._report_confinement_blocked(reason)
        return reason

    def _nested_marker(self, directory: Path) -> str | None:
        """Return a marker-shaped entry anywhere below *directory*, or ``None``; bounded and without following links.

        :param directory: A state directory mirroring a job directory.
        :return: The entry's path relative to *directory*, or a description of
            why the directory cannot be shown to hold no marker.
        """

        visited = 0
        failed: list[OSError] = []
        for root, directories, files in os.walk(directory, onerror=failed.append):
            for name in (*files, *directories):
                visited += 1
                if visited > _NESTED_SCAN_ENTRIES:
                    return f"more than {_NESTED_SCAN_ENTRIES} entries"
                if MARKER_PATTERN.fullmatch(name) is not None:
                    return os.path.relpath(os.path.join(root, name), directory)
        if any(not isinstance(error, FileNotFoundError) for error in failed):
            return f"unreadable: {failed[0]}"
        return None

    def _check_confinement_start(self, marker: Marker) -> None:
        """Refuse to confine a job whose directory is not a disjoint job directory.

        A placement component that parses as a job key, or another job placed
        below this job's directory, would put a second job inside the
        directory the sandbox makes writable. A state directory below the job
        directory counts only when it holds a marker; empty mirrors are left
        behind by garbage collection's later pruning.

        :param marker: The claimed job.
        :raises httk.workflow.errors.FormatError: If the placement violates the
            rule or the job directory contains another job.
        """

        check_job_placement(marker.placement)
        nested = marker.placement / marker.job_key
        for kind in sorted(STATE_KINDS):
            directory = self.workspace.state_directory(kind, nested)
            if not directory.is_dir():
                continue
            found = self._nested_marker(directory)
            if found is None:
                continue
            raise FormatError(
                f"cannot confine {marker.job_key}: its job directory contains another job "
                f"({kind} state below placement {nested.as_posix()}: {found}); job directories must not nest"
            )

    def _job_directory(self, marker: Marker) -> JobDirectory:
        """Open one job's directory without following any link below the workspace root."""

        return JobDirectory.open(self.workspace.root, marker.placement, marker.job_key)

    @staticmethod
    def _attempt_control_name(state: StateFrame) -> str:
        """Return the validated attempt-control path one frame names, relative to the payload.

        The component is validated before it is used, so a damaged or hostile
        frame is a protocol error of that job rather than a path that reaches
        outside its payload.
        """

        control_name = state.attempt_control
        if control_name is None:
            attempt_id = state.attempt_id
            if attempt_id is None:
                raise FormatError("state frame names neither an attempt control directory nor an attempt")
            control_name = validate_attempt_control(f"{ATTEMPTS_DIRECTORY}/{attempt_id}")
        return control_name

    def _open_attempt_control(self, marker: Marker, state: StateFrame) -> JobDirectory:
        """Open the attempt-control directory one frame names, through no-follow descriptors.

        :param marker: The job whose attempt control is opened.
        :param state: The frame naming the attempt.
        :return: The pinned attempt-control directory; the caller closes it.
        :raises httk.workflow.errors.FormatError: If the directory is missing, or
            it or a component above it is a symlink or not a directory.
        """

        control_name = self._attempt_control_name(state)
        try:
            with self._job_directory(marker) as job_dir:
                return job_dir.directory(control_name)
        except FileNotFoundError as exc:
            missing = self.workspace.payload_path(marker.placement, marker.job_key) / control_name
            raise FormatError(f"attempt directory is missing: {missing}") from exc

    def _attempt_control_path(self, marker: Marker, state: StateFrame) -> Path:
        """Return the attempt-control directory one frame names, once it is verified to be real."""

        with self._open_attempt_control(marker, state) as control:
            return control.path

    def _outcome_path(self, marker: Marker, state: StateFrame) -> Path:
        return self._attempt_control_path(marker, state) / "outcome.ready"

    def _published_outcome(self, marker: Marker, state: StateFrame) -> Path | None:
        """Return the published outcome directory of a running attempt, or ``None`` before it publishes.

        :param marker: The running job.
        :param state: Its running state frame.
        :return: The ``outcome.ready`` path, or ``None``.
        :raises JobDirectoryError: If ``outcome.ready`` is a symlink or not a
            real directory, which no runner publishes.
        """

        with self._open_attempt_control(marker, state) as control:
            return control.path / "outcome.ready" if control.exists_dir("outcome.ready") else None

    def _prepare_sandbox(
        self,
        confinement: _confine.ConfineSettings,
        job_dir: JobDirectory,
        workdir: Path,
        environment: Mapping[str, str],
        block_userns: bool,
    ) -> PreparedSandbox:
        """Build one attempt's Bubblewrap sandbox from the pinned workspace root and job directory.

        :param confinement: The effective confinement settings.
        :param job_dir: The job directory, pinned without following links.
        :param workdir: The attempt's working directory.
        :param environment: The filtered attempt environment.
        :param block_userns: Whether to block nested user namespaces.
        :return: The sandbox; the caller closes it after the spawn.
        :raises httk.workflow.errors.FormatError: If the sandbox cannot be built
            for this job, for instance when its directory is not inside the
            workspace's real path.
        """

        workspace_fd = os.open(self.workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            return _confine.prepare_attempt_sandbox(
                confinement,
                workspace_root=self.workspace.root,
                workspace_fd=workspace_fd,
                job_path=job_dir.path,
                job_fd=job_dir.fd,
                workdir=workdir,
                environment=environment,
                block_userns=block_userns,
            )
        except ValueError as exc:
            raise FormatError(f"cannot confine the attempt: {exc}") from exc
        except ConfinementUnavailableError as exc:
            raise FormatError(f"cannot confine the attempt: {exc}") from exc
        finally:
            os.close(workspace_fd)

    def _hash_payload_runner(self, job_dir: JobDirectory, job: JobDefinition) -> str | None:
        """Hash a payload runner through a no-follow, non-blocking descriptor.

        The runner is only hashed when the job does not pin its digest, but it
        is always checked: a symlink or special file planted at the runner path
        is the job's protocol error. A runner the manager cannot read (an
        execute-only file) or cannot open for another ordinary reason is still
        launched by its path, as before, just without a recorded digest.

        :param job_dir: The pinned job directory.
        :param job: The job whose payload runner is hashed.
        :return: The runner's SHA-256, or ``None`` when it is pinned or unreadable.
        :raises JobDirectoryError: If the runner or a directory above it is a
            symlink, or the runner is not a regular file.
        """

        try:
            descriptor = job_dir.open_read(job.runner_path)
        except JobDirectoryError:
            raise
        except PermissionError as exc:
            _LOGGER.debug("cannot read the runner of %s to hash it: %s", job.job_key, exc)
            return None
        except OSError as exc:
            _LOGGER.warning("cannot hash the runner for %s: %s", job.job_key, exc)
            return None
        try:
            return None if job.runner_sha256 else _manager_runners._hash_fd(descriptor)
        except OSError as exc:
            _LOGGER.warning("cannot hash the runner for %s: %s", job.job_key, exc)
            return None
        finally:
            os.close(descriptor)

    def _launch_payload_digest(self, marker: Marker) -> str | None:
        """Digest a payload at launch for the journal, or ``None`` when it cannot be described.

        The digest is informational. A payload that legitimately holds a
        contained symlink (a previous attempt may leave one in a persistent
        workdir), or that the job made undescribable, is logged and the launch
        proceeds without it.

        :param marker: The launched job.
        :return: The payload digest, or ``None``.
        """

        try:
            return self.workspace.payload_digest(marker)
        except (ValueError, OSError) as exc:
            _LOGGER.warning(
                "cannot record the launch payload digest of %s: %s",
                marker.job_key,
                exc,
                extra=self._event("payload_digest_unavailable", marker),
            )
            return None

    @staticmethod
    def _write_control_file(control_dir: JobDirectory, name: str, data: bytes) -> Path:
        """Create one new file in the pinned attempt-control directory for an executor."""

        control_dir.create_exclusive(name, data, mode=0o666)
        return control_dir.path / name

    def _begin_commit(self, marker: Marker, state: StateFrame, outcome_path: Path) -> None:
        # The published draft is read through descriptors pinned without
        # following links; *outcome_path* only names it.
        with (
            self._open_attempt_control(marker, state) as control,
            _manager_commit._open_draft(control) as draft,
        ):
            outcome = self._read_outcome(draft, marker, state)
            child_digests = self._child_digests(draft)
            # The spawn set is validated before the marker leaves running, so a
            # missing or ambiguous child label is a protocol error of the published
            # outcome rather than a commit failure of an accepted one.
            child_labels = self._spawn_labels(draft)
        attempt_id = state.attempt_id
        attempt_control = state.attempt_control
        if attempt_id is None or attempt_control is None:
            raise FormatError("a running frame must name its attempt and attempt control directory")
        committing = StateFrame.replace(
            state.carried(),
            manager_id=self.manager_id,
            writer_id=self.writer.writer_id,
            attempt_id=attempt_id,
            attempt_control=attempt_control,
            outcome_action=str(outcome["action"]),
            child_digests=child_digests,
            child_labels=child_labels,
            reason="outcome_published",
        )
        if "process" in state.members:
            committing = StateFrame.replace(committing, process=state.members["process"])
        self._transition(
            marker,
            "committing",
            committing,
        )
        # The attempt has published everything it will ever publish and its
        # marker has moved, so the local process is finishing rather than
        # orphaned. Reaping it is then routine and silent.
        local = self._running.get(attempt_id)
        if local is not None:
            local.fenced = True

    def _resume_committing(self) -> bool:
        return _manager_commit.resume(self, _LOGGER)

    def _supervise_launches(self) -> bool:
        return _manager_launches.supervise(self)

    def _process_committing(self, marker: Marker) -> None:
        if _manager_launches.holds_commit(self, marker):
            # Auto-seal and the exchange's eject follow the commit, so neither
            # happens while ranks of the attempt may still write the job.
            _LOGGER.debug("deferring the commit of %s until its launches are reaped", marker.job_key)
            return
        _manager_commit.process_committing(self, marker)

    def _declared_runner_steps(self, marker: Marker, outcome: Mapping[str, Any]) -> list[str] | None:
        return _manager_commit.declared_runner_steps(marker, outcome, _LOGGER)

    def _read_outcome(self, source: Path | JobDirectory, marker: Marker, state: StateFrame) -> dict[str, Any]:
        return _manager_commit.read_outcome(source, marker, state)

    def _child_digests(self, outcome: JobDirectory) -> dict[str, str]:
        return _manager_commit.child_digests(outcome, tree_digest)

    def _spawn_labels(self, outcome: JobDirectory) -> dict[str, str]:
        return _manager_commit.spawn_labels(outcome)

    def _labeled_join(self, join: Mapping[str, Any], outcome: JobDirectory) -> dict[str, object]:
        return _manager_commit.labeled_join(join, outcome)

    def _register_children(self, marker: Marker, state: StateFrame, outcome: JobDirectory) -> None:
        _manager_commit.register_children(self, marker, state, outcome, tree_digest)

    def _advance(
        self,
        marker: Marker,
        job: JobDefinition,
        state: StateFrame,
        next_step: str,
        progress: StateFrame,
        *,
        reason: str = "advance",
        join_summary: Sequence[object] | None = None,
        resources: Mapping[str, int] | None = None,
        priority: int | None = None,
    ) -> Marker:
        return _manager_commit.advance(
            self,
            marker,
            job,
            state,
            next_step,
            progress,
            reason=reason,
            join_summary=join_summary,
            resources=resources,
            priority=priority,
        )

    def _retry(
        self,
        marker: Marker,
        job: JobDefinition,
        state: StateFrame,
        progress: StateFrame,
        reason: str,
        *,
        unclean: bool,
        takeover_evidence: Mapping[str, object] | None = None,
        priority: int | None = None,
    ) -> Marker:
        return _manager_commit.retry(
            self,
            marker,
            job,
            state,
            progress,
            reason,
            unclean=unclean,
            takeover_evidence=takeover_evidence,
            priority=priority,
        )

    def _retry_budget_available(self, job: JobDefinition, state: StateFrame) -> bool:
        return _manager_commit.retry_budget_available(job, state)

    def _handle_attempt_failure(
        self,
        marker: Marker,
        job: JobDefinition,
        code: str,
        message: str,
        *,
        exit_status: int | None = None,
        unclean: bool = True,
        takeover_evidence: Mapping[str, object] | None = None,
    ) -> None:
        """Record one attempt failure, retrying it when the policy allows.

        Recording a failure is itself recovery, so a lost transition or an
        unreadable state frame is reported and never raised at a caller that is
        still processing other jobs.
        """

        _manager_commit.handle_attempt_failure(
            self,
            marker,
            job,
            code,
            message,
            exit_status=exit_status,
            unclean=unclean,
            takeover_evidence=takeover_evidence,
            logger=_LOGGER,
        )

    def _recover_abandoned_claims(self) -> bool:
        return _manager_scheduling.recover_abandoned_claims(self, _LOGGER)

    def _heartbeat_age(self, manager_id: str | None) -> float | None:
        """Return how long ago *manager_id* heartbeated, or ``None`` if never.

        The identifier is joined below ``managers/``, so it is validated as a
        canonical UUID before it becomes a path component; anything else is
        reported as the protocol violation it is and treated as no record.
        """

        if not manager_id:
            return None
        try:
            canonical_uuid(manager_id, "state.manager_id")
            heartbeat = read_json(self.workspace.control / "managers" / manager_id / "heartbeat.json")
            updated = timestamp_seconds(str(heartbeat["updated_at"]))
        except (WorkflowError, KeyError, ValueError):
            return None
        return time.time() - updated

    def _manager_alive(self, manager_id: str | None, *, lease_seconds: float) -> bool:
        age = self._heartbeat_age(manager_id)
        return age is not None and age <= lease_seconds

    def _recorded_writer_host(self, state: StateFrame) -> str | None:
        """Return the host that launched the recorded attempt, when it named one."""

        process = validate_process(state.members.get("process"))
        if process is None:
            return None
        host = process["hostname"]
        return host if isinstance(host, str) else None

    def _attempt_writer_dead(self, state: StateFrame) -> bool:
        """Report whether the process of the recorded attempt is provably gone.

        Only a process this host can ask about proves anything, so an attempt
        recorded on another host is never called dead here. Absence of proof is
        reported as ``False``: the caller decides what an unprovable attempt
        justifies.
        """

        process = validate_process(state.members.get("process"))
        if process is None:
            return False
        if process.get("hostname") != self.hostname:
            return False
        try:
            os.kill(cast(int, process["pid"]), 0)
        except ProcessLookupError:
            # Ranks of a confined launch write the job too, from their own
            # process groups: each recorded launch must be gone as well.
            return _manager_launches.recorded_launches_dead(self, state.attempt_id or "")
        except PermissionError:
            return False
        return False

    @staticmethod
    def _join_children(join: Mapping[str, Any]) -> Sequence[object]:
        return _manager_joins.children(join)

    def _observe_join_children(self, children: Sequence[object]) -> tuple[list[dict[str, object]], str | None]:
        return _manager_joins.observe_children(self, children)

    def _child_evidence(self, marker: Marker) -> dict[str, object]:
        return _manager_joins.child_evidence(self, marker)

    def _child_workdir_path(
        self,
        marker: Marker,
        state: StateFrame,
        payload: PurePosixPath,
    ) -> str | None:
        return _manager_joins.child_workdir_path(self, marker, state, payload)

    def _handle_unresolved_join(self, marker: Marker, state: StateFrame, child_id: str) -> bool:
        """Persist or apply the grace for a waiting job with an unresolvable child.

        The first instant a child is found unresolvable is written into the
        waiting frame, so the grace is measured from that instant and survives a
        manager restart instead of resetting to zero the way an in-memory clock
        did. When the grace has elapsed the join fails; otherwise the frame is
        left recording when the wait began.

        :param marker: The waiting job whose join child is unresolvable.
        :param state: The waiting job's current state frame.
        :param child_id: The identifier of the unresolvable child.
        :return: Whether this pass changed state.
        """

        now = time.time()
        recorded = state.join_unresolved
        # The instant is stored as an ISO timestamp like every other frame time,
        # not a raw epoch float, so 'job why' can render it readably; it is
        # parsed back to seconds only for the grace comparison.
        first_at_iso: str | None = None
        if isinstance(recorded, Mapping) and recorded.get("child_id") == child_id:
            candidate = recorded.get("first_unresolved_at")
            if isinstance(candidate, str) and candidate:
                first_at_iso = candidate
        already_recorded = first_at_iso is not None
        try:
            first_at = timestamp_seconds(first_at_iso) if first_at_iso is not None else now
        except ValueError:
            first_at, first_at_iso, already_recorded = now, None, False
        if now - first_at >= self.join_grace_seconds:
            self._fail_waiting(
                marker,
                state,
                "dependency_failure",
                f"join child {child_id} cannot be resolved in this workspace",
                "join_unresolvable",
            )
            return True
        if already_recorded:
            _LOGGER.debug("join child %s of %s is still within the grace", child_id, marker.job_key)
            return False
        # Record the first-unresolved instant exactly once, rewriting the waiting
        # frame in place so a restart reads the same deadline. The members a
        # waiting frame legitimately holds — the carried activation, its join,
        # and its next step — are preserved verbatim.
        base = state.select([*CARRIED_STATE_MEMBERS, "next_step", "join"])
        self._transition(
            marker,
            "waiting",
            StateFrame.replace(
                base,
                join_unresolved={"child_id": child_id, "first_unresolved_at": utc_now()},
                reason="join_child_unresolved",
            ),
        )
        return True

    def _fail_waiting(
        self,
        marker: Marker,
        state: StateFrame,
        code: str,
        message: str,
        reason: str,
    ) -> None:
        try:
            self._transition(
                marker,
                "failed",
                StateFrame.replace(
                    state.carried(),
                    failure=self._failure(code, message),
                    reason=reason,
                ),
            )
        except TransitionLostError:
            _LOGGER.debug("join failure record for %s was lost to another actor", marker.job_key)

    def _evaluate_joins(self) -> bool:
        changed = False
        for marker in self._window("evaluate_joins", "waiting"):
            self._pace()
            loaded = self._load_job_and_state(marker, "evaluate_joins")
            if loaded is None:
                continue
            parent_job, state = loaded
            if self._executor_for(parent_job) is None:
                _LOGGER.debug(
                    "skipping waiting job %s: runner executor %s is not served here",
                    marker.job_key,
                    parent_job.runner_executor,
                )
                continue
            try:
                changed |= self._evaluate_join(marker, parent_job, state)
            except TransitionLostError:
                pass
            except FormatError as exc:
                # A waiting job whose own join cannot be read would otherwise
                # wait forever with no diagnostic.
                self._fail_waiting(marker, state, "protocol_error", f"join is unusable: {exc}", "protocol_error")
                changed = True
            except (WorkflowError, OSError) as exc:
                self._report_anomaly(
                    f"join:{marker.job_key}",
                    f"cannot evaluate the join of {marker.job_key}: {exc}",
                    self._event("join_error", marker),
                )
        return changed

    def _evaluate_join(self, marker: Marker, parent_job: JobDefinition, state: StateFrame) -> bool:
        return _manager_joins.evaluate(self, marker, parent_job, state)

    @staticmethod
    def _join_satisfied(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> bool:
        return _manager_joins.satisfied(condition, join, kinds)

    @staticmethod
    def _join_impossible(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> bool:
        return _manager_joins.impossible(condition, join, kinds)

    def _resolve_request_marker(self, request: Mapping[str, Any]) -> Marker | None:
        return _manager_requests.resolve_marker(self, request)

    def _handle_requests(self) -> bool:
        return _manager_requests.handle(self)

    def _retire_request(self, claimed_path: Path, reason: str) -> None:
        """Retire one processed request that can never become actionable.

        A request names an exact marker generation, so one that no longer
        matches — or whose job moved while it was being applied — can never
        apply to anything later either. Removing it silently would leave an
        operator wondering; rereading it every tick would be a permanent cost.
        It is therefore moved out of the request flow with the reason recorded
        beside it, and never scanned again.
        """

        retired_dir = self.workspace.control / "requests" / "retired"
        try:
            retired_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                retired_dir / f"{claimed_path.name}.retirement",
                {
                    "format": "httk-workflow-retired-request",
                    "format_version": 2,
                    "request": claimed_path.name,
                    "manager_id": self.manager_id,
                    "reason": reason,
                    "retired_at": utc_now(),
                },
                durable=self.workspace.durable,
            )
            os.replace(claimed_path, retired_dir / claimed_path.name)
        except OSError as exc:
            self._report_anomaly(
                f"request:{claimed_path.name}",
                f"cannot retire the unactionable request {claimed_path.name}: {exc}",
                self._event("request_error", request=claimed_path.name),
            )
            return
        _LOGGER.info(
            "retired request %s: %s",
            claimed_path.name,
            reason,
            extra=self._event("request_retired", request=claimed_path.name, reason=reason),
        )

    def _request_operator_key(self, request: Mapping[str, Any]) -> str | None:
        return _manager_requests.operator_key(request, _LOGGER, self._event)

    def _apply_request(self, request: Mapping[str, Any]) -> str | None:
        """Apply one operator request, or say why it can never be applied."""

        return _manager_requests.apply(self, request)

    def _decided_join_hazard(self, marker: Marker, job: JobDefinition) -> dict[str, object] | None:
        """Return why reviving *marker* would race a join that already used it.

        A join decision is final: once the parent's transition committed, its
        observation vector is history and a later revival of a child never
        retracts it. What a revival *can* do is start writing the child's workdir
        and payload again while the parent activation that consumed it is reading
        them — a data race with no protocol answer, only an operator decision.

        The check is deliberately cheap and exact. A child's own ``job.json``
        names its parent, and the parent's current state frame carries the
        ``join_summary`` of the activation it is in, so one lookup and one frame
        read say whether this child is among the observations that activation was
        given. Nothing is inferred from history: a parent that has moved on to an
        activation with no join is not reading these outputs any more.
        """

        parent = job.parent
        if not parent:
            return None
        try:
            parent_id = canonical_uuid(parent.get("job_id"), "parent.job_id")
        except FormatError:
            return None
        parent_key = parent.get("job_key")
        parent_placement = parent.get("placement")
        if not isinstance(parent_key, str) or not isinstance(parent_placement, str):
            # A child written before spawns carried the parent's placement. The
            # guard is advisory, so it probes nothing rather than rescan the
            # whole workspace to reconstruct a hint the child should have held.
            return None
        try:
            parent_marker = self.workspace.find_marker_at(parent_key, normalize_placement(parent_placement))
            if parent_marker is None or parent_marker.job_id != parent_id:
                return None
            summary = self._read_frame(parent_marker).join_summary
        except (WorkflowError, OSError) as exc:
            # The guard is advisory: a parent whose state cannot be read is not
            # evidence that a join consumed this child.
            _LOGGER.debug("cannot check the join summary of parent %s: %s", parent_id, exc)
            return None
        if not isinstance(summary, Sequence) or isinstance(summary, (str, bytes)):
            return None
        for observation in summary:
            if not isinstance(observation, Mapping) or observation.get("job_id") != marker.job_id:
                continue
            return {
                "parent_job_id": parent_id,
                "parent_job_key": parent_marker.job_key,
                "parent_kind": parent_marker.kind,
                "parent_generation": parent_marker.generation,
                "observed_kind": str(observation.get("kind")),
                "observed_generation": observation.get("state_generation"),
            }
        return None

    def _request_cancel(
        self,
        marker: Marker,
        state: StateFrame,
        request: Mapping[str, Any],
        operator_key: str | None = None,
    ) -> str | None:
        return _manager_cancellation.request_cancel(
            self,
            marker,
            state,
            request,
            operator_key,
            _CANCELLING_MEMBERS,
            utc_now=utc_now,
            logger=_LOGGER,
        )

    def _process_cancelling(self) -> bool:
        return _manager_cancellation.process(self, _LOGGER)

    def _finish_cancellation(self, marker: Marker, state: StateFrame) -> bool:
        return _manager_cancellation.finish(self, marker, state, _CANCELLING_MEMBERS, utc_now=utc_now, logger=_LOGGER)

    def _cancellation_evidence(self, state: StateFrame) -> dict[str, object] | None:
        return _manager_cancellation.evidence(self, state, utc_now=utc_now)

    def _report_unverifiable_cancellation(self, marker: Marker, state: StateFrame) -> None:
        _manager_cancellation.report_unverifiable(
            self, marker, state, _CANCELLING_MEMBERS, utc_now=utc_now, logger=_LOGGER
        )

    def _terminate_attempt(
        self,
        marker: Marker,
        state: StateFrame,
        signal_number: int = signal.SIGTERM,
    ) -> None:
        """Signal the process group of one attempt without forgetting it.

        A locally tracked attempt stays in ``_running`` until it has been reaped
        and its exit verified: dropping it here is exactly what used to leave a
        cancelled process alive with nothing watching it.
        """

        attempt_id = state.attempt_id or ""
        local = self._running.get(attempt_id)
        if local is not None:
            local.cancelling = True
            local.fenced = True
            if local.process.poll() is None:
                self._terminate_process(local.process.pid, signal_number)
            _manager_launches.signal_all(self, local, signal_number, "the attempt is being cancelled")
            return
        # Launches a dead manager left behind are stopped with the attempt.
        _manager_launches.signal_recorded(self, state.attempt_id or "", signal_number)
        process = validate_process(state.members.get("process"))
        if process is None or process["hostname"] != self.hostname:
            return
        self._terminate_process(cast(int, process["process_group"]), signal_number)

    @staticmethod
    def _process_group_alive(process_group: int) -> bool:
        """Report whether one process group still exists on this host."""

        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            # It exists and belongs to somebody else, which is still existence.
            return True
        except OSError:
            return True
        return True

    @staticmethod
    def _terminate_process(process_group: int, signal_number: int = signal.SIGTERM) -> None:
        # killpg is load-bearing: a confined attempt is the launcher exec'ing
        # Bubblewrap, whose namespace init and command share this process
        # group. Signalling the outer pid alone (Popen.terminate/kill) would
        # leave them running, so every stop goes through the process group.
        try:
            os.killpg(process_group, signal_number)
        except ProcessLookupError:
            return
        except PermissionError as exc:
            _LOGGER.warning("cannot signal process group %d: %s", process_group, exc)

    @staticmethod
    def _failure(
        code: str,
        message: str,
        *,
        exit_status: int | None = None,
    ) -> dict[str, object]:
        """Return one canonical manager failure object."""

        return _manager_commit.failure(code, message, exit_status=exit_status)

    @staticmethod
    def _nested_reason(outcome: Mapping[str, Any], key: str) -> str:
        return _manager_commit.nested_reason(outcome, key)

    @staticmethod
    def _attempt_budget_failure(job: JobDefinition, attempt_ordinal: int, total_attempts: int) -> str | None:
        return _manager_commit.attempt_budget_failure(job, attempt_ordinal, total_attempts)
