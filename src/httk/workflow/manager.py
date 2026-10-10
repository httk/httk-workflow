"""The workflow task manager on the filesystem job kernel (plan §7.7, note §6).

A manager is one registered owner (:func:`httk.workflow._kernel.register_owner`).
Each :meth:`TaskManager.tick` checks that the owner is alive (fail-stop
otherwise), heals its own claims, recovers owners proven dead, supervises its
attempts and commits the ones that ended, and claims eligible ready jobs. Every
write to a job goes through the :class:`~httk.workflow._kernel.OwnedJob` the
claim returned; every cross-actor move goes through the kernel.
"""

import dataclasses
import json
import logging
import math
import os
import random
import shlex
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import FrameType
from typing import Any, Literal, Self

from . import (
    _attempt_process,
    _children,
    _confine,
    _data,
    _death,
    _fs,
    _joins,
    _kernel,
    _manager_launches,
    _manager_scheduling,
    _requests,
    _store,
    gc,
    removal,
    seals,
)
from ._allocation import (
    GPU_HIDING_VARIABLES,
    Allocation,
    Node,
    RecordedAllocation,
    bind_cpus_setting,
    format_cpulist,
    parse_cpulist,
)
from ._attempt_env import attempt_context, runner_environment
from ._attempt_process import start_gated, write_marker
from ._durations import format_duration
from ._exchange import ExchangeService
from ._job import JobDefinition
from ._kernel import OwnedJob, Release
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
from ._manager_commit import (
    attempt_budget_failure,
    cancel_intent,
    decide_join,
    declared_runner_steps,
    failure,
    failure_intent,
    outcome_intent,
    read_outcome,
    settle,
)
from ._manager_launches import AttemptLaunches, LaunchContext
from ._sandbox import BWRAP_USERNS_BLOCK, PreparedSandbox
from ._state import StateDoc, salvage_state
from ._util import json_bytes, utc_now
from .codes import code_environment
from .compat import runner_path
from .errors import (
    ConfinementUnavailableError,
    FormatError,
    RunnerResolutionError,
    SealError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .models import (
    EXCHANGE_EXTENSION,
    check_job_placement,
    expand_runner_command,
    normalize_placement,
    placement_text,
    validate_capacity,
)
from .packages import read_build_spec
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)
_DRAIN_SIGNALS = (signal.SIGTERM, signal.SIGINT)
#: How many ready jobs one claim pass reads before claiming; the next pass resumes after them.
DEFAULT_DISCOVERY_BUDGET = 4096
#: How long a stopped attempt has to exit after ``SIGTERM`` before its process group is killed.
DEFAULT_CANCEL_GRACE_SECONDS = 10.0
#: How long a failed Bubblewrap probe stands before the manager probes again.
CONFINE_REPROBE_SECONDS = 60.0
#: At most this many foreign owners are probed per tick (plan §7.7).
_PROBES_PER_TICK = 3
#: After this many consecutive failures to reconcile or commit one job, it is given back unchanged.
_GIVE_BACK_AFTER = 5
#: How much of the end of ``logs/runlog.jsonl`` a commit replay reads for the lines already written.
_RUNLOG_TAIL_BYTES = 1 << 16
_ENROLLED_MESSAGE = (
    "this workspace has the exchange extension enabled (WORKSPACE/exchange is written by clients), so every "
    "manager on it must confine its attempts: set manager.confine=bwrap as a workspace setting or pin it with "
    "--setting manager.confine=bwrap"
)
#: Built-in realizations run the runner file shipped beside their consumer package.
_BUILTIN_RUNNERS = {
    "cwl": ("httk.workflow.compat.cwl", "cwl_runner.py"),
    "pwd": ("httk.workflow.compat.pwd", "pwd_runner.py"),
    "jobflow": ("httk.workflow.compat.jobflow", "jobflow_runner.py"),
    "httk-v1": ("httk.workflow.compat.v1", "v1_runner.py"),
}


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


@dataclass(frozen=True)
class WorkCensus:
    """What one manager's scan found, tagged by why each job is or is not its work.

    ``ready_blocked`` groups the ready jobs this manager cannot claim by the
    requirement it lacks — ``pool``, ``capability``, ``calls`` (the workflow or
    a workflow it calls is not installed or not built here), ``requirements``
    (an unmet ``requires`` of the installed workflow, checked in this manager's
    environment), ``resources``, ``time`` (a ``mintime`` beyond the time left
    before this manager's drain start, or ``drain_point`` once it has passed),
    or ``confinement`` — mapping each requirement to the count of jobs it turns
    away. Every such job is attributed to exactly one requirement.

    :param succeeded: Terminal jobs that succeeded.
    :param failed: Terminal jobs that failed.
    :param ready_claimable: Ready jobs this manager could claim right now.
    :param ready_blocked: Requirement kind to requirement to blocked job count.
    :param waiting: Jobs waiting on their join children.
    :param paused: Jobs paused for an operator.
    :param actionable_count: Jobs this manager can still make progress on.
    """

    succeeded: int
    failed: int
    ready_claimable: int
    ready_blocked: Mapping[str, Mapping[str, int]]
    waiting: int
    paused: int
    actionable_count: int

    @property
    def actionable(self) -> bool:
        """Whether this manager still has work it can make progress on."""

        return self.actionable_count > 0

    @property
    def ready_blocked_total(self) -> int:
        """The number of jobs no requirement of this manager can claim here."""

        return sum(sum(group.values()) for group in self.ready_blocked.values())

    def _blocked_groups(self) -> list[str]:
        groups: list[str] = []
        for kind in ("pool", "capability", "requirements", "calls", "resources", "time", "confinement"):
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
        resources = sorted(self.ready_blocked.get("resources", {}))
        requirements = sorted(self.ready_blocked.get("requirements", {}))
        calls = sorted(self.ready_blocked.get("calls", {}))
        time_advice = self.time_advice()
        timed = time_advice is not None
        if not (pools or capabilities or resources or requirements or calls):
            return time_advice
        if resources and not (pools or capabilities or requirements or calls or timed):
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
        if requirements:
            lacks.append("the job requirement(s) " + "; ".join(requirements))
            remedies.append(
                "install the required distribution versions in this manager's environment and restart the manager"
            )
        if calls:
            lacks.append("the workflow(s) " + "; ".join(calls))
            remedies.append("install or build the workflows in the workspace as each problem says")
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
        advice = self.mismatch_advice()
        parts = [advice] if advice is not None else []
        if not parts:
            parts.append(
                "jobs are still running or claimable — rerun, raise --idle-timeout, or pass --idle to keep serving"
            )
        return f"{base}; {'; '.join(parts)}"


class NotIdleError(TimeoutError):
    """A manager did not become idle within its timeout.

    It carries the :class:`~httk.workflow.manager.WorkCensus` of the final scan,
    so a caller can name the actual mismatches. It subclasses :class:`TimeoutError`.

    :param census: The work census of the manager's last scan.
    """

    def __init__(self, census: WorkCensus) -> None:
        super().__init__("workflow manager did not become idle")
        self.census = census


@dataclass
class RunningAttempt:
    """Track one locally running attempt of an owned job.

    :param owned: The job's handle.
    :param job: The job definition.
    :param process: The launcher process; its pid is its process group.
    :param attempt_id: The attempt.
    :param control: The attempt directory ``attempts/<attempt-id>``.
    :param resources: The resources reserved while the attempt runs.
    :param started: The monotonic launch time the ``maxtime`` is measured from.
    :param maxtime: Stop the attempt after this many seconds, or never when ``None``.
    :param timeout_kill_at: Escalate a timed-out attempt to ``SIGKILL`` at this monotonic time.
    :param timed_out: Whether this manager stopped the attempt for exceeding its ``maxtime``.
    :param interrupted: Whether this manager signalled the attempt while draining or closing.
    :param placement: The nodes and slots the attempt was given, or ``None`` without a node inventory.
    :param launches: The confined launches of the attempt, or ``None``.
    :param cancel: The ``cancel`` request stopping the attempt, which then commits as cancelled.
    """

    owned: OwnedJob
    job: JobDefinition
    process: subprocess.Popen[bytes]
    attempt_id: str
    control: Path
    resources: Mapping[str, int]
    started: float
    maxtime: int | None
    timeout_kill_at: float | None = None
    timed_out: bool = False
    interrupted: bool = False
    placement: Placement | None = None
    launches: AttemptLaunches | None = None
    cancel: _requests.Request | None = None

    def __repr__(self) -> str:
        return f"RunningAttempt(attempt_id={self.attempt_id!r}, pid={self.process.pid})"


def _published(owned: OwnedJob, attempt_id: str) -> bool:
    """Report whether an attempt published its outcome directory.

    :param owned: The quiescent job.
    :param attempt_id: The attempt.
    :return: Whether ``attempts/<attempt-id>/outcome.ready`` is a real directory; ``False`` when absent.
    :raises httk.workflow._fs.UnsafePath: When a component is a symlink or not a directory.
    """

    try:
        os.close(_fs.open_dir_under(owned.path, f"attempts/{attempt_id}/outcome.ready"))
    except FileNotFoundError:
        return False
    return True


def _logged_events(owned: OwnedJob, attempt_id: str) -> set[str]:
    """Return the events the tail of ``logs/runlog.jsonl`` already records for *attempt_id*."""

    try:
        data, _ = _fs.read_tail(owned.path, "logs/runlog.jsonl", _RUNLOG_TAIL_BYTES)
    except (OSError, _fs.UnsafePath):
        return set()
    events: set[str] = set()
    for line in data.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict) and entry.get("attempt_id") == attempt_id and isinstance(entry.get("event"), str):
            events.add(entry["event"])
    return events


def _text(mapping: Mapping[str, object] | None, key: str) -> str | None:
    value = None if mapping is None else mapping.get(key)
    return value if isinstance(value, str) else None


def _number(mapping: Mapping[str, object] | None, key: str) -> int | None:
    value = None if mapping is None else mapping.get(key)
    return value if type(value) is int else None


class TaskManager:
    """Execute and recover jobs in one workflow workspace as one registered owner.

    :param workspace: The workspace.
    :param pools: Accept jobs assigned to these pools.
    :param capabilities: Advertise these execution capabilities.
    :param resources: Advertise these integer resource capacities.
    :param maximum_workers: Limit the number of local attempts.
    :param heartbeat_interval: Seconds between informational heartbeats.
    :param accept_any_pool: Accept jobs without requiring a configured pool match.
    :param join_grace_seconds: Wait this long for unresolved join children.
    :param cancel_grace_seconds: Wait this long after ``SIGTERM`` before killing a stopped attempt.
    :param discovery_budget: Read at most this many ready jobs per claim pass.
    :param placement_prefixes: Restrict scheduling to these placement subtrees.
    :param gc_interval: Run :func:`httk.workflow.gc.collect_garbage` (all but ``dead_owners``, which every tick
        probes) at most this often, in seconds; ``None`` never.
    :param on_attached: Called with the owner id once the owner is registered.
    :param end_time: The epoch second this manager's allocation ends, or ``None`` when unknown.
    :param deadline_margin: Stop claiming work this many seconds before *end_time*.
    :param allocation: The probed allocation this manager runs inside, or ``None``; recorded in ``owner.json``.
    :param setting_overrides: Pinned ``manager.confine``, ``manager.confine.block_mpi_spawn``,
        ``manager.launch_template``, ``manager.launch_mpi``, ``manager.bind_cpus`` and ``confine.*`` settings
        that win over the workspace settings for this manager's lifetime.
    :raises ValueError: If a manager limit, a pinned setting or the effective confinement settings are invalid.
    :raises httk.workflow.errors.ConfinementUnavailableError: If the effective ``manager.confine`` is ``bwrap``
        and Bubblewrap cannot build the attempt sandbox here, or the workspace has the exchange extension and
        attempts are not confined.
    """

    # Set by _reset_drain() and the drain signal handler; declared here because mypy meets reads first.
    _draining: bool

    def __init__(
        self,
        workspace: Workspace,
        *,
        pools: Sequence[str] = ("default",),
        capabilities: Sequence[str] = (),
        resources: Mapping[str, int] | None = None,
        maximum_workers: int = 1,
        heartbeat_interval: float = 30.0,
        accept_any_pool: bool = False,
        join_grace_seconds: float = 3600.0,
        cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
        discovery_budget: int = DEFAULT_DISCOVERY_BUDGET,
        placement_prefixes: Sequence[str] = (),
        gc_interval: float | None = None,
        on_attached: Callable[[str], None] | None = None,
        end_time: float | None = None,
        deadline_margin: float = 120.0,
        allocation: Allocation | None = None,
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
        if end_time is not None and not (math.isfinite(end_time) and end_time > 0):
            raise ValueError("end_time must be a finite positive epoch second")
        if not (math.isfinite(deadline_margin) and deadline_margin >= 0):
            raise ValueError("deadline_margin cannot be negative")
        overrides = dict(setting_overrides or {})
        for key, value in overrides.items():
            if not isinstance(key, str) or not _confine.is_override_key(key) or not isinstance(value, str):
                raise ValueError(
                    "a pinned setting must be manager.confine, manager.confine.block_mpi_spawn, "
                    "manager.launch_template, manager.launch_mpi, manager.bind_cpus or confine.* "
                    f"with a string value: {key!r}"
                )
        self.workspace = workspace
        #: Pinned settings that win over the workspace settings for this manager's lifetime.
        self.setting_overrides: dict[str, str] = overrides
        # The functional Bubblewrap probe per probed sandbox, and the confinement settings last validated.
        self._bwrap_probes: dict[tuple[Path, bool, tuple[str, ...]], tuple[bool | str, float]] = {}
        self._confine_checked: tuple[tuple[tuple[str, str], ...], _confine.ConfineSettings | str] | None = None
        self._confinement(self._effective_settings(), at_start=True)
        self.uid = os.getuid()
        self.hostname = socket.gethostname()
        self.pools = frozenset(pools)
        self.capabilities = frozenset(capabilities)
        self.resources = validated_resources
        self.maximum_workers = maximum_workers
        self.end_time = end_time
        self.allocation = allocation
        # Per-node free capacity when the allocation lists its nodes; the placed labels are then scheduled by it.
        self._inventory = Inventory.from_allocation(allocation, validated_resources)
        if self._inventory is None and allocation is not None and allocation.nodes:
            _LOGGER.warning(
                "the manager capacity exceeds the allocation's nodes; scheduling by counts only, without placement"
            )
        if self._inventory is not None:
            totals = self._inventory.empty().free()
            for label in self._inventory.labels & self.resources.keys():
                self.resources[label] = totals[label]
        #: The epoch second this manager stops claiming work that cannot fit, or ``None`` when unknown.
        self.drain_start = None if end_time is None else end_time - deadline_margin
        self.heartbeat_interval = heartbeat_interval
        self.join_grace_seconds = join_grace_seconds
        self.cancel_grace_seconds = cancel_grace_seconds
        self.discovery_budget = discovery_budget
        self.placement_prefixes: tuple[PurePosixPath, ...] = tuple(
            normalize_placement(prefix) for prefix in placement_prefixes
        )
        # The empty placement covers the whole state tree: it is no restriction at all.
        if any(not prefix.parts for prefix in self.placement_prefixes):
            self.placement_prefixes = ()
        self.accept_any_pool = accept_any_pool
        self.gc_interval = gc_interval
        self._next_gc = time.monotonic()
        self._running: dict[str, RunningAttempt] = {}
        self._reported: dict[str, str] = {}
        # The claim pass resumes its window of ready jobs after this cursor.
        self._cursor: str | None = None
        # Installed workflows and their closure problems, by workflow id, for one tick.
        self._workflows: dict[str, tuple[_store.Installed | None, str | None]] = {}
        self._scheduler = _death.SchedulerQueries()
        # When the launch directories of the confined attempts were last scanned for requests (monotonic).
        self._last_launch_scan = -math.inf
        # The joins pass: its cursor, and when each join child was first not found (by job id).
        self._join_cursor: str | None = None
        self._unresolved: dict[str, float] = {}
        # The ids of eject requests that wait for phase D, so their jobs are not claimed on every tick (every
        # other request applies at the boundary that sees it). D2: retire this once Eject is wired in removal.
        self._deferred: set[str] = set()
        # Consecutive reconcile or commit failures by job key; a job failing _GIVE_BACK_AFTER times is given back.
        self._failures: dict[str, int] = {}
        # The requests pass: its cursor over requests/ names, and the parsed requests by name with their mtime.
        self._request_cursor: str | None = None
        self._parsed: dict[str, tuple[int, _requests.Request]] = {}
        # The listings of one tick, shared by the requests and joins passes.
        self._cache = _kernel.ListingCache()
        self._recorded_allocation = None if allocation is None else RecordedAllocation.from_allocation(allocation)
        self._lost = False
        self._closed = False
        self._last_heartbeat = time.monotonic()
        self._reset_drain()
        self.owner = _kernel.register_owner(
            workspace,
            kind="manager",
            label=f"manager on {self.hostname}",
            allocation=None if self._recorded_allocation is None else self._recorded_allocation.as_json(),
            advertised={
                "pools": sorted(self.pools),
                "capabilities": sorted(self.capabilities),
                "prefixes": [placement_text(prefix) for prefix in self.placement_prefixes],
                "resources": dict(self.resources),
                # When the allocation ends and when this manager stops claiming, whatever the allocation says.
                "end_time": self.end_time,
                "drain_start": self.drain_start,
            },
        )
        #: This manager's owner id.
        self.manager_id = self.owner.owner_id
        if on_attached is not None:
            on_attached(self.manager_id)
        _LOGGER.info(
            "manager %s attached to workspace %s as %s pools=%s capabilities=%s workers=%d",
            self.manager_id,
            workspace.workspace_id,
            self.hostname,
            ",".join(sorted(self.pools)) or "-",
            ",".join(sorted(self.capabilities)) or "-",
            self.maximum_workers,
            extra=self._event("manager_started", workspace=str(workspace.root)),
        )
        if self.drain_start is not None and self.drain_start <= time.time():
            _LOGGER.warning(
                "the allocation's drain point passed %.0f s before this manager started; it will claim nothing",
                time.time() - self.drain_start,
                extra=self._event("drain_point_passed", end_time=self.end_time),
            )
        # Only an unrestricted manager serves the exchange (whether the extension is enabled, and attempts are
        # confined, is decided again on every tick): a restricted one could adopt jobs no manager runs.
        self._exchange: ExchangeService | None = None
        if not self.placement_prefixes and (self.accept_any_pool or "default" in self.pools):
            self._exchange = ExchangeService(workspace, self.owner)

    def __repr__(self) -> str:
        return f"TaskManager(workspace={self.workspace!r}, pools={tuple(sorted(self.pools))!r})"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @property
    def manager_directory(self) -> Path:
        """This manager's owner directory, ``.httk-workspace/owners/<owner-id>``."""

        return self.owner.path

    @property
    def running_attempts(self) -> int:
        """The number of local attempts this manager still tracks."""

        return len(self._running)

    @property
    def drained(self) -> str | None:
        """Why the last manager loop drained (``"signal"`` or ``"deadline"``), or ``None``."""

        return self._drain_reason

    def close(self) -> None:
        """Stop and reap the local attempts, commit what they left, and close the owner.

        After a fail-stop nothing is touched: the jobs wait for recovery. A job
        or launch that cannot be finished here keeps the owner record, so a
        later probe and recovery handle it.
        """

        if self._closed:
            return
        self._closed = True
        if self._lost:
            return
        if self._running:
            self._signal_running_attempts(signal.SIGTERM)
            self._wait_running(self.cancel_grace_seconds)
            self._signal_running_attempts(signal.SIGKILL)
            self._wait_running(5.0)
            self._supervise()
        try:
            self.owner.close()
        except WorkflowError as exc:
            _LOGGER.warning("owner %s keeps its record for recovery: %s", self.manager_id, exc)

    def _wait_running(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        for local in self._running.values():
            try:
                local.process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                continue

    def heartbeat(self, *, force: bool = False) -> None:
        """Write the informational ``heartbeat.json`` when the interval has elapsed.

        :param force: Write immediately instead of honoring the interval.
        """

        now = time.monotonic()
        if force or now - self._last_heartbeat >= self.heartbeat_interval:
            self.owner.heartbeat()
            self._last_heartbeat = now

    def _event(self, event: str, owned: OwnedJob | None = None, **fields: object) -> dict[str, object]:
        """Return structured logging fields describing one manager event."""

        data: dict[str, object] = {"event": event, "manager_id": self.manager_id}
        if owned is not None:
            data.update({"job_key": owned.job_key, "job_id": owned.job_id})
        data.update(fields)
        return data

    def _report_anomaly(self, key: str, text: str, fields: Mapping[str, object], *, level: int = logging.ERROR) -> None:
        """Report a possibly repeating anomaly loudly once, then quietly."""

        if self._reported.get(key) == text:
            _LOGGER.debug("%s (unchanged)", text, extra=dict(fields))
            return
        self._reported[key] = text
        _LOGGER.log(level, "%s", text, extra=dict(fields))

    def _owns(self, path: Path) -> bool:
        """Check that this manager's uid owns a job directory and its ``job.json`` (provenance, not authentication)."""

        try:
            return all(os.lstat(item).st_uid == self.uid for item in (path, path / "job.json"))
        except OSError:
            return False

    # -- the tick ---------------------------------------------------------------------------------------------------

    def tick(self) -> bool:
        """Perform one nonblocking pass: fail-stop check, self-healing, recovery, supervision, claims.

        :return: Whether the pass changed workflow state.
        :raises httk.workflow.errors.WorkflowError: When this owner was declared dead or recovered (fail-stop:
            every attempt it started has been killed and no job was touched), or the manager is closed.
        """

        if self._lost or self._closed:
            raise WorkflowError(f"manager {self.manager_id} is closed")
        self._workflows.clear()
        try:
            self.owner.check_alive()
            changed = self._self_heal()
            changed |= self._recover_dead_owners()
            serving = not self._draining and self._confinement_blocked() is None
            if self._exchange is not None and serving and EXCHANGE_EXTENSION in self.workspace.extensions:
                # First, so an adopted job is claimed and a translated request applied in this same tick.
                changed |= self._exchange.run()
            self._cache = _kernel.ListingCache()
            changed |= self._requests_pass()
            changed |= self._joins_pass()
            changed |= self._supervise()
            if serving:
                changed |= _manager_scheduling.claim_pass(self)
            if changed and self._exchange is not None:
                # A job may just have finished: the next pass looks for finished exchange trees at once.
                self._exchange.invalidate()
            self._collect_garbage()
            self.heartbeat()
        except _kernel.OwnerLost:
            self._fail_stop()
            raise
        return changed

    def _collect_garbage(self) -> None:
        """Run the background collection when ``gc_interval`` has passed since the last one."""

        if self.gc_interval is None or time.monotonic() < self._next_gc:
            return
        self._next_gc = time.monotonic() + self.gc_interval
        selected = tuple(name for name in gc.GC_CATEGORIES if name != "dead_owners")
        try:
            report = gc.collect_garbage(self.workspace, owner=self.owner, categories=selected, sizes=False)
        except (WorkflowError, OSError, _fs.UnsafePath) as exc:
            if isinstance(exc, _kernel.OwnerLost):
                raise
            _LOGGER.warning("background collection failed: %s", exc, extra=self._event("gc_failed"))
            return
        if report.removed:
            _LOGGER.info("background collection removed %d entries", report.removed, extra=self._event("gc"))

    def _fail_stop(self) -> None:
        """Kill every attempt this owner started and stop serving, touching no job (plan §5.2)."""

        self._lost = True
        _LOGGER.critical(
            "owner %s was declared dead or recovered: killing %d attempt(s) and stopping",
            self.manager_id,
            len(self._running),
            extra=self._event("fail_stop"),
        )
        for local in self._running.values():
            _manager_launches.signal_all(self, local, signal.SIGKILL, "the owner was declared dead")
            _attempt_process.terminate_process(local.process.pid, signal.SIGKILL)
        self._wait_running(5.0)

    def _self_heal(self) -> bool:
        """Reconcile every job in ``owned/<self>/`` this process does not hold, then return it (plan §5.4)."""

        running = {local.owned.path for local in self._running.values()}
        changed = False
        for ref in self.owner.owned():
            if ref.path in running:
                continue
            # A handle this process still holds is a commit it retries, not a claim it lost track of.
            held = self.owner.holds(ref)
            try:
                owned = self.owner.adopt_owned(ref)
            except (FormatError, _fs.UnsafePath, OSError) as exc:
                # A job won with a damaged job.json (its claim raised): no handle can release it, so quarantine.
                gc.quarantine_damaged(self.workspace, self.owner, ref.job_id, f"job.json is damaged: {exc}")
                changed = True
                continue
            if not held:
                owned.append_log("recovered", detail="self-healed")
            reconciled = self._reconcile_guarded(owned)
            if reconciled is not None:
                self._release(owned, reconciled[1], Release(owned.from_state, owned.from_priority))
            changed = True
        return changed

    def _recover_dead_owners(self) -> bool:
        """Probe a few foreign owners, chosen at random, and recover the ones proven dead."""

        candidates = [
            owner_id for owner_id in _kernel.recoverable_owners(self.workspace) if owner_id != self.manager_id
        ]
        changed = False
        for owner_id in random.sample(candidates, min(_PROBES_PER_TICK, len(candidates))):
            try:
                if _kernel.probe_owner(self.workspace, owner_id, scheduler=self._scheduler) is not _death.Liveness.DEAD:
                    continue
                report = _kernel.recover(self.workspace, self.owner, owner_id)
            except ValueError:
                # An owner of this very process (another manager or CLI owner in it) is never probed.
                continue
            except WorkflowError as exc:
                # A concurrent recoverer finished first, or entries keep appearing: the next probe retries.
                _LOGGER.debug("recovery of owner %s did not finish: %s", owner_id, exc)
                continue
            _LOGGER.warning(
                "recovered dead owner %s: %d job(s) returned, %d quarantined",
                owner_id,
                len(report.returned),
                len(report.quarantined),
                extra=self._event("owner_recovered", dead_owner=owner_id),
            )
            changed = True
        return changed

    # -- reconcile, commit, release -----------------------------------------------------------------------------------

    def _write(self, owned: OwnedJob, doc: StateDoc) -> StateDoc:
        doc = doc.updated(owner_id=self.manager_id)
        owned.write_state(doc)
        return doc

    def _reconcile_guarded(self, owned: OwnedJob) -> tuple[JobDefinition, StateDoc] | None:
        """Run :meth:`_reconcile`, reporting a job it cannot finish instead of stopping the tick."""

        try:
            reconciled = self._reconcile(owned)
        except _kernel.OwnerLost:
            raise
        except Exception as exc:
            self._job_failed(owned, "reconcile", exc)
            return None
        self._failures.pop(owned.job_key, None)
        return reconciled

    def _job_failed(self, owned: OwnedJob, what: str, exc: Exception) -> None:
        """Report a reconcile or commit that raised; give the job back unchanged once it kept failing.

        The job stays owned for the next pass until it failed :data:`_GIVE_BACK_AFTER` times in a row. Given
        back with its intent, it is retried by whichever manager claims it next; this one gives it back again
        at its next failure and reports that only quietly.
        """

        count = self._failures[owned.job_key] = self._failures.get(owned.job_key, 0) + 1
        text = f"cannot {what} {owned.job_key}; it stays owned for the next pass: {exc}"
        if count >= _GIVE_BACK_AFTER:
            try:
                owned.give_back()
                text = f"cannot {what} {owned.job_key} after {_GIVE_BACK_AFTER} attempts; it was given back: {exc}"
            except _kernel.OwnerLost:
                raise
            except (WorkflowError, OSError) as again:
                text = f"cannot {what} {owned.job_key}, nor give it back ({again}): {exc}"
        self._report_anomaly(f"{what}:{owned.job_key}", text, self._event(f"{what}_error", owned))

    def _reconcile(self, owned: OwnedJob) -> tuple[JobDefinition, StateDoc] | None:
        """Finish whatever the job's last owner left unfinished (plan §7.7, note §6.2).

        In order: leftover write temporaries go; a pending release is finished
        and nothing else runs; a ``commit`` intent is resumed from its
        transactions; a ``job.json`` that differs from the digest pinned at the
        first activation fails the job (``protocol_error``); an attempt left
        ``launching`` never ran and is rerun without counting, while one left
        ``running`` is dead: its valid published outcome is committed,
        otherwise it is an unclean restart (``owner_lost``) under the retry
        policy; uncommitted transaction staging is discarded (every committed
        transaction was applied by its own commit); finally the pending
        requests are applied, the first that takes effect releasing the job.

        :param owned: A quiescent owned job.
        :return: The job and its state when the job may launch now, or ``None`` once it was released.
        :raises httk.workflow.errors.WorkflowError: When the job was taken from this owner.
        """

        try:
            owned.remove_leftover_temporaries()
            job = JobDefinition.from_path(owned.path / "job.json")
            doc = owned.read_state()
        except _kernel.OwnerLost:
            raise
        except (WorkflowError, OSError) as exc:
            # FormatError, UnsafePath for a symlinked or special state.json and an unreadable file (the job made
            # it so): job damage.
            self._fail_damaged(owned, str(exc))
            return None
        if (pending := owned.pending_release()) is not None and doc is not None:
            owned.append_log("released", **{"from": owned.from_state, "to": pending.state}, detail="pending")
            owned.release(doc, pending)
            return None
        if doc is None or doc.activation is None:
            # A new job (a published child carries a state.json without an activation) starts its initial step.
            doc = (doc or StateDoc.empty(owned.job_id)).next_activation(job.initial_step, "initial")
        if doc.job_digest is None:
            # Pinned at the first sight of the job (its first activation); the next write records it.
            doc = doc.updated(job_digest=job.digest)
        kind, attempt_id = doc.phase["kind"], _text(doc.phase, "attempt_id")
        if (doc.commit is not None or kind != "idle") and doc.owner_id != self.manager_id:
            owned.append_log("recovered", attempt_id=attempt_id, detail=doc.owner_id)
        if doc.commit is not None:
            self._finish_commit(owned, job, doc, replay=True)
            return None
        if doc.job_digest != job.digest:
            # The job's processes rewrote job.json (note §3.3: it is not theirs to change).
            message = f"job.json changed since the job's first activation (digest {doc.job_digest}, now {job.digest})"
            failed = doc.with_phase("idle", None).with_failure(failure("protocol_error", message))
            # Its pending requests still apply, from failed: left pending, each would fail the job on every pass.
            boundary = removal.apply_requests(
                owned, job, failed, from_state="failed", cache=self._cache, deferred=self._deferred
            )
            if boundary is not None:
                self._release(owned, boundary, Release("failed", owned.from_priority))
            return None
        if kind == "launching" and doc.attempt is not None and attempt_id is not None:
            # The gate opens only after phase running is written: this attempt never ran. Its directory goes, so
            # the attempt is launched again under its own id.
            self._remove_attempt(owned, attempt_id)
            doc = self._write(owned, doc.with_phase("idle", None).updated(attempt={**doc.attempt, "started_at": None}))
        elif kind == "running" and attempt_id is not None:
            outcome_dir = owned.path / "attempts" / attempt_id / "outcome.ready"
            try:
                outcome = read_outcome(outcome_dir, doc) if _published(owned, attempt_id) else None
            except (FormatError, _fs.UnsafePath, OSError):
                outcome = None
            if outcome is not None:
                self._commit_outcome(owned, job, doc, outcome, outcome_dir)
                return None
            if (cancel := self._pending_cancel(owned)) is not None:
                # The dead owner was stopping the attempt for this request (or never saw it): it is cancelled.
                self._commit(owned, job, doc, cancel_intent(attempt_id, owned.from_priority, cancel))
                return None
            if doc.owner_id == self.manager_id:
                # This process ran the attempt and saw it end, but failed before recording the commit.
                code, message, unclean = "process_failure", "the attempt ended without a committed outcome", False
            else:
                code, message, unclean = "owner_lost", "the attempt's owner died before it published an outcome", True
            intent = failure_intent(job, doc, attempt_id, code, message, priority=owned.from_priority, unclean=unclean)
            self._commit(owned, job, doc, intent)
            return None
        _data.discard_uncommitted(owned)
        applied = removal.apply_requests(owned, job, doc, cache=self._cache, deferred=self._deferred)
        return None if applied is None else (job, applied)

    def _fail_damaged(self, owned: OwnedJob, message: str) -> None:
        """Fail a job whose ``job.json`` or ``state.json`` its own processes made unreadable."""

        doc = StateDoc.empty(owned.job_id).with_failure(failure("protocol_error", message))
        salvaged = salvage_state(owned.path / "state.json")
        # An exchange job stays one (its client still asks for it by its name), and job.json stays pinned.
        doc = doc.updated(
            **{key: salvaged[key] for key in ("origin", "exchange_name", "job_digest") if key in salvaged}
        )
        dropped: list[dict[str, str]] = []
        if "applied_requests" in salvaged:
            # Applied requests must stay skipped: their files may still exist (plan §3.3).
            doc = doc.updated(applied_requests=salvaged["applied_requests"])
        else:
            # Without the applied ids, no pending request can be told from an applied one: each one goes, recorded.
            directory = self.workspace.control / "requests"
            for name in _kernel.ListingCache().names(directory):
                if name.startswith(f"{owned.job_id}."):
                    request = self._parse_request(directory / name)
                    entry = {"request_id": name.split(".")[1], "action": "?" if request is None else request.action}
                    _fs.remove_file(_fs.loc(directory / name), durable=self.workspace.durable)
                    dropped.append(entry)
                    doc = doc.with_history("request_dropped", **entry, note="the job's state.json was damaged")
        _LOGGER.error(
            "failing %s: %s%s",
            owned.job_key,
            message,
            "".join(f"; dropped {item['action']} request {item['request_id']}" for item in dropped),
            extra=self._event("job_damaged", owned, dropped_requests=dropped),
        )
        try:
            self._release(owned, doc, Release("failed", owned.from_priority))
        except _kernel.OwnerLost:
            raise
        except WorkflowError as exc:
            # Without a readable placement the kernel cannot release it; fsck and an operator must.
            self._report_anomaly(
                f"damaged:{owned.job_key}",
                f"cannot release damaged job {owned.job_key}: {exc}",
                self._event("job_damaged", owned),
            )

    def _commit_outcome(
        self, owned: OwnedJob, job: JobDefinition, doc: StateDoc, outcome: Mapping[str, Any], outcome_dir: Path
    ) -> None:
        """Validate a published outcome and its children (commit step 1), then decide and execute the commit."""

        attempt_id = str(outcome["attempt_id"])
        owned.append_log("outcome", attempt_id=attempt_id, detail=outcome["action"])
        workspace_id = self.workspace.workspace_id
        try:
            plans = _children.validate_children(owned, job, doc, outcome_dir, workspace_id=workspace_id)
            intent = outcome_intent(
                job,
                doc,
                outcome,
                priority=owned.from_priority,
                plans=plans,
                workspace_id=workspace_id,
                seal=self._seal_enabled(job),
            )
        except (FormatError, UnsupportedExtensionError) as exc:
            intent = failure_intent(
                job,
                doc,
                attempt_id,
                "protocol_error",
                f"published outcome is unusable: {exc}",
                priority=owned.from_priority,
            )
        steps = declared_runner_steps(owned.job_key, outcome)
        if steps is not None:
            doc = doc.updated(runner_steps=steps)
        self._commit(owned, job, doc, intent)

    def _commit(self, owned: OwnedJob, job: JobDefinition, doc: StateDoc, intent: Mapping[str, object]) -> None:
        """Write the commit intent (the decision point) and execute it."""

        self._finish_commit(owned, job, self._write(owned, doc.with_commit(intent)))

    def _finish_commit(self, owned: OwnedJob, job: JobDefinition, doc: StateDoc, *, replay: bool = False) -> None:
        """Execute the ``commit`` intent of *doc* from its transactions on; a replay resumes here (plan §7.1).

        A *replay* writes the run-log lines of the commit only where the tail of the log lacks them.
        """

        assert doc.commit is not None
        intent: dict[str, Any] = dict(doc.commit)
        attempt_id = str(intent["attempt_id"])
        logged = _logged_events(owned, attempt_id) if replay else set[str]()
        if not intent.get("transactions_failed"):
            try:
                _data.discard_uncommitted(owned)
                _data.apply_transactions(owned)
            except (_data.DataConflict, FormatError) as exc:
                # The intent is rewritten to the failure, so a replay reaches the same decision without retrying.
                code = "data_conflict" if isinstance(exc, _data.DataConflict) else "protocol_error"
                intent.update(
                    target_state="failed",
                    failure=failure(code, f"cannot apply the committed transactions: {exc}"),
                    reason=code,
                    transactions_failed=True,
                    children=[],
                    seal=False,
                )
                doc = self._write(owned, doc.with_commit(intent))
        if intent.get("transactions_failed"):
            # The unapplied staging must not reach a later commit (which would fail the job again): it goes, and
            # the failure the intent records is the evidence.
            _data.discard_transactions(owned)
        # Step 4: children are published from the intent alone; a staged child that is gone was published.
        plans = [_children.ChildPlan.from_mapping(entry) for entry in intent.get("children") or ()]
        _children.publish_children(owned, plans)
        members = ("job_id", "job_key", "label", "placement", "spawn_id")
        entries = [{**{name: getattr(plan, name) for name in members}, "attempt_id": attempt_id} for plan in plans]
        # Step 5: the seal. What the job planted (a FIFO, a symlinked .httk-job) fails it for good, recorded in
        # the intent so a replay decides the same; an I/O error leaves the intent, and the commit is retried.
        seal: dict[str, object] | None = None
        if intent.get("seal"):
            try:
                sha256, signed = seals.seal_payload(
                    owned.path,
                    job_id=owned.job_id,
                    job_key=owned.job_key,
                    keys=self._seal_keys(),
                    durable=self.workspace.durable,
                )
            except (ValueError, FormatError, _fs.UnsafePath) as exc:
                intent.update(
                    target_state="failed",
                    failure=failure("protocol_error", f"cannot seal the job: {exc}"),
                    reason="protocol_error",
                    seal=False,
                    seal_failed=True,
                )
                doc = self._write(owned, doc.with_commit(intent))
            else:
                if "sealed" not in logged:
                    owned.append_log("sealed", attempt_id=attempt_id, detail={"sha256": sha256, "signed": signed})
                seal = {"sha256": sha256, "signed": signed}
        target = str(intent["target_state"])
        final = settle(doc, intent)
        if entries:
            final = final.with_children([*final.children, *entries])
        if seal is not None:
            final = final.updated(seal=seal)
        elif target == "succeeded":
            final = final.updated(seal={"disabled": True})
        if target not in ("failed", "cancelled"):
            self._remove_attempt(owned, attempt_id)
        if "committed" not in logged:
            owned.append_log("committed", attempt_id=attempt_id, to=target, detail=intent["action"])
        priority = intent.get("priority")
        request_id, audit = intent.get("request_id"), intent.get("request")
        if isinstance(request_id, str):
            if "request_applied" not in logged:
                owned.append_log(
                    "request_applied", attempt_id=attempt_id, detail={"request_id": request_id, "action": "cancel"}
                )
            if isinstance(audit, Mapping):
                # As _requests.apply records a request it applies: who asked, why, and the move.
                final = final.with_history("request_applied", **audit, **{"from": owned.from_state, "to": target})
        applied = (request_id,) if isinstance(request_id, str) else ()
        released = Release(target, priority if isinstance(priority, int) else owned.from_priority, applied)
        # The owner's own boundary: requests posted while the attempt ran take effect here, from the commit's
        # target state, instead of racing other managers' claims of the released job.
        boundary = removal.apply_requests(
            owned,
            job,
            final,
            from_state=target,
            from_priority=released.priority,
            exclude=applied,
            cache=self._cache,
            deferred=self._deferred,
        )
        if boundary is not None:
            self._release(owned, boundary, released)

    def _remove_attempt(self, owned: OwnedJob, attempt_id: str) -> None:
        try:
            owned.discard_subtree(f"attempts/{attempt_id}")
        except (_fs.UnsafePath, OSError) as exc:
            _LOGGER.warning("cannot remove attempt %s of %s: %s", attempt_id, owned.job_key, exc)
            return
        _fs.remove_empty_dir(_fs.loc(owned.path / "attempts"))

    def _release(self, owned: OwnedJob, doc: StateDoc, target: Release) -> _kernel.JobRef:
        """The release rule with this owner's history entry and run-log events."""

        return removal.release(owned, doc, target)

    def _seal_enabled(self, job: JobDefinition) -> bool:
        """Decide the seal of a ``succeed`` commit: the job's ``seal_succeeded``, else the ``seal.succeeded`` setting."""

        if job.seal_succeeded is not None:
            return job.seal_succeeded
        raw = self._effective_settings().get("seal.succeeded", True)
        return str(raw).strip().lower() not in {"false", "0", "no", "off"}

    def _seal_keys(self) -> tuple[seals.SealKey, ...]:
        """Return the signing keys of ``seal.keys``; none resolving means an unsigned seal."""

        try:
            return seals.default_workspace_keys(self.workspace).keys
        except SealError:
            return ()

    def _context_children(self, doc: StateDoc) -> list[dict[str, object]]:
        """Return the context ``children`` of an activation that follows a join: its observations, located now.

        Each child's ``payload_path``, ``workdir_path`` and ``data_path`` are workspace-relative paths of its
        current directory (plan §13.3), or ``None`` when it cannot be found at its placement.
        """

        if doc.activation is None or doc.activation.get("reason") != "join":
            return []
        children: list[dict[str, object]] = []
        observations = doc.as_mapping()["observations"]
        assert isinstance(observations, list)
        for observation in observations:
            assert isinstance(observation, dict)
            ref = _kernel.locate(
                self.workspace,
                str(observation["job_id"]),
                placement_hint=PurePosixPath(str(observation["placement"])),
            )
            path = None if ref is None else ref.path.relative_to(self.workspace.root)
            located = {
                name: None if path is None else (path / suffix).as_posix()
                for name, suffix in (("payload_path", "."), ("workdir_path", "run"), ("data_path", "data"))
            }
            children.append(
                {
                    **observation,
                    "workspace_id": self.workspace.workspace_id,
                    "kind": observation["state"],
                    "data_generation": None,
                    **located,
                }
            )
        return children

    # -- requests and joins -------------------------------------------------------------------------------------------

    def _requests_pass(self) -> bool:
        """Serve the request files of one bounded listing (plan §7.5).

        A ``cancel`` of an attempt this manager runs stops the attempt now; every other request of a running
        job waits for its boundary. A job found unowned at the request's placement is claimed, reconciled
        (which applies its requests) and released.
        """

        directory = self.workspace.control / "requests"
        names = [name for name in self._cache.names(directory) if not name.startswith(".")]
        # A window resumed after a cursor, like the claim pass, so deferred or unclaimable requests never starve
        # later ones.
        start = self._request_cursor
        window = [name for name in names if start is None or name > start][: self.discovery_budget]
        self._request_cursor = window[-1] if len(window) == self.discovery_budget else None
        self._parsed = {name: entry for name, entry in self._parsed.items() if name in set(names)}
        by_job: dict[str, list[Path]] = {}
        for name in window:
            by_job.setdefault(name.split(".", 1)[0], []).append(directory / name)
        running = {local.owned.job_id: local for local in self._running.values()}
        changed = False
        for job_id, paths in by_job.items():
            requests = [request for path in paths if (request := self._parse_request(path)) is not None]
            if not requests or all(request.request_id in self._deferred for request in requests):
                continue
            local = running.get(job_id)
            if local is not None:
                try:
                    doc = local.owned.read_state()
                except (FormatError, _fs.UnsafePath):
                    doc = None  # the commit fails a damaged job; a cancel still stops it
                cancel = next(
                    (
                        request
                        for request in requests
                        if request.action == "cancel"
                        and (doc is None or not removal.translation_applied(self.workspace, doc, request))
                    ),
                    None,
                )
                if cancel is not None and local.cancel is None:
                    self._cancel_attempt(local, cancel)
                    changed = True
                continue
            ref = _kernel.locate(
                self.workspace, job_id, placement_hint=requests[0].placement, include_owned=False, cache=self._cache
            )
            if ref is not None and self._owns(ref.path):
                changed |= self._claim_and_return(ref)
        return changed

    def _parse_request(self, path: Path) -> _requests.Request | None:
        # Parsed requests are reused while their file is unchanged: verifying a signature every tick is costly.
        try:
            stamp = os.lstat(path).st_mtime_ns
        except FileNotFoundError:
            return None
        cached = self._parsed.get(path.name)
        if cached is not None and cached[0] == stamp:
            return cached[1]
        try:
            request = _requests.parse(path)
        except FileNotFoundError:
            return None
        except FormatError as exc:
            # ponytail: a malformed request stays where it is; gc quarantines it.
            self._report_anomaly(f"request:{path.name}", f"ignoring malformed request {path}: {exc}", {})
            return None
        self._parsed[path.name] = (stamp, request)
        return request

    def _pending_cancel(self, owned: OwnedJob) -> _requests.Request | None:
        """Return the job's first pending ``cancel`` request, or ``None``."""

        for path in owned.request_files():
            request = self._parse_request(path)
            if request is not None and request.job_id == owned.job_id and request.action == "cancel":
                return request
        return None

    def _cancel_attempt(self, local: RunningAttempt, request: _requests.Request) -> None:
        """Stop a running attempt for a ``cancel`` request: ``SIGTERM``, then ``SIGKILL`` after the grace."""

        local.cancel = request
        local.timeout_kill_at = time.monotonic() + self.cancel_grace_seconds
        _attempt_process.terminate_process(local.process.pid)
        _manager_launches.stop_all(self, local, "the job is being cancelled")
        _LOGGER.info(
            "cancelling attempt %s of %s (request %s)",
            local.attempt_id,
            local.owned.job_key,
            request.request_id,
            extra=self._event("attempt_cancel", local.owned, attempt_id=local.attempt_id),
        )

    def _claim_and_return(self, ref: _kernel.JobRef) -> bool:
        """Claim an unowned job, reconcile it (applying its requests) and release it unless that already did."""

        try:
            owned = _kernel.claim(self.workspace, self.owner, ref)
            if owned is None:
                return False
            owned.append_log("claimed", **{"from": ref.state, "to": _kernel.OWNED})
            reconciled = self._reconcile_guarded(owned)
            if reconciled is not None:
                self._release(owned, reconciled[1], Release(owned.from_state, owned.from_priority))
        except _kernel.OwnerLost:
            raise
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(f"claim:{ref.job_key}", f"cannot serve {ref.job_key}: {exc}", {"event": "claim_error"})
        return True

    def _joins_pass(self) -> bool:
        """Evaluate one bounded window of waiting parents; claim and release each one whose join is decided."""

        refs = list(
            _kernel.list_jobs(
                self.workspace,
                "waiting",
                prefixes=self.placement_prefixes,
                limit=self.discovery_budget,
                start=self._join_cursor,
            )
        )
        self._join_cursor = refs[-1].cursor if len(refs) == self.discovery_budget else None
        changed = False
        for ref in refs:
            if not self._owns(ref.path):
                continue
            try:
                decision = _joins.evaluate(
                    self.workspace,
                    ref,
                    cache=self._cache,
                    unresolved_since=self._unresolved,
                    grace=self.join_grace_seconds,
                    now=time.time(),
                )
            except (FormatError, OSError) as exc:
                self._report_anomaly(f"join:{ref.job_key}", f"cannot evaluate the join of {ref.job_key}: {exc}", {})
                continue
            if decision is not None:
                changed |= self._decide_join(ref, decision)
        return changed

    def _decide_join(self, ref: _kernel.JobRef, decision: _joins.JoinDecision) -> bool:
        """Claim a waiting parent and release it as its decided join says."""

        try:
            owned = _kernel.claim(self.workspace, self.owner, ref)
            if owned is None:
                return False
            owned.append_log("claimed", **{"from": ref.state, "to": _kernel.OWNED})
            reconciled = self._reconcile_guarded(owned)
            if reconciled is None:
                return True
            job, doc = reconciled
            # The claim's view, from fresh listings: a decision made from the pass's cached listings is a hint.
            current = None
            if doc.join is not None:
                current = _joins.evaluate(
                    self.workspace,
                    owned.ref,
                    cache=_kernel.ListingCache(),
                    unresolved_since=self._unresolved,
                    grace=self.join_grace_seconds,
                    now=time.time(),
                )
            if current is None:
                self._release(owned, doc, Release(owned.from_state, owned.from_priority))
                return True
            decision = current
            decided, target = decide_join(job, doc, decision)
            decided = decided.with_history("join_decided", detail=decision.kind)
            self._release(owned, decided, Release(target, owned.from_priority))
        except _kernel.OwnerLost:
            raise
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(f"join:{ref.job_key}", f"cannot apply the join of {ref.job_key}: {exc}", {})
        return True

    # -- claiming and launching ---------------------------------------------------------------------------------------

    def _workflow(self, workflow_id: str) -> tuple[_store.Installed | None, str | None]:
        """Return the installed workflow and ``None``, or ``None`` and why its closure is not runnable here."""

        if workflow_id not in self._workflows:
            installed: _store.Installed | None = None
            try:
                report = _store.closure(self.workspace, workflow_id, check_builds=True)
                problems = [
                    *(f"{item} is not installed" for item in report.missing),
                    *(f"{item} is not built for this platform" for item in report.unbuilt),
                ]
                if not problems:
                    installed = _store.lookup(self.workspace, workflow_id)
            except (RunnerResolutionError, ValueError) as exc:
                problems = [str(exc)]
            problem = None if installed is not None else f"workflow {workflow_id}: {'; '.join(problems)}"
            self._workflows[workflow_id] = (installed, problem)
        return self._workflows[workflow_id]

    def _available_resources(self) -> dict[str, int]:
        """Return manager capacity after reservations of local attempts."""

        available = _manager_scheduling.available_resources(self.resources, self._running.values())
        if self._inventory is not None:
            available.update({name: value for name, value in self._inventory.free().items() if name in available})
        return available

    def _release_placement(self, placement: Placement | None) -> None:
        if placement is not None and self._inventory is not None:
            release(self._inventory, placement)

    def _claim_and_launch(self, ref: _kernel.JobRef) -> bool:
        """Claim one ready job, reconcile it and launch its attempt while it is still eligible."""

        try:
            owned = _kernel.claim(self.workspace, self.owner, ref)
            if owned is None:
                return False
            owned.append_log("claimed", **{"from": ref.state, "to": _kernel.OWNED})
            reconciled = self._reconcile_guarded(owned)
            if reconciled is None:
                return True
            job, doc = reconciled
            requirement, blocker = _manager_scheduling.assess(self, job, doc)
            if requirement is None or not _manager_scheduling.fits_now(self, requirement):
                _LOGGER.debug("returning %s: no longer eligible here (%s)", owned.job_key, blocker)
                self._release(owned, doc, Release(owned.from_state, owned.from_priority))
                return True
            self._launch(owned, job, doc, requirement)
        except _kernel.OwnerLost:
            raise
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"claim:{ref.job_key}", f"cannot claim or launch {ref.job_key}: {exc}", {"event": "claim_error"}
            )
        return True

    def _launch(self, owned: OwnedJob, job: JobDefinition, doc: StateDoc, requirement: Mapping[str, int]) -> None:
        """Launch the job's next attempt, or return the job when a host condition holds it back."""

        self.owner.check_alive()
        settings = self._effective_settings()
        try:
            checked = self._confinement(settings)
        except _ConfinementBlocked as exc:
            self._report_confinement_blocked(str(exc))
            self._release(owned, doc, Release(owned.from_state, owned.from_priority))
            return
        placement = None
        if self._inventory is not None:
            placement = assign(self._inventory, requirement)
            if placement is None:
                self._release(owned, doc, Release(owned.from_state, owned.from_priority))
                return
        try:
            if self._start_attempt(owned, job, doc, requirement, settings, checked, placement):
                placement = None
        finally:
            self._release_placement(placement)

    def _runner_command(self, installed: _store.Installed) -> tuple[list[str], Path | None]:
        """Return an installed workflow's runner argv and its build artifacts directory (``None`` when unbuilt)."""

        artifacts = None
        if installed.record.get("build") is True:
            spec = read_build_spec(installed.package)
            built = None if spec is None else installed.build_dir(_store.platform_tag(spec))
            artifacts = None if built is None else built / "artifacts"
        runner = installed.record.get("runner")
        runner = runner if isinstance(runner, Mapping) else {}
        command, builtin, entry = runner.get("command"), runner.get("builtin"), runner.get("entry")
        if isinstance(command, list):
            try:
                return list(
                    expand_runner_command([str(item) for item in command], installed.package, artifacts)
                ), artifacts
            except ValueError as exc:
                raise RunnerResolutionError("runner_unavailable", f"workflow {installed.id}: {exc}") from exc
        if isinstance(builtin, str) and builtin in _BUILTIN_RUNNERS:
            return [str(runner_path(*_BUILTIN_RUNNERS[builtin]))], artifacts
        if isinstance(entry, str):
            return [str(installed.package / entry)], artifacts
        raise RunnerResolutionError("runner_unavailable", f"workflow {installed.id} declares no runner")

    def _start_attempt(
        self,
        owned: OwnedJob,
        job: JobDefinition,
        doc: StateDoc,
        requirement: Mapping[str, int],
        settings: Mapping[str, Any],
        checked: _Confinement,
        placement: Placement | None,
    ) -> bool:
        """The launch sequence of plan §7.7: phase launching, attempt directory, launch record, gated start,
        phase running, ``begin_attempt``, open gate. Returns whether the attempt runs."""

        attempt = doc.attempt
        if attempt is None or attempt.get("started_at") is not None:
            doc = doc.next_attempt("claim" if attempt is None else "launch", unclean=False)
        attempt_id = str(_text(doc.attempt, "id"))
        if (exhausted := attempt_budget_failure(job, doc)) is not None:
            budget = doc.with_failure(failure("budget_exhausted", exhausted))
            self._release(owned, budget, Release("failed", owned.from_priority))
            return False
        try:
            installed, problem = self._workflow(job.workflow_id)
            if installed is None:
                raise RunnerResolutionError("runner_unavailable", problem or f"workflow {job.workflow_id} is gone")
            command, artifacts = self._runner_command(installed)
        except RunnerResolutionError as exc:
            self._commit(
                owned, job, doc, failure_intent(job, doc, attempt_id, exc.code, str(exc), priority=owned.from_priority)
            )
            return False
        pin = {"id": installed.id, "tree_sha256": installed.record.get("tree_sha256")}
        doc = self._write(owned, doc.updated(workflow_pin=pin).with_phase("launching", attempt_id))
        payload = owned.path
        control = payload / "attempts" / attempt_id
        confinement = checked.settings
        activation = doc.activation
        step = str(_text(activation, "step"))
        stdio_fd = gate_read = gate_write = -1
        process: subprocess.Popen[bytes] | None = None
        launch_dir: Path | None = None
        sandbox: PreparedSandbox | None = None
        try:
            # Anchored below the job directory: a symlink or non-directory the job left at attempts/ or run/
            # is refused (UnsafePath), never followed.
            attempts = _fs.open_dir_under(payload, "attempts", create=True, mode=0o777)
            try:
                os.mkdir(attempt_id, dir_fd=attempts)
            except FileExistsError as exc:
                raise FormatError(f"attempt directory {control} already exists") from exc
            finally:
                os.close(attempts)
            os.close(_fs.open_dir_under(payload, "run", create=True, mode=0o777))
            workdir = payload / "run"
            if confinement is not None:
                check_job_placement(job.placement)
            deadline = self._attempt_deadline(requirement)
            # The runner launch's record directory exists before the gate (without process.json a probe counts it
            # dead); it also holds the trusted nodefile of the launch prefix.
            launch_dir = self.owner.launch_dir(attempt_id, "0")
            binding, binding_environment, pin_cpus = (
                (None, {}, None)
                if placement is None
                else self._attempt_binding(placement, control, launch_dir, settings, requirement.get("mem"))
            )
            launch_context: LaunchContext | None = None
            if confinement is not None and "HTTK_WORKFLOW_LAUNCH" in binding_environment:
                # A rendered prefix would start ranks outside the sandbox: a confined attempt gets the launch
                # client instead, and the manager renders each launch from this in-memory context, never from
                # the job-writable nodefile or binding.json.
                assert placement is not None and self.allocation is not None and checked.launch is not None
                template = settings.get("manager.launch_template")
                launch_context = LaunchContext(
                    placement=placement,
                    template=template if isinstance(template, str) else None,
                    kind=self.allocation.kind,
                    gpus_present=self.resources.get("gpus", 0) > 0,
                    cpus_per_proc=self.allocation.cpus_per_proc,
                    mem=requirement.get("mem"),
                    # Already validated by the binding above, which rendered the prefix.
                    mpi=_confine.launch_mpi_setting(settings),
                    confinement=checked.launch,
                )
                binding_environment["HTTK_WORKFLOW_LAUNCH"] = _manager_launches.client_prefix()
            children = self._context_children(doc)
            context = attempt_context(
                workspace_id=self.workspace.workspace_id,
                job_id=job.id,
                job_key=job.job_key,
                placement=placement_text(job.placement),
                payload=str(payload.resolve()),
                step=step,
                activation_id=_text(activation, "id"),
                activation_ordinal=_number(activation, "ordinal"),
                attempt_id=attempt_id,
                attempt_ordinal=_number(doc.attempt, "ordinal"),
                total_attempts=_number(doc.counters, "attempts_total"),
                is_unclean_restart=bool(doc.attempt and doc.attempt.get("unclean")),
                attempt_reason=_text(doc.attempt, "reason"),
                previous_attempt_id=_text(doc.attempt, "previous_attempt_id"),
                activation_reason=_text(activation, "reason"),
                durable=self.workspace.durable,
                settings=settings,
                resources=requirement,
                deadline=deadline,
                binding=binding,
                join=children or None,
                children=children,
            )
            environment = runner_environment(
                base=os.environ.copy(),
                context=context,
                control=control,
                workspace_root=self.workspace.root,
                payload=payload,
                workdir=workdir,
                durable=self.workspace.durable,
                deadline=deadline,
                binding_environment=binding_environment,
                code_variables=code_environment(),
                data_dir=payload / "data",
                declared_environment=job.environment.get("declared", {}),
                settings=settings,
            )
            environment["HTTK_WORKFLOW_RUNNER_ROOT"] = str(installed.package)
            if artifacts is not None:
                environment["HTTK_WORKFLOW_RUNNER_ARTIFACTS"] = str(artifacts)
            command = self._with_prelude(control, job, command)
            stdio_fd = self._open_chronicle(owned)
            started_line = (
                f"=== httk attempt {attempt_id} step {step} ordinal {context['attempt_ordinal']} started {utc_now()}\n"
            )
            write_marker(stdio_fd, started_line, job.job_key)
            owned.append_log(
                "attempt_started",
                attempt_id=attempt_id,
                activation_id=context["activation_id"],
                step=step,
                detail={"command": command},
            )
            gate_read, gate_write = os.pipe()
            if confinement is not None:
                # The filtered environment is the launcher's env, which Bubblewrap passes through.
                environment = _confine.filtered_attempt_environment(environment)
                sandbox = self._prepare_sandbox(confinement, payload, workdir, environment, checked.block_userns)
            process = start_gated(
                command,
                gate_read=gate_read,
                cwd=workdir,
                environment=environment,
                stdio_fd=stdio_fd,
                confined=confinement is not None,
                sandbox=sandbox,
                runner_fd=None,
            )
            # The launch record is durable before the gate opens (plan §3.6): a probe of a dead owner then finds it.
            _manager_launches.write_process_record(
                self, launch_dir, attempt_id, "0", process.pid, ranks_local_only=True
            )
            doc = self._write(owned, doc.with_phase("running", attempt_id))
        except Exception as exc:
            if process is not None:
                # The gate stays closed, so the launcher reads end-of-file and exits; reap it.
                os.close(gate_write)
                gate_write = -1
                self._reap_launcher(process)
            if launch_dir is not None:
                self.owner.remove_launch(attempt_id, "0")
            if isinstance(exc, _kernel.OwnerLost):
                raise
            reason = str(exc).replace("\n", "\\n")
            self._chronicle(owned, f"=== httk attempt {attempt_id} ended {utc_now()} launch-failed {reason}\n")
            code = (
                exc.code
                if isinstance(exc, RunnerResolutionError)
                else "protocol_error"
                if isinstance(exc, (FormatError, _fs.UnsafePath))
                else "process_failure"
            )
            _LOGGER.error(
                "cannot launch an attempt for %s: %s",
                owned.job_key,
                exc,
                extra=self._event("launch_error", owned, attempt_id=attempt_id),
            )
            message = f"cannot launch runner: {exc}"
            self._commit(
                owned, job, doc, failure_intent(job, doc, attempt_id, code, message, priority=owned.from_priority)
            )
            return False
        finally:
            for descriptor in (stdio_fd, gate_read):
                if descriptor >= 0:
                    os.close(descriptor)
            if sandbox is not None:
                sandbox.close()
        # From here on the attempt's processes may run: the job is not quiescent until end_attempt (P5).
        owned.begin_attempt(attempt_id)
        if pin_cpus is not None:
            try:
                os.sched_setaffinity(process.pid, pin_cpus)
            except (AttributeError, OSError) as exc:
                _LOGGER.warning("cannot pin attempt %s to CPUs %s: %s", attempt_id, format_cpulist(pin_cpus), exc)
        try:
            _attempt_process.release_gate(gate_write)
        except OSError as exc:
            # The launcher then reads end-of-file and exits; its reap fails the attempt.
            _LOGGER.warning("cannot open the gate of attempt %s: %s", attempt_id, exc)
        finally:
            os.close(gate_write)
        self._running[attempt_id] = RunningAttempt(
            owned,
            job,
            process,
            attempt_id,
            control,
            dict(requirement),
            time.monotonic(),
            requirement.get("maxtime"),
            placement=placement,
            launches=None if confinement is None else AttemptLaunches(f"attempts/{attempt_id}", launch_context),
        )
        owned.append_log("launched", attempt_id=attempt_id, activation_id=context["activation_id"], step=step)
        _LOGGER.info(
            "launched attempt %s for %s as pid %d",
            attempt_id,
            owned.job_key,
            process.pid,
            extra=self._event(
                "launch",
                owned,
                attempt_id=attempt_id,
                pid=process.pid,
                step=step,
                **({"confined": True} if confinement is not None else {}),
            ),
        )
        return True

    def _with_prelude(self, control: Path, job: JobDefinition, command: list[str]) -> list[str]:
        """Wrap *command* in a login shell running the workflow's prelude (under ``set -e``), when it has one.

        The login profiles may reset ``PATH``, so the manager's interpreter directory is put first again
        before the prelude, which keeps the last word.
        """

        prelude = self.workspace.read_workflow_preludes().get(job.workflow_name, "")
        if not prelude.strip():
            return command
        interpreter = shlex.quote(os.path.dirname(sys.executable))
        script = control / "prelude.sh"
        text = f'set -e\nexport PATH={interpreter}:"$PATH"\n' + prelude + '\nexec "$@"\n'
        _fs.write_file(_fs.loc(script), text.encode("utf-8"), durable=self.workspace.durable)
        return ["bash", "-l", str(script), *command]

    def _open_chronicle(self, owned: OwnedJob) -> int:
        """Open ``logs/stdio.out`` for appending (:meth:`~httk.workflow._kernel.OwnedJob.open_log`: never a
        symlink or FIFO a job planted)."""

        try:
            return owned.open_log("stdio.out")
        except (_fs.UnsafePath, OSError) as exc:
            raise FormatError(f"cannot open the stdio chronicle of {owned.job_key}: {exc}") from exc

    def _chronicle(self, owned: OwnedJob, line: str) -> None:
        """Append one marker to the stdio chronicle; a failure is logged, never raised."""

        try:
            descriptor = self._open_chronicle(owned)
        except (FormatError, OSError) as exc:
            _LOGGER.warning("cannot append the stdio chronicle of %s: %s", owned.job_key, exc)
            return
        try:
            write_marker(descriptor, line, owned.job_key)
        finally:
            os.close(descriptor)

    # -- supervision ------------------------------------------------------------------------------------------------

    def _supervise(self) -> bool:
        """Enforce deadlines, reap ended attempts and commit them (plan §7.7 step 7)."""

        changed = self._enforce_deadlines()
        # Right before the attempts: a launch is stopped in the pass that observes its attempt stop.
        changed |= _manager_launches.supervise(self)
        for local in list(self._running.values()):
            return_code = local.process.poll()
            if return_code is None:
                continue
            if _manager_launches.unreaped(local):
                # The attempt is not over while a launch of it runs: it is handled once every launch is reaped.
                _manager_launches.stop_all(self, local, "the attempt process exited")
                continue
            if _attempt_process.process_group_alive(local.process.pid):
                # A process the runner left in its group may still write the job: the job is quiescent only
                # once the whole group is gone.
                _attempt_process.terminate_process(local.process.pid, signal.SIGKILL)
                continue
            try:
                self._finish_attempt(local, return_code)
            except _kernel.OwnerLost:
                raise
            except Exception as exc:
                # The job stays owned with its intent; self-healing resumes the commit on a later tick.
                self._job_failed(local.owned, "commit", exc)
            else:
                self._failures.pop(local.owned.job_key, None)
            changed = True
        return changed

    def _finish_attempt(self, local: RunningAttempt, return_code: int) -> None:
        """End a reaped attempt (``end_attempt``) and commit its outcome or its failure."""

        owned, attempt_id, job = local.owned, local.attempt_id, local.job
        self._running.pop(attempt_id, None)
        self._release_placement(local.placement)
        _manager_launches.forget(self, local)
        owned.end_attempt(attempt_id)
        try:
            self.owner.remove_launch(attempt_id, "0")
        except OSError as exc:
            # The record describes a reaped process group, which the death proof counts as dead.
            _LOGGER.warning("cannot remove the launch record of attempt %s: %s", attempt_id, exc)
        try:
            doc = owned.read_state()
        except _kernel.OwnerLost:
            raise
        except (WorkflowError, OSError) as exc:
            self._fail_damaged(owned, f"state.json is damaged: {exc}")
            return
        if doc is None:
            self._fail_damaged(owned, "state.json is gone")
            return
        outcome: Mapping[str, Any] | None = None
        problem: str | None = None
        try:
            if _published(owned, attempt_id):
                outcome = read_outcome(local.control / "outcome.ready", doc)
        except (FormatError, _fs.UnsafePath) as exc:
            problem = str(exc)
        # job.json is not the attempt's to change (note §3.3): a rewritten one voids whatever it published.
        tampered: str | None = None
        try:
            if JobDefinition.from_path(owned.path / "job.json").digest != job.digest:
                tampered = "the attempt rewrote job.json"
        except FormatError as exc:
            tampered = f"the attempt left job.json unreadable: {exc}"
        if tampered is not None:
            outcome = None
        action = "none" if outcome is None else str(outcome["action"])
        self._chronicle(owned, f"=== httk attempt {attempt_id} ended {utc_now()} exit {return_code} outcome {action}\n")
        owned.append_log("attempt_ended", attempt_id=attempt_id, detail={"exit_status": return_code})
        _LOGGER.info(
            "attempt %s of %s exited with status %d",
            attempt_id,
            owned.job_key,
            return_code,
            extra=self._event("attempt_exit", owned, attempt_id=attempt_id, exit_status=return_code),
        )
        if local.cancel is not None:
            # A cancelled attempt commits as cancelled whatever it published; its committed data still applies.
            self._commit(owned, job, doc, cancel_intent(attempt_id, owned.from_priority, local.cancel))
            return
        if outcome is not None:
            self._commit_outcome(owned, job, doc, outcome, local.control / "outcome.ready")
            return
        if (cancel := self._pending_cancel(owned)) is not None:
            # Without an outcome, a cancel posted while it ran (not yet served) decides: the job is cancelled.
            self._commit(owned, job, doc, cancel_intent(attempt_id, owned.from_priority, cancel))
            return
        if tampered is not None:
            code, message, unclean = "protocol_error", tampered, False
        elif problem is not None:
            code, message, unclean = "protocol_error", f"published outcome is unusable: {problem}", False
        elif local.timed_out and local.maxtime is not None:
            code, message, unclean = "timeout", f"attempt exceeded its maxtime {format_duration(local.maxtime)}", False
        elif local.interrupted:
            reason = self._drain_reason or "the manager stopped"
            code, message, unclean = (
                "owner_lost",
                f"the manager stopped the attempt before it finished ({reason})",
                True,
            )
        else:
            code = "protocol_error" if return_code == 0 else "process_failure"
            message, unclean = f"runner exited with status {return_code} without an outcome", False
        _LOGGER.warning(
            "attempt of %s failed with %s: %s",
            owned.job_key,
            code,
            message,
            extra=self._event("attempt_failure", owned, failure_code=code, exit_status=return_code),
        )
        intent = failure_intent(
            job,
            doc,
            attempt_id,
            code,
            message,
            priority=owned.from_priority,
            exit_status=return_code,
            unclean=unclean,
        )
        self._commit(owned, job, doc, intent)

    def _attempt_deadline(self, requirement: Mapping[str, int]) -> int | None:
        """Return the epoch second an attempt launched now must finish by: its ``maxtime`` or the drain start."""

        candidates: list[int] = []
        if "maxtime" in requirement:
            candidates.append(int(time.time()) + requirement["maxtime"])
        if self.drain_start is not None:
            candidates.append(int(self.drain_start))
        return min(candidates, default=None)

    def _enforce_deadlines(self) -> bool:
        """Stop every local attempt that has run longer than its ``maxtime``: ``SIGTERM``, then ``SIGKILL``.

        A timed-out attempt is reaped like any other; an outcome it still published is committed, and
        otherwise it fails with ``timeout``.
        """

        if self._draining:
            # The drain is already stopping every attempt on its own clock.
            return False
        changed = False
        now = time.monotonic()
        for local in self._running.values():
            if local.process.poll() is not None:
                continue
            if local.timeout_kill_at is not None and now >= local.timeout_kill_at:
                local.timeout_kill_at = None
                _attempt_process.terminate_process(local.process.pid, signal.SIGKILL)
                continue
            if local.maxtime is None or local.timed_out or local.cancel is not None:
                continue
            if now < local.started + local.maxtime:
                continue
            local.timed_out = True
            local.timeout_kill_at = now + self.cancel_grace_seconds
            _attempt_process.terminate_process(local.process.pid)
            _manager_launches.stop_all(self, local, "the attempt exceeded its maxtime")
            _LOGGER.warning(
                "attempt %s of %s exceeded its maxtime %s; terminating it",
                local.attempt_id,
                local.owned.job_key,
                format_duration(local.maxtime),
                extra=self._event("attempt_timeout", local.owned, attempt_id=local.attempt_id),
            )
            changed = True
        return changed

    def _reap_launcher(self, process: subprocess.Popen[bytes], *, grace_seconds: float = 5.0) -> None:
        """Terminate and reap a launcher whose gate never opened."""

        for signal_number in (signal.SIGTERM, signal.SIGKILL):
            if process.poll() is None:
                _attempt_process.terminate_process(process.pid, signal_number)
            try:
                process.wait(timeout=grace_seconds)
                return
            except subprocess.TimeoutExpired:
                continue
        _LOGGER.warning("abandoned launcher pid %d could not be reaped", process.pid)

    # -- binding --------------------------------------------------------------------------------------------------------

    def _local_share(self, placement: Placement) -> tuple[NodeShare, Node] | None:
        """Return a placement's only share and its node when that is this manager's host, else ``None``."""

        if len(placement.nodes) != 1 or self._inventory is None:
            return None
        share = placement.nodes[0]
        node = next(free.node for free in self._inventory.nodes if free.node.host == share.host)
        same = share.host == self.hostname or share.host.split(".")[0] == self.hostname.split(".")[0]
        return (share, node) if node.local or same else None

    def _attempt_binding(
        self, placement: Placement, control: Path, launch_dir: Path, settings: Mapping[str, Any], mem: int | None
    ) -> tuple[dict[str, Any], dict[str, str], set[int] | None]:
        """Write the attempt's nodefile and ``binding.json``; return its context ``binding``, environment and pin CPUs.

        The nodefile goes to the runner launch's directory below this owner (trusted, and its path has no ``~``
        that the job directory's name has), so the shell-quoted launch prefix needs no quoting of its own; the
        job-readable ``binding.json`` stays in the attempt control directory.
        """

        durable = self.workspace.durable
        nodefile = launch_dir / "nodefile"
        lines = "".join(f"{host}\n" for host in nodefile_lines(placement)).encode("utf-8")
        _fs.write_file(_fs.loc(nodefile), lines, durable=durable)
        template = settings.get("manager.launch_template")
        if template is not None and not isinstance(template, str):
            raise FormatError("workspace setting manager.launch_template must be a string")
        try:
            mpi = _confine.launch_mpi_setting(settings)
        except ValueError as exc:
            raise FormatError(f"workspace setting manager.launch_mpi: {exc}") from exc
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
                mpi=mpi,
            )
        except ValueError as exc:
            source = "workspace setting manager.launch_template" if template is not None else "scheduler launch"
            raise FormatError(f"{source}: {exc}") from exc
        nodes = describe(placement)
        full: dict[str, Any] = {"nodes": nodes, "nodefile": str(nodefile)}
        if launch is not None:
            full["launch"] = launch
        # The per-slot cpulists and GPU ids can be large, so the context keeps only the counts.
        _fs.write_file(_fs.loc(control / "binding.json"), json_bytes(full) + b"\n", durable=durable)
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
        # Device identity holds only for a runner executed here; an srun step chooses its own CPUs and GPUs.
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

    # -- serving and draining --------------------------------------------------------------------------------------------

    def serve(
        self, *, poll_interval: float = 1.0, drain_timeout: float = 30.0, drain_grace_seconds: float = 10.0
    ) -> None:
        """Run until interrupted, draining running attempts on a stop signal.

        A first ``SIGTERM`` or ``SIGINT`` stops claiming new work, terminates the
        local attempts and keeps ticking so their outcomes are committed. A second
        signal exits at once.

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
                _LOGGER.warning("cannot install a drain handler for signal %d outside the main thread", number)
        try:
            while not self._drain_begin(drain_timeout=drain_timeout, drain_grace_seconds=drain_grace_seconds):
                self.tick()
                if self._drain_after_tick():
                    return
                time.sleep(min(poll_interval, 0.25) if self._draining else poll_interval)
        except KeyboardInterrupt:
            _LOGGER.info("interrupted; stopping without a drain", extra=self._event("interrupted"))
        finally:
            for installed, handler in previous.items():
                if handler is not None:
                    signal.signal(installed, handler)
            self._draining = False

    def _reset_drain(self) -> None:
        self._draining = False
        self._drain_signals = 0
        self._drain_reason: str | None = None
        self._drain_deadline: float | None = None
        self._drain_kill_at: float | None = None

    def _request_drain(self, number: int, frame: FrameType | None) -> None:
        self._drain_signals += 1
        self._draining = True

    def _drain_begin(self, *, drain_timeout: float, drain_grace_seconds: float) -> bool:
        """Start a due drain before one tick, returning whether the loop must stop."""

        end_time = self.end_time
        if not self._draining and self.drain_start is not None and time.time() >= self.drain_start:
            self._draining = True
            self._drain_reason = "deadline"
            _LOGGER.info(
                "drain point reached with %s left before the allocation ends; draining",
                format_duration(max(0, int((end_time or time.time()) - time.time()))),
                extra=self._event("drain_deadline", end_time=end_time),
            )
        # A signal during a drain the deadline started is already a second request.
        if self._drain_signals >= (1 if self._drain_reason == "deadline" else 2):
            _LOGGER.warning("stop signal during a drain: killing %d running attempt(s)", len(self._running))
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
        if not self._running:
            _LOGGER.info("drain complete: no local attempt remains", extra=self._event("drain_complete"))
            return True
        if now >= self._drain_deadline:
            _LOGGER.warning("drain timeout expired with %d attempt(s) unreaped", len(self._running))
            self._signal_running_attempts(signal.SIGKILL)
            return True
        if self._drain_kill_at is not None and now >= self._drain_kill_at:
            _LOGGER.warning("drain grace expired: killing %d running attempt(s)", len(self._running))
            self._signal_running_attempts(signal.SIGKILL)
            self._drain_kill_at = None
        return False

    def _signal_running_attempts(self, signal_number: int) -> int:
        """Signal every local attempt's process group, marking it interrupted, and report how many."""

        signalled = 0
        for local in self._running.values():
            _manager_launches.signal_all(self, local, signal_number, "the manager is stopping")
            if local.process.poll() is not None:
                continue
            _attempt_process.terminate_process(local.process.pid, signal_number)
            # An attempt a drain stops without an outcome is lost to the manager, not failed by itself.
            local.interrupted = True
            signalled += 1
        return signalled

    def run_until_idle(
        self,
        *,
        timeout: float = 60.0,
        poll_interval: float = 0.02,
        drain_timeout: float = 30.0,
        drain_grace_seconds: float = 10.0,
    ) -> WorkCensus:
        """Run until no local attempt or claimable job remains, and report the census.

        A job this manager cannot progress (another pool, capability or
        workflow, waiting on children or paused) does not keep it awake: it is
        counted in the returned census. A ``SIGTERM`` or the allocation's drain
        start drains the manager as :meth:`serve` does and then returns.

        :param timeout: Stop waiting after this many seconds (extended by running attempts when the
            allocation end is known).
        :param poll_interval: Wait this long between scheduling passes.
        :param drain_timeout: Stop draining after this much time.
        :param drain_grace_seconds: Kill attempts after this much drain grace.
        :return: The work census of the settled workspace.
        :raises httk.workflow.manager.NotIdleError: If the manager does not become idle before the timeout.
        """

        self._reset_drain()
        previous: Any = None
        try:
            previous = signal.signal(signal.SIGTERM, self._request_drain)
        except ValueError:
            _LOGGER.warning("cannot install a drain handler for signal %d outside the main thread", signal.SIGTERM)
        try:
            deadline = time.monotonic() + timeout
            quiet_passes = 0
            while True:
                if self._drain_begin(drain_timeout=drain_timeout, drain_grace_seconds=drain_grace_seconds):
                    return self._work_census()
                if self.end_time is not None and self._running:
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
                quiet_passes = 0 if census.actionable else quiet_passes + 1
                if quiet_passes >= 2:
                    return census
                time.sleep(poll_interval)
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)
            self._draining = False

    def _work_census(self) -> WorkCensus:
        census = _manager_scheduling.work_census(self)
        if not census.ready_claimable or self._draining or self._confinement_blocked() is None:
            return census
        # Held back by a host or operator condition, the ready jobs are not this manager's work until it clears.
        return dataclasses.replace(
            census,
            ready_claimable=0,
            ready_blocked={**census.ready_blocked, "confinement": {"manager.confine": census.ready_claimable}},
            actionable_count=census.actionable_count - census.ready_claimable,
        )

    # -- confinement -----------------------------------------------------------------------------------------------

    def _effective_settings(self) -> dict[str, Any]:
        """Return the settings this manager decides by: the workspace's, with its pinned overrides applied.

        Workspace values are read live, and the enabled extensions are re-read with them; nothing a job
        carries enters this mapping.

        :return: The effective settings.
        """

        return {**self.workspace.read_settings(), **self.setting_overrides}

    @staticmethod
    def _confine_mode(settings: Mapping[str, Any]) -> Literal["none", "bwrap"]:
        raw = settings.get("manager.confine")
        if raw is None or raw == "none":
            return "none"
        if raw == "bwrap":
            return "bwrap"
        raise ValueError(f"setting manager.confine must be none or bwrap: {raw!r}")

    def _confinement(self, settings: Mapping[str, Any], *, at_start: bool = False) -> _Confinement:
        """Check how attempts started now are confined, validating and probing only what changed.

        :param settings: The effective settings.
        :param at_start: Whether this is the manager's start, which refuses a condition instead of reporting it.
        :return: The checked confinement.
        :raises ValueError: At start, if a confinement setting is invalid.
        :raises httk.workflow.errors.ConfinementUnavailableError: At start, if Bubblewrap is unusable or a
            workspace with the exchange extension is not confined.
        :raises _ConfinementBlocked: After start, for any of those conditions.
        """

        try:
            mode = self._confine_mode(settings)
        except ValueError as exc:
            if at_start:
                raise
            raise _ConfinementBlocked(f"invalid confinement setting: {exc}") from exc
        if mode == "none":
            if EXCHANGE_EXTENSION in self.workspace.extensions:
                if at_start:
                    raise ConfinementUnavailableError(f"refusing to start: {_ENROLLED_MESSAGE}")
                raise _ConfinementBlocked(_ENROLLED_MESSAGE)
            return _Confinement(None, False, None)
        key = tuple(
            sorted(
                (name, repr(value))
                for name, value in settings.items()
                if name in ("manager.confine", "manager.confine.block_mpi_spawn")
                or name.startswith(_confine.CONFINE_PREFIX)
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

        A failed probe is repeated at most every :data:`CONFINE_REPROBE_SECONDS`.
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

    def _prepare_sandbox(
        self,
        confinement: _confine.ConfineSettings,
        job_path: Path,
        workdir: Path,
        environment: Mapping[str, str],
        block_userns: bool,
    ) -> PreparedSandbox:
        """Build one attempt's Bubblewrap sandbox from the workspace root and the owned job directory."""

        workspace_fd = os.open(self.workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            job_fd = _fs.open_dir(job_path)
        except BaseException:
            os.close(workspace_fd)
            raise
        try:
            return _confine.prepare_attempt_sandbox(
                confinement,
                workspace_root=self.workspace.root,
                workspace_fd=workspace_fd,
                job_path=job_path,
                job_fd=job_fd,
                workdir=workdir,
                environment=environment,
                block_userns=block_userns,
            )
        except (ValueError, ConfinementUnavailableError) as exc:
            raise FormatError(f"cannot confine the attempt: {exc}") from exc
        finally:
            os.close(workspace_fd)
            os.close(job_fd)
