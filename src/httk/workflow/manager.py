"""Filesystem workflow task manager for the current core profile."""

import logging
import os
import re
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import FrameType
from typing import Any, BinaryIO, Self

from httk.core.digests import sha256_file, tree_digest

from . import (
    _manager_cancellation,
    _manager_commit,
    _manager_joins,
    _manager_requests,
    _manager_runners,
    _manager_scheduling,
)
from ._util import (
    read_json,
    timestamp_seconds,
    utc_now,
    write_json_atomic,
)
from .errors import (
    FormatError,
    RunnerResolutionError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .executors import AttemptLaunch, PathRunnerExecutor, RunnerExecutor
from .manifests import read_maintenance_lock
from .models import (
    ATTEMPT_CONTROL_PREFIX,
    CARRIED_STATE_MEMBERS,
    CORE_PROFILE,
    JobDefinition,
    Marker,
    StateFrame,
    canonical_uuid,
    normalize_placement,
    validate_attempt_control,
)
from .workspace import DISCOVERY_HEARTBEAT_STRIDE, MarkerStream, Workspace

_LOGGER = logging.getLogger(__name__)
_DRAIN_SIGNALS = (signal.SIGTERM, signal.SIGINT)
DEFAULT_RUNNER_MODULES = ("httk.workflow",)
# The entry point of a staged runner tree. A single file is the common case; a
# tree is pinned and staged as a whole, and this is the one name inside it the
# manager will execute.
RUNNER_TREE_ENTRY = "run"
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
    "operator",
    "operator_key",
    "operator_reason",
    "request_id",
)
_ENVIRONMENT_MARKER = ".httk-environment-resolution.json"


def _setting_variable_name(key: str) -> str:
    """Return the environment variable synthesized for one setting name."""

    return "HTTK_" + key.upper().replace(".", "_")


@dataclass(frozen=True)
class WorkCensus:
    """What one manager's scan found, tagged by why each job is or is not its work.

    ``ready_blocked`` groups the ready and unregisterable-submitted jobs this
    manager cannot progress by the requirement it lacks — ``executor``, ``pool``,
    or ``capability`` — mapping each requirement to the count of jobs it would
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
        for kind in ("executor", "pool", "capability"):
            for name, count in sorted(self.ready_blocked.get(kind, {}).items()):
                groups.append(f"{kind}={name}: {count}")
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

    def mismatch_advice(self) -> str | None:
        """Name the requirements blocked jobs need that this manager lacks.

        :return: The mismatch advice, or ``None`` when nothing is blocked.
        """

        pools = sorted(self.ready_blocked.get("pool", {}))
        capabilities = sorted(self.ready_blocked.get("capability", {}))
        executors = sorted(self.ready_blocked.get("executor", {}))
        if not (pools or capabilities or executors):
            return None
        lacks: list[str] = []
        remedies: list[str] = []
        flags: list[str] = []
        if pools:
            lacks.append("pool(s) " + ",".join(pools))
            flags += [f"--pool {name}" for name in pools]
        if capabilities:
            lacks.append("capability(ies) " + ",".join(capabilities))
            flags += [f"--capability {name}" for name in capabilities]
        if flags:
            remedies.append("start a manager with " + " ".join(flags))
        # An executor is installed, not passed as a flag, so it gets its own
        # remedy clause rather than being dropped when a pool or capability also
        # mismatches.
        if executors:
            lacks.append("executor(s) " + ",".join(executors))
            remedies.append("run a manager that has executor(s) " + ",".join(executors) + " installed")
        remedy = "; ".join(remedies) if remedies else "start a manager that serves them"
        return (
            f"{self.ready_blocked_total} job(s) cannot be claimed here because this manager does not serve "
            f"{'; '.join(lacks)}; {remedy}, or pass --idle to keep serving"
        )

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
                f"{self.unreadable} job(s) have an unreadable definition — "
                "repair them with 'httk workflow workspace fsck'"
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
    :param stdout: Hold the captured standard-output stream.
    :param stderr: Hold the captured standard-error stream.
    :param attempt_id: Identify the running attempt.
    :param fenced: Mark an attempt whose marker ownership is already resolved.
    :param cancelling: Mark an attempt currently being cancelled.
    :param owner_uid: Record the operating-system owner when known.
    """

    marker: Marker
    process: subprocess.Popen[bytes]
    stdout: BinaryIO
    stderr: BinaryIO
    attempt_id: str
    # Set once this attempt's outcome has been committed or its marker has been
    # fenced: the process may still be exiting, and reaping it is then routine
    # rather than the discovery of an orphan.
    fenced: bool = False
    # Set while a cancellation is stopping this attempt. It stays tracked until
    # its exit has been verified, because the verification is exactly what the
    # cancelled state has to record.
    cancelling: bool = False
    owner_uid: int | None = None

    def close_logs(self) -> None:
        """Close the captured output streams for this attempt."""

        self.stdout.close()
        self.stderr.close()


class TaskManager:
    """Execute and recover jobs in one workflow workspace.

    :param workspace: Attach the manager to this workspace.
    :param pools: Accept jobs assigned to these pools.
    :param capabilities: Advertise these execution capabilities.
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
    :raises ValueError: If a manager limit is invalid or executor configuration conflicts.
    :raises httk.workflow.errors.UnsupportedExtensionError: If the workspace profile is not writable by this manager.
    """

    def __init__(
        self,
        workspace: Workspace,
        *,
        pools: Sequence[str] = ("default",),
        capabilities: Sequence[str] = (),
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
    ) -> None:
        if maximum_workers < 1:
            raise ValueError("maximum_workers must be positive")
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
        if workspace.core_profile != CORE_PROFILE:
            # Serving a workspace means writing it, so an older profile is
            # refused here as well as at attach time.
            raise UnsupportedExtensionError(
                f"cannot serve a {workspace.core_profile!r} workspace: this manager writes {CORE_PROFILE!r}"
            )
        self.workspace = workspace
        self.uid = os.getuid()
        # Ordered roots for jobs whose runner.source is installed, plus the
        # module prefixes the reserved pkg: form may name. Both are deployment
        # policy of this manager and never taken from a job.
        self.runner_search_paths: tuple[Path, ...] = tuple(Path(item).expanduser() for item in runner_search_paths)
        self.runner_modules: tuple[str, ...] = tuple(runner_modules)
        self.pools = frozenset(pools)
        self.capabilities = frozenset(capabilities)
        self.maximum_workers = maximum_workers
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
        self._draining = False
        self._drain_signals = 0
        write_json_atomic(
            self._manager_dir / "manager.json",
            {
                "format": "httk-workflow-manager",
                "format_version": 1,
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
                "started_at": utc_now(),
            },
            durable=workspace.durable,
        )
        self.heartbeat(force=True)
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
        """Close local attempt logs and the manager's journal writer."""

        for attempt in self._running.values():
            attempt.close_logs()
        self._running.clear()
        self.writer.close()

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
            control = self._attempt_control_path(marker, self._read_frame(marker))
            control.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                control / "commit-wedge.json",
                {
                    "format": "httk-workflow-commit-wedge",
                    "format_version": 1,
                    "error": text,
                    "manager_id": self.manager_id,
                    "recorded_at": utc_now(),
                },
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
            if stat.S_ISLNK(job_stat.st_mode) or not stat.S_ISREG(job_stat.st_mode):
                _LOGGER.debug(
                    "skipping job %s: job.json is not an owned regular file (marker/payload ownership mismatch)",
                    marker.job_key,
                )
                return False
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
        for step in (
            self._handle_requests,
            self._register_submissions,
            # Cancellation runs before the running pass so that the attempts it
            # is terminating on purpose are never mistaken for orphans.
            self._process_cancelling,
            self._resume_committing,
            self._evaluate_joins,
            self._poll_running,
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
            report = self.workspace.collect_garbage()
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
                f"ignoring a stale maintenance lock held by {lock.describe()}; "
                "clear it with 'httk workflow workspace unlock'",
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
        self._draining = False
        self._drain_signals = 0
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
                signal.signal(installed, handler)
            self._draining = False

    def _request_drain(self, number: int, frame: FrameType | None) -> None:
        """Record one drain request from a signal handler."""

        self._drain_signals += 1
        self._draining = True

    def _serve_loop(
        self,
        *,
        poll_interval: float,
        drain_timeout: float,
        drain_grace_seconds: float,
    ) -> None:
        drain_deadline: float | None = None
        kill_at: float | None = None
        while True:
            if self._drain_signals >= 2:
                _LOGGER.warning(
                    "second stop signal: killing %d running attempt(s) and exiting",
                    len(self._running),
                    extra=self._event("drain_forced", attempts=len(self._running)),
                )
                self._signal_running_attempts(signal.SIGKILL)
                return
            if self._draining and drain_deadline is None:
                now = time.monotonic()
                drain_deadline = now + drain_timeout
                kill_at = now + drain_grace_seconds
                _LOGGER.info(
                    "draining: terminating %d running attempt(s) with a %.0fs timeout",
                    len(self._running),
                    drain_timeout,
                    extra=self._event("drain_started", attempts=len(self._running)),
                )
                self._signal_running_attempts(signal.SIGTERM)
            self.tick()
            if self._draining and drain_deadline is not None:
                now = time.monotonic()
                if not self._running:
                    _LOGGER.info("drain complete: no local attempt remains", extra=self._event("drain_complete"))
                    return
                if now >= drain_deadline:
                    _LOGGER.warning(
                        "drain timeout expired with %d attempt(s) unreaped; leaving them to lease recovery",
                        len(self._running),
                        extra=self._event("drain_timeout", attempts=len(self._running)),
                    )
                    self._signal_running_attempts(signal.SIGKILL)
                    return
                if kill_at is not None and now >= kill_at:
                    _LOGGER.warning(
                        "drain grace expired: killing %d running attempt(s)",
                        len(self._running),
                        extra=self._event("drain_kill", attempts=len(self._running)),
                    )
                    self._signal_running_attempts(signal.SIGKILL)
                    kill_at = None
            time.sleep(min(poll_interval, 0.25) if self._draining else poll_interval)

    def _signal_running_attempts(self, signal_number: int) -> int:
        """Signal every local attempt process group and report how many."""

        signalled = 0
        for attempt in list(self._running.values()):
            if attempt.process.poll() is not None:
                continue
            self._terminate_process(attempt.process.pid, signal_number)
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

    def run_until_idle(self, *, timeout: float = 60.0, poll_interval: float = 0.02) -> WorkCensus:
        """Run until no local process or claimable marker remains, and report it.

        A job this manager cannot progress — one whose pool, capability, or
        executor it does not serve, or one waiting on children or paused for an
        operator — does not keep it awake: it is counted in the returned census
        instead. The census is what the caller prints as the idle summary.

        :param timeout: Stop waiting after this many seconds.
        :param poll_interval: Wait this long between scheduling passes.
        :return: The work census of the settled workspace.
        :raises httk.workflow.manager.NotIdleError: If the manager does not become idle before the timeout.
        """

        deadline = time.monotonic() + timeout
        quiet_passes = 0
        while time.monotonic() < deadline:
            changed = self.tick()
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
        raise NotIdleError(self._work_census())

    def _work_census(self) -> WorkCensus:
        return _manager_scheduling.work_census(self)

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

    def _read_frame(self, marker: Marker) -> StateFrame:
        """Return the typed state frame one marker references."""

        return StateFrame.from_mapping(self.workspace.read_state(marker))

    def _register_submissions(self) -> bool:
        return _manager_scheduling.register_submissions(self)

    def _eligible_ready(self) -> list[Marker]:
        return _manager_scheduling.eligible_ready(self)

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
        attempt_ordinal = (state.attempt_ordinal or 0) + 1
        total_attempts = (state.total_attempts or 0) + 1
        budget_failure = self._attempt_budget_failure(job, attempt_ordinal, total_attempts)
        if budget_failure is not None:
            self._transition(
                marker,
                "failed",
                StateFrame.of(
                    state.carried(),
                    failure=self._failure("budget_exhausted", budget_failure),
                    reason="budget_exhausted",
                ),
            )
            return True
        attempt_id = str(uuid.uuid4())
        claimed = self._transition(
            marker,
            "claimed",
            StateFrame.of(
                state.carried(),
                manager_id=self.manager_id,
                writer_id=self.writer.writer_id,
                claim_id=str(uuid.uuid4()),
                attempt_id=attempt_id,
                attempt_control=f"{ATTEMPT_CONTROL_PREFIX}{attempt_id}",
                attempt_ordinal=attempt_ordinal,
                total_attempts=total_attempts,
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
        except TransitionLostError:
            raise
        except (WorkflowError, OSError) as exc:
            self._fail_attempt_preparation(claimed, job, exc)
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
                StateFrame.of(
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

    def _stage_runner(self, job: JobDefinition, control: Path) -> Path:
        return _manager_runners.stage_runner(
            self, job, control, tree_digest=tree_digest, sha256_file=sha256_file, log=_LOGGER
        )

    def _launch_claimed(self, marker: Marker, job: JobDefinition, previous_state: StateFrame) -> None:
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
        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        control = payload / control_name
        control.mkdir(exist_ok=False)
        runner = payload.joinpath(*job.runner_path.parts)
        if job.runner_source != "payload":
            runner = self._stage_runner(job, control)
        if job.workdir_mode == "persistent":
            workdir = payload.joinpath(*job.workdir_path.parts)
            workdir_reused = workdir.exists()
        else:
            base = payload.joinpath(*job.workdir_path.parts)
            workdir = base.parent / f"{base.name}.{attempt_id}"
            workdir_reused = False
        workdir.mkdir(parents=True, exist_ok=True)
        settings = self.workspace.read_settings()
        context = {
            "format": "httk-workflow-attempt-context",
            "format_version": 1,
            "workspace_id": self.workspace.workspace_id,
            "job_id": job.id,
            "job_key": job.job_key,
            "placement": marker.placement.as_posix(),
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
            # runner resolves a.setting("vasp.command") without the operator
            # re-exporting it for every job. This is the workspace layer of the
            # parameters → environment → workspace → default resolution.
            "settings": settings,
            "resources": dict(job.resources),
            "join": claimed_state.join_summary,
            # The enriched, labeled observations of this activation's join, or an
            # empty array when the activation follows no join. ``join`` keeps the
            # summary exactly as earlier profiles published it.
            "children": self._context_children(claimed_state.join_summary),
        }
        context_path = control / "context.json"
        write_json_atomic(context_path, context, durable=self.workspace.durable)
        environment = os.environ.copy()
        environment.update(
            {
                "HTTK_WORKFLOW_CONTEXT": str(context_path),
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
                "HTTK_WORKFLOW_BASH_API": str(Path(__file__).with_name("shell") / "httk-workflow.sh"),
                "HTTK_WORKFLOW_PERL_API": str(Path(__file__).with_name("native") / "perl"),
                "HTTK_WORKFLOW_VASP_BASH_API": str(Path(__file__).with_name("shell") / "httk-vasp.sh"),
            }
        )
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
        stdout = (control / "stdout.log").open("ab")
        stderr = (control / "stderr.log").open("ab")
        runner_command = list(
            executor.command(
                AttemptLaunch(
                    job=job,
                    marker=marker,
                    payload=payload,
                    workdir=workdir,
                    control=control,
                    context_path=context_path,
                    context=context,
                    runner=runner,
                )
            )
        )
        if not runner_command:
            raise FormatError(f"runner executor {job.runner_executor!r} returned an empty command")
        gate_read, gate_write = os.pipe()
        process: subprocess.Popen[bytes] | None = None
        running: Marker | None = None
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).with_name("_launcher.py")),
                    str(gate_read),
                    "--",
                    *runner_command,
                ],
                cwd=workdir,
                env=environment,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                pass_fds=(gate_read,),
            )
            os.close(gate_read)
            gate_read = -1
            write_json_atomic(
                control / "process.json",
                {
                    "pid": process.pid,
                    "process_group": process.pid,
                    "hostname": self.hostname,
                    "launched_at": utc_now(),
                    "launch_gated": True,
                },
                durable=self.workspace.durable,
            )
            running = self._transition(
                marker,
                "running",
                StateFrame.of(
                    claimed_state.carried(),
                    manager_id=self.manager_id,
                    writer_id=self.writer.writer_id,
                    attempt_id=attempt_id,
                    lease_seconds=self.lease_seconds,
                    started_at=utc_now(),
                    workdir=str(workdir.relative_to(payload)),
                    attempt_control=control.name,
                    reason="launched",
                ),
            )
            os.write(gate_write, b"R")
        except Exception as exc:
            if gate_read >= 0:
                os.close(gate_read)
            os.close(gate_write)
            stdout.close()
            stderr.close()
            if process is not None:
                # The gate is closed, so the launcher observes end-of-file and
                # exits, but an unreaped launcher must never be left behind.
                self._reap_launcher(process)
            if isinstance(exc, TransitionLostError):
                raise
            _LOGGER.error(
                "cannot launch an attempt for %s: %s",
                marker.job_key,
                exc,
                extra=self._event("launch_error", marker, attempt_id=attempt_id),
            )
            self._handle_attempt_failure(running or marker, job, "process_failure", f"cannot launch runner: {exc}")
            return
        os.close(gate_write)
        assert process is not None
        assert running is not None
        self._running[attempt_id] = RunningAttempt(running, process, stdout, stderr, attempt_id, owner_uid=self.uid)
        launch_fields: dict[str, object] = {"attempt_id": attempt_id, "pid": process.pid, "step": context["step"]}
        if job.runner_source == "payload":
            # A payload-source runner lives in the mutable payload, so record the
            # payload digest at launch: it makes post-hoc mutation of the runner
            # at least visible in the journal.
            # ponytail: full payload tree digest per payload launch; cache or cap
            # if payload launches ever dominate the manager's cost.
            launch_fields["payload_digest"] = self.workspace.payload_digest(running)
        _LOGGER.info(
            "launched attempt %s for %s as pid %d in %s",
            attempt_id,
            running.job_key,
            process.pid,
            workdir,
            extra=self._event("launch", running, **launch_fields),
        )

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
                # A frame whose attempt identity cannot be used is a protocol
                # violation of whatever wrote it, recorded against that job
                # alone rather than allowed to stop the pass.
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
        outcome_path = self._outcome_path(marker, state)
        if outcome_path.is_dir():
            return self._commit_published_outcome(marker, job, state, outcome_path)
        local = self._running.get(attempt_id)
        if local is not None:
            return_code = local.process.poll()
            if return_code is None:
                return False
            local.close_logs()
            del self._running[attempt_id]
            _LOGGER.info(
                "attempt %s of %s exited with status %d",
                attempt_id,
                marker.job_key,
                return_code,
                extra=self._event("attempt_exit", marker, attempt_id=attempt_id, exit_status=return_code),
            )
            if outcome_path.is_dir():
                self._commit_published_outcome(marker, job, state, outcome_path)
            else:
                code = "protocol_error" if return_code == 0 else "process_failure"
                self._handle_attempt_failure(
                    marker,
                    job,
                    code,
                    f"runner exited with status {return_code} without an outcome",
                    exit_status=return_code,
                )
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
        if self._attempt_writer_dead(marker, state):
            return {"evidence": "writer_process_dead", "heartbeat_age_seconds": age}
        if job.workdir_mode == "persistent":
            if self.unsafe_persistent_takeover:
                return {"evidence": "unsafe_persistent_takeover", "heartbeat_age_seconds": age, "unsafe": True}
            writer_host = self._recorded_writer_host(marker, state)
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
                if not exited:
                    _LOGGER.debug(
                        "terminating fenced attempt %s of %s after its outcome was committed",
                        attempt_id,
                        local.marker.job_key,
                    )
                    self._terminate_process(local.process.pid)
                else:
                    _LOGGER.debug("reaped fenced attempt %s of %s", attempt_id, local.marker.job_key)
            else:
                _LOGGER.warning(
                    "attempt %s of %s no longer owns a running marker; terminating it",
                    attempt_id,
                    local.marker.job_key,
                    extra=self._event("attempt_orphaned", local.marker, attempt_id=attempt_id),
                )
                if not exited:
                    self._terminate_process(local.process.pid)
            local.close_logs()
            del self._running[attempt_id]

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
            self._begin_commit(marker, state, outcome_path)
        except TransitionLostError:
            return True
        except FormatError as exc:
            # A malformed outcome is a protocol violation of the runner, never a
            # reason to stop the manager.
            self._handle_attempt_failure(marker, job, "protocol_error", f"published outcome is unusable: {exc}")
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"commit:{marker.job_key}",
                f"cannot begin the commit of {marker.job_key}: {exc}",
                self._event("commit_error", marker),
            )
        return True

    def _environment_log_ready(self, marker: Marker, state: StateFrame) -> bool:
        """Report whether a published outcome may be committed this tick."""

        marker_path = self._attempt_control_path(marker, state) / _ENVIRONMENT_MARKER
        if not marker_path.is_file():
            return True
        try:
            recorded = read_json(marker_path)
        except (FormatError, OSError):
            return True
        if recorded.get("status") != "resolved" or not recorded.get("log_pending"):
            return True
        deadline = recorded.get("log_deadline")
        deadline_expired = (
            isinstance(deadline, (int, float)) and not isinstance(deadline, bool) and time.time() >= deadline
        )
        if not self._attempt_writer_dead(marker, state) and not deadline_expired:
            return False
        return self._reconcile_environment_log_absence(marker_path)

    def _reconcile_environment_log_absence(self, marker_path: Path) -> bool:
        """Clear pending logging after writer death or its persisted grace."""

        # Re-read immediately before the atomic update so a runner that won the
        # race to clear the handshake is never overwritten by stale state.
        try:
            recorded = read_json(marker_path)
        except (FormatError, OSError):
            return True
        if recorded.get("status") != "resolved" or not recorded.get("log_pending"):
            return True
        recorded["log_pending"] = False
        recorded["log_absent"] = True
        write_json_atomic(marker_path, recorded, durable=self.workspace.durable)
        return True

    def _attempt_control_path(self, marker: Marker, state: StateFrame) -> Path:
        """Return the attempt-control directory one frame names.

        The component is validated before it is joined, so a damaged or hostile
        frame is a protocol error of that job rather than a path that reaches
        outside its payload.
        """

        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        control_name = state.attempt_control
        if control_name is None:
            attempt_id = state.attempt_id
            if attempt_id is None:
                raise FormatError("state frame names neither an attempt control directory nor an attempt")
            control_name = validate_attempt_control(f"{ATTEMPT_CONTROL_PREFIX}{attempt_id}")
        return payload / control_name

    def _outcome_path(self, marker: Marker, state: StateFrame) -> Path:
        return self._attempt_control_path(marker, state) / "outcome.ready"

    def _begin_commit(self, marker: Marker, state: StateFrame, outcome_path: Path) -> None:
        outcome = self._read_outcome(outcome_path / "outcome.json", marker, state)
        child_digests = self._child_digests(outcome_path)
        # The spawn set is validated before the marker leaves running, so a
        # missing or ambiguous child label is a protocol error of the published
        # outcome rather than a commit failure of an accepted one.
        child_labels = self._spawn_labels(outcome_path)
        attempt_id = state.attempt_id
        attempt_control = state.attempt_control
        if attempt_id is None or attempt_control is None:
            raise FormatError("a running frame must name its attempt and attempt control directory")
        self._transition(
            marker,
            "committing",
            StateFrame.of(
                state.carried(),
                manager_id=self.manager_id,
                writer_id=self.writer.writer_id,
                attempt_id=attempt_id,
                attempt_control=attempt_control,
                outcome_action=str(outcome["action"]),
                child_digests=child_digests,
                child_labels=child_labels,
                reason="outcome_published",
            ),
        )
        # The attempt has published everything it will ever publish and its
        # marker has moved, so the local process is finishing rather than
        # orphaned. Reaping it is then routine and silent.
        local = self._running.get(attempt_id)
        if local is not None:
            local.fenced = True

    def _resume_committing(self) -> bool:
        return _manager_commit.resume(self, _LOGGER)

    def _process_committing(self, marker: Marker) -> None:
        _manager_commit.process_committing(self, marker)

    def _declared_runner_steps(self, marker: Marker, outcome: Mapping[str, Any]) -> list[str] | None:
        return _manager_commit.declared_runner_steps(marker, outcome, _LOGGER)

    def _read_outcome(self, path: Path, marker: Marker, state: StateFrame) -> dict[str, Any]:
        return _manager_commit.read_outcome(path, marker, state)

    def _child_digests(self, outcome_path: Path) -> dict[str, str]:
        return _manager_commit.child_digests(outcome_path, tree_digest)

    def _spawn_labels(self, outcome_path: Path) -> dict[str, str]:
        return _manager_commit.spawn_labels(outcome_path)

    def _labeled_join(self, join: Mapping[str, Any], outcome_path: Path) -> dict[str, object]:
        return _manager_commit.labeled_join(join, outcome_path)

    def _register_children(self, marker: Marker, state: StateFrame, outcome_path: Path) -> None:
        _manager_commit.register_children(self, marker, state, outcome_path, tree_digest)

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
        priority: int | None = None,
    ) -> None:
        _manager_commit.advance(
            self, marker, job, state, next_step, progress, reason=reason, join_summary=join_summary, priority=priority
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
    ) -> None:
        _manager_commit.retry(
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

    def _recorded_writer_host(self, marker: Marker, state: StateFrame) -> str | None:
        """Return the host that launched the recorded attempt, when it named one."""

        try:
            process = read_json(self._attempt_control_path(marker, state) / "process.json")
        except (FormatError, WorkflowError, OSError):
            return None
        host = process.get("hostname")
        return host if isinstance(host, str) and host else None

    def _attempt_writer_dead(self, marker: Marker, state: StateFrame) -> bool:
        """Report whether the process of the recorded attempt is provably gone.

        Only a process this host can ask about proves anything, so an attempt
        recorded on another host is never called dead here. Absence of proof is
        reported as ``False``: the caller decides what an unprovable attempt
        justifies.
        """

        try:
            process_path = self._attempt_control_path(marker, state) / "process.json"
        except FormatError as exc:
            _LOGGER.warning("cannot locate the attempt control directory of %s: %s", marker.job_key, exc)
            return False
        try:
            process = read_json(process_path)
        except WorkflowError:
            return False
        if process.get("hostname") != self.hostname:
            return False
        try:
            pid = int(process["pid"])
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except (KeyError, TypeError, ValueError, PermissionError):
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
            StateFrame.of(
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
                StateFrame.of(
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
                    "format_version": 1,
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

    def _cancellation_evidence(self, marker: Marker, state: StateFrame) -> dict[str, object] | None:
        return _manager_cancellation.evidence(self, marker, state, read_json=read_json, utc_now=utc_now)

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
            return
        try:
            process = read_json(self._attempt_control_path(marker, state) / "process.json")
            if process.get("hostname") == self.hostname:
                self._terminate_process(int(process["process_group"]), signal_number)
        except (FormatError, WorkflowError, KeyError, TypeError, ValueError):
            return

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
