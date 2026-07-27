"""Filesystem workflow task manager for the current core profile."""

import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path, PurePosixPath
from types import FrameType
from typing import Any, BinaryIO, Self

from ._util import (
    read_json,
    require_int,
    require_mapping,
    require_string,
    sha256_file,
    timestamp_seconds,
    tree_digest,
    utc_now,
    write_json_atomic,
)
from .backends import AttemptLaunch, OutcomeCommit, PathRunnerBackend, RunnerBackend
from .configuration import verify_document
from .errors import (
    FormatError,
    RunnerResolutionError,
    TransactionError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .manifests import read_maintenance_lock
from .models import (
    ATTEMPT_CONTROL_PREFIX,
    CARRIED_STATE_MEMBERS,
    CORE_PROFILE,
    TERMINAL_KINDS,
    Failure,
    JobDefinition,
    Marker,
    StateFrame,
    canonical_uuid,
    is_payload_private,
    normalize_placement,
    parse_package_runner,
    validate_attempt_control,
    validate_failure,
    validate_label,
    validate_step,
)
from .transactions import replay_transaction
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


@dataclass
class RunningAttempt:
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

    def close_logs(self) -> None:
        self.stdout.close()
        self.stderr.close()


class TaskManager:
    """Execute and recover jobs in one workflow workspace."""

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
        runner_backends: Sequence[RunnerBackend] = (),
        allowed_backends: Sequence[str] | None = None,
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
        backends = [PathRunnerBackend(), *runner_backends]
        self.runner_backends = {backend.name: backend for backend in backends}
        if len(self.runner_backends) != len(backends):
            raise ValueError("runner backend names must be unique")
        self.allowed_backends = (
            frozenset(self.runner_backends) if allowed_backends is None else frozenset(allowed_backends)
        )
        unknown_allowed = self.allowed_backends - self.runner_backends.keys()
        if unknown_allowed:
            raise ValueError(f"allowed runner backends are not installed: {', '.join(sorted(unknown_allowed))}")
        self.accept_any_pool = accept_any_pool
        self.manager_id = str(uuid.uuid4())
        self.hostname = socket.gethostname()
        self.writer = workspace.open_journal_writer()
        self._manager_dir = workspace.control / "managers" / self.manager_id
        self._manager_dir.mkdir(parents=True, exist_ok=False)
        self._running: dict[str, RunningAttempt] = {}
        # Parent job key -> (unresolvable child job id, monotonic first-seen).
        self._join_unresolved: dict[str, tuple[str, float]] = {}
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
                "pools": sorted(self.pools),
                "capabilities": sorted(self.capabilities),
                "placement_prefixes": [prefix.as_posix() for prefix in self.placement_prefixes],
                "runner_backends": sorted(self.allowed_backends),
                "runner_search_paths": [str(path) for path in self.runner_search_paths],
                "runner_modules": list(self.runner_modules),
                "accept_any_pool": self.accept_any_pool,
                "started_at": utc_now(),
            },
            durable=workspace.durable,
        )
        self.heartbeat(force=True)
        _LOGGER.info(
            "manager %s attached to workspace %s as %s pools=%s capabilities=%s backends=%s workers=%d",
            self.manager_id,
            self.workspace.workspace_id,
            self.hostname,
            ",".join(sorted(self.pools)) or "-",
            ",".join(sorted(self.capabilities)) or "-",
            ",".join(sorted(self.allowed_backends)),
            self.maximum_workers,
            extra=self._event("manager_started", workspace=str(self.workspace.root)),
        )
        for name in self.allowed_backends:
            try:
                self.runner_backends[name].reconcile(self.workspace)
            except (WorkflowError, OSError) as exc:
                # Backend views are derived and must never prevent the manager
                # from attaching to authoritative marker state.
                _LOGGER.warning(
                    "runner backend %s could not reconcile its derived view: %s",
                    name,
                    exc,
                    extra=self._event("backend_error", backend=name),
                )
                continue

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        for attempt in self._running.values():
            attempt.close_logs()
        self._running.clear()
        self.writer.close()

    @property
    def manager_directory(self) -> Path:
        """Return this manager's own directory below ``managers/``."""

        return self._manager_dir

    def _event(self, event: str, marker: Marker | None = None, **fields: object) -> dict[str, object]:
        """Return structured logging fields describing one manager event."""

        data: dict[str, object] = {"event": event, "manager_id": self.manager_id}
        if marker is not None:
            data.update(
                {
                    "job_key": marker.job_key,
                    "job_id": marker.job_id,
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
            return
        self._reported[key] = text
        _LOGGER.log(level, "%s", text, extra=dict(fields))

    @property
    def heartbeat_period(self) -> float:
        """Return how long this manager may actually go without heartbeating.

        A configured interval longer than the lease it claims work under would
        let a manager expire its own claims, so the interval is capped at a
        fraction of the lease however it was configured.
        """

        if self.lease_seconds <= 0.0:
            return self.heartbeat_interval
        return min(self.heartbeat_interval, self.lease_seconds * _MAXIMUM_HEARTBEAT_LEASE_FRACTION)

    def heartbeat(self, *, force: bool = False) -> None:
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
        return stream.advance(
            processing_budget=self.maximum_pass_markers,
            discovery_budget=self.discovery_budget,
            heartbeat=self.heartbeat,
            heartbeat_every=DISCOVERY_HEARTBEAT_STRIDE,
        )

    def _walk(self, kinds: Sequence[str]) -> Iterable[Marker]:
        """Stream every marker of *kinds* this manager may schedule, exhaustively.

        A few passes must see all of their kind at once — polling running
        attempts to reap orphans, recovering abandoned claims, and the idleness
        probe — so they cannot be windowed. They still stream through the same
        scandir walker, restricted to the assigned placement subtrees and
        heartbeating from inside the walk, so a long exhaustive pass never holds
        its heartbeat and never touches a placement outside its assignment.
        """

        return self.workspace.walk_markers(kinds, roots=self.placement_prefixes, heartbeat=self.heartbeat)

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
        """Perform one nonblocking scheduling and recovery pass."""

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

        if self._draining:
            _LOGGER.debug("draining: not claiming new work")
            return changed
        if self._maintenance_paused():
            return changed
        for marker in self._eligible_ready():
            if len(self._running) >= self.maximum_workers:
                break
            try:
                changed |= self._claim_and_launch(marker)
            except TransitionLostError as exc:
                _LOGGER.debug("claim of %s was lost to another actor: %s", marker.job_key, exc)
                continue
            except (WorkflowError, OSError) as exc:
                # Defense in depth: no single job may abort the claim pass.
                self._report_anomaly(
                    f"claim:{marker.job_key}",
                    f"cannot claim or launch {marker.job_key}: {exc}",
                    self._event("claim_error", marker),
                )
                changed = True
                continue
        return changed

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

    def _backend_for(self, job: JobDefinition) -> RunnerBackend | None:
        if job.runner_backend not in self.allowed_backends:
            return None
        return self.runner_backends.get(job.runner_backend)

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
            backend = self._backend_for(job)
            if backend is not None:
                backend.marker_changed(self.workspace, moved)
        except (WorkflowError, OSError) as exc:
            # Backend views are recoverable derivatives. The committed marker
            # transition must remain successful even if refreshing one fails.
            _LOGGER.warning(
                "runner backend view for %s could not be refreshed: %s",
                moved.job_key,
                exc,
                extra=self._event("backend_error", moved),
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

    def run_until_idle(self, *, timeout: float = 60.0, poll_interval: float = 0.02) -> None:
        """Run until no local process or immediately actionable marker remains."""

        deadline = time.monotonic() + timeout
        quiet_passes = 0
        while time.monotonic() < deadline:
            changed = self.tick()
            actionable = self._has_actionable_work()
            if not changed and not self._running and not actionable:
                quiet_passes += 1
                if quiet_passes >= 2:
                    return
            else:
                quiet_passes = 0
            time.sleep(poll_interval)
        raise TimeoutError("workflow manager did not become idle")

    def _has_actionable_work(self) -> bool:
        for marker in self._walk(("submitted", "ready", "committing", "cancelling")):
            try:
                job = self.workspace.load_job(marker)
            except (WorkflowError, OSError):
                # An invalid submission is actionable because registration will
                # turn it into a protocol failure. A job that is already
                # registered and cannot be loaded is skipped, not waited for.
                if marker.kind == "submitted":
                    return True
                continue
            if self._backend_for(job) is not None:
                return True
        return False

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
        changed = False
        for marker in self._window("register_submissions", "submitted"):
            self._pace()
            try:
                job = self.workspace.validate_job_payload(marker)
                backend = self._backend_for(job)
                if backend is None:
                    _LOGGER.debug(
                        "skipping submitted job %s: runner backend %s is not served here",
                        marker.job_key,
                        job.runner_backend,
                    )
                    continue
                backend.validate(job, self.workspace.payload_path(marker.placement, marker.job_key))
                self._transition(
                    marker,
                    "ready",
                    StateFrame.of(
                        step=job.initial_step,
                        activation_id=str(uuid.uuid4()),
                        activation_ordinal=1,
                        attempt_ordinal=0,
                        total_attempts=0,
                        data_generation=0 if job.data_mode == "transactional" else None,
                        reason="submitted",
                        job_digest=job.digest,
                    ),
                )
            except TransitionLostError:
                pass
            except (FormatError, UnsupportedExtensionError) as exc:
                try:
                    self._transition(
                        marker,
                        "failed",
                        StateFrame.of(
                            failure=self._failure("protocol_error", str(exc)),
                            data_generation=None,
                            reason="submission_invalid",
                        ),
                    )
                except TransitionLostError:
                    pass
            changed = True
        return changed

    def _eligible_ready(self) -> list[Marker]:
        """Return the best-priority eligible work found in one bounded window.

        Discovery is a single streaming window over the ready tree — never a
        global scan and never a global sort. Within that window the manager
        claims the best-priority candidates first, and the window's round-robin
        rotation is what eventually reaches a starved subtree on a later tick.
        Priority is therefore best-within-window rather than exact-global; that
        is the deliberate price of bounded discovery, and a derived priority
        index would be the only way to recover exact global order — not built
        here, and only worth building if a deployment measures that it needs it.
        """

        eligible: list[Marker] = []
        for marker in self._window("eligible_ready", "ready"):
            self._pace()
            try:
                job = self.workspace.load_job(marker)
            except (WorkflowError, OSError) as exc:
                self._report_anomaly(
                    f"ready:{marker.job_key}",
                    f"skipping ready job {marker.job_key}: {exc}",
                    self._event("job_unusable", marker, pass_name="ready"),
                )
                continue
            self._reported.pop(f"ready:{marker.job_key}", None)
            if self._backend_for(job) is None:
                _LOGGER.debug(
                    "skipping ready job %s: runner backend %s is not served here",
                    marker.job_key,
                    job.runner_backend,
                )
                continue
            if not self.accept_any_pool and job.claim_pool not in self.pools:
                _LOGGER.debug(
                    "skipping ready job %s: pool %s is not served here",
                    marker.job_key,
                    job.claim_pool,
                )
                continue
            if not job.required_capabilities <= self.capabilities:
                _LOGGER.debug(
                    "skipping ready job %s: missing capabilities %s",
                    marker.job_key,
                    ",".join(sorted(job.required_capabilities - self.capabilities)),
                )
                continue
            eligible.append(marker)
        eligible.sort(key=lambda item: (item.priority, item.path.as_posix()))
        return eligible

    def _claim_and_launch(self, marker: Marker) -> bool:
        """Claim one ready job and launch its attempt, reporting local faults."""

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
        """Return the file or tree naming one runner stored outside the payload."""

        if job.runner_source == "workspace":
            try:
                candidate = self.workspace.runner_store_path(job.runner_path)
            except FormatError as exc:
                raise RunnerResolutionError("runner_unavailable", str(exc)) from exc
            if not candidate.exists():
                raise RunnerResolutionError(
                    "runner_unavailable",
                    f"workspace runner {job.runner_path.as_posix()} is not published in {self.workspace.runners}",
                )
            return candidate
        package = parse_package_runner(job.runner_path.as_posix())
        if package is not None:
            return self._resolve_package_runner(*package)
        for root in self.runner_search_paths:
            installed = self._contained(root, job.runner_path.parts)
            if installed is not None and installed.exists():
                return installed
        searched = ", ".join(str(path) for path in self.runner_search_paths) or "no configured search path"
        raise RunnerResolutionError(
            "runner_unavailable",
            f"installed runner {job.runner_path.as_posix()} was not found in {searched}",
        )

    def _resolve_package_runner(self, module: str, resource: PurePosixPath) -> Path:
        """Resolve one ``pkg:<module>/<resource>`` runner inside the allowlist."""

        if not any(module == allowed or module.startswith(f"{allowed}.") for allowed in self.runner_modules):
            raise RunnerResolutionError(
                "runner_unavailable",
                f"runner module {module} is not in this manager's runner module allowlist "
                f"({', '.join(self.runner_modules) or 'empty'})",
            )
        try:
            root = Path(str(files(module)))
        except (ImportError, TypeError, ValueError) as exc:
            raise RunnerResolutionError(
                "runner_unavailable", f"runner module {module} is not importable: {exc}"
            ) from exc
        candidate = self._contained(root, resource.parts)
        if candidate is None or not candidate.exists():
            raise RunnerResolutionError(
                "runner_unavailable",
                f"installed runner pkg:{module}/{resource.as_posix()} does not exist",
            )
        return candidate

    @staticmethod
    def _contained(root: Path, parts: Sequence[str]) -> Path | None:
        """Return ``root/parts`` when it really stays below the resolved *root*."""

        candidate = root.joinpath(*parts)
        try:
            resolved_root = root.resolve()
            resolved = candidate.resolve()
        except OSError:
            return None
        return candidate if resolved.is_relative_to(resolved_root) else None

    def _stage_runner(self, job: JobDefinition, control: Path) -> Path:
        """Stage and verify one shared runner, returning what will be executed.

        Nothing outside the payload is ever executed in place: the resolved
        runner is copied below the attempt control directory, the digest the job
        pinned is verified on that staged copy, and only the copy is launched. A
        runner replaced between resolution and execution therefore cannot be
        substituted for the verified bytes.
        """

        source = self._resolve_shared_runner(job)
        staged = control / "runner"
        try:
            if source.is_dir():
                shutil.copytree(source, staged, symlinks=False)
                for entry in sorted(staged.rglob("*")):
                    entry.chmod(0o500)
                staged.chmod(0o500)
                digest = tree_digest(staged)
            else:
                shutil.copyfile(source, staged)
                staged.chmod(0o500)
                digest = sha256_file(staged)
        except OSError as exc:
            raise RunnerResolutionError(
                "runner_unavailable",
                f"cannot stage runner {source} for {job.job_key}: {exc}",
            ) from exc
        except FormatError as exc:
            raise RunnerResolutionError("runner_unavailable", f"cannot pin runner {source}: {exc}") from exc
        if digest != job.runner_sha256:
            raise RunnerResolutionError(
                "runner_mismatch",
                f"{job.runner_source} runner {job.runner_path.as_posix()} has digest {digest}, "
                f"but the job pinned {job.runner_sha256}",
            )
        executable = staged / RUNNER_TREE_ENTRY if staged.is_dir() else staged
        if not executable.is_file():
            raise RunnerResolutionError(
                "runner_unavailable",
                f"staged runner tree {job.runner_path.as_posix()} has no {RUNNER_TREE_ENTRY} entry point",
            )
        _LOGGER.info(
            "staged %s runner %s for %s as %s (digest %s)",
            job.runner_source,
            job.runner_path.as_posix(),
            job.job_key,
            executable,
            digest,
            extra=self._event(
                "runner_staged",
                runner=job.runner_path.as_posix(),
                runner_source=job.runner_source,
                sha256=digest,
            ),
        )
        return executable

    def _launch_claimed(self, marker: Marker, job: JobDefinition, previous_state: StateFrame) -> None:
        claimed_state = self._read_frame(marker)
        backend = self._backend_for(job)
        if backend is None:
            raise FormatError(f"runner backend is unavailable: {job.runner_backend}")
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
                "HTTK_WORKFLOW_VASP_BASH_API": str(Path(__file__).with_name("shell") / "httk-vasp.sh"),
            }
        )
        if job.data_mode == "transactional":
            environment["HTTK_WORKFLOW_DATA_DIR"] = str(payload / "data")
        stdout = (control / "stdout.log").open("ab")
        stderr = (control / "stderr.log").open("ab")
        runner_command = list(
            backend.command(
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
            raise FormatError(f"runner backend {job.runner_backend!r} returned an empty command")
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
        self._running[attempt_id] = RunningAttempt(running, process, stdout, stderr, attempt_id)
        _LOGGER.info(
            "launched attempt %s for %s as pid %d in %s",
            attempt_id,
            running.job_key,
            process.pid,
            workdir,
            extra=self._event("launch", running, attempt_id=attempt_id, pid=process.pid, step=context["step"]),
        )

    @staticmethod
    def _context_children(join_summary: object) -> list[dict[str, object]]:
        """Return the enriched child observations one activation may read."""

        if not isinstance(join_summary, Sequence) or isinstance(join_summary, (str, bytes)):
            return []
        return [dict(item) for item in join_summary if isinstance(item, Mapping)]

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
            if self._backend_for(job) is None:
                _LOGGER.debug(
                    "skipping running job %s: runner backend %s is not served here",
                    marker.job_key,
                    job.runner_backend,
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
            self._commit_published_outcome(marker, job, state, outcome_path)
            return True
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
            _LOGGER.debug(
                "leaving %s to its persistent writer: takeover is not proven safe",
                marker.job_key,
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
    ) -> None:
        """Move one job with a published outcome into committing."""

        try:
            self._begin_commit(marker, state, outcome_path)
        except TransitionLostError:
            pass
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
        changed = False
        for marker in self._window("resume_committing", "committing"):
            self._pace()
            loaded = self._load_job_and_state(marker, "resume_committing")
            if loaded is None:
                continue
            job, state = loaded
            if self._backend_for(job) is None:
                _LOGGER.debug(
                    "skipping committing job %s: runner backend %s is not served here",
                    marker.job_key,
                    job.runner_backend,
                )
                continue
            try:
                self._process_committing(marker)
            except TransitionLostError:
                pass
            except (FormatError, TransactionError) as exc:
                _LOGGER.error(
                    "commit of %s failed: %s",
                    marker.job_key,
                    exc,
                    extra=self._event("commit_failed", marker),
                )
                try:
                    self._transition(
                        marker,
                        "failed",
                        StateFrame.of(
                            state.carried(),
                            failure=self._failure("transaction_corruption", str(exc)),
                            reason="commit_failed",
                        ),
                    )
                except TransitionLostError:
                    pass
            except (WorkflowError, OSError) as exc:
                self._report_anomaly(
                    f"resume_committing:{marker.job_key}",
                    f"cannot resume the commit of {marker.job_key}: {exc}",
                    self._event("commit_error", marker),
                )
            changed = True
        return changed

    def _process_committing(self, marker: Marker) -> None:
        state = self._read_frame(marker)
        job = self.workspace.load_job(marker)
        outcome_path = self._outcome_path(marker, state)
        outcome = self._read_outcome(outcome_path / "outcome.json", marker, state)
        data_generation = state.data_generation
        transaction_path = outcome_path / "transaction"
        if transaction_path.is_dir():
            if job.data_mode != "transactional" or data_generation is None:
                raise TransactionError("transaction published by a nontransactional job")
            if outcome.get("expected_data_generation") != data_generation:
                raise TransactionError("outcome expected_data_generation is stale")
            # A durable workspace synchronizes every replayed destination and
            # the directories that name it here, before the destination frame is
            # appended and the marker is renamed out of committing below, so the
            # committed data is on storage before the marker that claims it is.
            changed_data = replay_transaction(
                transaction_path,
                self.workspace.payload_path(marker.placement, marker.job_key) / "data",
                expected_generation=data_generation,
                durable=self.workspace.durable,
            )
            if changed_data:
                data_generation += 1
        self._register_children(marker, state, outcome_path)
        backend = self._backend_for(job)
        if backend is None:
            return
        backend.commit_outcome(
            OutcomeCommit(
                job=job,
                marker=marker,
                payload=self.workspace.payload_path(marker.placement, marker.job_key),
                outcome_path=outcome_path,
                outcome=outcome,
            )
        )
        action = outcome["action"]
        progress = StateFrame.of(state.carried(), data_generation=data_generation)
        declared_steps = self._declared_runner_steps(marker, outcome)
        if declared_steps is not None:
            progress = StateFrame.of(progress, runner_steps=declared_steps)
        priority_raw = outcome.get("priority")
        next_priority = (
            marker.priority if priority_raw is None else require_int(priority_raw, "outcome.priority", maximum=999)
        )
        if action == "advance":
            self._advance(
                marker,
                job,
                state,
                validate_step(outcome.get("next_step"), "next_step"),
                progress,
                priority=next_priority,
            )
        elif action == "retry":
            reason = self._nested_reason(outcome, "retry")
            self._retry(marker, job, state, progress, reason, unclean=False, priority=next_priority)
        elif action == "wait":
            next_step = validate_step(outcome.get("next_step"), "next_step")
            join = outcome.get("join")
            if not isinstance(join, Mapping):
                raise FormatError("wait outcome requires a join object")
            self._transition(
                marker,
                "waiting",
                StateFrame.of(
                    progress,
                    next_step=next_step,
                    join=self._labeled_join(join, outcome_path),
                    reason="waiting_for_children",
                ),
                priority=next_priority,
            )
        elif action == "succeed":
            self._transition(
                marker,
                "succeeded",
                StateFrame.of(progress, reason="succeeded"),
                priority=next_priority,
            )
        elif action == "fail":
            # A runner-published failure is untrusted input. A malformed one is
            # itself a protocol violation, recorded as such instead of being
            # stored verbatim or discarded.
            try:
                failure = validate_failure(outcome.get("failure"))
            except FormatError as exc:
                failure = Failure("protocol_error", f"runner published a malformed failure object: {exc}")
                reason = "protocol_error"
            else:
                reason = "declared_failure"
                if failure.retryable and self._retry_budget_available(job, state):
                    # The runner that produced the failure is the authority on
                    # whether repeating the attempt can help; the manager still
                    # owns the budget, and the same activation is repeated.
                    _LOGGER.info(
                        "retrying %s: the runner declared %s retryable",
                        marker.job_key,
                        failure.code,
                        extra=self._event("declared_retry", marker, failure_code=failure.code),
                    )
                    self._retry(
                        marker,
                        job,
                        state,
                        progress,
                        failure.code,
                        unclean=False,
                        priority=next_priority,
                    )
                    return
            self._transition(
                marker,
                "failed",
                StateFrame.of(progress, failure=failure.as_mapping(), reason=reason),
                priority=next_priority,
            )
        elif action == "pause":
            self._transition(
                marker,
                "paused",
                StateFrame.of(progress, pause=outcome.get("pause"), reason="step_paused"),
                priority=next_priority,
            )
        else:
            raise FormatError(f"unsupported outcome action: {action!r}")

    def _declared_runner_steps(self, marker: Marker, outcome: Mapping[str, Any]) -> list[str] | None:
        """Return the step set a runner declared, or ``None`` to keep the old one.

        The member is advisory evidence for an operator and for tools that draw a
        job's reachable steps, never an input of a manager decision, so a
        malformed declaration is logged and dropped instead of failing the job.
        """

        declared = outcome.get("runner_steps")
        if declared is None:
            return None
        if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
            _LOGGER.warning("ignoring runner_steps of %s: not an array", marker.job_key)
            return None
        try:
            return [validate_step(item, "runner_steps item") for item in declared]
        except FormatError as exc:
            _LOGGER.warning("ignoring runner_steps of %s: %s", marker.job_key, exc)
            return None

    def _read_outcome(self, path: Path, marker: Marker, state: StateFrame) -> dict[str, Any]:
        outcome = read_json(path)
        if outcome.get("format") != "httk-workflow-outcome" or outcome.get("format_version") != 1:
            raise FormatError("outcome must use httk-workflow-outcome version 1")
        for key, expected in (
            ("job_id", marker.job_id),
            ("activation_id", state.activation_id),
            ("attempt_id", state.attempt_id),
        ):
            if outcome.get(key) != expected:
                raise FormatError(f"outcome {key} disagrees with current attempt")
        if not isinstance(outcome.get("action"), str):
            raise FormatError("outcome action must be a string")
        return outcome

    def _child_digests(self, outcome_path: Path) -> dict[str, str]:
        jobs_dir = outcome_path / "children" / "jobs"
        if not jobs_dir.is_dir():
            return {}
        return {
            path.name: tree_digest(path, skip=is_payload_private)
            for path in sorted(jobs_dir.iterdir())
            if path.is_dir()
        }

    def _spawn_labels(self, outcome_path: Path) -> dict[str, str]:
        """Return the label of every spawned child, keyed by child job key.

        Every entry of a spawn set must carry a nonempty label that is unique
        within that set. A gather step selects its inputs by label, so an unnamed
        or ambiguous child would leave the parent's own join unusable, and the
        published outcome is rejected as a protocol error instead.
        """

        spawn_path = outcome_path / "children" / "spawn.json"
        if not spawn_path.is_file():
            return {}
        entries = read_json(spawn_path).get("children")
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
            raise FormatError("spawn children must be an array")
        labels: dict[str, str] = {}
        used: set[str] = set()
        for raw in entries:
            if not isinstance(raw, Mapping):
                raise FormatError("spawn child must be an object")
            job_key = require_string(raw.get("job_key"), "spawn child job_key")
            label = validate_label(raw.get("label"), "spawn child label")
            if label in used:
                raise FormatError(f"spawn child label is not unique within the spawn set: {label}")
            used.add(label)
            labels[job_key] = label
        return labels

    def _labeled_join(self, join: Mapping[str, Any], outcome_path: Path) -> dict[str, object]:
        """Return *join* with every child reference carrying its spawn label.

        A label is declared once, in the spawn set of the very outcome that
        registers the children a join names. Copying it into the waiting frame
        keeps join resolution a pure read of authoritative state, and keeps an
        explicit label in a reference authoritative over the derived one.
        """

        labels = self._spawn_labels(outcome_path)
        children = join.get("children")
        if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
            # Labeling is purely additive: a malformed join is recorded exactly
            # as the runner published it, and join evaluation reports it as that
            # runner's protocol error rather than as a commit failure here.
            return dict(join)
        referenced: list[object] = []
        for raw in children:
            if not isinstance(raw, Mapping):
                referenced.append(raw)
                continue
            reference = dict(raw)
            if reference.get("label") is None:
                label = labels.get(str(reference.get("job_key", "")))
                if label is not None:
                    reference["label"] = label
            referenced.append(reference)
        return {**join, "children": referenced}

    def _register_children(self, marker: Marker, state: StateFrame, outcome_path: Path) -> None:
        children_dir = outcome_path / "children"
        if not children_dir.is_dir():
            return
        spawn = read_json(children_dir / "spawn.json")
        entries = spawn.get("children")
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
            raise FormatError("spawn children must be an array")
        # Registration is the point of no return for a spawn set, so its labels
        # are revalidated here even though publication already checked them.
        self._spawn_labels(outcome_path)
        expected_digests = state.child_digests
        if expected_digests is None and state.has("child_digests"):
            raise FormatError("committing child_digests must be an object")
        expected_digests = {} if expected_digests is None else expected_digests
        for raw in entries:
            if not isinstance(raw, Mapping):
                raise FormatError("spawn child must be an object")
            job_key = str(raw.get("job_key", ""))
            placement = normalize_placement(str(raw.get("placement", "")))
            if raw.get("workspace_id", self.workspace.workspace_id) != self.workspace.workspace_id:
                raise UnsupportedExtensionError("cross-workspace child requires multiworkspace-v1")
            source = children_dir / "jobs" / job_key
            expected_digest = str(expected_digests.get(job_key, ""))
            target = self.workspace.payload_path(placement, job_key)
            published_here = False
            if source.is_dir():
                child = JobDefinition.from_mapping(read_json(source / "job.json"))
                if child.job_key != job_key:
                    raise FormatError("spawn job_key disagrees with child job.json")
                # A registered child may already be running, so its own attempt
                # control directory and job state are excluded here exactly as
                # they are from the digest recorded before publication.
                if tree_digest(source, skip=is_payload_private) != expected_digest:
                    raise FormatError("spawn child changed after outcome publication")
                target.parent.mkdir(parents=True, exist_ok=True)
                self.workspace._publish_path(source, target)
                # Publication is a rename of the very tree just verified, so
                # hashing the destination again would only re-read the same
                # bytes. The digest verified above is the digest of *target*.
                published_here = True
            if not target.is_dir():
                raise FormatError(f"registered child bundle does not match: {job_key}")
            if self.workspace.find_marker_at(job_key, placement) is not None:
                # This child is already registered. Registration is the point of
                # no return for a spawn set, so from the instant its marker
                # exists the child is a job in its own right: another manager
                # may already have claimed it, and its payload then holds a
                # workdir the spawning outcome never published. The marker is
                # the proof registration completed, and re-hashing a payload
                # whose owner has moved on would fail a resumed commit for doing
                # exactly what registration allows.
                continue
            if not published_here and tree_digest(target, skip=is_payload_private) != expected_digest:
                raise FormatError(f"registered child bundle does not match: {job_key}")
            child = JobDefinition.from_mapping(read_json(target / "job.json"))
            temporary = self.workspace.control / "tmp" / f"child-marker.{uuid.uuid4()}"
            temporary.touch(exist_ok=False)
            destination = self.workspace.marker_path("submitted", placement, job_key, child.priority, 0, "init")
            self.workspace._publish_path(temporary, destination)

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
        activation_ordinal = (state.activation_ordinal if state.activation_ordinal is not None else 1) + 1
        maximum = job.retry_policy.maximum_activations
        if maximum is not None and activation_ordinal > maximum:
            self._transition(
                marker,
                "failed",
                StateFrame.of(
                    progress,
                    failure=self._failure("budget_exhausted", "maximum_activations exceeded"),
                    reason="budget_exhausted",
                ),
            )
            return
        self._transition(
            marker,
            "ready",
            StateFrame.of(
                progress,
                step=next_step,
                activation_id=str(uuid.uuid4()),
                activation_ordinal=activation_ordinal,
                attempt_ordinal=0,
                reason=reason,
                join_summary=join_summary,
                previous_attempt_id=state.attempt_id,
            ),
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
    ) -> None:
        current_attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
        maximum = job.retry_policy.maximum_attempts_per_activation
        if maximum is not None and current_attempts >= maximum:
            self._transition(
                marker,
                "failed",
                StateFrame.of(
                    progress,
                    failure=self._failure("retry_exhausted", reason),
                    reason="retry_exhausted",
                ),
            )
            return
        evidence = {} if takeover_evidence is None else dict(takeover_evidence)
        retried = StateFrame.of(
            progress,
            reason=reason,
            unclean_restart=unclean,
            unsafe_persistent_takeover=evidence.get("evidence") == "unsafe_persistent_takeover",
            previous_attempt_id=state.attempt_id,
        )
        if takeover_evidence is not None:
            retried = StateFrame.of(retried, takeover_evidence=evidence)
        self._transition(marker, "ready", retried, priority=priority)

    def _retry_budget_available(self, job: JobDefinition, state: StateFrame) -> bool:
        """Report whether another attempt of this activation is still permitted."""

        attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
        total = state.total_attempts if state.total_attempts is not None else attempts
        per_activation = job.retry_policy.maximum_attempts_per_activation
        if per_activation is not None and attempts >= per_activation:
            return False
        maximum_total = job.retry_policy.maximum_total_attempts
        return maximum_total is None or total < maximum_total

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

        _LOGGER.warning(
            "attempt of %s failed with %s: %s",
            marker.job_key,
            code,
            message,
            extra=self._event("attempt_failure", marker, failure_code=code, exit_status=exit_status),
        )
        try:
            state = self._read_frame(marker)
            progress = state.carried()
            # Retry policy is keyed on the failure code, which is exactly the
            # string a job lists in retry_policy.retry_on.
            if code in job.retry_policy.retry_on:
                self._retry(
                    marker,
                    job,
                    state,
                    progress,
                    code,
                    unclean=unclean,
                    takeover_evidence=takeover_evidence,
                )
                return
            failed = StateFrame.of(
                progress,
                failure=self._failure(code, message, exit_status=exit_status),
                reason=code,
            )
            if takeover_evidence is not None:
                failed = StateFrame.of(failed, takeover_evidence=dict(takeover_evidence))
            self._transition(marker, "failed", failed)
        except TransitionLostError:
            _LOGGER.debug("failure record for %s was lost to another actor", marker.job_key)
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"failure:{marker.job_key}",
                f"cannot record the {code} failure of {marker.job_key}: {exc}",
                self._event("failure_error", marker, failure_code=code),
            )

    def _recover_abandoned_claims(self) -> bool:
        changed = False
        for marker in list(self._walk(("claimed",))):
            self._pace()
            loaded = self._load_job_and_state(marker, "recover_claims")
            if loaded is None:
                continue
            job, state = loaded
            if self._backend_for(job) is None:
                _LOGGER.debug(
                    "skipping claimed job %s: runner backend %s is not served here",
                    marker.job_key,
                    job.runner_backend,
                )
                continue
            try:
                owner = state.manager_id
            except FormatError as exc:
                self._report_anomaly(
                    f"claim_owner:{marker.job_key}",
                    f"claimed job {marker.job_key} does not name a usable manager: {exc}",
                    self._event("protocol_error", marker),
                )
                owner = None
            if self._manager_alive(
                owner,
                lease_seconds=self.lease_seconds if state.lease_seconds is None else state.lease_seconds,
            ):
                continue
            _LOGGER.warning(
                "recovering %s: the claim of manager %s was abandoned",
                marker.job_key,
                owner or "-",
                extra=self._event("claim_recovered", marker, previous_manager=owner),
            )
            self._release_claim(marker, "claim_abandoned", state)
            changed = True
        return changed

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
        children = join.get("children")
        if not isinstance(children, Sequence) or isinstance(children, (str, bytes)) or not children:
            raise FormatError("state.join.children must be a nonempty array")
        return children

    def _observe_join_children(self, children: Sequence[object]) -> tuple[list[dict[str, object]], str | None]:
        """Observe every join child, reporting the first unresolvable child id.

        One observation is the complete typed record of a child a gather step is
        allowed to depend on: its label, its terminal state kind, the failure it
        published if it ended badly, and the workspace-relative payload and
        workdir paths through which its results are read.
        """

        observations: list[dict[str, object]] = []
        for child_ref in children:
            reference = require_mapping(child_ref, "join child")
            child_id = canonical_uuid(reference.get("job_id"), "join child job_id")
            label_raw = reference.get("label")
            label = None if label_raw is None else validate_label(label_raw, "join child label")
            placement_hint = reference.get("placement_hint")
            job_key = reference.get("job_key")
            if not isinstance(placement_hint, str) or not isinstance(job_key, str):
                # Placement is required in a child reference: join evaluation
                # probes the child's exact state set and never falls back to a
                # whole-workspace scan inside a tick. A reference that lacks it —
                # only an old workspace can produce one — is a protocol error of
                # whatever wrote it, not a reason to rescan the tree per child.
                raise FormatError("join child reference must carry a job_key and placement_hint")
            child_marker = self.workspace.find_marker_at(job_key, normalize_placement(placement_hint))
            if child_marker is not None and child_marker.job_id != child_id:
                raise FormatError("join child identity disagrees with placement hint")
            if child_marker is None:
                return observations, child_id
            observations.append(
                {
                    "workspace_id": self.workspace.workspace_id,
                    "job_id": child_id,
                    "job_key": child_marker.job_key,
                    "label": label,
                    "placement": child_marker.placement.as_posix(),
                    "kind": child_marker.kind,
                    "state_generation": child_marker.generation,
                    "record_ref": child_marker.record_ref,
                    "payload_path": (child_marker.placement / child_marker.job_key).as_posix(),
                    **self._child_evidence(child_marker),
                }
            )
        return observations, None

    def _child_evidence(self, marker: Marker) -> dict[str, object]:
        """Return the terminal evidence a gather step needs about one child.

        Everything is derived from authoritative state, and every path is
        workspace relative so an observation survives moving the workspace.
        Evidence that cannot be read is reported as null instead of failing the
        parent: a gather step must still be able to see that a child ended badly
        when it is exactly the child's own payload that is damaged.
        """

        payload = marker.placement / marker.job_key
        try:
            state = self._read_frame(marker)
        except (WorkflowError, OSError) as exc:
            _LOGGER.debug("cannot read the state frame of join child %s: %s", marker.job_key, exc)
            state = StateFrame()
        failure: object = None
        if marker.kind in {"failed", "cancelled"}:
            raw = state.failure
            if raw is not None:
                try:
                    failure = validate_failure(raw).as_mapping()
                except FormatError:
                    # Evidence of a malformed record is still evidence.
                    failure = dict(raw)
        return {
            "failure": failure,
            "workdir_path": self._child_workdir_path(marker, state, payload),
            "data_generation": state.data_generation,
        }

    def _child_workdir_path(
        self,
        marker: Marker,
        state: StateFrame,
        payload: PurePosixPath,
    ) -> str | None:
        """Return the workspace-relative workdir of one observed child."""

        recorded = state.workdir
        try:
            job = self.workspace.load_job(marker)
        except (WorkflowError, OSError):
            return (payload / PurePosixPath(recorded)).as_posix() if recorded else None
        if job.workdir_mode == "persistent":
            return (payload / job.workdir_path).as_posix()
        attempt_id = state.attempt_id
        if attempt_id is not None:
            # An isolated workdir belongs to one attempt, so the last attempt
            # recorded in the child's own state frame names the last workdir.
            base = job.workdir_path
            return (payload / base.parent / f"{base.name}.{attempt_id}").as_posix()
        return (payload / PurePosixPath(recorded)).as_posix() if recorded else None

    def _join_child_grace_expired(self, marker: Marker, child_id: str) -> bool:
        """Report whether one unresolvable join child has outlived its grace.

        Children are registered before their parent leaves committing, and a
        child named by an unresolved core-v1 join must not relocate, so the
        complete workspace scan behind :meth:`_observe_join_children` is the
        authoritative answer. A brief absence is therefore only explainable by
        metadata visibility, which the protocol bounds with retries; a
        persistent absence is a corrupt dependency rather than an impossible
        join condition, and the parent must not wait for it forever. The
        first-unresolved instant is kept in memory only: a restarted manager
        restarts the grace, which is safe because the grace exists purely to
        absorb transient nonvisibility.
        """

        now = time.monotonic()
        recorded = self._join_unresolved.get(marker.job_key)
        if recorded is None or recorded[0] != child_id:
            recorded = (child_id, now)
            self._join_unresolved[marker.job_key] = recorded
        if now - recorded[1] < self.join_grace_seconds:
            return False
        del self._join_unresolved[marker.job_key]
        return True

    def _fail_waiting(
        self,
        marker: Marker,
        state: StateFrame,
        code: str,
        message: str,
        reason: str,
    ) -> None:
        self._join_unresolved.pop(marker.job_key, None)
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
            if self._backend_for(parent_job) is None:
                _LOGGER.debug(
                    "skipping waiting job %s: runner backend %s is not served here",
                    marker.job_key,
                    parent_job.runner_backend,
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
        """Observe one waiting job's join and act when it resolves."""

        join = require_mapping(state.join, "state.join")
        observations, unresolved = self._observe_join_children(self._join_children(join))
        if unresolved is not None:
            if not self._join_child_grace_expired(marker, unresolved):
                _LOGGER.debug("join child %s of %s is not yet visible", unresolved, marker.job_key)
                return False
            self._fail_waiting(
                marker,
                state,
                "dependency_failure",
                f"join child {unresolved} cannot be resolved in this workspace",
                "join_unresolvable",
            )
            return True
        self._join_unresolved.pop(marker.job_key, None)
        condition = str(join.get("condition", ""))
        kinds = [str(item["kind"]) for item in observations]
        satisfied = self._join_satisfied(condition, join, kinds)
        impossible = self._join_impossible(condition, join, kinds)
        if not satisfied and not impossible:
            _LOGGER.debug("join %s of %s is still pending: children are %s", condition, marker.job_key, kinds)
            return False
        if satisfied:
            self._advance(
                marker,
                parent_job,
                state,
                validate_step(state.next_step, "next_step"),
                state.carried(),
                reason="join_satisfied",
                join_summary=observations,
            )
            return True
        on_impossible = join.get("on_impossible")
        if isinstance(on_impossible, Mapping) and on_impossible.get("action") == "advance":
            self._advance(
                marker,
                parent_job,
                state,
                validate_step(on_impossible.get("next_step"), "on_impossible.next_step"),
                state.carried(),
                reason="join_impossible",
                join_summary=observations,
            )
            return True
        self._transition(
            marker,
            "failed",
            StateFrame.of(
                state.carried(),
                failure=self._failure("dependency_failure", "join condition became impossible"),
                join_summary=observations,
                reason="join_impossible",
            ),
        )
        return True

    @staticmethod
    def _join_satisfied(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> bool:
        if condition == "all_succeeded":
            return all(kind == "succeeded" for kind in kinds)
        if condition == "all_terminal":
            return all(kind in TERMINAL_KINDS for kind in kinds)
        if condition == "any_succeeded":
            return any(kind == "succeeded" for kind in kinds)
        if condition == "at_least":
            count = int(join.get("count", 0))
            return sum(kind == "succeeded" for kind in kinds) >= count
        raise FormatError(f"unknown join condition: {condition!r}")

    @staticmethod
    def _join_impossible(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> bool:
        if condition == "all_succeeded":
            return any(kind in {"failed", "cancelled"} for kind in kinds)
        if condition == "all_terminal":
            return False
        if condition == "any_succeeded":
            return all(kind in TERMINAL_KINDS for kind in kinds) and not any(kind == "succeeded" for kind in kinds)
        if condition == "at_least":
            count = int(join.get("count", 0))
            successes = sum(kind == "succeeded" for kind in kinds)
            nonterminal = sum(kind not in TERMINAL_KINDS for kind in kinds)
            return successes + nonterminal < count
        raise FormatError(f"unknown join condition: {condition!r}")

    def _resolve_request_marker(self, request: Mapping[str, Any]) -> Marker | None:
        """Resolve the job one operator request names, by its exact placement.

        A request carries the job's placement exactly as a join child reference
        does, so the manager probes the finite state set at that placement and
        never rescans the workspace from a scheduling tick. A request that lacks
        a placement — only a pre-release client can produce one — is a protocol
        error that is claimed and quarantined, not a reason to fall back to a
        global lookup.
        """

        job_key = request.get("job_key")
        placement = request.get("placement")
        if not isinstance(job_key, str) or not isinstance(placement, str):
            raise FormatError("request must carry a job_key and placement")
        return self.workspace.find_marker_at(job_key, normalize_placement(placement))

    def _handle_requests(self) -> bool:
        ready_dir = self.workspace.control / "requests" / "ready"
        changed = False
        for request_path in sorted(ready_dir.iterdir()) if ready_dir.exists() else ():
            if not request_path.is_file() or request_path.name in self._deferred_requests:
                continue
            self._pace()
            try:
                request = read_json(request_path)
                marker = self._resolve_request_marker(request)
                if marker is not None:
                    job = self.workspace.load_job(marker)
                    if self._backend_for(job) is None:
                        # Another manager may serve that backend, so the request
                        # stays where it is — but this manager has now decided
                        # about it and never reads it again.
                        _LOGGER.info(
                            "leaving request %s to another manager: runner backend %s is not served here",
                            request_path.name,
                            job.runner_backend,
                            extra=self._event(
                                "request_deferred",
                                request=request_path.name,
                                runner_backend=job.runner_backend,
                            ),
                        )
                        self._deferred_requests.add(request_path.name)
                        continue
            except (WorkflowError, OSError):
                # Claim malformed requests so one manager can quarantine them.
                pass
            claimed_dir = self.workspace.control / "requests" / "claimed" / self.manager_id
            claimed_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claimed_dir / request_path.name
            try:
                os.rename(request_path, claimed_path)
            except OSError as exc:
                _LOGGER.debug("request %s was claimed elsewhere: %s", request_path.name, exc)
                continue
            try:
                unactionable = self._apply_request(read_json(claimed_path))
            except TransitionLostError:
                self._retire_request(claimed_path, "the job moved to another state first")
            except (WorkflowError, OSError, ValueError) as exc:
                try:
                    self.workspace.quarantine(claimed_path, reason=f"invalid request: {exc}")
                except OSError as failure:
                    self._report_anomaly(
                        f"request:{claimed_path.name}",
                        f"cannot quarantine the invalid request {claimed_path.name}: {failure}",
                        self._event("request_error", request=claimed_path.name),
                    )
            else:
                if unactionable is not None:
                    self._retire_request(claimed_path, unactionable)
                else:
                    _LOGGER.info(
                        "handled request %s",
                        claimed_path.name,
                        extra=self._event("request_handled", request=claimed_path.name),
                    )
                    claimed_path.unlink(missing_ok=True)
            changed = True
        return changed

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
        """Verify the optional operator signature and return the key that made it.

        The signature is attribution, not authorization. A request that carries
        none is applied exactly as before — that is what lets an installation
        without an identity key, or one that predates signing, keep working — but
        a signature that is present and does not verify is refused, because a
        damaged or forged attribution is worse than none.
        """

        signature = verify_document(request)
        if not signature.present:
            _LOGGER.debug(
                "request %s carries no operator signature",
                request.get("request_id"),
                extra=self._event("request_unsigned", request_id=str(request.get("request_id"))),
            )
            return None
        if not signature.valid:
            raise FormatError(f"operator signature is invalid: {signature.reason}")
        _LOGGER.info(
            "request %s is signed by %s",
            request.get("request_id"),
            signature.operator_key,
            extra=self._event(
                "request_signature_verified",
                request_id=str(request.get("request_id")),
                operator_key=signature.operator_key,
            ),
        )
        return signature.operator_key

    def _apply_request(self, request: Mapping[str, Any]) -> str | None:
        """Apply one operator request, or say why it can never be applied."""

        if request.get("format") != "httk-workflow-request" or request.get("format_version") != 1:
            raise FormatError("request must use httk-workflow-request version 1")
        operator_key = self._request_operator_key(request)
        marker = self._resolve_request_marker(request)
        if marker is None:
            raise FormatError("request job does not exist")
        if marker.generation != int(request.get("expected_generation", -1)):
            return f"the job is at generation {marker.generation}, not the expected one"
        if marker.record_ref != request.get("expected_record_ref"):
            return "the job state changed after the request was published"
        action = request.get("action")
        state = self._read_frame(marker)
        job = self.workspace.load_job(marker)
        audit = StateFrame.of(
            state.carried(),
            operator=request.get("operator"),
            operator_reason=request.get("reason"),
            request_id=request.get("request_id"),
        )
        if operator_key is not None:
            audit = StateFrame.of(audit, operator_key=operator_key)
        if action == "cancel" and marker.kind not in TERMINAL_KINDS:
            return self._request_cancel(marker, state, request, operator_key)
        if action == "set_priority" and marker.kind in {"submitted", "ready", "waiting", "paused", "failed"}:
            priority = int(request.get("priority", -1))
            # A repriced job keeps the same state kind, so the members that make
            # that kind meaningful must survive the new frame. Dropping a join
            # would leave a waiting job with an unusable dependency record.
            preserved = state.select(("join", "join_summary", "next_step", "pause", "failure"))
            self._transition(
                marker,
                marker.kind,
                StateFrame.of(StateFrame({**audit.members, **preserved.members}), reason="operator_priority"),
                priority=priority,
            )
            return None
        if action == "pause" and marker.kind in {"submitted", "ready", "waiting"}:
            self._transition(marker, "paused", StateFrame.of(audit, reason="operator_pause"))
            return None
        if action in {"continue", "override_step"} and marker.kind in {"failed", "paused"}:
            hazard = self._decided_join_hazard(marker, job)
            if hazard is not None:
                if not bool(request.get("force")):
                    return (
                        f"this job was already consumed by the decided join of parent "
                        f"{hazard['parent_job_key']}, which observed it as {hazard['observed_kind']}; "
                        "reviving it now races the parent reading its outputs. Republish the request "
                        "with force to accept that hazard, or continue the parent instead"
                    )
                audit = StateFrame.of(audit, revival_hazard=hazard)
                _LOGGER.warning(
                    "forced revival of %s: its parent %s already decided a join on it",
                    marker.job_key,
                    hazard["parent_job_key"],
                    extra=self._event("revival_hazard", marker, **hazard),
                )
        if action == "continue" and marker.kind in {"failed", "paused"}:
            if state.activation_id is None:
                raise FormatError("job has no runnable activation to continue")
            self._retry(marker, job, state, audit, "manual_continue", unclean=False)
            return None
        if action == "override_step" and marker.kind in {"failed", "paused"}:
            self._advance(
                marker,
                job,
                state,
                validate_step(request.get("step"), "request.step"),
                audit,
                reason="operator_override_step",
            )
            return None
        raise FormatError(f"request action {action!r} is invalid from state {marker.kind}")

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
        """Fence one cancelled job, before anything is signalled or verified.

        Order is the whole point. A `running` job first moves to `cancelling`,
        which renames its marker and therefore fences the attempt: whatever that
        attempt does next, no manager will accept an outcome from it. Only then
        is its process group stopped, and only a verified exit moves the job to
        `cancelled`. A job with no live attempt is cancelled directly, with the
        same evidence member saying that there was nothing to stop.
        """

        audited = StateFrame.of(
            state.select(_CANCELLING_MEMBERS),
            operator=request.get("operator"),
            operator_reason=request.get("reason"),
            request_id=request.get("request_id"),
        )
        if operator_key is not None:
            audited = StateFrame.of(audited, operator_key=operator_key)
        if marker.kind == "cancelling":
            return "the job is already being cancelled"
        if marker.kind == "running":
            fenced = self._transition(marker, "cancelling", StateFrame.of(audited, reason="operator_cancel"))
            _LOGGER.warning(
                "cancelling %s: attempt %s is fenced and will be stopped",
                fenced.job_key,
                state.attempt_id or "-",
                extra=self._event("cancel_fenced", fenced, attempt_id=state.attempt_id),
            )
            local = self._running.get(state.attempt_id or "")
            if local is not None:
                local.cancelling = True
                local.fenced = True
            return None
        # Every other nonterminal state either never launched an attempt or has
        # already fenced it by moving its marker, so there is nothing left that
        # a later outcome could commit.
        self._terminate_attempt(marker, state)
        self._transition(
            marker,
            "cancelled",
            StateFrame.of(
                audited,
                cancellation={"verified": "no_live_attempt", "from_kind": marker.kind, "verified_at": utc_now()},
                reason="operator_cancel",
            ),
        )
        return None

    def _process_cancelling(self) -> bool:
        """Stop, verify, and finish every cancellation this manager can.

        This is both the live path and the recovery path: a `cancelling` marker
        left behind by a manager that died mid-cancellation is indistinguishable
        from one this manager fenced a moment ago, and gets exactly the same
        terminate, verify, and finish treatment.
        """

        changed = False
        for marker in self._window("process_cancelling", "cancelling"):
            self._pace()
            loaded = self._load_job_and_state(marker, "process_cancelling")
            if loaded is None:
                continue
            job, state = loaded
            if self._backend_for(job) is None:
                _LOGGER.debug(
                    "skipping cancelling job %s: runner backend %s is not served here",
                    marker.job_key,
                    job.runner_backend,
                )
                continue
            try:
                changed |= self._finish_cancellation(marker, state)
            except TransitionLostError:
                _LOGGER.debug("cancellation of %s was completed by another actor", marker.job_key)
            except (WorkflowError, OSError) as exc:
                self._report_anomaly(
                    f"cancelling:{marker.job_key}",
                    f"cannot complete the cancellation of {marker.job_key}: {exc}",
                    self._event("cancel_error", marker),
                )
        return changed

    def _finish_cancellation(self, marker: Marker, state: StateFrame) -> bool:
        """Terminate the fenced attempt of one cancelling job and finish it."""

        attempt_id = state.attempt_id or ""
        now = time.monotonic()
        kill_at = self._cancel_kill_at.get(attempt_id)
        if kill_at is None:
            self._cancel_kill_at[attempt_id] = now + self.cancel_grace_seconds
            self._terminate_attempt(marker, state)
        elif now >= kill_at:
            _LOGGER.warning(
                "cancelled attempt %s of %s did not exit within %.0fs; killing its process group",
                attempt_id or "-",
                marker.job_key,
                self.cancel_grace_seconds,
                extra=self._event("cancel_kill", marker, attempt_id=attempt_id),
            )
            self._terminate_attempt(marker, state, signal.SIGKILL)
        evidence = self._cancellation_evidence(marker, state)
        if evidence is None:
            self._report_unverifiable_cancellation(marker, state)
            return False
        local = self._running.pop(attempt_id, None)
        if local is not None:
            local.close_logs()
        self._cancel_kill_at.pop(attempt_id, None)
        self._cancel_unverified.discard(attempt_id)
        self._transition(
            marker,
            "cancelled",
            StateFrame.of(
                state.select(_CANCELLING_MEMBERS),
                cancellation=evidence,
                reason="operator_cancel",
            ),
        )
        _LOGGER.warning(
            "cancelled %s: attempt %s is %s",
            marker.job_key,
            attempt_id or "-",
            evidence.get("verified"),
            extra=self._event("cancel_completed", marker, attempt_id=attempt_id, **evidence),
        )
        return True

    def _cancellation_evidence(self, marker: Marker, state: StateFrame) -> dict[str, object] | None:
        """Return proof that the fenced attempt has stopped, or ``None``.

        A cancelled job asserts that nothing is still writing its workdir, so it
        may only be published against evidence. A process this manager launched
        is proven gone by reaping it; one launched by another manager on this
        host is proven gone when its process group no longer exists; a process
        recorded on a different host cannot be proven anything about here.
        """

        attempt_id = state.attempt_id or ""
        local = self._running.get(attempt_id)
        if local is not None:
            exit_status = local.process.poll()
            if exit_status is None:
                return None
            return {
                "verified": "process_exited",
                "hostname": self.hostname,
                "pid": local.process.pid,
                "exit_status": exit_status,
                "verified_at": utc_now(),
            }
        try:
            process = read_json(self._attempt_control_path(marker, state) / "process.json")
        except (FormatError, WorkflowError):
            # The launch gate records the process identity before the running
            # marker moves, so a fenced attempt with no record never reached its
            # runner and there is nothing left to stop.
            return {"verified": "no_launched_process", "verified_at": utc_now()}
        hostname = process.get("hostname")
        if hostname != self.hostname:
            return None
        try:
            group = int(process["process_group"])
        except (KeyError, TypeError, ValueError):
            return {"verified": "no_launched_process", "verified_at": utc_now()}
        if self._process_group_alive(group):
            return None
        return {
            "verified": "process_group_absent",
            "hostname": self.hostname,
            "process_group": group,
            "verified_at": utc_now(),
        }

    def _report_unverifiable_cancellation(self, marker: Marker, state: StateFrame) -> None:
        """Record once why one cancellation cannot be finished here.

        The marker stays in `cancelling`, which keeps the attempt fenced, and
        the reason is journaled so that an operator reading the job's history
        sees a stalled cancellation rather than silence. Every following tick
        retries it, and the manager that can see the process finishes it.
        """

        attempt_id = state.attempt_id or ""
        if attempt_id in self._cancel_unverified:
            _LOGGER.debug("cancellation of %s is still unverifiable here", marker.job_key)
            return
        self._cancel_unverified.add(attempt_id)
        detail = "its process is recorded on another host, so this manager cannot prove that it stopped"
        _LOGGER.warning(
            "cancellation of %s is not finished: %s",
            marker.job_key,
            detail,
            extra=self._event("cancel_unverified", marker, attempt_id=attempt_id),
        )
        try:
            self._transition(
                marker,
                "cancelling",
                StateFrame.of(
                    state.select(_CANCELLING_MEMBERS),
                    cancellation={
                        "verified": None,
                        "problem": "unverifiable_here",
                        "detail": detail,
                        "observed_by": self.manager_id,
                        "observed_at": utc_now(),
                    },
                    reason="cancel_unverified",
                ),
            )
        except TransitionLostError:
            _LOGGER.debug("cancellation note for %s was lost to another actor", marker.job_key)

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

        details = None if exit_status is None else {"exit_status": exit_status}
        return Failure(code, message, details=details).as_mapping()

    @staticmethod
    def _nested_reason(outcome: Mapping[str, Any], key: str) -> str:
        value = outcome.get(key)
        if not isinstance(value, Mapping) or not isinstance(value.get("reason"), str):
            raise FormatError(f"{key} outcome requires a reason")
        return str(value["reason"])

    @staticmethod
    def _attempt_budget_failure(job: JobDefinition, attempt_ordinal: int, total_attempts: int) -> str | None:
        per_activation = job.retry_policy.maximum_attempts_per_activation
        if per_activation is not None and attempt_ordinal > per_activation:
            return "maximum_attempts_per_activation exceeded"
        total = job.retry_policy.maximum_total_attempts
        if total is not None and total_attempts > total:
            return "maximum_total_attempts exceeded"
        return None
