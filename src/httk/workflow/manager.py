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
from .errors import (
    FormatError,
    RunnerResolutionError,
    TransactionError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .journal import JournalWriter
from .manifests import read_maintenance_lock
from .models import (
    CORE_PROFILE,
    TERMINAL_KINDS,
    Failure,
    JobDefinition,
    Marker,
    canonical_uuid,
    is_payload_private,
    normalize_placement,
    parse_package_runner,
    validate_failure,
    validate_label,
    validate_step,
)
from .transactions import replay_transaction
from .workspace import WorkflowWorkspace

_LOGGER = logging.getLogger(__name__)
_DRAIN_SIGNALS = (signal.SIGTERM, signal.SIGINT)
DEFAULT_RUNNER_MODULES = ("httk.workflow",)
# The entry point of a staged runner tree. A single file is the common case; a
# tree is pinned and staged as a whole, and this is the one name inside it the
# manager will execute.
RUNNER_TREE_ENTRY = "run"


@dataclass
class RunningAttempt:
    marker: Marker
    process: subprocess.Popen[bytes]
    stdout: BinaryIO
    stderr: BinaryIO
    attempt_id: str

    def close_logs(self) -> None:
        self.stdout.close()
        self.stderr.close()


class TaskManager:
    """Execute and recover jobs in one workflow workspace."""

    def __init__(
        self,
        workspace: WorkflowWorkspace,
        *,
        pools: Sequence[str] = ("default",),
        capabilities: Sequence[str] = (),
        maximum_workers: int = 1,
        lease_seconds: float = 900.0,
        heartbeat_interval: float = 30.0,
        unsafe_persistent_takeover: bool = False,
        runner_backends: Sequence[RunnerBackend] = (),
        allowed_backends: Sequence[str] | None = None,
        accept_any_pool: bool = False,
        join_grace_seconds: float = 3600.0,
        runner_search_paths: Iterable[str | os.PathLike[str]] = (),
        runner_modules: Iterable[str] = DEFAULT_RUNNER_MODULES,
    ) -> None:
        if maximum_workers < 1:
            raise ValueError("maximum_workers must be positive")
        if join_grace_seconds < 0:
            raise ValueError("join_grace_seconds cannot be negative")
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
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.unsafe_persistent_takeover = unsafe_persistent_takeover
        self.join_grace_seconds = join_grace_seconds
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
        self.writer = JournalWriter(workspace.control, durable=workspace.durable)
        self._manager_dir = workspace.control / "managers" / self.manager_id
        self._manager_dir.mkdir(parents=True, exist_ok=False)
        self._running: dict[str, RunningAttempt] = {}
        # Parent job key -> (unresolvable child job id, monotonic first-seen).
        self._join_unresolved: dict[str, tuple[str, float]] = {}
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

    def heartbeat(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_heartbeat < self.heartbeat_interval:
            return
        write_json_atomic(
            self._manager_dir / "heartbeat.json",
            {"manager_id": self.manager_id, "updated_at": utc_now()},
            durable=self.workspace.durable,
        )
        self._last_heartbeat = now

    def tick(self) -> bool:
        """Perform one nonblocking scheduling and recovery pass."""

        self.heartbeat()
        changed = False
        changed |= self._handle_requests()
        changed |= self._register_submissions()
        changed |= self._resume_committing()
        changed |= self._evaluate_joins()
        changed |= self._poll_running()
        changed |= self._recover_abandoned_claims()
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
        updates: Mapping[str, object],
        *,
        priority: int | None = None,
    ) -> Marker:
        moved = self.workspace.transition(self.writer, marker, kind, updates, priority=priority)
        _LOGGER.info(
            "job %s moved from %s to %s (reason %s)",
            moved.job_key,
            marker.kind,
            kind,
            updates.get("reason", "-"),
            extra=self._event("transition", moved, previous_kind=marker.kind, reason=updates.get("reason")),
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
        for marker in self.workspace.scan_markers(("submitted", "ready", "committing")):
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

    def _load_job_and_state(self, marker: Marker, pass_name: str) -> tuple[JobDefinition, dict[str, Any]] | None:
        """Load one job and its state frame, skipping and reporting damage.

        A job whose ``job.json`` or state frame cannot be read is a local
        defect of that job. Reporting it and continuing keeps one damaged job
        from stopping every other job in the workspace. Core-v1 leaves the
        repair of such a payload to an operator, so nothing is moved: the
        authoritative marker stays exactly where it is.
        """

        try:
            job = self.workspace.load_job(marker)
            state = self.workspace.read_state(marker)
        except (WorkflowError, OSError) as exc:
            self._report_anomaly(
                f"{pass_name}:{marker.job_key}",
                f"skipping {marker.kind} job {marker.job_key} during {pass_name}: {exc}",
                self._event("job_unusable", marker, pass_name=pass_name),
            )
            return None
        self._reported.pop(f"{pass_name}:{marker.job_key}", None)
        return job, state

    def _register_submissions(self) -> bool:
        changed = False
        for marker in list(self.workspace.scan_markers(("submitted",))):
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
                ready = {
                    "step": job.initial_step,
                    "activation_id": str(uuid.uuid4()),
                    "activation_ordinal": 1,
                    "attempt_ordinal": 0,
                    "total_attempts": 0,
                    "data_generation": 0 if job.data_mode == "transactional" else None,
                    "reason": "submitted",
                    "job_digest": job.digest,
                }
                self._transition(marker, "ready", ready)
            except TransitionLostError:
                pass
            except (FormatError, UnsupportedExtensionError) as exc:
                try:
                    self._transition(
                        marker,
                        "failed",
                        {
                            "failure": self._failure("protocol_error", str(exc)),
                            "data_generation": None,
                            "reason": "submission_invalid",
                        },
                    )
                except TransitionLostError:
                    pass
            changed = True
        return changed

    def _eligible_ready(self) -> list[Marker]:
        """Return one priority-ordered snapshot of locally eligible work."""

        eligible: list[Marker] = []
        for marker in self.workspace.scan_markers(("ready",)):
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
        attempt_ordinal = self._state_int(state, "attempt_ordinal", default=0) + 1
        total_attempts = self._state_int(state, "total_attempts", default=0) + 1
        budget_failure = self._attempt_budget_failure(job, attempt_ordinal, total_attempts)
        if budget_failure is not None:
            self._transition(
                marker,
                "failed",
                {
                    **self._state_progress(state),
                    "failure": self._failure("budget_exhausted", budget_failure),
                    "reason": "budget_exhausted",
                },
            )
            return True
        attempt_id = str(uuid.uuid4())
        claimed = self._transition(
            marker,
            "claimed",
            {
                **self._state_progress(state),
                "manager_id": self.manager_id,
                "writer_id": self.writer.writer_id,
                "claim_id": str(uuid.uuid4()),
                "attempt_id": attempt_id,
                "attempt_control": f".httk-attempt.{attempt_id}",
                "attempt_ordinal": attempt_ordinal,
                "total_attempts": total_attempts,
                "lease_seconds": self.lease_seconds,
                "matched_pool": job.claim_pool,
                "matched_capabilities": sorted(job.required_capabilities),
                "reason": state.get("reason", "claim"),
            },
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

    def _release_claim(self, marker: Marker, reason: str, state: Mapping[str, Any] | None = None) -> None:
        """Return one claimed job to ready without consuming its budget."""

        if state is None:
            try:
                state = self.workspace.read_state(marker)
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
                {
                    **self._state_progress(state),
                    "reason": reason,
                    "attempt_ordinal": max(0, self._state_int(state, "attempt_ordinal", default=1) - 1),
                    "total_attempts": max(0, self._state_int(state, "total_attempts", default=1) - 1),
                },
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

    def _launch_claimed(self, marker: Marker, job: JobDefinition, previous_state: Mapping[str, Any]) -> None:
        claimed_state = self.workspace.read_state(marker)
        backend = self._backend_for(job)
        if backend is None:
            raise FormatError(f"runner backend is unavailable: {job.runner_backend}")
        attempt_id = str(claimed_state["attempt_id"])
        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        control = payload / str(claimed_state["attempt_control"])
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
            "step": claimed_state["step"],
            "activation_id": claimed_state["activation_id"],
            "activation_ordinal": claimed_state["activation_ordinal"],
            "attempt_id": attempt_id,
            "attempt_ordinal": claimed_state["attempt_ordinal"],
            "total_attempts": claimed_state["total_attempts"],
            "is_restart": int(claimed_state["attempt_ordinal"]) > 1,
            "is_unclean_restart": bool(previous_state.get("unclean_restart", False)),
            "attempt_reason": claimed_state.get("reason", "claim"),
            "previous_attempt_id": previous_state.get("attempt_id"),
            "activation_reason": previous_state.get("reason"),
            "workdir_mode": job.workdir_mode,
            "workdir_reused": workdir_reused,
            "unsafe_persistent_takeover": bool(previous_state.get("unsafe_persistent_takeover", False)),
            "data_generation": claimed_state.get("data_generation"),
            "resources": dict(job.resources),
            "join": claimed_state.get("join_summary"),
            # The enriched, labeled observations of this activation's join, or an
            # empty array when the activation follows no join. ``join`` keeps the
            # summary exactly as earlier profiles published it.
            "children": self._context_children(claimed_state.get("join_summary")),
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
                {
                    **self._state_progress(claimed_state),
                    "manager_id": self.manager_id,
                    "writer_id": self.writer.writer_id,
                    "attempt_id": attempt_id,
                    "lease_seconds": self.lease_seconds,
                    "started_at": utc_now(),
                    "workdir": str(workdir.relative_to(payload)),
                    "attempt_control": control.name,
                    "reason": "launched",
                },
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
        for marker in list(self.workspace.scan_markers(("running",))):
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
            attempt_id = str(state.get("attempt_id", ""))
            current_by_attempt[attempt_id] = marker
            outcome_path = self._outcome_path(marker, state)
            if outcome_path.is_dir():
                self._commit_published_outcome(marker, job, state, outcome_path)
                changed = True
                continue
            local = self._running.get(attempt_id)
            if local is not None:
                return_code = local.process.poll()
                if return_code is None:
                    continue
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
                changed = True
                continue
            if self._manager_alive(
                str(state.get("manager_id", "")),
                lease_seconds=float(state.get("lease_seconds", self.lease_seconds)),
            ):
                continue
            unsafe_takeover = False
            if job.workdir_mode == "persistent" and not self._persistent_writer_dead(marker, state):
                if not self.unsafe_persistent_takeover:
                    _LOGGER.debug(
                        "leaving %s to its persistent writer: takeover is not proven safe",
                        marker.job_key,
                    )
                    continue
                unsafe_takeover = True
            _LOGGER.warning(
                "taking over %s: the lease of manager %s expired%s",
                marker.job_key,
                state.get("manager_id", "-"),
                " (unsafe persistent takeover)" if unsafe_takeover else "",
                extra=self._event(
                    "lease_takeover",
                    marker,
                    previous_manager=state.get("manager_id"),
                    unsafe_takeover=unsafe_takeover,
                ),
            )
            self._handle_attempt_failure(
                marker,
                job,
                "lease_lost",
                "owning manager heartbeat expired",
                unclean=True,
                unsafe_takeover=unsafe_takeover,
            )
            changed = True
        for attempt_id, local in list(self._running.items()):
            if attempt_id in current_by_attempt or local.marker.job_key in unreadable:
                continue
            _LOGGER.warning(
                "attempt %s of %s no longer owns a running marker; terminating it",
                attempt_id,
                local.marker.job_key,
                extra=self._event("attempt_orphaned", local.marker, attempt_id=attempt_id),
            )
            if local.process.poll() is None:
                self._terminate_process(local.process.pid)
            local.close_logs()
            del self._running[attempt_id]
        return changed

    def _commit_published_outcome(
        self,
        marker: Marker,
        job: JobDefinition,
        state: Mapping[str, Any],
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

    def _outcome_path(self, marker: Marker, state: Mapping[str, Any]) -> Path:
        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        control_name = str(state.get("attempt_control", f".httk-attempt.{state.get('attempt_id', '')}"))
        return payload / control_name / "outcome.ready"

    def _begin_commit(self, marker: Marker, state: Mapping[str, Any], outcome_path: Path) -> None:
        outcome = self._read_outcome(outcome_path / "outcome.json", marker, state)
        child_digests = self._child_digests(outcome_path)
        # The spawn set is validated before the marker leaves running, so a
        # missing or ambiguous child label is a protocol error of the published
        # outcome rather than a commit failure of an accepted one.
        child_labels = self._spawn_labels(outcome_path)
        self._transition(
            marker,
            "committing",
            {
                **self._state_progress(state),
                "manager_id": self.manager_id,
                "writer_id": self.writer.writer_id,
                "attempt_id": state["attempt_id"],
                "attempt_control": state["attempt_control"],
                "outcome_action": outcome["action"],
                "child_digests": child_digests,
                "child_labels": child_labels,
                "reason": "outcome_published",
            },
        )

    def _resume_committing(self) -> bool:
        changed = False
        for marker in list(self.workspace.scan_markers(("committing",))):
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
                        {
                            **self._state_progress(state),
                            "failure": self._failure("transaction_corruption", str(exc)),
                            "reason": "commit_failed",
                        },
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
        state = self.workspace.read_state(marker)
        job = self.workspace.load_job(marker)
        outcome_path = self._outcome_path(marker, state)
        outcome = self._read_outcome(outcome_path / "outcome.json", marker, state)
        data_generation_raw = state.get("data_generation")
        data_generation = int(data_generation_raw) if data_generation_raw is not None else None
        transaction_path = outcome_path / "transaction"
        if transaction_path.is_dir():
            if job.data_mode != "transactional" or data_generation is None:
                raise TransactionError("transaction published by a nontransactional job")
            if outcome.get("expected_data_generation") != data_generation:
                raise TransactionError("outcome expected_data_generation is stale")
            changed_data = replay_transaction(
                transaction_path,
                self.workspace.payload_path(marker.placement, marker.job_key) / "data",
                expected_generation=data_generation,
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
        progress = self._state_progress(state)
        progress["data_generation"] = data_generation
        self._record_runner_steps(marker, outcome, progress)
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
                {
                    **progress,
                    "next_step": next_step,
                    "join": self._labeled_join(join, outcome_path),
                    "reason": "waiting_for_children",
                },
                priority=next_priority,
            )
        elif action == "succeed":
            self._transition(
                marker,
                "succeeded",
                {**progress, "reason": "succeeded"},
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
                {**progress, "failure": failure.as_mapping(), "reason": reason},
                priority=next_priority,
            )
        elif action == "pause":
            self._transition(
                marker,
                "paused",
                {**progress, "pause": outcome.get("pause"), "reason": "step_paused"},
                priority=next_priority,
            )
        else:
            raise FormatError(f"unsupported outcome action: {action!r}")

    def _record_runner_steps(
        self,
        marker: Marker,
        outcome: Mapping[str, Any],
        progress: dict[str, object],
    ) -> None:
        """Copy the step set a runner declared into the resulting state frame.

        The member is advisory evidence for an operator and for tools that draw a
        job's reachable steps, never an input of a manager decision, so a
        malformed declaration is logged and dropped instead of failing the job.
        """

        declared = outcome.get("runner_steps")
        if declared is None:
            return
        if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
            _LOGGER.warning("ignoring runner_steps of %s: not an array", marker.job_key)
            return
        try:
            steps = [validate_step(item, "runner_steps item") for item in declared]
        except FormatError as exc:
            _LOGGER.warning("ignoring runner_steps of %s: %s", marker.job_key, exc)
            return
        progress["runner_steps"] = steps

    def _read_outcome(self, path: Path, marker: Marker, state: Mapping[str, Any]) -> dict[str, Any]:
        outcome = read_json(path)
        if outcome.get("format") != "httk-workflow-outcome" or outcome.get("format_version") != 1:
            raise FormatError("outcome must use httk-workflow-outcome version 1")
        for key, expected in (
            ("job_id", marker.job_id),
            ("activation_id", state.get("activation_id")),
            ("attempt_id", state.get("attempt_id")),
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

    def _register_children(self, marker: Marker, state: Mapping[str, Any], outcome_path: Path) -> None:
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
        expected_digests = state.get("child_digests", {})
        if not isinstance(expected_digests, Mapping):
            raise FormatError("committing child_digests must be an object")
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
            if source.is_dir():
                child = JobDefinition.from_mapping(read_json(source / "job.json"))
                if child.job_key != job_key:
                    raise FormatError("spawn job_key disagrees with child job.json")
                if tree_digest(source, skip=is_payload_private) != expected_digest:
                    raise FormatError("spawn child changed after outcome publication")
                target.parent.mkdir(parents=True, exist_ok=True)
                self.workspace._publish_path(source, target)
            # A registered child may already be running, so its own attempt
            # control directory and job state are excluded here exactly as they
            # are from the digest recorded before publication.
            if not target.is_dir() or tree_digest(target, skip=is_payload_private) != expected_digest:
                raise FormatError(f"registered child bundle does not match: {job_key}")
            child = JobDefinition.from_mapping(read_json(target / "job.json"))
            existing = self.workspace.find_markers(job_key)
            if existing:
                continue
            temporary = self.workspace.control / "tmp" / f"child-marker.{uuid.uuid4()}"
            temporary.touch(exist_ok=False)
            destination = self.workspace.marker_path("submitted", placement, job_key, child.priority, 0, "init")
            self.workspace._publish_path(temporary, destination)

    def _advance(
        self,
        marker: Marker,
        job: JobDefinition,
        state: Mapping[str, Any],
        next_step: str,
        progress: Mapping[str, object],
        *,
        reason: str = "advance",
        join_summary: object = None,
        priority: int | None = None,
    ) -> None:
        activation_ordinal = self._state_int(state, "activation_ordinal", default=1) + 1
        maximum = job.retry_policy.maximum_activations
        if maximum is not None and activation_ordinal > maximum:
            self._transition(
                marker,
                "failed",
                {
                    **progress,
                    "failure": self._failure("budget_exhausted", "maximum_activations exceeded"),
                    "reason": "budget_exhausted",
                },
            )
            return
        self._transition(
            marker,
            "ready",
            {
                **progress,
                "step": next_step,
                "activation_id": str(uuid.uuid4()),
                "activation_ordinal": activation_ordinal,
                "attempt_ordinal": 0,
                "reason": reason,
                "join_summary": join_summary,
                "previous_attempt_id": state.get("attempt_id"),
            },
            priority=priority,
        )

    def _retry(
        self,
        marker: Marker,
        job: JobDefinition,
        state: Mapping[str, Any],
        progress: Mapping[str, object],
        reason: str,
        *,
        unclean: bool,
        unsafe_takeover: bool = False,
        priority: int | None = None,
    ) -> None:
        current_attempts = self._state_int(state, "attempt_ordinal", default=1)
        maximum = job.retry_policy.maximum_attempts_per_activation
        if maximum is not None and current_attempts >= maximum:
            self._transition(
                marker,
                "failed",
                {
                    **progress,
                    "failure": self._failure("retry_exhausted", reason),
                    "reason": "retry_exhausted",
                },
            )
            return
        self._transition(
            marker,
            "ready",
            {
                **progress,
                "reason": reason,
                "unclean_restart": unclean,
                "unsafe_persistent_takeover": unsafe_takeover,
                "previous_attempt_id": state.get("attempt_id"),
            },
            priority=priority,
        )

    def _retry_budget_available(self, job: JobDefinition, state: Mapping[str, Any]) -> bool:
        """Report whether another attempt of this activation is still permitted."""

        attempts = self._state_int(state, "attempt_ordinal", default=1)
        total = self._state_int(state, "total_attempts", default=attempts)
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
        unsafe_takeover: bool = False,
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
            state = self.workspace.read_state(marker)
            progress = self._state_progress(state)
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
                    unsafe_takeover=unsafe_takeover,
                )
                return
            self._transition(
                marker,
                "failed",
                {
                    **progress,
                    "failure": self._failure(code, message, exit_status=exit_status),
                    "reason": code,
                },
            )
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
        for marker in list(self.workspace.scan_markers(("claimed",))):
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
            if self._manager_alive(
                str(state.get("manager_id", "")),
                lease_seconds=float(state.get("lease_seconds", self.lease_seconds)),
            ):
                continue
            _LOGGER.warning(
                "recovering %s: the claim of manager %s was abandoned",
                marker.job_key,
                state.get("manager_id", "-"),
                extra=self._event("claim_recovered", marker, previous_manager=state.get("manager_id")),
            )
            self._release_claim(marker, "claim_abandoned", state)
            changed = True
        return changed

    def _manager_alive(self, manager_id: str, *, lease_seconds: float) -> bool:
        if not manager_id:
            return False
        try:
            heartbeat = read_json(self.workspace.control / "managers" / manager_id / "heartbeat.json")
            updated = timestamp_seconds(str(heartbeat["updated_at"]))
        except (WorkflowError, KeyError, ValueError):
            return False
        return time.time() - updated <= lease_seconds

    def _persistent_writer_dead(self, marker: Marker, state: Mapping[str, Any]) -> bool:
        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        process_path = payload / str(state.get("attempt_control", "")) / "process.json"
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
            child_marker = None
            placement_hint = reference.get("placement_hint")
            job_key = reference.get("job_key")
            if isinstance(placement_hint, str) and isinstance(job_key, str):
                child_marker = self.workspace.find_marker_at(job_key, normalize_placement(placement_hint))
                if child_marker is not None and child_marker.job_id != child_id:
                    raise FormatError("join child identity disagrees with placement hint")
            if child_marker is None:
                child_marker = self.workspace.find_marker_by_id(child_id)
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
            state: Mapping[str, Any] = self.workspace.read_state(marker)
        except (WorkflowError, OSError) as exc:
            _LOGGER.debug("cannot read the state frame of join child %s: %s", marker.job_key, exc)
            state = {}
        failure: object = None
        if marker.kind in {"failed", "cancelled"}:
            raw = state.get("failure")
            if isinstance(raw, Mapping):
                try:
                    failure = validate_failure(raw).as_mapping()
                except FormatError:
                    # Evidence of a malformed record is still evidence.
                    failure = dict(raw)
        return {
            "failure": failure,
            "workdir_path": self._child_workdir_path(marker, state, payload),
            "data_generation": state.get("data_generation"),
        }

    def _child_workdir_path(
        self,
        marker: Marker,
        state: Mapping[str, Any],
        payload: PurePosixPath,
    ) -> str | None:
        """Return the workspace-relative workdir of one observed child."""

        try:
            job = self.workspace.load_job(marker)
        except (WorkflowError, OSError):
            recorded = state.get("workdir")
            return (payload / PurePosixPath(recorded)).as_posix() if isinstance(recorded, str) and recorded else None
        if job.workdir_mode == "persistent":
            return (payload / job.workdir_path).as_posix()
        attempt_id = state.get("attempt_id")
        if isinstance(attempt_id, str) and attempt_id:
            # An isolated workdir belongs to one attempt, so the last attempt
            # recorded in the child's own state frame names the last workdir.
            base = job.workdir_path
            return (payload / base.parent / f"{base.name}.{attempt_id}").as_posix()
        recorded = state.get("workdir")
        return (payload / PurePosixPath(recorded)).as_posix() if isinstance(recorded, str) and recorded else None

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
        state: Mapping[str, Any],
        code: str,
        message: str,
        reason: str,
    ) -> None:
        self._join_unresolved.pop(marker.job_key, None)
        try:
            self._transition(
                marker,
                "failed",
                {
                    **self._state_progress(state),
                    "failure": self._failure(code, message),
                    "reason": reason,
                },
            )
        except TransitionLostError:
            _LOGGER.debug("join failure record for %s was lost to another actor", marker.job_key)

    def _evaluate_joins(self) -> bool:
        changed = False
        for marker in list(self.workspace.scan_markers(("waiting",))):
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

    def _evaluate_join(self, marker: Marker, parent_job: JobDefinition, state: Mapping[str, Any]) -> bool:
        """Observe one waiting job's join and act when it resolves."""

        join = require_mapping(state.get("join"), "state.join")
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
                validate_step(state.get("next_step"), "next_step"),
                self._state_progress(state),
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
                self._state_progress(state),
                reason="join_impossible",
                join_summary=observations,
            )
            return True
        self._transition(
            marker,
            "failed",
            {
                **self._state_progress(state),
                "failure": self._failure("dependency_failure", "join condition became impossible"),
                "join_summary": observations,
                "reason": "join_impossible",
            },
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

    def _handle_requests(self) -> bool:
        ready_dir = self.workspace.control / "requests" / "ready"
        changed = False
        for request_path in sorted(ready_dir.iterdir()) if ready_dir.exists() else ():
            if not request_path.is_file():
                continue
            try:
                request = read_json(request_path)
                marker = self.workspace.find_marker_by_id(str(request.get("job_id", "")))
                if marker is not None:
                    job = self.workspace.load_job(marker)
                    if self._backend_for(job) is None:
                        _LOGGER.debug(
                            "leaving request %s: runner backend %s is not served here",
                            request_path.name,
                            job.runner_backend,
                        )
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
                self._apply_request(read_json(claimed_path))
            except TransitionLostError:
                _LOGGER.info("dropping request %s: the job moved first", claimed_path.name)
                claimed_path.unlink(missing_ok=True)
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
                _LOGGER.info(
                    "handled request %s",
                    claimed_path.name,
                    extra=self._event("request_handled", request=claimed_path.name),
                )
                claimed_path.unlink(missing_ok=True)
            changed = True
        return changed

    def _apply_request(self, request: Mapping[str, Any]) -> None:
        if request.get("format") != "httk-workflow-request" or request.get("format_version") != 1:
            raise FormatError("request must use httk-workflow-request version 1")
        marker = self.workspace.find_marker_by_id(str(request.get("job_id", "")))
        if marker is None:
            raise FormatError("request job does not exist")
        if marker.generation != int(request.get("expected_generation", -1)):
            return
        if marker.record_ref != request.get("expected_record_ref"):
            return
        action = request.get("action")
        state = self.workspace.read_state(marker)
        job = self.workspace.load_job(marker)
        progress = self._state_progress(state)
        audit = {
            "operator": request.get("operator"),
            "operator_reason": request.get("reason"),
            "request_id": request.get("request_id"),
        }
        if action == "cancel" and marker.kind not in TERMINAL_KINDS:
            self._transition(
                marker,
                "cancelled",
                {**progress, **audit, "reason": "operator_cancel"},
            )
            if marker.kind == "running":
                self._terminate_attempt(marker, state)
            return
        if action == "set_priority" and marker.kind in {"submitted", "ready", "waiting", "paused", "failed"}:
            priority = int(request.get("priority", -1))
            # A repriced job keeps the same state kind, so the members that make
            # that kind meaningful must survive the new frame. Dropping a join
            # would leave a waiting job with an unusable dependency record.
            preserved = {
                name: state[name] for name in ("join", "join_summary", "next_step", "pause", "failure") if name in state
            }
            self._transition(
                marker,
                marker.kind,
                {**progress, **preserved, **audit, "reason": "operator_priority"},
                priority=priority,
            )
            return
        if action == "pause" and marker.kind in {"submitted", "ready", "waiting"}:
            self._transition(
                marker,
                "paused",
                {**progress, **audit, "reason": "operator_pause"},
            )
            return
        if action == "continue" and marker.kind in {"failed", "paused"}:
            if "activation_id" not in state:
                raise FormatError("job has no runnable activation to continue")
            self._retry(marker, job, state, {**progress, **audit}, "manual_continue", unclean=False)
            return
        if action == "override_step" and marker.kind in {"failed", "paused"}:
            self._advance(
                marker,
                job,
                state,
                validate_step(request.get("step"), "request.step"),
                {**progress, **audit},
                reason="operator_override_step",
            )
            return
        raise FormatError(f"request action {action!r} is invalid from state {marker.kind}")

    def _terminate_attempt(self, marker: Marker, state: Mapping[str, Any]) -> None:
        attempt_id = str(state.get("attempt_id", ""))
        local = self._running.pop(attempt_id, None)
        if local is not None:
            self._terminate_process(local.process.pid)
            local.close_logs()
            return
        payload = self.workspace.payload_path(marker.placement, marker.job_key)
        try:
            process = read_json(payload / str(state.get("attempt_control", "")) / "process.json")
            if process.get("hostname") == self.hostname:
                self._terminate_process(int(process["process_group"]))
        except (WorkflowError, KeyError, TypeError, ValueError):
            return

    @staticmethod
    def _terminate_process(process_group: int, signal_number: int = signal.SIGTERM) -> None:
        try:
            os.killpg(process_group, signal_number)
        except ProcessLookupError:
            return
        except PermissionError as exc:
            _LOGGER.warning("cannot signal process group %d: %s", process_group, exc)

    @staticmethod
    def _state_int(state: Mapping[str, Any], name: str, *, default: int) -> int:
        value = state.get(name, default)
        return int(value) if value is not None else default

    @staticmethod
    def _state_progress(state: Mapping[str, Any]) -> dict[str, object]:
        names = (
            "step",
            "activation_id",
            "activation_ordinal",
            "attempt_id",
            "attempt_ordinal",
            "total_attempts",
            "data_generation",
            # The observed children of the join that started this activation are
            # inputs of the activation, exactly like its step: every attempt of
            # it, including one recovered from an abandoned claim or a retry,
            # must see the same children. A new activation resets the member.
            "join_summary",
            # The step set the runner of this job declared, carried forward until
            # a later outcome declares a different one.
            "runner_steps",
        )
        return {name: state[name] for name in names if name in state}

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
