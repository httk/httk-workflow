"""Core-v1 workflow task manager."""

import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Self

from ._util import read_json, timestamp_seconds, tree_digest, utc_now, write_json_atomic
from .errors import (
    FormatError,
    TransactionError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .journal import JournalWriter
from .models import (
    TERMINAL_KINDS,
    JobDefinition,
    Marker,
    normalize_placement,
    validate_step,
)
from .store import WorkflowStore
from .transactions import replay_transaction


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
    """Execute and recover jobs in one workflow store."""

    def __init__(
        self,
        store: WorkflowStore,
        *,
        pools: Sequence[str] = ("default",),
        capabilities: Sequence[str] = (),
        maximum_workers: int = 1,
        lease_seconds: float = 900.0,
        heartbeat_interval: float = 30.0,
        unsafe_persistent_takeover: bool = False,
    ) -> None:
        if maximum_workers < 1:
            raise ValueError("maximum_workers must be positive")
        self.store = store
        self.pools = frozenset(pools)
        self.capabilities = frozenset(capabilities)
        self.maximum_workers = maximum_workers
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.unsafe_persistent_takeover = unsafe_persistent_takeover
        self.manager_id = str(uuid.uuid4())
        self.hostname = socket.gethostname()
        self.writer = JournalWriter(store.control, durable=store.durable)
        self._manager_dir = store.control / "managers" / self.manager_id
        self._manager_dir.mkdir(parents=True, exist_ok=False)
        self._running: dict[str, RunningAttempt] = {}
        self._last_heartbeat = 0.0
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
                "started_at": utc_now(),
            },
            durable=store.durable,
        )
        self.heartbeat(force=True)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        for attempt in self._running.values():
            attempt.close_logs()
        self._running.clear()
        self.writer.close()

    def heartbeat(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_heartbeat < self.heartbeat_interval:
            return
        write_json_atomic(
            self._manager_dir / "heartbeat.json",
            {"manager_id": self.manager_id, "updated_at": utc_now()},
            durable=self.store.durable,
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
        for marker in self._eligible_ready():
            if len(self._running) >= self.maximum_workers:
                break
            try:
                self._claim_and_launch(marker)
            except TransitionLostError:
                continue
            changed = True
        return changed

    def serve(self, *, poll_interval: float = 1.0) -> None:
        """Run until interrupted."""

        try:
            while True:
                self.tick()
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            return

    def run_until_idle(self, *, timeout: float = 60.0, poll_interval: float = 0.02) -> None:
        """Run until no local process or immediately actionable marker remains."""

        deadline = time.monotonic() + timeout
        quiet_passes = 0
        while time.monotonic() < deadline:
            changed = self.tick()
            actionable = any(True for _ in self.store.scan_markers(("submitted", "ready", "committing")))
            if not changed and not self._running and not actionable:
                quiet_passes += 1
                if quiet_passes >= 2:
                    return
            else:
                quiet_passes = 0
            time.sleep(poll_interval)
        raise TimeoutError("workflow manager did not become idle")

    def _register_submissions(self) -> bool:
        changed = False
        for marker in list(self.store.scan_markers(("submitted",))):
            try:
                job = self.store.validate_job_payload(marker)
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
                self.store.transition(self.writer, marker, "ready", ready)
            except TransitionLostError:
                pass
            except (FormatError, UnsupportedExtensionError) as exc:
                try:
                    self.store.transition(
                        self.writer,
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
        for marker in self.store.scan_markers(("ready",)):
            try:
                job = self.store.load_job(marker)
            except WorkflowError:
                continue
            if job.claim_pool not in self.pools:
                continue
            if not job.required_capabilities <= self.capabilities:
                continue
            eligible.append(marker)
        eligible.sort(key=lambda item: (item.priority, item.path.as_posix()))
        return eligible

    def _claim_and_launch(self, marker: Marker) -> None:
        job = self.store.load_job(marker)
        state = self.store.read_state(marker)
        attempt_ordinal = self._state_int(state, "attempt_ordinal", default=0) + 1
        total_attempts = self._state_int(state, "total_attempts", default=0) + 1
        budget_failure = self._attempt_budget_failure(job, attempt_ordinal, total_attempts)
        if budget_failure is not None:
            self.store.transition(
                self.writer,
                marker,
                "failed",
                {
                    **self._state_progress(state),
                    "failure": self._failure("budget_exhausted", budget_failure),
                    "reason": "budget_exhausted",
                },
            )
            return
        attempt_id = str(uuid.uuid4())
        claimed = self.store.transition(
            self.writer,
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
        self._launch_claimed(claimed, job, state)

    def _launch_claimed(self, marker: Marker, job: JobDefinition, previous_state: Mapping[str, Any]) -> None:
        claimed_state = self.store.read_state(marker)
        attempt_id = str(claimed_state["attempt_id"])
        payload = self.store.payload_path(marker.placement, marker.job_key)
        control = payload / str(claimed_state["attempt_control"])
        control.mkdir(exist_ok=False)
        if job.workspace_mode == "persistent":
            workspace = payload.joinpath(*job.workspace_path.parts)
            workspace_reused = workspace.exists()
        else:
            base = payload.joinpath(*job.workspace_path.parts)
            workspace = base.parent / f"{base.name}.{attempt_id}"
            workspace_reused = False
        workspace.mkdir(parents=True, exist_ok=True)
        context = {
            "format": "httk-workflow-attempt-context",
            "format_version": 1,
            "store_id": self.store.store_id,
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
            "workspace_mode": job.workspace_mode,
            "workspace_reused": workspace_reused,
            "unsafe_persistent_takeover": bool(previous_state.get("unsafe_persistent_takeover", False)),
            "data_generation": claimed_state.get("data_generation"),
            "join": claimed_state.get("join_summary"),
        }
        context_path = control / "context.json"
        write_json_atomic(context_path, context, durable=self.store.durable)
        environment = os.environ.copy()
        environment.update(
            {
                "HTTK_WORKFLOW_CONTEXT": str(context_path),
                "HTTK_WORKFLOW_CONTROL_DIR": str(control),
                "HTTK_WORKFLOW_STORE_DIR": str(self.store.root),
                "HTTK_WORKFLOW_JOB_DIR": str(payload),
                "HTTK_WORKFLOW_RUN_DIR": str(workspace),
                "HTTK_WORKFLOW_IS_RESTART": "1" if context["is_restart"] else "0",
                "HTTK_WORKFLOW_UNCLEAN_RESTART": "1" if context["is_unclean_restart"] else "0",
                "HTTK_WORKFLOW_ATTEMPT_REASON": str(context["attempt_reason"]),
                "HTTK_WORKFLOW_STEP": str(context["step"]),
            }
        )
        if job.data_mode == "transactional":
            environment["HTTK_WORKFLOW_DATA_DIR"] = str(payload / "data")
        stdout = (control / "stdout.log").open("ab")
        stderr = (control / "stderr.log").open("ab")
        runner_command = [str(payload.joinpath(*job.runner_path.parts)), *job.runner_arguments]
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
                cwd=workspace,
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
                durable=self.store.durable,
            )
            running = self.store.transition(
                self.writer,
                marker,
                "running",
                {
                    **self._state_progress(claimed_state),
                    "manager_id": self.manager_id,
                    "writer_id": self.writer.writer_id,
                    "attempt_id": attempt_id,
                    "lease_seconds": self.lease_seconds,
                    "started_at": utc_now(),
                    "workspace": str(workspace.relative_to(payload)),
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
            if isinstance(exc, TransitionLostError):
                raise
            self._handle_attempt_failure(running or marker, job, "process_failure", f"cannot launch runner: {exc}")
            return
        os.close(gate_write)
        assert process is not None
        assert running is not None
        self._running[attempt_id] = RunningAttempt(running, process, stdout, stderr, attempt_id)

    def _poll_running(self) -> bool:
        changed = False
        current_by_attempt: dict[str, Marker] = {}
        for marker in list(self.store.scan_markers(("running",))):
            state = self.store.read_state(marker)
            attempt_id = str(state.get("attempt_id", ""))
            current_by_attempt[attempt_id] = marker
            outcome_path = self._outcome_path(marker, state)
            if outcome_path.is_dir():
                try:
                    self._begin_commit(marker, state, outcome_path)
                except TransitionLostError:
                    pass
                changed = True
                continue
            local = self._running.get(attempt_id)
            if local is not None:
                return_code = local.process.poll()
                if return_code is None:
                    continue
                local.close_logs()
                del self._running[attempt_id]
                if outcome_path.is_dir():
                    try:
                        self._begin_commit(marker, state, outcome_path)
                    except TransitionLostError:
                        pass
                else:
                    job = self.store.load_job(marker)
                    failure_class = "protocol_error" if return_code == 0 else "process_failure"
                    self._handle_attempt_failure(
                        marker,
                        job,
                        failure_class,
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
            job = self.store.load_job(marker)
            unsafe_takeover = False
            if job.workspace_mode == "persistent" and not self._persistent_writer_dead(marker, state):
                if not self.unsafe_persistent_takeover:
                    continue
                unsafe_takeover = True
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
            if attempt_id not in current_by_attempt:
                if local.process.poll() is None:
                    self._terminate_process(local.process.pid)
                local.close_logs()
                del self._running[attempt_id]
        return changed

    def _outcome_path(self, marker: Marker, state: Mapping[str, Any]) -> Path:
        payload = self.store.payload_path(marker.placement, marker.job_key)
        control_name = str(state.get("attempt_control", f".httk-attempt.{state.get('attempt_id', '')}"))
        return payload / control_name / "outcome.ready"

    def _begin_commit(self, marker: Marker, state: Mapping[str, Any], outcome_path: Path) -> None:
        outcome = self._read_outcome(outcome_path / "outcome.json", marker, state)
        child_digests = self._child_digests(outcome_path)
        self.store.transition(
            self.writer,
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
                "reason": "outcome_published",
            },
        )

    def _resume_committing(self) -> bool:
        changed = False
        for marker in list(self.store.scan_markers(("committing",))):
            try:
                self._process_committing(marker)
            except TransitionLostError:
                pass
            except (FormatError, TransactionError) as exc:
                try:
                    state = self.store.read_state(marker)
                    self.store.transition(
                        self.writer,
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
            changed = True
        return changed

    def _process_committing(self, marker: Marker) -> None:
        state = self.store.read_state(marker)
        job = self.store.load_job(marker)
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
                self.store.payload_path(marker.placement, marker.job_key) / "data",
                expected_generation=data_generation,
            )
            if changed_data:
                data_generation += 1
        self._register_children(marker, state, outcome_path)
        action = outcome["action"]
        progress = self._state_progress(state)
        progress["data_generation"] = data_generation
        if action == "advance":
            self._advance(marker, job, state, validate_step(outcome.get("next_step"), "next_step"), progress)
        elif action == "retry":
            reason = self._nested_reason(outcome, "retry")
            self._retry(marker, job, state, progress, reason, unclean=False)
        elif action == "wait":
            next_step = validate_step(outcome.get("next_step"), "next_step")
            join = outcome.get("join")
            if not isinstance(join, Mapping):
                raise FormatError("wait outcome requires a join object")
            self.store.transition(
                self.writer,
                marker,
                "waiting",
                {**progress, "next_step": next_step, "join": dict(join), "reason": "waiting_for_children"},
            )
        elif action == "succeed":
            self.store.transition(self.writer, marker, "succeeded", {**progress, "reason": "succeeded"})
        elif action == "fail":
            failure = outcome.get("failure")
            if not isinstance(failure, Mapping):
                raise FormatError("fail outcome requires a failure object")
            self.store.transition(
                self.writer,
                marker,
                "failed",
                {**progress, "failure": dict(failure), "reason": "declared_failure"},
            )
        elif action == "pause":
            self.store.transition(
                self.writer,
                marker,
                "paused",
                {**progress, "pause": outcome.get("pause"), "reason": "step_paused"},
            )
        else:
            raise FormatError(f"unsupported outcome action: {action!r}")

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
        return {path.name: tree_digest(path) for path in sorted(jobs_dir.iterdir()) if path.is_dir()}

    def _register_children(self, marker: Marker, state: Mapping[str, Any], outcome_path: Path) -> None:
        children_dir = outcome_path / "children"
        if not children_dir.is_dir():
            return
        spawn = read_json(children_dir / "spawn.json")
        entries = spawn.get("children")
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
            raise FormatError("spawn children must be an array")
        expected_digests = state.get("child_digests", {})
        if not isinstance(expected_digests, Mapping):
            raise FormatError("committing child_digests must be an object")
        for raw in entries:
            if not isinstance(raw, Mapping):
                raise FormatError("spawn child must be an object")
            job_key = str(raw.get("job_key", ""))
            placement = normalize_placement(str(raw.get("placement", "")))
            if raw.get("store_id", self.store.store_id) != self.store.store_id:
                raise UnsupportedExtensionError("cross-store child requires multistore-v1")
            source = children_dir / "jobs" / job_key
            expected_digest = str(expected_digests.get(job_key, ""))
            target = self.store.payload_path(placement, job_key)
            if source.is_dir():
                child = JobDefinition.from_mapping(read_json(source / "job.json"))
                if child.job_key != job_key:
                    raise FormatError("spawn job_key disagrees with child job.json")
                if tree_digest(source) != expected_digest:
                    raise FormatError("spawn child changed after outcome publication")
                target.parent.mkdir(parents=True, exist_ok=True)
                self.store._publish_path(source, target)
            if not target.is_dir() or tree_digest(target) != expected_digest:
                raise FormatError(f"registered child bundle does not match: {job_key}")
            child = JobDefinition.from_mapping(read_json(target / "job.json"))
            existing = self.store.find_markers(job_key)
            if existing:
                continue
            temporary = self.store.control / "tmp" / f"child-marker.{uuid.uuid4()}"
            temporary.touch(exist_ok=False)
            destination = self.store.marker_path("submitted", placement, job_key, child.priority, 0, "init")
            self.store._publish_path(temporary, destination)

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
    ) -> None:
        activation_ordinal = self._state_int(state, "activation_ordinal", default=1) + 1
        maximum = job.retry_policy.maximum_activations
        if maximum is not None and activation_ordinal > maximum:
            self.store.transition(
                self.writer,
                marker,
                "failed",
                {
                    **progress,
                    "failure": self._failure("budget_exhausted", "maximum_activations exceeded"),
                    "reason": "budget_exhausted",
                },
            )
            return
        self.store.transition(
            self.writer,
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
    ) -> None:
        current_attempts = self._state_int(state, "attempt_ordinal", default=1)
        maximum = job.retry_policy.maximum_attempts_per_activation
        if maximum is not None and current_attempts >= maximum:
            self.store.transition(
                self.writer,
                marker,
                "failed",
                {
                    **progress,
                    "failure": self._failure("retry_exhausted", reason),
                    "reason": "retry_exhausted",
                },
            )
            return
        self.store.transition(
            self.writer,
            marker,
            "ready",
            {
                **progress,
                "reason": reason,
                "unclean_restart": unclean,
                "unsafe_persistent_takeover": unsafe_takeover,
                "previous_attempt_id": state.get("attempt_id"),
            },
        )

    def _handle_attempt_failure(
        self,
        marker: Marker,
        job: JobDefinition,
        failure_class: str,
        summary: str,
        *,
        exit_status: int | None = None,
        unclean: bool = True,
        unsafe_takeover: bool = False,
    ) -> None:
        state = self.store.read_state(marker)
        progress = self._state_progress(state)
        if failure_class in job.retry_policy.retry_on:
            self._retry(
                marker,
                job,
                state,
                progress,
                failure_class,
                unclean=unclean,
                unsafe_takeover=unsafe_takeover,
            )
            return
        self.store.transition(
            self.writer,
            marker,
            "failed",
            {
                **progress,
                "failure": self._failure(failure_class, summary, exit_status=exit_status),
                "reason": failure_class,
            },
        )

    def _recover_abandoned_claims(self) -> bool:
        changed = False
        for marker in list(self.store.scan_markers(("claimed",))):
            state = self.store.read_state(marker)
            if self._manager_alive(
                str(state.get("manager_id", "")),
                lease_seconds=float(state.get("lease_seconds", self.lease_seconds)),
            ):
                continue
            try:
                self.store.transition(
                    self.writer,
                    marker,
                    "ready",
                    {
                        **self._state_progress(state),
                        "reason": "claim_abandoned",
                        "attempt_ordinal": max(0, self._state_int(state, "attempt_ordinal", default=1) - 1),
                        "total_attempts": max(0, self._state_int(state, "total_attempts", default=1) - 1),
                    },
                )
            except TransitionLostError:
                pass
            changed = True
        return changed

    def _manager_alive(self, manager_id: str, *, lease_seconds: float) -> bool:
        if not manager_id:
            return False
        try:
            heartbeat = read_json(self.store.control / "managers" / manager_id / "heartbeat.json")
            updated = timestamp_seconds(str(heartbeat["updated_at"]))
        except (WorkflowError, KeyError, ValueError):
            return False
        return time.time() - updated <= lease_seconds

    def _persistent_writer_dead(self, marker: Marker, state: Mapping[str, Any]) -> bool:
        payload = self.store.payload_path(marker.placement, marker.job_key)
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

    def _evaluate_joins(self) -> bool:
        changed = False
        for marker in list(self.store.scan_markers(("waiting",))):
            state = self.store.read_state(marker)
            join = state.get("join")
            if not isinstance(join, Mapping):
                continue
            children = join.get("children")
            if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
                continue
            observations: list[dict[str, object]] = []
            missing = False
            for child_ref in children:
                if not isinstance(child_ref, Mapping):
                    missing = True
                    break
                child_id = str(child_ref.get("job_id", ""))
                child_marker = None
                placement_hint = child_ref.get("placement_hint")
                job_key = child_ref.get("job_key")
                if isinstance(placement_hint, str) and isinstance(job_key, str):
                    child_marker = self.store.find_marker_at(job_key, normalize_placement(placement_hint))
                    if child_marker is not None and child_marker.job_id != child_id:
                        raise FormatError("join child identity disagrees with placement hint")
                if child_marker is None:
                    child_marker = self.store.find_marker_by_id(child_id)
                if child_marker is None:
                    missing = True
                    break
                observations.append(
                    {
                        "store_id": self.store.store_id,
                        "job_id": child_id,
                        "job_key": child_marker.job_key,
                        "placement": child_marker.placement.as_posix(),
                        "kind": child_marker.kind,
                        "state_generation": child_marker.generation,
                        "record_ref": child_marker.record_ref,
                    }
                )
            if missing:
                continue
            condition = str(join.get("condition", ""))
            kinds = [str(item["kind"]) for item in observations]
            satisfied = self._join_satisfied(condition, join, kinds)
            impossible = self._join_impossible(condition, join, kinds)
            if not satisfied and not impossible:
                continue
            job = self.store.load_job(marker)
            if satisfied:
                self._advance(
                    marker,
                    job,
                    state,
                    validate_step(state.get("next_step"), "next_step"),
                    self._state_progress(state),
                    reason="join_satisfied",
                    join_summary=observations,
                )
            else:
                on_impossible = join.get("on_impossible")
                if isinstance(on_impossible, Mapping) and on_impossible.get("action") == "advance":
                    self._advance(
                        marker,
                        job,
                        state,
                        validate_step(on_impossible.get("next_step"), "on_impossible.next_step"),
                        self._state_progress(state),
                        reason="join_impossible",
                        join_summary=observations,
                    )
                else:
                    self.store.transition(
                        self.writer,
                        marker,
                        "failed",
                        {
                            **self._state_progress(state),
                            "failure": self._failure("dependency_failure", "join condition became impossible"),
                            "join_summary": observations,
                            "reason": "join_impossible",
                        },
                    )
            changed = True
        return changed

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
        ready_dir = self.store.control / "requests" / "ready"
        changed = False
        for request_path in sorted(ready_dir.iterdir()) if ready_dir.exists() else ():
            if not request_path.is_file():
                continue
            claimed_dir = self.store.control / "requests" / "claimed" / self.manager_id
            claimed_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claimed_dir / request_path.name
            try:
                os.rename(request_path, claimed_path)
            except FileNotFoundError:
                continue
            try:
                self._apply_request(read_json(claimed_path))
            except TransitionLostError:
                claimed_path.unlink(missing_ok=True)
            except (WorkflowError, OSError, ValueError) as exc:
                self.store.quarantine(claimed_path, reason=f"invalid request: {exc}")
            else:
                claimed_path.unlink(missing_ok=True)
            changed = True
        return changed

    def _apply_request(self, request: Mapping[str, Any]) -> None:
        if request.get("format") != "httk-workflow-request" or request.get("format_version") != 1:
            raise FormatError("request must use httk-workflow-request version 1")
        marker = self.store.find_marker_by_id(str(request.get("job_id", "")))
        if marker is None:
            raise FormatError("request job does not exist")
        if marker.generation != int(request.get("expected_generation", -1)):
            return
        if marker.record_ref != request.get("expected_record_ref"):
            return
        action = request.get("action")
        state = self.store.read_state(marker)
        job = self.store.load_job(marker)
        progress = self._state_progress(state)
        audit = {
            "operator": request.get("operator"),
            "operator_reason": request.get("reason"),
            "request_id": request.get("request_id"),
        }
        if action == "cancel" and marker.kind not in TERMINAL_KINDS:
            self.store.transition(
                self.writer,
                marker,
                "cancelled",
                {**progress, **audit, "reason": "operator_cancel"},
            )
            if marker.kind == "running":
                self._terminate_attempt(marker, state)
            return
        if action == "set_priority" and marker.kind in {"submitted", "ready", "waiting", "paused", "failed"}:
            priority = int(request.get("priority", -1))
            self.store.transition(
                self.writer,
                marker,
                marker.kind,
                {**progress, **audit, "reason": "operator_priority"},
                priority=priority,
            )
            return
        if action == "pause" and marker.kind in {"submitted", "ready", "waiting"}:
            self.store.transition(
                self.writer,
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
        payload = self.store.payload_path(marker.placement, marker.job_key)
        try:
            process = read_json(payload / str(state.get("attempt_control", "")) / "process.json")
            if process.get("hostname") == self.hostname:
                self._terminate_process(int(process["process_group"]))
        except (WorkflowError, KeyError, TypeError, ValueError):
            return

    @staticmethod
    def _terminate_process(process_group: int) -> None:
        try:
            os.killpg(process_group, signal.SIGTERM)
        except ProcessLookupError:
            return

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
        )
        return {name: state[name] for name in names if name in state}

    @staticmethod
    def _failure(
        failure_class: str,
        summary: str,
        *,
        exit_status: int | None = None,
    ) -> dict[str, object]:
        result: dict[str, object] = {"class": failure_class, "summary": summary}
        if exit_status is not None:
            result["exit_status"] = exit_status
        return result

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
