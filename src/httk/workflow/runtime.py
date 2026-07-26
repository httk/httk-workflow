"""Application-side helpers for native v2 workflow attempts.

This API publishes the v2 outcome protocol directly. It is intentionally not
a Python spelling of the v1 ``HT_TASK_*`` functions.
"""

import os
import shutil
import signal
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from ._util import read_json
from .models import Failure
from .runtime_builders import (
    JoinSpec,
    OutcomeBuilder,
    ReplayableWorkdirBatch,
    RunLog,
    WorkdirState,
)

_OUTCOME_ACTIONS = frozenset({"advance", "retry", "wait", "succeed", "fail", "pause"})


@dataclass(frozen=True)
class AttemptContext:
    """The immutable identity and restart evidence for one running attempt."""

    workspace_id: str
    job_id: str
    job_key: str
    placement: str
    step: str
    activation_id: str
    attempt_id: str
    activation_ordinal: int | None
    attempt_ordinal: int | None
    total_attempts: int | None
    is_restart: bool
    is_unclean_restart: bool
    attempt_reason: str | None
    previous_attempt_id: str | None
    activation_reason: str | None
    workdir_mode: str | None
    workdir_reused: bool
    unsafe_persistent_takeover: bool
    data_generation: int | None
    resources: Mapping[str, object]
    join: object
    raw: Mapping[str, Any]

    @classmethod
    def read(cls, path: str | os.PathLike[str]) -> Self:
        """Read and validate a manager-written attempt context."""

        value = read_json(Path(path))
        if value.get("format") != "httk-workflow-attempt-context" or value.get("format_version") != 1:
            raise ValueError("attempt context must use httk-workflow-attempt-context version 1")
        required = ("workspace_id", "job_id", "job_key", "placement", "step", "activation_id", "attempt_id")
        if any(not isinstance(value.get(name), str) or not value[name] for name in required):
            raise ValueError("attempt context is missing a required string identity")
        generation = value.get("data_generation")
        if generation is not None and (not isinstance(generation, int) or isinstance(generation, bool)):
            raise ValueError("attempt data_generation must be an integer or null")
        resources_raw = value.get("resources", {})
        if not isinstance(resources_raw, Mapping):
            raise ValueError("attempt resources must be an object")

        def optional_integer(name: str) -> int | None:
            raw = value.get(name)
            if raw is None:
                return None
            if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
                raise ValueError(f"attempt {name} must be a nonnegative integer or null")
            return raw

        def optional_string(name: str) -> str | None:
            raw = value.get(name)
            if raw is None:
                return None
            if not isinstance(raw, str):
                raise ValueError(f"attempt {name} must be a string or null")
            return raw

        return cls(
            workspace_id=value["workspace_id"],
            job_id=value["job_id"],
            job_key=value["job_key"],
            placement=value["placement"],
            step=value["step"],
            activation_id=value["activation_id"],
            attempt_id=value["attempt_id"],
            activation_ordinal=optional_integer("activation_ordinal"),
            attempt_ordinal=optional_integer("attempt_ordinal"),
            total_attempts=optional_integer("total_attempts"),
            is_restart=bool(value.get("is_restart", False)),
            is_unclean_restart=bool(value.get("is_unclean_restart", False)),
            attempt_reason=optional_string("attempt_reason"),
            previous_attempt_id=optional_string("previous_attempt_id"),
            activation_reason=optional_string("activation_reason"),
            workdir_mode=optional_string("workdir_mode"),
            workdir_reused=bool(value.get("workdir_reused", False)),
            unsafe_persistent_takeover=bool(value.get("unsafe_persistent_takeover", False)),
            data_generation=generation,
            resources=dict(resources_raw),
            join=value.get("join"),
            raw=value,
        )


@dataclass(frozen=True)
class CommandResult:
    """Result of an argv-only supervised child process."""

    argv: tuple[str, ...]
    returncode: int
    stdout: bytes
    stderr: bytes
    timed_out: bool


def run_command(
    argv: Sequence[str],
    *,
    timeout: float | None = None,
    cwd: str | os.PathLike[str] | None = None,
    environment: Mapping[str, str] | None = None,
    termination_grace: float = 10.0,
) -> CommandResult:
    """Run an argv array and terminate its process group on timeout."""

    command = tuple(argv)
    if not command or not all(isinstance(item, str) and item for item in command):
        raise ValueError("command must be a nonempty sequence of nonempty strings")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be positive")
    if termination_grace < 0:
        raise ValueError("termination_grace cannot be negative")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=None if environment is None else dict(environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            stdout, stderr = process.communicate(timeout=termination_grace)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
    return CommandResult(command, process.returncode, stdout, stderr, timed_out)


@dataclass(frozen=True)
class AttemptRuntime:
    """Paths and outcome publication methods exposed to a native v2 runner."""

    context: AttemptContext
    control: Path
    job: Path
    workdir: Path
    workspace: Path
    data: Path | None = None

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> Self:
        """Construct the runtime from manager-provided environment variables."""

        values = os.environ if environment is None else environment

        def required(name: str) -> str:
            value = values.get(name)
            if not value:
                raise ValueError(f"missing workflow runtime variable: {name}")
            return value

        context = AttemptContext.read(required("HTTK_WORKFLOW_CONTEXT"))
        data_value = values.get("HTTK_WORKFLOW_DATA_DIR")
        return cls(
            context=context,
            control=Path(required("HTTK_WORKFLOW_CONTROL_DIR")).resolve(),
            job=Path(required("HTTK_WORKFLOW_JOB_DIR")).resolve(),
            workdir=Path(required("HTTK_WORKFLOW_WORKDIR")).resolve(),
            workspace=Path(required("HTTK_WORKFLOW_WORKSPACE_DIR")).resolve(),
            data=None if not data_value else Path(data_value).resolve(),
        )

    @classmethod
    def initialize(cls, environment: Mapping[str, str] | None = None) -> Self:
        """Construct the runtime and replay every sealed workdir batch."""

        result = cls.from_environment(environment)
        ReplayableWorkdirBatch.recover(result.workdir)
        return result

    @property
    def state(self) -> WorkdirState:
        """Return the application state associated with this workdir."""

        return WorkdirState(self.workdir)

    @property
    def runlog(self) -> RunLog:
        """Return the structured application run log."""

        return RunLog(self.workdir)

    def outcome(self) -> OutcomeBuilder:
        """Start a composable unpublished outcome."""

        return OutcomeBuilder(self)

    def workdir_batch(self) -> ReplayableWorkdirBatch:
        """Start a replayable group of workdir changes."""

        return ReplayableWorkdirBatch.create(self.workdir)

    def publish(
        self,
        action: str,
        *,
        next_step: str | None = None,
        priority: int | None = None,
        failure: Mapping[str, object] | None = None,
        retry: Mapping[str, object] | None = None,
        join: Mapping[str, object] | None = None,
        pause: Mapping[str, object] | None = None,
        expected_data_generation: int | None = None,
    ) -> Path:
        """Atomically publish one authoritative request for the manager."""

        if action not in _OUTCOME_ACTIONS:
            raise ValueError(f"unsupported workflow outcome action: {action!r}")
        builder = self.outcome()
        try:
            return builder.publish(
                action,  # type: ignore[arg-type]
                next_step=next_step,
                priority=priority,
                failure=failure,
                retry=retry,
                join=join,
                pause=pause,
                expected_data_generation=expected_data_generation,
            )
        except Exception:
            if builder.root.exists():
                shutil.rmtree(builder.root)
            raise

    def advance(self, next_step: str, *, priority: int | None = None) -> Path:
        """Request a new activation at *next_step*."""

        return self.publish("advance", next_step=next_step, priority=priority)

    def succeed(self) -> Path:
        """Mark the job successful."""

        return self.publish("succeed")

    def fail(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        retryable: bool = False,
    ) -> Path:
        """Publish a structured terminal failure.

        ``retryable`` is advisory evidence for an operator or a later report.
        Manager retry policy is keyed on ``code`` through ``retry_on``.
        """

        failure = Failure(code, message, details=details, retryable=retryable)
        return self.publish("fail", failure=failure.as_mapping())

    def retry(self, reason: str) -> Path:
        """Request another attempt of the current activation."""

        return self.publish("retry", retry={"reason": reason})

    def pause(self, reason: str) -> Path:
        """Pause the job for operator action."""

        return self.publish("pause", pause={"reason": reason})

    def wait(
        self,
        next_step: str,
        outcome: OutcomeBuilder,
        *,
        join: JoinSpec | None = None,
        priority: int | None = None,
    ) -> Path:
        """Wait for the children of *outcome* before starting *next_step*.

        A join is resolvable only when the manager registers the children named
        by it, and children are registered from the very outcome bundle that
        publishes the wait. The wait therefore must be published through the
        builder that holds those children, never through a fresh childless one.
        Omit *join* for the default ``all_succeeded`` condition over exactly
        ``outcome.children``, or pass a :class:`~httk.workflow.JoinSpec` to select another
        condition.
        """

        if outcome.runtime is not self:
            raise ValueError("outcome builder belongs to a different attempt runtime")
        return outcome.publish("wait", next_step=next_step, join=join, priority=priority)
