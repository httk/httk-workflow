"""Application-side helpers for native v2 workflow attempts.

This API publishes the v2 outcome protocol directly. It is intentionally not
a Python spelling of the v1 ``HT_TASK_*`` functions.
"""

import os
import signal
import subprocess
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from ._util import read_json, write_json_atomic

_OUTCOME_ACTIONS = frozenset({"advance", "retry", "wait", "succeed", "fail", "pause"})


@dataclass(frozen=True)
class AttemptContext:
    """The immutable identity and restart evidence for one running attempt."""

    store_id: str
    job_id: str
    job_key: str
    placement: str
    step: str
    activation_id: str
    attempt_id: str
    is_restart: bool
    is_unclean_restart: bool
    data_generation: int | None
    raw: Mapping[str, Any]

    @classmethod
    def read(cls, path: str | os.PathLike[str]) -> Self:
        """Read and validate a manager-written attempt context."""

        value = read_json(Path(path))
        if value.get("format") != "httk-workflow-attempt-context" or value.get("format_version") != 1:
            raise ValueError("attempt context must use httk-workflow-attempt-context version 1")
        required = ("store_id", "job_id", "job_key", "placement", "step", "activation_id", "attempt_id")
        if any(not isinstance(value.get(name), str) or not value[name] for name in required):
            raise ValueError("attempt context is missing a required string identity")
        generation = value.get("data_generation")
        if generation is not None and (not isinstance(generation, int) or isinstance(generation, bool)):
            raise ValueError("attempt data_generation must be an integer or null")
        return cls(
            store_id=value["store_id"],
            job_id=value["job_id"],
            job_key=value["job_key"],
            placement=value["placement"],
            step=value["step"],
            activation_id=value["activation_id"],
            attempt_id=value["attempt_id"],
            is_restart=bool(value.get("is_restart", False)),
            is_unclean_restart=bool(value.get("is_unclean_restart", False)),
            data_generation=generation,
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
    workspace: Path
    store: Path
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
            workspace=Path(required("HTTK_WORKFLOW_RUN_DIR")).resolve(),
            store=Path(required("HTTK_WORKFLOW_STORE_DIR")).resolve(),
            data=None if not data_value else Path(data_value).resolve(),
        )

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
        if priority is not None and (isinstance(priority, bool) or not 0 <= priority <= 999):
            raise ValueError("priority must be an integer from 0 through 999")
        ready = self.control / "outcome.ready"
        if ready.exists():
            raise FileExistsError(f"an outcome is already published: {ready}")
        temporary = self.control / f"outcome.tmp.{uuid.uuid4()}"
        temporary.mkdir()
        body: dict[str, object] = {
            "format": "httk-workflow-outcome",
            "format_version": 1,
            "job_id": self.context.job_id,
            "activation_id": self.context.activation_id,
            "attempt_id": self.context.attempt_id,
            "action": action,
        }
        optional = {
            "next_step": next_step,
            "priority": priority,
            "failure": None if failure is None else dict(failure),
            "retry": None if retry is None else dict(retry),
            "join": None if join is None else dict(join),
            "pause": None if pause is None else dict(pause),
            "expected_data_generation": expected_data_generation,
        }
        body.update({name: value for name, value in optional.items() if value is not None})
        try:
            write_json_atomic(temporary / "outcome.json", body)
            os.rename(temporary, ready)
        except Exception:
            if temporary.exists():
                for child in temporary.iterdir():
                    child.unlink()
                temporary.rmdir()
            raise
        return ready

    def advance(self, next_step: str, *, priority: int | None = None) -> Path:
        """Request a new activation at *next_step*."""

        return self.publish("advance", next_step=next_step, priority=priority)

    def succeed(self) -> Path:
        """Mark the job successful."""

        return self.publish("succeed")

    def fail(
        self,
        error_class: str,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> Path:
        """Publish a structured terminal failure."""

        failure: dict[str, object] = {"class": error_class, "message": message}
        if details is not None:
            failure["details"] = dict(details)
        return self.publish("fail", failure=failure)

    def retry(self, reason: str) -> Path:
        """Request another attempt of the current activation."""

        return self.publish("retry", retry={"reason": reason})

    def pause(self, reason: str) -> Path:
        """Pause the job for operator action."""

        return self.publish("pause", pause={"reason": reason})
