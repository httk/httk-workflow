"""Attempt identity and supervised commands.

The attempt context and :func:`run_command` are protocol-level facilities every
native runner uses; the environment-binding helper below is what the authoring
SDK and the Bash bridge both recover an attempt through.
"""

import os
import signal
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from ._util import read_json

__all__ = [
    "AttemptContext",
    "CommandResult",
    "run_command",
]


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
    #: Whether the workspace this attempt runs in claims storage-crash
    #: durability. A runner threads it into every artifact it publishes so an
    #: outcome, a transaction, or a spawned child is synchronized before it is
    #: renamed authoritative. An old context that predates the member reads as
    #: ``False``: process-interruption safety only, which is what such a context
    #: was written under.
    durable: bool
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
            durable=bool(value.get("durable", False)),
            resources=dict(resources_raw),
            join=value.get("join"),
            raw=value,
        )


@dataclass(frozen=True)
class _AttemptEnvironment:
    """The attempt one process was launched for, read from the environment."""

    context: AttemptContext
    control: Path
    payload: Path
    workdir: Path
    workspace: Path
    data: Path | None
    step: str


def _read_environment(environment: Mapping[str, str] | None = None) -> _AttemptEnvironment:
    """Bind one process to the attempt its manager launched it for.

    Binding is a pure read: nothing on disk changes, so a caller that only wants
    to inspect the attempt is free to do it. Recovering an interrupted attempt is
    a separate, deliberate step.
    """

    values = os.environ if environment is None else environment

    def required(name: str) -> str:
        value = values.get(name)
        if not value:
            raise ValueError(f"missing workflow runtime variable: {name}")
        return value

    context = AttemptContext.read(required("HTTK_WORKFLOW_CONTEXT"))
    step = values.get("HTTK_WORKFLOW_STEP") or context.step
    if step != context.step:
        raise ValueError(
            f"HTTK_WORKFLOW_STEP is {step!r} but the attempt context names step {context.step!r}",
        )
    data_value = values.get("HTTK_WORKFLOW_DATA_DIR")
    return _AttemptEnvironment(
        context=context,
        control=Path(required("HTTK_WORKFLOW_CONTROL_DIR")).resolve(),
        payload=Path(required("HTTK_WORKFLOW_JOB_DIR")).resolve(),
        workdir=Path(required("HTTK_WORKFLOW_WORKDIR")).resolve(),
        workspace=Path(required("HTTK_WORKFLOW_WORKSPACE_DIR")).resolve(),
        data=None if not data_value else Path(data_value).resolve(),
        step=step,
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
