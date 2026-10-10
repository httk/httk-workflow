"""Foreground debugging of one job: a private manager claims exactly that job and drives it."""

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .. import _joins, _kernel
from .._durations import TIME_RESOURCES
from .._job import JobDefinition
from .._kernel import JobRef
from .._state import TERMINAL_STATES, StateDoc
from ..errors import WorkflowError
from ..manager import TaskManager
from ..models import LOGS_DIRECTORY, normalize_placement, placement_text, validate_step
from ..scaffold import submit_payload
from ..workspace import Workspace
from ._diagnosis import observe_join
from ._reading import read_job, read_job_tail, read_state, resolve_job, resolve_job_selectors

DEBUG_EXIT_SUCCEEDED = 0
DEBUG_EXIT_FAILED = 3
DEBUG_EXIT_UNFINISHED = 4
_TAIL_CHUNK = 1 << 20


@dataclass(frozen=True)
class DebugOutcome:
    """How one foreground debug run of a job ended.

    :param job_id: The job UUID.
    :param job_key: The job key.
    :param state: The state it stopped in.
    :param exit_code: The command's exit code for that state.
    """

    job_id: str
    job_key: str
    state: str
    exit_code: int

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this outcome.

        :return: The mapping.
        """

        return {"job_id": self.job_id, "job_key": self.job_key, "state": self.state, "exit_code": self.exit_code}


def _exit_code(state: str) -> int:
    if state == "succeeded":
        return DEBUG_EXIT_SUCCEEDED
    if state == "failed":
        return DEBUG_EXIT_FAILED
    return DEBUG_EXIT_UNFINISHED


class _Tail:
    """A console tail of one growing attempt log, ``logs/stdio.out`` of *job*."""

    def __init__(self, job: Path, write: Callable[[str], None]) -> None:
        self.job = job
        self._write = write
        self._offset = 0
        self._partial = ""

    def pump(self, *, final: bool = False) -> None:
        """Print every complete line that appeared since the last pump."""

        while True:
            try:
                data, self._offset = read_job_tail(self.job, f"{LOGS_DIRECTORY}/stdio.out", _TAIL_CHUNK, self._offset)
            except (WorkflowError, OSError):
                return
            lines = (self._partial + data.decode("utf-8", "replace")).split("\n")
            self._partial = lines.pop()
            for line in lines:
                self._write(line)
            if len(data) < _TAIL_CHUNK:
                break
        if final and self._partial:
            self._write(self._partial)
            self._partial = ""


def _submit_payload(workspace: Workspace, source: Path, placement: str, step: str | None) -> JobRef:
    """Submit a copy of a prepared payload at *placement* (and *step*) from a CLI owner's scratch."""

    changes: dict[str, object] = {"placement": placement_text(normalize_placement(placement))}
    if step is not None:
        changes["initial_step"] = validate_step(step, "step")
    with _kernel.register_owner(workspace, kind="cli", label="job debug", allocation=None, advertised={}) as owner:
        return submit_payload(workspace, owner, source, move=False, changes=changes)


def debug_job(
    workspace: Workspace,
    target: str,
    *,
    placement: str = "debug",
    step: str | None = None,
    follow_children: bool = False,
    timeout: float = 3600.0,
    poll_interval: float = 0.05,
    emit: Callable[[str], None] | None = None,
    cwd: Path | None = None,
) -> DebugOutcome:
    """Drive one job to a state where it stops progressing, in the foreground.

    :param workspace: Run the job in this workspace.
    :param target: Identify an existing job, or give a prepared payload directory to submit.
    :param placement: Place a newly submitted payload here.
    :param step: Override the initial step of a newly submitted payload.
    :param follow_children: Drive the children of a waiting job too.
    :param timeout: Stop after this many seconds.
    :param poll_interval: Delay between foreground observations.
    :param emit: Write debug lines with this callback.
    :param cwd: Resolve relative job paths from this directory.
    :return: The final state and exit status of the debugged job.
    :raises ValueError: For a selector naming no job or several, or a step override of an existing job.
    :raises TimeoutError: When the job does not stop progressing within *timeout*.
    """

    write = emit if emit is not None else _print_line
    base = (cwd if cwd is not None else Path.cwd()).resolve()
    source = Path(target).expanduser()
    candidate = source if source.is_absolute() else base / source
    if any(character in target for character in "*?[") or (
        candidate.exists() and candidate.resolve().is_relative_to(workspace.root.resolve())
    ):
        refs = resolve_job_selectors(workspace, base, [target])
        if len(refs) != 1:
            raise ValueError("job debug takes one job")
        ref = refs[0]
    elif candidate.is_dir() and (candidate / "job.json").is_file():
        ref = _submit_payload(workspace, candidate, placement, step)
        write(f"[debug] submitted {ref.job_key} at {placement_text(normalize_placement(placement))}")
        if step is not None:
            write(f"[debug] initial step overridden to {step}")
        step = None
    else:
        ref = resolve_job(workspace, target)
    if step is not None:
        raise ValueError(
            f"job {ref.job_key} already exists, so its step cannot be overridden here; post an operator request "
            "instead: 'httk job request override_step --workspace WORKSPACE --step STEP --operator NAME --reason WHY JOB'"
        )
    final = _drive(
        workspace,
        ref,
        label=None,
        follow_children=follow_children,
        timeout=timeout,
        poll_interval=poll_interval,
        write=write,
    )
    return DebugOutcome(final.job_id, final.job_key, final.state, _exit_code(final.state))


def _print_line(line: str) -> None:
    print(line, flush=True)


def _debug_capacity(job: JobDefinition, dynamic: Mapping[str, object] | None = None) -> dict[str, int]:
    """Return capacity sufficient for every declared consumable requirement of *job*."""

    capacity: dict[str, int] = {}
    for requirement in (job.resources, *job.step_resources.values(), dynamic or {}):
        if not isinstance(requirement, Mapping):
            continue
        for name, value in requirement.items():
            if name not in TIME_RESOURCES and isinstance(value, int):
                capacity[name] = max(capacity.get(name, 0), max(1, value))
    return capacity


def _summary(doc: StateDoc | None) -> str:
    """Summarize a ``state.json`` on one console line."""

    if doc is None:
        return "never claimed"
    activation, attempt, failure = doc.activation or {}, doc.attempt or {}, doc.failure
    parts = [f"step={activation.get('step') or '-'}", f"reason={activation.get('reason') or '-'}"]
    if attempt.get("ordinal"):
        parts.append(f"attempt={attempt.get('ordinal')}")
    if failure is not None:
        parts.append(f"failure={failure.get('code')}: {failure.get('message')}")
    return " ".join(parts)


def _drive(
    workspace: Workspace,
    ref: JobRef,
    *,
    label: str | None,
    follow_children: bool,
    timeout: float,
    poll_interval: float,
    write: Callable[[str], None],
) -> JobRef:
    """Drive one job with a private manager that claims only this job, until it stops progressing."""

    meta = "debug" if label is None else f"debug child:{label}"
    job, job_error = read_job(ref)
    if job is None:
        raise ValueError(f"cannot debug {ref.job_key}: {job_error}")
    write(f"[{meta}] {ref.job_key} at {placement_text(job.placement)} is {ref.state} (workflow {job.workflow_id})")
    tail = _Tail(ref.path, write)
    deadline = time.monotonic() + timeout
    seen: tuple[str, str] | None = None
    driven_children = False
    with TaskManager(
        workspace,
        accept_any_pool=True,
        capabilities=sorted(job.required_capabilities),
        resources=_debug_capacity(job),
        maximum_workers=1,
        heartbeat_interval=0.01,
        join_grace_seconds=timeout + 60.0,
    ) as manager:
        while True:
            manager.owner.check_alive()
            current = _kernel.locate(workspace, ref.job_id, placement_hint=job.placement, settle=True)
            if current is None:
                raise ValueError(f"job {ref.job_key} disappeared while being debugged")
            doc, _ = read_state(current)
            tail.job = current.path
            tail.pump()
            if (current.state, current.token) != seen:
                seen = (current.state, current.token)
                write(f"[{meta}] {current.state} {_summary(doc)}")
            if current.state in TERMINAL_STATES or current.state == "paused":
                tail.pump(final=True)
                write(f"[{meta}] {current.job_key} finished as {current.state}")
                return current
            if current.state == "ready":
                # This single-job foreground manager sizes its capacity to the job's latest dynamic resources.
                manager.resources = _debug_capacity(job, None if doc is None else doc.resources)
                manager._claim_and_launch(current)
            elif current.state == "waiting":
                if not follow_children:
                    write(
                        f"[{meta}] {current.job_key} waits for its children; rerun with --follow-children to drive them here"
                    )
                    return current
                if not driven_children:
                    driven_children = True
                    _drive_children(workspace, current, doc, timeout=timeout, poll_interval=poll_interval, write=write)
                    deadline = time.monotonic() + timeout
                decision = _joins.evaluate(
                    workspace,
                    current,
                    cache=_kernel.ListingCache(),
                    unresolved_since={},
                    grace=math.inf,
                    now=time.time(),
                )
                if decision is not None:
                    manager._decide_join(current, decision)
            manager._supervise()
            if time.monotonic() >= deadline:
                raise TimeoutError(f"debugging {current.job_key} did not reach a terminal state within {timeout:.0f}s")
            time.sleep(poll_interval)


def _drive_children(
    workspace: Workspace,
    ref: JobRef,
    doc: StateDoc | None,
    *,
    timeout: float,
    poll_interval: float,
    write: Callable[[str], None],
) -> None:
    """Drive every child of one waiting job, depth first."""

    observations = [] if doc is None or doc.join is None else observe_join(workspace, doc.join)
    if not observations:
        write(f"[debug] {ref.job_key} waits on a join with no readable child")
        return
    for observation in observations:
        label = observation.get("label")
        name = label or observation.get("job_key")
        if observation.get("kind") is None:
            write(f"[debug] child {name} is not resolvable in this workspace; leaving it to the manager")
            continue
        if observation.get("terminal"):
            write(f"[debug] child {name} is already {observation.get('kind')}")
            continue
        child = _kernel.locate(workspace, str(observation["job_id"]), placement_hint=None, exhaustive=True)
        if child is not None:
            _drive(
                workspace,
                child,
                label=str(label) if label else child.job_id[:8],
                follow_children=True,
                timeout=timeout,
                poll_interval=poll_interval,
                write=write,
            )
