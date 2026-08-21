"""Foreground debugging of one job with a scoped private manager."""

import json
import shutil
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .._util import read_json
from ..errors import WorkspaceCorruptionError
from ..manager import TaskManager
from ..manifests import read_maintenance_lock
from ..models import CORE_STATE_KINDS, TERMINAL_KINDS, Marker, validate_step
from ..workspace import MarkerFault, Workspace
from ._diagnosis import observe_join
from ._reading import (
    _attempt_control,
    _job_of,
    _optional_string,
    _state_of,
    resolve_job,
)

DEBUG_EXIT_SUCCEEDED = 0
DEBUG_EXIT_FAILED = 3
DEBUG_EXIT_UNFINISHED = 4


class ScopedWorkspace(Workspace):
    """A workspace whose scheduling scans observe only the named jobs."""

    def __init__(self, root: str | Path, scope: Iterable[str], *, durable: bool = True) -> None:
        super().__init__(root, durable=durable)
        self.scope = frozenset(scope)

    def scan_marker_entries(self, kinds: Iterable[str] | None = None) -> Iterator[Marker | MarkerFault]:
        """Yield only the markers of the scoped jobs, plus every unusable entry."""

        for entry in super().scan_marker_entries(kinds):
            if isinstance(entry, MarkerFault) or entry.job_key in self.scope:
                yield entry

    def _scheduling_includes(self, marker: Marker) -> bool:
        """Restrict the streaming scheduler to the scoped jobs."""

        return marker.job_key in self.scope

    def _unscoped_markers(self, kinds: Iterable[str] | None = None) -> Iterator[Marker]:
        for entry in Workspace.scan_marker_entries(self, kinds):
            if isinstance(entry, Marker):
                yield entry
            else:
                self.report_marker_fault(entry)

    def find_markers(self, job_key: str, kinds: Iterable[str] | None = None) -> list[Marker]:
        """Find one job key anywhere in the workspace, ignoring the scope."""

        selected = tuple(kinds or CORE_STATE_KINDS)
        if set(selected) <= set(CORE_STATE_KINDS):
            return super().find_markers(job_key, selected)
        return [marker for marker in self._unscoped_markers(selected) if marker.job_key == job_key]


@dataclass(frozen=True)
class DebugOutcome:
    """How one foreground debug run of a job ended."""

    job_id: str
    job_key: str
    state: str
    exit_code: int

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this outcome."""

        return {
            "job_id": self.job_id,
            "job_key": self.job_key,
            "state": self.state,
            "exit_code": self.exit_code,
        }


def _exit_code(kind: str) -> int:
    if kind == "succeeded":
        return DEBUG_EXIT_SUCCEEDED
    if kind == "failed":
        return DEBUG_EXIT_FAILED
    return DEBUG_EXIT_UNFINISHED


class _Tail:
    """A console tail of one growing attempt log."""

    def __init__(self, path: Path, prefix: str, write: Callable[[str], None]) -> None:
        self.path = path
        self.prefix = prefix
        self._write = write
        self._offset = 0
        self._partial = ""

    def pump(self, *, final: bool = False) -> None:
        """Print every complete line that appeared since the last pump."""

        try:
            with self.path.open("rb") as handle:
                handle.seek(self._offset)
                data = handle.read()
                self._offset = handle.tell()
        except OSError:
            return
        if data:
            text = self._partial + data.decode("utf-8", "replace")
            lines = text.split("\n")
            self._partial = lines.pop()
            for line in lines:
                self._write(f"{self.prefix}{line}")
        if final and self._partial:
            self._write(f"{self.prefix}{self._partial}")
            self._partial = ""


def _stage_payload_with_step(source: Path, step: str) -> Path:
    """Copy one payload into a staging directory whose initial step is *step*."""

    validate_step(step, "step")
    staging = Path(tempfile.mkdtemp(prefix="httk-debug-"))
    payload = staging / "payload"
    shutil.copytree(source, payload, symlinks=False)
    definition = read_json(payload / "job.json")
    definition["initial_step"] = step
    (payload / "job.json").write_text(json.dumps(definition, sort_keys=True), encoding="utf-8")
    return payload


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
) -> DebugOutcome:
    """Drive one job to a terminal state in the foreground."""

    write = emit if emit is not None else _print_line
    lock = read_maintenance_lock(workspace)
    if lock is not None and not lock.is_stale():
        raise ValueError(
            f"the workspace maintenance lock held by {lock.describe()} pauses every launch; "
            "release it with 'httk workflow workspace unlock WORKSPACE' first"
        )
    source = Path(target).expanduser()
    if (source / "job.json").is_file():
        staged: Path | None = None
        try:
            if step is not None:
                staged = _stage_payload_with_step(source, step)
                marker = workspace.submit(staged, placement)
            else:
                marker = workspace.submit(source, placement)
        finally:
            if staged is not None:
                shutil.rmtree(staged.parent, ignore_errors=True)
        write(f"[debug] submitted {marker.job_key} at {marker.placement.as_posix()}")
        if step is not None:
            write(f"[debug] initial step overridden to {step}")
    else:
        marker = resolve_job(workspace, target)
        if step is not None:
            raise ValueError(
                f"job {marker.job_key} already has a history, so its step cannot be overridden here; "
                "publish an operator request instead: "
                "'httk workflow job request override_step WORKSPACE JOB --step STEP --operator NAME --reason WHY'"
            )
    final = _drive(
        workspace,
        marker,
        label=None,
        follow_children=follow_children,
        timeout=timeout,
        poll_interval=poll_interval,
        write=write,
    )
    return DebugOutcome(
        job_id=final.job_id,
        job_key=final.job_key,
        state=final.kind,
        exit_code=_exit_code(final.kind),
    )


def _print_line(line: str) -> None:
    print(line, flush=True)


def _drive(
    workspace: Workspace,
    marker: Marker,
    *,
    label: str | None,
    follow_children: bool,
    timeout: float,
    poll_interval: float,
    write: Callable[[str], None],
) -> Marker:
    """Drive one job with a private manager until it stops making progress."""

    meta = "debug" if label is None else f"debug child:{label}"
    context = "" if label is None else f"child:{label} "
    job, job_error = _job_of(workspace, marker)
    if job_error is not None:
        raise ValueError(f"cannot debug {marker.job_key}: {job_error}")
    assert job is not None
    write(
        f"[{meta}] {marker.job_key} at {marker.placement.as_posix()} is {marker.kind} "
        f"(runner {job.runner_source}:{job.runner_path.as_posix()} on executor {job.runner_executor})"
    )
    scoped = ScopedWorkspace(workspace.root, {marker.job_key}, durable=workspace.durable)
    tails: dict[str, list[_Tail]] = {}
    deadline = time.monotonic() + timeout
    seen_generation = -1
    driven_children = False
    current = marker
    with TaskManager(
        scoped,
        capabilities=sorted(job.required_capabilities),
        accept_any_pool=True,
        maximum_workers=1,
        heartbeat_interval=0.01,
        join_grace_seconds=timeout + 60.0,
    ) as manager:
        while True:
            manager.tick()
            found = workspace.find_marker_by_id(marker.job_id)
            if found is None:
                raise WorkspaceCorruptionError(f"job {marker.job_key} lost its state marker while being debugged")
            current = found
            state, _ = _state_of(workspace, current)
            _pump(workspace, current, state, tails, context, write)
            if current.generation != seen_generation:
                seen_generation = current.generation
                write(f"[{meta}] g{current.generation} {current.kind} {_frame_summary(state)}")
            if current.kind in TERMINAL_KINDS or current.kind == "paused":
                for group in tails.values():
                    for tail in group:
                        tail.pump(final=True)
                write(f"[{meta}] {current.job_key} finished as {current.kind}")
                return current
            if current.kind == "waiting":
                if not follow_children:
                    write(
                        f"[{meta}] {current.job_key} waits for its children; rerun with --follow-children to drive them here"
                    )
                    return current
                if not driven_children:
                    driven_children = True
                    _drive_children(
                        workspace,
                        current,
                        state,
                        timeout=timeout,
                        poll_interval=poll_interval,
                        write=write,
                    )
                    deadline = time.monotonic() + timeout
                    continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"debugging {current.job_key} did not reach a terminal state within {timeout:.0f}s")
            time.sleep(poll_interval)


def _frame_summary(state: Mapping[str, Any]) -> str:
    """Summarize one state frame on a single console line."""

    parts = [f"step={state.get('step') or '-'}", f"reason={state.get('reason') or '-'}"]
    ordinal = state.get("attempt_ordinal")
    if ordinal:
        parts.append(f"attempt={ordinal}")
    failure = state.get("failure")
    if isinstance(failure, Mapping):
        parts.append(f"failure={failure.get('code')}: {failure.get('message')}")
    return " ".join(parts)


def _pump(
    workspace: Workspace,
    marker: Marker,
    state: Mapping[str, Any],
    tails: dict[str, list[_Tail]],
    context: str,
    write: Callable[[str], None],
) -> None:
    """Stream whatever the attempts of this job have written since the last pass."""

    attempt_id = _optional_string(state.get("attempt_id"))
    control = _attempt_control(workspace, marker, state)
    if attempt_id is not None and attempt_id not in tails and control is not None and control.is_dir():
        step = state.get("step") or "-"
        tails[attempt_id] = [
            _Tail(control / "stdout.log", f"[{context}{step}] ", write),
            _Tail(control / "stderr.log", f"[{context}{step}] (stderr) ", write),
        ]
    for group in tails.values():
        for tail in group:
            tail.pump()


def _drive_children(
    workspace: Workspace,
    marker: Marker,
    state: Mapping[str, Any],
    *,
    timeout: float,
    poll_interval: float,
    write: Callable[[str], None],
) -> None:
    """Drive every child of one waiting job depth first."""

    join = state.get("join")
    observations = observe_join(workspace, join) if isinstance(join, Mapping) else []
    if not observations:
        write(f"[debug] {marker.job_key} waits on a join with no readable child")
        return
    for observation in observations:
        label = observation.get("label")
        identity = observation.get("job_id") or observation.get("job_key")
        if observation.get("kind") is None:
            write(f"[debug] child {label or identity} is not resolvable in this workspace; leaving it to the manager")
            continue
        if observation.get("terminal"):
            write(f"[debug] child {label or identity} is already {observation.get('kind')}")
            continue
        child = workspace.find_marker_by_id(str(observation["job_id"]))
        if child is None:
            continue
        _drive(
            workspace,
            child,
            label=str(label) if label else str(child.job_id)[:8],
            follow_children=True,
            timeout=timeout,
            poll_interval=poll_interval,
            write=write,
        )
