"""Authoritative job, marker, state, and journal readers."""

import glob
import json
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from .._util import read_json
from ..errors import FormatError, WorkflowError, WorkspaceCorruptionError
from ..journal import read_record
from ..models import (
    ATTEMPTS_DIRECTORY,
    LOGS_DIRECTORY,
    STATE_KINDS,
    JobDefinition,
    Marker,
    normalize_placement,
    validate_attempt_control,
)
from ..workspace import Workspace

JOB_HISTORY_FORMAT = "httk-workflow-job-history"
JOB_LIST_FORMAT = "httk-workflow-job-list"
_HISTORY_READ_DEADLINE_SECONDS = 0.1
#: How much of a runlog's tail to read when surfacing its last headline. A
#: runlog can grow without bound, so the report reads only the final slice.
_RUNLOG_TAIL_BYTES = 65536


def read_last_headline(payload: str | Path | None) -> str | None:
    """Return the message of the last ``headline`` run-log event, if any.

    The runlog is JSON lines a runner appends in its payload; a ``headline``
    event is the runner's own one-line summary of where it is. Only the tail of
    the file is read, so a long-running runner's headline is cheap to surface.

    :param payload: The job payload whose ``logs/runlog.jsonl`` to read.
    :return: The last headline message, or ``None`` when there is none to read.
    """

    if payload is None:
        return None
    path = Path(payload) / LOGS_DIRECTORY / "runlog.jsonl"
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            handle.seek(max(0, handle.tell() - _RUNLOG_TAIL_BYTES))
            data = handle.read()
    except OSError:
        return None
    headline: str | None = None
    for raw in data.split(b"\n"):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
        except ValueError:
            continue
        if isinstance(event, Mapping) and event.get("kind") == "headline" and isinstance(event.get("message"), str):
            headline = event["message"]
    return headline


def resolve_job(workspace: Workspace, selector: str) -> Marker:
    """Return the marker of the one job *selector* names.

    A selector is a job UUID, a complete ``tag--uuid`` job key, or any unique
    prefix of either. An ambiguous selector is refused with the candidates it
    matched rather than resolved arbitrarily.
    """

    markers = list(workspace.scan_markers(STATE_KINDS))
    exact = [marker for marker in markers if selector in {marker.job_id, marker.job_key}]
    if len(exact) > 1:
        raise WorkspaceCorruptionError(f"job {selector} has more than one state marker")
    if exact:
        return exact[0]
    if not selector:
        raise ValueError("a job selector cannot be empty")
    matches = [
        marker for marker in markers if marker.job_id.startswith(selector) or marker.job_key.startswith(selector)
    ]
    if not matches:
        raise ValueError(f"no job in {workspace.root} matches {selector!r}")
    if len(matches) > 1:
        candidates = ", ".join(sorted(marker.job_key for marker in matches)[:5])
        raise ValueError(f"job selector {selector!r} matches {len(matches)} jobs: {candidates}")
    return matches[0]


def selector_is_path(cwd: Path, selector: str) -> bool:
    """Return whether *selector* must be interpreted as a local path."""

    return any(character in selector for character in "*?[") or (cwd / selector).exists()


def selector_uses_remote_path(cwd: Path, selector: str) -> bool:
    """Return whether a selector cannot be forwarded to a remote workspace."""

    return "/" in selector or selector_is_path(cwd, selector)


class JobSelectorResolver:
    """Resolve job selectors while sharing one marker scan for a batch."""

    def __init__(self, workspace: Workspace, cwd: Path) -> None:
        self.workspace = workspace
        self.cwd = cwd.resolve()
        self._markers: list[Marker] | None = None

    def _all_markers(self) -> list[Marker]:
        if self._markers is None:
            self._markers = list(self.workspace.scan_markers(STATE_KINDS))
        return self._markers

    def _resolve_id(self, selector: str) -> Marker:
        markers = self._all_markers()
        exact = [marker for marker in markers if selector in {marker.job_id, marker.job_key}]
        if len(exact) > 1:
            raise WorkspaceCorruptionError(f"job {selector} has more than one state marker")
        if exact:
            return exact[0]
        if not selector:
            raise ValueError("a job selector cannot be empty")
        matches = [
            marker for marker in markers if marker.job_id.startswith(selector) or marker.job_key.startswith(selector)
        ]
        if not matches:
            raise ValueError(f"no job in {self.workspace.root} matches {selector!r}")
        if len(matches) > 1:
            candidates = ", ".join(sorted(marker.job_key for marker in matches)[:5])
            raise ValueError(f"job selector {selector!r} matches {len(matches)} jobs: {candidates}")
        return matches[0]

    def _resolve_job_definition(self, path: Path, display_path: str) -> list[Marker]:
        job = JobDefinition.from_path(path)
        matches = [marker for marker in self._all_markers() if marker.job_id == job.id]
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job.id} has more than one state marker")
        if not matches:
            raise ValueError(f"{display_path} is a job directory without a state marker (removed job?)")
        return matches

    def _resolve_path(self, path_name: str) -> list[Marker]:
        path = Path(path_name)
        resolved = (path if path.is_absolute() else self.cwd / path).resolve()
        root = self.workspace.root.resolve()
        if not resolved.is_relative_to(root):
            raise ValueError(f"{path_name} is not inside workspace {root}")

        if resolved.is_file():
            raise ValueError(f"{path_name} is a file, not a job directory")
        if not resolved.is_dir():
            raise ValueError(f"{path_name} is not a job directory or placement directory")
        job_json = resolved / "job.json"
        if job_json.is_file():
            return self._resolve_job_definition(job_json, path_name)

        matches = [
            marker
            for marker in self._all_markers()
            if self.workspace.payload_path(marker.placement, marker.job_key).resolve().is_relative_to(resolved)
        ]
        if not matches:
            raise ValueError(f"no jobs below {path_name}")
        return sorted(
            matches,
            key=lambda marker: str(self.workspace.payload_path(marker.placement, marker.job_key).resolve()),
        )

    def resolve_one(self, selector: str) -> list[Marker]:
        """Resolve one selector, expanding a glob or returning matching jobs."""

        if any(character in selector for character in "*?["):
            paths = sorted(glob.glob(selector, root_dir=self.cwd))
            if not paths:
                raise ValueError(f"no path matches {selector!r} below {self.cwd}")
            markers: list[Marker] = []
            for path in paths:
                markers.extend(self._resolve_path(path))
            return self._deduplicate(markers)
        if (self.cwd / selector).exists():
            return self._deduplicate(self._resolve_path(selector))
        return [self._resolve_id(selector)]

    @staticmethod
    def _deduplicate(markers: Iterable[Marker]) -> list[Marker]:
        """Remove repeated jobs while retaining their first expansion order."""

        unique: list[Marker] = []
        seen: set[str] = set()
        for marker in markers:
            if marker.job_id not in seen:
                seen.add(marker.job_id)
                unique.append(marker)
        return unique


def resolve_job_selector(workspace: Workspace, cwd: Path, selector: str) -> list[Marker]:
    """Resolve one job selector relative to *cwd*.

    :param workspace: Provide the jobs and their live state markers.
    :param cwd: Resolve relative path selectors from this directory.
    :param selector: Name, prefix, path, or glob naming jobs.
    :return: The live markers named by the selector, in payload-path order for globs.
    """

    return JobSelectorResolver(workspace, cwd).resolve_one(selector)


def resolve_job_selectors(workspace: Workspace, cwd: Path, selectors: Iterable[str]) -> list[Marker]:
    """Resolve and deduplicate job selectors relative to *cwd*.

    :param workspace: Provide the jobs and their live state markers.
    :param cwd: Resolve relative path selectors from this directory.
    :param selectors: Selectors in the order supplied by the operator.
    :return: Unique live markers preserving selector and expansion order.
    """

    resolver = JobSelectorResolver(workspace, cwd)
    resolved: list[Marker] = []
    seen: set[str] = set()
    for selector in selectors:
        for marker in resolver.resolve_one(selector):
            if marker.job_id not in seen:
                seen.add(marker.job_id)
                resolved.append(marker)
    return resolved


def _state_of(workspace: Workspace, marker: Marker) -> tuple[dict[str, Any], str | None]:
    """Return one job's state frame, reporting rather than raising on damage."""

    try:
        return workspace.read_state(marker), None
    except (WorkflowError, OSError) as exc:
        return {}, str(exc)


def _job_of(workspace: Workspace, marker: Marker) -> tuple[JobDefinition | None, str | None]:
    """Return one job's immutable definition, reporting rather than raising."""

    try:
        return workspace.load_job(marker), None
    except (WorkflowError, OSError) as exc:
        return None, str(exc)


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _workdir_relative(job: JobDefinition | None, state: Mapping[str, Any]) -> PurePosixPath | None:
    """Return the payload-relative workdir of one job's current attempt."""

    recorded = _optional_string(state.get("workdir"))
    if recorded is not None:
        return PurePosixPath(recorded)
    if job is None:
        return None
    if job.workdir_mode == "persistent":
        return job.workdir_path
    attempt_id = _optional_string(state.get("attempt_id"))
    if attempt_id is None:
        return None
    base = job.workdir_path
    return base.parent / f"{base.name}.{attempt_id}"


def _attempt_control(workspace: Workspace, marker: Marker, state: Mapping[str, Any]) -> Path | None:
    """Return the attempt control directory of one job's last attempt."""

    name = _optional_string(state.get("attempt_control"))
    if name is None:
        attempt_id = _optional_string(state.get("attempt_id"))
        name = None if attempt_id is None else f"{ATTEMPTS_DIRECTORY}/{attempt_id}"
    if name is None:
        return None
    try:
        name = validate_attempt_control(name, "state.attempt_control")
    except FormatError:
        return None
    return workspace.payload_path(marker.placement, marker.job_key) / name


def read_error_breadcrumb(control: Path | None) -> dict[str, Any] | None:
    """Return the ``error.json`` breadcrumb of one attempt, when it left one."""

    if control is None:
        return None
    try:
        return read_json(control / "error.json")
    except WorkflowError:
        return None


def job_frames(workspace: Workspace, marker: Marker, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Return the state frames of one job, oldest first.

    The walk starts at the frame the authoritative marker names and follows
    ``previous_record_ref`` backward, which is the only ordering the protocol
    guarantees. A frame that cannot be read is reported in place as an ``error``
    entry and ends the walk: whatever history remains readable is still shown.
    """

    frames: list[dict[str, Any]] = []
    record_ref: str | None = None if marker.record_ref == "init" else marker.record_ref
    seen: set[str] = set()
    while record_ref is not None:
        if record_ref in seen:
            frames.append(
                {
                    "record_ref": record_ref,
                    "error": "the journal chain of this job is cyclic",
                }
            )
            break
        seen.add(record_ref)
        try:
            frame = read_record(
                workspace.control,
                record_ref,
                deadline_seconds=_HISTORY_READ_DEADLINE_SECONDS,
            )
        except (WorkflowError, ValueError) as exc:
            frames.append({"record_ref": record_ref, "error": str(exc)})
            break
        frames.append({**frame, "record_ref": record_ref})
        if limit is not None and len(frames) >= limit:
            break
        record_ref = _optional_string(frame.get("previous_record_ref"))
    frames.reverse()
    return frames


def list_jobs(
    workspace: Workspace,
    *,
    kinds: Iterable[str] | None = None,
    placement: str | None = None,
) -> list[dict[str, Any]]:
    """Return one cheap row per job, optionally filtered by kind and placement."""

    prefix = None if placement is None else normalize_placement(placement).parts
    rows: list[dict[str, Any]] = []
    for marker in workspace.scan_markers(kinds or STATE_KINDS):
        if prefix is not None and marker.placement.parts[: len(prefix)] != prefix:
            continue
        state, _ = _state_of(workspace, marker)
        rows.append(
            {
                "job_key": marker.job_key,
                "job_id": marker.job_id,
                "state": marker.kind,
                "step": state.get("step"),
                "placement": marker.placement.as_posix(),
                "priority": marker.priority,
                "generation": marker.generation,
                "reason": state.get("reason"),
            }
        )
    rows.sort(key=lambda row: (str(row["placement"]), str(row["job_key"])))
    return rows
