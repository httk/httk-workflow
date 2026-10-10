"""Read-only views of jobs on the filesystem kernel: selectors, listings, state and run logs."""

import glob
import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .. import _fs, _kernel
from .._job import JobDefinition
from .._kernel import OWNED, JobRef
from .._state import UNOWNED_STATES, StateDoc, read_state_unowned
from ..errors import FormatError, WorkflowError
from ..models import ATTEMPTS_DIRECTORY, JOB_STATE_DIRECTORY, normalize_placement, parse_job_key, placement_text
from ..workspace import Workspace

JOB_HISTORY_FORMAT = "httk-workflow-job-history"
JOB_LIST_FORMAT = "httk-workflow-job-list"
#: Every state a job directory can be in, in listing order: the six unowned states, then owned.
JOB_STATES = (*UNOWNED_STATES, OWNED)
#: How much of a run log's tail to read when surfacing its last headline. A
#: run log can grow without bound, so the report reads only the final slice.
_RUNLOG_TAIL_BYTES = 65536
#: The largest run log a reader accepts.
LOG_LIMIT = 16 << 20
#: The largest JSON document of a job a reader accepts.
JSON_LIMIT = 1 << 20


@dataclass(frozen=True)
class JobListPage:
    """A page of job rows and the cursor needed to request the next page.

    Enumeration is weakly consistent while owners move jobs: a job can appear
    twice or be missed if it changes state between page requests. Clients
    should deduplicate pages by ``job_id``. When ``tag_contains`` is active with
    a finite ``limit``, a page may be partial: at most ``max(limit * 100,
    10_000)`` jobs are examined before the returned cursor resumes the filter
    scan.

    :param jobs: The rows in state, placement and directory-name order.
    :param next_after: The cursor of the last row when more matching rows exist.
    :param counts: Optional counts carried by a remote page response.
    """

    jobs: list[dict[str, Any]]
    next_after: str | None
    counts: dict[str, int] | None = None

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate over the rows."""

        return iter(self.jobs)

    def __len__(self) -> int:
        """Return the number of rows in the page."""

        return len(self.jobs)

    def __getitem__(self, index: int) -> dict[str, Any]:
        """Return one row by its page index.

        :param index: The row index.
        :return: The row.
        """

        return self.jobs[index]


def _check_states(states: Iterable[str] | None) -> tuple[str, ...]:
    requested = set(JOB_STATES if states is None else states)
    if unknown := requested - set(JOB_STATES):
        raise ValueError(f"unknown job state {', '.join(sorted(unknown))}; states are {', '.join(JOB_STATES)}")
    return tuple(state for state in JOB_STATES if state in requested)


def _owned_refs(workspace: Workspace, after: tuple[str, str] | None = None) -> Iterator[JobRef]:
    """Yield the owned jobs in ``(owner id, directory name)`` order, strictly after *after*."""

    root = workspace.jobs / OWNED
    try:
        owners = sorted(entry.name for entry in root.iterdir() if entry.is_dir() and not entry.is_symlink())
    except OSError:
        return
    for owner_id in owners:
        try:
            names = sorted(entry.name for entry in (root / owner_id).iterdir())
        except OSError:
            continue
        for name in names:
            if after is not None and (owner_id, name) <= after:
                continue
            try:
                yield JobRef.from_path(root / owner_id / name, state=OWNED, owner_id=owner_id)
            except FormatError:
                continue


def job_placement(ref: JobRef) -> PurePosixPath | None:
    """Return a job's placement: from its name for an unowned job, from ``job.json`` for an owned one.

    :param ref: The job.
    :return: The placement, or ``None`` when an owned job's ``job.json`` cannot be read.
    """

    if ref.placement is not None:
        return ref.placement
    job, _ = read_job(ref)
    return None if job is None else job.placement


def iter_jobs(
    workspace: Workspace,
    states: Iterable[str] | None = None,
    *,
    placement_prefix: str | PurePosixPath | None = None,
) -> Iterator[JobRef]:
    """Stream the jobs of the selected states, in :data:`JOB_STATES` and then placement order.

    :param workspace: The workspace.
    :param states: The states to list; every state when ``None``.
    :param placement_prefix: Restrict the listing to this placement subtree.
    :yields: The job references (hints: a job may move right after it is listed).
    :raises ValueError: For an unknown state.
    """

    prefix = None if placement_prefix is None else normalize_placement(placement_prefix)
    for state in _check_states(states):
        if state != OWNED:
            yield from _kernel.list_jobs(workspace, state, prefixes=() if prefix is None else (prefix,))
            continue
        for ref in _owned_refs(workspace):
            placement = job_placement(ref) if prefix is not None else None
            if prefix is None or (placement is not None and placement.is_relative_to(prefix)):
                yield ref


def count_jobs(workspace: Workspace, state: str, placement_prefix: str | PurePosixPath | None = None) -> int:
    """Count the jobs of one state by listing, reading nothing but owned jobs' placements.

    :param workspace: The workspace.
    :param state: One of :data:`JOB_STATES`.
    :param placement_prefix: Restrict the count to this placement subtree.
    :return: The number of jobs.
    :raises ValueError: For an unknown state.
    """

    return sum(1 for _ in iter_jobs(workspace, (state,), placement_prefix=placement_prefix))


def read_job(ref: JobRef) -> tuple[JobDefinition | None, str | None]:
    """Return a job's definition, reporting rather than raising on damage.

    :param ref: The job.
    :return: The definition and ``None``, or ``None`` and the reason it is unreadable.
    """

    try:
        return JobDefinition.from_path(ref.path / "job.json"), None
    except (WorkflowError, OSError, ValueError) as exc:
        return None, str(exc)


def read_state(ref: JobRef) -> tuple[StateDoc | None, str | None]:
    """Return a job's ``state.json``, absent before its first claim, reporting damage instead of raising.

    :param ref: The job.
    :return: The document (or ``None``) and the damage description (or ``None``).
    """

    doc, damaged = read_state_unowned(ref.path / "state.json")
    if damaged:
        return None, f"{ref.path / 'state.json'} is damaged"
    if doc is not None and doc.job_id != ref.job_id:
        return None, f"{ref.path / 'state.json'} names job {doc.job_id}"
    return doc, None


def read_job_file(job: Path, relative: str | PurePosixPath, limit: int) -> bytes | None:
    """Read one file below a job directory the way job-writable content must be read.

    No symlink below *job* is followed, a FIFO or other special file is
    refused without blocking, and the content is bounded.

    :param job: The job directory (a trusted anchor).
    :param relative: The file below it, such as ``.httk-job/runlog.jsonl``.
    :param limit: The largest accepted size in bytes.
    :return: The content, or ``None`` when the file or a directory on its way is absent.
    :raises httk.workflow._fs.UnsafePath: If a component is a symlink, or the file is not a regular file.
    :raises httk.workflow._fs.TooLarge: If the file is larger than *limit*.
    """

    path = PurePosixPath(relative)
    try:
        directory = _fs.open_dir_under(job, path.parent) if path.parent.parts else _fs.open_dir(job)
    except (FileNotFoundError, NotADirectoryError):
        return None
    try:
        return _fs.read_bounded(_fs.anchored(directory, path.name), limit, nonblock=True)
    finally:
        os.close(directory)


def _jsonl(job: Path, relative: str, *, tail: int | None = None) -> list[dict[str, Any]]:
    """Read a JSON-lines log of a job, skipping a torn last line; an unreadable line becomes an ``error`` entry."""

    try:
        data = read_job_file(job, relative, LOG_LIMIT) or b""
    except (WorkflowError, OSError) as exc:
        return [{"error": f"{job / relative}: {exc}"}]
    start = 0 if tail is None else max(0, len(data) - tail)
    lines = data[start:].split(b"\n")
    # A file that does not end in a newline ends in a line still being written.
    lines.pop()
    if start and lines:
        lines.pop(0)  # the slice starts mid-line
    events: list[dict[str, Any]] = []
    for number, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
        except ValueError:
            event = None
        events.append(
            event if isinstance(event, dict) else {"error": f"{job / relative}: line {number} is not a JSON object"}
        )
    return events


def job_events(ref: JobRef, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Return the owner run log ``logs/runlog.jsonl`` of one job, oldest first.

    Each event is the object an owner appended (``at``, ``event``,
    ``owner_id`` and, as relevant, ``attempt_id``, ``activation_id``, ``step``,
    ``from``, ``to``, ``detail``). A torn last line is skipped; an unreadable
    earlier line is reported in place as ``{"error": ...}``.

    :param ref: The job.
    :param limit: Keep only the newest *limit* events.
    :return: The events.
    """

    events = _jsonl(ref.path, "logs/runlog.jsonl")
    return events if limit is None else events[-limit:]


def read_last_headline(payload: str | Path | None) -> str | None:
    """Return the message of the runner's last ``headline`` record in ``.httk-job/runlog.jsonl``, if any.

    Only the tail of the file is read, so a long-running runner's headline is cheap to surface.

    :param payload: The job directory.
    :return: The last headline message, or ``None`` when there is none to read.
    """

    if payload is None:
        return None
    headline: str | None = None
    for event in _jsonl(Path(payload), f"{JOB_STATE_DIRECTORY}/runlog.jsonl", tail=_RUNLOG_TAIL_BYTES):
        if event.get("kind") == "headline" and isinstance(event.get("message"), str):
            headline = event["message"]
    return headline


def attempt_control(ref: JobRef, doc: StateDoc | None) -> Path | None:
    """Return the attempt directory of a job's current (or last) attempt.

    :param ref: The job.
    :param doc: Its state.
    :return: ``attempts/<attempt-id>``, or ``None`` without a recorded attempt.
    """

    attempt_id = None if doc is None or doc.attempt is None else doc.attempt.get("id")
    return ref.path / ATTEMPTS_DIRECTORY / attempt_id if isinstance(attempt_id, str) else None


def read_error_breadcrumb(control: Path | None) -> dict[str, Any] | None:
    """Return the ``error.json`` breadcrumb of one attempt, when it left one.

    :param control: The attempt directory, ``<job>/attempts/<attempt-id>``.
    :return: The breadcrumb, or ``None`` when it is absent, unsafe or unreadable.
    """

    if control is None:
        return None
    try:
        # Anchored at the job, so a symlinked attempts/<attempt-id> is refused rather than followed.
        data = read_job_file(control.parent.parent, f"{control.parent.name}/{control.name}/error.json", JSON_LIMIT)
        value = None if data is None else json.loads(data)
    except (WorkflowError, OSError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _matches(ref: JobRef, selector: str) -> bool:
    return ref.job_id.startswith(selector) or ref.job_key.startswith(selector)


def _resolve_id(workspace: Workspace, selector: str, refs: Iterable[JobRef]) -> JobRef:
    if not selector:
        raise ValueError("a job selector cannot be empty")
    try:
        job_id = parse_job_key(selector)[1]
    except FormatError:
        job_id = None
    if job_id is not None:
        ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
        if ref is not None and selector in {ref.job_id, ref.job_key}:
            return ref
    matches = {ref.job_id: ref for ref in refs if _matches(ref, selector)}
    if not matches:
        raise ValueError(f"no job in {workspace.root} matches {selector!r}")
    if len(matches) > 1:
        candidates = ", ".join(sorted(ref.job_key for ref in matches.values())[:5])
        raise ValueError(f"job selector {selector!r} matches {len(matches)} jobs: {candidates}")
    return next(iter(matches.values()))


def resolve_job(workspace: Workspace, selector: str) -> JobRef:
    """Return the one job *selector* names.

    A selector is a job UUID, a complete ``tag--uuid`` job key, or any unique
    prefix of either. An ambiguous selector is refused with the candidates it
    matched rather than resolved arbitrarily.

    :param workspace: The workspace.
    :param selector: The selector.
    :return: The job.
    :raises ValueError: For an empty, unknown or ambiguous selector.
    """

    return _resolve_id(workspace, selector, iter_jobs(workspace))


def selector_is_path(cwd: Path, selector: str) -> bool:
    """Return whether *selector* must be interpreted as a local path.

    :param cwd: The directory relative selectors resolve from.
    :param selector: The selector.
    :return: Whether it is a glob or an existing path.
    """

    return any(character in selector for character in "*?[") or (cwd / selector).exists()


def selector_uses_remote_path(cwd: Path, selector: str) -> bool:
    """Return whether a selector cannot be forwarded to a remote workspace.

    :param cwd: The directory relative selectors resolve from.
    :param selector: The selector.
    :return: Whether it names a local path.
    """

    return "/" in selector or selector_is_path(cwd, selector)


class JobSelectorResolver:
    """Resolve job selectors while sharing one listing of the workspace for a batch.

    A path selector names a job directory, or any directory below ``jobs/``
    (a state, a placement), meaning every job below it.

    :param workspace: The workspace.
    :param cwd: The directory relative path selectors resolve from.
    """

    def __init__(self, workspace: Workspace, cwd: Path) -> None:
        self.workspace = workspace
        self.cwd = cwd.resolve()
        self._refs: list[JobRef] | None = None

    def _all(self) -> list[JobRef]:
        if self._refs is None:
            self._refs = list(iter_jobs(self.workspace))
        return self._refs

    def _resolve_path(self, path_name: str) -> list[JobRef]:
        path = Path(path_name)
        resolved = (path if path.is_absolute() else self.cwd / path).resolve()
        if not resolved.is_relative_to(self.workspace.root.resolve()):
            raise ValueError(f"{path_name} is not inside workspace {self.workspace.root}")
        jobs = self.workspace.jobs.resolve()
        if not resolved.is_relative_to(jobs):
            raise ValueError(f"{path_name} is not below the jobs directory {self.workspace.jobs}; jobs live in jobs/")
        if resolved.is_file():
            raise ValueError(f"{path_name} is a file, not a job directory")
        if not resolved.is_dir():
            raise ValueError(f"{path_name} is not a job directory or a directory of jobs")
        matches = sorted(
            (ref for ref in self._all() if ref.path.resolve().is_relative_to(resolved)), key=lambda ref: str(ref.path)
        )
        if not matches:
            raise ValueError(f"no jobs below {path_name}")
        return matches

    def resolve_one(self, selector: str) -> list[JobRef]:
        """Resolve one selector, expanding a glob or a directory into the jobs below it.

        :param selector: A job id, key, unique prefix, path or glob.
        :return: The jobs, without repeats.
        :raises ValueError: When the selector names no job or is ambiguous.
        """

        if any(character in selector for character in "*?["):
            paths = sorted(glob.glob(selector, root_dir=self.cwd))
            if not paths:
                raise ValueError(f"no path matches {selector!r} below {self.cwd}")
            return _unique(ref for path in paths for ref in self._resolve_path(path))
        if (self.cwd / selector).exists():
            return _unique(self._resolve_path(selector))
        return [_resolve_id(self.workspace, selector, self._all())]


def _unique(refs: Iterable[JobRef]) -> list[JobRef]:
    seen: dict[str, JobRef] = {}
    for ref in refs:
        seen.setdefault(ref.job_id, ref)
    return list(seen.values())


def resolve_job_selector(workspace: Workspace, cwd: Path, selector: str) -> list[JobRef]:
    """Resolve one job selector relative to *cwd*.

    :param workspace: The workspace.
    :param cwd: Resolve relative path selectors from this directory.
    :param selector: Name, prefix, path, or glob naming jobs.
    :return: The jobs, in path order for paths and globs.
    """

    return JobSelectorResolver(workspace, cwd).resolve_one(selector)


def resolve_job_selectors(workspace: Workspace, cwd: Path, selectors: Iterable[str]) -> list[JobRef]:
    """Resolve and deduplicate job selectors relative to *cwd*.

    :param workspace: The workspace.
    :param cwd: Resolve relative path selectors from this directory.
    :param selectors: Selectors in the order supplied by the operator.
    :return: Unique jobs preserving selector and expansion order.
    """

    resolver = JobSelectorResolver(workspace, cwd)
    return _unique(ref for selector in selectors for ref in resolver.resolve_one(selector))


def _ref_cursor(ref: JobRef) -> str:
    """Return ``<state>:<placement>/<name>`` (unowned) or ``owned:<owner-id>/<name>``."""

    tail = f"{ref.owner_id}/{ref.path.name}" if ref.state == OWNED else ref.cursor
    return f"{ref.state}:{tail}"


def _stream_after(
    workspace: Workspace, states: tuple[str, ...], prefix: PurePosixPath | None, after: str | None
) -> Iterator[JobRef]:
    after_state, after_tail = None, ""
    if after is not None:
        after_state, separator, after_tail = after.partition(":")
        if not separator or after_state not in JOB_STATES or not after_tail:
            raise ValueError("job list cursor must be '<state>:<placement>/<name>' or 'owned:<owner-id>/<name>'")
        if after_state not in states:
            raise ValueError(f"job list cursor state {after_state!r} is not among the selected states")
    started = after_state is None
    for state in states:
        if not started and state != after_state:
            continue
        resume = None if started else after_tail
        started = True
        if state != OWNED:
            prefixes = () if prefix is None else (prefix,)
            yield from _kernel.list_jobs(workspace, state, prefixes=prefixes, start=resume)
            continue
        owner_id, _, name = (resume or "").partition("/")
        for ref in _owned_refs(workspace, (owner_id, name) if resume else None):
            placement = job_placement(ref) if prefix is not None else None
            if prefix is None or (placement is not None and placement.is_relative_to(prefix)):
                yield ref


def _tag_matches(ref: JobRef, tag_contains: str | None) -> bool:
    if tag_contains is None:
        return True
    tag = parse_job_key(ref.job_key)[0]
    return tag is not None and tag_contains in tag


def list_jobs(
    workspace: Workspace,
    *,
    kinds: Iterable[str] | None = None,
    placement_prefix: str | None = None,
    after: str | None = None,
    limit: int | None = None,
    tag_contains: str | None = None,
) -> JobListPage:
    """Return one page of rows, reading ``state.json`` only for the rows of that page.

    :param workspace: The workspace.
    :param kinds: The states to list; every state when ``None``.
    :param placement_prefix: Restrict the listing to this placement subtree.
    :param after: The ``next_after`` cursor of the previous page.
    :param limit: The most rows on the page.
    :param tag_contains: Only list jobs whose tag contains this text.
    :return: The page.
    :raises ValueError: For a nonpositive limit, an unknown state or a malformed cursor.
    """

    if limit is not None and limit < 1:
        raise ValueError("--limit must be positive")
    prefix = None if placement_prefix is None else normalize_placement(placement_prefix)
    stream = _stream_after(workspace, _check_states(kinds), prefix, after)
    scan_limit = max(limit * 100, 10_000) if tag_contains is not None and limit is not None else None
    rows: list[dict[str, Any]] = []
    examined = 0
    last: JobRef | None = None
    next_after: str | None = None
    for ref in stream:
        if limit is not None and len(rows) >= limit:
            # The page is full; it has a next page only if another matching job follows.
            if _tag_matches(ref, tag_contains):
                next_after = _ref_cursor(last) if last is not None else None
                break
            examined += 1
            if scan_limit is not None and examined >= scan_limit:
                next_after = _ref_cursor(ref)
                break
            continue
        examined += 1
        if _tag_matches(ref, tag_contains):
            doc, _ = read_state(ref)
            placement = job_placement(ref)
            rows.append(
                {
                    "job_key": ref.job_key,
                    "job_id": ref.job_id,
                    "state": ref.state,
                    "step": None if doc is None or doc.activation is None else doc.activation.get("step"),
                    "phase": None if doc is None else doc.phase.get("kind"),
                    "placement": None if placement is None else placement_text(placement),
                    "priority": ref.priority,
                    "token": ref.token,
                    "owner_id": ref.owner_id,
                }
            )
            last = ref
        if scan_limit is not None and examined >= scan_limit:
            next_after = _ref_cursor(ref)
            break
    return JobListPage(rows, next_after)
