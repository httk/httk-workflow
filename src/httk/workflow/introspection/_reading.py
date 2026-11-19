"""Read-only views of jobs on the filesystem kernel: selectors, listings, state and run logs."""

import glob
import json
import logging
import os
import stat
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any

from .. import _fs, _kernel, _pool
from .._job import JobDefinition
from .._kernel import OWNED, JobRef
from .._manager_scheduling import current_step
from .._state import UNOWNED_STATES, StateDoc, read_state_unowned
from ..errors import FormatError, WorkflowError
from ..models import ATTEMPTS_DIRECTORY, JOB_STATE_DIRECTORY, normalize_placement, parse_job_key, placement_text
from ..workspace import Workspace

_LOGGER = logging.getLogger(__name__)

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
#: How many rows' documents a filtered page reads at once with more than one ``io.workers``.
_READ_AHEAD = 64
#: Each read cache below is cleared whole when it passes this many entries. Measured with ~160-character job
#: paths: about 330 bytes per row and 320 per location (at most about 32 and 31 MiB); entries grow with the path
#: length (~515 and 462 bytes at ~195 characters).
_CACHE_LIMIT = 100_000
#: How long a cached row is used, in seconds.
_ROW_TTL = 60.0
#: ``job list`` rows ``(stored at, phase kind, step)`` of unowned, locked jobs whose two documents were read
#: intact, by job directory, for the life of the process. Display only. Only a job's owner writes inside it and
#: every move out of ``owned/`` names the directory afresh, so no protocol actor changes these documents while the
#: path exists (an unlocked job's ``job.json`` is edited in place, so it is never kept); rows expire after
#: :data:`_ROW_TTL` so hand edits show within a minute.
_ROWS: dict[str, tuple[float, object, str]] = {}
#: Where each job was last seen, by ``(workspace root, job UUID)``: ``(path, state, owner id or placement)``,
#: a hint checked with one ``lstat`` before use. Entries never expire: a stale one costs one ``lstat`` and the
#: walk, and the dict is cleared whole at the limit.
_LOCATIONS: dict[tuple[str, str], tuple[str, str, str]] = {}

type _Documents = tuple[tuple[StateDoc | None, str | None], tuple[JobDefinition | None, str | None]]


@dataclass(frozen=True)
class JobListPage:
    """A page of job rows and the cursor needed to request the next page.

    Enumeration is weakly consistent while owners move jobs: a job can appear
    twice or be missed if it changes state between page requests. Clients
    should deduplicate pages by ``job_id``. When ``tag_contains`` or ``steps``
    is active with a finite ``limit``, a page may be partial: at most
    ``max(limit * 100, 10_000)`` jobs are examined before the returned cursor
    resumes the filter scan.

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


def _remember[K, V](cache: dict[K, V], key: K, value: V) -> None:
    if len(cache) >= _CACHE_LIMIT:
        cache.clear()
    cache[key] = value


def _location(ref: JobRef) -> tuple[str, str, str]:
    detail = ref.owner_id if ref.state == OWNED else ref.placement
    return str(ref.path), ref.state, str(detail)


def _relocated(location: tuple[str, str, str]) -> JobRef:
    path, state, detail = location
    if state == OWNED:
        return JobRef.from_path(Path(path), state=state, owner_id=detail)
    return JobRef.from_path(Path(path), state=state, placement=PurePosixPath(detail))


def _present(ref: JobRef) -> bool:
    """Whether a remembered job directory is still there; ``False`` also when observing it fails."""

    try:
        status = _fs.lstat(_fs.loc(ref.path))
    except OSError:
        return False
    return status is not None and stat.S_ISDIR(status.st_mode)


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

    return _at_job_file(job, relative, lambda src: _fs.read_bounded(src, limit, nonblock=True))


def digest_job_file(job: Path, relative: str | PurePosixPath, keep: int) -> tuple[int, str, bytes | None] | None:
    """Stream the SHA-256 of one file below a job directory, of any size, as job-writable content must be read.

    No symlink below *job* is followed and a FIFO or other special file is
    refused without blocking (:func:`httk.workflow._fs.digest_regular`).

    :param job: The job directory (a trusted anchor).
    :param relative: The file below it, such as ``files/POSCAR``.
    :param keep: Also return the content when it is at most this many bytes.
    :return: The size, the lowercase hex digest and the content (``None`` past *keep*),
        or ``None`` when the file or a directory on its way is absent.
    :raises httk.workflow._fs.UnsafePath: If a component is a symlink, or the file is not a regular file.
    """

    return _at_job_file(job, relative, lambda src: _fs.digest_regular(src, keep, nonblock=True))


def _at_job_file[T](job: Path, relative: str | PurePosixPath, read: Callable[[_fs.Loc], T]) -> T | None:
    """Apply *read* to one file below *job*, opening its directory without following a symlink."""

    path = PurePosixPath(relative)
    try:
        directory = _fs.open_dir_under(job, path.parent) if path.parent.parts else _fs.open_dir(job)
    except (FileNotFoundError, NotADirectoryError):
        return None
    try:
        return read(_fs.anchored(directory, path.name))
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


def _location_index(
    workspace: Workspace,
    states: Iterable[str] | None = None,
    placement_prefix: str | PurePosixPath | None = None,
) -> _kernel.LocationIndex:
    """Return a location index for one command whose one listing is :func:`iter_jobs` of *states* (every state by
    default) under *placement_prefix*.

    Its listing is the one a command's selection shares (:class:`JobSelectorResolver`, a ``--state`` selection), so a
    command that selects its jobs and then serves requests lists what it selects from once. A restricted listing
    traverses no more than the selection does; jobs outside it (named by id, or ancestors) are located on demand.

    :param workspace: The workspace.
    :param states: The states to list; every state when ``None``.
    :param placement_prefix: Restrict the listing to this placement subtree.
    :return: The index; its listing is taken when first needed.
    """

    if states is None and placement_prefix is None:
        return _kernel.LocationIndex(workspace, lambda: iter_jobs(workspace))
    selected = None if states is None else tuple(states)
    return _kernel.LocationIndex(
        workspace, lambda: iter_jobs(workspace, selected, placement_prefix=placement_prefix), complete=False
    )


def _resolve_id(
    workspace: Workspace,
    selector: str,
    refs: Callable[[], Iterable[JobRef]],
    index: _kernel.LocationIndex | None = None,
) -> JobRef:
    if not selector:
        raise ValueError("a job selector cannot be empty")
    try:
        job_id = parse_job_key(selector)[1]
    except FormatError:
        job_id = None
    if job_id is not None:
        key = (str(workspace.root), job_id)
        ref = None if (location := _LOCATIONS.get(key)) is None else _relocated(location)
        # Only this lstat guards a remembered location: a job moving between it and the caller's read is the
        # window the walk has too.
        if ref is None or not _present(ref):
            if index is not None:
                ref = index.locate(job_id, placement_hint=None, exhaustive=True)
                if ref is None:
                    # A walk just found no such job: the index's listing, older than that, must not bring it back.
                    raise ValueError(f"no job in {workspace.root} matches {selector!r}")
            else:
                ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
        if ref is not None and selector in {ref.job_id, ref.job_key}:
            _remember(_LOCATIONS, key, _location(ref))
            return ref
    matches = {ref.job_id: ref for ref in refs() if _matches(ref, selector)}
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

    return _resolve_id(workspace, selector, lambda: iter_jobs(workspace))


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
    (a state, a placement), meaning every job below it. A job id or key is
    found at its remembered location, else by a walk of the workspace.

    :param workspace: The workspace.
    :param cwd: The directory relative path selectors resolve from.
    """

    def __init__(self, workspace: Workspace, cwd: Path) -> None:
        self.workspace = workspace
        self.cwd = cwd.resolve()
        # A many-job command's location index (_resolve_selectors): its listing is shared with the selectors, and it
        # answers a job id or key, each entry confirmed by an lstat, instead of one walk per selector.
        self._index: _kernel.LocationIndex | None = None
        self._refs: list[JobRef] | None = None

    def _all(self) -> list[JobRef]:
        if self._refs is None:
            if self._index is not None and self._index.complete:
                self._refs = self._index.refs()
            else:
                self._refs = list(iter_jobs(self.workspace))
                if self._index is not None:
                    # A restricted command index: these selectors list every state themselves; the index takes them.
                    self._index.add(self._refs)
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
        return [_resolve_id(self.workspace, selector, self._all, self._index)]


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


def _resolve_selectors(
    workspace: Workspace, cwd: Path, selectors: Iterable[str], index: _kernel.LocationIndex
) -> list[JobRef]:
    """Resolve as :func:`resolve_job_selectors` does, through the command's location index *index*
    (:func:`_location_index`), whose listing the selectors share with what the command does next."""

    resolver = JobSelectorResolver(workspace, cwd)
    resolver._index = index
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


def _read_row(ref: JobRef) -> _Documents | Exception:
    # An unexpected error is kept for the loop: it surfaces only if the loop reaches this job, as it did unpooled.
    try:
        return read_state(ref), read_job(ref)
    except Exception as exc:
        return exc


class _Ahead:
    """A listed job, its cached row or its documents (or the error reading them) once read, possibly ahead."""

    __slots__ = ("documents", "ref", "row")

    def __init__(self, ref: JobRef) -> None:
        self.ref = ref
        self.documents: _Documents | Exception | None = None
        cached = None if ref.state == OWNED else _ROWS.get(str(ref.path))
        self.row = None if cached is None or time.monotonic() - cached[0] > _ROW_TTL else (cached[1], cached[2])


def _tag_matches(ref: JobRef, tag_contains: str | None) -> bool:
    if tag_contains is None:
        return True
    tag = parse_job_key(ref.job_key)[0]
    return tag is not None and tag_contains in tag


def list_jobs(
    workspace: Workspace,
    *,
    states: Iterable[str] | None = None,
    placement_prefix: str | None = None,
    after: str | None = None,
    limit: int | None = None,
    tag_contains: str | None = None,
    steps: Iterable[str] | None = None,
) -> JobListPage:
    """Return one page of rows, reading job documents only for its rows (with *steps*, for every examined job).

    With more than one ``io.workers`` the documents are read in chunks, up to that many jobs at once; the rows,
    the cursor and the warnings are those of the one-by-one reading (a *steps* filter may read up to 63 more jobs).
    The row of an unowned, locked job read intact in this process within the last minute is not read again: no
    protocol actor changes such a directory's documents while its name exists.

    :param workspace: The workspace.
    :param states: The states to list; every state when ``None``.
    :param placement_prefix: Restrict the listing to this placement subtree.
    :param after: The ``next_after`` cursor of the previous page.
    :param limit: The most rows on the page.
    :param tag_contains: Only list jobs whose tag contains this text.
    :param steps: Only list jobs at one of these steps: the current activation's, or ``job.json``'s initial step
        before the first; this reads every examined job.
    :return: The page.
    :raises ValueError: For a nonpositive limit, an unknown state or a malformed cursor.
    """

    if limit is not None and limit < 1:
        raise ValueError("--limit must be positive")
    prefix = None if placement_prefix is None else normalize_placement(placement_prefix)
    stream = _stream_after(workspace, _check_states(states), prefix, after)
    wanted_steps = None if steps is None else frozenset(steps)
    filtered = tag_contains is not None or wanted_steps is not None
    scan_limit = max(limit * 100, 10_000) if filtered and limit is not None else None
    workers = workspace.io_workers
    rows: list[dict[str, Any]] = []
    # The jobs taken from the listing ahead of the loop (for a pooled read), ending with the listing's error if
    # taking one more failed, and how many were taken in all.
    ahead: deque[_Ahead | Exception] = deque()
    drawn = 0

    def take() -> _Ahead | None:
        """The next listed job, in listing order; a listing error taken ahead is raised here, where it occurred."""

        nonlocal drawn
        if ahead:
            entry = ahead.popleft()
            if isinstance(entry, Exception):
                raise entry
            return entry
        ref = next(stream, None)
        if ref is None:
            return None
        drawn += 1
        return _Ahead(ref)

    def documents(entry: _Ahead) -> _Documents:
        """The job's ``state.json`` and ``job.json``, read with those of the next jobs that will need them."""

        if entry.documents is None:
            _read_ahead(entry)
        assert entry.documents is not None
        if isinstance(entry.documents, Exception):
            raise entry.documents
        return entry.documents

    def _read_ahead(entry: _Ahead) -> None:
        """Read the documents of *entry* and of the next jobs that will need them."""

        nonlocal drawn
        # A chunk holds at most the rows still wanted (without a filter every read job is a row; with a tag filter
        # only the tags that match are read), and 64 with a filter (a --step filter may read up to that many beyond
        # the page). It never takes a job past the scan limit from the listing.
        size = 1
        if workers > 1:
            wanted = _READ_AHEAD if limit is None else max(1, limit - len(rows))
            size = _READ_AHEAD if wanted_steps is not None else min(wanted, _READ_AHEAD) if filtered else wanted
        unread = (
            later
            for later in ahead
            if isinstance(later, _Ahead) and later.documents is None and _tag_matches(later.ref, tag_contains)
        )
        # The next jobs that will need their rows; those with a cached row are not read.
        window = [entry, *islice(unread, size - 1)]
        while len(window) < size and (scan_limit is None or drawn < scan_limit):
            if ahead and isinstance(ahead[-1], Exception):
                break  # the listing failed there: nothing follows
            try:
                ref = next(stream, None)
            except Exception as exc:
                ahead.append(exc)  # raised by take() if the loop gets that far, as without the read-ahead
                break
            if ref is None:
                break
            drawn += 1
            ahead.append(later := _Ahead(ref))
            if _tag_matches(ref, tag_contains):
                window.append(later)
        chunk = [later for later in window if later.row is None]
        for later, read in zip(
            chunk, _pool.run_ordered(_read_row, [later.ref for later in chunk], workers), strict=True
        ):
            later.documents = read

    def matching(entry: _Ahead) -> tuple[object, str | None] | None:
        """The job's phase kind and step when it passes the filters, else ``None``."""

        ref = entry.ref
        if not _tag_matches(ref, tag_contains):
            return None
        row = entry.row
        if row is None:
            (doc, state_error), (job, job_error) = documents(entry)
            phase = None if doc is None else doc.phase.get("kind")
            if job is None or state_error is not None:
                if wanted_steps is None:
                    return phase, None
                # Its step is unknown: a damaged state.json is not the initial step.
                _LOGGER.warning("%s: %s; left out of the --step listing", ref.job_key, job_error or state_error)
                return None
            row = phase, current_step(job, doc)
            if ref.state != OWNED and (doc is None or doc.unlocked is None):
                _remember(_ROWS, str(ref.path), (time.monotonic(), *row))
        return row if wanted_steps is None or row[1] in wanted_steps else None

    examined = 0
    last: JobRef | None = None
    next_after: str | None = None
    while (entry := take()) is not None:
        ref = entry.ref
        if limit is not None and len(rows) >= limit:
            # The page is full; it has a next page only if another matching job follows.
            if _tag_matches(ref, tag_contains) if wanted_steps is None else matching(entry) is not None:
                next_after = _ref_cursor(last) if last is not None else None
                break
            examined += 1
            if scan_limit is not None and examined >= scan_limit:
                next_after = _ref_cursor(ref)
                break
            continue
        examined += 1
        if (found := matching(entry)) is not None:
            phase, step = found
            placement = job_placement(ref)
            rows.append(
                {
                    "job_key": ref.job_key,
                    "job_id": ref.job_id,
                    "state": ref.state,
                    "step": step,
                    "phase": phase,
                    "placement": None if placement is None else placement_text(placement),
                    "priority": ref.priority,
                    "token": ref.token,
                    "owner_id": ref.owner_id,
                }
            )
            last = ref
            _remember(_LOCATIONS, (str(workspace.root), ref.job_id), _location(ref))
        if scan_limit is not None and examined >= scan_limit:
            next_after = _ref_cursor(ref)
            break
    return JobListPage(rows, next_after)
