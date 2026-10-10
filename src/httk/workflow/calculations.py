"""Collect finished calculations found anywhere in a directory tree.

A *recognized-calculation collector* is a workflow package with a
``[workflow.recognize]`` table and no runner: it is collected, never run.
:func:`collect_tree` walks a tree and asks every collector whether a directory
is one of its calculations. The cheap ``requires`` markers are checked first;
only when every marker matches is the package's ``recognize(directory)`` hook
called. The winning collector's ``collect`` hook then reads the directory
through a stand-in :class:`~httk.workflow.collecting.JobRecord`, and the result
is assembled into a :class:`~httk.workflow.collecting.CollectedJob` exactly as
for a workspace job. :func:`claims` reports the same decisions without
collecting, for a dry run.

A claimed calculation is identified by ``<collector name>:<identity>``, where
the collector derives the identity from the calculation's content (see
:func:`content_digest`), so moving a directory keeps its identity. Within one
sweep, two directories claiming the same identity with identical marker files
are copies and the second is skipped; with different marker files they are an
:class:`IdentityCollisionError`.
"""

import fnmatch
import hashlib
import logging
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import IO, Literal, cast

from httk.core import Run
from httk.core.datastream.compression import open_compressed, split_compression_suffix
from httk.core.register.codes import collector_support, known_collectors

from .collecting import (
    CollectedJob,
    JobRecord,
    _assemble_collected,
    _CollectEnvironmentError,
    _degraded_job,
    _input_roles,
    _job_workflow_document,
    collect,
    existing_file,
)
from .hookapi import Claim, Unclaimed
from .models import WORKSPACE_DIRECTORY, make_job_key
from .packages import _source_hook, load_workflow_package
from .scaffold import RecognizeSpec, WorkflowProvider
from .workspace import Workspace

__all__ = [
    "AmbiguousClaimError",
    "DirectoryClaim",
    "IdentityCollisionError",
    "claims",
    "collect_tree",
    "content_digest",
]

_LOGGER = logging.getLogger(__name__)
_CONTEXT = {"context": "workflow"}
_CHUNK = 1 << 20


class AmbiguousClaimError(ValueError):
    """Several collectors claimed one directory at the same, highest priority."""


class IdentityCollisionError(ValueError):
    """Two directories with different content claimed the same identity in one sweep."""


@dataclass(frozen=True)
class DirectoryClaim:
    """One directory's recognition outcome, as reported by :func:`claims`.

    :param directory: The directory, relative to the swept root (POSIX).
    :param kind: ``"claimed"``, ``"unclaimed"`` or ``"workspace"``.
    :param collector: The winning collector's name (``claimed``), the declining
        collector's name (``unclaimed``), or ``None`` (``workspace``).
    :param priority: The winning collector's priority, or ``None``.
    :param identity: The claimed identity, or ``None``.
    :param reason: The ``Unclaimed`` reason, or ``None``.
    :param also_matched: Names of lower-priority collectors that also claimed the directory.
    :param duplicate_of: For a claimed copy of an earlier directory of this sweep
        (same collector, identity and marker content), that directory; it is not
        collected again.
    :param consumes: The subdirectories the claim names as part of its
        calculation, relative to the swept root (POSIX).
    """

    directory: str
    kind: Literal["claimed", "unclaimed", "workspace"]
    collector: str | None = None
    priority: int | None = None
    identity: str | None = None
    reason: str | None = None
    also_matched: tuple[str, ...] = ()
    duplicate_of: str | None = None
    consumes: tuple[str, ...] = ()


@dataclass(frozen=True)
class _Collector:
    name: str
    provider: WorkflowProvider
    spec: RecognizeSpec
    recognize: Callable[[Path], object]
    collect: Callable[[JobRecord], object]


def _chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as raw, open_compressed(raw, compression="extension", name=path.name) as stream:
        reader = cast(IO[bytes], stream)
        while chunk := reader.read(_CHUNK):
            yield chunk


def content_digest(directory: Path, names: Iterable[str]) -> str:
    """Return the SHA-256 hex digest of the named files of a directory, decompressed.

    Each distinct name is framed, in sorted order, as ``name NUL length NUL
    content``, where ``length`` is the decimal byte count of the decompressed
    content. A name absent from the directory is framed as ``name NUL - NUL``,
    so an absent file differs from an empty one. A name is found as itself or,
    when that is absent, as its compressed copy (``INCAR.gz`` for ``INCAR``), so
    compressing a file does not change the digest. Collectors use this to
    derive a claim's identity from a calculation's input files.

    :param directory: The directory holding the files.
    :param names: The uncompressed basenames to digest; order and repetition do not matter.
    :return: The hex digest.
    """

    base = Path(directory)
    digest = hashlib.sha256()
    for name in sorted(set(names)):
        digest.update(name.encode() + b"\0")
        path = existing_file(base / name)
        if path is None:
            digest.update(b"-\0")
            continue
        if split_compression_suffix(path.name)[1] is None:
            length = path.stat().st_size
        else:
            length = sum(len(chunk) for chunk in _chunks(path))
        digest.update(str(length).encode() + b"\0")
        for chunk in _chunks(path):
            digest.update(chunk)
    return digest.hexdigest()


def _loaded(path: Path) -> _Collector:
    provider = load_workflow_package(path, register=False)
    spec = provider.recognize
    if spec is None or provider.directory is None:
        raise ValueError(f"{path}: not a recognized-calculation collector package (no [workflow.recognize])")
    if not callable(provider.collector):
        raise ValueError(f"{path}: a recognized-calculation collector needs a Python collect hook")
    recognize = _source_hook(provider.directory, spec.file, "recognize")
    return _Collector(provider.workflow_id, provider, spec, recognize, provider.collector)


def _collector_set(collectors: Iterable[str | os.PathLike[str]]) -> list[_Collector]:
    loaded: dict[str, _Collector] = {}
    for name in known_collectors():
        try:
            collector = _loaded(collector_support(name).path())
            if collector.name != name:
                raise ValueError(f"its package is named {collector.name!r}")
        except (OSError, ValueError) as exc:
            _LOGGER.warning("skipping registered collector %s: %s", name, exc, extra=_CONTEXT)
            continue
        loaded[name] = collector
    local: set[str] = set()
    for path in collectors:
        collector = _loaded(Path(path).expanduser().resolve())
        if collector.name in local:
            raise ValueError(f"two collector packages are named {collector.name!r}")
        local.add(collector.name)
        loaded[collector.name] = collector
    return [loaded[name] for name in sorted(loaded)]


def _directories(root: Path, exclude: tuple[str, ...]) -> Iterator[tuple[Path, PurePosixPath, tuple[str, ...], bool]]:
    """Yield ``(directory, relative, file names, is workspace)`` top-down in sorted order."""

    stack = [(root, PurePosixPath("."))]
    while stack:
        directory, relative = stack.pop()
        if (directory / WORKSPACE_DIRECTORY / "format.json").is_file():
            yield directory, relative, (), True
            continue
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
        except OSError as exc:
            _LOGGER.warning("skipping unreadable directory %s: %s", directory, exc, extra=_CONTEXT)
            continue
        files: list[str] = []
        subdirectories: list[tuple[Path, PurePosixPath]] = []
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                child = relative / entry.name
                if not entry.name.startswith(".") and not any(
                    fnmatch.fnmatchcase(child.as_posix(), pattern) for pattern in exclude
                ):
                    subdirectories.append((Path(entry.path), child))
            elif entry.is_file():
                files.append(entry.name)
        yield directory, relative, tuple(files), False
        stack.extend(reversed(subdirectories))


def _marker_files(marker: str, stripped: Mapping[str, list[str]]) -> list[str]:
    """Return the compression-stripped file names a marker matches."""

    lowered = marker.lower()
    if lowered.startswith("*."):
        return [name for key, names in stripped.items() if key.endswith(lowered[1:]) for name in names]
    return list(stripped.get(lowered, ()))


def _recognize(collector: _Collector, directory: Path) -> Claim | Unclaimed | None:
    try:
        result = collector.recognize(directory)
        if result is None:
            return None
        if isinstance(result, Claim):
            return Claim(result.identity, result.consumes)
        if isinstance(result, Unclaimed):
            return result
        return Unclaimed(f"recognize returned {type(result).__name__}")
    except (ImportError, _CollectEnvironmentError):
        # A missing host dependency is the operator's problem, not a verdict on the directory.
        raise
    except Exception as exc:
        _LOGGER.warning(
            "collector %s failed to recognize %s: %s", collector.name, directory, exc, exc_info=True, extra=_CONTEXT
        )
        return Unclaimed(f"recognize failed: {exc}")


def _sweep(
    root: str | os.PathLike[str],
    collectors: Iterable[str | os.PathLike[str]],
    prefer: Iterable[str],
    exclude: Iterable[str],
) -> Iterator[tuple[DirectoryClaim, Path, _Collector | None]]:
    base = Path(root).expanduser().resolve()
    if not base.is_dir():
        raise ValueError(f"not a directory: {root}")
    loaded = _collector_set(collectors)
    preferred = tuple(prefer)
    seen: dict[tuple[str, str], tuple[str, str]] = {}
    for directory, relative, files, workspace in _directories(base, tuple(exclude)):
        where = relative.as_posix()
        if workspace:
            yield DirectoryClaim(where, "workspace"), directory, None
            continue
        stripped: dict[str, list[str]] = {}
        for name in files:
            inner = split_compression_suffix(name)[0]
            stripped.setdefault(inner.lower(), []).append(inner)
        found: list[tuple[_Collector, Claim]] = []
        declined: list[tuple[_Collector, Unclaimed]] = []
        for collector in loaded:
            if not all(_marker_files(marker, stripped) for marker in collector.spec.requires):
                continue
            outcome = _recognize(collector, directory)
            if isinstance(outcome, Claim):
                found.append((collector, outcome))
            elif isinstance(outcome, Unclaimed):
                declined.append((collector, outcome))
        if not found:
            if declined:
                collector, unclaimed = declined[0]
                yield DirectoryClaim(where, "unclaimed", collector.name, reason=unclaimed.reason), directory, None
            continue
        found.sort(key=lambda item: -item[0].spec.priority)
        top = [item for item in found if item[0].spec.priority == found[0][0].spec.priority]
        winner = top[0]
        if len(top) > 1:
            names = [item[0].name for item in top]
            choice = next((name for name in preferred if name in names), None)
            if choice is None:
                raise AmbiguousClaimError(
                    f"{where}: collectors {', '.join(names)} all claim it at priority {top[0][0].spec.priority}; "
                    f"choose one with prefer=(NAME,) (--prefer NAME)"
                )
            winner = next(item for item in top if item[0].name == choice)
        collector, claim = winner
        also = tuple(item[0].name for item in found if item is not winner)
        if also:
            _LOGGER.info(
                "claimed %s by %s (priority %d; also matched: %s)",
                where,
                collector.name,
                collector.spec.priority,
                ", ".join(also),
                extra=_CONTEXT,
            )
        markers = {name for marker in collector.spec.requires for name in _marker_files(marker, stripped)}
        fingerprint = content_digest(directory, markers)
        key = (collector.name, claim.identity)
        first = seen.setdefault(key, (where, fingerprint))
        duplicate_of: str | None = None
        if first != (where, fingerprint):
            if first[1] != fingerprint:
                raise IdentityCollisionError(
                    f"{first[0]} and {where} both claim {collector.name}:{claim.identity} with different content; "
                    f"skip one with exclude=(PATTERN,) (--exclude PATTERN)"
                )
            duplicate_of = first[0]
            _LOGGER.info("skipping %s: same calculation as %s", where, first[0], extra=_CONTEXT)
        yield (
            DirectoryClaim(
                where,
                "claimed",
                collector.name,
                collector.spec.priority,
                claim.identity,
                also_matched=also,
                duplicate_of=duplicate_of,
                consumes=tuple((relative / path).as_posix() for path in claim.consumes),
            ),
            directory,
            collector,
        )


def claims(
    root: str | os.PathLike[str],
    *,
    collectors: Iterable[str | os.PathLike[str]] = (),
    prefer: Iterable[str] = (),
    exclude: Iterable[str] = (),
) -> Iterator[DirectoryClaim]:
    """Report how :func:`collect_tree` would treat each directory, without collecting.

    Ordinary directories that no collector's markers and hook recognize are not
    reported. Parameters and errors are those of :func:`collect_tree`.

    :param root: The tree to sweep.
    :param collectors: Extra collector package directories; see :func:`collect_tree`.
    :param prefer: Collector names that win priority ties, in order.
    :param exclude: Root-relative POSIX glob patterns of directories to skip.
    :yields: One outcome per claimed, declined or workspace directory, in walk order.
    """

    for outcome, _directory, _collector in _sweep(root, collectors, prefer, exclude):
        yield outcome


def _record(root: Path, relative: PurePosixPath, collector: _Collector, identity: str) -> JobRecord:
    declaration = collector.provider.declarations.get("workflow")
    return JobRecord(
        workspace_root=root,
        workspace_id=collector.name,
        job_id=identity,
        job_key=make_job_key(identity, None),
        job={"workflow": collector.name, "parameters": {}},
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=relative.parent,
        payload_path=relative,
        workdir_path=relative,
        data_path=None,
        provenance={"activations": [], "gaps": False},
        runner_steps=None,
        children={},
        declarations={"workflow": {"declared": None if declaration is None else dict(declaration), "observed": None}},
    )


def _collect_directory(root: Path, outcome: DirectoryClaim, collector: _Collector) -> CollectedJob:
    identity = cast(str, outcome.identity)
    relative = PurePosixPath(outcome.directory)
    record = _record(root, relative, collector, identity)
    source_id = f"{collector.name}:{identity}"
    run = Run(
        workflow_declaration_uri=collector.provider.declaration_uri,
        workflow_definition_uri=None,
        inputs=(),
        artifacts=(),
        outputs=(),
        source_id=source_id,
        last_modified=None,
    )
    try:
        raw = collector.collect(record)
        if not isinstance(raw, Mapping):
            raise ValueError("collector must return a mapping of roles")
        input_roles = _input_roles(_job_workflow_document(record, collector.provider))
        inputs = {role: value for role, value in raw.items() if role in input_roles}
        outputs = {role: value for role, value in raw.items() if role not in input_roles}
        collected = _assemble_collected(source_id, record, collector.provider, run, outputs, inputs=inputs)
    except (ImportError, _CollectEnvironmentError):
        raise
    except Exception as exc:
        collected = _degraded_job(record, collector.provider, run, f"{outcome.directory}: {exc}")
    return replace(collected, identity_stable=True)


@dataclass
class _Parent:
    """A collected calculation held back until its consumed subdirectories were walked."""

    outcome: DirectoryClaim
    collector: _Collector
    item: CollectedJob
    reached: dict[str, DirectoryClaim]


def _within(directory: str, ancestor: str) -> bool:
    return ancestor == "." or directory == ancestor or directory.startswith(ancestor + "/")


def _linked(base: Path, parent: _Parent, failed: Mapping[str, str]) -> CollectedJob:
    """Give a held-back calculation its child runs, or degrade it for a consumed directory not collected.

    *failed* maps the source id of every degraded item of the sweep to its reason.
    """

    links: list[tuple[str, str]] = []
    missing: list[str] = []
    prefix = PurePosixPath(parent.outcome.directory)
    for consumed in parent.outcome.consumes:
        label = PurePosixPath(consumed).relative_to(prefix).as_posix()
        reached = parent.reached.get(consumed)
        if reached is None:
            path = base.joinpath(*PurePosixPath(consumed).parts)
            missing.append(
                f"{consumed} does not exist"
                if not path.exists() and not path.is_symlink()
                else f"{consumed} was not collected (excluded, a symlink or dot directory, or recognized by no collector)"
            )
        elif reached.kind == "claimed":
            # A copy deduplicated onto an earlier directory has that directory's
            # source id (same collector and identity), so the link names its run.
            source_id = f"{reached.collector}:{reached.identity}"
            if source_id in failed:
                # A failed part leaves the calculation incomplete, and its run is not stored.
                missing.append(f"consumed calculation {consumed} failed: {failed[source_id]}")
            else:
                links.append((label, source_id))
        elif reached.kind == "unclaimed":
            missing.append(f"{consumed} was declined by {reached.collector}: {reached.reason}")
        else:
            missing.append(f"{consumed} is a workspace, not a calculation")
    item = replace(parent.item, child_runs=(*parent.item.child_runs, *links))
    if missing and item.missing_collector is None:
        reason = f"{parent.outcome.directory}: consumed directories not collected: " + "; ".join(missing)
        degraded = _degraded_job(item.record, parent.collector.provider, item.run, reason)
        item = replace(degraded, identity_stable=True, child_runs=item.child_runs)
    return item


def collect_tree(
    root: str | os.PathLike[str],
    *,
    collectors: Iterable[str | os.PathLike[str]] = (),
    prefer: Iterable[str] = (),
    exclude: Iterable[str] = (),
    fail_fast: bool = False,
    on_unclaimed: Callable[[DirectoryClaim], None] | None = None,
    on_skipped: Callable[[str], None] | None = None,
) -> Iterator[CollectedJob]:
    """Collect every recognized calculation below *root*.

    The collector set is every registered collector
    (``httk.core.register.codes.register_collector``) plus the package
    directories in *collectors*. A local package named like a registered one
    replaces it, which is how a shipped collector is overridden. A registered
    collector whose package cannot be loaded is logged and skipped.

    The walk starts at *root* itself and descends in sorted name order. It does
    not follow symlinked directories and skips names starting with ``.``. A
    directory holding ``.httk-workspace/format.json`` is collected as a
    workspace (:func:`~httk.workflow.collecting.collect`) and not descended.
    In any other directory, each collector whose ``requires`` markers all match
    the directory's file names (case-insensitively, compression suffixes
    stripped) has its recognize hook called; the highest-priority claim wins.
    A hook that raises declines the directory, except that an ``ImportError``
    (a missing dependency) propagates. Directories below a claimed one
    are still visited.

    A claimed directory is read through a stand-in job record whose workdir is
    the directory. Its collect hook returns declared input roles, which become
    the run's input edges and the ``inputs`` of
    :class:`~httk.workflow.collecting.CollectedJob`, and output roles.
    The run's ``source_id`` is ``<collector name>:<identity>``. A collect-hook
    failure yields a degraded item.

    A claim that consumes subdirectories (the ``consumes`` of
    :class:`~httk.workflow.hookapi.Claim`) is a
    multi-directory calculation. Its subdirectories are walked and collected by
    their own collectors as usual, and the claiming item is held back until the
    walk leaves its subtree. It is then yielded, after those children, with
    one ``(relative path, child source id)`` pair per consumed directory in
    ``child_runs``, so :func:`~httk.workflow.storing.store_collected` links its
    run to theirs. A consumed directory that was a deduplicated copy links to the
    earlier copy's run, which has the same source id. One that is missing, not
    visited, a workspace, or not claimed, or whose own item degraded ("consumed
    calculation ... failed: ..."), degrades the claiming item, which then names
    only the children that were collected. The degraded child is still yielded.
    Items are otherwise yielded in walk order. A directory a collector declines with
    :class:`~httk.workflow.hookapi.Unclaimed` is logged as a warning and passed
    to *on_unclaimed*.

    :param root: The tree to sweep.
    :param collectors: Extra collector package directories, loaded alongside the registered ones.
    :param prefer: Collector names that win a tie at the highest priority, first named first.
    :param exclude: Root-relative POSIX glob patterns (:func:`fnmatch.fnmatchcase`) of
        directories that are neither visited nor descended.
    :param fail_fast: Raise the first degraded item instead of yielding it.
    :param on_unclaimed: Receive the outcome of every declined directory.
    :param on_skipped: Receive the job key of every nested workspace job dropped for
        an unreadable ``job.json``.
    :yields: One collected job per recognized calculation and per workspace job, in walk order.
    :raises ValueError: If *root* is not a directory, a *collectors* package is not a
        loadable recognize package, or *fail_fast* observes a degraded item.
    :raises AmbiguousClaimError: If collectors tie at the highest priority and none is preferred.
    :raises IdentityCollisionError: If two directories claim one identity with different marker content.
    """

    failed: dict[str, str] = {}

    def emitted(item: CollectedJob) -> CollectedJob:
        if item.missing_collector is not None and item.run.source_id is not None:
            failed[item.run.source_id] = item.missing_collector
        if fail_fast and item.missing_collector is not None:
            raise ValueError(item.missing_collector)
        return item

    base = Path(root).expanduser().resolve()
    # Calculations that consume subdirectories, innermost last. The walk is a
    # depth-first preorder, so a parent's subtree is complete as soon as the walk
    # reaches a directory outside it; only then is the parent linked and yielded.
    held: list[_Parent] = []
    for outcome, directory, collector in _sweep(base, collectors, prefer, exclude):
        while held and not _within(outcome.directory, held[-1].outcome.directory):
            yield emitted(_linked(base, held.pop(), failed))
        for parent in held:
            if outcome.directory in parent.outcome.consumes:
                parent.reached[outcome.directory] = outcome
        if outcome.kind == "workspace":
            yield from collect(Workspace(directory), fail_fast=fail_fast, on_skipped=on_skipped)
            continue
        if outcome.kind == "unclaimed":
            _LOGGER.warning(
                "%s: not collected by %s: %s", outcome.directory, outcome.collector, outcome.reason, extra=_CONTEXT
            )
            if on_unclaimed is not None:
                on_unclaimed(outcome)
            continue
        if collector is None or outcome.duplicate_of is not None:
            continue
        collected = _collect_directory(base, outcome, collector)
        if collected.missing_collector is not None and collected.run.source_id is not None:
            failed[collected.run.source_id] = collected.missing_collector
        if outcome.consumes:
            held.append(_Parent(outcome, collector, collected, {}))
            continue
        yield emitted(collected)
    while held:
        yield emitted(_linked(base, held.pop(), failed))
