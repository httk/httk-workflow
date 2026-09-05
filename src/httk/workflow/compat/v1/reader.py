"""Read and collect finished result trees written by httk v1.

Run timestamps are parsed from v1's timezone-less names and represented as
UTC-aware datetime values. Manifest-backed immutable ids are stable across
machines; path-derived ids include the source root and are weaker, changing
when the tree moves.
"""

from __future__ import annotations

import bz2
import configparser
import hashlib
import logging
import os
import re
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

import httk.core

from ...collecting import (
    CollectedJob,
    JobRecord,
    _assemble_collected,
    _CollectEnvironmentError,
    _degraded_job,
    _validate_batch_size,
)
from ...models import make_job_key
from ...packages import load_workflow_package

if TYPE_CHECKING:
    from ...scaffold import WorkflowProvider

_LOGGER = logging.getLogger(__name__)
_RUN_FORMAT = "%Y-%m-%d_%H.%M.%S"
_V1_UUID_NAMESPACE = uuid.UUID("f4a4b2d1-7e5a-4b1f-9a0d-2f3c8e6b1d90")
_TASK_PATTERN = re.compile(
    r"^ht\.task\.(?P<taskset>[^.]+)\.(?P<task_id>[^.]+)\.(?P<step>[^.]+)\."
    r"(?P<restarts>[0-9]+)\.(?P<owner>[^.]+)\.(?P<priority>[1-5])\."
    r"(?P<status>waitstart|waitstep|waitsubtasks|running|finished|broken|stopped|timeout)$"
)


def parse_v1_task_name(value: str) -> dict[str, str] | None:
    """Parse a legacy task basename, returning ``None`` when unrelated.

    :param value: Parse this legacy task basename.
    :return: Parsed task fields, or ``None`` for an unrelated name.
    """

    match = _TASK_PATTERN.fullmatch(value)
    return None if match is None else match.groupdict()


@dataclass(frozen=True)
class V1FinishedTask:
    """Describe one finished v1 task and its newest dated run directory."""

    directory: Path
    rundir: Path
    taskset: str
    task_id: str
    computation_date: datetime
    code_name: str
    code_version: str
    description: str | None
    manifest_hash: str | None
    _stable_path: str | None = field(default=None, repr=False, compare=False)

    @property
    def immutable_id(self) -> str:
        """Return the stable manifest id or a weaker path-derived identity."""

        fallback = self._stable_path or f"{self.directory.name}/{self.rundir.name}"
        return "httk-v1:" + (self.manifest_hash or hashlib.sha256(fallback.encode()).hexdigest())

    @property
    def identity_stable(self) -> bool:
        """Whether this task's identity is manifest-backed and relocation-stable.

        A task without a readable ``ht.manifest`` falls back to a path-derived
        identity that changes when the tree moves, so its collected result cannot
        be de-duplicated across machines; this is ``False`` for those tasks.
        """

        return self.manifest_hash is not None


def _dated_runs(directory: Path) -> list[tuple[datetime, Path]]:
    runs: list[tuple[datetime, Path]] = []
    try:
        children = directory.iterdir()
    except OSError:
        return runs
    for child in children:
        if not child.is_dir() or not child.name.startswith("ht.run."):
            continue
        try:
            date = datetime.strptime(child.name[7:], _RUN_FORMAT).replace(tzinfo=UTC)
        except ValueError:
            continue
        runs.append((date, child))
    return sorted(runs, key=lambda item: (item[0], item[1].name))


def code_of(program_path: Path) -> tuple[str, str]:
    """Read code name/version from lines 2 and 3 of a v1 program.

    Both ht_steps and ht_run use this format. Missing, unreadable, or
    malformed metadata returns the legacy fallback ("unknown", "0").
    """

    try:
        lines = program_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return "unknown", "0"
    if len(lines) < 3 or not lines[1].startswith("#") or not lines[2].startswith("#"):
        return "unknown", "0"
    name, version = lines[1][1:].strip(), lines[2][1:].strip()
    return (name, version) if name and version else ("unknown", "0")


def task_file(directory: Path, name: str) -> Path:
    """Return a plain or bzip2-compressed task member without opening it."""

    plain = directory / name
    compressed = directory / f"{name}.bz2"
    if plain.exists():
        return plain
    if compressed.exists():
        return compressed
    raise FileNotFoundError(f"task file {name!r} is missing from {directory} and {compressed}")


def _manifest_hash(path: Path) -> str | None:
    try:
        body = bz2.decompress(path.read_bytes())
        lines = body.splitlines(keepends=True)
        # This mirrors old httk.task.reader.read_manifest: ''.join(lines[:-2]).
        hashed = body if len(lines) < 3 else b"".join(lines[:-2])
        return hashlib.sha256(hashed).hexdigest()
    except (OSError, EOFError, ValueError):
        _LOGGER.warning("cannot read legacy manifest %s", path, exc_info=True)
        return None


def _finished_task(directory: Path, parsed: Mapping[str, str], root: Path) -> V1FinishedTask | None:
    runs = _dated_runs(directory)
    if not runs:
        _LOGGER.warning("skipping finished v1 task without a dated run directory: %s", directory)
        return None
    computation_date, rundir = runs[-1]
    program = directory / "ht_steps"
    if not program.is_file():
        program = directory / "ht_run"
    code_name, code_version = code_of(program)
    description: str | None = None
    config = configparser.ConfigParser()
    try:
        config.read(directory / "ht.config", encoding="utf-8")
        if config.has_option("main", "description"):
            description = config.get("main", "description")
    except (OSError, configparser.Error):
        pass
    manifest = directory / "ht.manifest.bz2"
    try:
        relative = directory.resolve().relative_to(root.resolve())
        stable_path = PurePosixPath(root.resolve().as_posix(), relative.as_posix(), rundir.name).as_posix()
    except ValueError:
        stable_path = PurePosixPath(directory, rundir.name).as_posix()
    return V1FinishedTask(
        directory=directory,
        rundir=rundir,
        taskset=parsed["taskset"],
        task_id=parsed["task_id"],
        computation_date=computation_date,
        code_name=code_name,
        code_version=code_version,
        description=description,
        manifest_hash=_manifest_hash(manifest) if manifest.is_file() else None,
        _stable_path=stable_path,
    )


def finished_tasks(root: str | os.PathLike[str], *, stats: dict[str, object] | None = None) -> Iterator[V1FinishedTask]:
    """Lazily yield finished v1 tasks in deterministic walk order.

    Every ht.task.* directory is a walk boundary, so nested subtasks are not
    scanned. ht.run.current is intentionally ignored because only dated run
    names define a computation date.

    When *stats* is supplied it is populated, once the walk is exhausted, with
    ``unfinished_by_status`` (a status-keyed :class:`collections.Counter` of the
    tasks the regex matched that were not ``.finished``) and ``skipped_no_rundir``
    (the count of finished tasks dropped for lacking a dated run directory).

    :param root: Walk this finished result tree.
    :param stats: Receive the finished-tree counters, or leave unpopulated.
    :yields: One finished, run-bearing task per readable directory.
    """

    base = Path(root)
    unfinished: Counter[str] = Counter()
    skipped_no_rundir = 0
    for walk_root, dirs, _files in os.walk(base, topdown=True):
        dirs[:] = sorted(dirs)
        for name in tuple(dirs):
            if not name.startswith("ht.task."):
                continue
            dirs.remove(name)
            parsed = parse_v1_task_name(name)
            if parsed is None:
                continue
            if parsed["status"] != "finished":
                unfinished[parsed["status"]] += 1
                continue
            task = _finished_task(Path(walk_root) / name, parsed, base)
            if task is None:
                skipped_no_rundir += 1
                continue
            yield task
    if stats is not None:
        stats["unfinished_by_status"] = unfinished
        stats["skipped_no_rundir"] = skipped_no_rundir


def run_directory(record: JobRecord) -> Path:
    """Return the result directory described by record.workdir.

    Postprocess hooks must read result files from this path. Live v1 jobs use
    ht.run.current; synthesized old-tree records use their dated run dir.
    """

    directory = record.workdir
    if directory is None:
        raise ValueError("v1 collect record has no workdir")
    return directory


def _relative(root: Path, path: Path) -> PurePosixPath:
    try:
        return PurePosixPath(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return PurePosixPath(path)


def _record(root: Path, task: V1FinishedTask, workflow_id: str, declaration: Mapping[str, object] | None) -> JobRecord:
    payload = _relative(root, task.directory)
    workdir = _relative(root, task.rundir)
    job_id = str(uuid.uuid5(_V1_UUID_NAMESPACE, task.immutable_id))
    tag = "v1-" + hashlib.sha256(task.immutable_id.encode()).hexdigest()[:16]
    placement = payload.parent if payload.parent != PurePosixPath(".") else PurePosixPath("v1")
    declarations = {"workflow": {"declared": None if declaration is None else dict(declaration), "observed": None}}
    return JobRecord(
        workspace_root=root,
        workspace_id="httk-v1",
        job_id=job_id,
        job_key=make_job_key(job_id, tag),
        job={"workflow": workflow_id, "parameters": {}},
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=placement,
        payload_path=payload,
        workdir_path=workdir,
        data_path=None,
        data_generation=None,
        provenance={"activations": [], "gaps": False},
        runner_steps=None,
        children={},
        declarations=declarations,
    )


def _infer_record(record: JobRecord, outputs: Mapping[str, object]) -> JobRecord:
    """Give an extract-only sweep the declarations assembly needs."""

    declaration: dict[str, object] = {"outputs": []}
    output_list = declaration["outputs"]
    assert isinstance(output_list, list)
    for role, value in outputs.items():
        entry_type = getattr(value, "type", None)
        if not isinstance(role, str) or not isinstance(entry_type, str):
            raise ValueError("extract output roles require string roles and entry types")
        output_list.append({"name": role, "entry_type": entry_type})
    return replace(record, declarations={"workflow": {"declared": declaration, "observed": None}})


def collect_finished_tree(
    root: str | os.PathLike[str],
    *,
    workflow_dir: str | os.PathLike[str] | None = None,
    extract: Callable[[V1FinishedTask], Mapping[str, object]] | None = None,
    workflow_id: str = "httk.v1.finished",
    stats: dict[str, object] | None = None,
    fail_fast: bool = False,
    batch_size: int = 64,
) -> Iterator[CollectedJob]:
    """Collect old finished trees through a package hook or direct extractor.

    Exactly one of workflow_dir and extract is required. A per-task
    hook/extractor failure degrades that task and the sweep continues unless
    ``fail_fast`` is set. Missing host dependencies still abort collection.

    :param root: Collect the finished tree rooted here.
    :param workflow_dir: Locate the directory workflow package, when used.
    :param extract: Supply a direct per-task extractor, when used.
    :param workflow_id: Name the synthesized workflow, for the extractor path.
    :param stats: Receive the finished-tree counters from :func:`finished_tasks`.
    :param fail_fast: Raise the first per-task collection failure instead of
        yielding its degraded result.
    :param batch_size: Validate the live-collection compatible positive window
        size; v1 tasks already stream one at a time.
    :yields: One collected job per finished task, degraded ones included.
    :raises ValueError: If not exactly one of ``workflow_dir``/``extract`` is
        given, or the package declares no collector.
    """

    _validate_batch_size(batch_size)
    if (workflow_dir is None) == (extract is None):
        raise ValueError("exactly one of workflow_dir or extract is required")
    provider: WorkflowProvider | None = None
    hook: Callable[[JobRecord], Mapping[str, object]] | None = None
    declaration: Mapping[str, object] | None = None
    if workflow_dir is not None:
        provider = load_workflow_package(Path(workflow_dir), register=False)
        if not callable(provider.collector):
            raise ValueError(f"workflow package {Path(workflow_dir) / 'httk_workflow.toml'} has no collector")
        hook = provider.collector
        workflow_id = provider.workflow_id
        declaration = provider.declarations.get("workflow")
    base = Path(root).resolve()
    unstable = 0
    for task in finished_tasks(base, stats=stats):
        if not task.identity_stable:
            unstable += 1
        record = _record(base, task, workflow_id, declaration)
        run = httk.core.Run(
            workflow_declaration_uri=None if provider is None else provider.declaration_uri,
            inputs=(),
            artifacts=(),
            outputs=(),
            source_id=task.immutable_id,
            last_modified=task.computation_date,
        )
        try:
            if hook is not None:
                raw_outputs = hook(record)
            else:
                assert extract is not None
                raw_outputs = extract(task)
            if not isinstance(raw_outputs, Mapping):
                raise ValueError("collector must return a mapping of output roles")
            if provider is None:
                record = _infer_record(record, raw_outputs)
            collected = _assemble_collected(
                f"{record.workspace_id}:{record.job_id}", record, provider, run, raw_outputs
            )
        except (ImportError, _CollectEnvironmentError):
            raise
        except Exception as exc:
            collected = _degraded_job(record, provider, run, f"{task.directory}: {exc}")
        collected = replace(collected, identity_stable=task.identity_stable)
        if fail_fast and collected.missing_collector is not None:
            raise ValueError(collected.missing_collector)
        yield collected
    if unstable:
        _LOGGER.warning(
            "v1 finished-tree collect: %d task(s) have path-derived (unstable) identities without a manifest hash; "
            "moving the tree changes their collected identity",
            unstable,
        )
