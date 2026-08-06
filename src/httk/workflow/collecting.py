"""The results-collect contract: everything a data layer needs about one job.

Collecting is the read-only counterpart of running work. A manager decides what
happens next; a job_records reports what already happened, once, per job that
stopped, in a shape a data layer can store without knowing anything about
markers, journals, or leases.

That shape — :class:`~httk.workflow.collecting.JobRecord` — is the layering boundary of *httk₂*.
*httk-workflow* has no database dependency and never will: it produces records,
and something else consumes them. A consumer therefore reads results like this,
and nothing in this module knows what ``store`` or ``load_vasp`` are:

.. code-block:: python

    for record in job_records(workspace):
        store.save(load_vasp(record))

Every member of a record is derived from exactly the authoritative state a
manager reads — the marker below ``state/``, the journal frames that marker's
chain names, and the immutable ``job.json`` — so a record never says anything the
workspace does not. Two properties follow from that and are the reason this
module exists at all:

* **The executed code is pinned.** A record carries the immutable job digest and
  the complete runner identity: executor, source, path, and the SHA-256 the job
  pinned for every runner that lives outside its payload. For a runner named by
  the reserved ``pkg:`` form the installed distribution and its version are
  reported as well, so a stored result names the software that produced it.
* **Damage is reported, never guessed.** A job whose journal chain is broken is
  still collected, with whatever remains readable and ``gaps`` set, because a
  result that exists must not become invisible just because part of its history
  did not survive.

:func:`job_records` is lazily evaluated over one scan of the workspace. By design it
iterates jobs without materializing the workspace, and building one record reads
only that job's own payload and journal chain.
"""

from __future__ import annotations

import importlib
import json
import logging
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from importlib import metadata
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote

from ._util import read_json, require_mapping, require_string, tree_digest
from .errors import FormatError
from .introspection import (
    _job_of,
    _optional_int,
    _optional_string,
    _state_of,
    _workdir_relative,
    job_frames,
)
from .models import (
    JOB_STATE_DIRECTORY,
    STATE_KINDS,
    TERMINAL_KINDS,
    Failure,
    JobDefinition,
    Marker,
    canonical_uuid,
    normalize_placement,
    parse_job_key,
    parse_package_runner,
    validate_declaration_name,
    validate_failure,
    validate_label,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "COLLECTABLE_KINDS",
    "COLLECT_FORMAT",
    "COLLECT_FORMAT_VERSION",
    "DEFAULT_COLLECT_STATES",
    "CollectedJob",
    "JobRecord",
    "children_of",
    "collect",
    "collect_kinds",
    "declarations_of",
    "job_records",
    "module_distribution",
    "record_of",
    "runner_provenance",
    "timeline",
]

COLLECT_FORMAT = "httk-workflow-collect"
COLLECT_FORMAT_VERSION = 1
#: The state kinds a job_records may read: a job that stopped. The terminal kinds are
#: final, and ``paused`` is included because a paused job published a real outcome
#: and produced real results before an operator was asked to look at it.
COLLECTABLE_KINDS = tuple(kind for kind in STATE_KINDS if kind in TERMINAL_KINDS or kind == "paused")
#: The default selection: the jobs that finished the way they were meant to.
DEFAULT_COLLECT_STATES = ("succeeded",)
_FILE_URL_PREFIX = "file://"

if TYPE_CHECKING:
    import httk.core

    from .scaffold import WorkflowProvider


def _core():
    """Load optional core provenance types only when the collect layer is used."""

    return importlib.import_module("httk.core")


@dataclass(frozen=True)
class CollectedJob:
    """One collected job and the framework-owned provenance it assembled."""

    workflow_id: str
    outputs: Mapping[str, object]
    unfulfilled: tuple[str, ...]
    run: httk.core.Run
    products: tuple[httk.core.ProductLink, ...]
    record: JobRecord
    missing_postprocessor: str | None = None


# ---------------------------------------------------------------------------
# Runner provenance
# ---------------------------------------------------------------------------


def _editable_root(distribution: metadata.Distribution) -> Path | None:
    """Return the source directory one editable installation was made from."""

    try:
        raw = distribution.read_text("direct_url.json")
    except OSError:
        return None
    if raw is None:
        return None
    try:
        recorded = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(recorded, Mapping):
        return None
    directory = recorded.get("dir_info")
    if not isinstance(directory, Mapping) or not directory.get("editable"):
        return None
    url = recorded.get("url")
    if not isinstance(url, str) or not url.startswith(_FILE_URL_PREFIX):
        return None
    return Path(unquote(url[len(_FILE_URL_PREFIX) :]))


def _provides_module(root: Path, relative: PurePosixPath) -> bool:
    """Report whether *root* is the source tree holding one importable module."""

    for prefix in ((), ("src",)):
        base = root.joinpath(*prefix, *relative.parts)
        try:
            if (base / "__init__.py").is_file() or base.with_suffix(".py").is_file():
                return True
        except OSError:  # pragma: no cover - unreadable installation metadata
            continue
    return False


@cache
def module_distribution(module: str) -> tuple[str, str] | None:
    """Return the ``(name, version)`` of the distribution installing *module*.

    The answer is read from installation metadata alone and never by importing
    anything: a module name comes out of an untrusted ``job.json``, and importing
    it to ask which package it belongs to would execute code during a read-only
    job_records. A wheel installation is recognized by the module path recorded in
    its file list, and an editable installation by the source tree its
    ``direct_url.json`` names. Anything else — a module on ``PYTHONPATH`` that no
    installed distribution owns — is reported as unknown rather than guessed.
    """

    relative = PurePosixPath(*module.split("."))
    recorded = {(relative / "__init__.py").as_posix(), f"{relative.as_posix()}.py"}
    for distribution in metadata.distributions():
        try:
            name = distribution.name
            if not name:
                continue
            if any(entry.as_posix() in recorded for entry in distribution.files or ()):
                return name, distribution.version
            root = _editable_root(distribution)
            if root is not None and _provides_module(root, relative):
                return name, distribution.version
        except (OSError, metadata.PackageNotFoundError):  # pragma: no cover - damaged metadata
            continue
    return None


def runner_provenance(job: JobDefinition) -> dict[str, object] | None:
    """Return what installation provenance exists for one job's runner.

    Only the reserved ``pkg:<module>/<resource>`` form of an installed runner
    resolves to a Python distribution, so every other runner reports ``None``: a
    payload runner is pinned by the job digest, and a workspace or plain
    installed runner is pinned by ``runner.sha256`` and nothing else is known
    about where it came from.
    """

    if job.runner_source != "installed":
        return None
    try:
        package = parse_package_runner(job.runner_path.as_posix())
    except FormatError:  # pragma: no cover - a stored job was validated already
        return None
    if package is None:
        return None
    module, resource = package
    distribution = module_distribution(module)
    return {
        "module": module,
        "resource": resource.as_posix(),
        "distribution": None if distribution is None else distribution[0],
        "version": None if distribution is None else distribution[1],
    }


# ---------------------------------------------------------------------------
# The journal-derived timeline
# ---------------------------------------------------------------------------


def _attempt(frame: Mapping[str, Any]) -> dict[str, object]:
    """Open one attempt record from the ``claimed`` frame that started it."""

    return {
        "attempt_id": _optional_string(frame.get("attempt_id")),
        "ordinal": _optional_int(frame.get("attempt_ordinal")),
        "manager_id": _optional_string(frame.get("manager_id")),
        "writer_id": _optional_string(frame.get("writer_id")),
        "record_ref": _optional_string(frame.get("record_ref")),
        "claimed_at": _optional_string(frame.get("created_at")),
        "started_at": None,
        "finished_at": None,
        "outcome_action": None,
        "failure": None,
    }


def _activation(frame: Mapping[str, Any]) -> dict[str, object]:
    """Open one activation record from the frame that started it."""

    return {
        "activation_id": _optional_string(frame.get("activation_id")),
        "activation_ordinal": _optional_int(frame.get("activation_ordinal")),
        "step": _optional_string(frame.get("step")),
        "reason": _optional_string(frame.get("reason")),
        "attempts": [],
    }


def timeline(frames: Sequence[Mapping[str, Any]]) -> dict[str, object]:
    """Return the activation and attempt timeline of one job, oldest first.

    The frames are exactly the ones the workspace's ``job_frames`` reader
    walked, so this is a pure regrouping of recorded history: an activation is
    every consecutive frame sharing one ``activation_id``, and an attempt is
    opened by the ``claimed`` frame that consumed a budget and closed by the
    first frame that reported how it ended. A frame the journal could not return
    sets ``gaps`` and is skipped, which keeps a job with a damaged history
    collectable instead of silent.
    """

    activations: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    attempt: dict[str, object] | None = None
    gaps = False
    for frame in frames:
        if frame.get("error") is not None:
            gaps = True
            continue
        activation_id = _optional_string(frame.get("activation_id"))
        if current is None or current["activation_id"] != activation_id:
            current = _activation(frame)
            activations.append(current)
            attempt = None
        created = _optional_string(frame.get("created_at"))
        kind = _optional_string(frame.get("kind"))
        if kind == "claimed":
            attempt = _attempt(frame)
            attempts = current["attempts"]
            if isinstance(attempts, list):
                attempts.append(attempt)
            continue
        if attempt is None or _optional_string(frame.get("attempt_id")) != attempt["attempt_id"]:
            continue
        if kind == "running":
            attempt["started_at"] = _optional_string(frame.get("started_at")) or created
            continue
        if kind == "committing":
            attempt["outcome_action"] = _optional_string(frame.get("outcome_action"))
        failure = frame.get("failure")
        if attempt["failure"] is None and isinstance(failure, Mapping):
            attempt["failure"] = dict(failure)
        if attempt["finished_at"] is None:
            attempt["finished_at"] = created
    return {"activations": activations, "gaps": gaps}


# ---------------------------------------------------------------------------
# The children of a campaign parent
# ---------------------------------------------------------------------------


def _job_id_of(job_key: str) -> str | None:
    try:
        return parse_job_key(job_key)[1]
    except FormatError:
        return None


def _observed_children(value: object) -> Iterator[tuple[str, str | None, str | None, str | None]]:
    """Yield ``(label, job_id, job_key, kind)`` of every labeled child reference."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return
    for raw in value:
        if not isinstance(raw, Mapping):
            continue
        label = _optional_string(raw.get("label"))
        if label is None:
            continue
        yield (
            label,
            _optional_string(raw.get("job_id")),
            _optional_string(raw.get("job_key")),
            _optional_string(raw.get("kind")),
        )


def _frame_children(frame: Mapping[str, Any]) -> Iterator[tuple[str, str | None, str | None, str | None]]:
    """Yield every labeled child one state frame names."""

    labels = frame.get("child_labels")
    if isinstance(labels, Mapping):
        # The spawn set of the outcome being committed: the label is declared
        # there, and the child identity is its job key.
        for job_key, label in labels.items():
            if isinstance(job_key, str) and isinstance(label, str) and label:
                yield label, _job_id_of(job_key), job_key, None
    join = frame.get("join")
    if isinstance(join, Mapping):
        yield from _observed_children(join.get("children"))
    yield from _observed_children(frame.get("join_summary"))


def children_of(frames: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, object]]:
    """Return the labeled children one job registered, keyed by spawn label.

    A campaign therefore collects as a tree: every record names the children it
    spawned, and each of those is a job a consumer collects in its own right. A
    label is mandatory in ``core-v2``, so an unlabeled child reference — only
    possible in a workspace written by an older profile — is left out rather than
    given an invented name. A label reused by a later activation names the child
    of the most recent spawn under it.
    """

    children: dict[str, dict[str, object]] = {}
    for frame in frames:
        if frame.get("error") is not None:
            continue
        for label, job_id, job_key, kind in _frame_children(frame):
            existing = children.get(label)
            if existing is None or existing.get("job_id") != job_id:
                children[label] = {"job_id": job_id, "job_key": job_key, "kind": kind}
            elif kind is not None:
                existing["kind"] = kind
    return children


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


def _job_mapping(job: JobDefinition) -> dict[str, object]:
    """Return the validated job definition as one JSON-safe mapping.

    This is the definition as the protocol validated it, not the stored bytes:
    those are what ``digest`` pins, and a consumer that needs them reads
    ``job.json`` at ``payload_path``.
    """

    policy = job.retry_policy
    return {
        "id": job.id,
        "tag": job.tag,
        "job_key": job.job_key,
        "name": job.name,
        "workflow": job.workflow,
        "digest": job.digest,
        "initial_step": job.initial_step,
        "priority": job.priority,
        "runner": {
            "executor": job.runner_executor,
            "source": job.runner_source,
            "path": job.runner_path.as_posix(),
            "sha256": job.runner_sha256,
            "arguments": list(job.runner_arguments),
        },
        "workdir": {"mode": job.workdir_mode, "path": job.workdir_path.as_posix()},
        "data": {"mode": job.data_mode},
        "claim": {"pool": job.claim_pool, "required_capabilities": sorted(job.required_capabilities)},
        "retry_policy": {
            "maximum_attempts_per_activation": policy.maximum_attempts_per_activation,
            "maximum_total_attempts": policy.maximum_total_attempts,
            "maximum_activations": policy.maximum_activations,
            "retry_on": sorted(policy.retry_on),
        },
        "resources": dict(job.resources),
        "inputs": dict(job.inputs),
        "parent": None if job.parent is None else dict(job.parent),
    }


def _optional_posix(value: object) -> PurePosixPath | None:
    return PurePosixPath(value) if isinstance(value, str) and value else None


def _failure_of(state: Mapping[str, Any]) -> tuple[Failure | None, bool]:
    """Return the unified failure of one terminal state, reporting damage."""

    raw = state.get("failure")
    if not isinstance(raw, Mapping):
        return None, False
    try:
        return validate_failure(raw), False
    except FormatError as exc:
        # Evidence of a malformed record is still evidence, so the record keeps
        # what was written and says that its history is not intact.
        return Failure("protocol_error", f"the recorded failure object is unusable: {exc}", details=dict(raw)), True


@dataclass(frozen=True)
class JobRecord:
    """Everything a data layer needs about one job that stopped.

    Paths appear twice on purpose. The members ``payload_path``,
    ``workdir_path``, and ``data_path`` are workspace relative, which is what a
    stored record must hold so it survives moving the workspace; the properties
    :attr:`payload`, :attr:`workdir`, and :attr:`data` resolve them against the
    workspace this record was collected from, which is what code reading result
    files wants.
    """

    #: The absolute root of the workspace this record was collected from.
    workspace_root: Path
    workspace_id: str
    job_id: str
    job_key: str
    #: The validated job definition, including its immutable digest and the
    #: complete identity of the runner that executed it.
    job: Mapping[str, object]
    #: The installed distribution behind a ``pkg:`` runner, or ``None``.
    runner_provenance: Mapping[str, object] | None
    #: The terminal state kind this job stopped in.
    state: str
    failure: Failure | None
    placement: PurePosixPath
    payload_path: PurePosixPath
    workdir_path: PurePosixPath | None
    data_path: PurePosixPath | None
    data_generation: int | None
    #: The activation and attempt timeline derived from the journal, oldest
    #: first, plus the ``gaps`` flag of :func:`timeline`.
    provenance: Mapping[str, object]
    #: The step set this job's runner declared, when one was ever recorded.
    runner_steps: tuple[str, ...] | None
    #: The labeled children this job spawned, keyed by spawn label.
    children: Mapping[str, Mapping[str, object]]
    #: The workflow declarations of this job, carried verbatim and keyed by name.
    #: Every name either source knows maps to ``{"declared": ..., "observed":
    #: ...}``: what ``job.json`` declared before the job ran, and what the job
    #: itself observed at run time, each ``None`` when that source has nothing.
    #: The two are reported side by side and never merged — only a consumer that
    #: understands the vocabulary may reconcile them.
    declarations: Mapping[str, Mapping[str, Mapping[str, object] | None]]
    #: Reserved: the machine-readable self-description a runner prints for
    #: ``--describe`` attaches here in a later phase. Always ``None`` today.
    runner_description: Mapping[str, object] | None = None

    @property
    def payload(self) -> Path:
        """The absolute payload directory of this job."""

        return self.workspace_root.joinpath(*self.payload_path.parts)

    @property
    def workdir(self) -> Path | None:
        """The absolute workdir of this job's last attempt, when one is known."""

        return None if self.workdir_path is None else self.workspace_root.joinpath(*self.workdir_path.parts)

    @property
    def data(self) -> Path | None:
        """The absolute transactional data directory, for a job that has one."""

        return None if self.data_path is None else self.workspace_root.joinpath(*self.data_path.parts)

    @property
    def gaps(self) -> bool:
        """Whether part of this job's recorded history could not be read."""

        return bool(self.provenance.get("gaps", False))

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this record."""

        return {
            "format": COLLECT_FORMAT,
            "format_version": COLLECT_FORMAT_VERSION,
            "workspace": str(self.workspace_root),
            "workspace_id": self.workspace_id,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "job": dict(self.job),
            "runner_provenance": None if self.runner_provenance is None else dict(self.runner_provenance),
            "state": self.state,
            "failure": None if self.failure is None else self.failure.as_mapping(),
            "placement": self.placement.as_posix(),
            "payload_path": self.payload_path.as_posix(),
            "workdir_path": None if self.workdir_path is None else self.workdir_path.as_posix(),
            "data_path": None if self.data_path is None else self.data_path.as_posix(),
            "data_generation": self.data_generation,
            "provenance": dict(self.provenance),
            "runner_steps": None if self.runner_steps is None else list(self.runner_steps),
            "runner_description": None if self.runner_description is None else dict(self.runner_description),
            "children": {label: dict(child) for label, child in self.children.items()},
            "declarations": {
                name: {
                    "declared": None if entry.get("declared") is None else dict(entry["declared"] or {}),
                    "observed": None if entry.get("observed") is None else dict(entry["observed"] or {}),
                }
                for name, entry in self.declarations.items()
            },
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> JobRecord:
        """Rebuild one record from the mapping :meth:`as_mapping` produced."""

        if value.get("format") != COLLECT_FORMAT or value.get("format_version") != COLLECT_FORMAT_VERSION:
            raise FormatError(f"collect record must use {COLLECT_FORMAT} version {COLLECT_FORMAT_VERSION}")
        failure = value.get("failure")
        provenance = value.get("provenance")
        description = value.get("runner_description")
        provided = value.get("runner_provenance")
        return cls(
            workspace_root=Path(require_string(value.get("workspace"), "workspace")),
            workspace_id=require_string(value.get("workspace_id"), "workspace_id"),
            job_id=canonical_uuid(value.get("job_id"), "job_id"),
            job_key=require_string(value.get("job_key"), "job_key"),
            job=dict(require_mapping(value.get("job"), "job")),
            runner_provenance=None if not isinstance(provided, Mapping) else dict(provided),
            state=require_string(value.get("state"), "state"),
            failure=None if not isinstance(failure, Mapping) else validate_failure(failure),
            placement=normalize_placement(require_string(value.get("placement"), "placement")),
            payload_path=PurePosixPath(require_string(value.get("payload_path"), "payload_path")),
            workdir_path=_optional_posix(value.get("workdir_path")),
            data_path=_optional_posix(value.get("data_path")),
            data_generation=_optional_int(value.get("data_generation")),
            provenance={} if provenance is None else dict(require_mapping(provenance, "provenance")),
            runner_steps=_steps(value.get("runner_steps")),
            children=_children(value.get("children")),
            declarations=_declarations(value.get("declarations")),
            runner_description=None if not isinstance(description, Mapping) else dict(description),
        )


def _steps(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise FormatError("runner_steps must be an array")
    return tuple(require_string(item, "runner_steps item") for item in value)


def _recorded_steps(value: object) -> tuple[str, ...] | None:
    """Return the step set a state frame recorded, tolerating a damaged one."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return None
    return tuple(item for item in value if isinstance(item, str) and item)


def _declarations(value: object) -> dict[str, dict[str, Mapping[str, object] | None]]:
    """Rebuild the declared/observed pairs :meth:`as_mapping` produced."""

    mapping = require_mapping({} if value is None else value, "declarations")
    result: dict[str, dict[str, Mapping[str, object] | None]] = {}
    for name, entry in mapping.items():
        declaration = validate_declaration_name(name, "declaration name")
        item = require_mapping(entry, f"declarations.{declaration}")
        result[declaration] = {
            side: None if item.get(side) is None else dict(require_mapping(item[side], f"declarations.{name}.{side}"))
            for side in ("declared", "observed")
        }
    return result


def declarations_of(
    job: JobDefinition, payload: Path
) -> tuple[dict[str, dict[str, Mapping[str, object] | None]], bool]:
    """Return the declarations of one job and whether any observed one is lost.

    Every name either source knows appears exactly once. ``declared`` is the
    document ``job.json`` carried, and ``observed`` is the runtime-refined one
    the job wrote below ``.httk-job/declarations/``; both are carried verbatim
    and reported side by side, because merging them would require understanding
    a vocabulary this module deliberately does not implement. An observed
    document that cannot be read is reported as ``None`` with the damage flag
    set, exactly like every other unreadable evidence a job_records still reports.
    """

    result: dict[str, dict[str, Mapping[str, object] | None]] = {
        name: {"declared": dict(document), "observed": None} for name, document in sorted(job.declarations.items())
    }
    damaged = False
    directory = payload / JOB_STATE_DIRECTORY / "declarations"
    try:
        stored = sorted(item for item in directory.glob("*.json") if item.is_file())
    except OSError as exc:
        _LOGGER.warning("cannot list the observed declarations of %s: %s", job.job_key, exc)
        return result, True
    for path in stored:
        try:
            name = validate_declaration_name(path.name.removesuffix(".json"), "declaration name")
        except FormatError as exc:
            _LOGGER.warning("ignoring the observed declaration %s of %s: %s", path.name, job.job_key, exc)
            damaged = True
            continue
        entry = result.setdefault(name, {"declared": None, "observed": None})
        try:
            entry["observed"] = read_json(path)
        except FormatError as exc:
            _LOGGER.warning("cannot read the observed declaration %s of %s: %s", name, job.job_key, exc)
            damaged = True
    return result, damaged


def _children(value: object) -> dict[str, Mapping[str, object]]:
    mapping = require_mapping({} if value is None else value, "children")
    return {
        validate_label(label, "child label"): dict(require_mapping(child, "child")) for label, child in mapping.items()
    }


def record_of(workspace: Workspace, marker: Marker) -> JobRecord | None:
    """Return the job_records record of the one job *marker* names.

    ``None`` means this job has no readable ``job.json`` and therefore no
    definition to report: the whole contract of a record is the *validated* job
    behind a result, so an unusable payload is reported through the module logger
    and left to a workspace tool instead of being described by guesswork.
    """

    job, job_error = _job_of(workspace, marker)
    if job is None:
        _LOGGER.error(
            "not collecting %s: %s (repair the payload with a workspace tool)",
            marker.job_key,
            job_error,
            extra={"event": "collect_unusable", "job_key": marker.job_key},
        )
        return None
    state, state_error = _state_of(workspace, marker)
    if state_error is not None:
        _LOGGER.warning("collecting %s without its state frame: %s", marker.job_key, state_error)
    frames = job_frames(workspace, marker)
    provenance = timeline(frames)
    failure, failure_damaged = _failure_of(state)
    if state_error is not None or failure_damaged:
        provenance["gaps"] = True
    payload_path = marker.placement / marker.job_key
    workdir = _workdir_relative(job, state)
    declarations, declarations_damaged = declarations_of(job, workspace.payload_path(marker.placement, marker.job_key))
    if declarations_damaged:
        provenance["gaps"] = True
    return JobRecord(
        workspace_root=workspace.root,
        workspace_id=workspace.workspace_id,
        job_id=marker.job_id,
        job_key=marker.job_key,
        job=_job_mapping(job),
        runner_provenance=runner_provenance(job),
        state=marker.kind,
        failure=failure,
        placement=marker.placement,
        payload_path=payload_path,
        workdir_path=None if workdir is None else payload_path / workdir,
        data_path=payload_path / "data" if job.data_mode == "transactional" else None,
        data_generation=_optional_int(state.get("data_generation")),
        provenance=provenance,
        runner_steps=_recorded_steps(state.get("runner_steps")),
        children=children_of(frames),
        declarations=declarations,
    )


def collect_kinds(states: Iterable[str]) -> tuple[str, ...]:
    """Validate the requested state kinds against what collect may read."""

    kinds = tuple(dict.fromkeys(states))
    if not kinds:
        raise ValueError("collect needs at least one state kind")
    unknown = [kind for kind in kinds if kind not in COLLECTABLE_KINDS]
    if unknown:
        raise ValueError(
            f"collect only reads jobs that stopped, so {', '.join(unknown)} cannot be collected; "
            f"collectable kinds: {', '.join(COLLECTABLE_KINDS)}"
        )
    return kinds


def job_records(
    workspace: Workspace,
    *,
    states: Iterable[str] = DEFAULT_COLLECT_STATES,
    placement: str | PurePosixPath | None = None,
) -> Iterator[JobRecord]:
    """Yield one :class:`~httk.workflow.collecting.JobRecord` per finished job of *workspace*.

    *states* selects which stopped jobs are reported and defaults to the
    successful ones; every requested kind is validated against
    ``COLLECTABLE_KINDS`` before anything is read. *placement* restricts the
    job_records to the jobs at or below one placement, exactly as
    ``httk workflow job list --placement`` does.

    The result is a lazy iterator over one scan of the requested state
    directories. Nothing is materialized, and building a record reads only that
    job's own ``job.json`` and journal chain, so collecting is a single pass over
    a workspace of any size. Attach read-only — ``Workspace(root,
    mutable=False)`` — when nothing else in the process needs to write.
    """

    kinds = collect_kinds(states)
    prefix = None if placement is None else normalize_placement(placement).parts
    for marker in workspace.scan_markers(kinds):
        if prefix is not None and marker.placement.parts[: len(prefix)] != prefix:
            continue
        record = record_of(workspace, marker)
        if record is not None:
            yield record


def _overlay_edges(
    base: tuple[httk.core.RunEdge, ...], owned: tuple[httk.core.RunEdge, ...]
) -> tuple[httk.core.RunEdge, ...]:
    replacements = {edge.label: edge for edge in owned}
    result: list[httk.core.RunEdge] = []
    replaced: set[str] = set()
    for edge in base:
        replacement = replacements.get(edge.label)
        result.append(edge if replacement is None else replacement)
        if replacement is not None:
            replaced.add(edge.label)
    result.extend(edge for edge in owned if edge.label not in replaced)
    return tuple(result)


def _workflow_document(provider: object) -> Mapping[str, object]:
    declarations = getattr(provider, "declarations", {})
    document = declarations.get("workflow") if isinstance(declarations, Mapping) else None
    return document if isinstance(document, Mapping) else {}


def _job_workflow_document(record: JobRecord, provider: object | None) -> Mapping[str, object]:
    """Select the job's observed-then-declared workflow declaration.

    The provider is only a compatibility fallback for old jobs that carried no
    workflow declaration; new jobs are interpreted from their immutable record.
    """

    entry = record.declarations.get("workflow")
    if isinstance(entry, Mapping):
        if "parameters" in entry or "output_types" in entry:
            return entry
        observed = entry.get("observed")
        declared = entry.get("declared")
        chosen = observed if observed is not None else declared
        if isinstance(chosen, Mapping):
            return chosen
    return _workflow_document(provider) if provider is not None else {}


def _output_roles(document: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    raw = document.get("output_types")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return {}
    return {item["name"]: item for item in raw if isinstance(item, Mapping) and isinstance(item.get("name"), str)}


def _entry_edge(identity: str, role: str, value: object, declared: Mapping[str, object]) -> httk.core.RunEdge:
    entry_id = getattr(value, "id", None)
    if not isinstance(entry_id, str):
        raise ValueError(f"{identity}: output role {role!r} has no string entry id")
    entry_type = getattr(value, "type", None)
    if not isinstance(entry_type, str):
        entry_type = declared.get("entry_type")
    if not isinstance(entry_type, str):
        raise ValueError(f"{identity}: output role {role!r} has no string entry type")
    return _core().RunEdge(role, entry_type, entry_id)


def _postprocessor(value: object) -> Callable[[JobRecord], Mapping[str, object]] | None:
    if callable(value):
        return cast(Callable[[JobRecord], Mapping[str, object]], value)
    if not isinstance(value, str) or ":" not in value:
        return None
    module_name, function_name = value.rsplit(":", 1)
    function = getattr(importlib.import_module(module_name), function_name, None)
    return cast(Callable[[JobRecord], Mapping[str, object]], function) if callable(function) else None


def _job_postprocessor(
    workspace: Workspace, record: JobRecord, workflow_id: str
) -> tuple[Callable[[JobRecord], Mapping[str, object]] | None, WorkflowProvider | None, str | None]:
    """Resolve a postprocessor from the job's pinned directory runner tree."""

    runner = record.job.get("runner")
    if not isinstance(runner, Mapping):
        return None, None, "job-pinned postprocessor requires a runner mapping"
    if runner.get("source") != "workspace":
        return None, None, "job-pinned postprocessor requires runner.source='workspace'"
    path_value = runner.get("path")
    if not isinstance(path_value, str):
        return None, None, "job-pinned postprocessor requires a workspace runner path"
    try:
        store_tree = workspace.runner_store_path(path_value)
    except Exception as exc:
        return None, None, f"job-pinned postprocessor runner path is invalid: {exc}"
    store_root = workspace.runners.resolve()
    try:
        resolved_tree = store_tree.resolve()
    except OSError as exc:
        return None, None, f"job-pinned postprocessor runner tree cannot be resolved: {exc}"
    if store_tree.is_symlink() or not resolved_tree.is_relative_to(store_root) or not store_tree.is_dir():
        return None, None, f"job-pinned postprocessor requires a directory runner tree: {store_tree}"
    pinned = runner.get("sha256")
    if not isinstance(pinned, str):
        return None, None, "job-pinned postprocessor requires runner.sha256"
    try:
        actual = tree_digest(store_tree)
    except Exception as exc:
        return None, None, f"pinned runner tree was modified: {store_tree} could not be verified: {exc}"
    if actual != pinned:
        return (
            None,
            None,
            f"pinned runner tree was modified: {store_tree} digest {actual} does not match pinned {pinned}",
        )
    manifest = store_tree / "workflow.toml"
    if not manifest.is_file():
        return None, None, f"job-pinned postprocessor manifest is missing: {manifest}"
    try:
        from .packages import _tree_hook, parse_workflow_manifest

        provider = parse_workflow_manifest(store_tree)
    except Exception as exc:
        return None, None, f"job-pinned postprocessor manifest is invalid: {exc}"
    if provider.workflow_id != workflow_id:
        return (
            None,
            None,
            f"job-pinned postprocessor manifest id {provider.workflow_id!r} does not match job workflow {workflow_id!r}",
        )
    if provider.postprocess_file is None:
        return None, None, "job-pinned workflow tree has no postprocess hook"
    return (
        cast(
            Callable[[JobRecord], Mapping[str, object]],
            _tree_hook(store_tree, pinned, provider.postprocess_file, "postprocess"),
        ),
        provider,
        None,
    )


def collect(
    workspace: Workspace,
    *,
    states: Iterable[str] = DEFAULT_COLLECT_STATES,
    placement: str | PurePosixPath | None = None,
    allow_job_postprocessor: bool = False,
) -> Iterator[CollectedJob]:
    """Collect records through registered or explicitly allowed job postprocessors.

    A fallback reads and verifies the package manifest from the pinned runner
    tree itself. A refusal, including a changed tree, degrades that job and does
    not stop the rest of the sweep; the changed-tree reason is deliberately loud.
    """

    from .provenance import run_record
    from .scaffold import workflow_provider

    core = _core()

    for record in job_records(workspace, states=states, placement=placement):
        identity = f"{record.workspace_id}:{record.job_id}"
        workflow_id = record.job.get("workflow")
        workflow_id = workflow_id if isinstance(workflow_id, str) else ""
        provider = workflow_provider(workflow_id)
        run = run_record(record)
        fallback = False
        try:
            adapter = _postprocessor(provider.postprocessor if provider is not None else None)
        except Exception as exc:
            raise ValueError(f"{identity}: postprocessor resolution failed: {exc}") from exc
        if adapter is None:
            if not allow_job_postprocessor:
                reason = (
                    f"no provider for workflow {workflow_id!r}; pass allow_job_postprocessor=True to use a pinned "
                    "workspace workflow tree"
                    if provider is None
                    else f"no postprocessor registered for workflow {workflow_id!r}; pass "
                    "allow_job_postprocessor=True to use a pinned workspace workflow tree"
                )
                yield CollectedJob(workflow_id, {}, (), run, (), record, reason)
                continue
            adapter, fallback_provider, fallback_reason = _job_postprocessor(workspace, record, workflow_id)
            if adapter is None or fallback_provider is None:
                yield CollectedJob(
                    workflow_id, {}, (), run, (), record, fallback_reason or "job postprocessor unavailable"
                )
                continue
            provider = fallback_provider
            fallback = True
        try:
            raw_outputs = adapter(record)
            if not isinstance(raw_outputs, Mapping):
                raise ValueError("postprocessor must return a mapping of output roles")
            roles = _output_roles(_job_workflow_document(record, provider))
            unknown = [role for role in raw_outputs if role not in roles]
            if unknown:
                raise ValueError(f"{identity}: unknown output role {unknown[0]!r}")
            outputs = dict(raw_outputs)
            unfulfilled = tuple(role for role in roles if role not in outputs)
            owned = tuple(_entry_edge(identity, role, value, roles[role]) for role, value in outputs.items())
            run = core.Run(
                workflow_declaration_uri=run.workflow_declaration_uri,
                inputs=run.inputs,
                artifacts=_overlay_edges(run.artifacts, owned),
                outputs=_overlay_edges(run.outputs, owned),
                immutable_id=run.immutable_id,
                last_modified=run.last_modified,
            )
            products: list[httk.core.ProductLink] = []
            input_edges = {edge.label: edge for edge in run.inputs}
            for role, declaration in roles.items():
                source_role = declaration.get("product_of")
                output_edge = next((edge for edge in owned if edge.label == role), None)
                source_edge = input_edges.get(source_role) if isinstance(source_role, str) else None
                if output_edge is not None and source_edge is not None:
                    products.append(
                        core.ProductLink(
                            source_type=source_edge.entry_type,
                            source_id=source_edge.entry_id,
                            target_type=output_edge.entry_type,
                            target_id=output_edge.entry_id,
                            label=role,
                            workflow_declaration_uri=run.workflow_declaration_uri,
                        )
                    )
            yield CollectedJob(workflow_id, outputs, unfulfilled, run, tuple(products), record)
        except Exception as exc:
            from .packages import _PinnedTreeError

            if fallback and isinstance(exc, _PinnedTreeError):
                yield CollectedJob(
                    workflow_id,
                    {},
                    (),
                    run,
                    (),
                    record,
                    f"pinned runner tree was modified: {exc}",
                )
                continue
            if str(exc).startswith(identity + ":"):
                raise
            raise ValueError(f"{identity}: postprocessor failed: {exc}") from exc
