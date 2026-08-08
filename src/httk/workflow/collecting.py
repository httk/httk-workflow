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
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from importlib import metadata
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote

from . import languages
from ._util import read_json, require_mapping, require_string, sha256_file, tree_digest
from .errors import FormatError
from .hookapi import COLLECT_STREAM_FORMAT, COLLECT_STREAM_VERSION
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
DEFAULT_COLLECT_TIMEOUT: float | None = 3600.0
#: Maximum UTF-8 response-line size retained from an executable collector.
MAX_COLLECT_RESPONSE_LINE_BYTES = 1024 * 1024
#: Maximum stderr line retained from an executable collector.
MAX_COLLECT_STDERR_LINE_BYTES = 64 * 1024
#: Maximum total stderr retained from an executable collector.
MAX_COLLECT_STDERR_BYTES = 1024 * 1024


class _CollectEnvironmentError(RuntimeError):
    """Report an operator-side collect dependency failure."""


if TYPE_CHECKING:
    import httk.core

    from .scaffold import WorkflowProvider


def _core():
    """Load optional core provenance types only when the collect layer is used."""

    return importlib.import_module("httk.core")


@dataclass(frozen=True)
class CollectedJob:
    """Represent one job after workflow collecting and provenance assembly.

    :param workflow_id: Identify the workflow that produced the job.
    :param outputs: Map declared output roles to collector results.
    :param unfulfilled: Name declared output roles omitted after a collector
        ran; leave empty for degraded jobs.
    :param run: Carry the framework-assembled run provenance.
    :param products: Carry the framework-assembled product links.
    :param record: Preserve the mechanical job readout behind the collection.
    :param missing_collector: Explain why collecting was unavailable,
        or leave it unset when collection completed.
    """

    workflow_id: str
    outputs: Mapping[str, object]
    unfulfilled: tuple[str, ...]
    run: httk.core.Run
    products: tuple[httk.core.ProductLink, ...]
    record: JobRecord
    missing_collector: str | None = None


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

    :param module: Name the module whose installed distribution to locate.
    :return: The distribution name and version, or ``None`` when ownership is
        unknown.
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

    :param job: Supply the validated job definition and runner identity.
    :return: Installation metadata for a reserved package runner, or ``None``.
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

    :param frames: Supply the journal frames read for one job.
    :return: The oldest-first activation and attempt timeline with its damage
        flag.
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

    :param frames: Supply the state and journal frames for one job.
    :return: Child records keyed by their spawn labels.
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
    result: dict[str, object] = {
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
        "parameters": dict(job.parameters),
        "parent": None if job.parent is None else dict(job.parent),
    }
    if job.environment:
        result["environment"] = dict(job.environment)
    return result


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

    :param workspace_root: Identify the absolute workspace root.
    :param workspace_id: Identify the workspace.
    :param job_id: Identify the job.
    :param job_key: Preserve the complete job key.
    :param job: Preserve the validated immutable job definition.
    :param runner_provenance: Preserve installed package provenance, when known.
    :param state: Record the terminal state in which the job stopped.
    :param failure: Record the terminal failure, when one exists.
    :param placement: Locate the job within the workspace hierarchy.
    :param payload_path: Locate the workspace-relative job payload.
    :param workdir_path: Locate the last workspace-relative workdir, when known.
    :param data_path: Locate transactional data, when the job has it.
    :param data_generation: Record the committed data generation, when present.
    :param provenance: Preserve the journal-derived timeline and damage flag.
    :param runner_steps: Preserve the runner steps, when recorded.
    :param children: Preserve labeled child references.
    :param declarations: Preserve declared and observed workflow documents.
    :param runner_description: Preserve the reserved runner description, when
        available.
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
        """Rebuild one record from a serialized record mapping.

        :param value: Supply the mapping produced by :meth:`as_mapping`.
        :return: The reconstructed job record.
        :raises httk.workflow.errors.FormatError: If the mapping has the wrong format or invalid
            record members.
        """

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

    :param job: Supply the validated job whose declared documents are available.
    :param payload: Locate the payload containing observed documents.
    :return: The side-by-side declarations and whether reading observed data
        found damage.
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

    :param workspace: Read the workspace containing the marked job.
    :param marker: Identify the stopped job to read.
    :return: The validated job record, or ``None`` when its payload is unreadable.
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
    """Validate the requested state kinds against what collect may read.

    :param states: Select the stopped state kinds to collect.
    :return: The distinct validated state kinds in request order.
    :raises ValueError: If no state or an uncollectable state is requested.
    """

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

    :param workspace: Read jobs from this workspace.
    :param states: Select the stopped state kinds to report.
    :param placement: Restrict results to this placement and its descendants.
    :yields: Mechanical job records, one for each readable selected job.
    :raises ValueError: If ``states`` contains no collectable state.
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
        if "inputs" in entry or "outputs" in entry:
            return entry
        observed = entry.get("observed")
        declared = entry.get("declared")
        chosen = observed if observed is not None else declared
        if isinstance(chosen, Mapping):
            return chosen
    return _workflow_document(provider) if provider is not None else {}


def _output_roles(document: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    raw = document.get("outputs")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return {}
    return {item["name"]: item for item in raw if isinstance(item, Mapping) and isinstance(item.get("name"), str)}


def _provider_output_roles(provider: object | None) -> dict[str, Mapping[str, object]]:
    raw = getattr(provider, "outputs", {}) if provider is not None else {}
    if not isinstance(raw, Mapping):
        return {}
    return {str(entry.get("role", name)): entry for name, entry in raw.items() if isinstance(entry, Mapping)}


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


def _collector(value: object) -> Callable[[JobRecord], Mapping[str, object]] | None:
    if callable(value):
        return cast(Callable[[JobRecord], Mapping[str, object]], value)
    if not isinstance(value, str) or ":" not in value:
        return None
    module_name, function_name = value.rsplit(":", 1)
    function = getattr(importlib.import_module(module_name), function_name, None)
    return cast(Callable[[JobRecord], Mapping[str, object]], function) if callable(function) else None


def _workspace_file_record(
    record: JobRecord,
    recorded: str,
    *,
    published_prefix: str | None = None,
    port: str | None = None,
    index: int = 0,
    name: str | None = None,
) -> object:
    """Resolve one workspace-confined path into a core ``FileRecord``."""

    from httk.core import FileRecord

    root = record.workspace_root.resolve()
    recorded_path = Path(recorded)
    actual: Path | None = None
    invalid_absolute = False
    try:
        if recorded_path.is_absolute():
            resolved = recorded_path.resolve()
            if not resolved.is_relative_to(root):
                invalid_absolute = True
            elif resolved.is_file():
                actual = resolved
        elif record.workdir is not None:
            workdir = record.workdir.resolve()
            candidate = (workdir / recorded_path).resolve()
            if candidate.is_relative_to(workdir) and candidate.is_relative_to(root) and candidate.is_file():
                actual = candidate
        if (
            actual is None
            and not invalid_absolute
            and published_prefix is not None
            and port is not None
            and record.data is not None
        ):
            data_root = record.data.resolve()
            published = (data_root / published_prefix / port).resolve()
            if published.is_relative_to(data_root) and published.is_relative_to(root) and published.is_dir():
                candidate = (published / f"{index:04d}-{recorded_path.name}").resolve()
                if candidate.is_relative_to(published) and candidate.is_relative_to(root) and candidate.is_file():
                    actual = candidate
    except (OSError, RuntimeError):
        actual = None
    if actual is None:
        raise ValueError(f"file path {recorded!r} is missing or outside the workspace/workdir/data roots")
    descriptor_path = actual.relative_to(root).as_posix()
    return FileRecord(
        url=descriptor_path,
        name=recorded_path.name if name is None else name,
        size=actual.stat().st_size,
        sha256=sha256_file(actual),
    )


def _executable_path(provider: object, root: Path) -> Path:
    member = getattr(provider, "collector_exec", None)
    if not isinstance(member, str):
        raise ValueError("executable collector has no member")
    relative = PurePosixPath(member)
    source = root.joinpath(*relative.parts)
    resolved = source.resolve()
    if source.is_symlink() or not source.is_file() or not resolved.is_relative_to(root.resolve()):
        raise ValueError(f"executable collector member is unavailable in trusted tree: {member}")
    if not os.access(source, os.X_OK):
        raise ValueError(f"executable collector {member!r} is not executable; chmod +x")
    return source


def _entry_record(value: Mapping[str, object]) -> object:
    from httk.core.register import entry_record_info, known_entry_records, resolve_entry_family, resolve_entry_record

    entry_type = value.get("type")
    if not isinstance(entry_type, str):
        raise ValueError("entry output must contain a string type")
    known: list[str] = []
    for name in known_entry_records():
        try:
            candidate = resolve_entry_record(name)
        except (ImportError, ModuleNotFoundError, TypeError, ValueError):
            continue
        _, family_name, definition_id = entry_record_info(name)
        instance_type = None
        if family_name is not None:
            try:
                instance_type = getattr(resolve_entry_family(family_name), "type", None)
            except (ImportError, ModuleNotFoundError, TypeError, ValueError):
                instance_type = None
        if not isinstance(instance_type, str) and isinstance(definition_id, str):
            instance_type = definition_id.rsplit("/", 1)[-1]
        if isinstance(instance_type, str):
            known.append(instance_type)
            if instance_type == entry_type:
                fields = dict(value)
                fields.pop("type", None)
                expected_id = fields.pop("id", None)
                result = cast(Any, candidate).create(fields)
                if expected_id is not None and expected_id != getattr(result, "id", None):
                    raise ValueError(f"entry output id {expected_id!r} does not match the constructed record")
                return result
    raise ValueError(f"unknown entry type {entry_type!r}; known types: {', '.join(sorted(set(known))) or '(none)'}")


def _resolve_executable_output(record: JobRecord, provider: object, role: str, value: object) -> object:
    if not isinstance(value, Mapping):
        raise ValueError("output must be exactly one of {'entry': {...}}, {'value': ...}, or {'file': '...'}")
    keys = set(value)
    if keys == {"entry"} and isinstance(value.get("entry"), Mapping):
        return _entry_record(cast(Mapping[str, object], value["entry"]))
    if keys == {"value"}:
        raw = value["value"]
        declared = _provider_output_roles(provider).get(role, {})
        ref = declared.get("ref")
        if isinstance(ref, str):
            try:
                from httk.data import validation
            except ImportError as exc:
                raise _CollectEnvironmentError(
                    "hard collect validation requires httk-data; install with `pip install httk-data`"
                ) from exc
            definition = _core().load_property_definition(ref)
            validation.validate_property(definition, raw)
            return _core().DataRecord.from_value(definition.definition_id, definition.name, raw)
        from .languages import _data_record

        return _data_record(role, raw)
    if keys == {"file"} and isinstance(value.get("file"), str):
        return _workspace_file_record(record, value["file"])
    raise ValueError("output must be exactly one of {'entry': {...}}, {'value': ...}, or {'file': '...'}")


def _run_executable_collector(
    records: Sequence[JobRecord], provider: object, root: Path
) -> tuple[dict[int, Mapping[str, object]], dict[int, str]]:
    """Run one executable collector and resolve its ordered responses."""

    def group_failures(reason: str) -> dict[int, str]:
        return {index: reason for index in range(len(records))}

    try:
        executable = _executable_path(provider, root)
    except (OSError, RuntimeError, ValueError) as exc:
        return {}, group_failures(f"executable collector cannot be resolved: {exc}")
    environment = dict(os.environ)
    for variable in tuple(environment):
        if variable.startswith("HTTK_WORKFLOW_"):
            environment.pop(variable)
    environment["HTTK_WORKFLOW_WORKSPACE_DIR"] = str(records[0].workspace_root)
    payload = "\n".join(
        [
            json.dumps(
                {"format": COLLECT_STREAM_FORMAT, "format_version": COLLECT_STREAM_VERSION}, separators=(",", ":")
            ),
            *(json.dumps({"record": record.as_mapping()}, separators=(",", ":")) for record in records),
            "",
        ]
    ).encode("utf-8")
    try:
        process = subprocess.Popen(
            [str(executable)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=root,
            env=environment,
            start_new_session=True,
        )
    except OSError as exc:
        return {}, group_failures(
            f"executable collector could not be launched: {exc}; check its shebang and executable permissions"
        )

    stdin = process.stdin
    stdout = process.stdout
    stderr = process.stderr
    assert stdin is not None and stdout is not None and stderr is not None
    response_lines: list[bytes] = []
    stderr_capture = bytearray()
    breach_reason: str | None = None
    breach_lock = threading.Lock()
    termination_lock = threading.Lock()
    terminated = False

    def terminate_group() -> None:
        nonlocal terminated
        with termination_lock:
            if terminated:
                return
            terminated = True
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            try:
                process.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def breach(reason: str) -> None:
        nonlocal breach_reason
        with breach_lock:
            if breach_reason is None:
                breach_reason = reason
        terminate_group()

    def drain_stdout() -> None:
        current = bytearray()
        while True:
            try:
                chunk = stdout.read(64 * 1024)
            except (OSError, ValueError):
                return
            if not chunk:
                break
            if breach_reason is not None:
                continue
            start = 0
            while start < len(chunk):
                newline = chunk.find(b"\n", start)
                end = len(chunk) if newline < 0 else newline + 1
                part = chunk[start : newline if newline >= 0 else end]
                if len(current) + len(part) > MAX_COLLECT_RESPONSE_LINE_BYTES:
                    breach(f"executable collector response line exceeded {MAX_COLLECT_RESPONSE_LINE_BYTES} bytes")
                    current.clear()
                    break
                current.extend(part)
                if newline >= 0:
                    line = bytes(current)
                    current.clear()
                    if len(response_lines) < len(records):
                        response_lines.append(line)
                    elif line.strip():
                        breach("executable collector emitted surplus response lines; write diagnostics to stderr")
                        break
                start = end
            if breach_reason is not None:
                continue
        if breach_reason is None and current:
            if len(response_lines) < len(records):
                response_lines.append(bytes(current))
            elif current.strip():
                breach("executable collector emitted surplus response lines; write diagnostics to stderr")

    def drain_stderr() -> None:
        current_length = 0
        total_length = 0
        while True:
            try:
                chunk = stderr.read(64 * 1024)
            except (OSError, ValueError):
                return
            if not chunk:
                break
            if len(stderr_capture) < MAX_COLLECT_STDERR_BYTES:
                stderr_capture.extend(chunk[: MAX_COLLECT_STDERR_BYTES - len(stderr_capture)])
            total_length += len(chunk)
            if total_length > MAX_COLLECT_STDERR_BYTES:
                breach(f"executable collector stderr exceeded {MAX_COLLECT_STDERR_BYTES} bytes")
                continue
            start = 0
            while start < len(chunk):
                newline = chunk.find(b"\n", start)
                end = len(chunk) if newline < 0 else newline + 1
                current_length += (newline if newline >= 0 else end) - start
                if current_length > MAX_COLLECT_STDERR_LINE_BYTES:
                    breach(f"executable collector stderr line exceeded {MAX_COLLECT_STDERR_LINE_BYTES} bytes")
                    break
                if newline >= 0:
                    current_length = 0
                start = end

    def feed_stdin() -> None:
        try:
            stdin.write(payload)
            stdin.close()
        except (BrokenPipeError, OSError, ValueError):
            try:
                stdin.close()
            except (OSError, ValueError):
                pass

    def close_pipes() -> None:
        for stream in (stdin, stdout, stderr):
            try:
                stream.close()
            except (OSError, ValueError):
                pass

    deadline = None if DEFAULT_COLLECT_TIMEOUT is None else time.monotonic() + DEFAULT_COLLECT_TIMEOUT

    def remaining() -> float | None:
        return None if deadline is None else max(0.0, deadline - time.monotonic())

    stdout_thread = threading.Thread(target=drain_stdout, daemon=True)
    stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
    stdin_thread = threading.Thread(target=feed_stdin, daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    stdin_thread.start()
    timed_out = False
    try:
        process.wait(timeout=remaining())
    except subprocess.TimeoutExpired:
        timed_out = True
        breach("executable collector timed out before responding")
        process.wait()
    if breach_reason is not None:
        close_pipes()
    for thread in (stdin_thread, stdout_thread, stderr_thread):
        timeout = remaining()
        thread.join(timeout=timeout)
        if thread.is_alive():
            timed_out = True
            breach("executable collector drain did not finish before the deadline")
            close_pipes()
            thread.join(timeout=0.1)
    close_pipes()
    if timed_out:
        return {}, group_failures("executable collector timed out before responding")
    if breach_reason is not None:
        _LOGGER.warning(
            "%s; stderr capture truncated to %d bytes: %s",
            breach_reason,
            MAX_COLLECT_STDERR_BYTES,
            bytes(stderr_capture).decode("utf-8", errors="replace"),
        )
        return {}, group_failures(breach_reason)

    responses = response_lines
    resolved: dict[int, Mapping[str, object]] = {}
    failures: dict[int, str] = {}
    for index, record in enumerate(records):
        identity = f"{record.workspace_id}:{record.job_id}"
        if index >= len(responses):
            failures[index] = f"{identity}: executable collector ended before responding"
            continue
        line = responses[index]
        if len(line) > MAX_COLLECT_RESPONSE_LINE_BYTES:
            failures[index] = (
                f"{identity}: executable collector response line exceeded {MAX_COLLECT_RESPONSE_LINE_BYTES} bytes"
            )
            continue
        try:
            response = json.loads(line.decode("utf-8"))
            if not isinstance(response, Mapping):
                raise ValueError("response is not a JSON object")
            if response.get("job_id") != record.job_id:
                raise ValueError(f"response job_id {response.get('job_id')!r} does not match {record.job_id!r}")
            if isinstance(response.get("error"), str):
                raise ValueError(response["error"])
            outputs = response.get("outputs")
            if not isinstance(outputs, Mapping):
                raise ValueError("response has no outputs mapping")
            resolved[index] = {
                str(role): _resolve_executable_output(record, provider, str(role), output)
                for role, output in outputs.items()
            }
        except UnicodeDecodeError as exc:
            failures[index] = f"{identity}: executable collector response is not UTF-8: {exc}"
        except ValueError as exc:
            failures[index] = f"{identity}: executable collector response failed: {exc}"
        except TypeError as exc:
            failures[index] = f"{identity}: executable collector response is malformed: {exc}"
    if len(responses) == len(records) and process.returncode:
        _LOGGER.warning(
            "executable collector %s exited with status %s after complete responses", executable, process.returncode
        )
    return resolved, failures


def _job_collector(
    workspace: Workspace, record: JobRecord, workflow_id: str
) -> tuple[Callable[[JobRecord], Mapping[str, object]] | None, WorkflowProvider | None, str | None]:
    """Resolve a collector from the job's pinned directory runner tree."""

    runner = record.job.get("runner")
    if not isinstance(runner, Mapping):
        return None, None, "job-pinned collector requires a runner mapping"
    if runner.get("source") != "workspace":
        return None, None, "job-pinned collector requires runner.source='workspace'"
    path_value = runner.get("path")
    if not isinstance(path_value, str):
        return None, None, "job-pinned collector requires a workspace runner path"
    try:
        store_tree = workspace.runner_store_path(path_value)
    except Exception as exc:
        return None, None, f"job-pinned collector runner path is invalid: {exc}"
    store_root = workspace.runners.resolve()
    try:
        resolved_tree = store_tree.resolve()
    except OSError as exc:
        return None, None, f"job-pinned collector runner tree cannot be resolved: {exc}"
    if store_tree.is_symlink() or not resolved_tree.is_relative_to(store_root) or not store_tree.is_dir():
        return None, None, f"job-pinned collector requires a directory runner tree: {store_tree}"
    pinned = runner.get("sha256")
    if not isinstance(pinned, str):
        return None, None, "job-pinned collector requires runner.sha256"
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
    manifest = store_tree / "httk_workflow.toml"
    if not manifest.is_file():
        return None, None, f"job-pinned collector manifest is missing: {manifest}"
    try:
        from .packages import _tree_hook, parse_workflow_manifest

        provider = parse_workflow_manifest(store_tree)
    except Exception as exc:
        return None, None, f"job-pinned collector manifest is invalid: {exc}"
    if provider.workflow_id != workflow_id:
        return (
            None,
            None,
            f"job-pinned collector manifest id {provider.workflow_id!r} does not match job workflow {workflow_id!r}",
        )
    if provider.collect_file is None:
        return None, None, "job-pinned workflow tree has no collect hook"
    if provider.collector_exec is not None:
        return None, provider, None
    return (
        cast(
            Callable[[JobRecord], Mapping[str, object]],
            _tree_hook(store_tree, pinned, provider.collect_file, "collect"),
        ),
        provider,
        None,
    )


def _assemble_collected(
    identity: str,
    record: JobRecord,
    provider: object | None,
    run: httk.core.Run,
    outputs: Mapping[str, object],
) -> CollectedJob:
    """Validate outputs and overlay their edges onto one collected run."""

    workflow_id = record.job.get("workflow")
    workflow_id = workflow_id if isinstance(workflow_id, str) else ""
    roles = _output_roles(_job_workflow_document(record, provider))
    unknown = [role for role in outputs if role not in roles]
    if unknown:
        raise ValueError(f"{identity}: unknown output role {unknown[0]!r}")
    unfulfilled = tuple(role for role in roles if role not in outputs)
    owned = tuple(_entry_edge(identity, role, value, roles[role]) for role, value in outputs.items())
    core = _core()
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
    owned_edges = {edge.label: edge for edge in owned}
    for role, curation in _provider_output_roles(provider).items():
        source_role = curation.get("product_of")
        output_edge = owned_edges.get(role)
        if isinstance(source_role, str):
            source_edge = input_edges.get(source_role) or owned_edges.get(source_role)
        else:
            source_edge = None
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
    return CollectedJob(workflow_id, dict(outputs), unfulfilled, run, tuple(products), record)


def collect(
    workspace: Workspace,
    *,
    states: Iterable[str] = DEFAULT_COLLECT_STATES,
    placement: str | PurePosixPath | None = None,
    allow_job_collector: bool = False,
) -> Iterator[CollectedJob]:
    """Collect records through registered or explicitly allowed job collectors.

    A fallback reads and verifies the package manifest from the pinned runner
    tree itself. A changed pinned tree raises ``_PinnedTreeError``, which degrades
    that job and does not stop the rest of the sweep; other hook-loading errors
    propagate and stop iteration.

    :param workspace: Read jobs from this workspace.
    :param states: Select the stopped state kinds to report.
    :param placement: Restrict results to this placement and its descendants.
    :param allow_job_collector: Permit digest-verified collectors from
        job-pinned workspace package trees.
    :yields: Framework-assembled collected jobs, including degraded jobs.
    :raises ValueError: If a registered collector fails to resolve or
        returns invalid output roles.
    """

    from .provenance import run_record
    from .scaffold import workflow_provider

    records = list(job_records(workspace, states=states, placement=placement))
    results: list[CollectedJob | None] = [None] * len(records)
    executable_groups: dict[str, list[tuple[int, JobRecord, WorkflowProvider, httk.core.Run]]] = {}

    for index, record in enumerate(records):
        identity = f"{record.workspace_id}:{record.job_id}"
        workflow_id = record.job.get("workflow")
        workflow_id = workflow_id if isinstance(workflow_id, str) else ""
        provider = workflow_provider(workflow_id)
        run = run_record(record)
        fallback = False
        if provider is not None and provider.collector_exec is not None:
            if provider.directory is None:
                raise ValueError(f"{identity}: executable collector has no trusted package directory")
            key = f"{provider.directory.resolve()}:{provider.collector_exec}"
            executable_groups.setdefault(key, []).append((index, record, provider, run))
            continue
        try:
            adapter = _collector(provider.collector if provider is not None else None)
        except Exception as exc:
            raise ValueError(f"{identity}: collector resolution failed: {exc}") from exc
        language_fallback = False
        if adapter is None:
            parameters = record.job.get("parameters")
            language_realization = False
            language_name: object = None
            if isinstance(parameters, Mapping):
                language_realization = parameters.get("workflow_realization") == "language"
                language_name = parameters.get("workflow_language")
            package_collect = (
                language_realization
                and isinstance(parameters, Mapping)
                and parameters.get("workflow_collect") == "package"
            )
            if provider is None and package_collect:
                results[index] = CollectedJob(
                    workflow_id,
                    {},
                    (),
                    run,
                    (),
                    record,
                    f"{identity}: workflow package collect hook is unavailable without its registered provider; "
                    "collect while the package is registered",
                )
                continue
            if provider is None and language_realization and isinstance(language_name, str):
                try:
                    lang = languages.language(language_name)
                    if not lang.has_default_collector:
                        results[index] = CollectedJob(
                            workflow_id,
                            {},
                            (),
                            run,
                            (),
                            record,
                            f"{identity}: workflow language {language_name!r} has no default collector; "
                            "its package declares [workflow.collect]",
                        )
                        continue
                    adapter = lang.collect
                except Exception as exc:
                    results[index] = CollectedJob(
                        workflow_id,
                        {},
                        (),
                        run,
                        (),
                        record,
                        f"{identity}: workflow language {language_name!r} collector unavailable: {exc}",
                    )
                    continue
                language_fallback = True
        if adapter is None:
            if not allow_job_collector:
                reason = (
                    f"no provider for workflow {workflow_id!r}; pass allow_job_collector=True to use a pinned "
                    "workspace workflow tree"
                    if provider is None
                    else f"no collector registered for workflow {workflow_id!r}; pass "
                    "allow_job_collector=True to use a pinned workspace workflow tree"
                )
                results[index] = CollectedJob(workflow_id, {}, (), run, (), record, reason)
                continue
            adapter, fallback_provider, fallback_reason = _job_collector(workspace, record, workflow_id)
            if fallback_provider is not None and fallback_provider.collector_exec is not None:
                if fallback_provider.directory is None:
                    raise ValueError(f"{identity}: executable collector has no trusted package directory")
                key = f"{fallback_provider.directory.resolve()}:{fallback_provider.collector_exec}"
                executable_groups.setdefault(key, []).append((index, record, fallback_provider, run))
                continue
            if adapter is None or fallback_provider is None:
                results[index] = CollectedJob(
                    workflow_id, {}, (), run, (), record, fallback_reason or "job collector unavailable"
                )
                continue
            provider = fallback_provider
            fallback = True
        try:
            raw_outputs = adapter(record)
            if not isinstance(raw_outputs, Mapping):
                raise ValueError("collector must return a mapping of output roles")
            results[index] = _assemble_collected(identity, record, provider, run, raw_outputs)
        except Exception as exc:
            from .packages import _PinnedTreeError

            if fallback and isinstance(exc, _PinnedTreeError):
                results[index] = CollectedJob(
                    workflow_id,
                    {},
                    (),
                    run,
                    (),
                    record,
                    f"pinned runner tree was modified: {exc}",
                )
                continue
            if language_fallback and isinstance(exc, languages.LanguageOutputsMissingError):
                results[index] = CollectedJob(workflow_id, {}, (), run, (), record, str(exc))
                continue
            if str(exc).startswith(identity + ":"):
                raise
            raise ValueError(f"{identity}: collector failed: {exc}") from exc

    for entries in executable_groups.values():
        group_records = [entry[1] for entry in entries]
        provider = entries[0][2]
        assert provider.directory is not None
        resolved, failures = _run_executable_collector(group_records, provider, provider.directory.resolve())
        for local_index, (index, record, group_provider, run) in enumerate(entries):
            identity = f"{record.workspace_id}:{record.job_id}"
            failure_reason = failures.get(local_index)
            if failure_reason is not None:
                results[index] = CollectedJob(group_provider.workflow_id, {}, (), run, (), record, failure_reason)
                continue
            try:
                results[index] = _assemble_collected(identity, record, group_provider, run, resolved[local_index])
            except Exception as exc:
                results[index] = CollectedJob(
                    group_provider.workflow_id,
                    {},
                    (),
                    run,
                    (),
                    record,
                    f"{identity}: executable collector output failed: {exc}",
                )

    for result in results:
        assert result is not None
        yield result
