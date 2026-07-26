"""Job introspection and the foreground debug runner.

Everything here answers one operator question about one job — what it is, what
happened to it, and why it is not running — from exactly the authoritative state
a manager reads: the marker below ``state/``, the journal frame that marker
names, the immutable ``job.json``, and the manifests every manager publishes
below ``managers/``.

The claim preconditions explained here are a deliberate read-only restatement of
what :class:`~httk.workflow.manager.TaskManager` decides. The *job* side of each
precondition is pulled from the :class:`~httk.workflow.models.JobDefinition`
itself, so it cannot drift; the *manager* side is deployment policy of whichever
manager happens to be running and is read from that manager's own manifest. A
manager that is not running, or that publishes no manifest, therefore cannot be
reasoned about, and that is reported instead of guessed.

The foreground debug runner is the one part of this module that makes a job move.
It writes no protocol state of its own: it drives a private
:class:`~httk.workflow.manager.TaskManager` whose scans are restricted to the
debugged job, so every transition is produced by exactly the code paths a
production manager uses.
"""

import json
import shutil
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from ._util import read_json, timestamp_seconds
from .errors import WorkflowError, WorkspaceCorruptionError
from .journal import read_record
from .manager import TaskManager
from .manifests import read_maintenance_lock
from .models import (
    CORE_PROFILE,
    CORE_STATE_KINDS,
    DEFAULT_LEASE_SECONDS,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TERMINAL_KINDS,
    JobDefinition,
    Marker,
    normalize_placement,
    validate_step,
)
from .workspace import MarkerFault, WorkflowWorkspace

JOB_REPORT_FORMAT = "httk-workflow-job-report"
JOB_HISTORY_FORMAT = "httk-workflow-job-history"
JOB_DIAGNOSIS_FORMAT = "httk-workflow-job-diagnosis"
JOB_LIST_FORMAT = "httk-workflow-job-list"
# Exit statuses of the foreground debug runner.
DEBUG_EXIT_SUCCEEDED = 0
DEBUG_EXIT_FAILED = 3
DEBUG_EXIT_UNFINISHED = 4
# How long a history walk waits for one frame to become visible. The budget of
# a state read is the workspace's full visibility deadline because a state read
# must succeed; a history walk is diagnostic, so it reports damage quickly.
_HISTORY_READ_DEADLINE_SECONDS = 0.1


def resolve_job(workspace: WorkflowWorkspace, selector: str) -> Marker:
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


def _state_of(workspace: WorkflowWorkspace, marker: Marker) -> tuple[dict[str, Any], str | None]:
    """Return one job's state frame, reporting rather than raising on damage."""

    try:
        return workspace.read_state(marker), None
    except (WorkflowError, OSError) as exc:
        return {}, str(exc)


def _job_of(workspace: WorkflowWorkspace, marker: Marker) -> tuple[JobDefinition | None, str | None]:
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


# ---------------------------------------------------------------------------
# Registered managers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ManagerRecord:
    """One manager's published manifest and the liveness of its heartbeat."""

    manager_id: str
    hostname: str | None
    pid: int | None
    pools: frozenset[str]
    capabilities: frozenset[str]
    runner_backends: frozenset[str]
    accept_any_pool: bool
    started_at: str | None
    heartbeat_at: str | None
    heartbeat_age_seconds: float | None

    def alive(self, *, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> bool:
        """Whether this manager's heartbeat is still inside *lease_seconds*.

        The default is the protocol default rather than any one workspace's
        policy, because a record can be rendered without its workspace at hand;
        a caller that has the workspace passes its ``policy.lease_seconds``.
        """

        age = self.heartbeat_age_seconds
        return age is not None and age <= lease_seconds

    def describe(self) -> str:
        """Describe this manager for an operator diagnostic."""

        where = self.hostname or "an unrecorded host"
        pools = "any pool" if self.accept_any_pool else ",".join(sorted(self.pools)) or "no pool"
        capabilities = ",".join(sorted(self.capabilities)) or "-"
        backends = ",".join(sorted(self.runner_backends)) or "-"
        age = (
            "no heartbeat" if self.heartbeat_age_seconds is None else f"heartbeat {self.heartbeat_age_seconds:.0f}s ago"
        )
        return (
            f"{self.manager_id} on {where} (pools {pools}, capabilities {capabilities}, " f"backends {backends}, {age})"
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this manager record."""

        return {
            "manager_id": self.manager_id,
            "hostname": self.hostname,
            "pid": self.pid,
            "pools": sorted(self.pools),
            "capabilities": sorted(self.capabilities),
            "runner_backends": sorted(self.runner_backends),
            "accept_any_pool": self.accept_any_pool,
            "started_at": self.started_at,
            "heartbeat_at": self.heartbeat_at,
            "heartbeat_age_seconds": self.heartbeat_age_seconds,
            "alive": self.alive(),
        }


def _label_set(value: object) -> frozenset[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return frozenset()
    return frozenset(item for item in value if isinstance(item, str))


def read_managers(workspace: WorkflowWorkspace) -> list[ManagerRecord]:
    """Return every manager that has ever registered in this workspace.

    The records are exactly what each manager published for itself. A manager
    whose heartbeat has expired is still listed, because an expired heartbeat is
    the evidence an operator needs when a job is stuck in a claimed state.
    """

    directory = workspace.control / "managers"
    records: list[ManagerRecord] = []
    if not directory.is_dir():
        return records
    for entry in sorted(directory.iterdir()):
        if not entry.is_dir():
            continue
        try:
            manifest = read_json(entry / "manager.json")
        except WorkflowError:
            continue
        heartbeat_at: str | None = None
        age: float | None = None
        try:
            heartbeat_at = _optional_string(read_json(entry / "heartbeat.json").get("updated_at"))
        except WorkflowError:
            heartbeat_at = None
        if heartbeat_at is not None:
            try:
                age = max(0.0, time.time() - timestamp_seconds(heartbeat_at))
            except ValueError:
                age = None
        records.append(
            ManagerRecord(
                manager_id=str(manifest.get("manager_id", entry.name)),
                hostname=_optional_string(manifest.get("hostname")),
                pid=_optional_int(manifest.get("pid")),
                pools=_label_set(manifest.get("pools")),
                capabilities=_label_set(manifest.get("capabilities")),
                runner_backends=_label_set(manifest.get("runner_backends")),
                accept_any_pool=bool(manifest.get("accept_any_pool", False)),
                started_at=_optional_string(manifest.get("started_at")),
                heartbeat_at=heartbeat_at,
                heartbeat_age_seconds=age,
            )
        )
    return records


# ---------------------------------------------------------------------------
# Claim preconditions and budgets
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClaimRequirements:
    """What one job demands of any manager that claims it.

    Every member comes from the immutable ``job.json``, which is the only side of
    a claim decision the job itself owns. Whether a given manager offers them is
    that manager's own deployment policy, read from its manifest.
    """

    backend: str
    pool: str
    capabilities: frozenset[str]

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of these requirements."""

        return {
            "runner_backend": self.backend,
            "claim_pool": self.pool,
            "required_capabilities": sorted(self.capabilities),
        }


def claim_requirements(job: JobDefinition) -> ClaimRequirements:
    """Return the claim preconditions *job* imposes on a manager."""

    return ClaimRequirements(
        backend=job.runner_backend,
        pool=job.claim_pool,
        capabilities=job.required_capabilities,
    )


def manager_refusals(record: ManagerRecord, requirements: ClaimRequirements) -> list[str]:
    """Return why *record* would not claim a job with *requirements*.

    An empty list means this manager offers everything the job demands, which is
    exactly the test :class:`~httk.workflow.manager.TaskManager` applies in its
    own eligibility filter.
    """

    reasons: list[str] = []
    if requirements.backend not in record.runner_backends:
        reasons.append(f"does not serve runner backend {requirements.backend}")
    if not record.accept_any_pool and requirements.pool not in record.pools:
        reasons.append(f"does not serve claim pool {requirements.pool}")
    missing = requirements.capabilities - record.capabilities
    if missing:
        reasons.append(f"lacks capabilities {','.join(sorted(missing))}")
    return reasons


@dataclass(frozen=True)
class BudgetStatus:
    """The attempt and activation budgets of one job against what it consumed."""

    attempts_this_activation: int
    maximum_attempts_per_activation: int | None
    total_attempts: int
    maximum_total_attempts: int | None
    activations: int
    maximum_activations: int | None

    @property
    def attempt_budget_exhausted(self) -> bool:
        """Whether one more attempt of this activation is already over budget."""

        per_activation = self.maximum_attempts_per_activation
        if per_activation is not None and self.attempts_this_activation + 1 > per_activation:
            return True
        total = self.maximum_total_attempts
        return total is not None and self.total_attempts + 1 > total

    @property
    def activation_budget_exhausted(self) -> bool:
        """Whether one more activation of this job is already over budget."""

        maximum = self.maximum_activations
        return maximum is not None and self.activations + 1 > maximum

    def describe(self) -> list[str]:
        """Describe every budget as ``consumed/limit`` for an operator."""

        def limit(value: int | None) -> str:
            return "unlimited" if value is None else str(value)

        return [
            f"attempts this activation {self.attempts_this_activation}/{limit(self.maximum_attempts_per_activation)}",
            f"total attempts {self.total_attempts}/{limit(self.maximum_total_attempts)}",
            f"activations {self.activations}/{limit(self.maximum_activations)}",
        ]

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of these budgets."""

        return {
            "attempts_this_activation": self.attempts_this_activation,
            "maximum_attempts_per_activation": self.maximum_attempts_per_activation,
            "total_attempts": self.total_attempts,
            "maximum_total_attempts": self.maximum_total_attempts,
            "activations": self.activations,
            "maximum_activations": self.maximum_activations,
            "attempt_budget_exhausted": self.attempt_budget_exhausted,
            "activation_budget_exhausted": self.activation_budget_exhausted,
        }


def budget_status(job: JobDefinition, state: Mapping[str, Any]) -> BudgetStatus:
    """Return what *job* consumed of its budgets according to *state*."""

    policy = job.retry_policy
    return BudgetStatus(
        attempts_this_activation=_optional_int(state.get("attempt_ordinal")) or 0,
        maximum_attempts_per_activation=policy.maximum_attempts_per_activation,
        total_attempts=_optional_int(state.get("total_attempts")) or 0,
        maximum_total_attempts=policy.maximum_total_attempts,
        activations=_optional_int(state.get("activation_ordinal")) or 0,
        maximum_activations=policy.maximum_activations,
    )


# ---------------------------------------------------------------------------
# Join observation
# ---------------------------------------------------------------------------


def observe_join(workspace: WorkflowWorkspace, join: Mapping[str, Any]) -> list[dict[str, object]]:
    """Observe every child one waiting job's join names.

    This is a read-only observation for an operator: an unresolvable child is
    reported with a null state kind instead of ending the parent, and a
    malformed child reference is reported as such. The manager owns the
    authoritative evaluation of the same join.
    """

    children = join.get("children")
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
        return []
    observations: list[dict[str, object]] = []
    for raw in children:
        if not isinstance(raw, Mapping):
            observations.append(
                {"label": None, "job_id": None, "kind": None, "error": "child reference is not an object"}
            )
            continue
        job_id = _optional_string(raw.get("job_id"))
        job_key = _optional_string(raw.get("job_key"))
        label = _optional_string(raw.get("label"))
        marker: Marker | None = None
        error: str | None = None
        placement_hint = _optional_string(raw.get("placement_hint"))
        try:
            if placement_hint is not None and job_key is not None:
                marker = workspace.find_marker_at(job_key, normalize_placement(placement_hint))
            if marker is None and job_id is not None:
                marker = workspace.find_marker_by_id(job_id)
        except (WorkflowError, OSError) as exc:
            error = str(exc)
        observations.append(
            {
                "label": label,
                "job_id": job_id,
                "job_key": job_key if marker is None else marker.job_key,
                "placement": None if marker is None else marker.placement.as_posix(),
                "kind": None if marker is None else marker.kind,
                "terminal": None if marker is None else marker.kind in TERMINAL_KINDS,
                "error": error,
            }
        )
    return observations


def _describe_child(observation: Mapping[str, object]) -> str:
    label = observation.get("label") or "-"
    identity = observation.get("job_key") or observation.get("job_id") or "-"
    kind = observation.get("kind")
    state = "not resolvable in this workspace" if kind is None else str(kind)
    error = observation.get("error")
    suffix = f" ({error})" if isinstance(error, str) and error else ""
    return f"{label}: {identity} is {state}{suffix}"


# ---------------------------------------------------------------------------
# job show
# ---------------------------------------------------------------------------


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


def _attempt_control(workspace: WorkflowWorkspace, marker: Marker, state: Mapping[str, Any]) -> Path | None:
    """Return the attempt control directory of one job's last attempt."""

    name = _optional_string(state.get("attempt_control"))
    if name is None:
        attempt_id = _optional_string(state.get("attempt_id"))
        name = None if attempt_id is None else f".httk-attempt.{attempt_id}"
    if name is None:
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


def describe_job(workspace: WorkflowWorkspace, marker: Marker) -> dict[str, Any]:
    """Return the complete machine-readable report of one job."""

    state, state_error = _state_of(workspace, marker)
    job, job_error = _job_of(workspace, marker)
    payload = workspace.payload_path(marker.placement, marker.job_key)
    workdir = _workdir_relative(job, state)
    control = _attempt_control(workspace, marker, state)
    report: dict[str, Any] = {
        "format": JOB_REPORT_FORMAT,
        "format_version": 1,
        "workspace": str(workspace.root),
        "workspace_id": workspace.workspace_id,
        "core_profile": workspace.core_profile,
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "state": marker.kind,
        "placement": marker.placement.as_posix(),
        "priority": marker.priority,
        "generation": marker.generation,
        "record_ref": marker.record_ref,
        "marker": str(marker.path),
        "reason": state.get("reason"),
        "created_at": state.get("created_at"),
        "step": state.get("step"),
        "runner_steps": state.get("runner_steps"),
        "activation": {
            "id": state.get("activation_id"),
            "ordinal": state.get("activation_ordinal"),
        },
        "attempt": {
            "id": state.get("attempt_id"),
            "ordinal": state.get("attempt_ordinal"),
            "total": state.get("total_attempts"),
            "control": None if control is None else str(control),
        },
        "failure": state.get("failure"),
        "pause": state.get("pause"),
        "data_generation": state.get("data_generation"),
        "paths": {
            "payload": str(payload),
            "workdir": None if workdir is None else str(payload.joinpath(*workdir.parts)),
            "data": str(payload / "data") if job is not None and job.data_mode == "transactional" else None,
        },
        "state_error": state_error,
        "job_error": job_error,
    }
    if job is not None:
        report.update(
            {
                "name": job.name,
                "workflow": job.workflow,
                "tag": job.tag,
                "job_digest": job.digest,
                "initial_step": job.initial_step,
                "runner": {
                    "backend": job.runner_backend,
                    "source": job.runner_source,
                    "path": job.runner_path.as_posix(),
                    "sha256": job.runner_sha256,
                    "arguments": list(job.runner_arguments),
                },
                "claim": claim_requirements(job).as_mapping(),
                "budgets": budget_status(job, state).as_mapping(),
                "retry_on": sorted(job.retry_policy.retry_on),
                "workdir_mode": job.workdir_mode,
                "data_mode": job.data_mode,
            }
        )
    # Only the frame written at registration records the digest the manager
    # validated, so a later frame simply reports none rather than guessing.
    report["registered_job_digest"] = _optional_string(state.get("job_digest"))
    if marker.kind in {"claimed", "running", "committing"}:
        report["owner"] = {
            "manager_id": state.get("manager_id"),
            "writer_id": state.get("writer_id"),
            "lease_seconds": state.get("lease_seconds"),
            "started_at": state.get("started_at"),
        }
    if marker.kind == "waiting":
        join = state.get("join")
        report["join"] = {
            "condition": join.get("condition") if isinstance(join, Mapping) else None,
            "count": join.get("count") if isinstance(join, Mapping) else None,
            "next_step": state.get("next_step"),
            "children": observe_join(workspace, join) if isinstance(join, Mapping) else [],
        }
    if marker.kind in {"failed", "cancelled", "paused"}:
        report["error_breadcrumb"] = read_error_breadcrumb(control)
    return report


def _pair(name: str, value: object) -> str:
    return f"{name:<22s} {'-' if value is None or value == '' else value}"


def render_job(report: Mapping[str, Any]) -> str:
    """Render the report of :func:`describe_job` for a terminal."""

    lines = [
        f"job {report['job_key']} ({report['state']})",
        _pair("job id", report["job_id"]),
        _pair("name", report.get("name")),
        _pair("workflow", report.get("workflow")),
        _pair("placement", report["placement"]),
        _pair("priority", report["priority"]),
        _pair("generation", report["generation"]),
        _pair("reason", report.get("reason")),
        _pair("updated at", report.get("created_at")),
        _pair("job digest", report.get("job_digest")),
    ]
    runner = report.get("runner")
    if isinstance(runner, Mapping):
        lines.append(_pair("runner", f"{runner['source']}:{runner['path']} (backend {runner['backend']})"))
        lines.append(_pair("runner sha256", runner.get("sha256") or "pinned by the job digest"))
        arguments = runner.get("arguments")
        if isinstance(arguments, Sequence) and arguments:
            lines.append(_pair("runner arguments", " ".join(str(item) for item in arguments)))
    claim = report.get("claim")
    if isinstance(claim, Mapping):
        capabilities = claim.get("required_capabilities")
        joined = ",".join(str(item) for item in capabilities) if isinstance(capabilities, Sequence) else ""
        lines.append(_pair("claim pool", claim.get("claim_pool")))
        lines.append(_pair("capabilities", joined or "-"))
    lines.append(_pair("step", f"{report.get('step') or '-'} (initial {report.get('initial_step') or '-'})"))
    steps = report.get("runner_steps")
    if isinstance(steps, Sequence) and not isinstance(steps, (str, bytes)):
        lines.append(_pair("runner steps", ",".join(str(item) for item in steps)))
    activation = report.get("activation")
    attempt = report.get("attempt")
    if isinstance(activation, Mapping):
        lines.append(_pair("activation", f"{activation.get('ordinal') or '-'} ({activation.get('id') or '-'})"))
    if isinstance(attempt, Mapping):
        lines.append(
            _pair(
                "attempt",
                f"{attempt.get('ordinal') or '-'} of activation, {attempt.get('total') or '-'} total "
                f"({attempt.get('id') or '-'})",
            )
        )
    budgets = report.get("budgets")
    if isinstance(budgets, Mapping):
        status = BudgetStatus(
            attempts_this_activation=int(budgets["attempts_this_activation"]),
            maximum_attempts_per_activation=_optional_int(budgets["maximum_attempts_per_activation"]),
            total_attempts=int(budgets["total_attempts"]),
            maximum_total_attempts=_optional_int(budgets["maximum_total_attempts"]),
            activations=int(budgets["activations"]),
            maximum_activations=_optional_int(budgets["maximum_activations"]),
        )
        for index, text in enumerate(status.describe()):
            lines.append(_pair("budgets" if index == 0 else "", text))
    owner = report.get("owner")
    if isinstance(owner, Mapping):
        lines.append(_pair("owning manager", owner.get("manager_id")))
        lines.append(_pair("lease seconds", owner.get("lease_seconds")))
        lines.append(_pair("started at", owner.get("started_at")))
    paths = report.get("paths")
    if isinstance(paths, Mapping):
        lines.append(_pair("payload", paths.get("payload")))
        lines.append(_pair("workdir", paths.get("workdir")))
        if paths.get("data"):
            lines.append(_pair("data", paths.get("data")))
    if isinstance(attempt, Mapping) and attempt.get("control"):
        lines.append(_pair("attempt control", attempt.get("control")))
    failure = report.get("failure")
    if isinstance(failure, Mapping):
        lines.append(_pair("failure", f"{failure.get('code')}: {failure.get('message')}"))
        details = failure.get("details")
        if isinstance(details, Mapping) and details:
            lines.append(_pair("failure details", json.dumps(details, sort_keys=True)))
        if failure.get("retryable"):
            lines.append(_pair("", "the runner declared this failure retryable"))
    pause = report.get("pause")
    if pause:
        lines.append(_pair("pause", json.dumps(pause, sort_keys=True)))
    join = report.get("join")
    if isinstance(join, Mapping):
        lines.append(_pair("join", f"{join.get('condition') or '-'} then step {join.get('next_step') or '-'}"))
        children = join.get("children")
        if isinstance(children, Sequence):
            for child in children:
                if isinstance(child, Mapping):
                    lines.append(_pair("", _describe_child(child)))
    breadcrumb = report.get("error_breadcrumb")
    if isinstance(breadcrumb, Mapping):
        lines.append(
            _pair(
                "error breadcrumb",
                f"{breadcrumb.get('exception')}: {breadcrumb.get('message')} in step {breadcrumb.get('step')}",
            )
        )
    for name in ("state_error", "job_error"):
        if report.get(name):
            lines.append(_pair(name.replace("_", " "), report[name]))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# job log
# ---------------------------------------------------------------------------


def job_frames(workspace: WorkflowWorkspace, marker: Marker, *, limit: int | None = None) -> list[dict[str, Any]]:
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
            frames.append({"record_ref": record_ref, "error": "the journal chain of this job is cyclic"})
            break
        seen.add(record_ref)
        try:
            frame = read_record(workspace.control, record_ref, deadline_seconds=_HISTORY_READ_DEADLINE_SECONDS)
        except (WorkflowError, ValueError) as exc:
            frames.append({"record_ref": record_ref, "error": str(exc)})
            break
        frames.append({**frame, "record_ref": record_ref})
        if limit is not None and len(frames) >= limit:
            break
        record_ref = _optional_string(frame.get("previous_record_ref"))
    frames.reverse()
    return frames


def render_frames(frames: Sequence[Mapping[str, Any]]) -> str:
    """Render the frames of :func:`job_frames` one line each, oldest first."""

    lines: list[str] = []
    previous_kind = "submitted"
    for frame in frames:
        error = frame.get("error")
        if error is not None:
            lines.append(f"{'?':32s} {'-':<6s} {frame.get('record_ref')}: {error}")
            continue
        kind = str(frame.get("kind", "?"))
        created = str(frame.get("created_at") or "-")
        generation = frame.get("state_generation")
        transition = f"{previous_kind}->{kind}"
        previous_kind = kind
        parts = [f"{created:32s}", f"g{generation:<5}", f"{transition:<24s}"]
        parts.append(f"step={frame.get('step') or '-'}")
        parts.append(f"attempt={frame.get('attempt_ordinal') if frame.get('attempt_ordinal') is not None else '-'}")
        parts.append(f"reason={frame.get('reason') or '-'}")
        if isinstance(frame.get("outcome_action"), str):
            parts.append(f"action={frame['outcome_action']}")
        failure = frame.get("failure")
        if isinstance(failure, Mapping):
            parts.append(f"failure={failure.get('code')}")
        lines.append(" ".join(parts))
    if not lines:
        lines.append("this job has no recorded transition: it is still exactly as submitted")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# job list
# ---------------------------------------------------------------------------


def list_jobs(
    workspace: WorkflowWorkspace,
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


def render_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    """Render the rows of :func:`list_jobs` as a plain table."""

    if not rows:
        return "no job matches this selection"
    width = max(len(str(row["job_key"])) for row in rows)
    lines = [f"{'JOB':{width}s} {'STATE':<11s} {'STEP':<16s} {'PRI':>3s} PLACEMENT"]
    for row in rows:
        lines.append(
            f"{str(row['job_key']):{width}s} {str(row['state']):<11s} "
            f"{str(row['step'] or '-'):<16s} {int(row['priority']):>3d} {row['placement']}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# job why
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Check:
    """One precondition of progress and whether this job satisfies it."""

    name: str
    satisfied: bool | None
    detail: str

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this check."""

        return {"name": self.name, "satisfied": self.satisfied, "detail": self.detail}


@dataclass(frozen=True)
class Diagnosis:
    """Why one job is or is not making progress, and what to do about it."""

    job_id: str
    job_key: str
    state: str
    summary: str
    blocked: bool
    checks: tuple[Check, ...] = ()
    hints: tuple[str, ...] = ()

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this diagnosis."""

        return {
            "format": JOB_DIAGNOSIS_FORMAT,
            "format_version": 1,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "state": self.state,
            "summary": self.summary,
            "blocked": self.blocked,
            "checks": [check.as_mapping() for check in self.checks],
            "hints": list(self.hints),
        }

    def render(self) -> str:
        """Render this diagnosis for a terminal."""

        marks = {True: "ok  ", False: "no  ", None: "?   "}
        lines = [f"job {self.job_key} is {self.state}", self.summary]
        for check in self.checks:
            lines.append(f"  {marks[check.satisfied]}{check.name}: {check.detail}")
        for hint in self.hints:
            lines.append(f"  -> {hint}")
        return "\n".join(lines)


@dataclass
class _Diagnosing:
    """Mutable accumulator for the checks and hints of one diagnosis."""

    checks: list[Check] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)

    def check(self, name: str, satisfied: bool | None, detail: str) -> None:
        self.checks.append(Check(name=name, satisfied=satisfied, detail=detail))

    def hint(self, text: str) -> None:
        self.hints.append(text)


def _maintenance_check(workspace: WorkflowWorkspace, report: _Diagnosing) -> bool:
    """Record whether a live maintenance lock forbids launching work."""

    lock = read_maintenance_lock(workspace)
    if lock is None:
        report.check("maintenance lock", True, "no maintenance lock exists")
        return False
    if lock.is_stale():
        report.check(
            "maintenance lock",
            True,
            f"a stale lock held by {lock.describe()} is ignored by managers",
        )
        report.hint("clear the stale lock with 'httk workflow workspace unlock WORKSPACE'")
        return False
    report.check(
        "maintenance lock",
        False,
        f"launching is paused by the maintenance lock held by {lock.describe()}",
    )
    report.hint("wait for the maintenance operation, or 'httk workflow workspace unlock WORKSPACE --force'")
    return True


def _profile_check(workspace: WorkflowWorkspace, report: _Diagnosing) -> bool:
    """Record whether this workspace's core profile can be served at all."""

    if workspace.core_profile == CORE_PROFILE:
        report.check("core profile", True, f"the workspace is {workspace.core_profile}")
        return True
    report.check(
        "core profile",
        False,
        f"the workspace is {workspace.core_profile}, and this implementation only serves {CORE_PROFILE}; "
        "no manager built from it will claim any job here",
    )
    return False


def _manager_checks(
    workspace: WorkflowWorkspace,
    requirements: ClaimRequirements,
    report: _Diagnosing,
    *,
    backend_only: bool,
) -> None:
    """Record which registered managers would accept one job, and why not.

    Only a manager's own manifest can answer this: pools, capabilities, and
    served backends are deployment policy of the manager, never of the job.
    """

    records = read_managers(workspace)
    live = [record for record in records if record.alive()]
    if not records:
        report.check("registered manager", False, "no manager has ever registered in this workspace")
        report.hint("start one with 'httk workflow manager run WORKSPACE'")
        return
    if not live:
        report.check(
            "registered manager",
            False,
            f"{len(records)} manager(s) registered here, but none has a live heartbeat",
        )
        report.hint("start one with 'httk workflow manager run WORKSPACE'")
        for record in records:
            report.check("stopped manager", None, record.describe())
        return
    accepting: list[ManagerRecord] = []
    for record in live:
        reasons = manager_refusals(record, requirements)
        if backend_only:
            reasons = [reason for reason in reasons if "runner backend" in reason]
        if reasons:
            report.check("live manager", False, f"{record.describe()} {'; '.join(reasons)}")
        else:
            accepting.append(record)
            report.check("live manager", True, f"{record.describe()} offers everything this job requires")
    demanded = "runner backend" if backend_only else "runner backend, claim pool, and capabilities"
    report.check(
        "eligible manager",
        bool(accepting),
        (
            f"{len(accepting)} of {len(live)} live manager(s) match the {demanded} of this job"
            if accepting
            else f"no live manager matches the {demanded} of this job"
        ),
    )
    if not accepting:
        report.hint(
            "run a manager that matches, for example "
            f"'httk workflow manager run WORKSPACE --pool {requirements.pool}"
            + "".join(f" --capability {name}" for name in sorted(requirements.capabilities))
            + "'"
        )


def _requirement_checks(job: JobDefinition, report: _Diagnosing) -> ClaimRequirements:
    """Record the claim preconditions the job itself declares."""

    requirements = claim_requirements(job)
    report.check("claim pool", None, f"this job asks for pool {requirements.pool}")
    report.check(
        "required capabilities",
        None,
        ",".join(sorted(requirements.capabilities)) or "this job requires no capability",
    )
    report.check(
        "runner backend",
        None,
        f"this job runs on the {requirements.backend} runner backend "
        f"({job.runner_source}:{job.runner_path.as_posix()})",
    )
    return requirements


def _budget_checks(job: JobDefinition, state: Mapping[str, Any], report: _Diagnosing) -> BudgetStatus:
    """Record the budget consumption of one job."""

    budgets = budget_status(job, state)
    for text in budgets.describe():
        report.check("budget", not budgets.attempt_budget_exhausted, text)
    if budgets.attempt_budget_exhausted:
        report.hint(
            "the next claim of this job will fail it with budget_exhausted; raise retry_policy in a resubmitted job"
        )
    return budgets


def _owner_checks(workspace: WorkflowWorkspace, state: Mapping[str, Any], report: _Diagnosing) -> bool:
    """Record who owns a claimed or running job and whether that owner lives."""

    manager_id = _optional_string(state.get("manager_id"))
    # A recorded lease of zero is a real lease, so the default only applies when
    # the frame records nothing usable at all.
    recorded_lease = _optional_float(state.get("lease_seconds"))
    lease_seconds = workspace.policy.lease_seconds if recorded_lease is None else recorded_lease
    if manager_id is None:
        report.check("owning manager", False, "the state frame records no owning manager")
        return False
    record = next((item for item in read_managers(workspace) if item.manager_id == manager_id), None)
    if record is None:
        report.check("owning manager", False, f"manager {manager_id} published no manifest in this workspace")
        return False
    age = record.heartbeat_age_seconds
    alive = record.alive(lease_seconds=lease_seconds)
    report.check(
        "owning manager",
        alive,
        f"{record.describe()}; the lease is {lease_seconds:.0f}s and the heartbeat is "
        + ("of unknown age" if age is None else f"{age:.0f}s old"),
    )
    if not alive:
        report.check(
            "lease",
            False,
            "the lease has expired, so the next manager that serves this job's backend recovers it",
        )
        report.hint("start or keep a manager running so the expired lease is recovered")
    return alive


def _continue_checks(job: JobDefinition | None, state: Mapping[str, Any], report: _Diagnosing) -> None:
    """Record whether an operator ``continue`` request applies to this job."""

    if _optional_string(state.get("activation_id")) is None:
        report.check(
            "operator continue",
            False,
            "this job has no recorded activation, so 'continue' is refused as invalid",
        )
        return
    if job is None:
        report.check("operator continue", None, "the job definition is unreadable, so its budget is unknown")
        return
    budgets = budget_status(job, state)
    if budgets.attempt_budget_exhausted:
        report.check(
            "operator continue",
            False,
            "'continue' would immediately end the job again with retry_exhausted: " + "; ".join(budgets.describe()),
        )
        report.hint(
            "use 'httk workflow job request WORKSPACE JOB override_step --step STEP "
            "--operator NAME --reason WHY' to start a new activation instead"
        )
        return
    report.check("operator continue", True, "'continue' repeats this activation: " + "; ".join(budgets.describe()))
    report.hint("resume it with 'httk workflow job request WORKSPACE JOB continue --operator NAME --reason WHY'")


def _breadcrumb_check(control: Path | None, report: _Diagnosing) -> None:
    """Record the runner error breadcrumb of the last attempt, when it left one."""

    breadcrumb = read_error_breadcrumb(control)
    if breadcrumb is None:
        report.check("error breadcrumb", None, "the last attempt left no error.json breadcrumb")
        return
    report.check(
        "error breadcrumb",
        False,
        f"step {breadcrumb.get('step')} raised {breadcrumb.get('exception')}: {breadcrumb.get('message')}",
    )
    if control is not None:
        report.hint(f"read the complete traceback in {control / 'error.json'}")


def explain_job(workspace: WorkflowWorkspace, marker: Marker) -> Diagnosis:
    """Explain why one job is, or is not, making progress."""

    state, state_error = _state_of(workspace, marker)
    job, job_error = _job_of(workspace, marker)
    report = _Diagnosing()
    if state_error is not None:
        report.check("state frame", False, state_error)
    if job_error is not None:
        report.check("job definition", False, job_error)
        report.hint("repair the payload with a workspace tool: nothing schedules a job it cannot read")
    control = _attempt_control(workspace, marker, state)
    kind = marker.kind
    blocked = kind not in {"claimed", "running", "committing", "succeeded"}
    summary = f"state {kind}"

    if kind == "submitted":
        summary = (
            "this job is submitted but not registered: no manager has validated it and moved it to ready. "
            "Registration only needs a manager that serves this job's runner backend; the claim pool, the "
            "capabilities, and the maintenance lock are checked later, when the job is claimed."
        )
        served = _profile_check(workspace, report)
        if job is not None and served:
            requirements = _requirement_checks(job, report)
            _manager_checks(workspace, requirements, report, backend_only=True)
    elif kind == "ready":
        summary = "this job is ready and waiting to be claimed; every claim precondition is listed below"
        served = _profile_check(workspace, report)
        if job is not None and served:
            requirements = _requirement_checks(job, report)
            _manager_checks(workspace, requirements, report, backend_only=False)
            _budget_checks(job, state, report)
        paused = _maintenance_check(workspace, report)
        if paused:
            summary = "this job is ready, but a live maintenance lock stops every manager from launching work"
    elif kind in {"claimed", "running"}:
        alive = _owner_checks(workspace, state, report)
        summary = (
            f"this job is {kind} by a live manager, so it is progressing"
            if alive
            else f"this job is {kind} by a manager whose lease expired; it is recovered rather than stuck"
        )
        blocked = not alive
        if kind == "running" and control is not None:
            report.check("attempt logs", None, f"the live attempt writes {control / 'stdout.log'}")
    elif kind == "committing":
        summary = (
            "this job published an outcome and its commit is pending; any manager that serves its runner "
            "backend resumes the commit, so no operator action is needed"
        )
        _owner_checks(workspace, state, report)
        if job is not None:
            _manager_checks(workspace, claim_requirements(job), report, backend_only=True)
    elif kind == "waiting":
        join = state.get("join")
        condition = str(join.get("condition", "-")) if isinstance(join, Mapping) else "-"
        observations = observe_join(workspace, join) if isinstance(join, Mapping) else []
        report.check("join condition", None, f"{condition} then step {state.get('next_step') or '-'}")
        blocking = [item for item in observations if not item.get("terminal")]
        unresolvable = [item for item in observations if item.get("kind") is None]
        for item in observations:
            report.check("join child", bool(item.get("terminal")), _describe_child(item))
        if unresolvable:
            summary = (
                f"this job waits on {len(unresolvable)} child(ren) that cannot be resolved in this workspace; "
                "after the manager's join grace expires it fails with dependency_failure"
            )
        elif blocking:
            summary = f"this job waits for {len(blocking)} of {len(observations)} child(ren) to become terminal"
            report.hint("inspect a blocking child with 'httk workflow job why WORKSPACE CHILD_JOB'")
        elif observations:
            summary = "every join child is terminal, so the next manager pass resolves this join"
            blocked = False
        else:
            summary = (
                "this job waits on a join that names no readable child, which the manager reports as a protocol error"
            )
        if job is not None:
            _manager_checks(workspace, claim_requirements(job), report, backend_only=True)
    elif kind == "failed":
        failure = state.get("failure")
        if isinstance(failure, Mapping):
            report.check("failure", False, f"{failure.get('code')}: {failure.get('message')}")
            details = failure.get("details")
            if isinstance(details, Mapping) and details:
                report.check("failure details", None, json.dumps(details, sort_keys=True))
            summary = f"this job failed with {failure.get('code')} and stays failed until an operator resumes it"
        else:
            summary = "this job failed without a readable failure record"
        _breadcrumb_check(control, report)
        _continue_checks(job, state, report)
    elif kind == "paused":
        summary = "this job is paused and only an operator request moves it"
        pause = state.get("pause")
        if pause:
            report.check("pause", None, json.dumps(pause, sort_keys=True))
        _breadcrumb_check(control, report)
        _continue_checks(job, state, report)
    elif kind == "cancelling":
        # Cancelling is neither stuck nor finished: the fence is already in
        # place, and what remains is proving that the fenced process stopped.
        summary = (
            "this job is being cancelled: its attempt is already fenced, so nothing it does can commit, "
            "and the marker moves to cancelled only once a manager verifies that the process ended"
        )
        report.check("operator", None, f"{state.get('operator') or '-'}: {state.get('operator_reason') or '-'}")
        report.check(
            "fencing",
            True,
            "the marker was renamed out of running before anything was signalled, so a late outcome "
            "from that attempt can no longer be applied",
        )
        alive = _owner_checks(workspace, state, report)
        cancellation = state.get("cancellation")
        if isinstance(cancellation, Mapping) and cancellation:
            report.check("termination evidence", None, json.dumps(cancellation, sort_keys=True))
        else:
            report.check(
                "termination evidence",
                None,
                "no exit has been verified yet; acceptable evidence is process_exited, "
                "process_group_absent, no_launched_process, or no_live_attempt",
            )
        if control is not None:
            report.check("attempt logs", None, f"the fenced attempt wrote {control / 'stdout.log'}")
        if job is not None:
            _manager_checks(workspace, claim_requirements(job), report, backend_only=True)
        if alive:
            blocked = False
            report.hint("no operator action is needed; the owning manager terminates and verifies the attempt")
        else:
            report.hint(
                "a cancellation that stays here is usually an attempt on another host: run a manager on "
                "the host named in the frame, or confirm with the batch system that the allocation ended"
            )
    elif kind == "succeeded":
        summary = "this job succeeded; nothing is left to run"
        blocked = False
    elif kind == "cancelled":
        summary = "this job was cancelled by an operator request and is terminal; resubmit it to run it again"
        report.check("operator", None, f"{state.get('operator') or '-'}: {state.get('operator_reason') or '-'}")
    else:
        summary = f"state {kind} is not a core state of this profile; inspect it with a workspace tool"

    if kind in QUIESCENT_KINDS and kind not in TERMINAL_KINDS:
        report.hint("drive it in the foreground with 'httk workflow job debug WORKSPACE JOB'")
    return Diagnosis(
        job_id=marker.job_id,
        job_key=marker.job_key,
        state=kind,
        summary=summary,
        blocked=blocked,
        checks=tuple(report.checks),
        hints=tuple(report.hints),
    )


# ---------------------------------------------------------------------------
# The foreground debug runner
# ---------------------------------------------------------------------------


class ScopedWorkspace(WorkflowWorkspace):
    """A workspace whose scheduling scans observe only the named jobs.

    The debug runner drives exactly one job with the real manager code paths, so
    the only thing it may restrict is what a private manager *scans*, never what
    it can *look up*: join children, marker-rename verification, and child
    registration keep resolving against the complete workspace exactly as they do
    for a production manager. Nothing in the manager changes; it simply never
    sees a marker that does not belong to the job being debugged.
    """

    def __init__(
        self,
        root: str | Path,
        scope: Iterable[str],
        *,
        durable: bool = True,
    ) -> None:
        super().__init__(root, durable=durable)
        self.scope = frozenset(scope)

    def scan_marker_entries(self, kinds: Iterable[str] | None = None) -> Iterator[Marker | MarkerFault]:
        """Yield only the markers of the scoped jobs, plus every unusable entry."""

        for entry in super().scan_marker_entries(kinds):
            if isinstance(entry, MarkerFault) or entry.job_key in self.scope:
                yield entry

    def _unscoped_markers(self, kinds: Iterable[str] | None = None) -> Iterator[Marker]:
        for entry in WorkflowWorkspace.scan_marker_entries(self, kinds):
            if isinstance(entry, Marker):
                yield entry
            else:
                self.report_marker_fault(entry)

    def find_markers(self, job_key: str, kinds: Iterable[str] | None = None) -> list[Marker]:
        """Find one job key anywhere in the workspace, ignoring the scope.

        The base implementation resolves through the workspace's own job-id
        index, which is built from the unscoped scan precisely so that a lookup
        is never narrowed by what a private manager is allowed to schedule.
        Only a lookup that must consider a non-core kind falls back to a scan
        here, and that scan is the unscoped one.
        """

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
    """Copy one payload into a staging directory whose initial step is *step*.

    ``job.json`` is immutable once submitted and its bytes are the job digest, so
    an initial-step override can only be applied before submission, to a copy.
    """

    validate_step(step, "step")
    staging = Path(tempfile.mkdtemp(prefix="httk-debug-"))
    payload = staging / "payload"
    shutil.copytree(source, payload, symlinks=False)
    definition = read_json(payload / "job.json")
    definition["initial_step"] = step
    (payload / "job.json").write_text(json.dumps(definition, sort_keys=True), encoding="utf-8")
    return payload


def debug_job(
    workspace: WorkflowWorkspace,
    target: str,
    *,
    placement: str = "debug",
    step: str | None = None,
    follow_children: bool = False,
    timeout: float = 3600.0,
    poll_interval: float = 0.05,
    emit: Callable[[str], None] | None = None,
) -> DebugOutcome:
    """Drive one job to a terminal state in the foreground.

    *target* is either a payload directory, which is submitted fresh, or a
    selector of a job that already exists. A private manager restricted to that
    one job performs every transition, and the attempt's stdout and stderr are
    streamed to the console as they grow.

    ``step`` overrides the initial step of a fresh payload. Overriding the step of
    a job that already has a history is refused: that is what the operator
    ``override_step`` request is for, and it belongs in the journal.

    ``follow_children`` drives the children a waiting job spawned, depth first,
    before resuming the parent.
    """

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
                "'httk workflow job request WORKSPACE JOB override_step --step STEP --operator NAME --reason WHY'"
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
    workspace: WorkflowWorkspace,
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
        f"(runner {job.runner_source}:{job.runner_path.as_posix()} on backend {job.runner_backend})"
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
            # Whatever the attempt wrote belongs before the transition it caused:
            # an attempt that has left running has already flushed everything.
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
                        f"[{meta}] {current.job_key} waits for its children; "
                        "rerun with --follow-children to drive them here"
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
    workspace: WorkflowWorkspace,
    marker: Marker,
    state: Mapping[str, Any],
    tails: dict[str, list[_Tail]],
    context: str,
    write: Callable[[str], None],
) -> None:
    """Stream whatever the attempts of this job have written since the last pass.

    Every tail ever opened is pumped, not only the current attempt's: a retried
    or superseded attempt must not lose the last lines it wrote.
    """

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
    workspace: WorkflowWorkspace,
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
