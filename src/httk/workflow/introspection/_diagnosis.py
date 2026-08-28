"""Claim-precondition and job-progress diagnosis."""

import json
import socket
import stat
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .._manager_runners import runner_module_allowed
from .._util import read_json, timestamp_seconds
from ..errors import WorkflowError
from ..manifests import read_maintenance_lock
from ..models import (
    CORE_PROFILE,
    DEFAULT_LEASE_SECONDS,
    QUIESCENT_KINDS,
    TERMINAL_KINDS,
    JobDefinition,
    Marker,
    normalize_placement,
    parse_package_runner,
    validate_process,
)
from ..workspace import Workspace
from ._reading import (
    _attempt_control,
    _job_of,
    _optional_float,
    _optional_int,
    _optional_string,
    _state_of,
    job_frames,
    read_error_breadcrumb,
)

#: The default packaged-runner module allowlist a manager publishes if its
#: manifest names none, matching :data:`~httk.workflow.manager.DEFAULT_RUNNER_MODULES`.
DEFAULT_RUNNER_MODULES = ("httk.workflow",)

#: Attempts under an unlimited budget beyond which ``job why`` calls a job
#: flapping rather than progressing.
FLAPPING_ATTEMPTS = 10

JOB_DIAGNOSIS_FORMAT = "httk-workflow-job-diagnosis"


@dataclass(frozen=True)
class ManagerRecord:
    """One manager's published manifest and the liveness of its heartbeat."""

    manager_id: str
    hostname: str | None
    pid: int | None
    pools: frozenset[str]
    capabilities: frozenset[str]
    placement_prefixes: tuple[str, ...]
    executors: frozenset[str]
    accept_any_pool: bool
    started_at: str | None
    heartbeat_at: str | None
    heartbeat_age_seconds: float | None
    uid: int | None = None
    runner_modules: tuple[str, ...] = DEFAULT_RUNNER_MODULES
    runner_search_paths: tuple[str, ...] = ()

    def alive(self, *, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> bool:
        """Whether this manager's heartbeat is still inside *lease_seconds*."""

        age = self.heartbeat_age_seconds
        return age is not None and age <= lease_seconds

    def describe(self) -> str:
        """Describe this manager for an operator diagnostic."""

        where = self.hostname or "an unrecorded host"
        pools = "any pool" if self.accept_any_pool else ",".join(sorted(self.pools)) or "no pool"
        capabilities = ",".join(sorted(self.capabilities)) or "-"
        prefixes = ",".join(self.placement_prefixes) if self.placement_prefixes else "whole workspace"
        executors = ",".join(sorted(self.executors)) or "-"
        age = (
            "no heartbeat" if self.heartbeat_age_seconds is None else f"heartbeat {self.heartbeat_age_seconds:.0f}s ago"
        )
        return (
            f"{self.manager_id} on {where} (pools {pools}, capabilities {capabilities}, "
            f"placement {prefixes}, executors {executors}, {age})"
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this manager record."""

        return {
            "manager_id": self.manager_id,
            "hostname": self.hostname,
            "pid": self.pid,
            "pools": sorted(self.pools),
            "capabilities": sorted(self.capabilities),
            "placement_prefixes": list(self.placement_prefixes),
            "executors": sorted(self.executors),
            "accept_any_pool": self.accept_any_pool,
            "uid": self.uid,
            "runner_modules": list(self.runner_modules),
            "runner_search_paths": list(self.runner_search_paths),
            "started_at": self.started_at,
            "heartbeat_at": self.heartbeat_at,
            "heartbeat_age_seconds": self.heartbeat_age_seconds,
            "alive": self.alive(),
        }


def _label_set(value: object) -> frozenset[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return frozenset()
    return frozenset(item for item in value if isinstance(item, str))


def _label_sequence(value: object) -> tuple[str, ...]:
    """Return a manifest's ordered string list, or nothing if it has none."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def read_managers(workspace: Workspace) -> list[ManagerRecord]:
    """Return every manager that has ever registered in this workspace."""

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
                placement_prefixes=_label_sequence(manifest.get("placement_prefixes")),
                executors=_label_set(manifest.get("executors")),
                accept_any_pool=bool(manifest.get("accept_any_pool", False)),
                uid=_optional_int(manifest.get("uid")),
                runner_modules=(
                    DEFAULT_RUNNER_MODULES
                    if manifest.get("runner_modules") is None
                    else _label_sequence(manifest.get("runner_modules"))
                ),
                runner_search_paths=_label_sequence(manifest.get("runner_search_paths")),
                started_at=_optional_string(manifest.get("started_at")),
                heartbeat_at=heartbeat_at,
                heartbeat_age_seconds=age,
            )
        )
    return records


@dataclass(frozen=True)
class ClaimRequirements:
    """What one job demands of any manager that claims it."""

    executor: str
    pool: str
    capabilities: frozenset[str]

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of these requirements."""

        return {
            "runner_executor": self.executor,
            "claim_pool": self.pool,
            "required_capabilities": sorted(self.capabilities),
        }


def claim_requirements(job: JobDefinition) -> ClaimRequirements:
    """Return the claim preconditions *job* imposes on a manager."""

    return ClaimRequirements(
        executor=job.runner_executor,
        pool=job.claim_pool,
        capabilities=job.required_capabilities,
    )


def _placement_covered(record: ManagerRecord, placement: str) -> bool:
    """Whether *placement* lies at or below one of *record*'s scanned prefixes."""

    parts = normalize_placement(placement).parts
    for prefix in record.placement_prefixes:
        prefix_parts = normalize_placement(prefix).parts
        if parts[: len(prefix_parts)] == prefix_parts:
            return True
    return False


def _payload_ownership_issue(workspace: Workspace, marker: Marker) -> str | None:
    """Describe a marker/payload ownership failure visible to ``job why``."""

    try:
        marker_uid = marker.path.lstat().st_uid
        payload = workspace.payload_path(marker.placement, marker.job_key)
        payload_stat = payload.lstat()
        if stat.S_ISLNK(payload_stat.st_mode) or not stat.S_ISDIR(payload_stat.st_mode):
            return "payload path is a symlink or not a directory"
        job_stat = (payload / "job.json").lstat()
        if stat.S_ISLNK(job_stat.st_mode) or not stat.S_ISREG(job_stat.st_mode):
            return "payload job.json is a symlink or not a regular file"
        if {marker_uid, payload_stat.st_uid, job_stat.st_uid} != {marker_uid}:
            return (
                f"marker uid {marker_uid} does not match payload uid {payload_stat.st_uid} "
                f"and job.json uid {job_stat.st_uid}"
            )
    except OSError as exc:
        return f"payload ownership cannot be checked: {exc}"
    return None


def _runner_refusal(record: ManagerRecord, job: JobDefinition | None) -> str | None:
    """Return why *record* could never resolve *job*'s runner, or ``None``.

    Only a shared installed runner can be refused on the manifest alone: a
    ``pkg:`` runner outside the manager's module allowlist can never resolve
    there, and a plain installed runner needs at least one search path. A
    workspace or payload runner is resolvable by any manager, and a plain
    installed runner with a search path can only be judged on the manager's own
    host, so neither is refused here.

    :param record: The manager whose runner reach is checked.
    :param job: The job whose runner reference is checked, when readable.
    :return: A refusal reason, or ``None`` when the runner is not refused.
    """

    if job is None or job.runner_source != "installed":
        return None
    package = parse_package_runner(job.runner_path.as_posix())
    if package is not None:
        module = package[0]
        if not runner_module_allowed(module, record.runner_modules):
            allowed = ",".join(record.runner_modules) or "none"
            return f"does not allow runner module {module} (allows {allowed})"
        return None
    if not record.runner_search_paths:
        return f"has no runner search path for installed runner {job.runner_path.as_posix()}"
    return None


def manager_refusals(
    record: ManagerRecord,
    requirements: ClaimRequirements,
    *,
    placement: str | None = None,
    owner_uid: int | None = None,
    job: JobDefinition | None = None,
) -> list[str]:
    """Return why *record* would not claim a job with *requirements*.

    :param record: The manager whose claim preconditions are checked.
    :param requirements: The executor, pool, and capabilities the job demands.
    :param placement: The job placement, when a placement restriction applies.
    :param owner_uid: The job owner uid, when ownership is checked.
    :param job: The job definition, when its runner reachability is checked.
    :return: One human-readable reason per unmet precondition.
    """

    reasons: list[str] = []
    if owner_uid is not None and record.uid is not None and record.uid != owner_uid:
        reasons.append(f"is owned by another user (uid {owner_uid}); managers run only jobs owned by their account")
    if requirements.executor not in record.executors:
        reasons.append(f"does not serve runner executor {requirements.executor}")
    if not record.accept_any_pool and requirements.pool not in record.pools:
        reasons.append(f"does not serve claim pool {requirements.pool}")
    missing = requirements.capabilities - record.capabilities
    if missing:
        reasons.append(f"lacks capabilities {','.join(sorted(missing))}")
    if placement is not None and record.placement_prefixes and not _placement_covered(record, placement):
        reasons.append(f"does not scan placement {placement} (restricted to {','.join(record.placement_prefixes)})")
    runner = _runner_refusal(record, job)
    if runner is not None:
        reasons.append(runner)
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
        per_activation = self.maximum_attempts_per_activation
        if per_activation is not None and self.attempts_this_activation + 1 > per_activation:
            return True
        total = self.maximum_total_attempts
        return total is not None and self.total_attempts + 1 > total

    @property
    def activation_budget_exhausted(self) -> bool:
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


def observe_join(workspace: Workspace, join: Mapping[str, Any]) -> list[dict[str, object]]:
    """Observe every child one waiting job's join names."""

    children = join.get("children")
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
        return []
    observations: list[dict[str, object]] = []
    for raw in children:
        if not isinstance(raw, Mapping):
            observations.append(
                {
                    "label": None,
                    "job_id": None,
                    "kind": None,
                    "error": "child reference is not an object",
                }
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


def _describe_child(observation: Mapping[str, object], *, with_failure: bool = False) -> str:
    label = observation.get("label") or "-"
    identity = observation.get("job_key") or observation.get("job_id") or "-"
    kind = observation.get("kind")
    state = "not resolvable in this workspace" if kind is None else str(kind)
    error = observation.get("error")
    suffix = f" ({error})" if isinstance(error, str) and error else ""
    if with_failure:
        failure = observation.get("failure")
        if isinstance(failure, Mapping) and failure.get("code"):
            suffix += f" [{failure.get('code')}]"
    return f"{label}: {identity} is {state}{suffix}"


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
            "format_version": 2,
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


def _maintenance_check(workspace: Workspace, report: _Diagnosing) -> bool:
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
        report.hint("clear the stale lock with 'httk workspace unlock WORKSPACE'")
        return False
    report.check(
        "maintenance lock",
        False,
        f"launching is paused by the maintenance lock held by {lock.describe()}",
    )
    report.hint("wait for the maintenance operation, or 'httk workspace unlock --force WORKSPACE'")
    return True


def _profile_check(workspace: Workspace, report: _Diagnosing) -> bool:
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
    workspace: Workspace,
    requirements: ClaimRequirements,
    report: _Diagnosing,
    *,
    executor_only: bool,
    placement: str | None = None,
    owner_uid: int | None = None,
    job: JobDefinition | None = None,
) -> None:
    """Record which registered managers would accept one job, and why not."""

    records = read_managers(workspace)
    live = [record for record in records if record.alive()]
    if not records:
        report.check(
            "registered manager",
            False,
            "no manager has ever registered in this workspace",
        )
        report.hint("start one with 'httk workflow manager run --workspace WORKSPACE'")
        return
    if not live:
        report.check(
            "registered manager",
            False,
            f"{len(records)} manager(s) registered here, but none has a live heartbeat",
        )
        report.hint("start one with 'httk workflow manager run --workspace WORKSPACE'")
        for record in records:
            report.check("stopped manager", None, record.describe())
        return
    accepting: list[ManagerRecord] = []
    for record in live:
        reasons = manager_refusals(record, requirements, placement=placement, owner_uid=owner_uid, job=job)
        if executor_only:
            reasons = [
                reason
                for reason in reasons
                if "runner executor" in reason
                or "does not scan placement" in reason
                or "owned by another user" in reason
            ]
        if reasons:
            report.check("live manager", False, f"{record.describe()} {'; '.join(reasons)}")
        else:
            accepting.append(record)
            report.check(
                "live manager",
                True,
                f"{record.describe()} offers everything this job requires",
            )
    demanded = "runner executor" if executor_only else "runner executor, claim pool, and capabilities"
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
            f"'httk workflow manager run --pool {requirements.pool} --workspace WORKSPACE"
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
        "runner executor",
        None,
        f"this job runs on the {requirements.executor} runner executor "
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


def _owner_checks(workspace: Workspace, state: Mapping[str, Any], report: _Diagnosing) -> bool:
    """Record who owns a claimed or running job and whether that owner lives."""

    manager_id = _optional_string(state.get("manager_id"))
    recorded_lease = _optional_float(state.get("lease_seconds"))
    lease_seconds = workspace.policy.lease_seconds if recorded_lease is None else recorded_lease
    if manager_id is None:
        report.check("owning manager", False, "the state frame records no owning manager")
        return False
    record = next(
        (item for item in read_managers(workspace) if item.manager_id == manager_id),
        None,
    )
    if record is None:
        report.check(
            "owning manager",
            False,
            f"manager {manager_id} published no manifest in this workspace",
        )
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
            "the lease has expired, so the next manager that serves this job's executor recovers it",
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
        report.check(
            "operator continue",
            None,
            "the job definition is unreadable, so its budget is unknown",
        )
        return
    budgets = budget_status(job, state)
    if budgets.attempt_budget_exhausted:
        report.check(
            "operator continue",
            False,
            "'continue' would immediately end the job again with retry_exhausted: " + "; ".join(budgets.describe()),
        )
        report.hint(
            "use 'httk job request override_step --workspace WORKSPACE --step STEP --operator NAME --reason WHY JOB' to start a new activation instead"
        )
        return
    report.check(
        "operator continue",
        True,
        "'continue' repeats this activation: " + "; ".join(budgets.describe()),
    )
    report.hint("resume it with 'httk job request continue --workspace WORKSPACE --operator NAME --reason WHY JOB'")


def _attempt_history_check(workspace: Workspace, marker: Marker, state: Mapping[str, Any], report: _Diagnosing) -> None:
    """Fold this job's journal into one attempt-history line.

    Every attempt is claimed with a fresh ``attempt_id`` and every activation
    with a fresh ``activation_id``, so distinct identifiers count attempts and
    activations across the whole history; the ``unclean_restart`` frame member,
    otherwise unread, counts how many attempts followed an unclean exit.

    :param workspace: The workspace holding the job's journal.
    :param marker: The authoritative marker of the job.
    :param state: The current state frame, used only for the current step.
    :param report: The diagnosis being accumulated.
    """

    attempts: set[str] = set()
    activations: set[str] = set()
    unclean = 0
    step: str | None = None
    for frame in job_frames(workspace, marker):
        attempt_id = _optional_string(frame.get("attempt_id"))
        if attempt_id is not None:
            attempts.add(attempt_id)
        activation_id = _optional_string(frame.get("activation_id"))
        if activation_id is not None:
            activations.add(activation_id)
        frame_step = _optional_string(frame.get("step"))
        if frame_step is not None:
            step = frame_step
        if frame.get("unclean_restart") is True:
            unclean += 1
    if not attempts:
        return
    step = step or _optional_string(state.get("step")) or "-"
    report.check(
        "attempt history",
        None,
        f"{len(attempts)} attempts across {len(activations)} activations at step {step!r}; "
        f"{unclean} after unclean exits",
    )


def _flapping_check(job: JobDefinition | None, state: Mapping[str, Any], report: _Diagnosing) -> None:
    """Warn when an unlimited-budget job keeps attempting without progressing.

    :param job: The job definition, when readable.
    :param state: The current state frame.
    :param report: The diagnosis being accumulated.
    """

    if job is None:
        return
    budgets = budget_status(job, state)
    unlimited = budgets.maximum_attempts_per_activation is None and budgets.maximum_total_attempts is None
    attempts = max(budgets.total_attempts, budgets.attempts_this_activation)
    if not unlimited or attempts <= FLAPPING_ATTEMPTS:
        return
    report.check(
        "flapping",
        False,
        f"this job has attempted {attempts} times under an unlimited budget; it is flapping, not progressing "
        "— consider maximum_attempts_per_activation or retry_on",
    )


def _read_request(path: Path) -> dict[str, Any] | None:
    """Return one request or retirement document, or ``None`` when unreadable."""

    try:
        return read_json(path)
    except WorkflowError:
        return None


def _request_checks(workspace: Workspace, marker: Marker, job: JobDefinition | None, report: _Diagnosing) -> None:
    """Surface a pending operator request and the most recent retirement.

    A request in ``requests/ready`` has not been applied by any manager yet, and
    is applied only by one serving this job's runner executor; a request in
    ``requests/retired`` can never apply again, and the reason recorded beside it
    is the last operator action's fate.

    :param workspace: The workspace holding the request flow.
    :param marker: The authoritative marker of the job.
    :param job: The job definition, used for the serving executor.
    :param report: The diagnosis being accumulated.
    """

    requests = workspace.control / "requests"
    executor = "-" if job is None else job.runner_executor
    ready = requests / "ready"
    for path in sorted(ready.iterdir()) if ready.is_dir() else ():
        if not path.is_file():
            continue
        request = _read_request(path)
        if request is None or request.get("job_key") != marker.job_key:
            continue
        action = _optional_string(request.get("action")) or "?"
        operator = _optional_string(request.get("operator")) or "-"
        report.check(
            "pending request",
            None,
            f"a {action} request from {operator} is pending; it is applied by a manager serving executor {executor!r}",
        )
    retired = requests / "retired"
    latest: tuple[str, str] | None = None
    for path in sorted(retired.iterdir()) if retired.is_dir() else ():
        if path.suffix != ".retirement":
            continue
        retirement = _read_request(path)
        request = _read_request(retired / path.name[: -len(".retirement")])
        if retirement is None or request is None or request.get("job_key") != marker.job_key:
            continue
        retired_at = _optional_string(retirement.get("retired_at")) or ""
        reason = _optional_string(retirement.get("reason")) or "unknown"
        if latest is None or retired_at > latest[0]:
            latest = (retired_at, reason)
    if latest is not None:
        report.check(
            "retired request",
            None,
            f"the most recent operator request for this job was retired: {latest[1]}",
        )


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


def _persistent_writer_host(state: Mapping[str, object]) -> str | None:
    """Return the foreign host that launched a persistent attempt, if any.

    A persistent-workdir attempt whose writer ran on another host cannot be
    recovered from here: no manager on this host can prove the foreign writer
    stopped, so taking the workdir over would risk two writers in one shared
    directory. This returns that host only when it is neither absent nor this
    one.
    """

    process = validate_process(state.get("process"))
    if process is None:
        return None
    host = process["hostname"]
    if not isinstance(host, str) or host == socket.gethostname():
        return None
    return host


def _commit_wedge(control: Path | None) -> str | None:
    """Return a persisted committing-wedge error, when a manager recorded one."""

    if control is None:
        return None
    try:
        recorded = read_json(control / "commit-wedge.json")
    except WorkflowError:
        return None
    error = recorded.get("error")
    return error if isinstance(error, str) and error else None


def _retained_log_path(workspace: Workspace, marker: Marker, state: Mapping[str, object]) -> Path:
    """Return the retained stdio path named by a failure frame, if valid."""

    payload = workspace.payload_path(marker.placement, marker.job_key)
    failure = state.get("failure")
    if isinstance(failure, Mapping):
        details = failure.get("details")
        if isinstance(details, Mapping):
            paths = details.get("log_paths")
            if isinstance(paths, Sequence) and not isinstance(paths, (str, bytes)):
                for value in paths:
                    if not isinstance(value, str):
                        continue
                    relative = Path(value)
                    if not relative.is_absolute() and ".." not in relative.parts:
                        return payload / relative
    return payload / "logs" / "stdio.out"


def explain_job(workspace: Workspace, marker: Marker) -> Diagnosis:
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
    pause_requested = state.get("pause_requested")
    if isinstance(pause_requested, Mapping):
        report.check(
            "pause requested",
            None,
            f"by {pause_requested.get('operator') or '-'} ({pause_requested.get('reason') or '-'})"
            "; will pause at the next attempt boundary",
        )
    try:
        owner_uid: int | None = marker.path.lstat().st_uid
    except FileNotFoundError:
        owner_uid = None
    ownership_issue = _payload_ownership_issue(workspace, marker)
    if ownership_issue is not None:
        report.check(
            "payload ownership",
            False,
            f"marker and payload ownership mismatch: {ownership_issue}; managers refuse this job by default",
        )

    if kind == "submitted":
        summary = (
            "this job is submitted but not registered: no manager has validated it and moved it to ready. "
            "Registration only needs a manager that serves this job's runner executor; the claim pool, the "
            "capabilities, and the maintenance lock are checked later, when the job is claimed."
        )
        served = _profile_check(workspace, report)
        if job is not None and served:
            requirements = _requirement_checks(job, report)
            _manager_checks(
                workspace,
                requirements,
                report,
                executor_only=True,
                placement=marker.placement.as_posix(),
                owner_uid=owner_uid,
                job=job,
            )
    elif kind == "ready":
        summary = "this job is ready and waiting to be claimed; every claim precondition is listed below"
        served = _profile_check(workspace, report)
        if job is not None and served:
            requirements = _requirement_checks(job, report)
            _manager_checks(
                workspace,
                requirements,
                report,
                executor_only=False,
                placement=marker.placement.as_posix(),
                owner_uid=owner_uid,
                job=job,
            )
            _budget_checks(job, state, report)
        paused = _maintenance_check(workspace, report)
        if paused:
            summary = "this job is ready, but a live maintenance lock stops every manager from launching work"
        _attempt_history_check(workspace, marker, state, report)
        _flapping_check(job, state, report)
        _request_checks(workspace, marker, job, report)
    elif kind in {"claimed", "running"}:
        alive = _owner_checks(workspace, state, report)
        summary = (
            f"this job is {kind} by a live manager, so it is progressing"
            if alive
            else f"this job is {kind} by a manager whose lease expired; it is recovered rather than stuck"
        )
        blocked = not alive
        foreign_host = (
            _persistent_writer_host(state)
            if kind == "running" and not alive and job is not None and job.workdir_mode == "persistent"
            else None
        )
        if foreign_host is not None:
            blocked = True
            summary = (
                f"this job is running a persistent workdir whose writer was last seen on {foreign_host}; its lease "
                "expired, but recovery cannot happen from this host"
            )
            report.check(
                "persistent takeover",
                False,
                f"the recorded writer ran on {foreign_host}, not this host, so no manager here can prove it stopped",
            )
            report.hint(
                f"no manager on another host can prove the writer stopped; run a manager on {foreign_host}, "
                "or pass --unsafe-persistent-takeover"
            )
        if kind == "running" and control is not None:
            report.check(
                "attempt logs",
                None,
                f"the job writes {_retained_log_path(workspace, marker, state)}",
            )
        _attempt_history_check(workspace, marker, state, report)
        _flapping_check(job, state, report)
    elif kind == "committing":
        wedge = _commit_wedge(control)
        if wedge is not None:
            blocked = True
            summary = f"this job's commit is wedged and keeps failing: {wedge}"
            report.check("commit", False, wedge)
            report.hint(
                "the commit is not making progress; inspect the outcome in the attempt control directory and "
                "repair the payload, then a manager can resume it"
            )
        else:
            summary = (
                "this job published an outcome and its commit is pending; any manager that serves its runner "
                "executor resumes the commit, so no operator action is needed"
            )
        _owner_checks(workspace, state, report)
        if job is not None:
            _manager_checks(
                workspace,
                claim_requirements(job),
                report,
                executor_only=True,
                placement=marker.placement.as_posix(),
                owner_uid=owner_uid,
                job=job,
            )
    elif kind == "waiting":
        join = state.get("join")
        condition = str(join.get("condition", "-")) if isinstance(join, Mapping) else "-"
        observations = observe_join(workspace, join) if isinstance(join, Mapping) else []
        report.check(
            "join condition",
            None,
            f"{condition} then step {state.get('next_step') or '-'}",
        )
        blocking = [item for item in observations if not item.get("terminal")]
        unresolvable = [item for item in observations if item.get("kind") is None]
        for item in observations:
            report.check("join child", bool(item.get("terminal")), _describe_child(item))
        if unresolvable:
            recorded = state.get("join_unresolved")
            since = (
                str(recorded.get("first_unresolved_at"))
                if isinstance(recorded, Mapping) and recorded.get("first_unresolved_at") is not None
                else None
            )
            grace_clause = (
                f"the join grace is counting from when a manager first recorded it unresolvable ({since}), "
                "is persisted in the state frame so it survives a manager restart, and once it elapses "
                "the job fails with dependency_failure"
                if since is not None
                else (
                    "the join grace starts when a manager first records the child unresolvable and is persisted "
                    "in the state frame; once it elapses the job fails with dependency_failure"
                )
            )
            summary = (
                f"this job waits on {len(unresolvable)} child(ren) that cannot be resolved in this workspace; "
                f"{grace_clause}"
            )
        elif blocking:
            summary = f"this job waits for {len(blocking)} of {len(observations)} child(ren) to become terminal"
            report.hint("inspect a blocking child with 'httk job why --workspace WORKSPACE CHILD_JOB'")
        elif observations:
            summary = "every join child is terminal, so the next manager pass resolves this join"
            blocked = False
        else:
            summary = (
                "this job waits on a join that names no readable child, which the manager reports as a protocol error"
            )
        if job is not None:
            _manager_checks(
                workspace,
                claim_requirements(job),
                report,
                executor_only=True,
                placement=marker.placement.as_posix(),
                owner_uid=owner_uid,
                job=job,
            )
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
        join_summary = state.get("join_summary")
        if isinstance(join_summary, Sequence) and not isinstance(join_summary, (str, bytes)):
            for item in join_summary:
                if isinstance(item, Mapping) and item.get("kind") != "succeeded":
                    report.check("dependency child", False, _describe_child(item, with_failure=True))
        _breadcrumb_check(control, report)
        if control is not None:
            report.check(
                "attempt logs",
                None,
                f"the job writes {_retained_log_path(workspace, marker, state)}",
            )
        _continue_checks(job, state, report)
        _attempt_history_check(workspace, marker, state, report)
        _flapping_check(job, state, report)
        _request_checks(workspace, marker, job, report)
    elif kind == "paused":
        summary = "this job is paused and only an operator request moves it"
        pause = state.get("pause")
        if pause:
            report.check("pause", None, json.dumps(pause, sort_keys=True))
        _breadcrumb_check(control, report)
        _continue_checks(job, state, report)
        _request_checks(workspace, marker, job, report)
    elif kind == "cancelling":
        # Cancelling is neither stuck nor finished: the fence is already in
        # place, and what remains is proving that the fenced process stopped.
        summary = (
            "this job is being cancelled: its attempt is already fenced, so nothing it does can commit, "
            "and the marker moves to cancelled only once a manager verifies that the process ended"
        )
        report.check(
            "operator",
            None,
            f"{state.get('operator') or '-'}: {state.get('operator_reason') or '-'}",
        )
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
                "process_group_absent, or no_live_attempt",
            )
        if control is not None:
            report.check(
                "attempt logs",
                None,
                f"the job writes {_retained_log_path(workspace, marker, state)}",
            )
        if job is not None:
            _manager_checks(
                workspace,
                claim_requirements(job),
                report,
                executor_only=True,
                placement=marker.placement.as_posix(),
                owner_uid=owner_uid,
                job=job,
            )
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
        report.check(
            "operator",
            None,
            f"{state.get('operator') or '-'}: {state.get('operator_reason') or '-'}",
        )
    else:
        summary = f"state {kind} is not a core state of this profile; inspect it with a workspace tool"

    if kind in QUIESCENT_KINDS and kind not in TERMINAL_KINDS:
        report.hint("drive it in the foreground with 'httk job debug WORKSPACE JOB'")
    return Diagnosis(
        job_id=marker.job_id,
        job_key=marker.job_key,
        state=kind,
        summary=summary,
        blocked=blocked,
        checks=tuple(report.checks),
        hints=tuple(report.hints),
    )
