"""Claim-precondition and job-progress diagnosis (``job why``), read-only.

Owner liveness comes from :func:`httk.workflow._death.probe` directly, which
writes nothing: a diagnosis never writes a tombstone or recovers a job (only a
manager tick and workspace maintenance do, through the kernel).
"""

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from .. import _death, _kernel, _moving, _requests, _store
from .._bundles import read_manifest
from .._carried import carried_problem
from .._durations import format_duration
from .._job import JobDefinition
from .._kernel import OWNED, JobRef, OwnerRecord
from .._manager_commit import attempt_budget_failure
from .._manager_scheduling import binding_problem, bound_workflows, effective_workflow, unmet_job_requirements
from .._state import TERMINAL_STATES, StateDoc
from .._util import timestamp_seconds
from ..errors import FormatError, RunnerResolutionError, WorkflowError
from ..models import BUDGET_LIMITS, normalize_placement, placement_text
from ..workspace import Workspace
from ._reading import (
    attempt_control,
    job_events,
    job_placement,
    read_error_breadcrumb,
    read_job,
    read_state,
)

#: Attempts under an unlimited budget beyond which ``job why`` calls a job
#: flapping rather than progressing.
FLAPPING_ATTEMPTS = 10

JOB_DIAGNOSIS_FORMAT = "httk-workflow-job-diagnosis"


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


@dataclass(frozen=True)
class ManagerRecord:
    """One manager registered in ``owners/``, as its ``owner.json`` advertises it.

    :param manager_id: The owner id.
    :param hostname: The host it runs on.
    :param pid: Its process id.
    :param pools: The claim pools it serves.
    :param capabilities: The capabilities it advertises.
    :param placement_prefixes: The placement subtrees it schedules; empty for the whole workspace.
    :param started_at: When it registered.
    :param heartbeat_at: Its last informational heartbeat.
    :param heartbeat_age_seconds: The heartbeat's age (informational: no liveness is decided by a clock).
    :param liveness: ``alive``, ``dead`` or ``unknown``, as the side-effect-free death proof concludes here.
    :param uid: The account owning its owner directory.
    :param end_time: The epoch second its recorded allocation ends, when known.
    """

    manager_id: str
    hostname: str | None
    pid: int | None
    pools: frozenset[str]
    capabilities: frozenset[str]
    placement_prefixes: tuple[str, ...]
    started_at: str | None
    heartbeat_at: str | None
    heartbeat_age_seconds: float | None
    liveness: str = _death.Liveness.UNKNOWN.value
    uid: int | None = None
    end_time: float | None = None

    def ends(self) -> str | None:
        """Describe when this manager's allocation ends, or ``None`` when it recorded no end.

        :return: The description.
        """

        if self.end_time is None:
            return None
        left = int(self.end_time - time.time())
        return f"ends in {format_duration(left)}" if left > 0 else "ended"

    def alive(self) -> bool:
        """Whether this manager is not proven dead (a manager on another host is never proven alive here).

        :return: Whether it may still claim work.
        """

        return self.liveness != _death.Liveness.DEAD.value

    def describe(self) -> str:
        """Describe this manager for an operator diagnostic.

        :return: The description.
        """

        pools = ",".join(sorted(self.pools)) or "no pool"
        capabilities = ",".join(sorted(self.capabilities)) or "-"
        prefixes = ",".join(self.placement_prefixes) or "whole workspace"
        age = (
            "no heartbeat" if self.heartbeat_age_seconds is None else f"heartbeat {self.heartbeat_age_seconds:.0f}s ago"
        )
        ends = self.ends()
        return (
            f"{self.manager_id} on {self.hostname or 'an unrecorded host'} ({self.liveness}; pools {pools}, "
            f"capabilities {capabilities}, placement {prefixes}, {age}{'' if ends is None else ', ' + ends})"
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this manager record.

        :return: The mapping.
        """

        return {
            "manager_id": self.manager_id,
            "hostname": self.hostname,
            "pid": self.pid,
            "pools": sorted(self.pools),
            "capabilities": sorted(self.capabilities),
            "placement_prefixes": list(self.placement_prefixes),
            "uid": self.uid,
            "started_at": self.started_at,
            "heartbeat_at": self.heartbeat_at,
            "heartbeat_age_seconds": self.heartbeat_age_seconds,
            "end_time": self.end_time,
            "liveness": self.liveness,
            "alive": self.alive(),
        }


def probe_liveness(workspace: Workspace, owner_id: str) -> tuple[_death.Liveness, tuple[_death.Evidence, ...]]:
    """Run the death proof on one owner without side effects (no tombstone, no visibility wait).

    :param workspace: The workspace.
    :param owner_id: The owner id.
    :return: The verdict and its evidence.
    """

    return _death.probe(
        workspace.control / "owners" / owner_id,
        visibility_deadline=0.0,
        scheduler=_death.SchedulerQueries(),
        here=_death.process_identity(),
    )


def _heartbeat(owner: OwnerRecord) -> tuple[str | None, float | None]:
    try:
        document = json.loads((owner.path / "heartbeat.json").read_text(encoding="utf-8"))
        at = document.get("updated_at") if isinstance(document, dict) else None
        if isinstance(at, str):
            return at, max(0.0, time.time() - timestamp_seconds(at))
    except (OSError, ValueError):
        pass
    return None, None


def _manager_record(workspace: Workspace, owner: OwnerRecord) -> ManagerRecord:
    record = owner.record or {}
    allocation = record.get("allocation")
    heartbeat_at, age = _heartbeat(owner)
    try:
        uid: int | None = owner.path.stat().st_uid
    except OSError:
        uid = None
    hostname, pid, started = record.get("hostname"), record.get("pid"), record.get("started_at")
    return ManagerRecord(
        manager_id=owner.owner_id,
        hostname=hostname if isinstance(hostname, str) else None,
        pid=pid if isinstance(pid, int) and not isinstance(pid, bool) else None,
        pools=frozenset(_strings(record.get("pools"))),
        capabilities=frozenset(_strings(record.get("capabilities"))),
        placement_prefixes=_strings(record.get("prefixes")),
        started_at=started if isinstance(started, str) else None,
        heartbeat_at=heartbeat_at,
        heartbeat_age_seconds=age,
        liveness=probe_liveness(workspace, owner.owner_id)[0].value,
        uid=uid,
        end_time=_number(record.get("end_time"))
        or (_number(allocation.get("end_time")) if isinstance(allocation, Mapping) else None),
    )


def read_managers(workspace: Workspace) -> list[ManagerRecord]:
    """Return every registered manager (``owner.json`` of kind ``manager``), each probed for liveness.

    :param workspace: The workspace.
    :return: The managers, by owner id.
    """

    return [
        _manager_record(workspace, owner)
        for owner in _kernel.list_owners(workspace)
        if owner.record is not None and owner.record.get("kind") == "manager"
    ]


@dataclass(frozen=True)
class ClaimRequirements:
    """What one job demands of any manager that claims it.

    :param pool: The claim pool.
    :param capabilities: The required capabilities.
    """

    pool: str
    capabilities: frozenset[str]

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of these requirements.

        :return: The mapping.
        """

        return {"claim_pool": self.pool, "required_capabilities": sorted(self.capabilities)}


def claim_requirements(job: JobDefinition) -> ClaimRequirements:
    """Return the claim preconditions *job* imposes on a manager.

    :param job: The job.
    :return: Its requirements.
    """

    return ClaimRequirements(pool=job.claim_pool, capabilities=job.required_capabilities)


def _placement_covered(record: ManagerRecord, placement: str) -> bool:
    parts = normalize_placement(placement).parts
    return any(
        parts[: len(prefix)] == prefix for prefix in (normalize_placement(p).parts for p in record.placement_prefixes)
    )


def manager_refusals(
    record: ManagerRecord,
    requirements: ClaimRequirements,
    *,
    placement: str | None = None,
    owner_uid: int | None = None,
    job: JobDefinition | None = None,
) -> list[str]:
    """Return why *record* would not claim a job with *requirements*.

    :param record: The manager.
    :param requirements: The pool and capabilities the job demands.
    :param placement: The job placement, when a placement restriction applies.
    :param owner_uid: The job directory's owning uid, when ownership is checked.
    :param job: The job definition, when its time requirement is checked against the allocation end.
    :return: One human-readable reason per unmet precondition.
    """

    reasons: list[str] = []
    if owner_uid is not None and record.uid is not None and record.uid != owner_uid:
        reasons.append(f"is owned by another user (uid {owner_uid}); managers run only jobs owned by their account")
    if requirements.pool not in record.pools:
        reasons.append(f"does not serve claim pool {requirements.pool}")
    if missing := requirements.capabilities - record.capabilities:
        reasons.append(f"lacks capabilities {','.join(sorted(missing))}")
    if placement is not None and record.placement_prefixes and not _placement_covered(record, placement):
        reasons.append(f"does not scan placement {placement} (restricted to {','.join(record.placement_prefixes)})")
    if job is not None and record.end_time is not None:
        # ponytail: reads the initial step's mintime, then the job's; a job past its first step may need another.
        step = job.step_resources.get(job.initial_step)
        mintime = step.get("mintime") if isinstance(step, Mapping) else None
        mintime = job.resources.get("mintime", 0) if mintime is None else mintime
        left = record.end_time - time.time()
        if left <= 0:
            reasons.append("its allocation has ended")
        elif isinstance(mintime, int) and mintime > left:
            reasons.append(
                f"its allocation ends in {format_duration(int(left))}; the job needs mintime {format_duration(mintime)}"
            )
    return reasons


@dataclass(frozen=True)
class BudgetStatus:
    """The attempt and activation budgets of one job against what it consumed.

    :param attempts_this_activation: Attempts of the current activation.
    :param maximum_attempts_per_activation: Its limit, or ``None``.
    :param total_attempts: Attempts in total.
    :param maximum_total_attempts: Its limit, or ``None``.
    :param activations: Activations in total.
    :param maximum_activations: Its limit, or ``None``.
    :param declared: The three limits as ``job.json`` declares them, by name; the limits above are effective.
    :param budget_override: The operator's ``set_budget`` override from ``state.json``, or ``None``.
    """

    attempts_this_activation: int
    maximum_attempts_per_activation: int | None
    total_attempts: int
    maximum_total_attempts: int | None
    activations: int
    maximum_activations: int | None
    declared: Mapping[str, int | None]
    budget_override: Mapping[str, int | None] | None

    @property
    def attempt_budget_exhausted(self) -> bool:
        """Whether one more attempt exceeds a budget."""

        per_activation = self.maximum_attempts_per_activation
        if per_activation is not None and self.attempts_this_activation + 1 > per_activation:
            return True
        total = self.maximum_total_attempts
        return total is not None and self.total_attempts + 1 > total

    @property
    def activation_budget_exhausted(self) -> bool:
        """Whether one more activation exceeds its budget."""

        maximum = self.maximum_activations
        return maximum is not None and self.activations + 1 > maximum

    def describe(self) -> list[str]:
        """Describe every budget as ``consumed/limit`` for an operator, with the declared limit it overrides.

        :return: One line per budget.
        """

        def limit(name: str, value: int | None) -> str:
            text = "unlimited" if value is None else str(value)
            declared = self.declared.get(name)
            if declared != value:
                text += f" (declared {'unlimited' if declared is None else declared})"
            return text

        return [
            f"attempts this activation {self.attempts_this_activation}/"
            + limit("maximum_attempts_per_activation", self.maximum_attempts_per_activation),
            f"total attempts {self.total_attempts}/" + limit("maximum_total_attempts", self.maximum_total_attempts),
            f"activations {self.activations}/" + limit("maximum_activations", self.maximum_activations),
        ]

    def set_budget_option(self) -> str:
        """Name the ``job request set_budget`` options that raise the exhausted attempt budgets.

        :return: ``--max-attempts-per-activation N``, ``--max-total-attempts N``, or both when both are exhausted.
        """

        per_activation, total = self.maximum_attempts_per_activation, self.maximum_total_attempts
        options = []
        if per_activation is not None and self.attempts_this_activation + 1 > per_activation:
            options.append("--max-attempts-per-activation N")
        if total is not None and self.total_attempts + 1 > total:
            options.append("--max-total-attempts N")
        return " ".join(options)

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of these budgets.

        :return: The mapping.
        """

        return {
            "attempts_this_activation": self.attempts_this_activation,
            "maximum_attempts_per_activation": self.maximum_attempts_per_activation,
            "total_attempts": self.total_attempts,
            "maximum_total_attempts": self.maximum_total_attempts,
            "activations": self.activations,
            "maximum_activations": self.maximum_activations,
            "attempt_budget_exhausted": self.attempt_budget_exhausted,
            "activation_budget_exhausted": self.activation_budget_exhausted,
            "declared": dict(self.declared),
            "budget_override": None if self.budget_override is None else dict(self.budget_override),
        }


def _count(mapping: Mapping[str, object] | None, key: str) -> int:
    value = None if mapping is None else mapping.get(key)
    return value if type(value) is int else 0


def budget_status(job: JobDefinition, doc: StateDoc | None) -> BudgetStatus:
    """Return what *job* consumed of its budgets according to its ``state.json``.

    :param job: The job.
    :param doc: Its state, or ``None`` before its first claim.
    :return: The budget status.
    """

    declared = job.retry_policy
    # StateDoc validated the override's shape, and overridden() checks it again.
    override = cast(Mapping[str, int | None] | None, None if doc is None else doc.budget_override)
    policy = declared.overridden(override)
    return BudgetStatus(
        attempts_this_activation=_count(None if doc is None else doc.attempt, "ordinal"),
        maximum_attempts_per_activation=policy.maximum_attempts_per_activation,
        total_attempts=_count(None if doc is None else doc.counters, "attempts_total"),
        maximum_total_attempts=policy.maximum_total_attempts,
        activations=_count(None if doc is None else doc.counters, "activations"),
        maximum_activations=policy.maximum_activations,
        declared={name: getattr(declared, name) for name in BUDGET_LIMITS},
        budget_override=override,
    )


def observe_join(workspace: Workspace, join: Mapping[str, object]) -> list[dict[str, object]]:
    """Locate every child one waiting job's join names, reading only.

    A child is looked for at its placement in the unowned states first; one not
    found there is looked for in every ``owned/<id>/``, and only a miss there
    too leaves it unresolved (``kind`` ``None``).

    :param workspace: The workspace.
    :param join: The ``join`` member of the parent's ``state.json``.
    :return: One observation per child: ``label``, ``job_id``, ``job_key``, ``placement``, ``kind``,
        ``terminal`` and ``error``.
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
        job_id, hint = raw.get("job_id"), raw.get("placement_hint")
        ref: JobRef | None = None
        error: str | None = None
        try:
            if not isinstance(job_id, str):
                raise FormatError("the child reference has no job_id")
            placement = normalize_placement(hint) if isinstance(hint, str) else None
            ref = _kernel.locate(workspace, job_id, placement_hint=placement, include_owned=False) or _kernel.locate(
                workspace, job_id, placement_hint=None, include_owned=True
            )
        except (WorkflowError, OSError, ValueError) as exc:
            error = str(exc)
        observations.append(
            {
                "label": raw.get("label"),
                "job_id": job_id,
                "job_key": raw.get("job_key") if ref is None else ref.job_key,
                "placement": hint if isinstance(hint, str) else None,
                "kind": None if ref is None else ref.state,
                "terminal": None if ref is None else ref.state in TERMINAL_STATES,
                "error": error,
            }
        )
    return observations


def _describe_child(observation: Mapping[str, object], *, with_failure: bool = False) -> str:
    label = observation.get("label") or "-"
    identity = observation.get("job_key") or observation.get("job_id") or "-"
    kind = observation.get("kind") or observation.get("state")
    state = "not resolvable in this workspace" if kind is None else str(kind)
    error = observation.get("error")
    suffix = f" ({error})" if isinstance(error, str) and error else ""
    failure = observation.get("failure")
    if with_failure and isinstance(failure, Mapping) and failure.get("code"):
        suffix += f" [{failure.get('code')}]"
    return f"{label}: {identity} is {state}{suffix}"


@dataclass(frozen=True)
class Check:
    """One precondition of progress and whether this job satisfies it.

    :param name: The check.
    :param satisfied: ``True``, ``False``, or ``None`` when informational.
    :param detail: What was found.
    """

    name: str
    satisfied: bool | None
    detail: str

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this check.

        :return: The mapping.
        """

        return {"name": self.name, "satisfied": self.satisfied, "detail": self.detail}


@dataclass(frozen=True)
class Diagnosis:
    """Why one job is or is not making progress, and what to do about it.

    :param job_id: The job UUID.
    :param job_key: The job key.
    :param state: The job's state directory.
    :param summary: One explanatory sentence.
    :param blocked: Whether the job cannot progress without action.
    :param checks: The checks.
    :param hints: Suggested actions.
    """

    job_id: str
    job_key: str
    state: str
    summary: str
    blocked: bool
    checks: tuple[Check, ...] = ()
    hints: tuple[str, ...] = ()

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this diagnosis.

        :return: The mapping.
        """

        return {
            "format": JOB_DIAGNOSIS_FORMAT,
            "format_version": 3,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "state": self.state,
            "summary": self.summary,
            "blocked": self.blocked,
            "checks": [check.as_mapping() for check in self.checks],
            "hints": list(self.hints),
        }

    def render(self) -> str:
        """Render this diagnosis for a terminal.

        :return: The text.
        """

        marks = {True: "ok  ", False: "no  ", None: "?   "}
        lines = [f"job {self.job_key} is {self.state}", self.summary]
        lines += [f"  {marks[check.satisfied]}{check.name}: {check.detail}" for check in self.checks]
        lines += [f"  -> {hint}" for hint in self.hints]
        return "\n".join(lines)


@dataclass
class _Diagnosing:
    checks: list[Check] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)

    def check(self, name: str, satisfied: bool | None, detail: str) -> None:
        self.checks.append(Check(name=name, satisfied=satisfied, detail=detail))

    def hint(self, text: str) -> None:
        self.hints.append(text)


def _workflow_checks(
    workspace: Workspace, job: JobDefinition, doc: StateDoc | None, payload: Path, report: _Diagnosing
) -> bool:
    """Record whether the job's effective workflow and its calls are installed and built here, and its ``requires``
    met; a carried workflow's ``workflow/`` is checked (read only) against ``job.json`` as a launch checks it."""

    workflow_id, _name = effective_workflow(job, doc)
    carried = _store.carried(job, payload, require_tree=False)
    try:
        closure = _store.closure(workspace, workflow_id, check_builds=True, also=bound_workflows(job))
        installed = carried or _store.lookup(workspace, workflow_id)
    except (RunnerResolutionError, ValueError) as exc:
        report.check("installed workflow", False, f"workflow {workflow_id} cannot be checked: {exc}")
        return False
    tree_ok = True
    if carried is not None:
        tree_sha256 = str(carried.record.get("tree_sha256"))
        if doc is not None and doc.unlocked is not None:
            report.check("carried workflow", None, "the job is unlocked: its workflow/ launches unverified")
        else:
            problem = carried_problem(payload / _store.RUNNER_COPY, tree_sha256)
            tree_ok = problem is None
            report.check(
                "carried workflow", tree_ok, problem or f"workflow/ has the digest {tree_sha256[:12]} job.json froze"
            )
    binding = None if installed is None else binding_problem(job, installed.id, installed.record)
    if binding is not None:
        report.check("call slot bindings", False, binding)
    if closure.missing:
        report.check("installed workflow", False, f"not installed in this workspace: {', '.join(closure.missing)}")
        report.hint("install it with 'httk workflow install --workspace WORKSPACE SOURCE'")
    if closure.unbuilt:
        report.check("built workflow", False, f"not built for this platform: {', '.join(closure.unbuilt)}")
        report.hint("build it with 'httk workflow build --workspace WORKSPACE WORKFLOW'")
    if closure.ok:
        installed_text = "its calls are" if carried is not None else f"{workflow_id} and its calls are"
        report.check("installed workflow", True, f"{installed_text} installed and built here")
    requires = () if installed is None else _strings(installed.record.get("requires"))
    if requires:
        unmet = unmet_job_requirements(requires)
        report.check(
            "required distributions",
            not unmet,
            f"unmet in this process's environment: {'; '.join(unmet)}; a manager claims this job only when its own "
            "environment meets every requirement"
            if unmet
            else f"{', '.join(requires)} met in this process's environment; each manager re-checks them in its own",
        )
    return closure.ok and binding is None and tree_ok


def _manager_checks(
    workspace: Workspace,
    requirements: ClaimRequirements,
    report: _Diagnosing,
    *,
    claiming: bool,
    placement: str | None,
    owner_uid: int | None,
    job: JobDefinition | None,
) -> None:
    """Record which managers would serve the job; *claiming* also checks pool, capabilities and time."""

    records = read_managers(workspace)
    live = [record for record in records if record.alive()]
    if not live:
        detail = (
            f"{len(records)} manager(s) registered here, all proven dead"
            if records
            else "no manager is registered in this workspace"
        )
        report.check("registered manager", False, detail)
        report.hint("start one with 'httk manager run --workspace WORKSPACE'")
        return
    accepting: list[ManagerRecord] = []
    for record in live:
        reasons = manager_refusals(record, requirements, placement=placement, owner_uid=owner_uid, job=job)
        if not claiming:
            reasons = [reason for reason in reasons if "does not scan placement" in reason or "another user" in reason]
        if reasons:
            report.check("live manager", False, f"{record.describe()} {'; '.join(reasons)}")
        else:
            accepting.append(record)
            report.check("live manager", True, f"{record.describe()} offers everything this job requires")
    demanded = "claim pool, capabilities and placement" if claiming else "placement"
    report.check(
        "eligible manager",
        bool(accepting),
        f"{len(accepting)} of {len(live)} manager(s) not proven dead match the {demanded} of this job"
        if accepting
        else f"no manager that is not proven dead matches the {demanded} of this job",
    )
    if not accepting:
        report.hint(
            f"run a manager that matches, for example 'httk manager run --pool {requirements.pool} --workspace WORKSPACE"
            + "".join(f" --capability {name}" for name in sorted(requirements.capabilities))
            + "'"
        )


def _launch_exhausted(job: JobDefinition, doc: StateDoc | None) -> bool:
    # As the manager's launch: an unstarted attempt is the one launched, otherwise a fresh one is recorded first.
    if doc is None or doc.activation is None:
        return False
    attempt = doc.attempt
    if attempt is None or attempt.get("started_at") is not None:
        doc = doc.next_attempt("launch", unclean=False)
    return attempt_budget_failure(job, doc) is not None


def _budget_checks(job: JobDefinition, doc: StateDoc | None, report: _Diagnosing) -> None:
    budgets = budget_status(job, doc)
    exhausted = _launch_exhausted(job, doc)
    for text in budgets.describe():
        report.check("budget", not exhausted, text)
    if exhausted:
        report.hint(
            "the next claim of this job will fail it with budget_exhausted; raise the limit with 'httk job request "
            f"set_budget --workspace WORKSPACE {budgets.set_budget_option()} --operator NAME --reason WHY JOB'"
        )


def _owner_checks(workspace: Workspace, ref: JobRef, report: _Diagnosing) -> _death.Liveness:
    """Record who owns an owned job and what the death proof says about that owner."""

    owner_id = ref.owner_id or "-"
    owner = next((item for item in _kernel.list_owners(workspace) if item.owner_id == owner_id), None)
    record = None if owner is None else owner.record
    if record is not None:
        _, age = _heartbeat(owner) if owner is not None else (None, None)
        report.check(
            "owner",
            None,
            f"{record.get('kind')} {owner_id} on {record.get('hostname') or 'an unrecorded host'} "
            f"(pid {record.get('pid')}; " + ("no heartbeat" if age is None else f"heartbeat {age:.0f}s ago") + ")",
        )
    verdict, evidence = probe_liveness(workspace, owner_id)
    detail = "; ".join(item.detail for item in evidence) or verdict.value
    if verdict is _death.Liveness.ALIVE:
        report.check("owner liveness", True, f"alive: {detail}")
    elif verdict is _death.Liveness.DEAD:
        report.check("owner liveness", False, f"dead: {detail}; the next manager tick recovers this job")
        report.hint("start or keep a manager running so the dead owner's jobs are recovered")
    else:
        report.check("owner liveness", None, f"cannot be proven alive or dead from this host: {detail}")
        report.hint(
            "probe the owner from its own host (a manager or 'httk workspace status' there recovers it if it is "
            "dead), or attest it dead with 'httk workspace attest-dead' when you know it is gone"
        )
    return verdict


def _continue_checks(
    job: JobDefinition | None, doc: StateDoc | None, report: _Diagnosing, *, verb: str = "continue"
) -> None:
    # verb is "restart" for a parent that failed at its wait, whose continue reopens the wait instead.
    name = f"operator {verb}"
    if doc is None or doc.activation is None:
        report.check(name, False, f"this job has no recorded activation, so '{verb}' is refused")
        return
    if job is None:
        report.check(name, None, "the job definition is unreadable, so its budget is unknown")
        return
    budgets = budget_status(job, doc)
    raised, revived = _requests.restored_budget(job, doc, action=verb)
    after = budget_status(job, revived)
    restoring = ", restoring the declared retry allowance" if raised else ""
    report.check(name, True, f"'{verb}' repeats this activation{restoring}: " + "; ".join(after.describe()))
    if _launch_exhausted(job, doc):
        report.hint(
            f"with --keep-budget, '{verb}' would immediately end the job again on its exhausted budget: "
            + "; ".join(budgets.describe())
        )
    failure = doc.failure
    exhausted = failure is not None and failure.get("code") == "budget_exhausted"
    if exhausted and doc.join is None and budgets.activation_budget_exhausted:
        report.hint("with --keep-budget, the rerun step's advance would exceed maximum_activations again")
    if verb == "continue":
        report.hint("resume it with 'httk job request continue --workspace WORKSPACE --operator NAME --reason WHY JOB'")


def _history_check(ref: JobRef, report: _Diagnosing) -> None:
    """Fold the owner run log into one attempt-history line."""

    attempts: set[str] = set()
    activations: set[str] = set()
    recovered = 0
    step: str | None = None
    for event in job_events(ref):
        if event.get("event") == "attempt_started":
            attempts.add(str(event.get("attempt_id")))
            activations.add(str(event.get("activation_id")))
            step = event.get("step") if isinstance(event.get("step"), str) else step
        elif event.get("event") == "recovered":
            recovered += 1
    if attempts:
        report.check(
            "attempt history",
            None,
            f"{len(attempts)} attempts across {len(activations)} activations, last at step {step or '-'!r}; "
            f"{recovered} recoveries after a lost owner",
        )


def _flapping_check(job: JobDefinition | None, doc: StateDoc | None, report: _Diagnosing) -> None:
    if job is None:
        return
    budgets = budget_status(job, doc)
    unlimited = budgets.maximum_attempts_per_activation is None and budgets.maximum_total_attempts is None
    attempts = max(budgets.total_attempts, budgets.attempts_this_activation)
    if unlimited and attempts > FLAPPING_ATTEMPTS:
        report.check(
            "flapping",
            False,
            f"this job has attempted {attempts} times under an unlimited budget; it is flapping, not progressing "
            "— consider maximum_attempts_per_activation or retry_on",
        )


def _request_checks(workspace: Workspace, ref: JobRef, doc: StateDoc | None, report: _Diagnosing) -> None:
    """Surface the pending operator requests: files ``requests/<job-uuid>.<request-uuid>.json`` not yet applied."""

    applied = set() if doc is None else set(doc.applied_requests)
    for path in sorted((workspace.control / "requests").glob(f"{ref.job_id}.*.json")):
        try:
            request = _requests.parse(path)
        except (FormatError, OSError) as exc:
            report.check("pending request", False, f"{path.name} is unusable: {exc}")
            continue
        if request.request_id in applied:
            continue
        operator = request.document.get("operator") or "-"
        report.check(
            "pending request",
            None,
            f"a {request.action} request from {operator} waits; the job's next owner applies it at a boundary",
        )


def _breadcrumb_check(control: Path | None, report: _Diagnosing) -> None:
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


def _retained_log_path(ref: JobRef, doc: StateDoc | None) -> Path:
    """Return the stdio path a failure names, if valid, else ``logs/stdio.out``."""

    failure = None if doc is None else doc.failure
    details = failure.get("details") if failure is not None else None
    paths = details.get("log_paths") if isinstance(details, Mapping) else None
    for value in _strings(paths):
        relative = Path(value)
        if not relative.is_absolute() and ".." not in relative.parts:
            return ref.path / relative
    return ref.path / "logs" / "stdio.out"


def explain_job(workspace: Workspace, ref: JobRef) -> Diagnosis:
    """Explain why one job is, or is not, making progress.

    :param workspace: The workspace.
    :param ref: The job.
    :return: The diagnosis.
    """

    doc, state_error = read_state(ref)
    job, job_error = read_job(ref)
    report = _Diagnosing()
    if state_error is not None:
        report.check("state.json", False, state_error)
    if job_error is not None:
        report.check("job definition", False, job_error)
        report.hint("repair or delete the job: nothing schedules a job whose job.json it cannot read")
    if doc is not None and doc.unlocked is not None:
        report.check(
            "debug mode",
            None,
            "this job is unlocked: its integrity policies are lifted, for good, and `httk collect` collects it only "
            "with --force",
        )
    control = attempt_control(ref, doc)
    placement = job_placement(ref)
    placement_name = None if placement is None else placement_text(placement)
    try:
        owner_uid: int | None = ref.path.lstat().st_uid
    except OSError:
        owner_uid = None
    state = ref.state
    blocked = state not in {OWNED, "succeeded"}
    summary = f"state {state}"

    def manager_checks(*, claiming: bool) -> None:
        if job is not None:
            requirements = claim_requirements(job)
            if claiming:
                report.check("claim pool", None, f"this job asks for pool {requirements.pool}")
                report.check(
                    "required capabilities",
                    None,
                    ",".join(sorted(requirements.capabilities)) or "this job requires no capability",
                )
            _manager_checks(
                workspace,
                requirements,
                report,
                claiming=claiming,
                placement=placement_name,
                owner_uid=owner_uid,
                job=job,
            )

    if state == "ready":
        summary = "this job is ready and waiting to be claimed; every claim precondition is listed below"
        if job is not None:
            _workflow_checks(workspace, job, doc, ref.path, report)
            manager_checks(claiming=True)
            _budget_checks(job, doc, report)
        _history_check(ref, report)
        _flapping_check(job, doc, report)
        _request_checks(workspace, ref, doc, report)
    elif state == OWNED:
        verdict = _owner_checks(workspace, ref, report)
        phase = "idle" if doc is None else str(doc.phase.get("kind"))
        report.check(
            "phase",
            None,
            f"{phase}" + ("" if doc is None or doc.attempt is None else f", attempt {doc.attempt.get('id')}"),
        )
        if verdict is _death.Liveness.ALIVE:
            summary = f"this job is owned by a live owner (phase {phase}), so it is progressing"
            blocked = False
        elif verdict is _death.Liveness.DEAD:
            summary = (
                "this job's owner is dead; the next manager tick recovers it, so it is recovered rather than stuck"
            )
            blocked = True
        else:
            summary = "this job's owner cannot be proven alive or dead from this host"
            blocked = False
        if phase == "running":
            report.check("attempt logs", None, f"the job writes {_retained_log_path(ref, doc)}")
        _history_check(ref, report)
        _flapping_check(job, doc, report)
        _request_checks(workspace, ref, doc, report)
    elif state == "waiting":
        join = None if doc is None else doc.join
        observations = [] if join is None else observe_join(workspace, join)
        condition = "-" if join is None else join.get("condition", "-")
        next_step = None if join is None else join.get("next_step")
        report.check("join condition", None, f"{condition} then step {next_step or '-'}")
        for item in observations:
            report.check("join child", bool(item.get("terminal")), _describe_child(item))
        unresolved = [item for item in observations if item.get("kind") is None]
        pending = [item for item in observations if not item.get("terminal")]
        if unresolved:
            summary = (
                f"this job waits on {len(unresolved)} child(ren) that cannot be found in this workspace; a manager "
                "fails it with dependency_failure once they stay unresolvable past its join grace"
            )
        elif pending:
            summary = f"this job waits for {len(pending)} of {len(observations)} child(ren) to become terminal"
            report.hint("inspect a pending child with 'httk job why --workspace WORKSPACE CHILD_JOB'")
        elif observations:
            summary = "every join child is terminal, so the next manager pass resolves this join"
            blocked = False
        else:
            summary = "this job waits on a join that names no readable child"
        manager_checks(claiming=False)
    elif state == "failed":
        failure = None if doc is None else doc.failure
        join = None if doc is None else doc.join
        if failure is not None:
            report.check("failure", False, f"{failure.get('code')}: {failure.get('message')}")
            details = failure.get("details")
            if isinstance(details, Mapping) and details:
                report.check("failure details", None, json.dumps(details, sort_keys=True, default=dict))
            failed = f"this job failed with {failure.get('code')}"
            if join is not None and failure.get("code") == "dependency_failure":
                failed += " while waiting on its children"
            summary = f"{failed} and stays failed until an operator resumes it"
        else:
            summary = "this job failed without a readable failure record"
        for observed in () if doc is None else doc.observations:
            if observed.get("state") != "succeeded":
                report.check("dependency child", False, _describe_child(observed, with_failure=True))
        _breadcrumb_check(control, report)
        report.check("attempt logs", None, f"the job wrote {_retained_log_path(ref, doc)}")
        if join is not None and failure is not None and failure.get("code") == "budget_exhausted":
            # The join was decided, but its next activation exceeds maximum_activations: continue re-decides it.
            children = join.get("children")
            count = len(children) if isinstance(children, tuple) else 0
            after = "the declared one"
            if job is not None and doc is not None:
                _, revived = _requests.restored_budget(job, doc, action="continue", reopen=True)
                maximum = budget_status(job, revived).maximum_activations
                after = "unlimited" if maximum is None else str(maximum)
            report.check(
                "wait",
                None,
                f"the wait on {count} children can be reopened with `httk job request continue`, which restores the "
                f"declared retry allowance (maximum_activations {after} after it)",
            )
            report.hint(
                "'httk job request continue --workspace WORKSPACE --operator NAME --reason WHY JOB' reopens the wait; "
                "with --keep-budget, raise maximum_activations first with 'httk job request set_budget --workspace "
                "WORKSPACE --max-activations N --operator NAME --reason WHY JOB'"
            )
        elif join is not None:
            # A kept join makes continue reopen the wait, which no budget binds; restart is budget-bound.
            children = join.get("children")
            count = len(children) if isinstance(children, tuple) else 0
            step = None if doc is None or doc.activation is None else doc.activation.get("step")
            report.check(
                "wait",
                None,
                f"the wait on {count} children can be reopened with `httk job request continue`; "
                f"`restart` reruns step {step or '-'}, which reuses the children that still exist, failed or "
                "cancelled ones included: revive such a child first, or delete it to have the step create a "
                "fresh one",
            )
            _continue_checks(job, doc, report, verb="restart")
        else:
            _continue_checks(job, doc, report)
        _history_check(ref, report)
        _flapping_check(job, doc, report)
        _request_checks(workspace, ref, doc, report)
    elif state == "paused":
        summary = "this job is paused and only an operator request moves it"
        history = () if doc is None else doc.history_tail
        paused = [entry for entry in history if entry.get("event") == "paused"]
        if paused and paused[-1].get("detail") is not None:
            report.check("pause", None, json.dumps(paused[-1]["detail"], sort_keys=True, default=dict))
        if doc is not None and doc.failure is not None:
            report.check("failure", None, f"{doc.failure.get('code')}: {doc.failure.get('message')}")
        _breadcrumb_check(control, report)
        _continue_checks(job, doc, report)
        _request_checks(workspace, ref, doc, report)
    elif state == "succeeded":
        summary = "this job succeeded; nothing is left to run"
    elif state == "cancelled":
        summary = "this job was cancelled by an operator request and is terminal; resubmit it to run it again"
    if state in {"ready", "waiting", "paused"}:
        report.hint("drive it in the foreground with 'httk job debug [--workspace WS] JOB'")
    return Diagnosis(
        job_id=ref.job_id,
        job_key=ref.job_key,
        state=state,
        summary=summary,
        blocked=blocked,
        checks=tuple(report.checks),
        hints=tuple(report.hints),
    )


def explain_held(workspace: Workspace, selector: str) -> list[Diagnosis]:
    """Explain the held jobs *selector* names: jobs in neither state tree, held for a transfer (§9.3).

    :param workspace: The workspace whose ``transfers/outgoing/`` to search.
    :param selector: A job UUID, job key, or a prefix of either.
    :return: One diagnosis per held job it matches; empty when none does.
    """

    found = []
    for hold in _moving.held(workspace):
        destination = hold.destination_locator or "no recorded destination"
        read = read_manifest(Path(hold.path) / "bundle.json")
        for member in read[1].members if read is not None else ():
            if not (member.job_id.startswith(selector) or member.job_key.startswith(selector)):
                continue
            report = _Diagnosing()
            report.check("held bundle", False, f"{hold.path} (transfer {hold.transfer_id}, to {destination})")
            report.check("returns to", None, f"{member.state} at priority {member.priority}")
            report.hint("finish the transfer with `httk job transfer --resume`")
            report.hint(
                f"after checking that {destination} does not have it, take it back with `httk job adopt {hold.path}`"
            )
            summary = f"this job is held for a transfer to {destination}; it is in neither workspace's state trees"
            found.append(
                Diagnosis(
                    member.job_id, member.job_key, "held", summary, True, tuple(report.checks), tuple(report.hints)
                )
            )
    return found
