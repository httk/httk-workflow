"""Private outcome and commit decisions of the task manager (plan §7.1, note §7.1).

A commit is decided once and executed by idempotent steps. :func:`outcome_intent`
and :func:`failure_intent` turn a validated outcome, or a manager-detected
failure, into the ``commit`` intent of ``state.json``; the manager writes it,
applies the job's committed transactions and releases the job to the intent's
``target_state`` with the document :func:`settle` computes. A crash anywhere
leaves the intent (or, after its last write, ``release_to``) for the next
claimant's ``reconcile``, so a decided outcome is never relaunched.

**The owner run log** (``logs/runlog.jsonl``, plan §3.4 and §13.3) is written
only through :meth:`httk.workflow._kernel.OwnedJob.append_log`. Every line is
one JSON object with ``at``, ``event`` and ``owner_id`` (added by the kernel)
and, as relevant, ``attempt_id``, ``activation_id``, ``step``, ``from``,
``to`` and ``detail``. The manager emits, in a job's order of events:

- ``claimed`` (``from``: the unowned state, ``to``: ``owned``);
- ``recovered`` (``detail``: the previous owner, or ``self-healed``) when a
  claim or self-healing finds work a dead owner left unfinished;
- ``attempt_started`` (``attempt_id``, ``activation_id``, ``step``,
  ``detail``: the runner command) before the gate opens, and ``launched``
  (the same members) right after it;
- ``attempt_ended`` (``attempt_id``, ``detail``: the exit status) once the
  attempt and its launches are reaped;
- ``outcome`` (``attempt_id``, ``detail``: the action) when a published
  outcome is accepted;
- ``committed`` (``attempt_id``, ``to``: the target state, ``detail``: the
  action) when the intent is executed;
- ``failed`` (``attempt_id``, ``detail``: the failure code) when a job is
  released to ``failed``;
- ``released`` (``from``, ``to``, ``detail``: the priority) right before the
  directory moves.
"""

import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from . import _fs, _joins
from ._children import ChildPlan, labeled_join
from ._job import JobDefinition
from ._requests import Request
from ._state import StateDoc
from ._util import require_int
from .errors import FormatError
from .models import (
    Failure,
    canonical_uuid,
    parse_job_key,
    parse_placement_text,
    placement_text,
    validate_failure,
    validate_label,
    validate_resources,
    validate_step,
)

_LOGGER = logging.getLogger("httk.workflow.manager")
_OUTCOME_LIMIT = 1 << 20
# A malformed or partial outcome is the signature of an outcome assembled in
# place instead of published by an atomic rename, so the remedy is attached to
# the parse and shape errors, not to the identity comparisons.
_ASSEMBLY_REMEDY = (
    "; an outcome must be published by renaming a staged directory onto outcome.ready, never assembled in place"
)


def failure(code: str, message: str, *, exit_status: int | None = None) -> dict[str, object]:
    """Return one canonical manager failure object.

    :param code: The failure code.
    :param message: The message.
    :param exit_status: The attempt's exit status, if known.
    :return: The failure mapping.
    """

    details: dict[str, object] = {"log_paths": ["logs/stdio.out"]}
    if exit_status is not None:
        details["exit_status"] = exit_status
    return Failure(code, message, details=details).as_mapping()


def nested_reason(outcome: Mapping[str, Any], key: str) -> str:
    """Return ``outcome[key]["reason"]``.

    :param outcome: The outcome.
    :param key: The action member.
    :return: The reason.
    :raises httk.workflow.errors.FormatError: Without a string reason.
    """

    value = outcome.get(key)
    if not isinstance(value, Mapping) or not isinstance(value.get("reason"), str):
        raise FormatError(f"{key} outcome requires a reason")
    return str(value["reason"])


def _count(mapping: Mapping[str, object] | None, key: str) -> int:
    value = None if mapping is None else mapping.get(key)
    return value if type(value) is int else 0


def attempt_budget_failure(job: JobDefinition, doc: StateDoc) -> str | None:
    """Return why the job's current attempt may not start, or ``None`` within its budgets.

    :param job: The job.
    :param doc: Its state, whose ``attempt`` is the one about to start.
    :return: The exhausted budget, or ``None``.
    """

    per_activation = job.retry_policy.maximum_attempts_per_activation
    if per_activation is not None and _count(doc.attempt, "ordinal") > per_activation:
        return "maximum_attempts_per_activation exceeded"
    total = job.retry_policy.maximum_total_attempts
    if total is not None and _count(doc.counters, "attempts_total") > total:
        return "maximum_total_attempts exceeded"
    return None


def retry_budget_available(job: JobDefinition, doc: StateDoc) -> bool:
    """Report whether another attempt of the current activation is permitted.

    :param job: The job.
    :param doc: Its state.
    :return: Whether a retry fits the budgets.
    """

    per_activation = job.retry_policy.maximum_attempts_per_activation
    if per_activation is not None and _count(doc.attempt, "ordinal") >= per_activation:
        return False
    total = job.retry_policy.maximum_total_attempts
    return total is None or _count(doc.counters, "attempts_total") < total


def declared_runner_steps(job_key: str, outcome: Mapping[str, Any]) -> list[str] | None:
    """Return the outcome's valid ``runner_steps``, ignoring (and logging) a malformed one.

    :param job_key: The job key, for the log.
    :param outcome: The outcome.
    :return: The steps, or ``None``.
    """

    declared = outcome.get("runner_steps")
    if declared is None:
        return None
    if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
        _LOGGER.warning("ignoring runner_steps of %s: not an array", job_key)
        return None
    try:
        return [validate_step(item, "runner_steps item") for item in declared]
    except FormatError as exc:
        _LOGGER.warning("ignoring runner_steps of %s: %s", job_key, exc)
        return None


def read_outcome(directory: Path, doc: StateDoc) -> dict[str, Any]:
    """Read and check one published ``outcome.ready/outcome.json`` of a quiescent job.

    :param directory: The ``outcome.ready`` directory (checked to be a real directory by the caller).
    :param doc: The job's state, naming the activation and attempt.
    :return: The outcome document.
    :raises httk.workflow.errors.FormatError: If the outcome is unreadable, malformed, foreign,
        a symlink, special file, or oversized.
    """

    path = directory / "outcome.json"
    try:
        data = _fs.read_bounded(_fs.loc(path), _OUTCOME_LIMIT, nonblock=True)
    except (_fs.UnsafePath, OSError) as exc:
        raise FormatError(f"cannot read {path}: {exc}{_ASSEMBLY_REMEDY}") from exc
    if data is None:
        raise FormatError(f"{path} does not exist{_ASSEMBLY_REMEDY}")
    try:
        outcome = json.loads(data)
    except (ValueError, RecursionError) as exc:
        raise FormatError(f"{path} is not JSON: {exc}{_ASSEMBLY_REMEDY}") from exc
    if not isinstance(outcome, dict):
        raise FormatError(f"{path} is not a JSON object{_ASSEMBLY_REMEDY}")
    if outcome.get("format") != "httk-workflow-outcome" or outcome.get("format_version") != 2:
        raise FormatError(f"outcome must use httk-workflow-outcome version 2{_ASSEMBLY_REMEDY}")
    activation, attempt = doc.activation or {}, doc.attempt or {}
    for key, expected in (
        ("job_id", doc.job_id),
        ("activation_id", activation.get("id")),
        ("attempt_id", attempt.get("id")),
    ):
        if outcome.get(key) != expected:
            raise FormatError(f"outcome {key} is {outcome.get(key)!r} but this attempt is {expected!r}")
    if not isinstance(outcome.get("action"), str):
        raise FormatError(f"outcome action must be a string{_ASSEMBLY_REMEDY}")
    return outcome


def _intent(attempt_id: str, action: str, target: str, priority: int, **members: object) -> dict[str, object]:
    intent: dict[str, object] = {
        "attempt_id": attempt_id,
        "action": action,
        "target_state": target,
        "next_step": None,
        "seal": False,
        "failure": None,
        "priority": priority,
        "resources": None,
        "children": [],
        # Members beyond plan §3.3, read by settle(): why a retried attempt runs and whether it follows an
        # unclean end.
        "reason": None,
        "unclean": False,
    }
    intent.update(members)
    return intent


def _retry(
    job: JobDefinition, doc: StateDoc, attempt_id: str, action: str, reason: str, priority: int, **members: object
) -> dict[str, object]:
    if not retry_budget_available(job, doc):
        exhausted = failure("retry_exhausted", reason)
        return _intent(attempt_id, action, "failed", priority, **{**members, "failure": exhausted, "reason": reason})
    return _intent(attempt_id, action, "ready", priority, **{**members, "reason": reason})


def failure_intent(
    job: JobDefinition,
    doc: StateDoc,
    attempt_id: str,
    code: str,
    message: str,
    *,
    priority: int,
    exit_status: int | None = None,
    unclean: bool = False,
) -> dict[str, object]:
    """Return the intent of a manager-detected failure: a retry when ``retry_on`` and the budgets allow.

    :param job: The job.
    :param doc: Its state.
    :param attempt_id: The failed attempt.
    :param code: The failure code (``process_failure``, ``protocol_error``, ``timeout``, ``owner_lost``, ...).
    :param message: The message.
    :param priority: The priority the job is released with.
    :param exit_status: The exit status, if known.
    :param unclean: Whether the attempt ended uncleanly (a lost owner or a drain).
    :return: The intent.
    """

    recorded = failure(code, message, exit_status=exit_status)
    if code in job.retry_policy.retry_on:
        return _retry(job, doc, attempt_id, "fail", code, priority, failure=recorded, unclean=unclean)
    return _intent(attempt_id, "fail", "failed", priority, failure=recorded, reason=code)


def _join(outcome: Mapping[str, Any], plans: Sequence[ChildPlan], workspace_id: str) -> dict[str, object]:
    """Return the ``state.join`` a ``wait`` outcome records: exact children with placement hints."""

    raw = outcome.get("join")
    if not isinstance(raw, Mapping):
        raise FormatError("wait outcome requires a join object")
    labeled = labeled_join(raw, plans)
    condition = labeled.get("condition")
    if condition not in _joins.CONDITIONS:
        raise FormatError(f"unknown join condition: {condition!r}")
    count = labeled.get("count", 0)
    if type(count) is not int or count < 0 or (condition == "at_least" and count < 1):
        raise FormatError("join count must be a non-negative integer, positive for at_least")
    on_impossible = labeled.get("on_impossible")
    if on_impossible is not None and not isinstance(on_impossible, Mapping):
        raise FormatError("join on_impossible must be an object")
    children = labeled.get("children")
    if not isinstance(children, list) or not children:
        raise FormatError("join children must be a nonempty array")
    references: list[dict[str, object]] = []
    for raw_child in children:
        if not isinstance(raw_child, Mapping):
            raise FormatError("join child must be an object")
        if raw_child.get("workspace_id", workspace_id) != workspace_id:
            raise FormatError("join children must be jobs of this workspace")
        job_id = canonical_uuid(raw_child.get("job_id"), "join child job_id")
        job_key = raw_child.get("job_key")
        if not isinstance(job_key, str) or parse_job_key(job_key)[1] != job_id:
            raise FormatError("join child job_key must carry its job_id")
        hint = parse_placement_text(raw_child.get("placement_hint"), "join child placement_hint")
        label = raw_child.get("label")
        references.append(
            {
                "label": None if label is None else validate_label(label, "join child label"),
                "job_id": job_id,
                "job_key": job_key,
                "placement_hint": placement_text(hint),
            }
        )
    return {
        "children": references,
        "condition": condition,
        "count": count,
        "on_impossible": None if on_impossible is None else dict(on_impossible),
        "next_step": validate_step(outcome.get("next_step"), "next_step"),
    }


def cancel_intent(attempt_id: str, priority: int, request: Request) -> dict[str, object]:
    """Return the intent of an attempt a ``cancel`` request stopped: the job is cancelled, the request applied.

    :param attempt_id: The stopped attempt.
    :param priority: The priority the job is released with.
    :param request: The applied ``cancel`` request; its operator and reason are recorded in the job's history.
    :return: The intent.
    """

    document = request.document
    audit = {"request_id": request.request_id, "action": "cancel", "operator": document["operator"]}
    audit["reason"] = document["reason"]
    if "operator_key" in document:
        audit["operator_key"] = document["operator_key"]
    return _intent(
        attempt_id, "cancel", "cancelled", priority, reason="cancelled", request_id=request.request_id, request=audit
    )


def outcome_intent(
    job: JobDefinition,
    doc: StateDoc,
    outcome: Mapping[str, Any],
    *,
    priority: int,
    plans: Sequence[ChildPlan] = (),
    workspace_id: str,
    seal: bool,
) -> dict[str, object]:
    """Return the commit intent of a validated outcome, carrying its validated children.

    :param job: The job.
    :param doc: Its state.
    :param outcome: The outcome from :func:`read_outcome`.
    :param priority: The job's current priority, kept unless the outcome sets one.
    :param plans: The children :func:`httk.workflow._children.validate_children` accepted.
    :param workspace_id: This workspace, which every join child must belong to.
    :param seal: Whether a ``succeed`` is sealed (decided once, here, so a replay is deterministic).
    :return: The intent.
    :raises httk.workflow.errors.FormatError: For an outcome the manager cannot commit (a protocol error).
    """

    intent = _outcome_intent(job, doc, outcome, priority, plans, workspace_id)
    intent["children"] = [plan.as_mapping() for plan in plans]
    if intent["target_state"] == "succeeded":
        intent["seal"] = seal
    return intent


def _outcome_intent(
    job: JobDefinition,
    doc: StateDoc,
    outcome: Mapping[str, Any],
    priority: int,
    plans: Sequence[ChildPlan],
    workspace_id: str,
) -> dict[str, object]:
    action, attempt_id = str(outcome["action"]), str(outcome["attempt_id"])
    if "priority" in outcome and outcome["priority"] is not None:
        priority = require_int(outcome["priority"], "outcome.priority", maximum=999)
    resources: dict[str, int] | None = None
    if "resources" in outcome:
        if action not in {"advance", "wait"}:
            raise FormatError("outcome resources are only valid for advance or wait")
        resources = validate_resources(outcome["resources"], "outcome.resources")
    if action == "advance":
        next_step = validate_step(outcome.get("next_step"), "next_step")
        maximum = job.retry_policy.maximum_activations
        if maximum is not None and _count(doc.counters, "activations") + 1 > maximum:
            exhausted = failure("budget_exhausted", "maximum_activations exceeded")
            return _intent(attempt_id, action, "failed", priority, failure=exhausted, reason="budget_exhausted")
        return _intent(attempt_id, action, "ready", priority, next_step=next_step, resources=resources)
    if action == "retry":
        return _retry(job, doc, attempt_id, action, nested_reason(outcome, "retry"), priority)
    if action == "wait":
        join = _join(outcome, plans, workspace_id)
        return _intent(attempt_id, action, "waiting", priority, join=join, resources=resources)
    if action == "succeed":
        return _intent(attempt_id, action, "succeeded", priority)
    if action == "fail":
        try:
            declared = validate_failure(outcome.get("failure"))
        except FormatError as exc:
            malformed = failure("protocol_error", f"runner published a malformed failure object: {exc}")
            return _intent(attempt_id, action, "failed", priority, failure=malformed, reason="protocol_error")
        if declared.retryable and retry_budget_available(job, doc):
            return _intent(attempt_id, action, "ready", priority, failure=declared.as_mapping(), reason=declared.code)
        return _intent(attempt_id, action, "failed", priority, failure=declared.as_mapping(), reason=declared.code)
    if action == "pause":
        return _intent(attempt_id, action, "paused", priority, pause=outcome.get("pause"))
    raise FormatError(f"unsupported outcome action: {action!r}")


def settle(doc: StateDoc, intent: Mapping[str, Any]) -> StateDoc:
    """Return the final ``state.json`` an executed intent releases the job with.

    ``advance`` opens the next activation; a retry (``retry``, a retryable
    ``fail`` or a manager-detected failure in ``retry_on``) records the next
    attempt of the same activation, not yet started; ``failed`` records the
    failure. The ``commit`` intent itself is cleared by the release.

    :param doc: The state carrying the intent.
    :param intent: The intent.
    :return: The document to release with.
    """

    doc = doc.with_phase("idle", None)
    target, recorded = intent["target_state"], intent.get("failure")
    if recorded is not None:
        doc = doc.with_failure(recorded)
    if target == "ready" and intent["action"] == "advance":
        doc = doc.with_failure(None).next_activation(intent["next_step"], "advance")
        return doc.updated(resources=intent.get("resources"))
    if target == "ready":
        return doc.next_attempt(str(intent.get("reason") or "retry"), unclean=bool(intent.get("unclean")))
    if target == "succeeded":
        return doc.with_failure(None)
    if target == "waiting":
        return doc.updated(join=intent["join"], resources=intent.get("resources"))
    if target == "paused":
        # The step's pause request (its reason) is evidence for the operator who continues the job.
        return doc.with_history("paused", detail=intent.get("pause"))
    return doc


def decide_join(job: JobDefinition, doc: StateDoc, decision: _joins.JoinDecision) -> tuple[StateDoc, str]:
    """Return a waiting parent's document after its join was decided, and the state to release it to.

    ``satisfied`` opens the activation of the join's ``next_step``; ``impossible``
    opens the ``on_impossible`` step when it is ``{"action": "advance", "next_step": ...}``;
    otherwise, and for ``unresolvable``, the parent fails with ``dependency_failure``.
    Every case records the observations and clears the join.

    :param job: The parent.
    :param doc: Its state, carrying the join.
    :param decision: The decision of :func:`httk.workflow._joins.evaluate`.
    :return: The new document and its target state.
    """

    join = doc.join or {}
    doc = doc.with_observations([item.as_mapping() for item in decision.observations]).updated(join=None)
    step: object = None
    message = f"join child(ren) {', '.join(decision.unresolved)} cannot be found"
    if decision.kind == "satisfied":
        step = join.get("next_step")
    elif decision.kind == "impossible":
        rescue = join.get("on_impossible")
        message = f"the join condition {join.get('condition')} can no longer be satisfied"
        if isinstance(rescue, Mapping) and rescue.get("action") == "advance":
            step = rescue.get("next_step")
    try:
        step = None if step is None else validate_step(step, "join next_step")
    except FormatError as exc:
        step, message = None, f"the join names an invalid step: {exc}"
    if step is None:
        return doc.with_failure(failure("dependency_failure", message)), "failed"
    maximum = job.retry_policy.maximum_activations
    if maximum is not None and _count(doc.counters, "activations") + 1 > maximum:
        return doc.with_failure(failure("budget_exhausted", "maximum_activations exceeded")), "failed"
    return doc.with_failure(None).next_activation(step, "join"), "ready"
