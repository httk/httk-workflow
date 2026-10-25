"""Operator requests (format ``httk-workflow-request``, version 3): posting, parsing and pure application.

A request is a file ``requests/<job-uuid>.<request-uuid>.json``. Only the
job's owner applies it: :func:`apply` decides the :data:`Effect` and the new
``state.json`` without any I/O, and the manager carries the effect out with
exactly one kernel or moving call (plan §7.5). A signature is attribution, not
authorization: an unsigned request applies, one whose signature does not
verify is malformed.
"""

import json
import logging
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from httk.core.identity import sign_document, verify_document

from httk.workflow import _fs, _kernel
from httk.workflow._job import JobDefinition
from httk.workflow._state import TERMINAL_STATES, Release, StateDoc
from httk.workflow._util import json_bytes, require_int, require_string
from httk.workflow.errors import FormatError
from httk.workflow.models import (
    EMBEDDED_WORKFLOW_PREFIX,
    Failure,
    canonical_uuid,
    is_workflow_id,
    parse_placement_text,
    placement_text,
    validate_budget_override,
    validate_step,
)

__all__ = [
    "ACTIONS",
    "MAX_REQUEST_BYTES",
    "REQUEST_FORMAT",
    "REQUEST_FORMAT_VERSION",
    "Defer",
    "Discard",
    "Drop",
    "Effect",
    "Eject",
    "Request",
    "Seal",
    "Unlock",
    "Unseal",
    "apply",
    "operator_key",
    "parse",
    "post",
    "restored_budget",
    "unsealed",
    "validate_envelope",
]

_LOGGER = logging.getLogger(__name__)

REQUEST_FORMAT = "httk-workflow-request"
REQUEST_FORMAT_VERSION = 3
#: The largest accepted request file, in bytes.
MAX_REQUEST_BYTES = 1 << 16
ACTIONS = (
    "cancel",
    "pause",
    "continue",
    "restart",
    "override_step",
    "set_priority",
    "set_budget",
    "set_workflow",
    "detach",
    "eject",
    "delete",
    "seal",
    "unseal",
    "unlock",
)

_REQUIRED = frozenset(
    {"format", "format_version", "request_id", "job_id", "placement", "action", "operator", "reason", "created_at"}
)
#: Each optional member and the actions that may carry it; the first three must carry theirs.
_OPTIONS = {
    "priority": ("set_priority",),
    "step": ("override_step",),
    "budget": ("set_budget",),
    "workflow": ("set_workflow",),
    "destination": ("eject",),
    "force": ("continue", "restart", "override_step"),
    "keep_budget": ("continue", "restart", "override_step"),
    "tree": ("eject",),
    "no_copy": ("unlock",),
}
#: Optional flags: present only as ``true``.
_FLAGS = frozenset({"force", "keep_budget", "tree", "no_copy"})
_SIGNATURE = frozenset({"operator_key", "signature"})
#: The actions that revive a failed or paused job (and may carry ``force`` and ``keep_budget``).
_REVIVALS = ("continue", "restart", "override_step")


@dataclass(frozen=True)
class Request:
    """One parsed, validated request file.

    :param request_id: The request UUID.
    :param job_id: The target job's UUID.
    :param placement: The target's placement (a hint for locating it).
    :param action: One of :data:`ACTIONS`.
    :param document: The whole read-only document.
    :param path: The request file.
    """

    request_id: str
    job_id: str
    placement: PurePosixPath
    action: str
    document: Mapping[str, object]
    path: Path


@dataclass(frozen=True)
class Discard:
    """Delete the job (``OwnedJob.discard``)."""


@dataclass(frozen=True)
class Eject:
    """Eject the owned job to *destination* (``_moving.eject``).

    :param destination: The request's destination.
    :param tree: Eject the job's descendants with it (the request's ``tree``).
    """

    destination: str
    tree: bool = False


@dataclass(frozen=True)
class Defer:
    """Leave the request file; apply it at the job's next boundary."""


@dataclass(frozen=True)
class Drop:
    """The request does not apply: record its id, delete the file, release back to the claimed state.

    :param reason: Why it does not apply.
    """

    reason: str


@dataclass(frozen=True)
class Seal:
    """Seal the job's payload (``seals.seal_payload``), record ``state.json`` ``seal``, then release.

    :param release: Where the job goes afterwards (back to ``succeeded``), with the request id.
    """

    release: Release


@dataclass(frozen=True)
class Unseal:
    """Remove the job's seal document and release the job with the unsealed document.

    :param release: Where the job goes afterwards (back to ``succeeded``), with the request id.
    """

    release: Release


@dataclass(frozen=True)
class Unlock:
    """Release the job with ``state.json`` ``unlocked`` set, after copying its runner when *copy* asks for it.

    :param release: Where the job goes afterwards (where it was claimed from), with the request id.
    :param copy: Copy the effective installation's package to ``workflow/`` in the payload first (unless the request
        carries ``no_copy``).
    """

    release: Release
    copy: bool = True


#: What :func:`apply` decides; :class:`~httk.workflow._state.Release` carries the applied request id.
type Effect = Release | Discard | Eject | Defer | Drop | Seal | Unseal | Unlock


def unsealed(doc: StateDoc | None) -> bool:
    """Report whether an operator released a succeeded job from its seal protection (``job unseal``).

    A succeeded job is never changed by the protocol until then, whether or not it carries a seal.

    :param doc: The job's ``state.json``.
    :return: Whether ``state.json`` records the release.
    """

    return doc is not None and doc.seal is not None and doc.seal.get("released") is True


def validate_envelope(document: Mapping[str, object]) -> None:
    """Check a request document's exact v3 schema (the signature is checked by :func:`operator_key`).

    :param document: The document.
    :raises httk.workflow.errors.FormatError: If a member is missing, unknown, mistyped or not allowed by the action.
    """

    members = set(document)
    if bool(members & _SIGNATURE) and not _SIGNATURE <= members:
        raise FormatError("a request must carry both operator_key and signature, or neither")
    action = document.get("action")
    if action not in ACTIONS:
        raise FormatError(f"request action {action!r} is not one of {', '.join(ACTIONS)}")
    allowed = {name for name, actions in _OPTIONS.items() if action in actions}
    required = _REQUIRED | (allowed - _FLAGS)
    if not required <= members <= required | allowed | _SIGNATURE:
        raise FormatError(f"{action} request members differ: {sorted(members ^ required)}")
    if document["format"] != REQUEST_FORMAT or document["format_version"] != REQUEST_FORMAT_VERSION:
        raise FormatError("request is not httk-workflow-request version 3")
    canonical_uuid(document["request_id"], "request_id")
    canonical_uuid(document["job_id"], "job_id")
    parse_placement_text(document["placement"], "request placement")
    for member in ("operator", "created_at", *sorted((_SIGNATURE | {"destination"}) & members)):
        require_string(document[member], f"request {member}")
    if not isinstance(document["reason"], str):
        raise FormatError("request reason must be a string")
    if "priority" in allowed:
        require_int(document["priority"], "request priority", maximum=999)
    if "step" in allowed:
        validate_step(document["step"], "request step")
    if "budget" in allowed:
        validate_budget_override(document["budget"], "request budget")
    if "workflow" in allowed and (
        not is_workflow_id(target := require_string(document["workflow"], "request workflow"))
        or target.startswith(EMBEDDED_WORKFLOW_PREFIX)
    ):
        # A request names its workflow by installed id only, never by a short name or a job's embedded id.
        raise FormatError(f"request workflow must be an installed workflow id, not {document['workflow']!r}")
    for flag in sorted(_FLAGS & members):
        if document[flag] is not True:
            raise FormatError(f"request {flag} must be true")
    if len(json_bytes(document)) > MAX_REQUEST_BYTES:
        raise FormatError(f"request is larger than {MAX_REQUEST_BYTES} bytes")


def operator_key(document: Mapping[str, object]) -> str | None:
    """Verify a request's optional identity signature, for attribution.

    :param document: The document.
    :return: The verified operator key, or ``None`` for an unsigned request.
    :raises httk.workflow.errors.FormatError: If a signature is present and does not verify.
    """

    signature = verify_document(document)
    if not signature.present:
        _LOGGER.debug("request %s carries no operator signature", document.get("request_id"))
        return None
    if not signature.valid:
        raise FormatError(f"operator signature is invalid: {signature.reason}")
    _LOGGER.info("request %s is signed by %s", document.get("request_id"), signature.operator_key)
    return signature.operator_key


def post(
    workspace: _kernel.KernelWorkspace,
    *,
    action: str,
    job_id: str,
    placement: str | PurePosixPath,
    operator: str,
    reason: str,
    seed_path: Path | None = None,
    **extra: object,
) -> Path:
    """Build, optionally sign, and post a request for one job.

    :param workspace: The workspace.
    :param action: One of :data:`ACTIONS`.
    :param job_id: The target job's UUID.
    :param placement: The target's placement.
    :param operator: The operator label.
    :param reason: The operator's explanation.
    :param seed_path: Sign with this identity seed; ``None`` posts unsigned.
    :param **extra: ``priority``, ``step``, ``budget``, ``workflow``, ``force``, ``keep_budget``, ``destination``,
        ``tree``, ``no_copy`` (``None`` or ``False`` are left out), or a deterministic ``request_id`` and ``created_at``.
    :return: The posted request file.
    :raises httk.workflow.errors.FormatError: If the document would be invalid.
    """

    document: dict[str, object] = {
        "format": REQUEST_FORMAT,
        "format_version": REQUEST_FORMAT_VERSION,
        "request_id": str(uuid.uuid4()),
        "job_id": job_id,
        "placement": placement_text(PurePosixPath(placement)),
        "action": action,
        "operator": operator,
        "reason": reason,
        "created_at": datetime.now(UTC).isoformat(),
        **{name: value for name, value in extra.items() if value is not None and value is not False},
    }
    validate_envelope(document)
    if seed_path is not None:
        document = sign_document(document, seed_path=seed_path)
        operator_key(document)
    return _kernel.post_request(workspace, document)


def parse(path: Path) -> Request:
    """Read and validate one request file: bounded, regular file only, never blocking.

    :param path: The file ``requests/<job-uuid>.<request-uuid>.json``.
    :return: The request.
    :raises FileNotFoundError: If the file is gone.
    :raises httk.workflow.errors.FormatError: If the file is unsafe, malformed, misnamed or wrongly signed.
    """

    try:
        data = _fs.read_bounded(_fs.loc(path), MAX_REQUEST_BYTES, nonblock=True)
    except _fs.UnsafePath as exc:
        raise FormatError(str(exc)) from exc
    if data is None:
        raise FileNotFoundError(path)
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise FormatError(f"{path} is not JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise FormatError(f"{path} is not a JSON object")
    validate_envelope(value)
    if path.name != f"{value['job_id']}.{value['request_id']}.json":
        raise FormatError(f"{path.name} does not name the request's job and request ids")
    operator_key(value)
    return Request(
        request_id=value["request_id"],
        job_id=value["job_id"],
        placement=parse_placement_text(value["placement"]),
        action=value["action"],
        document=MappingProxyType(value),
        path=path,
    )


def _budget_failure(job: JobDefinition, doc: StateDoc) -> Mapping[str, object] | None:
    # The budgets of job.json (as set_budget overrides them) bind manual revival too (legacy retry/advance).
    policy = job.retry_policy.overridden(doc.budget_override)
    attempt, counters, activation = doc.attempt or {}, doc.counters, doc.activation or {}
    checks = (
        ("retry_exhausted", attempt.get("ordinal"), policy.maximum_attempts_per_activation, "attempts per activation"),
        ("retry_exhausted", counters.get("attempts_total"), policy.maximum_total_attempts, "total attempts"),
        ("budget_exhausted", activation.get("ordinal"), policy.maximum_activations, "activations"),
    )
    for code, used, limit, what in checks:
        if limit is not None and isinstance(used, int) and used > limit:
            return Failure(code, f"maximum {what} ({limit}) exceeded").as_mapping()
    return None


def restored_budget(
    job: JobDefinition, doc: StateDoc, *, action: str, reopen: bool = False
) -> tuple[dict[str, int | None], StateDoc]:
    """Raise a job's effective retry limits so a revival again has its declared allowance from this point.

    Each declared limit ``D`` becomes at least ``U + D``, where ``U`` counts the units already started in the
    scope the next unit lands in (an unstarted attempt is that next unit, except for ``override_step``, which
    abandons it while its counters keep it); a limit the operator lifted stays unlimited, and a
    declared-unlimited limit is restored by dropping its ``budget_override`` key.

    :param job: The job's definition.
    :param doc: Its ``state.json`` before the revival's own attempt or activation.
    :param action: The revival: ``continue``, ``restart`` or ``override_step``.
    :param reopen: Whether a ``continue`` reopens a kept join (its next attempt starts a new activation).
    :return: The changed limits (``None`` restores unlimited; empty when none change) and the raised document.
    """

    attempt = doc.attempt or {}
    ordinal, total, activations = (
        value if type(value) is int else 0
        for value in (attempt.get("ordinal"), doc.counters.get("attempts_total"), doc.counters.get("activations"))
    )
    abandons = action == "override_step"
    unstarted = int(not abandons and doc.attempt is not None and attempt.get("started_at") is None)
    # override_step abandons an unstarted attempt or activation, but the counters keep its slot: count it as used.
    idle_activation = not abandons and (doc.attempt is None or (unstarted == 1 and ordinal == 1))
    started = {
        "maximum_attempts_per_activation": 0 if abandons or reopen else max(ordinal - unstarted, 0),
        "maximum_total_attempts": max(total - unstarted, 0),
        "maximum_activations": max(activations - int(idle_activation), 0),
    }
    declared, effective = job.retry_policy, job.retry_policy.overridden(doc.budget_override)
    raised: dict[str, int | None] = {}
    for name, used in started.items():
        limit, current = getattr(declared, name), getattr(effective, name)
        if limit is None and current is not None:
            raised[name] = None  # declared unlimited: restored by dropping the operator's override
        elif limit is not None and current is not None and used + limit > current:
            raised[name] = used + limit
    if not raised:
        return raised, doc
    override = {**(doc.budget_override or {}), **raised}
    kept = {name: value for name, value in override.items() if name not in raised or value is not None}
    return raised, doc.updated(budget_override=kept or None)


def _refusal(job: JobDefinition, doc: StateDoc, from_state: str, action: str, no_copy: bool) -> str | None:
    if from_state == "away" and action != "delete":
        # An away job is an inert copy: only giving it up applies; `job recover` brings it back first.
        return f"the job is away at {doc.away['locator'] if doc.away else '?'}; recover it first"
    terminal = from_state in TERMINAL_STATES
    unlocked = doc.unlocked is not None
    # An unlocked (debug-mode) job may be changed after it succeeded; cancelled stays final.
    settled = ("cancelled",) if unlocked else ("succeeded", "cancelled")
    if (action == "cancel" and terminal) or (action in ("set_budget", "set_workflow") and from_state in settled):
        return f"the job is already {from_state}"
    if action == "unlock":
        live_seal = doc.seal is not None and isinstance(doc.seal.get("sha256"), str)
        if from_state == "cancelled":
            return "the job is already cancelled"
        if from_state == "succeeded" and live_seal:
            return "a sealed succeeded job cannot be unlocked; run `job unseal` first"
        # With a copy asked for (the default), the owner decides once it sees whether the payload has one.
        return "already unlocked" if unlocked and no_copy else None
    if action == "pause" and (terminal or from_state == "paused"):
        return f"the job is already {from_state}"
    if action == "pause" and from_state == "waiting":
        # A paused parent has no wait to reopen: its continue, like a restart or override_step, would rerun the
        # spawning step and orphan the children the join waits on. Only a parent failed at its wait reopens it.
        return "a parent waiting on its children cannot be paused"
    if action in _REVIVALS and from_state not in ("failed", "paused", *(("succeeded",) if unlocked else ())):
        return f"{action} applies to failed or paused jobs, not {from_state}"
    if action == "detach" and (job.parent is None or doc.detached is not None):
        return "the job has no parent link to detach"
    if action == "delete" and not (terminal or from_state in ("paused", "away")):
        return f"only terminal or paused jobs are deleted, not {from_state}"
    if action == "delete" and from_state == "succeeded" and not (unsealed(doc) or unlocked):
        return "a succeeded job is never changed; release it with `job unseal` first"
    if action == "seal" and unlocked:
        return "an unlocked job cannot be sealed"
    if action in ("seal", "unseal") and from_state != "succeeded":
        return f"only succeeded jobs are {action}ed, not {from_state}"
    if action == "seal" and doc.seal is not None and isinstance(doc.seal.get("sha256"), str):
        return "the job is already sealed"
    if action == "unseal" and unsealed(doc):
        return "the job is already unsealed"
    return None


def apply(
    job: JobDefinition,
    doc: StateDoc,
    from_state: str,
    from_priority: int,
    request: Request,
    *,
    refusal: Callable[[Request], str | None] | None = None,
    workflow_name: Callable[[str], str | None] | None = None,
) -> tuple[StateDoc, Effect]:
    """Decide one request for an owned job: the new document and the effect. Pure: no I/O.

    Every effect but :class:`Defer` records the request id in the document's
    ``applied_requests`` and a ``request_applied`` or ``request_dropped`` entry
    in ``history_tail``; a :class:`~httk.workflow._state.Release` also carries the id.
    A request that applies while an attempt is live (``phase`` not ``idle``) is
    deferred: for ``cancel`` the manager signals the attempt meanwhile.
    ``continue`` of a parent that failed at its wait (its join kept) reopens the
    wait; otherwise it, and always ``restart``, retries the activation, while
    ``override_step`` opens a new one; both drop a kept join. A revival without
    ``keep_budget`` first raises ``budget_override`` so that each declared limit
    again allows its declared number of units from this point (a declared
    unlimited one is lifted again); history records the raised limits.
    ``set_budget`` merges its limits into ``state.json`` ``budget_override``,
    which overrides ``job.json``'s ``retry_policy`` at every budget check, and
    releases the job to the state and priority it was claimed from.
    ``set_workflow`` likewise records its installed workflow id, with the
    installation's name, as ``state.json`` ``workflow_override`` (the workflow
    every later attempt runs instead of ``job.json``'s) and clears
    ``runner_steps``, which described the previous runner; history records the
    previous and the new id.
    ``seal`` (a repair verb for a succeeded job without a seal) and ``unseal``
    (the operator's release of a succeeded job, after which ``delete`` applies)
    need the payload, so their effects are executed by the owner.
    ``unlock`` sets the one-way ``state.json`` ``unlocked`` marker (debug mode)
    and leaves the job where it is; without ``no_copy`` the owner first copies the
    job's runner into its payload. An unlocked job may also be revived, have
    its budget or workflow set, and be deleted after it succeeded, and is never
    sealed.

    :param job: The job's definition.
    :param doc: Its current ``state.json``.
    :param from_state: The unowned state the job was claimed from.
    :param from_priority: The priority it was claimed with.
    :param request: The request, for this job.
    :param refusal: The caller's check for ``continue``, ``restart``, ``override_step`` (the revival guard),
        ``delete`` (the waiting-parent check), ``eject`` (the destination check) and ``set_workflow`` (the
        compatibility check): why the request must not apply, or ``None``. A reason drops the request, unless
        a revival carries ``force``.
    :param workflow_name: The short name of the installation with exactly this id, or ``None`` when none is
        installed; a ``set_workflow`` without it is dropped.
    :return: The new document and the effect.
    :raises ValueError: If the request, document and definition name different jobs.
    """

    if not request.job_id == doc.job_id == job.id:
        raise ValueError(f"request for {request.job_id} applied to {doc.job_id} ({job.id})")
    action, document = request.action, request.document
    audit: dict[str, object] = {
        "request_id": request.request_id,
        "action": action,
        "operator": document["operator"],
        "reason": document["reason"],
        "from": from_state,
    }
    if "operator_key" in document:
        audit["operator_key"] = document["operator_key"]
    if action == "set_workflow":
        # Recorded whether the switch applies or is dropped.
        previous = job.workflow_id if doc.workflow_override is None else doc.workflow_override["id"]
        audit["workflow"] = {"from": previous, "to": document["workflow"]}
    reason = _refusal(job, doc, from_state, action, document.get("no_copy") is True)
    daemon_pinned = isinstance(document["operator"], str) and document["operator"].startswith("daemon:")
    checked = daemon_pinned or (reason is None and action in (*_REVIVALS, "delete", "eject", "set_workflow"))
    if checked and refusal is not None and (hazard := refusal(request)) is not None:
        if daemon_pinned:
            reason = hazard
        elif document.get("force") is not True:
            forceable = action in _REVIVALS
            reason = f"{hazard}; republish with force to accept the hazard" if forceable else hazard
        else:
            _LOGGER.warning("forced revival of %s: %s", job.job_key, hazard)
            audit["revival_hazard"] = hazard
    applied = (*doc.applied_requests, request.request_id)
    switched: dict[str, str] | None = None
    if reason is None and action == "set_workflow":
        target_id = require_string(document["workflow"], "request workflow")
        name = None if workflow_name is None else workflow_name(target_id)
        if name is None:
            reason = f"workflow {target_id} is not installed in the job's workspace"
        else:
            switched = {"id": target_id, "name": name}
    if reason is not None:
        dropped = doc.with_history("request_dropped", **audit, note=reason).updated(applied_requests=applied)
        return dropped, Drop(reason)
    if doc.phase["kind"] != "idle":
        return doc, Defer()
    new, target, priority = doc, from_state, from_priority
    if action == "cancel":
        target = "cancelled"
    elif action == "pause":
        target = "paused"
    elif action == "set_priority":
        priority = require_int(document["priority"], "request priority", maximum=999)
    elif action == "set_budget":
        # Merged: successive requests on different limits compose; a later value for the same limit wins.
        budget = validate_budget_override(document["budget"], "request budget")
        new = doc.updated(budget_override={**(doc.budget_override or {}), **budget})
        audit["budget"] = budget
    elif action == "set_workflow":
        assert switched is not None
        # Switching back to job.json's workflow clears the override. The recorded runner steps described the
        # previous runner; the new one records its own.
        override = None if switched["id"] == job.workflow_id else switched
        new = doc.updated(workflow_override=override, runner_steps=None)
    elif action == "detach":
        new = doc.updated(detached={"at": datetime.now(UTC).isoformat(), "operator": document["operator"]})
    elif action == "unlock":
        # One-way: a second unlock (asking for a runner copy) keeps the first marker.
        if doc.unlocked is None:
            marker = {"at": datetime.now(UTC).isoformat(), "operator": document["operator"]}
            new = doc.updated(unlocked={**marker, "reason": document["reason"]} if document["reason"] else marker)
        audit["unlocked"] = True
    elif action in _REVIVALS:
        reopen = action == "continue" and from_state == "failed" and doc.join is not None
        raised: dict[str, int | None] = {}
        revived = doc
        if document.get("keep_budget") is not True:
            raised, revived = restored_budget(job, doc, action=action, reopen=reopen)
        if raised:
            audit["budget"] = raised
        if reopen:
            # A parent that failed at its wait (dependency_failure keeps the join): reopen the wait, rerun nothing.
            new, target = revived.with_failure(None), "waiting"
        else:
            new, target = revived.with_failure(None).updated(join=None), "ready"
            if action == "override_step":
                new = new.next_activation(validate_step(document["step"]), "override_step")
            elif doc.attempt is not None and doc.attempt.get("started_at") is not None:
                # Only a started attempt is retried; an unstarted (or absent) one is simply launched from ready.
                new = new.next_attempt("manual_continue", unclean=False)
            if (failure := _budget_failure(job, new)) is not None:
                # Reachable only with keep_budget (a raise always fits the next unit): today's no-raise refusal.
                new, target = doc.with_failure(failure), "failed"
    elif action == "unseal":
        # The release that makes `delete` apply; the owner removes the seal document.
        released = {"released": True, "at": datetime.now(UTC).isoformat(), "request_id": request.request_id}
        new = doc.updated(seal=released).with_history("unsealed", reason="unseal")
    audit["to"] = target
    new = new.with_history("request_applied", **audit).updated(applied_requests=applied)
    if action == "eject":
        return new, Eject(require_string(document["destination"], "request destination"), document.get("tree") is True)
    if action == "delete":
        return new, Discard()
    release = Release(target, priority, (request.request_id,))
    if action == "seal":
        return new, Seal(release)
    if action == "unseal":
        return new, Unseal(release)
    if action == "unlock":
        return new, Unlock(release, document.get("no_copy") is not True)
    return new, release
