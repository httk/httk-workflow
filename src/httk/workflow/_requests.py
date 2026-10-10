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
from httk.workflow.models import Failure, canonical_uuid, parse_placement_text, placement_text, validate_step

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
    "Unseal",
    "apply",
    "operator_key",
    "parse",
    "post",
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
    "override_step",
    "set_priority",
    "detach",
    "eject",
    "delete",
    "seal",
    "unseal",
)

_REQUIRED = frozenset(
    {"format", "format_version", "request_id", "job_id", "placement", "action", "operator", "reason", "created_at"}
)
#: Each optional member and the actions that may carry it; the first two must carry theirs.
_OPTIONS = {
    "priority": ("set_priority",),
    "step": ("override_step",),
    "destination": ("eject",),
    "force": ("continue", "override_step"),
}
_SIGNATURE = frozenset({"operator_key", "signature"})


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
    """

    destination: str


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


#: What :func:`apply` decides; :class:`~httk.workflow._state.Release` carries the applied request id.
type Effect = Release | Discard | Eject | Defer | Drop | Seal | Unseal


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
    required = _REQUIRED | {name for name in allowed if name != "force"}
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
    if "force" in members and document["force"] is not True:
        raise FormatError("request force must be true")
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
    :param **extra: ``priority``, ``step``, ``force``, ``destination`` (``None`` or ``False`` are left out), or a
        deterministic ``request_id``.
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
    # The budgets of job.json bind manual revival too (legacy retry/advance).
    policy, attempt, counters, activation = job.retry_policy, doc.attempt or {}, doc.counters, doc.activation or {}
    checks = (
        ("retry_exhausted", attempt.get("ordinal"), policy.maximum_attempts_per_activation, "attempts per activation"),
        ("retry_exhausted", counters.get("attempts_total"), policy.maximum_total_attempts, "total attempts"),
        ("budget_exhausted", activation.get("ordinal"), policy.maximum_activations, "activations"),
    )
    for code, used, limit, what in checks:
        if limit is not None and isinstance(used, int) and used > limit:
            return Failure(code, f"maximum {what} ({limit}) exceeded").as_mapping()
    return None


def _refusal(job: JobDefinition, doc: StateDoc, from_state: str, action: str) -> str | None:
    terminal = from_state in TERMINAL_STATES
    if action == "cancel" and terminal:
        return f"the job is already {from_state}"
    if action == "pause" and (terminal or from_state == "paused"):
        return f"the job is already {from_state}"
    if action in ("continue", "override_step") and from_state not in ("failed", "paused"):
        return f"{action} applies to failed or paused jobs, not {from_state}"
    if action == "detach" and (job.parent is None or doc.detached is not None):
        return "the job has no parent link to detach"
    if action == "delete" and not (terminal or from_state == "paused"):
        return f"only terminal or paused jobs are deleted, not {from_state}"
    if action == "delete" and from_state == "succeeded" and not unsealed(doc):
        return "a succeeded job is never changed; release it with `job unseal` first"
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
) -> tuple[StateDoc, Effect]:
    """Decide one request for an owned job: the new document and the effect. Pure: no I/O.

    Every effect but :class:`Defer` records the request id in the document's
    ``applied_requests`` and a ``request_applied`` or ``request_dropped`` entry
    in ``history_tail``; a :class:`~httk.workflow._state.Release` also carries the id.
    A request that applies while an attempt is live (``phase`` not ``idle``) is
    deferred: for ``cancel`` the manager signals the attempt meanwhile.
    ``seal`` (a repair verb for a succeeded job without a seal) and ``unseal``
    (the operator's release of a succeeded job, after which ``delete`` applies)
    need the payload, so their effects are executed by the owner.

    :param job: The job's definition.
    :param doc: Its current ``state.json``.
    :param from_state: The unowned state the job was claimed from.
    :param from_priority: The priority it was claimed with.
    :param request: The request, for this job.
    :param refusal: The caller's check for ``continue``, ``override_step`` (the revival guard) and ``delete``
        (the waiting-parent check): why the request must not apply, or ``None``. A reason drops the request,
        unless a ``continue``/``override_step`` carries ``force``.
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
    reason = _refusal(job, doc, from_state, action)
    checked = reason is None and action in ("continue", "override_step", "delete")
    if checked and refusal is not None and (hazard := refusal(request)) is not None:
        if document.get("force") is not True:
            reason = hazard if action == "delete" else f"{hazard}; republish with force to accept the hazard"
        else:
            _LOGGER.warning("forced revival of %s: %s", job.job_key, hazard)
            audit["revival_hazard"] = hazard
    applied = (*doc.applied_requests, request.request_id)
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
    elif action == "detach":
        new = doc.updated(detached={"at": datetime.now(UTC).isoformat(), "operator": document["operator"]})
    elif action in ("continue", "override_step"):
        new, target = doc.with_failure(None), "ready"
        if action == "override_step":
            new = new.next_activation(validate_step(document["step"]), "override_step")
        elif doc.attempt is not None and doc.attempt.get("started_at") is not None:
            # Only a started attempt is retried; an unstarted (or absent) one is simply launched from ready.
            new = new.next_attempt("manual_continue", unclean=False)
        if (failure := _budget_failure(job, new)) is not None:
            new, target = doc.with_failure(failure), "failed"
    elif action == "unseal":
        # The release that makes `delete` apply; the owner removes the seal document.
        released = {"released": True, "at": datetime.now(UTC).isoformat(), "request_id": request.request_id}
        new = doc.updated(seal=released).with_history("unsealed", reason="unseal")
    audit["to"] = target
    new = new.with_history("request_applied", **audit).updated(applied_requests=applied)
    if action == "eject":
        return new, Eject(require_string(document["destination"], "request destination"))
    if action == "delete":
        return new, Discard()
    release = Release(target, priority, (request.request_id,))
    if action == "seal":
        return new, Seal(release)
    if action == "unseal":
        return new, Unseal(release)
    return new, release
