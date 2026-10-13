"""Owner-side execution of operator requests, and job removal on top of it.

A request (``requests/<job-uuid>.<request-uuid>.json``) is applied only by the
job's owner: :func:`httk.workflow._requests.apply` decides the effect, and
:func:`apply_requests` carries it out with one kernel call. A manager runs it
at every boundary of a job it holds; :func:`serve` runs it for one unowned job
as a short-lived CLI owner, which is how ``job delete``, ``job seal`` and
``job unseal`` take effect without waiting for a manager.
"""

import dataclasses
import logging
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, cast

from . import _fs, _joins, _kernel, _moving, _requests
from ._job import JobDefinition
from ._kernel import JobRef, ListingCache, OwnedJob, Release
from ._state import TERMINAL_STATES, StateDoc, read_state_unowned
from .errors import FormatError, WorkflowError

if TYPE_CHECKING:  # pragma: no cover
    from .workspace import Workspace

__all__ = [
    "REMOVABLE_KINDS",
    "RemovalOutcome",
    "RemovalReport",
    "RequestOutcome",
    "RequestOwner",
    "apply_requests",
    "record_translation",
    "refusal",
    "release",
    "remove_jobs",
    "request_now",
    "serve",
    "serve_requests",
    "translation_applied",
]

_LOGGER = logging.getLogger("httk.workflow.manager")

#: The states a ``delete`` request applies to (a succeeded job only after ``job unseal``).
REMOVABLE_KINDS = frozenset({*TERMINAL_STATES, "paused"})

#: The kernel owner used to serve requests immediately.
RequestOwner = _kernel.Owner


@dataclass(frozen=True)
class RemovalOutcome:
    """What happened to one selected job.

    :param job_key: The job key.
    :param kind: The state it was found in.
    :param removed: Whether it was deleted.
    :param reason: Why not, if it was not.
    :param request: The immediate request observation, when deletion was requested.
    """

    job_key: str
    kind: str
    removed: bool
    reason: str | None = None
    request: "RequestOutcome | None" = None

    @property
    def refused(self) -> bool:
        """Whether this job was not removed."""

        return not self.removed


@dataclass(frozen=True)
class RemovalReport:
    """The per-job results of one removal.

    :param outcomes: One outcome per selected job.
    """

    outcomes: tuple[RemovalOutcome, ...]

    @property
    def removed(self) -> tuple[RemovalOutcome, ...]:
        """The jobs that were deleted."""

        return tuple(outcome for outcome in self.outcomes if outcome.removed)

    @property
    def refused(self) -> tuple[RemovalOutcome, ...]:
        """The jobs that were not deleted."""

        return tuple(outcome for outcome in self.outcomes if outcome.refused)

    @property
    def removed_count(self) -> int:
        """The number of jobs deleted."""

        return len(self.removed)


@dataclass(frozen=True)
class RequestOutcome:
    """The result of immediately serving one published request.

    :param job_id: The target job id.
    :param request_id: The request id.
    :param status: ``applied``, ``queued``, ``refused`` or ``uncertain``.
    :param explanation: What happened or why it is waiting.
    :param state: The resulting state, when known.
    """

    job_id: str
    request_id: str
    status: Literal["applied", "queued", "refused", "uncertain"]
    explanation: str
    state: str | None = None


def release(owned: OwnedJob, doc: StateDoc, target: Release) -> JobRef:
    """The release rule with the owner's history entry and run-log events.

    :param owned: The owned job.
    :param doc: The document to release with.
    :param target: Where to, with the applied request ids.
    :return: The released job's reference.
    """

    owner_id = owned.owner.owner_id
    moved = {"from": owned.from_state, "to": target.state}
    doc = doc.updated(owner_id=owner_id).with_history("released", owner_id=owner_id, **moved)
    attempt = doc.attempt.get("id") if doc.attempt is not None else None
    if target.state == "failed":
        code = doc.failure.get("code") if doc.failure is not None else None
        owned.append_log("failed", attempt_id=attempt, detail=code)
    owned.append_log("released", **moved, detail={"priority": target.priority})
    ref = owned.release(doc, target)
    _LOGGER.info(
        "job %s moved from %s to %s",
        owned.job_key,
        owned.from_state,
        target.state,
        extra={"event": "released", "job_key": owned.job_key, "job_id": owned.job_id, "to": target.state},
    )
    return ref


def refusal(
    workspace: _kernel.KernelWorkspace,
    job: JobDefinition,
    request: _requests.Request,
    cache: ListingCache | None = None,
) -> str | None:
    """The revival guard for ``continue``/``override_step``, the waiting-parent check for ``delete``, and the
    destination check for ``eject`` (an unusable destination is a refusal that moves nothing).

    :param workspace: The workspace.
    :param job: The job the request is for.
    :param request: The request.
    :param cache: The pass's listings, if any.
    :return: Why the request must not apply, or ``None``.
    """

    if request.action in ("continue", "override_step"):
        return _joins.consumed_by_decided_join(workspace, job, cache=cache)
    if request.action == "eject":
        return _moving.destination_problem(Path(str(request.document["destination"])), workspace)
    parent = job.parent
    if request.action != "delete" or parent is None:
        return None
    ref = _kernel.locate(
        workspace, str(parent["job_id"]), placement_hint=PurePosixPath(str(parent["placement"])), cache=cache
    )
    if ref is None or ref.state in TERMINAL_STATES:
        return None
    parent_doc, _damaged = read_state_unowned(ref.path / "state.json")
    children = None if parent_doc is None or parent_doc.join is None else parent_doc.join.get("children")
    if isinstance(children, tuple) and any(
        isinstance(child, Mapping) and child.get("job_id") == job.id for child in children
    ):
        return f"its parent {ref.job_key} ({ref.state}) is waiting on it"
    return None


def _parse(path: Path) -> _requests.Request | None:
    try:
        return _requests.parse(path)
    except FileNotFoundError:
        return None
    except FormatError as exc:
        # A malformed request stays where it is; gc quarantines it after its grace.
        _LOGGER.warning("ignoring malformed request %s: %s", path, exc)
        return None


def _exchange_name(doc: StateDoc, operator: object) -> str | None:
    """The exchange name an exchange-derived request (by *operator*) of an exchange job is guarded by, else ``None``."""

    if doc.origin == "exchange" and doc.exchange_name is not None and operator == "exchange":
        return doc.exchange_name
    return None


def translation_applied(workspace: _kernel.KernelWorkspace, doc: StateDoc, request: _requests.Request) -> bool:
    """Report whether *request* is a job action from the exchange that this exchange job applied already.

    Once ``applied_requests`` no longer holds its id, the index's ``translated/`` set is what tells a lagging
    translator's re-post apart (plan §13.4).

    :param workspace: The workspace.
    :param doc: The job's ``state.json``.
    :param request: The request.
    :return: Whether the request must be dropped unapplied.
    """

    name = _exchange_name(doc, request.document["operator"])
    return name is not None and _kernel.exchange_translation_applied(workspace, name, request.request_id)


def record_translation(workspace: _kernel.KernelWorkspace, doc: StateDoc, request_id: str, operator: object) -> None:
    """Record an applied request in the exchange index's ``translated/`` set when it is an exchange job action.

    Every path that applies a request calls it after the effect: :func:`apply_requests`, and a commit that
    applies the ``cancel`` that stopped an attempt.

    :param workspace: The workspace.
    :param doc: The job's ``state.json``.
    :param request_id: The applied request's id.
    :param operator: The request's ``operator``; only ``exchange`` requests of an exchange job are recorded.
    """

    name = _exchange_name(doc, operator)
    if name is None:
        return
    try:
        _kernel.record_exchange_translation(workspace, name, request_id)
    except WorkflowError:
        # The entry is gone (the job was deleted or returned): nothing can re-post into it.
        _LOGGER.debug("no exchange index entry %s to record request %s in", name, request_id)


def _history_outcome(workspace: _kernel.KernelWorkspace, ref: JobRef, request_id: str) -> RequestOutcome:
    """Recover a request result from its matching state-history record, when that proves the effect."""

    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    if current is None:
        return RequestOutcome(ref.job_id, request_id, "uncertain", "job disappeared before its effect was observed")
    doc, _damage = read_state_unowned(current.path / "state.json")
    if doc is None:
        return RequestOutcome(
            ref.job_id, request_id, "uncertain", "request effect is absent from readable state history"
        )
    entries = [entry for entry in doc.history_tail if entry.get("request_id") == request_id]
    dropped = next((entry for entry in reversed(entries) if entry.get("event") == "request_dropped"), None)
    if dropped is not None:
        return RequestOutcome(
            ref.job_id, request_id, "refused", str(dropped.get("note", "request was dropped")), current.state
        )
    applied = next((entry for entry in reversed(entries) if entry.get("event") == "request_applied"), None)
    if applied is None:
        reconciling = (
            current.state == _kernel.OWNED
            or doc.release_to is not None
            or doc.commit is not None
            or doc.phase["kind"] != "idle"
        )
        status: Literal["queued", "uncertain"] = "queued" if reconciling else "uncertain"
        if current.state == _kernel.OWNED:
            explanation = "another owner holds the job"
        elif reconciling:
            explanation = "job needs reconciliation"
        else:
            explanation = "request effect is not in state history"
        return RequestOutcome(ref.job_id, request_id, status, explanation, current.state)
    action = applied.get("action")
    if action in {"delete", "eject"}:
        return RequestOutcome(
            ref.job_id,
            request_id,
            "uncertain",
            f"{action} history records intent but cannot prove delivery",
            current.state,
        )
    if action == "seal" and doc.seal is None:
        return RequestOutcome(ref.job_id, request_id, "uncertain", "seal effect is not present", current.state)
    if action == "unseal" and (
        doc.seal is None or doc.seal.get("released") is not True or doc.seal.get("request_id") != request_id
    ):
        return RequestOutcome(ref.job_id, request_id, "uncertain", "unseal effect is not present", current.state)
    return RequestOutcome(ref.job_id, request_id, "applied", f"{action} applied", current.state)


def _seal(owned: OwnedJob, doc: StateDoc, request: _requests.Request) -> StateDoc:
    """Seal the owned job's payload and return the document recording it (the ``Seal`` effect)."""

    from . import seals

    workspace = cast("Workspace", owned.owner.workspace)
    try:
        keys = seals.default_workspace_keys(workspace).keys
    except seals.SealError:
        keys = ()
    try:
        sha256, signed = seals.seal_payload(
            owned.path, job_id=owned.job_id, job_key=owned.job_key, keys=keys, durable=workspace.durable
        )
    except (ValueError, FormatError, _fs.UnsafePath) as exc:
        # What the job left in its payload cannot be sealed: the request is dropped, the job unchanged.
        return doc.with_history("request_dropped", request_id=request.request_id, action="seal", note=str(exc))
    owned.append_log("sealed", detail={"sha256": sha256, "signed": signed, "request_id": request.request_id})
    return doc.updated(seal={"sha256": sha256, "signed": signed})


def _eject(
    owned: OwnedJob, doc: StateDoc, request: _requests.Request, effect: _requests.Eject
) -> tuple[Literal["applied", "refused", "uncertain"], str]:
    """Eject a job request and distinguish a recorded rollback from an unresolved delivery."""

    workspace = cast("Workspace", owned.owner.workspace)
    # The applied id and its history entry travel with the bundle, so a returning job never re-applies it.
    # The applied marker travels with a successful bundle; a verified rollback gets a matching dropped event.
    owned.write_state(doc)
    _fs.remove_file(_fs.loc(request.path), durable=workspace.durable)
    try:
        report = _moving.eject(workspace, owned.owner, owned, destination=Path(effect.destination), tree=effect.tree)
    except _kernel.OwnerLost:
        raise
    except Exception as exc:
        _LOGGER.warning("eject request %s for %s refused: %s", request.request_id, owned.job_key, exc)
        current = _kernel.locate(workspace, request.job_id, placement_hint=request.placement, exhaustive=True)
        refused = False
        if current is not None and current.state != _kernel.OWNED:
            returned = _kernel.claim(workspace, owned.owner, current)
            if returned is not None:
                note = f"eject failed and the job was returned: {exc}"
                try:
                    returned_doc = returned.read_state()
                    if returned_doc is not None and request.request_id in returned_doc.applied_requests:
                        returned.append_log(
                            "request_dropped",
                            detail={"request_id": request.request_id, "action": "eject", "note": note},
                        )
                        release(
                            returned,
                            returned_doc.with_history(
                                "request_dropped", request_id=request.request_id, action="eject", note=note
                            ),
                            Release(returned.from_state, returned.from_priority),
                        )
                        refused = True
                except (WorkflowError, OSError, FormatError, _fs.UnsafePath) as record_error:
                    _LOGGER.warning("cannot record eject rollback for %s: %s", request.request_id, record_error)
                finally:
                    if owned.owner.holds(returned.ref):
                        try:
                            returned.give_back()
                        except (WorkflowError, OSError, FormatError, _fs.UnsafePath) as release_error:
                            _LOGGER.error(
                                "cannot release eject rollback claim for %s: %s", request.request_id, release_error
                            )
                if refused:
                    return "refused", note
        return "uncertain", f"eject outcome is unresolved: {exc}"
    # A delivered exchange root left the exchange index inside _moving (also on crash recovery).
    _LOGGER.info(
        "ejected %s to %s (request %s)",
        owned.job_key,
        report.destination,
        request.request_id,
        extra={"event": "ejected", "job_key": owned.job_key, "job_id": owned.job_id},
    )
    return "applied", f"ejected to {report.destination}"


def _recording(target: Release, extra: tuple[str, ...]) -> Release:
    """Return *target* also carrying the request ids *extra*."""

    return dataclasses.replace(target, applied_requests=(*target.applied_requests, *extra))


def apply_requests(
    owned: OwnedJob,
    job: JobDefinition,
    doc: StateDoc,
    *,
    from_state: str | None = None,
    from_priority: int | None = None,
    exclude: Collection[str] = (),
    cache: ListingCache | None = None,
    deferred: set[str] | None = None,
    only: Collection[str] | None = None,
    outcomes: list[RequestOutcome] | None = None,
) -> StateDoc | None:
    """Apply the job's pending requests oldest first; ``None`` once one released (or deleted) the job.

    It runs where the job is quiescent: the last step of a reconcile, the last
    step of a commit (where the job is at the commit's target state, given as
    *from_state* and *from_priority*), and :func:`serve`. Requests apply in
    ``created_at`` order, then by request id; the release one makes also
    records the *exclude* ids.

    :param owned: The owned, quiescent job.
    :param job: Its definition.
    :param doc: Its current document.
    :param from_state: The state the job is at; the state it was claimed from by default.
    :param from_priority: The priority it is at; the claimed priority by default.
    :param exclude: Request ids the caller applies itself; a release made here records them too.
    :param cache: The pass's listings, for the refusal checks.
    :param deferred: Collects the ids of requests that wait for a later boundary.
    :param only: Apply only these request ids when provided.
    :param outcomes: Append observations of effects actually carried out.
    :return: The document when every request waits, or ``None`` once the job was released.
    """

    workspace = owned.owner.workspace
    from_state = owned.from_state if from_state is None else from_state
    from_priority = owned.from_priority if from_priority is None else from_priority
    requests = [
        request
        for path in owned.request_files()
        if (request := _parse(path)) is not None
        and request.job_id == owned.job_id
        and request.request_id not in exclude
        and (only is None or request.request_id in only)
    ]
    requests.sort(key=lambda request: (str(request.document["created_at"]), request.request_id))
    extra = tuple(exclude)
    for request in requests:
        if translation_applied(workspace, doc, request):
            # A lagging exchange translator re-posted an action this job applied already: never again.
            _fs.remove_file(_fs.loc(request.path), durable=workspace.durable)
            owned.append_log(
                "request_applied",
                detail={"request_id": request.request_id, "action": request.action, "dropped": "already applied"},
            )
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(
                        request.job_id, request.request_id, "applied", "exchange request was already applied"
                    )
                )
            continue
        new, effect = _requests.apply(
            job,
            doc,
            from_state,
            from_priority,
            request,
            refusal=lambda candidate: refusal(workspace, job, candidate, cache),
        )
        if isinstance(effect, _requests.Eject) and (from_state, from_priority) != (
            owned.from_state,
            owned.from_priority,
        ):
            # At a commit the job is not yet where it is released to; the requests pass ejects it from there.
            continue
        if isinstance(effect, _requests.Defer):
            if deferred is not None:
                deferred.add(request.request_id)
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(request.job_id, request.request_id, "queued", "waits for the job boundary")
                )
            continue
        if deferred is not None:
            deferred.discard(request.request_id)
        detail: dict[str, object] = {"request_id": request.request_id, "action": request.action}
        if isinstance(effect, _requests.Drop):
            detail["dropped"] = effect.reason
        owned.append_log("request_applied", detail=detail)
        if isinstance(effect, _requests.Discard):
            owned.discard()
            _LOGGER.info("deleted %s (request %s)", owned.job_key, request.request_id)
            if outcomes is not None:
                outcomes.append(RequestOutcome(request.job_id, request.request_id, "applied", "job deleted"))
        elif isinstance(effect, _requests.Drop):
            release(owned, new, Release(from_state, from_priority, (request.request_id, *extra)))
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(request.job_id, request.request_id, "refused", effect.reason, from_state)
                )
        elif isinstance(effect, _requests.Seal):
            sealed = _seal(owned, new, request)
            note = next(
                (
                    entry.get("note")
                    for entry in reversed(sealed.history_tail)
                    if entry.get("request_id") == request.request_id and entry.get("event") == "request_dropped"
                ),
                None,
            )
            release(owned, sealed, _recording(effect.release, extra))
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(
                        request.job_id,
                        request.request_id,
                        "refused" if note else "applied",
                        str(note or "job sealed"),
                        effect.release.state,
                    )
                )
        elif isinstance(effect, _requests.Eject):
            status, explanation = _eject(owned, new, request, effect)
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(
                        request.job_id,
                        request.request_id,
                        status,
                        explanation,
                    )
                )
        elif isinstance(effect, _requests.Unseal):
            try:
                owned.discard_subtree(".httk-job/seal.json")
            except _fs.UnsafePath as exc:
                _LOGGER.warning("%s has no real seal document to remove: %s", owned.job_key, exc)
            release(owned, new, _recording(effect.release, extra))
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(request.job_id, request.request_id, "applied", "job unsealed", effect.release.state)
                )
        else:
            release(owned, new, _recording(effect, extra))
            if outcomes is not None:
                outcomes.append(
                    RequestOutcome(
                        request.job_id, request.request_id, "applied", f"{request.action} applied", effect.state
                    )
                )
        # Recorded after the effect: a crash in between can at worst let a lagging re-post apply once more, while
        # recording first could lose the action altogether.
        record_translation(workspace, doc, request.request_id, request.document["operator"])
        return None
    return doc


def serve(
    workspace: _kernel.KernelWorkspace,
    owner: RequestOwner,
    ref: JobRef,
    *,
    only: Collection[str] | None = None,
    outcomes: list[RequestOutcome] | None = None,
) -> bool:
    """Apply an unowned job's pending requests as *owner* (a CLI owner), without a manager.

    A job whose last owner left work unfinished (a pending release, a commit
    intent, an attempt that did not end cleanly) is released back unchanged:
    finishing it is a manager's ``reconcile``.

    :param workspace: The workspace.
    :param owner: The serving owner.
    :param ref: The unowned job.
    :param only: Apply only these request ids when provided.
    :param outcomes: Append observations of effects actually carried out.
    :return: Whether the job was claimed (``False`` when another actor moved it first).
    :raises httk.workflow.errors.WorkflowError: If ``job.json`` or ``state.json`` cannot be read; the job is
        released back first, or quarantined when that cannot be done (its ``job.json`` was damaged already at
        the claim, or its ``state.json`` is unreadable).
    """

    from .gc import quarantine, quarantine_damaged

    try:
        owned = _kernel.claim(workspace, owner, ref)
    except (FormatError, _fs.UnsafePath, OSError) as exc:
        reason = f"job.json is damaged: {exc}"
        moved = quarantine_damaged(cast("Workspace", workspace), owner, ref.job_id, reason)
        raise WorkflowError(
            f"quarantined {ref.job_key}: {reason}" if moved else f"cannot claim {ref.job_key}: {exc}"
        ) from exc
    if owned is None:
        return False
    owned.append_log("claimed", **{"from": ref.state, "to": _kernel.OWNED})
    try:
        job = JobDefinition.from_path(owned.path / "job.json")
        doc = owned.read_state()
    except (FormatError, _fs.UnsafePath, OSError) as exc:
        try:
            # Back unchanged, as recovery would return it; a manager fails a damaged job.
            owned.give_back()
        except (FormatError, _fs.UnsafePath, OSError):
            # An unreadable state.json leaves nothing to give back by: the job goes to quarantine, through a
            # quarantine scratch whose reconciler finishes the move should this owner die in between.
            scratch = owner.scratch("quarantine")
            owned.extract(scratch / owned.path.name)
            quarantine(cast("Workspace", workspace), owner, scratch / owned.path.name, f"damaged: {exc}")
            _fs.remove_empty_dir(_fs.loc(scratch))
            raise WorkflowError(f"quarantined {ref.job_key}: {exc}") from exc
        if isinstance(exc, WorkflowError):
            raise
        raise WorkflowError(f"cannot read {ref.job_key}: {exc}") from exc
    if owned.pending_release() is not None or (
        doc is not None and (doc.commit is not None or doc.phase["kind"] != "idle")
    ):
        if outcomes is not None:
            for path in owned.request_files():
                request = _parse(path)
                if request is not None and (only is None or request.request_id in only):
                    outcomes.append(
                        RequestOutcome(
                            request.job_id,
                            request.request_id,
                            "queued",
                            "job needs reconciliation",
                            owned.from_state,
                        )
                    )
        owned.give_back()
        return True
    if doc is None or doc.activation is None:
        doc = (doc or StateDoc.empty(owned.job_id)).next_activation(job.initial_step, "initial")
    if apply_requests(owned, job, doc, only=only, outcomes=outcomes) is not None:
        release(owned, doc, Release(owned.from_state, owned.from_priority))
    return True


def serve_requests(
    workspace: "Workspace", published: Iterable[tuple[JobRef, Path]], *, owner: RequestOwner | None = None
) -> tuple[RequestOutcome, ...]:
    """Immediately serve a bounded snapshot of published requests for unowned jobs.

    Requests for each job run in ``(created_at, request_id)`` order through
    the requested id. A live owner, lost claim, unfinished reconciliation, or
    more than 256 preceding requests leaves the request queued. Arrivals after
    the snapshot are not considered.

    :param workspace: The local workspace.
    :param published: Published request files paired with their observed jobs.
    :param owner: An existing CLI owner, or ``None`` to register one.
    :return: One result per published request, in input order.
    """

    items = list(published)
    if not items:
        return ()
    results: dict[str, RequestOutcome] = {}
    snapshots: dict[str, list[_requests.Request]] = {}
    oversized: set[str] = set()
    for ref, path in items:
        if ref.job_id not in snapshots:
            found = []
            candidates = list(islice((workspace.control / "requests").glob(f"{ref.job_id}.*.json"), 257))
            if len(candidates) > 256:
                oversized.add(ref.job_id)
            for candidate in candidates[:256]:
                parsed = _parse(candidate)
                if parsed is not None:
                    found.append(parsed)
            found.sort(key=lambda request: (str(request.document["created_at"]), request.request_id))
            snapshots[ref.job_id] = found

    def process(active: _kernel.Owner) -> None:
        for ref, path in items:
            request_id = path.name.split(".")[1]
            ordered = snapshots[ref.job_id]
            target = next((request for request in ordered if request.request_id == request_id), None)
            if ref.job_id in oversized:
                results[request_id] = RequestOutcome(
                    ref.job_id, request_id, "queued", "more than 256 requests; retry after reconciliation"
                )
                continue
            if target is None:
                results[request_id] = _history_outcome(workspace, ref, request_id)
                continue
            target_key = (str(target.document["created_at"]), target.request_id)
            for request in ordered:
                if (str(request.document["created_at"]), request.request_id) > target_key:
                    break
                if request.request_id in results and results[request.request_id].status in {"applied", "refused"}:
                    continue
                current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
                if current is None:
                    result = RequestOutcome(
                        ref.job_id, request.request_id, "uncertain", "job disappeared before the request was observed"
                    )
                    results[request.request_id] = result
                    results[request_id] = dataclasses.replace(result, request_id=request_id)
                    break
                if current.state == _kernel.OWNED:
                    explanation = "another owner holds the job"
                    results[request.request_id] = RequestOutcome(
                        ref.job_id, request.request_id, "queued", explanation, current.state
                    )
                    results[request_id] = RequestOutcome(ref.job_id, request_id, "queued", explanation, current.state)
                    break
                observed: list[RequestOutcome] = []
                try:
                    claimed = serve(workspace, active, current, only={request.request_id}, outcomes=observed)
                except (WorkflowError, OSError, _fs.UnsafePath) as exc:
                    results[request.request_id] = RequestOutcome(ref.job_id, request.request_id, "uncertain", str(exc))
                    break
                if not claimed:
                    result = _history_outcome(workspace, ref, request.request_id)
                    if (
                        result.status == "uncertain"
                        and _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
                        is not None
                    ):
                        result = dataclasses.replace(
                            result, status="queued", explanation="another owner claimed the job first"
                        )
                    results[request.request_id] = result
                    if result.status in {"applied", "refused"}:
                        continue
                    results[request_id] = dataclasses.replace(result, request_id=request_id)
                    break
                if observed:
                    results[request.request_id] = observed[-1]
                else:
                    outcome = _history_outcome(workspace, ref, request.request_id)
                    results[request.request_id] = outcome
                    if outcome.status not in {"applied", "refused"}:
                        results[request_id] = outcome
                        break

    if owner is None:
        with _kernel.register_owner(
            workspace, kind="cli", label="immediate job requests", allocation=None, advertised={}
        ) as active:
            process(active)
    else:
        process(owner)
    return tuple(
        results.get(
            path.name.split(".")[1],
            RequestOutcome(ref.job_id, path.name.split(".")[1], "uncertain", "no effect was observed"),
        )
        for ref, path in items
    )


def request_now(
    workspace: "Workspace",
    owner: RequestOwner,
    ref: JobRef,
    action: str,
    reason: str,
    *,
    outcomes: list[RequestOutcome] | None = None,
) -> str | None:
    """Post one request for a job and apply it now when the job is unowned (``job delete|seal|unseal``).

    A job a manager holds gets the request at its next boundary. Nothing is posted for a job that is not found.

    :param workspace: The workspace.
    :param owner: The CLI owner that applies it.
    :param ref: The job, as last observed.
    :param action: The request action.
    :param reason: The operator's reason.
    :param outcomes: Append the immediate typed result when supplied.
    :return: ``None`` when the request took effect, otherwise why it did not (yet).
    """

    placement = ref.placement if ref.placement is not None else JobDefinition.from_path(ref.path / "job.json").placement
    current = _kernel.locate(workspace, ref.job_id, placement_hint=placement)
    if current is None:
        if outcomes is not None:
            outcomes.append(RequestOutcome(ref.job_id, "", "uncertain", "the job was not found"))
        return "the job was not found"
    path = _requests.post(
        workspace, action=action, job_id=ref.job_id, placement=placement, operator="cli", reason=reason
    )
    (outcome,) = serve_requests(workspace, [(current, path)], owner=owner)
    if outcomes is not None:
        outcomes.append(outcome)
    if outcome.status == "applied":
        return None
    if outcome.status == "queued" and outcome.explanation == "another owner holds the job":
        return "a manager holds the job; the request applies at its next boundary"
    return outcome.explanation


def remove_jobs(workspace: "Workspace", refs: Iterable[JobRef], *, force: bool = False) -> RemovalReport:
    """Post a ``delete`` request for each job and apply it now when the job is unowned.

    :param workspace: The workspace.
    :param refs: The jobs, as last observed.
    :param force: Ignored; a ``delete`` request has no force (the waiting-parent check holds).
    :return: The per-job outcomes.
    """

    del force
    outcomes: list[RemovalOutcome] = []
    with _kernel.register_owner(workspace, kind="cli", label="job delete", allocation=None, advertised={}) as owner:
        for ref in refs:
            observed: list[RequestOutcome] = []
            reason = request_now(workspace, owner, ref, "delete", "job delete", outcomes=observed)
            request = observed[0] if observed else None
            outcomes.append(RemovalOutcome(ref.job_key, ref.state, reason is None, reason, request))
    return RemovalReport(tuple(outcomes))
