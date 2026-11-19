"""Owner-side execution of operator requests, and job removal on top of it.

A request (``requests/<job-uuid>.<request-uuid>.json``) is applied only by the
job's owner: :func:`httk.workflow._requests.apply` decides the effect, and
:func:`apply_requests` carries it out with one kernel call. A manager runs it
at every boundary of a job it holds; :func:`serve` runs it for one unowned job
as a short-lived CLI owner, which is how :func:`serve_requests` lets ``job
request``, ``job delete``, ``job seal``, ``job unseal`` and ``job detach`` take effect without
waiting for a manager.
"""

import dataclasses
import json
import logging
import os
import re
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, cast

from . import _fs, _joins, _kernel, _moving, _requests, _store
from ._job import JobDefinition
from ._kernel import JobRef, ListingCache, LocationIndex, OwnedJob, Release
from ._manager_scheduling import current_step, effective_workflow
from ._state import TERMINAL_STATES, StateDoc, _thaw, read_state_unowned
from .errors import FormatError, WorkflowError
from .models import JOB_STATE_DIRECTORY, canonical_uuid
from .scaffold import WorkflowProvider, _check_parameter_types

if TYPE_CHECKING:  # pragma: no cover
    from .workspace import Workspace

__all__ = [
    "REMOVABLE_KINDS",
    "RemovalOutcome",
    "RemovalReport",
    "RequestOutcome",
    "apply_requests",
    "daemon_attribution_refusal",
    "refusal",
    "release",
    "remove_jobs",
    "request_jobs",
    "request_now",
    "serve",
    "serve_requests",
    "workflow_switch_problem",
]

_LOGGER = logging.getLogger("httk.workflow.manager")

#: The states a ``delete`` request applies to (a succeeded job only after ``job unseal``).
REMOVABLE_KINDS = frozenset({*TERMINAL_STATES, "paused", "away"})

#: Why a request waits for the owner that holds its job.
_HELD = "another owner holds the job; the request applies at its next boundary"
#: The most requests of one job that :func:`serve_requests` considers; beyond it every request waits for a manager.
_SNAPSHOT_LIMIT = 256
#: The outcomes that settle a request.
_SETTLED = frozenset({"applied", "refused"})
#: The most jobs a command posts requests for before it serves them (plan WORKFLOW_LOCAL_COMMANDS §4.3).
_CHUNK_JOBS = 256


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


def daemon_attribution_refusal(
    workspace: _kernel.KernelWorkspace, job_id: str, placement: PurePosixPath, request: _requests.Request
) -> str | None:
    """Return why a ``daemon:`` request no longer holds for the exchange adoption it was authorized for.

    Every owner applies this before any effect of a daemon request, including stopping a running attempt.

    :param workspace: The workspace.
    :param job_id: The job's UUID.
    :param placement: The job's placement.
    :param request: The request.
    :return: Why the request must not apply, or ``None`` (always for requests of other operators).
    """

    operator = request.document["operator"]
    if isinstance(operator, str) and operator.startswith("daemon:"):
        attribution = operator.split(":", 2)
        if len(attribution) != 3 or not attribution[2]:
            return "the daemon request carries malformed exchange adoption attribution"
        try:
            exchange_name = canonical_uuid(attribution[1], "daemon exchange name")
        except FormatError:
            return "the daemon request carries malformed exchange adoption attribution"
        if exchange_name != attribution[1]:
            return "the daemon request carries malformed exchange adoption attribution"
        expected = (job_id, placement, attribution[2])
        if _kernel.exchange_index(workspace, exchange_name) != expected:
            return "the exchange adoption changed after this daemon request was authorized"
    return None


def _output_roles(provider: WorkflowProvider) -> list[str]:
    """The output names a workflow's declaration lists: each output's role."""

    return sorted(str(entry.get("role", name)) for name, entry in provider.outputs.items())


def _types(declared: Mapping[str, object]) -> dict[str, object]:
    """Each declared name with its declared ``type`` (``None`` when untyped)."""

    return {
        name: metadata.get("type") if isinstance(metadata, Mapping) else None for name, metadata in declared.items()
    }


#: Why ``set_workflow`` refuses a job that embeds its workflow in ``workflow/``.
_EMBEDDED_SWITCH_REFUSAL = "an embedded workflow cannot be switched; unlock the job and edit workflow/ instead"


def workflow_switch_problem(
    workspace: _kernel.KernelWorkspace,
    job: JobDefinition,
    doc: StateDoc | None,
    candidate: _store.Installed,
    providers: dict[str, WorkflowProvider | None] | None = None,
) -> str | None:
    """Return why the job cannot switch to run *candidate* (``set_workflow``), naming the failed rule.

    The candidate, installed in the job's workspace (rule 1, the caller's lookup), must (2) carry the job's
    stored declaration ``$id`` (the current installation's for a job that stores no declaration), or without
    one the same short name; (3) declare the same inputs, by name, destination and hook-consumed flag; (4)
    declare the same output names; (5) declare no parameter the job lacks a value for (unless the job's own
    declaration also lacked it) and none of the job's declared parameters may be unknown to it, with every
    value of its declared type; (6) declare the same environment names and types; and (7) declare the step
    the job runs next (for a waiting parent, the steps its join advances to); and (8) declare exactly the call
    slots the job binds, each binding fitting the candidate's slot as installed now. A language realization never
    switches: its executed document lives in the payload, and neither does a job that embeds its workflow. An unlocked job (debug mode) skips rules 2-8. A job made from an installed package (it stores
    ``declarations.workflow``) records empty parameter, input and environment declarations by leaving them
    out; any other job without a snapshot (a spawned child) is compared with its current installation, and
    without that the switch is refused as unverifiable.

    :param workspace: The job's workspace.
    :param job: The job.
    :param doc: Its ``state.json``, or ``None`` before the first claim.
    :param candidate: The installation to switch to.
    :param providers: A batch's cache of the jobs' current installations, by id (``None`` when not installed).
    :return: The refusal, or ``None`` when the switch is compatible.
    """

    if job.embedded is not None:
        return _EMBEDDED_SWITCH_REFUSAL
    try:
        provider = candidate.provider()
    except ValueError as exc:
        return f"workflow {candidate.id} cannot be read: {exc}"
    if job.parameters.get("workflow_realization") == "language" or provider.language is not None:
        return "a language realization cannot switch workflows: its executed document lives in the payload"
    if doc is not None and doc.unlocked is not None:
        return None
    current_id, current_name = effective_workflow(job, doc)
    cache = {} if providers is None else providers

    def current() -> WorkflowProvider | None:
        # The job's current installation stands in only where job.json carries no snapshot: looked up lazily.
        if current_id not in cache:
            found = _store.lookup(workspace, current_id)
            try:
                cache[current_id] = None if found is None else found.provider()
            except ValueError:
                cache[current_id] = None
        return cache[current_id]

    def unverifiable(rule: str) -> str:
        return f"{rule}: cannot be verified: job.json has no snapshot and {current_id} is gone"

    stored = job.declarations.get("workflow")
    # A job made from an installed package stores its workflow declaration and leaves out empty snapshots.
    # A spawned child may carry its parent's declarations, but never the snapshots the package left out.
    scaffolded = isinstance(stored, Mapping) and (job.parent is None or "inputs" in job.declared)
    declaration: Mapping[str, object] = stored if isinstance(stored, Mapping) else {}
    if not isinstance(stored, Mapping) and (installed := current()) is not None:
        declaration = installed.declarations.get("workflow", {})  # a spawned child: its installation's
    offered = provider.declarations.get("workflow", {})
    if "$id" in declaration:
        if offered.get("$id") != declaration["$id"]:
            return (
                f"rule 2 (declaration): {candidate.id} declares $id {offered.get('$id')!r}, "
                f"the job's declaration has $id {declaration['$id']!r}"
            )
    elif candidate.name != current_name:
        return f"rule 2 (short name): {candidate.id} is named {candidate.name!r}, the job's workflow {current_name!r}"
    snapshot = job.declared.get("inputs", {} if scaffolded else None)
    if isinstance(snapshot, Mapping):
        inputs: dict[str, object] = {
            name: metadata.get("destination") if isinstance(metadata, Mapping) else None
            for name, metadata in snapshot.items()
        }
    elif (installed := current()) is not None:
        inputs = dict(installed.inputs)
    else:
        return unverifiable("rule 3 (inputs)")
    if dict(provider.inputs) != inputs:
        return f"rule 3 (inputs): {candidate.id} declares inputs {dict(provider.inputs)}, the job {inputs}"
    listed = declaration.get("outputs")
    if isinstance(listed, Sequence) and not isinstance(listed, str):
        outputs = sorted(str(entry.get("name")) for entry in listed if isinstance(entry, Mapping))
    elif (installed := current()) is not None:
        outputs = _output_roles(installed)
    else:
        return unverifiable("rule 4 (outputs)")
    if _output_roles(provider) != outputs:
        return f"rule 4 (outputs): {candidate.id} declares outputs {_output_roles(provider)}, the job {outputs}"
    declared_parameters = job.declared.get("parameters", {} if scaffolded else None)
    if isinstance(declared_parameters, Mapping):
        known = set(declared_parameters)
    elif (installed := current()) is not None:
        known = set(installed.parameters)
    else:
        return unverifiable("rule 5 (parameters)")
    if lacking := sorted(set(provider.parameters) - set(job.parameters) - known):
        return f"rule 5 (parameters): {candidate.id} declares {', '.join(lacking)}, which the job has no value for"
    if unknown := sorted(known - set(provider.parameters)):
        return f"rule 5 (parameters): the job's parameters {', '.join(unknown)} are unknown to {candidate.id}"
    try:
        _check_parameter_types(cast(dict[str, object], _thaw(job.parameters)), provider.parameters)
    except ValueError as exc:
        return f"rule 5 (parameters): {exc}"
    # job.json always reads back with environment.declared; for a job that stores no declaration an empty one
    # only says the spawner set none, so its installation stands in.
    environment = job.environment.get("declared")
    if isinstance(environment, Mapping) and (scaffolded or environment):
        expected = _types(environment)
    elif (installed := current()) is not None:
        expected = _types(installed.environment)
    else:
        return unverifiable("rule 6 (environment)")
    if _types(provider.environment) != expected:
        return f"rule 6 (environment): {candidate.id} declares another environment than the job"
    # A restart reruns the current step; a waiting parent (or one failed at its wait) runs next whichever step
    # its join advances to.
    steps: list[object] = [current_step(job, doc)]
    join = None if doc is None else doc.join
    if join is not None:
        rescue = join.get("on_impossible")
        steps += [join.get("next_step"), rescue.get("next_step") if isinstance(rescue, Mapping) else None]
    for step in steps:
        if isinstance(step, str) and step not in provider.steps:
            return f"rule 7 (step): {candidate.id} does not declare the job's step {step!r}"
    return _slots_problem(workspace, job, candidate.id, provider, current if job.calls else None)


def _slots_problem(
    workspace: _kernel.KernelWorkspace,
    job: JobDefinition,
    candidate: str,
    provider: WorkflowProvider,
    current: Callable[[], WorkflowProvider | None] | None,
) -> str | None:
    """Rule 8: the job's call slot bindings carry over to the same interfaces, each fitting as installed now."""

    from ._slots import SlotMismatch, fit, interface_key

    slots = provider.slots or {}
    bindings = job.calls or {}
    if sorted(slots) != sorted(bindings):
        return (
            f"rule 8 (call slots): {candidate} has call slots {sorted(slots) or 'none'}, "
            f"the job binds {sorted(bindings) or 'none'}"
        )
    present = None if current is None else current()
    for alias in bindings:
        # What the running step reads back is the interface's outputs, so the interface itself must not change.
        before = None if present is None else (present.slots or {}).get(alias)
        if before is None:
            return f"rule 8 (call slots): cannot be verified: the job's current workflow has no call slot {alias!r}"
        if interface_key(before.interface) != interface_key(slots[alias].interface):
            return f"rule 8 (call slots): {candidate} declares another interface for call slot {alias!r}"
    for alias, binding in bindings.items():
        bound = str(cast(Mapping[str, object], binding)["workflow"])
        found = _store.lookup(workspace, bound)
        if found is None or found.id != bound:
            return f"rule 8 (call slots): the workflow {bound} bound to call slot {alias!r} is not installed"
        try:
            fit(slots[alias].interface, found.provider())
        except (SlotMismatch, ValueError) as exc:
            return f"rule 8 (call slots): {bound} bound to call slot {alias!r} does not fit {candidate}: {exc}"
    return None


def refusal(
    workspace: _kernel.KernelWorkspace,
    job: JobDefinition,
    request: _requests.Request,
    cache: ListingCache | None = None,
    doc: StateDoc | None = None,
    installations: dict[str, _store.Installed | None] | None = None,
    payload: Path | None = None,
) -> str | None:
    """The revival guard for ``continue``/``restart``/``override_step``, the waiting-parent check for ``delete``,
    the destination check for ``eject`` (an unusable destination is a refusal that moves nothing), and the
    compatibility check for ``set_workflow`` (:func:`workflow_switch_problem`, against the installation with
    exactly the requested id; refused for a job that runs its own runner copy).

    :param workspace: The workspace.
    :param job: The job the request is for.
    :param request: The request.
    :param cache: The pass's listings, if any.
    :param doc: The job's current ``state.json``, for ``set_workflow``.
    :param installations: Installations looked up by exact id, shared with the caller (filled here).
    :param payload: The owned job's directory, for the runner-copy check of ``set_workflow``.
    :return: Why the request must not apply, or ``None``.
    """

    # Within a CLI command's serving, its location index answers the parent probes where it still holds.
    index = None if (serving := _SERVING.get()) is None else serving.index

    attribution_refusal = daemon_attribution_refusal(workspace, job.id, job.placement, request)
    if attribution_refusal is not None:
        return attribution_refusal
    if request.action == "set_workflow":
        if job.embedded is not None:
            return _EMBEDDED_SWITCH_REFUSAL
        workflow_id = str(request.document["workflow"])
        found = {} if installations is None else installations
        if workflow_id not in found:
            found[workflow_id] = _store.lookup(workspace, workflow_id)  # id-shaped: exact id only
        candidate = found[workflow_id]
        if candidate is None:
            return f"rule 1 (installed): workflow {workflow_id} is not installed in the job's workspace"
        unlocked = doc is not None and doc.unlocked is not None
        if unlocked and payload is not None and os.path.lexists(payload / _store.RUNNER_COPY):
            return (
                f"the job runs its own workflow copy at {_store.RUNNER_COPY}/; edit or remove it instead of switching"
            )
        return workflow_switch_problem(workspace, job, doc, candidate)
    if request.action in ("continue", "restart", "override_step"):
        return _joins.consumed_by_decided_join(workspace, job, cache=cache, index=index)
    if request.action == "eject":
        return _moving.destination_problem(Path(str(request.document["destination"])), workspace)
    parent = job.parent
    if request.action != "delete" or parent is None:
        return None
    hint = PurePosixPath(str(parent["placement"]))
    if index is not None:
        ref = index.locate(str(parent["job_id"]), placement_hint=hint, cache=cache)
    else:
        ref = _kernel.locate(workspace, str(parent["job_id"]), placement_hint=hint, cache=cache)
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


def _order(request: _requests.Request) -> tuple[datetime, str]:
    """The application order of *request*: its ``created_at`` instant, then its id.

    Instants are compared, not strings, since posters write ``+00:00`` or ``Z`` and may omit zero microseconds; a
    naive time counts as UTC and an unreadable one sorts first.
    """

    try:
        created = datetime.fromisoformat(str(request.document["created_at"]))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC), request.request_id
    return created if created.tzinfo is not None else created.replace(tzinfo=UTC), request.request_id


def _history_outcome(
    workspace: _kernel.KernelWorkspace, ref: JobRef, request_id: str, index: LocationIndex
) -> RequestOutcome:
    """Recover a request result from its matching state-history record, when that proves the effect."""

    current = index.locate(ref.job_id, placement_hint=ref.placement, exhaustive=True)
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
            explanation = _HELD
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


def _unlock(
    owned: OwnedJob, job: JobDefinition, doc: StateDoc, new: StateDoc, request: _requests.Request, copy: bool
) -> tuple[StateDoc, bool, str]:
    """Carry out the ``Unlock`` effect: copy the job's runner when asked; the document to release, whether the
    request was dropped, and the outcome's explanation."""

    applied = (new, False, "job unlocked")

    def dropped(note: str) -> tuple[StateDoc, bool, str]:
        # A late refusal, as a seal that cannot be made: the job unchanged (no marker), the request recorded.
        noted = doc.with_history("request_dropped", request_id=request.request_id, action="unlock", note=note)
        return noted.updated(applied_requests=(*doc.applied_requests, request.request_id)), True, note

    if not copy:
        return applied
    if job.embedded is not None:
        # An embedded workflow is the job's own workflow/: never looked up in the store, never replaced.
        if _fs.lstat(_fs.loc(owned.path / _store.RUNNER_COPY)) is None:
            return dropped("the embedded workflow/ is missing; nothing to copy")
        if doc.unlocked is not None:
            return dropped("already unlocked")
        owned.append_log("runner_copy_kept", detail={"workflow": job.workflow_id, "request_id": request.request_id})
        return new, False, "job unlocked; existing runner copy kept"
    workflow_id, _name = effective_workflow(job, doc)
    installed = _store.lookup(owned.owner.workspace, workflow_id)
    if installed is None:
        return dropped(f"workflow {workflow_id} is not installed in the job's workspace; nothing to copy")
    try:
        provider = installed.provider()
    except ValueError as exc:
        return dropped(f"workflow {workflow_id} cannot be read: {exc}")
    if provider.language is not None or job.parameters.get("workflow_realization") == "language":
        return applied  # a builtin language runner: nothing to copy
    name = _store.RUNNER_COPY
    # Built in the protocol-owned state directory, so no job file shares the temporary name space.
    temporary = f"{name}.{_fs.fresh_token()}"
    try:
        anchor = _fs.open_dir(owned.path)
        try:
            state = _fs.open_dir_under(anchor, JOB_STATE_DIRECTORY, create=True, mode=0o777)
        except BaseException:
            os.close(anchor)
            raise
        try:
            for leftover in os.listdir(state):
                # A copy an earlier crash left unfinished (the job is owned and idle here).
                if re.fullmatch(rf"{re.escape(name)}\.[a-z2-7]{{16}}", leftover):
                    owned.discard_subtree(f"{JOB_STATE_DIRECTORY}/{leftover}")
            if _fs.lstat(_fs.anchored(anchor, name)) is not None:
                # An existing copy is never overwritten: a first unlock keeps one the operator placed.
                if doc.unlocked is not None:
                    return dropped("already unlocked")
                owned.append_log("runner_copy_kept", detail={"workflow": workflow_id, "request_id": request.request_id})
                return new, False, "job unlocked; existing runner copy kept"
            try:
                # ponytail: the copied files are not fsynced; after a power loss the copy may be torn (remove it
                # and unlock again).
                _store._copy_package(installed.package, owned.path / JOB_STATE_DIRECTORY / temporary, provider.build)
                # Never a half copy at the final name.
                _fs.move_owned(
                    _fs.anchored(state, temporary),
                    _fs.anchored(anchor, name),
                    durable=owned.owner.workspace.durable,
                    create_parents=False,
                )
            except BaseException:
                owned.discard_subtree(f"{JOB_STATE_DIRECTORY}/{temporary}")
                raise
        finally:
            os.close(state)
            os.close(anchor)
    except (ValueError, OSError, WorkflowError) as exc:
        return dropped(f"cannot copy the runner of workflow {workflow_id}: {exc}")
    owned.append_log("runner_copied", detail={"workflow": workflow_id, "request_id": request.request_id})
    return applied


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
    requests.sort(key=_order)
    extra = tuple(exclude)

    def observe(
        request: _requests.Request,
        status: Literal["applied", "queued", "refused", "uncertain"],
        explanation: str,
        state: str | None = None,
    ) -> None:
        if outcomes is not None:
            outcomes.append(RequestOutcome(request.job_id, request.request_id, status, explanation, state))

    # One lookup (and one manifest parse) per set_workflow id serves the check and the override's name.
    installations: dict[str, _store.Installed | None] = {}
    for request in requests:
        new, effect = _requests.apply(
            job,
            doc,
            from_state,
            from_priority,
            request,
            refusal=lambda candidate: refusal(workspace, job, candidate, cache, doc, installations, owned.path),
            workflow_name=lambda workflow_id: None if (found := installations.get(workflow_id)) is None else found.name,
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
            observe(request, "queued", "waits for the job boundary")
            continue
        if deferred is not None:
            deferred.discard(request.request_id)
        detail: dict[str, object] = {"request_id": request.request_id, "action": request.action}
        if isinstance(effect, _requests.Drop):
            detail["dropped"] = effect.reason
        if (serving := _SERVING.get()) is not None:
            # Its first durable mutation follows: the run-log record of its application, before any effect.
            serving.started.add(request.request_id)
        owned.append_log("request_applied", detail=detail)
        if isinstance(effect, _requests.Discard):
            owned.discard()
            _LOGGER.info("deleted %s (request %s)", owned.job_key, request.request_id)
            observe(request, "applied", "job deleted")
        elif isinstance(effect, _requests.Drop):
            release(owned, new, Release(from_state, from_priority, (request.request_id, *extra)))
            observe(request, "refused", effect.reason, from_state)
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
            observe(request, "refused" if note else "applied", str(note or "job sealed"), effect.release.state)
        elif isinstance(effect, _requests.Unlock):
            unlocked, refused, explanation = _unlock(owned, job, doc, new, request, effect.copy)
            release(owned, unlocked, _recording(effect.release, extra))
            observe(request, "refused" if refused else "applied", explanation, effect.release.state)
        elif isinstance(effect, _requests.Eject):
            observe(request, *_eject(owned, new, request, effect))
        elif isinstance(effect, _requests.Unseal):
            try:
                owned.discard_subtree(".httk-job/seal.json")
            except _fs.UnsafePath as exc:
                _LOGGER.warning("%s has no real seal document to remove: %s", owned.job_key, exc)
            release(owned, new, _recording(effect.release, extra))
            observe(request, "applied", "job unsealed", effect.release.state)
        else:
            release(owned, new, _recording(effect, extra))
            observe(request, "applied", f"{request.action} applied", effect.state)
        return None
    return doc


def serve(
    workspace: _kernel.KernelWorkspace,
    owner: _kernel.Owner,
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

    from .gc import quarantine_damaged

    owned: OwnedJob | None = None
    try:
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
        return _serve_claimed(workspace, owner, ref, owned, only=only, outcomes=outcomes)
    finally:
        if (serving := _SERVING.get()) is not None:
            # Where this command moved the job, or that its place is no longer known: the index's next answer.
            serving.index.moved(ref.job_id, owned)


def _serve_claimed(
    workspace: _kernel.KernelWorkspace,
    owner: _kernel.Owner,
    ref: JobRef,
    owned: OwnedJob,
    *,
    only: Collection[str] | None,
    outcomes: list[RequestOutcome] | None,
) -> bool:
    """Serve the job whose claim won *owned*: the rest of :func:`serve`."""

    from .gc import quarantine

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


def _serve_one(
    workspace: "Workspace", owner: _kernel.Owner, ref: JobRef, request_id: str, index: LocationIndex
) -> tuple[RequestOutcome, bool]:
    """Serve one request of an unowned job as *owner*; the flag says that later requests of the job must wait."""

    current = index.locate(ref.job_id, placement_hint=ref.placement, exhaustive=True)
    if current is None:
        explanation = "job disappeared before the request was observed"
        return RequestOutcome(ref.job_id, request_id, "uncertain", explanation), True
    if current.state == _kernel.OWNED:
        return RequestOutcome(ref.job_id, request_id, "queued", _HELD, current.state), True
    observed: list[RequestOutcome] = []
    try:
        claimed = serve(workspace, owner, current, only={request_id}, outcomes=observed)
    except (WorkflowError, OSError, _fs.UnsafePath) as exc:
        return RequestOutcome(ref.job_id, request_id, "uncertain", str(exc)), True
    if observed:
        # Queued only when the job needs a manager's reconciliation first, which every later request waits for too.
        return observed[-1], observed[-1].status == "queued"
    result = _history_outcome(workspace, ref, request_id, index)
    if not claimed and result.status == "uncertain" and index.present(ref.job_id):
        result = dataclasses.replace(result, status="queued", explanation="another owner claimed the job first")
    return result, result.status not in _SETTLED


@dataclass
class _Progress:
    """How far serving one snapshot got: the outcome of each request so far, the requests whose application began
    (their first durable mutation, the run-log record of it, was reached), and the job being served right now.

    :param index: The command's location index, through which the serving locates jobs and records their moves.
    """

    index: LocationIndex
    results: dict[str, RequestOutcome] = field(default_factory=dict)
    started: set[str] = field(default_factory=set)
    serving: str | None = None


#: The snapshot a CLI command is serving now, if any: :func:`serve`, :func:`apply_requests` and :func:`refusal` record
#: into it and locate through its index. Never set for a manager, whose calls behave exactly as before.
_SERVING: ContextVar[_Progress | None] = ContextVar("httk_workflow_removal_serving", default=None)


def _order_snapshot(workspace: "Workspace", job_ids: Iterable[str]) -> dict[str, list[_requests.Request] | None]:
    """Decide from one listing of ``requests/`` which requests of each job are served, and in which order.

    Each job gets the requests the glob ``<job-uuid>.*.json`` would match, parsed and in application order, or
    ``None`` when it has more than :data:`_SNAPSHOT_LIMIT` of them. This is only the order snapshot: a claim still
    lists the job's requests afresh (:meth:`~httk.workflow._kernel.OwnedJob.request_files`), and only that listing
    can show an applied request retired (plan WORKFLOW_LOCAL_COMMANDS §4.2).
    """

    directory = workspace.control / "requests"
    listed: dict[str, list[str]] = {job_id: [] for job_id in job_ids}
    for name in _kernel.list_names(directory):
        # A job UUID holds no dot: the glob's "<job-uuid>." is the name up to its first dot.
        job_id, _dot, rest = name.partition(".")
        names = listed.get(job_id)
        if names is not None and rest.endswith(".json"):
            names.append(name)
    snapshots: dict[str, list[_requests.Request] | None] = {}
    for job_id, names in listed.items():
        if len(names) > _SNAPSHOT_LIMIT:
            snapshots[job_id] = None
            continue
        found = [request for name in names if (request := _parse(directory / name)) is not None]
        snapshots[job_id] = sorted(found, key=_order)
    return snapshots


def _serve_snapshot(
    workspace: "Workspace",
    owner: _kernel.Owner,
    items: Sequence[tuple[JobRef, str]],
    snapshots: Mapping[str, list[_requests.Request] | None],
    progress: _Progress,
) -> None:
    """Serve each item's job through its request, as :func:`serve_requests` describes, recording *progress*."""

    results, index = progress.results, progress.index
    token = _SERVING.set(progress)
    try:
        for ref, request_id in items:
            ordered = snapshots[ref.job_id]
            if ordered is None:
                explanation = f"more than {_SNAPSHOT_LIMIT} requests are pending; they wait for a manager"
                results[request_id] = RequestOutcome(ref.job_id, request_id, "queued", explanation)
                continue
            target = next((request for request in ordered if request.request_id == request_id), None)
            if target is None:
                results[request_id] = _history_outcome(workspace, ref, request_id, index)
                continue
            for request in ordered:
                if _order(request) > _order(target):
                    break
                previous = results.get(request.request_id)
                if previous is not None and previous.status in _SETTLED:
                    continue
                progress.serving = ref.job_id
                result, halt = _serve_one(workspace, owner, ref, request.request_id, index)
                progress.serving = None
                results[request.request_id] = result
                if halt:
                    # The requested one waits behind it, for the same reason.
                    results[request_id] = dataclasses.replace(result, request_id=request_id)
                    break
    finally:
        _SERVING.reset(token)


def serve_requests(
    workspace: "Workspace", published: Iterable[tuple[JobRef, Path]], *, owner: _kernel.Owner | None = None
) -> tuple[RequestOutcome, ...]:
    """Immediately serve a bounded snapshot of published requests for unowned jobs.

    Requests for each job run in ``(created_at, request_id)`` order through
    the requested id. A live owner, lost claim or unfinished reconciliation
    stops at the request it meets and leaves the requested one queued; a job
    with more than 256 pending requests is not served here at all, leaving
    them to a manager. Arrivals after the snapshot are not considered. One
    listing of ``requests/`` takes the snapshot of every job, and one listing
    of the workspace locates them.

    :param workspace: The local workspace.
    :param published: Published request files paired with their observed jobs.
    :param owner: An existing CLI owner, or ``None`` to register one.
    :return: One result per published request, in input order.
    """

    return _serve_requests(workspace, published, owner=owner, index=LocationIndex(workspace))


def _serve_requests(
    workspace: "Workspace",
    published: Iterable[tuple[JobRef, Path]],
    *,
    owner: _kernel.Owner | None,
    index: LocationIndex,
) -> tuple[RequestOutcome, ...]:
    """Serve as :func:`serve_requests` does, through the command's location index *index*."""

    items = [(ref, path.name.split(".")[1]) for ref, path in published]
    if not items:
        return ()
    snapshots = _order_snapshot(workspace, dict.fromkeys(ref.job_id for ref, _request_id in items))
    progress = _Progress(index)
    if owner is None:
        with _kernel.register_owner(
            workspace, kind="cli", label="immediate job requests", allocation=None, advertised={}
        ) as active:
            _serve_snapshot(workspace, active, items, snapshots, progress)
    else:
        _serve_snapshot(workspace, owner, items, snapshots, progress)
    return tuple(progress.results[request_id] for _ref, request_id in items)


def _chunks[T](posts: Iterable[tuple[str, T]]) -> Iterator[list[tuple[str, T]]]:
    """Cut *posts* (job id first) into chunks of at most :data:`_CHUNK_JOBS`, never holding one job twice.

    A job named again starts a new chunk, so its second request is posted only after the first was served, as when
    every request was posted and served in turn.
    """

    chunk: list[tuple[str, T]] = []
    jobs: set[str] = set()
    for post in posts:
        if len(chunk) >= _CHUNK_JOBS or post[0] in jobs:
            yield chunk
            chunk, jobs = [], set()
        chunk.append(post)
        jobs.add(post[0])
    if chunk:
        yield chunk


#: The error types of one job's withdrawal, which leave its request armed rather than stop the withdrawal.
_WITHDRAWAL_ERRORS = (OSError, FormatError, WorkflowError, _fs.UnsafePath)


def _held_for_withdrawal(
    workspace: "Workspace",
    owner: _kernel.Owner,
    index: LocationIndex,
    job_id: str,
    here: Mapping[str, JobRef],
    claimed: list[OwnedJob],
) -> OwnedJob | str:
    """Hold a job so that no other owner can be applying its requests: the handle, or why it cannot be held.

    A job in this owner's ``owned/`` is held already; any other is claimed from where it is unowned, and joins
    *claimed* (to be given back once the withdrawal is durable).
    """

    if (mine := here.get(job_id)) is not None:
        held = owner.holds(mine)
        handle = owner.adopt_owned(mine)
        if not held:
            claimed.append(handle)
        return handle
    current = index.locate(job_id, placement_hint=None, exhaustive=True)
    if current is None:
        return "the job was not found"
    if current.state == _kernel.OWNED:
        return "another owner holds the job"
    won = _kernel.claim(workspace, owner, current)
    if won is None:
        return "another owner claimed the job first"
    claimed.append(won)
    return won


def _withdraw(
    workspace: "Workspace",
    owner: Callable[[], _kernel.Owner],
    posted: Sequence[tuple[JobRef, Path]],
    progress: _Progress,
    *,
    stopped: bool,
) -> None:
    """Withdraw this command's requests of one chunk that are left armed, each while this command holds its job.

    Left armed is a request whose outcome is ``uncertain`` and, when the command *stopped* (an exception or an
    interrupt), every request without an outcome yet, including one whose publication raised after its file was
    named; but only one whose application never began (:class:`_Progress`) is withdrawn. Its job is held first
    (claimed, unless this command holds it already), so no other owner can be applying it; while held, its file is
    unlinked, unless a previous owner may have begun applying it (:func:`_kept_for_recovery`). Every unlink that may
    have happened, also one an interrupt cut short, is sealed by one fsync of ``requests/`` before any job goes back,
    here or in the owner's exit; when that fsync fails or is interrupted, the owner fail-stops with every job held
    and the original exception propagates. A request whose job cannot be held, whose application began, or whose
    withdrawal fails stays armed and is logged by id, as is the job being served when the command stopped.
    """

    in_flight = progress.serving if stopped else None
    # The requests to withdraw, decided from this command's own record alone (no I/O, so nothing interrupts it).
    candidates: list[tuple[JobRef, str, Path]] = []
    for ref, path in posted:
        request_id = path.name.split(".")[1]
        result = progress.results.get(request_id)
        if ref.job_id == in_flight:
            _LOGGER.warning("job %s was being served when the command stopped: its state is uncertain", ref.job_id)
        if not ((result is None and stopped) or (result is not None and result.status == "uncertain")):
            continue
        if request_id in progress.started:
            _LOGGER.warning("request %s for job %s stays pending: its application began", request_id, ref.job_id)
            continue
        candidates.append((ref, request_id, path))
    if not candidates:
        return
    active: _kernel.Owner | None = None
    claimed: list[OwnedJob] = []
    # Every unlink that may have happened, recorded before the unlink is attempted: the barrier covers each of them.
    pending: list[tuple[str, str]] = []
    unlinked: list[tuple[str, str]] = []
    # The requests settled (withdrawn, nothing to withdraw, or kept and reported); an interrupt anywhere in the
    # withdrawal reports the rest by id.
    settled: set[str] = set()
    stopping: BaseException | None = None
    here: dict[str, JobRef] = {}
    try:
        armed: list[tuple[JobRef, str, Path]] = []
        for ref, request_id, path in candidates:
            try:
                if _fs.lstat(_fs.loc(path)) is None:
                    settled.add(request_id)  # never published, or retired with its job meanwhile: nothing to withdraw
                    continue
            except _WITHDRAWAL_ERRORS as exc:
                _LOGGER.warning("request %s for job %s was not withdrawn: %s", request_id, ref.job_id, exc)
                settled.add(request_id)
                continue
            armed.append((ref, request_id, path))
        if armed:
            try:
                active = owner()
                active.check_alive()
                here = {ref.job_id: ref for ref in active.owned()}
            except _WITHDRAWAL_ERRORS as exc:
                for ref, request_id, _path in armed:
                    _LOGGER.warning(
                        "request %s for job %s was not withdrawn (%s); it may still be applied",
                        request_id,
                        ref.job_id,
                        exc,
                    )
                    settled.add(request_id)
                armed = []
        for ref, request_id, path in armed:
            assert active is not None
            try:
                handle = _held_for_withdrawal(workspace, active, progress.index, ref.job_id, here, claimed)
                if isinstance(handle, str):
                    _LOGGER.warning(
                        "request %s for job %s was not withdrawn (%s); it may still be applied",
                        request_id,
                        ref.job_id,
                        handle,
                    )
                    settled.add(request_id)
                    continue
                if (kept := _kept_for_recovery(handle, path, request_id)) is not None:
                    _LOGGER.warning("request %s for job %s kept: %s", request_id, ref.job_id, kept)
                    settled.add(request_id)
                    continue
                pending.append((ref.job_id, request_id))
                _fs.remove_file(_fs.loc(path), durable=False)
            except _WITHDRAWAL_ERRORS as exc:
                _LOGGER.warning(
                    "request %s for job %s was not withdrawn (%s); it may still be applied", request_id, ref.job_id, exc
                )
                settled.add(request_id)
                continue
            unlinked.append((ref.job_id, request_id))
            settled.add(request_id)
    except BaseException as exc:
        stopping = exc  # an interrupt: the unlinks made so far are still sealed before any job goes back
        for ref, request_id, _path in candidates:
            if request_id not in settled:
                _LOGGER.warning(
                    "request %s for job %s: its withdrawal was cut short (%s); it may still be applied",
                    request_id,
                    ref.job_id,
                    type(exc).__name__,
                )
    if (failed := _seal_withdrawals(workspace, pending)) is not None:
        # No job may go back while a withdrawal is not durable: fail-stop. The flight is never left, so the owner's
        # exit touches nothing and keeps owner.json; recovery returns every job it holds.
        assert active is not None  # an unlink was attempted, so the owner holds its job
        active.enter_flight()
        for job_id, request_id in pending:
            _LOGGER.warning(
                "the withdrawal of request %s for job %s is not durable (%s); its jobs stay held for recovery",
                request_id,
                job_id,
                failed,
            )
        raise stopping if stopping is not None else failed
    for job_id, request_id in unlinked:
        _LOGGER.warning(
            "request %s for job %s withdrawn while held by this command; it had not been applied", request_id, job_id
        )
    # Only now, the withdrawals durable, do the jobs claimed for them go back.
    for handle in claimed:
        try:
            handle.give_back()
        except _WITHDRAWAL_ERRORS as exc:
            _LOGGER.warning("job %s stays with this command's owner after a withdrawal: %s", handle.job_id, exc)
        progress.index.moved(handle.job_id, handle)
    if stopping is not None:
        raise stopping


#: The most of a job's run log a withdrawal reads for an owner's record of applying a request; a longer log keeps it.
_RUNLOG_EVIDENCE_LIMIT = 64 << 20
#: How much of the run log one read takes.
_RUNLOG_CHUNK = 1 << 20


def _application_logged(handle: OwnedJob, request_id: str) -> bool | None:
    """Whether the held job's run log records any owner's ``request_applied`` of *request_id*; ``None`` if unknown.

    ``apply_requests`` appends that record durably before any effect of the request, so its absence proves that no
    owner began applying it. The whole log is read (``None`` when it cannot be, or passes the limit).
    """

    needle, offset, carry = request_id.encode(), 0, b""
    while True:
        try:
            data, offset = _fs.read_tail(handle.path, "logs/runlog.jsonl", _RUNLOG_CHUNK, offset)
        except (OSError, _fs.UnsafePath):
            return None
        if offset > _RUNLOG_EVIDENCE_LIMIT:
            return None
        lines = (carry + data).split(b"\n")
        carry = lines.pop() if data else b""  # a line the next read completes; at the end, the last line is whole
        for line in lines:
            if needle not in line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                return None  # a line naming the request that cannot be read: it may be the record
            detail = entry.get("detail") if isinstance(entry, dict) else None
            if (
                isinstance(detail, dict)
                and entry.get("event") == "request_applied"
                and detail.get("request_id") == request_id
            ):
                return True
        if not data:
            return False


def _kept_for_recovery(handle: OwnedJob, path: Path, request_id: str) -> str | None:
    """Why a request of a held job must stay although this command never began applying it, or ``None``.

    Any owner's run-log record of applying it: that owner (dead, its job returned) may have begun its effect, and the
    pending request is what finishes it. A job whose ``state.json`` shows an attempt that may be live (``phase`` not
    ``idle``) or an unfinished commit as well: a manager signals a running attempt for a ``cancel`` before it logs
    the cancel. A ``delete`` or ``eject`` of an exchange-origin job as well: an exchange index may have been dropped.
    A request, run log or ``state.json`` that cannot be read keeps it too.
    """

    try:
        request = _requests.parse(path)
    except FileNotFoundError:
        return None
    except FormatError as exc:
        return f"it cannot be read: {exc}"
    logged = _application_logged(handle, request_id)
    if logged is None:
        return "the job's run log cannot be read"
    if logged:
        return "an owner began applying it"
    try:
        doc = handle.read_state()
    except (FormatError, _fs.UnsafePath, OSError):
        return "a previous owner may have begun applying it"
    if doc is not None and (doc.phase["kind"] != "idle" or doc.commit is not None):
        # A manager acts on some requests before it logs them: it signals a running attempt for a cancel and records
        # the cancel only at the attempt's commit. An attempt that may be live, or a commit that may be unfinished,
        # may already carry the request's effect.
        return "the job had a running attempt"
    if request.action in ("delete", "eject") and doc is not None and doc.origin == "exchange":
        return "a previous owner may have begun applying it"
    return None


def _seal_withdrawals(workspace: "Workspace", pending: Sequence[tuple[str, str]]) -> BaseException | None:
    """Fsync ``requests/`` once for the withdrawals that may have been made; what stopped it, or ``None``."""

    if not pending or not workspace.durable:
        return None
    try:
        descriptor = _fs.open_dir(workspace.control / "requests")
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException as exc:
        return exc
    return None


#: Prepares one job's request: the job as located and its verified document, or the job's outcome when there is none.
type _Prepare = Callable[[], tuple[JobRef, Mapping[str, object]] | RequestOutcome]


def _post_and_serve(
    workspace: "Workspace",
    posts: Sequence[tuple[str, _Prepare]],
    *,
    owner: Callable[[], _kernel.Owner],
    index: LocationIndex,
    report: Callable[[Sequence[RequestOutcome]], None] | None = None,
) -> list[RequestOutcome]:
    """Post and serve one command's requests chunk by chunk (plan WORKFLOW_LOCAL_COMMANDS §4.3).

    Each post is a job id and its preparation. A chunk holds at most :data:`_CHUNK_JOBS` jobs, none twice: every
    request is named in the chunk's record before it is published, then all are served from one order snapshot, in
    order, by the owner *owner* returns (asked for when the first chunk is served). At the end of a chunk the requests
    whose outcome is ``uncertain`` are withdrawn (:func:`_withdraw`); on an exception or an interrupt, so is every
    request of the chunk without an outcome, the outcomes known so far are reported, and the original exception
    propagates to the owner, which gives its jobs back as before. Requests left armed by a SIGKILL, or a signal the
    CLI does not handle, are at most one chunk's.

    :param workspace: The local workspace.
    :param posts: The jobs and their preparations, in input order.
    :param owner: Returns the command's CLI owner.
    :param index: The command's location index.
    :param report: Called with each chunk's outcomes once it is served, or with those known when it stops.
    :return: One outcome per post, in input order.
    """

    outcomes: list[RequestOutcome] = []
    for chunk in _chunks(posts):
        made: list[RequestOutcome | None] = []
        posted: list[tuple[JobRef, Path]] = []
        progress = _Progress(index)
        try:
            for _job_id, prepare in chunk:
                prepared = prepare()
                if isinstance(prepared, RequestOutcome):
                    made.append(prepared)
                    continue
                ref, document = prepared
                made.append(None)
                # Recorded before it is published: a post that renamed its file and then raised is withdrawn too.
                posted.append((ref, _kernel.request_file(workspace, document)))
                _kernel.post_request(workspace, document)
            if posted:
                items = [(ref, path.name.split(".")[1]) for ref, path in posted]
                snapshots = _order_snapshot(workspace, dict.fromkeys(ref.job_id for ref, _path in posted))
                _serve_snapshot(workspace, owner(), items, snapshots, progress)
        except BaseException:
            try:
                _withdraw(workspace, owner, posted, progress, stopped=True)
            except BaseException:
                _LOGGER.warning("the withdrawal of this command's unserved requests stopped", exc_info=True)
            _report_known(report, made, posted, progress)
            raise
        try:
            _withdraw(workspace, owner, posted, progress, stopped=False)
        except BaseException:
            # The withdrawal reported what it left; no second cleanup. The outcomes known so far are reported.
            _report_known(report, made, posted, progress)
            raise
        chunk_outcomes = [outcome for outcome in _known(made, posted, progress) if outcome is not None]
        if report is not None:
            report(chunk_outcomes)
        outcomes += chunk_outcomes
    return outcomes


def _report_known(
    report: Callable[[Sequence[RequestOutcome]], None] | None,
    made: Sequence[RequestOutcome | None],
    posted: Sequence[tuple[JobRef, Path]],
    progress: _Progress,
) -> None:
    """Report the outcomes of a chunk known when the command stopped, in input order; never raise doing so."""

    if report is None:
        return
    try:
        report([outcome for outcome in _known(made, posted, progress) if outcome is not None])
    except BaseException:
        _LOGGER.warning("the outcomes known before the command stopped were not reported", exc_info=True)


def _known(
    made: Sequence[RequestOutcome | None], posted: Sequence[tuple[JobRef, Path]], progress: _Progress
) -> list[RequestOutcome | None]:
    """Each prepared job's outcome so far, in input order: its own, or its posted request's (``None`` if none yet)."""

    served = iter(posted)
    known: list[RequestOutcome | None] = []
    for outcome in made:
        if outcome is None and (post := next(served, None)) is not None:
            outcome = progress.results.get(post[1].name.split(".")[1])
        known.append(outcome)
    return known


def _post_now(
    index: LocationIndex, ref: JobRef, action: str, reason: str, copy: bool
) -> tuple[JobRef, Mapping[str, object]] | RequestOutcome:
    """Prepare one unsigned CLI request for a job found where it was observed; the job's outcome otherwise.

    A job that is not found, or whose ``job.json`` cannot be read for its placement, gets an ``uncertain`` outcome.
    """

    placement = ref.placement
    if placement is None:
        try:
            placement = JobDefinition.from_path(ref.path / "job.json").placement
        except (OSError, FormatError, _fs.UnsafePath) as exc:
            return RequestOutcome(ref.job_id, "", "uncertain", f"the job cannot be read: {exc}")
    current = index.locate(ref.job_id, placement_hint=placement)
    if current is None:
        return RequestOutcome(ref.job_id, "", "uncertain", "the job was not found")
    document = _requests.compose(
        action=action,
        job_id=ref.job_id,
        placement=placement,
        operator="cli",
        reason=reason,
        no_copy=action == "unlock" and not copy,
    )
    return current, document


def request_now(
    workspace: "Workspace",
    owner: _kernel.Owner,
    ref: JobRef,
    action: str,
    reason: str,
    *,
    outcomes: list[RequestOutcome] | None = None,
    copy: bool = True,
) -> str | None:
    """Post one request for a job and apply it now when the job is unowned (``job delete|seal|unseal|detach``).

    A job another owner holds gets the request at its next boundary. Nothing is posted for a job that is not found,
    or whose ``job.json`` cannot be read. :func:`request_jobs` is its many-job counterpart.

    :param workspace: The workspace.
    :param owner: The CLI owner that applies it.
    :param ref: The job, as last observed.
    :param action: The request action.
    :param reason: The operator's reason.
    :param outcomes: Append the immediate typed result when supplied.
    :param copy: Whether an ``unlock`` copies the job's runner (``False`` posts ``no_copy``); other actions
        ignore it.
    :return: ``None`` when the request took effect, otherwise why it did not (yet).
    """

    index = LocationIndex(workspace)
    prepared = _post_now(index, ref, action, reason, copy)
    if isinstance(prepared, RequestOutcome):
        outcome = prepared
    else:
        current, document = prepared
        path = _kernel.post_request(workspace, document)
        (outcome,) = _serve_requests(workspace, [(current, path)], owner=owner, index=index)
    if outcomes is not None:
        outcomes.append(outcome)
    return None if outcome.status == "applied" else outcome.explanation


def request_jobs(
    workspace: "Workspace",
    owner: _kernel.Owner,
    refs: Sequence[JobRef],
    action: str,
    reason: str,
    *,
    copy: bool = True,
    report: Callable[[Sequence[RequestOutcome]], None] | None = None,
) -> list[RequestOutcome]:
    """Post one request per job and apply it now where the job is unowned: :func:`request_now` for many jobs.

    The requests are posted and served in chunks of at most 256 jobs. A request of the command left armed by an
    ``uncertain`` outcome, or by an error or interrupt before it was served, is withdrawn while the command holds its
    job, unless its application began; what cannot be withdrawn is logged by id.

    :param workspace: The workspace.
    :param owner: The CLI owner that applies them.
    :param refs: The jobs, as last observed.
    :param action: The request action.
    :param reason: The operator's reason.
    :param copy: Whether an ``unlock`` copies the job's runner.
    :param report: Called with each chunk's outcomes once the chunk is served (or with those known when it stops).
    :return: One outcome per job, in order.
    """

    return _request_jobs(
        workspace, owner, refs, action, reason, copy=copy, index=LocationIndex(workspace), report=report
    )


def _request_jobs(
    workspace: "Workspace",
    owner: _kernel.Owner,
    refs: Sequence[JobRef],
    action: str,
    reason: str,
    *,
    copy: bool,
    index: LocationIndex,
    report: Callable[[Sequence[RequestOutcome]], None] | None = None,
) -> list[RequestOutcome]:
    """Request as :func:`request_jobs` does, through the command's location index *index*."""

    posts = [(ref.job_id, partial(_post_now, index, ref, action, reason, copy)) for ref in refs]
    return _post_and_serve(workspace, posts, owner=lambda: owner, index=index, report=report)


def remove_jobs(workspace: "Workspace", refs: Iterable[JobRef], *, force: bool = False) -> RemovalReport:
    """Post a ``delete`` request for each job and apply it now when the job is unowned, as :func:`request_jobs` does.

    :param workspace: The workspace.
    :param refs: The jobs, as last observed.
    :param force: Ignored; a ``delete`` request has no force (the waiting-parent check holds).
    :return: The per-job outcomes.
    """

    del force
    return _remove_jobs(workspace, refs, LocationIndex(workspace))


def _remove_jobs(workspace: "Workspace", refs: Iterable[JobRef], index: LocationIndex) -> RemovalReport:
    """Remove as :func:`remove_jobs` does, through the command's location index *index*."""

    selected = list(refs)
    with _kernel.register_owner(workspace, kind="cli", label="job delete", allocation=None, advertised={}) as owner:
        observed = _request_jobs(workspace, owner, selected, "delete", "job delete", copy=True, index=index)
    outcomes = [
        RemovalOutcome(
            ref.job_key,
            ref.state,
            outcome.status == "applied",
            None if outcome.status == "applied" else outcome.explanation,
            outcome,
        )
        for ref, outcome in zip(selected, observed, strict=True)
    ]
    return RemovalReport(tuple(outcomes))
