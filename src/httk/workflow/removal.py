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
import logging
import os
import re
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, cast

from . import _fs, _joins, _kernel, _moving, _requests, _store
from ._job import JobDefinition
from ._kernel import JobRef, ListingCache, OwnedJob, Release
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


def _serve_one(
    workspace: "Workspace", owner: _kernel.Owner, ref: JobRef, request_id: str
) -> tuple[RequestOutcome, bool]:
    """Serve one request of an unowned job as *owner*; the flag says that later requests of the job must wait."""

    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
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
    result = _history_outcome(workspace, ref, request_id)
    if (
        not claimed
        and result.status == "uncertain"
        and _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True) is not None
    ):
        result = dataclasses.replace(result, status="queued", explanation="another owner claimed the job first")
    return result, result.status not in _SETTLED


def serve_requests(
    workspace: "Workspace", published: Iterable[tuple[JobRef, Path]], *, owner: _kernel.Owner | None = None
) -> tuple[RequestOutcome, ...]:
    """Immediately serve a bounded snapshot of published requests for unowned jobs.

    Requests for each job run in ``(created_at, request_id)`` order through
    the requested id. A live owner, lost claim or unfinished reconciliation
    stops at the request it meets and leaves the requested one queued; a job
    with more than 256 pending requests is not served here at all, leaving
    them to a manager. Arrivals after the snapshot are not considered.

    :param workspace: The local workspace.
    :param published: Published request files paired with their observed jobs.
    :param owner: An existing CLI owner, or ``None`` to register one.
    :return: One result per published request, in input order.
    """

    items = [(ref, path.name.split(".")[1]) for ref, path in published]
    if not items:
        return ()
    results: dict[str, RequestOutcome] = {}
    snapshots: dict[str, list[_requests.Request] | None] = {}
    for ref, _request_id in items:
        if ref.job_id not in snapshots:
            paths = list(islice((workspace.control / "requests").glob(f"{ref.job_id}.*.json"), _SNAPSHOT_LIMIT + 1))
            found = [request for path in paths if (request := _parse(path)) is not None]
            snapshots[ref.job_id] = None if len(paths) > _SNAPSHOT_LIMIT else sorted(found, key=_order)

    def process(active: _kernel.Owner) -> None:
        for ref, request_id in items:
            ordered = snapshots[ref.job_id]
            if ordered is None:
                explanation = f"more than {_SNAPSHOT_LIMIT} requests are pending; they wait for a manager"
                results[request_id] = RequestOutcome(ref.job_id, request_id, "queued", explanation)
                continue
            target = next((request for request in ordered if request.request_id == request_id), None)
            if target is None:
                results[request_id] = _history_outcome(workspace, ref, request_id)
                continue
            for request in ordered:
                if _order(request) > _order(target):
                    break
                previous = results.get(request.request_id)
                if previous is not None and previous.status in _SETTLED:
                    continue
                result, halt = _serve_one(workspace, active, ref, request.request_id)
                results[request.request_id] = result
                if halt:
                    # The requested one waits behind it, for the same reason.
                    results[request_id] = dataclasses.replace(result, request_id=request_id)
                    break

    if owner is None:
        with _kernel.register_owner(
            workspace, kind="cli", label="immediate job requests", allocation=None, advertised={}
        ) as active:
            process(active)
    else:
        process(owner)
    return tuple(results[request_id] for _ref, request_id in items)


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

    A job another owner holds gets the request at its next boundary. Nothing is posted for a job that is not found.

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

    placement = ref.placement if ref.placement is not None else JobDefinition.from_path(ref.path / "job.json").placement
    current = _kernel.locate(workspace, ref.job_id, placement_hint=placement)
    if current is None:
        if outcomes is not None:
            outcomes.append(RequestOutcome(ref.job_id, "", "uncertain", "the job was not found"))
        return "the job was not found"
    path = _requests.post(
        workspace,
        action=action,
        job_id=ref.job_id,
        placement=placement,
        operator="cli",
        reason=reason,
        no_copy=action == "unlock" and not copy,
    )
    (outcome,) = serve_requests(workspace, [(current, path)], owner=owner)
    if outcomes is not None:
        outcomes.append(outcome)
    return None if outcome.status == "applied" else outcome.explanation


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
