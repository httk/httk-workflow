"""The Python authoring SDK for native *httk₂* workflow runners.

One runner is one program that implements the steps of one workflow. Steps are
registered on a :class:`Runner`, and :meth:`Runner.main` dispatches the step the
manager asked for to the handler that implements it, giving it one
:class:`Attempt` object:

.. code-block:: python

    from httk.workflow import ChildSpec, Runner

    run = Runner("defects")


    @run.step
    def characterize(a):
        for site in range(a.parameter("sites")):
            a.spawn(ChildSpec(step="relax", parameters={"site": site}), label=f"site-{site}")
        a.gather("aggregate", on_impossible="triage")


    if __name__ == "__main__":
        raise SystemExit(run.main())

Nothing declares the shape of the workflow up front: a step decides at run time
which children to spawn and which step runs next, so the graph of a job is
whatever its steps published. Exactly one outcome is published per attempt, and
the handler that returns without publishing one, or raises, is reported as such
instead of leaving the attempt ambiguous.
"""

import json
import logging
import os
import shutil
import sys
import traceback
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, Self, cast, overload

from . import _kernel
from ._children import newest_labeled
from ._data import Transaction
from ._durations import cap_maxtime
from ._job import JobDefinition
from ._state import StateDoc, _thaw, read_state_unowned
from ._util import json_bytes, read_json, require_string, validate_inputs, write_json_atomic
from .errors import FormatError, WorkflowError
from .models import (
    JOB_STATE_DIRECTORY,
    RESERVED_WORKFLOW_ENVIRONMENT_PREFIX,
    Failure,
    _matches_environment_type,
    environment_variable_name,
    normalize_resources,
    parse_placement_text,
    placement_text,
    validate_declaration_name,
    validate_declarations,
    validate_failure,
    validate_label,
    validate_resources,
    validate_step,
)
from .runtime import AttemptContext, CommandResult, _read_environment, run_command
from .runtime_builders import (
    ChildReference,
    JobSpec,
    JobState,
    JoinCondition,
    OutcomeAction,
    OutcomeDraft,
    ReplayableWorkdirBatch,
    RunLog,
    join_mapping,
)

#: The failure code of a job called through a call slot whose requested outputs cannot be recorded.
_OUTPUTS_UNAVAILABLE = "workflow.outputs_unavailable"
#: The ``.httk-job/`` file a job called through a call slot records its requested outputs in, and its format.
_OUTPUTS_FILE = "outputs.json"
_OUTPUTS_FORMAT = "httk-workflow-outputs"
_OUTPUTS_LIMIT = 64 << 20

if TYPE_CHECKING:
    from ._slots import SlotMapping
    from ._store import Installed
    from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "RUNNER_DESCRIPTION_FORMAT",
    "RUNNER_ERROR_FORMAT",
    "Attempt",
    "ChildResult",
    "ChildSpec",
    "ChildrenView",
    "InstantiateHandler",
    "ParentJob",
    "Runner",
]

RUNNER_DESCRIPTION_FORMAT = "httk-workflow-runner-description"
RUNNER_ERROR_FORMAT = "httk-workflow-runner-error"
_DESCRIBE_VARIABLE = "HTTK_WORKFLOW_DESCRIBE"
_DESCRIBE_FLAG = "--describe"
_ENVIRONMENT_MARKER = ".httk-environment-resolution.json"

type StepHandler = Callable[["Attempt"], object]
type InstantiateHandler = Callable[[Any], object]
type EnvironmentSource = Literal["override", "environment-variable", "workspace-setting", "default"]
#: Inside an outcome draft: the six-digit sequence of the attempt's implicit transaction, for a resuming process.
_IMPLICIT_TRANSACTION = ".implicit-transaction"
_NESTED_PLACEMENT_BUDGET = 1024


class _Missing:
    """The sentinel telling a missing parameter from a parameter that is null."""

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "<missing>"


_MISSING = _Missing()


class _EnvironmentResolutionError(ValueError):
    """Identify the declared environment entry whose external value was invalid."""

    def __init__(self, name: str, message: str) -> None:
        self.name = name
        super().__init__(message)


def _decode_environment_layer(name: str, metadata: Mapping[str, object], value: object, layer: str) -> object:
    """Decode and validate one external workflow environment layer."""

    environment_type = metadata.get("type")
    if not isinstance(environment_type, str):
        return value
    if environment_type == "string" and isinstance(value, str):
        return value
    decoded = value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise _EnvironmentResolutionError(
                name,
                f"workflow environment {name!r} from {layer} is not valid JSON for declared type {environment_type!r}",
            ) from exc
    if not _matches_environment_type(decoded, environment_type):
        raise _EnvironmentResolutionError(
            name,
            f"workflow environment {name!r} from {layer} does not match declared type {environment_type!r}",
        )
    return decoded


def _resolve_environment_value(
    job: JobDefinition,
    context_settings: Mapping[str, object],
    name: str,
    default: object = _MISSING,
    *,
    include_process_environment: bool = True,
) -> tuple[object, EnvironmentSource | None]:
    """Resolve one declared environment value using the SDK's layer order."""

    environment = cast(Mapping[str, object], _thaw(job.environment))
    declared = environment.get("declared", {})
    if not isinstance(declared, Mapping) or name not in declared:
        available = ", ".join(sorted(declared)) if isinstance(declared, Mapping) else "none"
        raise KeyError(f"workflow environment {name!r} is not declared; declared environment: {available or 'none'}")
    metadata = declared[name]
    if not isinstance(metadata, Mapping):  # pragma: no cover - JobDefinition validates this
        raise KeyError(f"workflow environment {name!r} is malformed")
    setting = metadata.get("setting", name)
    if not isinstance(setting, str):  # pragma: no cover - JobDefinition validates this
        setting = name
    variable = environment_variable_name(setting)
    if variable.startswith(RESERVED_WORKFLOW_ENVIRONMENT_PREFIX):
        raise _EnvironmentResolutionError(
            name,
            f"workflow environment {name!r} derives reserved {RESERVED_WORKFLOW_ENVIRONMENT_PREFIX!r} "
            f"variable {variable!r}; choose a workflow setting outside the manager-owned namespace",
        )
    overrides = environment.get("overrides", {})
    if isinstance(overrides, Mapping) and name in overrides:
        return overrides[name], "override"
    if include_process_environment and variable in os.environ:
        return _decode_environment_layer(name, metadata, os.environ[variable], f"environment variable {variable}"), (
            "environment-variable"
        )
    if setting in context_settings:
        return _decode_environment_layer(
            name, metadata, context_settings[setting], f"workspace setting {setting!r}"
        ), "workspace-setting"
    if "default" in metadata:
        return metadata["default"], "default"
    if isinstance(default, _Missing):
        return None, None
    return default, None


def resolve_declared_environment(
    job: JobDefinition,
    context_settings: Mapping[str, object],
    *,
    include_process_environment: bool = True,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    """Resolve every declared workflow environment entry before a step runs.

    :param job: The immutable job definition.
    :param context_settings: The workspace settings captured for this attempt.
    :param include_process_environment: Include this process's environment layer.
    :return: Resolved values with their source layer, and names without a value.
    :raises ValueError: If an environment or workspace layer has the wrong type.
    """

    declared = cast(Mapping[str, object], _thaw(job.environment)).get("declared", {})
    if not isinstance(declared, Mapping):  # pragma: no cover - JobDefinition validates this
        return {}, []
    values: dict[str, dict[str, object]] = {}
    unresolved: list[str] = []
    for name in sorted(declared):
        value, source = _resolve_environment_value(
            job,
            context_settings,
            name,
            include_process_environment=include_process_environment,
        )
        if source is None:
            unresolved.append(name)
        else:
            values[name] = {"value": value, "source": source}
    return values, unresolved


def _short_environment_value(value: object) -> str:
    """Render one resolved value for the bounded one-line run note."""

    try:
        rendered = json.dumps(value, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):  # pragma: no cover - job environment is JSON-validated
        rendered = repr(value)
    return rendered if len(rendered) <= 80 else rendered[:77] + "..."


def _environment_log_message(values: Mapping[str, Mapping[str, object]]) -> str:
    """Build the one-line note for a resolved environment document."""

    pairs = ", ".join(
        f"{name}={_short_environment_value(item['value'])}({item['source']})" for name, item in values.items()
    )
    return f"parameters are in job.json; environment resolved as {pairs}"


@dataclass(frozen=True)
class ChildSpec:
    """A complete child job described by the step and parameters it starts with.

    A child of a :class:`ChildSpec` runs the spawning job's installed workflow
    (another workflow is started with :meth:`Attempt.call`). Everything not given
    follows the spawning job: its claim pool, its priority, its resources and
    its sealing. The child therefore differs from its parent in exactly what the
    campaign varies, which is normally only *step* and *parameters*.

    :param step: The first step the child runs.
    :param parameters: The opaque implementation knobs given to the child.
    :param declarations: The workflow declarations carried by the child.
    :param name: The child job name, or a generated name when omitted.
    :param tag: The child's optional job tag, or the spawn label when omitted.
    :param priority: The child's priority, or the parent's when omitted.
    :param claim_pool: The child's claim pool, or the parent's when omitted.
    :param required_capabilities: Capabilities required by the child.
    :param resources: Resources requested by the child, or the parent's without ``mintime`` when
        omitted; ``maxtime`` and ``mintime`` are Slurm duration strings.
    :param step_resources: Per-step resources requested by the child, or the parent's without
        ``mintime`` when omitted; time labels are spelled as in *resources*.
    :param maximum_attempts_per_activation: The child's per-activation attempt budget.
    :param maximum_total_attempts: The child's total attempt budget.
    :param maximum_activations: The child's activation budget.
    :param retry_on: Failure codes eligible for manager-detected retry.
    """

    step: str
    parameters: Mapping[str, object] = field(default_factory=dict)
    #: The child's own workflow declarations. A declaration describes the job it
    #: belongs to, so nothing is inherited: a child that declares carries what it
    #: was given here, and a child that declares nothing carries nothing.
    declarations: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    name: str | None = None
    tag: str | None = None
    priority: int | None = None
    claim_pool: str | None = None
    required_capabilities: tuple[str, ...] = ()
    resources: Mapping[str, int | str] | None = None
    step_resources: Mapping[str, Mapping[str, int | str]] | None = None
    maximum_attempts_per_activation: int | None = None
    maximum_total_attempts: int | None = None
    maximum_activations: int | None = None
    retry_on: tuple[str, ...] = ()

    def _job_spec(
        self, parent: JobDefinition, workflow: tuple[str, str], label: str, maxtime_cap: int | None = None
    ) -> JobSpec:
        """Return the job specification of this child under *parent*.

        The child runs *workflow* (``(id, name)``), the installation the spawning attempt was launched from
        (the parent's effective workflow, not necessarily ``job.json``'s) with the parent's call slot
        bindings; a child of the parent's embedded workflow embeds it too (the manager copies the parent's
        ``workflow/`` into it at publication). A child never inherits
        ``mintime``, and every ``maxtime`` it ends with is at most *maxtime_cap*, the spawning attempt's
        effective ``maxtime``.
        """

        if self.resources is None:
            inherited = validate_resources(_thaw(parent.resources))
            resources = {name: value for name, value in inherited.items() if name != "mintime"}
        else:
            resources = normalize_resources(self.resources, "child resources")
        if self.step_resources is None:
            step_resources = {
                step: {
                    name: value for name, value in validate_resources(_thaw(requirement)).items() if name != "mintime"
                }
                for step, requirement in parent.step_resources.items()
            }
        else:
            step_resources = {
                step: normalize_resources(requirement, f"child step_resources.{step}")
                for step, requirement in self.step_resources.items()
            }
        resources, step_resources = cap_maxtime(resources, step_resources, maxtime_cap)
        return JobSpec(
            name=self.name or f"{parent.name}: {self.step} ({label})",
            workflow_id=workflow[0],
            workflow_name=workflow[1],
            initial_step=validate_step(self.step, "child step"),
            tag=self.tag or label,
            priority=parent.priority if self.priority is None else self.priority,
            claim_pool=parent.claim_pool if self.claim_pool is None else self.claim_pool,
            required_capabilities=self.required_capabilities,
            maximum_attempts_per_activation=self.maximum_attempts_per_activation,
            maximum_total_attempts=self.maximum_total_attempts,
            maximum_activations=self.maximum_activations,
            retry_on=self.retry_on,
            resources=resources,
            step_resources=step_resources,
            parameters=dict(self.parameters),
            declarations=validate_declarations(self.declarations, "child declarations"),
            seal_succeeded=parent.seal_succeeded,
            # The child runs the parent's workflow, so it keeps the parent's call slot bindings.
            calls=None if parent.calls is None else cast(Mapping[str, Mapping[str, object]], _thaw(parent.calls)),
            embedded=(
                cast(Mapping[str, object], _thaw(parent.embedded))
                if parent.embedded is not None and workflow[0] == parent.workflow_id
                else None
            ),
        )


@dataclass(frozen=True)
class ChildResult:
    """What a step may know about one child its job spawned.

    Every member is derived from this job's own ``state.json``: the join's
    observations for :attr:`Attempt.children`, the published children for
    :attr:`Attempt.spawned`. The children are located in the workspace
    (read-only, one listing of their placements) when the view is first read.
    Paths are absolute, and ``None`` when the child was not found. A child's
    directory moves when it is claimed, so a path read here is valid while the
    child stays terminal.
    """

    label: str | None
    job_id: str
    job_key: str
    kind: str
    failure: Failure | None
    placement: PurePosixPath
    payload: Path | None
    workdir: Path | None
    data: Path | None
    raw: Mapping[str, object]

    @property
    def succeeded(self) -> bool:
        """Report whether this child ended successfully."""

        return self.kind == "succeeded"

    @property
    def failed(self) -> bool:
        """Report whether this child ended badly."""

        return self.kind in {"failed", "cancelled"}


def _child_result(raw: Mapping[str, object], kind: str, failure: object, ref: _kernel.JobRef | None) -> ChildResult:
    """Build one :class:`ChildResult` from a ``state.json`` record and where the child was located."""

    label = raw.get("label")
    return ChildResult(
        label=None if label is None else validate_label(label, "child label"),
        job_id=require_string(raw.get("job_id"), "child job_id"),
        job_key=require_string(raw.get("job_key"), "child job_key"),
        kind=kind,
        failure=None if not isinstance(failure, Mapping) else validate_failure(failure, "child failure"),
        placement=parse_placement_text(raw.get("placement"), "child placement"),
        payload=None if ref is None else ref.path,
        workdir=None if ref is None else ref.path / "run",
        data=None if ref is None else ref.path / "data",
        raw=raw,
    )


@dataclass(frozen=True)
class ParentJob:
    """Where the job that spawned this one lives, for reading its files in place.

    The parent is found from the immutable ``parent`` member every spawned
    child's ``job.json`` carries: a read-only lookup at its placement in this
    workspace. Nothing is copied or frozen: the parent's workdir is ordinary
    storage its own steps may still change, so a parent that shares files this
    way writes them before it publishes the spawning outcome and leaves them
    alone until every child that reads them is terminal. The parent's
    directory moves while a manager holds it, so the paths are those of the
    moment of the lookup. Paths are absolute.

    :param job_id: The parent job UUID.
    :param job_key: The parent job key.
    :param placement: The parent's workspace placement.
    :param payload: The parent's job directory.
    :param workdir: The parent's persistent workdir, ``run/`` of its job directory.
    :param raw: The ``parent`` member of this job's ``job.json``, verbatim.
    """

    job_id: str
    job_key: str
    placement: PurePosixPath
    payload: Path
    workdir: Path
    raw: Mapping[str, object]


@dataclass(frozen=True)
class ChildrenView:
    """A sequence of children: those observed by a join, or every child a job published.

    :attr:`Attempt.children` is empty for an activation that follows no join, so
    a step can read it unconditionally.
    """

    all: tuple[ChildResult, ...] = ()

    @property
    def succeeded(self) -> tuple[ChildResult, ...]:
        """The children that ended successfully, in spawn order."""

        return tuple(child for child in self.all if child.succeeded)

    @property
    def failed(self) -> tuple[ChildResult, ...]:
        """The children that ended badly, in spawn order."""

        return tuple(child for child in self.all if child.failed)

    @property
    def labels(self) -> tuple[str, ...]:
        """The labels of every observed child, in spawn order."""

        return tuple(child.label for child in self.all if child.label is not None)

    def get(self, label: str, default: ChildResult | None = None) -> ChildResult | None:
        """Return the child spawned under *label*, or *default*.

        A label names one child of a job, as a spawn of an existing child's label reuses that child; only
        after a child that no longer existed was created anew does a label name several children, and then
        the most recently published one (the last in the view) is returned.

        :param label: The spawn label to find.
        :param default: The value to return when no child has that label.
        :return: The matching child, or the default value.
        """

        for child in reversed(self.all):
            if child.label == label:
                return child
        return default

    def __getitem__(self, label: str) -> ChildResult:
        child = self.get(label)
        if child is None:
            raise KeyError(f"no child was spawned under label {label!r}; observed labels: {', '.join(self.labels)}")
        return child

    def __contains__(self, label: object) -> bool:
        return any(child.label == label for child in self.all)

    def __iter__(self) -> Iterator[ChildResult]:
        return iter(self.all)

    def __len__(self) -> int:
        return len(self.all)


class Attempt:
    """Everything one attempt of one step may read, do, and publish.

    An attempt owns exactly one implicit outcome draft. The draft is created by
    the first :meth:`spawn`, :meth:`call` or :meth:`put`, and it is published by
    exactly one of :meth:`advance`, :meth:`gather`, :meth:`succeed`,
    :meth:`retry`, :meth:`pause`, or :meth:`fail`. Publication is the single
    atomic rename the manager observes, so nothing a step did takes effect until
    the step says how it ended; only a :meth:`transaction` committed earlier is
    applied at the next attempt boundary whatever the outcome.

    :param context: The manager-written identity and restart context.
    :param control: The attempt control directory.
    :param payload: The immutable job payload directory.
    :param workdir: The directory in which the step works.
    :param workspace: The workspace root containing the job.
    :param data: The job's data directory, ``data/`` of the payload when omitted.
    :param step: The step this attempt runs, or the context step when omitted.
    :param runner: The runner dispatching this attempt, when available.
    """

    def __init__(
        self,
        context: AttemptContext,
        *,
        control: Path,
        payload: Path,
        workdir: Path,
        workspace: Path,
        data: Path | None = None,
        step: str | None = None,
        runner: "Runner | None" = None,
    ) -> None:
        self.context = context
        self.control = control
        self.payload = payload
        self.workdir = workdir
        self.workspace = workspace
        self.data = payload / "data" if data is None else data
        self.step = step or context.step
        # Every artifact this attempt publishes inherits the workspace's
        # durability from the manager-written context, so a durable workspace
        # synchronizes an outcome, a transaction, or a spawned child before it
        # is renamed authoritative without the step asking.
        self.state = JobState(payload, durable=context.durable)
        self.log = RunLog(payload)
        self._runner = runner
        self._draft: OutcomeDraft | None = None
        self._implicit: Transaction | None = None
        self._published: Path | None = None
        self._action: str | None = None
        self._job: JobDefinition | None = None
        self._parameters: dict[str, object] | None = None
        self._children: ChildrenView | None = None
        self._parent: ParentJob | None | _Missing = _MISSING
        self._environment_snapshot: dict[str, object] | None = None
        self._own: Installed | None = None
        self._fits: dict[tuple[str, object], SlotMapping] = {}
        self._recorded: dict[str, tuple[dict[str, object], _kernel.JobRef | None]] | None = None

    def __repr__(self) -> str:
        return f"Attempt(job_id={self.context.job_id!r}, step={self.step!r}, attempt_id={self.context.attempt_id!r})"

    @classmethod
    def initialize(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        runner: "Runner | None" = None,
    ) -> Self:
        """Bind this process to its attempt and recover an interrupted one.

        Recovery replays every workdir batch an earlier attempt sealed but did not
        get to apply, so a handler always starts from a workdir whose sealed
        changes are complete. This is the only constructor a runner needs.

        :param environment: The process environment, or the current environment when omitted.
        :param runner: The runner dispatching this attempt, when called by :meth:`Runner.main`.
        :return: The initialized attempt.
        :raises ValueError: If the manager-written attempt environment or context is invalid.
        """

        bound = _read_environment(environment)
        ReplayableWorkdirBatch.recover(bound.workdir, durable=bound.context.durable)
        return cls(
            bound.context,
            control=bound.control,
            payload=bound.payload,
            workdir=bound.workdir,
            workspace=bound.workspace,
            data=bound.data,
            step=bound.step,
            runner=runner,
        )

    @property
    def job(self) -> JobDefinition:
        """The immutable definition of the job this attempt belongs to."""

        if self._job is None:
            self._job = JobDefinition.from_path(self.payload / "job.json")
        return self._job

    @property
    def deadline(self) -> int | None:
        """Return the epoch second at which the manager stops this attempt, or None when it has no time limit."""

        return self.context.deadline

    @property
    def binding(self) -> Mapping[str, Any] | None:
        """Return the nodes, slots, and launch prefix this attempt was given, or None without a node inventory."""

        return self.context.binding

    @property
    def parameters(self) -> Mapping[str, object]:
        """The application-defined ``parameters`` object of this job."""

        if self._parameters is None:
            self._parameters = cast(dict[str, object], _thaw(self.job.parameters))
        return self._parameters

    @property
    def children(self) -> ChildrenView:
        """The children observed by the join that started this activation.

        Read from this job's ``state.json`` observations on first access, and
        located then; empty when this activation follows no join.

        :raises httk.workflow.errors.FormatError: If this job's ``state.json`` is absent, damaged or
            names another job, or does not record the join that started this activation.
        """

        if self._children is None:
            doc = self._own_state()
            activation = doc.activation
            if (
                activation is None
                or activation.get("id") != self.context.activation_id
                or activation.get("reason") != "join"
            ):
                if self.context.activation_reason == "join":
                    # An aggregate step must never be handed zero children by a state that is not its join's.
                    raise FormatError("this job's state.json does not record the join that started this activation")
                self._children = ChildrenView()
            else:
                observations = [cast(dict[str, object], _thaw(item)) for item in doc.observations]
                refs = self._locate_children(observations)
                self._children = ChildrenView(
                    tuple(
                        _child_result(item, str(item.get("state")), item.get("failure"), refs.get(str(item["job_id"])))
                        for item in observations
                    )
                )
        return self._children

    @property
    def spawned(self) -> ChildrenView:
        """Every child this job published, across all its activations, in publication order.

        A live snapshot, re-read from this job's ``state.json`` and re-located
        on every access: ``kind`` is the child's current state (``owned`` while a
        manager holds it), or ``missing`` when it is not found at its placement.
        ``failure`` is read from a failed or cancelled child's own
        ``state.json``. ``raw`` is the published entry (with ``activation_id``
        and ``attempt_id`` when recorded), plus ``from_state`` for an owned child.
        A reused child appears once, as published; a label appears twice only
        when its child no longer existed and was created anew.

        :raises httk.workflow.errors.FormatError: If this job's ``state.json`` is absent, damaged or
            names another job.
        """

        entries = [cast(dict[str, object], _thaw(item)) for item in self._own_state().children]
        refs = self._locate_children(entries)
        results: list[ChildResult] = []
        for entry in entries:
            ref = refs.get(str(entry["job_id"]))
            failure: object = None
            if ref is not None and ref.state in {"failed", "cancelled"}:
                child_state, _damaged = read_state_unowned(ref.path / "state.json")
                failure = None if child_state is None else _thaw(child_state.failure)
            raw = entry if ref is None or ref.from_state is None else {**entry, "from_state": ref.from_state}
            results.append(_child_result(raw, "missing" if ref is None else ref.state, failure, ref))
        return ChildrenView(tuple(results))

    def _own_state(self) -> StateDoc:
        """Read this job's own ``state.json`` without owning it, refusing an absent, damaged or foreign one."""

        path = self.payload / "state.json"
        doc, damaged = read_state_unowned(path)
        if doc is None:
            if damaged:
                raise FormatError(f"this job's state.json is damaged: {path}")
            raise FormatError("this job's state.json is gone: the job was taken away from this attempt")
        if doc.job_id != self.context.job_id:
            raise FormatError(f"this job's state.json names job {doc.job_id}, not {self.context.job_id}")
        return doc

    def _locate_children(
        self, records: Sequence[Mapping[str, object]], *, settle: bool = False
    ) -> dict[str, _kernel.JobRef]:
        """Locate the children *records* name with one read-only listing of their placements (owned included)."""

        if not records:
            return {}
        return _kernel.locate_many(
            self._workspace(),
            [require_string(item.get("job_id"), "child job_id") for item in records],
            placements=[parse_placement_text(item.get("placement"), "child placement") for item in records],
            settle=settle,
        )

    def _reuse_child(self, label: str) -> ChildReference | None:
        """Register the existing child this job published under *label* in the draft, or return ``None``.

        :param label: The validated child label.
        :return: The existing child, or ``None`` when there is none.
        :raises ValueError: If the label is already used in this attempt.
        :raises httk.workflow.errors.FormatError: If this job's ``state.json`` is damaged or names another job.
        """

        found = self._existing_child(label)
        if found is None:
            return None
        reference, state = found
        self._require_draft().add_existing_child(reference, label=label)
        if state in {"failed", "cancelled"}:
            _LOGGER.warning(
                "reusing existing child %s: %s, which is %s; revive it, or delete it with `httk job delete` "
                "to have the step create a fresh child",
                label,
                reference.job_key,
                state,
            )
        else:
            _LOGGER.info("reusing existing child %s: %s", label, reference.job_key)
        return reference

    def _existing_child(self, label: str) -> tuple[ChildReference, str] | None:
        """Return the existing child this job published under *label* and its state, or ``None`` when there is none.

        Only the newest child recorded under a label counts, and it exists when it is found in the
        workspace (owned included). This job's ``state.json`` is read and its children located once per
        attempt: its ``children`` change only when this attempt's outcome is committed. An absent
        ``state.json`` (a payload no manager has claimed) records no children.

        :param label: The validated child label.
        :return: The child with its recorded key and placement, and its state (the state an owned child was
            claimed from), or ``None``.
        :raises httk.workflow.errors.FormatError: If this job's ``state.json`` is damaged or names another job.
        """

        if self._recorded is None:
            doc = self._own_state() if (self.payload / "state.json").exists() else None
            newest = newest_labeled(() if doc is None else doc.children)
            refs = self._locate_children(list(newest.values()))
            # A child caught mid-rename (a claim or release) is not gone: settle only the misses.
            missing = [entry for entry in newest.values() if str(entry["job_id"]) not in refs]
            refs.update(self._locate_children(missing, settle=True))
            self._recorded = {name: (entry, refs.get(str(entry["job_id"]))) for name, entry in newest.items()}
        entry, ref = self._recorded.get(label, ({}, None))
        if ref is None:
            return None
        # The recorded placement, not the located one: an owned child has none, and a join looks for the
        # child where its job.json places it.
        reference = ChildReference(
            self.context.workspace_id, str(entry["job_id"]), str(entry["job_key"]), str(entry["placement"])
        )
        return reference, ref.from_state or ref.state

    @property
    def parent(self) -> ParentJob | None:
        """The job that spawned this one, when it is reachable in this workspace.

        ``None`` when this job has no parent or was detached from it with
        ``httk job detach``, or when the parent is not found at its recorded
        placement in this workspace: a child transferred away from its parent, or
        a parent transferred or removed. A parent and child transferred together
        still find each other when both keep their placements. Reading it lists
        the parent's placement once (read-only) and reads its ``job.json``.

        :raises httk.workflow.errors.FormatError: If this job's ``parent`` member or
            the parent's ``job.json`` is malformed.
        """

        if isinstance(self._parent, _Missing):
            self._parent = self._locate_parent()
        return self._parent

    def _locate_parent(self, attempts: int = 1) -> ParentJob | None:
        """Look up this job's ``parent`` member in the workspace, without changing anything.

        :param attempts: How many listings to make while the parent is not found or moves before its
            ``job.json`` is read; it is renamed whenever it is re-claimed. Each is one unsettled listing.
        :return: The parent, or ``None``.
        :raises httk.workflow.errors.FormatError: If a ``parent`` member or the parent's ``job.json`` is malformed.
        """

        raw = self.job.parent
        if raw is None:
            return None
        # An operator-detached child is independent: it no longer has a parent to read.
        state, _damaged = read_state_unowned(self.payload / "state.json")
        if state is not None and state.detached is not None:
            return None
        placement = parse_placement_text(raw["placement"], "parent placement")
        for _ in range(attempts):
            ref = _kernel.locate(self._workspace(), str(raw["job_id"]), placement_hint=placement)
            if ref is None:
                continue
            try:
                definition = JobDefinition.from_path(ref.path / "job.json")
            except FormatError:
                # A parent moved between the listing and the read is absent, not corrupt.
                if (ref.path / "job.json").exists():
                    raise
                continue
            if definition.id != raw["job_id"]:
                return None
            break
        else:
            return None
        return ParentJob(
            job_id=definition.id,
            job_key=definition.job_key,
            placement=placement,
            payload=ref.path,
            workdir=ref.path / "run",
            raw=dict(raw),
        )

    @property
    def published(self) -> bool:
        """Report whether this attempt already published its outcome."""

        return self._published is not None

    def parameter(self, name: str, default: object = _MISSING) -> object:
        """Return one member of the job's ``parameters`` object.

        Without a *default*, a missing parameter is a :exc:`KeyError`: a step that
        needs a parameter cannot run without it, and saying so immediately is better
        than failing later on a value that was never there.

        :param name: The parameter name to look up.
        :param default: The value to return when the parameter is absent.
        :return: The parameter value or the supplied default.
        :raises KeyError: If the parameter is absent and no default was supplied.
        """

        parameters = self.parameters
        if name in parameters:
            return parameters[name]
        if isinstance(default, _Missing):
            available = ", ".join(sorted(parameters)) or "none"
            raise KeyError(f"job parameter {name!r} is not defined; defined parameters: {available}")
        return default

    def stage_input(self, name: str, destination: str | os.PathLike[str], default: object = _MISSING) -> Path | None:
        """Copy one payload input file, named by a job parameter, into the workdir.

        The parameter holds a payload-relative path, such as ``files/POSCAR``. Only
        the bytes are copied; an existing destination is overwritten and mode bits
        are not preserved. The value is joined to the payload as given, without
        validation, so an absolute path is used as is.

        :param name: The parameter that names the payload-relative source path.
        :param destination: The target path, relative to the workdir unless absolute.
        :param default: The payload-relative path to use when the parameter is absent.
        :return: The copied file's path, or ``None`` when the payload has no such file.
        :raises KeyError: If the parameter is absent and no default was supplied.
        :raises ValueError: If the parameter value is not a string.
        :raises OSError: If the file cannot be copied.
        """

        value = self.parameter(name, default)
        if not isinstance(value, str):
            raise ValueError(
                f"parameter {name!r} (or its default) must name a payload-relative path, not {type(value).__name__}"
            )
        source = self.payload / value
        if not source.is_file():
            return None
        target = self.workdir / destination
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return target

    def setting(self, name: str, default: object = None) -> object:
        """Resolve one application setting through its layers.

        The layers are consulted most-specific first, and the first that has the
        name wins: this job's ``parameters`` object, then the environment variable
        ``HTTK_`` + the name upper-cased with dots as underscores (so
        ``vasp.command`` reads ``HTTK_VASP_COMMAND``), then the workspace's
        application settings, then *default*. This is how a step reads the VASP
        command a workspace was configured with without the operator exporting it
        for every job, while still letting one job or one shell override it.

        :param name: The dotted application setting name to resolve.
        :param default: The value to return when no layer defines the setting.
        :return: The first value found in the resolution layers, or the default.
        """

        parameters = self.parameters
        if name in parameters:
            return parameters[name]
        variable = "HTTK_" + name.upper().replace(".", "_")
        if variable in os.environ:
            return os.environ[variable]
        settings = self.context.settings
        if name in settings:
            return settings[name]
        return default

    def environment(self, name: str, default: object = _MISSING) -> object:
        """Resolve one declared workflow environment value through its layers.

        Once the runner's start gate has run, this attempt reads the immutable
        snapshot resolved there. Before that gate, overrides, the declared setting's
        environment variable, workspace settings, the declaration default, and
        *default* are consulted in that order.

        :param name: The declared environment name to look up.
        :param default: The value to return when no declared value exists.
        :return: The resolved environment value.
        :raises KeyError: If the name is undeclared or unresolved without a default.
        """

        if self._environment_snapshot is not None and name in self._environment_snapshot:
            return self._environment_snapshot[name]
        value, source = _resolve_environment_value(self.job, self.context.settings, name, default)
        if source is None:
            if isinstance(default, _Missing):
                raise KeyError(f"workflow environment {name!r} is unresolved")
            return value
        return value

    def _prepare_environment(self) -> bool:
        """Resolve and record the job environment once before dispatch.

        :return: ``True`` when the step may run, otherwise ``False`` after a
            structured terminal failure was published.
        """

        declared = self.job.environment.get("declared", {})
        if not isinstance(declared, Mapping) or not declared:
            return True
        marker = self.control / _ENVIRONMENT_MARKER
        if marker.is_file():
            recorded = read_json(marker)
            values = recorded.get("values")
            if recorded.get("status") == "resolved" and isinstance(values, Mapping):
                self._environment_snapshot = {
                    name: item["value"]
                    for name, item in values.items()
                    if isinstance(name, str) and isinstance(item, Mapping) and "value" in item
                }
                return True
            self._environment_snapshot = {}
            return False
        if self.published:
            return False
        try:
            values, unresolved = resolve_declared_environment(self.job, self.context.settings)
        except _EnvironmentResolutionError as exc:
            message = (
                f"workflow environment {exc.name!r} could not be resolved: {exc}; "
                "remedies: set the workspace setting, pass --environment NAME=VALUE, or add a manifest default"
            )
            write_json_atomic(
                marker,
                {"format": "httk-workflow-environment-resolution-marker", "format_version": 2, "status": "failed"},
                durable=self.context.durable,
            )
            self.fail("environment_unresolved", message, details={"unresolved": [exc.name]}, retryable=False)
            return False
        if unresolved:
            entries = ", ".join(repr(name) for name in unresolved)
            layer_details: list[str] = []
            for name in unresolved:
                metadata = declared[name]
                setting = metadata.get("setting", name) if isinstance(metadata, Mapping) else name
                setting = setting if isinstance(setting, str) else name
                layer_details.append(
                    f"{name}: job override, environment variable {environment_variable_name(setting)}, "
                    f"workspace setting {setting!r}, manifest default"
                )
            layers = "; ".join(layer_details)
            message = (
                f"workflow environment entries {entries} are unresolved; consulted layers: {layers}; "
                "remedies: set the workspace setting, pass --environment NAME=VALUE, or add a manifest default"
            )
            write_json_atomic(
                marker,
                {"format": "httk-workflow-environment-resolution-marker", "format_version": 2, "status": "failed"},
                durable=self.context.durable,
            )
            self.fail("environment_unresolved", message, details={"unresolved": unresolved}, retryable=False)
            return False

        self._environment_snapshot = {name: item["value"] for name, item in values.items()}
        document = {
            "format": "httk-workflow-environment-resolution",
            "format_version": 2,
            "values": values,
        }
        observed_path = self._declaration_path("environment")
        observed = read_json(observed_path) if observed_path.is_file() else None
        changed = observed is None or json_bytes(observed) != json_bytes(document)
        if changed:
            self.declare("environment", document)
        marker_document: dict[str, object] = {
            "format": "httk-workflow-environment-resolution-marker",
            "format_version": 2,
            "status": "resolved",
            "values": values,
            "log_pending": changed,
        }
        write_json_atomic(marker, marker_document, durable=self.context.durable)
        return True

    def _finish_environment_log(self) -> None:
        """Emit the deferred environment note after the handler owns the workdir."""

        marker = self.control / _ENVIRONMENT_MARKER
        if not marker.is_file() or not self.workdir.is_dir():
            return
        recorded = read_json(marker)
        if recorded.get("status") != "resolved" or not recorded.get("log_pending"):
            return
        values = recorded.get("values")
        if not isinstance(values, Mapping):
            return
        self.log.append("note", _environment_log_message(values))
        recorded = dict(recorded)
        recorded["log_pending"] = False
        write_json_atomic(marker, recorded, durable=self.context.durable)

    def declare(self, name: str, document: Mapping[str, object]) -> Path:
        """Record the observed workflow declaration *name* of this job.

        The static declarations of a job are the ones ``job.json`` carried at
        submission, and they cannot change. A dynamic campaign nevertheless only
        learns at run time what it actually consumed and produced, so a step
        writes the refined document here and it is stored beside the job state as
        ``.httk-job/declarations/<name>.json``, atomically. The bytes are carried
        verbatim: nothing here interprets the document, whose own members say
        which vocabulary and version it follows.

        The write is runner-private, so it never disturbs the payload digest, and
        repeating it overwrites: what a job observed is whatever its last word on
        the subject was. A collect reports the observed document beside the
        declared one and never merges the two.

        :param name: The declaration name to record.
        :param document: The declaration document to store verbatim.
        :return: The path of the stored observed declaration.
        :raises httk.workflow.errors.FormatError: If the declaration name or document is invalid.
        """

        declaration = validate_declaration_name(name, "declaration name")
        validated = validate_declarations({declaration: document}, "declaration")[declaration]
        path = self._declaration_path(declaration)
        write_json_atomic(path, validated, durable=self.context.durable)
        _LOGGER.debug("declared %s for job %s", declaration, self.context.job_key)
        return path

    def declaration(self, name: str) -> Mapping[str, object] | None:
        """Return the workflow declaration *name*, observed first.

        The document this job observed is returned when one was written,
        otherwise the one ``job.json`` declared, otherwise ``None``.

        :param name: The declaration name to read.
        :return: The observed or declared document, or ``None`` when absent.
        :raises httk.workflow.errors.FormatError: If the declaration name is invalid or its document is malformed.
        """

        declaration = validate_declaration_name(name, "declaration name")
        path = self._declaration_path(declaration)
        if path.is_file():
            return read_json(path)
        declared = _thaw(self.job.declarations.get(declaration))
        return declared if isinstance(declared, dict) else None

    def _declaration_path(self, declaration: str) -> Path:
        """Return where the observed document of one declaration is stored."""

        return self.payload / JOB_STATE_DIRECTORY / "declarations" / f"{declaration}.json"

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: float | None = None,
        cwd: str | os.PathLike[str] | None = None,
        environment: Mapping[str, str] | None = None,
        termination_grace: float = 10.0,
    ) -> CommandResult:
        """Run an argv array in the workdir and reap its process group.

        :param argv: The command and its arguments.
        :param timeout: The maximum runtime before terminating the process group.
        :param cwd: The working directory, or this attempt's workdir when omitted.
        :param environment: The complete child environment, or the process environment when omitted.
        :param termination_grace: The grace period after a timeout before forceful termination.
        :return: The completed command result.
        """

        return run_command(
            argv,
            timeout=timeout,
            cwd=self.workdir if cwd is None else cwd,
            environment=environment,
            termination_grace=termination_grace,
        )

    def workdir_batch(self) -> ReplayableWorkdirBatch:
        """Start a replayable group of workdir changes.

        :return: A batch that seals changes for replay after interruption.
        """

        return ReplayableWorkdirBatch.initialize(self.workdir, durable=self.context.durable)

    def transaction(self) -> Transaction:
        """Begin a transaction of this attempt, for a commit point in the middle of a step.

        Stage files with :meth:`~httk.workflow._data.Transaction.put` (destinations
        are relative to the job directory, so ``data/result`` lands in the data
        directory), then :meth:`~httk.workflow._data.Transaction.commit`. The
        manager applies every committed transaction at the next attempt boundary,
        whatever the outcome, and before any later attempt starts; an
        uncommitted one is discarded.

        :return: The new transaction.
        """

        return Transaction(self.control, durable=self.context.durable)

    def put(self, source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> PurePosixPath:
        """Stage a copy of one file or directory for ``data/<destination>`` of this job.

        The copy is staged now into the attempt's implicit transaction, which
        commits together with the outcome; the manager applies it when it
        commits the outcome, exactly once. A file replaces what is there, and a
        directory is merged into what is there.

        :param source: The file or directory to stage.
        :param destination: The destination path below the job's data directory.
        :return: The payload-relative destination, ``data/<destination>``.
        :raises ValueError: If the destination is absolute, empty or escapes the data directory.
        """

        self._reject_published()
        target = PurePosixPath("data") / PurePosixPath(os.fspath(destination))
        if PurePosixPath(os.fspath(destination)).is_absolute():
            raise ValueError(f"put destination must be relative to the data directory: {os.fspath(destination)!r}")
        if self._implicit is None:
            draft = self._require_draft()
            self._implicit = self.transaction()
            (draft.root / _IMPLICIT_TRANSACTION).write_text(self._implicit.seq, encoding="utf-8")
        self._implicit.put(source, target)
        return target

    def _default_child_placement(self) -> str:
        """Return the placement of a child spawned without one, per the ``children.placement`` setting."""

        # The workspace setting alone decides: never through setting(), which parameters and env override.
        mode = self.context.settings.get("children.placement", "nested")
        here = self.context.placement
        if mode == "flat":
            return here
        if mode != "nested":
            raise ValueError(f"workspace setting children.placement must be 'nested' or 'flat', not {mode!r}")
        nested = f"{self.context.job_key}.children" if not here else f"{here}/{self.context.job_key}.children"
        # ponytail: past the byte budget the tree continues flat from this level; no shortening of components.
        return here if len(nested.encode()) > _NESTED_PLACEMENT_BUDGET else nested

    def spawn(
        self,
        child: "ChildSpec | str | os.PathLike[str]",
        *,
        label: str,
        placement: str | PurePosixPath | None = None,
    ) -> ChildReference:
        """Register one child job under *label*, to be created on publication.

        *child* is either a :class:`ChildSpec`, which needs no payload at all, or
        the path of a prepared payload directory. The label is mandatory and names
        one child for the life of this job: it is how :meth:`gather` and
        :attr:`children` name this child later.

        A label that names an existing child of this job (published by an
        earlier activation or attempt, as a rerun spawning step does) returns
        that child and registers nothing: *child* and *placement* are ignored and
        the child is kept exactly as it is, also when it failed or was cancelled.
        A child that no longer exists in the workspace is created anew. A step
        wanting several children under one base name numbers the labels
        (``scf-1``, ``scf-2``).

        A :class:`ChildSpec` child never inherits ``mintime``, and its ``maxtime``
        requirements are capped at this attempt's effective ``maxtime``. A
        prepared payload directory is registered exactly as written, uncapped.

        :param child: The child specification or prepared payload directory.
        :param label: The label used to observe the child later, unique within this job.
        :param placement: The workspace placement for the child; when omitted, ``<this placement>/<this job key>.children``,
            or this attempt's placement under the workspace setting ``children.placement = flat`` or when
            the nested placement would exceed 1024 bytes.
        :return: The reference to the registered or existing child.
        :raises ValueError: If the child or label is invalid, or the label is already used in this attempt.
        :raises httk.workflow.errors.FormatError: If this job's ``state.json`` is damaged or names another job.
        """

        self._reject_published()
        entry_label = validate_label(label, "child label")
        if isinstance(child, ChildSpec):
            # The child runs this very workflow, so its initial step is a step of
            # this runner and a typo in it is catchable here, also when the child exists.
            self._check_step(child.step, "spawned child step")
        if (existing := self._reuse_child(entry_label)) is not None:
            return existing
        target = self._default_child_placement() if placement is None else placement
        if not isinstance(child, ChildSpec):
            return self._require_draft().add_child(child, target, label=entry_label)
        # Everything this child needs is validated before the draft exists, so a
        # refused spawn leaves the attempt exactly as it found it.
        spec = child._job_spec(
            self.job,
            (self.context.workflow_id, self.context.workflow_name),
            entry_label,
            self.context.resources.get("maxtime"),
        )
        reference = self._require_draft().add_child_job(spec.as_mapping(), target, label=entry_label)
        _LOGGER.debug("spawned %s at step %s as %s", reference.job_key, child.step, entry_label)
        return reference

    def _workspace(self) -> "Workspace":
        """Return this attempt's workspace, for read-only lookups (jobs, installed workflows)."""

        from .workspace import Workspace

        return Workspace(self.workspace, durable=self.context.durable)

    def _callee(self, workflow: str) -> "Installed":
        """Return the installed workflow *workflow* names among this job's declared calls.

        The calls are the ``[workflow.calls]`` of the installation this attempt was launched from (the job's
        effective workflow, or the ``calls`` an embedded workflow's ``job.json`` froze), alias to installed id;
        *workflow* is one of the aliases or one of the ids.
        """

        from . import _store

        workspace = self._workspace()
        own = self._own_installation()
        recorded = own.record.get("calls")
        calls = {str(alias): str(target) for alias, target in recorded.items()} if isinstance(recorded, dict) else {}
        target = calls.get(workflow, workflow if workflow in calls.values() else None)
        if target is None:
            declared = ", ".join(f"{alias} = {callee}" for alias, callee in sorted(calls.items())) or "none"
            raise ValueError(
                f"workflow {workflow!r} is not declared in [workflow.calls] of {own.name} (declared: {declared}); "
                "declare it there and reinstall the workflow"
            )
        installed = _store.lookup(workspace, target)
        if installed is None or installed.id != target:
            raise ValueError(
                f"the called workflow {target} is not installed in the workspace; "
                f"install it with `httk workflow install {target}`"
            )
        return installed

    def _own_installation(self) -> "Installed":
        """Return the installation this attempt was launched from (the job's effective workflow), viewed at the
        job's own runner copy when the job is unlocked and has one; an embedded workflow's view of ``workflow/``."""

        from . import _store

        if self._own is None and self.job.embedded is not None:
            embedded = _store.embedded(self.job, self.payload)
            if embedded is None:
                raise ValueError(f"this job's embedded workflow {self.job.workflow_id} has no workflow/ directory")
            self._own = embedded
        if self._own is None:
            own = _store.lookup(self._workspace(), self.context.workflow_id)
            if own is None:
                raise ValueError(f"this job's workflow {self.context.workflow_id} is not installed in the workspace")
            doc, _damaged = read_state_unowned(self.payload / "state.json")
            if doc is not None and doc.unlocked is not None:
                own = _store.runner_copy(own, self.payload) or own
            self._own = own
        return self._own

    def _slot_callee(self, alias: str) -> "tuple[Installed, SlotMapping, Mapping[str, object]] | None":
        """Return the workflow call slot *alias* is bound to, how the slot maps onto it, and its bound parameters.

        ``None`` when *alias* is no call slot of this job's workflow. The binding comes from ``job.json``, which may
        have arrived through the exchange, so the installed target is fitted to the slot again (once per tree).
        """

        from . import _store
        from ._slots import SlotMismatch, fit

        own = self._own_installation()
        slot = (own.provider().slots or {}).get(alias)
        if slot is None:
            return None
        binding = cast(Mapping[str, object] | None, (self.job.calls or {}).get(alias))
        if binding is None:
            raise ValueError(
                f"job.json binds no workflow to call slot {alias!r} of {own.id}; the job does not match its workflow"
            )
        target = str(binding["workflow"])
        installed = _store.lookup(self._workspace(), target)
        if installed is None or installed.id != target:
            raise ValueError(
                f"the workflow {target} bound to call slot {alias!r} is not installed in the workspace; "
                f"install it with `httk workflow install {target}`"
            )
        key = (alias, installed.record.get("tree_sha256"))
        if key not in self._fits:
            try:
                self._fits[key] = fit(slot.interface, installed.provider())
            except SlotMismatch as exc:
                raise ValueError(f"call slot {alias!r} bound to {target}: {exc}") from exc
        return installed, self._fits[key], cast(Mapping[str, object], _thaw(binding["parameters"]))

    def call(
        self,
        workflow: str,
        *,
        label: str,
        inputs: Mapping[str, object] | None = None,
        files: Mapping[str, str | os.PathLike[str]] | None = None,
        parameters: Mapping[str, object] | None = None,
        environment: Mapping[str, object] | None = None,
        tag: str | None = None,
        placement: str | PurePosixPath | None = None,
        priority: int | None = None,
        step: str | None = None,
        name: str | None = None,
    ) -> ChildReference:
        """Spawn a job of another installed workflow as a child under *label*.

        This is :func:`~httk.workflow.scaffold.new_job` from inside a running
        step. *workflow* is an alias of the ``[workflow.calls]`` this job's
        installed workflow declares, or the installed id an alias names, and the
        called workflow must be installed in the workspace too. A complete child
        payload is built from the installed package (its *files* and *inputs*
        staged, its instantiate hook run, its ``job.json`` written) and
        registered as a child of this attempt's outcome. Wait for it with
        :meth:`gather`, which resumes this job when the child is terminal, and
        read it back through :attr:`children`.

        The child takes the called workflow's resources, with every ``maxtime``
        capped at this attempt's effective ``maxtime``.

        As for :meth:`spawn`, a label that names an existing child of this job
        returns that child and registers nothing: no payload is built, the other
        arguments are ignored, and the called workflow need not be installed. A
        child that no longer exists in the workspace is created anew.

        When *workflow* is a call slot alias, the child runs the workflow this
        job's ``calls`` binding names, fitted to the slot's interface again:

        - *inputs* are named by the interface, and every interface input must be
          given; they reach the bound workflow's inputs through the fit;
        - a path whose destination has a registered writer is loaded and staged
          in the bound workflow's format;
        - a staged structure must hold the supplied structure's sites in the
          same order and cell (order sites species-contiguously for POSCAR);
        - the binding's parameters are merged with *parameters*, and a name in
          both is refused;
        - the child records the interface's outputs when it succeeds
          (``persist_outputs``), read back with :meth:`child_outputs`.

        :param workflow: The declared alias, call slot alias, or installed id of the workflow to call.
        :param label: The label used to gather and observe the child later, unique within this job.
        :param inputs: Supply the called workflow's declared inputs.
        :param files: Map payload names to files to stage for the child.
        :param parameters: Supply opaque parameters for the child job.
        :param environment: Supply overrides for the child's declared environment values.
        :param tag: Set the child job tag, defaulting to *label* when omitted.
        :param placement: The workspace placement for the child; when omitted, ``<this placement>/<this job key>.children``,
            or this attempt's placement under the workspace setting ``children.placement = flat`` or when
            the nested placement would exceed 1024 bytes.
        :param priority: Set the child scheduling priority.
        :param step: Override the called workflow's initial step.
        :param name: Set the child job's display name.
        :return: The reference to the registered or existing child.
        :raises ValueError: If the workflow is undeclared or not installed, the label is invalid or already
            used in this attempt, or the inputs or job settings are invalid; for a call slot, also if the job
            binds no workflow to it, the bound workflow no longer fits, an interface input is unknown or
            missing, a path cannot be loaded, a parameter is given twice, or a staged structure does not keep
            the supplied sites.
        :raises httk.workflow.errors.FormatError: If this job's ``state.json`` is damaged or names another job.
        """

        self._reject_published()
        validate_label(label, "child label")
        if (existing := self._reuse_child(label)) is not None:
            return existing
        target = self._default_child_placement() if placement is None else placement
        slot = self._slot_callee(workflow)
        persist_outputs: dict[str, str] | None = None
        if slot is None:
            installed = self._callee(workflow)
        else:
            installed, mapping, bound_parameters = slot
            inputs = self._slot_inputs(workflow, mapping, installed, inputs)
            if collisions := sorted(set(bound_parameters) & set(parameters or {})):
                raise ValueError(
                    f"call slot {workflow!r} parameter {collisions[0]!r} is already given by the job's binding"
                )
            parameters = {**bound_parameters, **(parameters or {})}
            persist_outputs = dict(mapping.outputs)
        from .scaffold import _build_payload, _prepare, payload_relative

        staging = self.control / f"call.{uuid.uuid4()}"
        staging.mkdir(parents=True, exist_ok=False)
        try:
            _build_payload(
                self._workspace(),
                _prepare(installed, step=step),
                staging,
                inputs=inputs,
                files=files,
                parameters=parameters,
                environment=environment,
                tag=tag if tag is not None else label,
                priority=priority,
                name=name,
                maxtime_cap=self.context.resources.get("maxtime"),
                persist_outputs=persist_outputs,
            )
            if slot is not None:
                from ._slots import check_staged_sites

                destinations = installed.provider().inputs
                for key, value in (inputs or {}).items():
                    destination = destinations.get(key)
                    if destination is not None and not isinstance(value, (str, os.PathLike)):
                        staged = staging.joinpath(*payload_relative(destination).parts)
                        check_staged_sites(value, staged, f"call slot {workflow!r} input for {key!r}")
            reference = self._require_draft().add_child(staging, target, label=label, move=True)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        _LOGGER.debug("called workflow %s as %s (%s)", installed.id, label, reference.job_key)
        return reference

    def child_outputs(self, child: ChildResult, *, call: str) -> Mapping[str, object]:
        """Return the outputs a child called through call slot *call* recorded for this job.

        A job called through a slot records, when it succeeds, the property value of each output of the slot's
        interface in ``.httk-job/outputs.json``; this reads them back, with no installation of the called
        workflow needed. The values are plain JSON (a list of lists of floats for per-site vectors), and a
        per-site value is in the site order of the structure the call staged, which is the order of the
        structure passed to :meth:`call`.

        :param child: A succeeded child, from :attr:`children` or :attr:`spawned`.
        :param call: The call slot the child was called through, an alias of this job's workflow.
        :return: A read-only mapping from each output name of the slot's interface to its value.
        :raises ValueError: If *call* is no call slot, the child did not succeed or is not located, or its
            recorded outputs are missing, malformed, or not the slot interface's outputs and definitions.
        """

        from .introspection._reading import read_job_file

        slot = (self._own_installation().provider().slots or {}).get(call)
        if slot is None:
            raise ValueError(f"{call!r} is no call slot of {self.context.workflow_id}")
        if not child.succeeded or child.payload is None:
            raise ValueError(f"child {child.job_key} has not succeeded or is not located; it has no outputs to read")
        relative = f"{JOB_STATE_DIRECTORY}/{_OUTPUTS_FILE}"
        try:
            data = read_job_file(child.payload, relative, _OUTPUTS_LIMIT)
            document = None if data is None else json.loads(data)
        except (OSError, ValueError, WorkflowError) as exc:
            raise ValueError(f"child {child.job_key}: cannot read {relative}: {exc}") from exc
        if document is None:
            raise ValueError(f"child {child.job_key} recorded no outputs; was it called through call slot {call!r}?")
        if (
            not isinstance(document, dict)
            or document.get("format") != _OUTPUTS_FORMAT
            or document.get("format_version") != 1
            or not isinstance(document.get("outputs"), dict)
        ):
            raise ValueError(f"child {child.job_key}: {relative} is not an {_OUTPUTS_FORMAT} version 1 document")
        recorded = cast(dict[str, object], document["outputs"])
        interface = {
            str(entry["name"]): str(entry["ref"])
            for entry in cast(list[Mapping[str, object]], slot.interface["outputs"])
        }
        if set(recorded) != set(interface):
            raise ValueError(
                f"child {child.job_key} recorded outputs {sorted(recorded)}, call slot {call!r} has {sorted(interface)}"
            )
        values: dict[str, object] = {}
        for name, ref in interface.items():
            entry = recorded[name]
            if not isinstance(entry, dict) or entry.get("definition") != ref or "value" not in entry:
                raise ValueError(f"child {child.job_key}: output {name!r} is not a recorded {ref} value")
            values[name] = entry["value"]
        return MappingProxyType(values)

    def _slot_inputs(
        self,
        alias: str,
        mapping: "SlotMapping",
        callee: "Installed",
        inputs: Mapping[str, object] | None,
    ) -> dict[str, object]:
        """Translate a slot call's inputs to the callee's input keys, loading a path its writer can restage."""

        import httk.core

        supplied = dict(inputs or {})
        if unknown := sorted(set(supplied) - set(mapping.inputs)):
            raise ValueError(
                f"call slot {alias!r} has no input {unknown[0]!r}; its inputs: {', '.join(mapping.inputs)}"
            )
        if missing := sorted(set(mapping.inputs) - set(supplied)):
            raise ValueError(f"call slot {alias!r} needs every input; missing: {', '.join(missing)}")
        destinations = callee.provider().inputs
        translated: dict[str, object] = {}
        for name, value in supplied.items():
            key = mapping.inputs[name]
            destination = destinations.get(key)
            if (
                isinstance(value, (str, os.PathLike))
                and destination is not None
                and httk.core.has_writer_for(PurePosixPath(destination).name)
            ):
                # Staged in the callee's own format, whatever format the caller had it in.
                try:
                    value = httk.core.load(os.fspath(value))
                except (OSError, ValueError) as exc:
                    raise ValueError(f"call slot {alias!r} input {name!r}: {exc}") from exc
            translated[key] = value
        return translated

    def advance(
        self,
        step: str,
        *,
        state: Mapping[str, object] | None = None,
        priority: int | None = None,
        resources: Mapping[str, int | str] | None = None,
    ) -> Path:
        """Publish a new activation of this job at *step*.

        *state* is written to :attr:`state` before the outcome is published, so
        the step that runs next always finds the state that decided to run it.

        :param step: The next registered step.
        :param state: State members to merge before publication.
        :param priority: The priority of the new activation, when changed.
        :param resources: The requirement of the new activation, when changed; ``maxtime`` and
            ``mintime`` are Slurm duration strings such as ``"1:30:00"``.
        :return: The path of the published outcome.
        :raises RuntimeError: If this attempt already published an outcome.
        """

        self._reject_published()
        self._check_step(step, "advance target")
        if state:
            self.state.merge(state)
        return self._publish("advance", next_step=step, priority=priority, resources=resources)

    def gather(
        self,
        step: str,
        *,
        when: JoinCondition = "all_succeeded",
        count: int | None = None,
        on_impossible: str | None = None,
        rejoin: Iterable[str] = (),
        priority: int | None = None,
        resources: Mapping[str, int | str] | None = None,
    ) -> Path:
        """Wait for this attempt's and optionally earlier children, then run *step*.

        The join names the children :meth:`spawn` and :meth:`call` registered or
        reused on this attempt, in the order of those calls, and labels from
        earlier join activations named by *rejoin*. *when* is one of
        ``all_succeeded``, ``all_terminal``, ``any_succeeded``, ``any_terminal``,
        or ``at_least`` with *count*. When the condition can no longer be met,
        the job advances to *on_impossible* if one is named and fails with
        ``dependency_failure`` otherwise.

        :param step: The step to run when the join condition is met.
        :param when: The child completion condition.
        :param count: The required count when ``when`` is ``at_least``.
        :param on_impossible: The step to run when the condition cannot be met.
        :param rejoin: Labels of children observed by an earlier join activation.
        :param priority: The priority of the join activation, when changed.
        :param resources: The requirement of the join activation, when changed; ``maxtime`` and
            ``mintime`` are Slurm duration strings such as ``"1:30:00"``.
        :return: The path of the published wait outcome.
        :raises ValueError: If no children were spawned, reused or rejoined, a rejoined label is unknown or names
            a child already joined, or a named step is invalid.
        """

        self._reject_published()
        self._check_step(step, "gather target")
        if on_impossible is not None:
            self._check_step(on_impossible, "on_impossible target")
        draft = self._draft
        # Spawned and reused children in the order of their calls: a reused child records how many children
        # were registered before it. A spawned child's label is filled in by the manager from the spawn set.
        ordered: list[tuple[tuple[int, int], str | None, Mapping[str, object]]] = []
        if draft is not None:
            ordered += [
                ((index, 1), None if entry.get("label") is None else str(entry["label"]), child.as_mapping())
                for index, (child, entry) in enumerate(draft._children)
            ]
            ordered += [
                ((position, 0), label, {**child.as_mapping(), "label": label})
                for child, label, position in draft.reused
            ]
        ordered.sort(key=lambda item: item[0])
        joined: list[tuple[str | None, Mapping[str, object]]] = [(label, entry) for _, label, entry in ordered]
        for label in rejoin:
            observed = self.children.get(label)
            if observed is None:
                known_labels = ", ".join(self.children.labels) or "none"
                raise ValueError(f"unknown rejoin child label {label!r}; known labels: {known_labels}")
            entry = {
                "workspace_id": self.context.workspace_id,
                "job_id": observed.job_id,
                "job_key": observed.job_key,
                "label": label,
                "placement_hint": placement_text(observed.placement),
            }
            joined.append((label, entry))
        labels: set[str] = set()
        identities: set[tuple[object, object]] = set()
        for joined_label, joined_entry in joined:
            if joined_label is not None:
                if joined_label in labels:
                    raise ValueError(f"duplicate join child label: {joined_label}")
                labels.add(joined_label)
            identity = (joined_entry["workspace_id"], joined_entry["job_id"])
            if identity in identities:
                raise ValueError(f"duplicate join child identity: {joined_entry['job_id']}")
            identities.add(identity)
        if not joined:
            raise ValueError(
                "gather requires children spawned or reused on this attempt or rejoin entries; none was given"
            )
        join = join_mapping((), when, count, on_impossible, [entry for _, entry in joined])
        return self._publish("wait", next_step=step, priority=priority, join=join, resources=resources)

    def succeed(self) -> Path:
        """Publish the successful completion of this job.

        A job called through a call slot (its ``job.json`` names ``persist_outputs``) first records the outputs
        its caller reads: its own workflow's collect hook runs on this job, and each requested role's property
        value is written to ``.httk-job/outputs.json``. When that fails, the job fails instead, with code
        ``workflow.outputs_unavailable`` naming the role and the cause.

        :return: The path of the published outcome.
        """

        self._reject_published()
        # An attempt bound without a job.json (a fabricated context) has nothing to persist, as for parent.
        defined = self._job is not None or (self.payload / "job.json").exists()
        if defined and self.job.persist_outputs is not None:
            try:
                self._persist_outputs()
            except Exception as exc:  # every cause, from a hook's own exception to a wrong definition, fails the job
                _LOGGER.warning("job %s cannot persist its outputs: %s", self.context.job_key, exc)
                return self.fail(_OUTPUTS_UNAVAILABLE, f"the outputs requested by the caller are unavailable: {exc}")
        return self._publish("succeed")

    def _persist_outputs(self) -> Path:
        """Collect this job's requested outputs and write them to ``.httk-job/outputs.json``."""

        from ._slots import property_value
        from .collecting import JobRecord, _collect_outputs_strict, _job_mapping, children_of, declarations_of

        # Checked before the hook runs: a role that is no records output with a ref cannot be recorded.
        requested = {name: str(role) for name, role in (self.job.persist_outputs or {}).items()}
        declared = {
            str(metadata.get("role", key)): metadata
            for key, metadata in self._own_installation().provider().outputs.items()
        }
        refs: dict[str, str] = {}
        for role in requested.values():
            metadata = declared.get(role)
            ref = None if metadata is None else metadata.get("ref")
            if metadata is None or metadata.get("entry_type") != "records" or not isinstance(ref, str):
                raise ValueError(f"output role {role!r} is no records output with a ref of {self.context.workflow_id}")
            refs[role] = ref
        doc = self._own_state()
        if doc.workflow_pin is None:
            raise ValueError("this job's state.json pins no installed workflow")
        workspace = self._workspace()
        root = workspace.root.resolve()
        payload = PurePosixPath(self.payload.resolve().relative_to(root).as_posix())
        # The manager applies this attempt's transactions only after the outcome, in sequence order: the record
        # overlays their data trees, newest first, on the committed data, as the job's data will be.
        transactions = [path for path in (self.control / "txn").glob("[0-9]*") if path.name.isdigit()]
        if self._implicit is not None:
            transactions.append(self._implicit.path)
        pending = tuple(
            path / "data"
            for path in sorted(transactions, key=lambda path: path.name[:6], reverse=True)
            if (path / "data").is_dir()
        )
        record = JobRecord(
            workspace_root=root,
            workspace_id=self.context.workspace_id,
            job_id=self.context.job_id,
            job_key=self.context.job_key,
            job=_job_mapping(self.job, doc),
            runner_provenance=cast(Mapping[str, object], _thaw(doc.workflow_pin)),
            state="succeeded",
            failure=None,
            placement=self.job.placement,
            payload_path=payload,
            workdir_path=PurePosixPath(self.workdir.resolve().relative_to(root).as_posix()),
            data_path=payload / "data" if (self.payload / "data").is_dir() else None,
            provenance={},
            runner_steps=None,
            children=children_of(doc),
            declarations=declarations_of(self.job, self.payload)[0],
            _pending_data=pending,
        )
        entries = _collect_outputs_strict(workspace, record, sorted(set(requested.values())))
        outputs: dict[str, object] = {}
        for name, role in requested.items():
            ref = refs[role]
            try:
                definition, value = property_value(entries[role])
            except ValueError as exc:
                raise ValueError(f"output role {role!r}: {exc}") from exc
            if definition != ref:
                raise ValueError(f"output role {role!r} holds a {definition} value, not the declared {ref}")
            outputs[name] = {"role": role, "definition": definition, "value": value}
        path = self.payload / JOB_STATE_DIRECTORY / _OUTPUTS_FILE
        document = {"format": _OUTPUTS_FORMAT, "format_version": 1, "outputs": outputs}
        write_json_atomic(path, document, durable=self.context.durable)
        return path

    def retry(self, reason: str) -> Path:
        """Ask for another attempt of this same activation.

        :param reason: The reason recorded with the retry request.
        :return: The path of the published outcome.
        """

        return self._publish("retry", retry={"reason": reason})

    def pause(self, reason: str) -> Path:
        """Pause this job until an operator resumes it.

        :param reason: The reason recorded with the pause request.
        :return: The path of the published outcome.
        """

        return self._publish("pause", pause={"reason": reason})

    def fail(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        retryable: bool = False,
        priority: int | None = None,
    ) -> Path:
        """Publish a structured terminal failure.

        ``code`` is the token a job lists in ``retry_on``. ``retryable`` declares
        that repeating this attempt could help, which the manager honours within
        the attempt budgets of the job.

        :param code: The stable failure code.
        :param message: The human-readable failure message.
        :param details: Optional structured failure details.
        :param retryable: Whether repeating this attempt could help.
        :param priority: The terminal priority, when changed.
        :return: The path of the published outcome.
        """

        failure = Failure(code, message, details=details, retryable=retryable)
        return self._publish("fail", failure=failure.as_mapping(), priority=priority)

    def _reject_published(self) -> None:
        """Refuse a second outcome, before anything of the first is disturbed."""

        if self._published is not None:
            raise RuntimeError(f"this attempt already published its {self._action} outcome")

    def _require_draft(self) -> OutcomeDraft:
        """Return this attempt's outcome draft, creating it on first use."""

        self._reject_published()
        if self._draft is None:
            self._draft = OutcomeDraft(self.context, self.control, durable=self.context.durable)
        return self._draft

    def _check_step(self, step: str, name: str) -> None:
        """Reject a step this runner does not implement, at the call that names it."""

        validate_step(step, name)
        if self._runner is None or step in self._runner.steps:
            return
        raise ValueError(
            f"{name} {step!r} is not a step of the {self._runner.workflow} runner; "
            f"registered steps: {', '.join(sorted(self._runner.steps)) or 'none'}"
        )

    def _publish(
        self,
        action: OutcomeAction,
        *,
        next_step: str | None = None,
        priority: int | None = None,
        failure: Mapping[str, object] | None = None,
        retry: Mapping[str, object] | None = None,
        join: Mapping[str, object] | None = None,
        pause: Mapping[str, object] | None = None,
        resources: Mapping[str, int | str] | None = None,
    ) -> Path:
        draft = self._require_draft()
        declared = self._undeclared_steps()
        try:
            if self._implicit is not None:
                # Committed first, so the outcome the manager sees never names data that is not committed.
                self._implicit.commit()
                self._implicit = None
                (draft.root / _IMPLICIT_TRANSACTION).unlink()
            published = draft.publish(
                action,
                next_step=next_step,
                priority=priority,
                failure=failure,
                retry=retry,
                join=join,
                pause=pause,
                resources=resources,
                runner_steps=declared,
            )
        except Exception:
            self._discard_draft()
            raise
        self._published = published
        self._action = action
        return published

    def _undeclared_steps(self) -> list[str]:
        """Return the complete step set to carry in every published outcome."""

        return [] if self._runner is None else sorted(self._runner.steps)

    def _discard_draft(self) -> None:
        """Remove an unpublished draft so no half-outcome survives this attempt."""

        draft = self._draft
        self._draft = None
        self._implicit = None
        if draft is None or self._published is not None:
            return
        shutil.rmtree(draft.root, ignore_errors=True)

    def _abort(self, exception: BaseException) -> None:
        """Discard the draft and record why this attempt ended abruptly."""

        self._discard_draft()
        try:
            write_json_atomic(
                self.control / "error.json",
                {
                    "format": RUNNER_ERROR_FORMAT,
                    "format_version": 2,
                    "step": self.step,
                    "exception": type(exception).__name__,
                    "message": str(exception),
                    "traceback": "".join(traceback.format_exception(exception)),
                },
                durable=self.context.durable,
            )
        except OSError as exc:  # pragma: no cover - the original exception wins
            _LOGGER.debug("cannot write the error breadcrumb of %s: %s", self.context.attempt_id, exc)


class Runner:
    """The registered steps of one workflow and the dispatch into them.

    A runner is created once at module level, its steps are registered with
    :meth:`step` before any work happens, and :meth:`main` is what the manager
    invokes. Registration is therefore complete before the first step runs, which
    is what lets every step name in a published outcome be checked against the
    steps that really exist.

    :param workflow: The workflow identifier and registry key implemented by this runner.
    :param inputs: The immutable creation-time staged-input declarations.
    """

    def __init__(self, workflow: str, *, inputs: Mapping[str, str | None] | None = None) -> None:
        self.workflow = require_string(workflow, "workflow")
        self._inputs = MappingProxyType(validate_inputs(inputs or {}))
        self._steps: dict[str, StepHandler] = {}
        self._instantiate: InstantiateHandler | None = None

    def __repr__(self) -> str:
        return f"Runner(workflow={self.workflow!r}, steps={len(self._steps)})"

    @property
    def inputs(self) -> Mapping[str, str | None]:
        """The immutable declared-input staging map."""

        return self._inputs

    @property
    def steps(self) -> frozenset[str]:
        """The names of every registered step."""

        return frozenset(self._steps)

    @property
    def has_instantiate(self) -> bool:
        """Whether this runner has a creation-time instantiate hook."""

        return self._instantiate is not None

    @overload
    def step(self, function: StepHandler) -> StepHandler: ...

    @overload
    def step(self, *, name: str | None = None) -> Callable[[StepHandler], StepHandler]: ...

    def step(
        self,
        function: StepHandler | None = None,
        *,
        name: str | None = None,
    ) -> StepHandler | Callable[[StepHandler], StepHandler]:
        """Register one step handler, named after the function unless *name* is given.

        :param function: The step handler, or ``None`` when used as a decorator factory.
        :param name: The registered step name, or the handler name when omitted.
        :return: The handler or a decorator that registers it.
        :raises ValueError: If the step name is invalid or already registered.
        """

        def register(handler: StepHandler) -> StepHandler:
            step = validate_step(handler.__name__ if name is None else name, "step")
            if step in self._steps:
                raise ValueError(f"step {step!r} is already registered on the {self.workflow} runner")
            self._steps[step] = handler
            return handler

        return register if function is None else register(function)

    def instantiate(self, function: InstantiateHandler) -> InstantiateHandler:
        """Register the hook receiving ``httk.workflow.scaffold.InstantiateContext``.

        :param function: The creation-time instantiate handler.
        :return: The handler, unchanged.
        :raises ValueError: If an instantiate handler is already registered.
        """

        if self._instantiate is not None:
            raise ValueError(f"an instantiate hook is already registered on the {self.workflow} runner")
        self._instantiate = function
        return function

    def description(self) -> dict[str, object]:
        """Return the machine-readable description of this runner.

        :return: The runner description document.
        """

        description = {
            "format": RUNNER_DESCRIPTION_FORMAT,
            "format_version": 2,
            "workflow": self.workflow,
            "steps": sorted(self._steps),
        }
        if self.inputs:
            description["inputs"] = dict(self.inputs)
        if self.has_instantiate:
            description["instantiate"] = True
        return description

    def main(self, argv: Sequence[str] | None = None) -> int:
        """Run the step this process was launched for and publish its outcome.

        Asked to describe itself — through ``HTTK_WORKFLOW_DESCRIBE=1`` or
        ``--describe`` — the runner prints its description and exits without
        touching anything, so a tool can enumerate the steps of a runner it is
        not running.

        Every ending is an outcome: a step that publishes none is reported as
        ``no_outcome``, an unimplemented step as ``unknown_step``, and a step that
        raises leaves an ``error.json`` breadcrumb and lets the exception reach the
        manager, whose retry policy owns what happens next.

        :param argv: Command-line arguments, or the process arguments when omitted.
        :return: Zero after dispatching or describing the runner.
        """

        arguments = sys.argv[1:] if argv is None else list(argv)
        if _DESCRIBE_FLAG in arguments or os.environ.get(_DESCRIBE_VARIABLE) == "1":
            print(json.dumps(self.description(), sort_keys=True))
            return 0
        attempt = Attempt.initialize(runner=self)
        if not attempt._prepare_environment():
            return 0
        handler = self._steps.get(attempt.step)
        if handler is None:
            registered = ", ".join(sorted(self._steps)) or "none"
            _LOGGER.error("step %s is not implemented by the %s runner", attempt.step, self.workflow)
            attempt.fail(
                "unknown_step",
                f"step {attempt.step!r} is not implemented by the {self.workflow} runner; "
                f"registered steps: {registered}",
            )
            return 0
        try:
            handler(attempt)
        except BaseException as exception:
            attempt._abort(exception)
            raise
        attempt._finish_environment_log()
        if not attempt.published:
            attempt.fail("no_outcome", f"step {attempt.step!r} finished without publishing an outcome")
        return 0
