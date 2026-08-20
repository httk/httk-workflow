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
import time
import traceback
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Literal, Self, cast, overload

from ._util import json_bytes, read_json, require_string, validate_inputs, write_json_atomic
from .errors import FormatError
from .models import (
    JOB_STATE_DIRECTORY,
    RESERVED_WORKFLOW_ENVIRONMENT_PREFIX,
    Failure,
    JobDefinition,
    _matches_environment_type,
    environment_variable_name,
    validate_declaration_name,
    validate_declarations,
    validate_failure,
    validate_label,
    validate_sha256,
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
    TransactionBuilder,
    join_mapping,
)

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "RUNNER_DESCRIPTION_FORMAT",
    "RUNNER_ERROR_FORMAT",
    "Attempt",
    "ChildResult",
    "ChildSpec",
    "ChildrenView",
    "InstantiateHandler",
    "Runner",
    "RunnerRef",
]

RUNNER_DESCRIPTION_FORMAT = "httk-workflow-runner-description"
RUNNER_ERROR_FORMAT = "httk-workflow-runner-error"
_DESCRIBE_VARIABLE = "HTTK_WORKFLOW_DESCRIBE"
_DESCRIBE_FLAG = "--describe"
_RUNNER_STEPS_FILE = "runner-steps.json"
_ENVIRONMENT_MARKER = ".httk-environment-resolution.json"
_ENVIRONMENT_LOG_GRACE_SECONDS = 1.0

type StepHandler = Callable[["Attempt"], object]
type InstantiateHandler = Callable[[Any], object]
type RunnerSource = Literal["payload", "workspace", "installed"]
type EnvironmentSource = Literal["override", "environment-variable", "workspace-setting", "default"]


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

    environment = job.environment
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

    declared = job.environment.get("declared", {})
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
class RunnerRef:
    """Which runner executes a child job synthesized by :class:`ChildSpec`.

    A synthesized child has no payload of its own, so its runner must be one that
    lives outside a payload: an entry of the workspace runner store, or an
    installed runner on the machine that runs it. :meth:`inherit` copies the
    reference of the spawning job itself, which is what a campaign whose steps all
    live in one published runner wants.

    :param source: The location from which the child runner is loaded.
    :param path: The workspace or installed runner path when one is selected.
    :param sha256: The digest pin for a workspace or installed runner.
    """

    source: Literal["inherit", "workspace", "installed"] = "inherit"
    path: str | None = None
    sha256: str | None = None

    @classmethod
    def inherit(cls) -> "RunnerRef":
        """Reference exactly the runner of the spawning job.

        :return: The inherited runner reference.
        """

        return cls("inherit")

    @classmethod
    def workspace(cls, path: str | PurePosixPath, sha256: str) -> "RunnerRef":
        """Reference one runner published in the workspace runner store.

        :param path: The path within the workspace runner store.
        :param sha256: The runner digest.
        :return: The workspace runner reference.
        """

        return cls("workspace", str(PurePosixPath(path)), validate_sha256(sha256, "runner sha256"))

    @classmethod
    def installed(cls, path: str | PurePosixPath, sha256: str) -> "RunnerRef":
        """Reference one runner installed on the machine that runs the child.

        :param path: The installed runner path.
        :param sha256: The runner digest.
        :return: The installed runner reference.
        """

        return cls("installed", str(PurePosixPath(path)), validate_sha256(sha256, "runner sha256"))

    def _resolve(self, parent: JobDefinition) -> tuple[str, RunnerSource, str, str | None, tuple[str, ...]]:
        """Return ``(executor, source, path, sha256, arguments)`` for a child."""

        if self.source != "inherit":
            if self.path is None or self.sha256 is None:
                raise ValueError("a workspace or installed runner reference needs a path and a digest")
            return "path", self.source, self.path, self.sha256, ()
        if parent.runner_source == "payload":
            raise ValueError(
                "RunnerRef.inherit() needs a runner that lives outside the payload, but this job runs the "
                f"payload runner {parent.runner_path.as_posix()!r}; publish it with "
                "Workspace.publish_runner and reference it with RunnerRef.workspace(path, sha256), "
                "or spawn a prepared payload directory instead of a ChildSpec"
            )
        return (
            parent.runner_executor,
            # Validated against the protocol's runner sources when the job was read.
            cast(RunnerSource, parent.runner_source),
            parent.runner_path.as_posix(),
            parent.runner_sha256,
            parent.runner_arguments,
        )


@dataclass(frozen=True)
class ChildSpec:
    """A complete child job described by the step and parameters it starts with.

    Everything not given follows the spawning job: its workflow, its claim pool,
    its priority, its resources, and its runner. The child therefore differs from
    its parent in exactly what the campaign varies, which is normally only *step*
    and *parameters*.

    :param step: The first step the child runs.
    :param parameters: The opaque implementation knobs given to the child.
    :param declarations: The workflow declarations carried by the child.
    :param runner: The runner reference used to execute the child.
    :param name: The child job name, or a generated name when omitted.
    :param workflow: The child's workflow identifier, or the parent's when omitted.
    :param tag: The child's optional job tag, or the spawn label when omitted.
    :param workdir_mode: Whether the child's workdir persists or is isolated.
    :param workdir_path: The child's relative workdir path.
    :param data_mode: Whether the child has transactional data.
    :param priority: The child's priority, or the parent's when omitted.
    :param claim_pool: The child's claim pool, or the parent's when omitted.
    :param required_capabilities: Capabilities required by the child.
    :param resources: Resources requested by the child, or the parent's when omitted.
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
    runner: RunnerRef = field(default_factory=RunnerRef.inherit)
    name: str | None = None
    workflow: str | None = None
    tag: str | None = None
    workdir_mode: Literal["persistent", "isolated"] = "persistent"
    workdir_path: str = "run"
    data_mode: Literal["none", "transactional"] = "none"
    priority: int | None = None
    claim_pool: str | None = None
    required_capabilities: tuple[str, ...] = ()
    resources: Mapping[str, object] | None = None
    maximum_attempts_per_activation: int | None = None
    maximum_total_attempts: int | None = None
    maximum_activations: int | None = None
    retry_on: tuple[str, ...] = ()

    def _job_spec(self, parent: JobDefinition, label: str) -> JobSpec:
        """Return the job specification of this child under *parent*."""

        executor, source, path, sha256, arguments = self.runner._resolve(parent)
        return JobSpec(
            name=self.name or f"{parent.name}: {self.step} ({label})",
            workflow=self.workflow or parent.workflow,
            runner_path=path,
            initial_step=validate_step(self.step, "child step"),
            tag=self.tag or label,
            runner_executor=executor,
            runner_source=source,
            runner_sha256=sha256,
            runner_arguments=arguments,
            workdir_mode=self.workdir_mode,
            workdir_path=self.workdir_path,
            data_mode=self.data_mode,
            priority=parent.priority if self.priority is None else self.priority,
            claim_pool=parent.claim_pool if self.claim_pool is None else self.claim_pool,
            required_capabilities=self.required_capabilities,
            maximum_attempts_per_activation=self.maximum_attempts_per_activation,
            maximum_total_attempts=self.maximum_total_attempts,
            maximum_activations=self.maximum_activations,
            retry_on=self.retry_on,
            resources=dict(parent.resources) if self.resources is None else dict(self.resources),
            parameters=dict(self.parameters),
            declarations=validate_declarations(self.declarations, "child declarations"),
        )


@dataclass(frozen=True)
class ChildResult:
    """What one gathering step may know about one child it spawned.

    Every member is derived from authoritative state by the manager before the
    gathering activation starts, so reading a child is a pure read of the
    attempt context and never a scan of the workspace. Paths are absolute.
    """

    label: str | None
    job_id: str
    job_key: str
    kind: str
    failure: Failure | None
    placement: PurePosixPath
    payload: Path
    workdir: Path | None
    data: Path | None
    data_generation: int | None
    raw: Mapping[str, object]

    @property
    def succeeded(self) -> bool:
        """Report whether this child ended successfully."""

        return self.kind == "succeeded"

    @property
    def failed(self) -> bool:
        """Report whether this child ended badly."""

        return self.kind in {"failed", "cancelled"}

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object], workspace: Path) -> "ChildResult":
        label = raw.get("label")
        payload_path = PurePosixPath(require_string(raw.get("payload_path"), "child payload_path"))
        workdir_raw = raw.get("workdir_path")
        failure_raw = raw.get("failure")
        generation = raw.get("data_generation")
        payload = workspace.joinpath(*payload_path.parts)
        return cls(
            label=None if label is None else validate_label(label, "child label"),
            job_id=require_string(raw.get("job_id"), "child job_id"),
            job_key=require_string(raw.get("job_key"), "child job_key"),
            kind=require_string(raw.get("kind"), "child kind"),
            failure=None if not isinstance(failure_raw, Mapping) else validate_failure(failure_raw, "child failure"),
            placement=PurePosixPath(require_string(raw.get("placement"), "child placement")),
            payload=payload,
            workdir=None if not isinstance(workdir_raw, str) else workspace.joinpath(*PurePosixPath(workdir_raw).parts),
            data=None if generation is None else payload / "data",
            data_generation=None if not isinstance(generation, int) or isinstance(generation, bool) else generation,
            raw=dict(raw),
        )


@dataclass(frozen=True)
class ChildrenView:
    """The children observed by the join that started this activation.

    The view is empty for an activation that follows no join, so a step can read
    it unconditionally.
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

        :param label: The unique spawn label to find.
        :param default: The value to return when no child has that label.
        :return: The matching child, or the default value.
        """

        for child in self.all:
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
    the first :meth:`spawn`, :meth:`put`, or :meth:`remove`, and it is published
    by exactly one of :meth:`advance`, :meth:`gather`, :meth:`succeed`,
    :meth:`retry`, :meth:`pause`, or :meth:`fail`. Publication is the single
    atomic rename the manager observes, so nothing a step did takes effect until
    the step says how it ended.

    :param context: The manager-written identity and restart context.
    :param control: The attempt control directory.
    :param payload: The immutable job payload directory.
    :param workdir: The directory in which the step works.
    :param workspace: The workspace root containing the job.
    :param data: The job's transactional data directory, when enabled.
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
        self.data = data
        self.step = step or context.step
        # Every artifact this attempt publishes inherits the workspace's
        # durability from the manager-written context, so a durable workspace
        # synchronizes an outcome, a transaction, or a spawned child before it
        # is renamed authoritative without the step asking.
        self.state = JobState(payload, durable=context.durable)
        self.log = RunLog(workdir)
        self._runner = runner
        self._draft: OutcomeDraft | None = None
        self._transaction: TransactionBuilder | None = None
        self._operations = 0
        self._published: Path | None = None
        self._action: str | None = None
        self._job: JobDefinition | None = None
        self._children: ChildrenView | None = None
        self._environment_snapshot: dict[str, object] | None = None

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
    def parameters(self) -> Mapping[str, object]:
        """The application-defined ``parameters`` object of this job."""

        return self.job.parameters

    @property
    def children(self) -> ChildrenView:
        """The children observed by the join that started this activation."""

        if self._children is None:
            observations = self.context.raw.get("children")
            if not isinstance(observations, Sequence) or isinstance(observations, (str, bytes)):
                observations = ()
            self._children = ChildrenView(
                tuple(
                    ChildResult.from_mapping(item, self.workspace) for item in observations if isinstance(item, Mapping)
                )
            )
        return self._children

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

        parameters = self.job.parameters
        if name in parameters:
            return parameters[name]
        if isinstance(default, _Missing):
            available = ", ".join(sorted(parameters)) or "none"
            raise KeyError(f"job parameter {name!r} is not defined; defined parameters: {available}")
        return default

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

        parameters = self.job.parameters
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
        return self.job.declarations.get(declaration)

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

    def put(self, source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> str:
        """Stage one file or directory for the job's transactional data.

        The operation is applied by the manager when the outcome is committed,
        exactly once, whatever happens to this process in between. Operation
        identifiers are generated in call order, so replaying the same step
        produces the same manifest.

        :param source: The file or directory to stage.
        :param destination: The destination path in transactional data.
        :return: The generated transaction operation identifier.
        :raises ValueError: If this job has no transactional data.
        """

        transaction = self._data_transaction()
        operation_id = self._next_operation()
        path = Path(source)
        if path.is_dir() and not path.is_symlink():
            # A file put overwrites its destination; a directory put is made just
            # as idempotent by replacing a destination tree that already exists in
            # the committed data, so a step that advances back onto a step and
            # re-puts the same tree succeeds instead of failing on put-tree.
            replace = self.data is not None and (self.data / destination).exists()
            transaction.put_tree(operation_id, path, destination, replace=replace)
        else:
            transaction.put_file(operation_id, path, destination)
        return operation_id

    def remove(self, destination: str | os.PathLike[str], *, missing_ok: bool = False) -> str:
        """Remove one path from the job's transactional data.

        :param destination: The path to remove from transactional data.
        :param missing_ok: Whether an absent destination is acceptable.
        :return: The generated transaction operation identifier.
        :raises ValueError: If this job has no transactional data.
        """

        transaction = self._data_transaction()
        operation_id = self._next_operation()
        transaction.remove(operation_id, destination, missing_ok=missing_ok)
        return operation_id

    def spawn(
        self,
        child: "ChildSpec | str | os.PathLike[str]",
        *,
        label: str,
        placement: str | PurePosixPath | None = None,
    ) -> ChildReference:
        """Register one child job under *label*, to be created on publication.

        *child* is either a :class:`ChildSpec`, which needs no payload at all, or
        the path of a prepared payload directory. The label is mandatory and must
        be unique within one attempt: it is how :meth:`gather` and
        :attr:`children` name this child later.

        :param child: The child specification or prepared payload directory.
        :param label: The unique label used to observe the child later.
        :param placement: The workspace placement for the child, or this attempt's placement when omitted.
        :return: The reference to the registered child.
        :raises ValueError: If the child or label is invalid, or the label is reused.
        """

        self._reject_published()
        entry_label = validate_label(label, "child label")
        target = self.context.placement if placement is None else placement
        if not isinstance(child, ChildSpec):
            return self._require_draft().add_child(child, target, label=entry_label)
        if child.runner.source == "inherit":
            # An inherited runner is this very program, so the child's initial
            # step is a step of this runner and a typo in it is catchable here.
            self._check_step(child.step, "spawned child step")
        # Everything this child needs is validated before the draft exists, so a
        # refused spawn leaves the attempt exactly as it found it.
        spec = child._job_spec(self.job, entry_label)
        reference = self._require_draft().add_child_job(spec.as_mapping(), target, label=entry_label)
        _LOGGER.debug("spawned %s at step %s as %s", reference.job_key, child.step, entry_label)
        return reference

    def advance(
        self,
        step: str,
        *,
        state: Mapping[str, object] | None = None,
        priority: int | None = None,
    ) -> Path:
        """Publish a new activation of this job at *step*.

        *state* is written to :attr:`state` before the outcome is published, so
        the step that runs next always finds the state that decided to run it.

        :param step: The next registered step.
        :param state: State members to merge before publication.
        :param priority: The priority of the new activation, when changed.
        :return: The path of the published outcome.
        :raises RuntimeError: If this attempt already published an outcome.
        """

        self._reject_published()
        self._check_step(step, "advance target")
        if state:
            self.state.merge(state)
        return self._publish("advance", next_step=step, priority=priority)

    def gather(
        self,
        step: str,
        *,
        when: JoinCondition = "all_succeeded",
        count: int | None = None,
        on_impossible: str | None = None,
        rejoin: Iterable[str] = (),
        priority: int | None = None,
    ) -> Path:
        """Wait for this attempt's and optionally earlier children, then run *step*.

        The join names children :meth:`spawn` registered on this attempt and
        labels from earlier join activations named by *rejoin*. *when* is one of
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
        :return: The path of the published wait outcome.
        :raises ValueError: If no children were spawned or rejoined, a rejoined label is unknown, or a named step is invalid.
        """

        self._reject_published()
        self._check_step(step, "gather target")
        if on_impossible is not None:
            self._check_step(on_impossible, "on_impossible target")
        rejoin_children: list[dict[str, object]] = []
        known_labels = ", ".join(self.children.labels) or "none"
        draft = self._draft
        children = () if draft is None else draft.children
        labels: set[str] = set()
        identities: set[tuple[str, str]] = set()
        if draft is not None:
            for draft_child, entry in draft._children:
                label = entry.get("label")
                if label is not None:
                    label = str(label)
                    if label in labels:
                        raise ValueError(f"duplicate join child label: {label}")
                    labels.add(label)
                identity = (draft_child.workspace_id, draft_child.job_id)
                if identity in identities:
                    raise ValueError(f"duplicate join child identity: {draft_child.job_id}")
                identities.add(identity)
        for label in rejoin:
            observed = self.children.get(label)
            if observed is None:
                raise ValueError(f"unknown rejoin child label {label!r}; known labels: {known_labels}")
            if label in labels:
                raise ValueError(f"duplicate join child label: {label}")
            identity = (self.context.workspace_id, observed.job_id)
            if identity in identities:
                raise ValueError(f"duplicate join child identity: {observed.job_id}")
            labels.add(label)
            identities.add(identity)
            rejoin_children.append(
                {
                    "workspace_id": self.context.workspace_id,
                    "job_id": observed.job_id,
                    "job_key": observed.job_key,
                    "label": label,
                    "placement_hint": observed.placement.as_posix(),
                }
            )
        if not children and not rejoin_children:
            raise ValueError(
                "gather requires children spawned on this attempt or rejoin entries, and neither was provided"
            )
        join = join_mapping(children, when, count, on_impossible, rejoin_children)
        return self._publish("wait", next_step=step, priority=priority, join=join)

    def succeed(self) -> Path:
        """Publish the successful completion of this job.

        :return: The path of the published outcome.
        """

        return self._publish("succeed")

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

    def _data_transaction(self) -> TransactionBuilder:
        if self.context.data_generation is None:
            raise ValueError(
                "this job has data.mode none, so it has no data transaction; "
                "create the job with data_mode='transactional' to publish data operations"
            )
        if self._transaction is None:
            self._transaction = self._require_draft().transaction()
        return self._transaction

    def _next_operation(self) -> str:
        self._operations += 1
        return f"op-{self._operations:04d}"

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
    ) -> Path:
        draft = self._require_draft()
        declared = self._undeclared_steps()
        try:
            published = draft.publish(
                action,
                next_step=next_step,
                priority=priority,
                failure=failure,
                retry=retry,
                join=join,
                pause=pause,
                runner_steps=declared,
            )
        except Exception:
            self._discard_draft()
            raise
        self._published = published
        self._action = action
        self._activate_environment_log_deadline()
        if declared is not None:
            self._record_steps(declared)
        return published

    def _activate_environment_log_deadline(self) -> None:
        """Start the logging grace period when this attempt publishes."""

        marker = self.control / _ENVIRONMENT_MARKER
        if not marker.is_file():
            return
        recorded = read_json(marker)
        if recorded.get("status") != "resolved" or not recorded.get("log_pending"):
            return
        recorded = dict(recorded)
        recorded["log_deadline"] = time.time() + _ENVIRONMENT_LOG_GRACE_SECONDS
        write_json_atomic(marker, recorded, durable=self.context.durable)

    def _undeclared_steps(self) -> list[str] | None:
        """Return the runner's step set when this job has not recorded it yet."""

        if self._runner is None:
            return None
        steps = sorted(self._runner.steps)
        path = self.payload / JOB_STATE_DIRECTORY / _RUNNER_STEPS_FILE
        if path.is_file():
            try:
                if read_json(path).get("steps") == steps:
                    return None
            except FormatError:
                pass
        return steps

    def _record_steps(self, steps: Sequence[str]) -> None:
        path = self.payload / JOB_STATE_DIRECTORY / _RUNNER_STEPS_FILE
        try:
            write_json_atomic(path, {"steps": list(steps)}, durable=self.context.durable)
        except OSError as exc:  # pragma: no cover - only an optimization
            _LOGGER.debug("cannot record the declared steps of %s: %s", self.context.job_key, exc)

    def _discard_draft(self) -> None:
        """Remove an unpublished draft so no half-outcome survives this attempt."""

        draft = self._draft
        self._draft = None
        self._transaction = None
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
