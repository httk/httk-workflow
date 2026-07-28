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
        for site in range(a.input("sites")):
            a.spawn(ChildSpec(step="relax", inputs={"site": site}), label=f"site-{site}")
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
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Literal, Self, cast, overload

from ._util import read_json, require_string, write_json_atomic
from .errors import FormatError
from .models import (
    JOB_STATE_DIRECTORY,
    Failure,
    JobDefinition,
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
    "Runner",
    "RunnerRef",
]

RUNNER_DESCRIPTION_FORMAT = "httk-workflow-runner-description"
RUNNER_ERROR_FORMAT = "httk-workflow-runner-error"
_DESCRIBE_VARIABLE = "HTTK_WORKFLOW_DESCRIBE"
_DESCRIBE_FLAG = "--describe"
_RUNNER_STEPS_FILE = "runner-steps.json"

type StepHandler = Callable[["Attempt"], object]
type RunnerSource = Literal["payload", "workspace", "installed"]


class _Missing:
    """The sentinel telling a missing input from an input that is null."""

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "<missing>"


_MISSING = _Missing()


@dataclass(frozen=True)
class RunnerRef:
    """Which runner executes a child job synthesized by :class:`ChildSpec`.

    A synthesized child has no payload of its own, so its runner must be one that
    lives outside a payload: an entry of the workspace runner store, or an
    installed runner on the machine that runs it. :meth:`inherit` copies the
    reference of the spawning job itself, which is what a campaign whose steps all
    live in one published runner wants.
    """

    source: Literal["inherit", "workspace", "installed"] = "inherit"
    path: str | None = None
    sha256: str | None = None

    @classmethod
    def inherit(cls) -> "RunnerRef":
        """Reference exactly the runner of the spawning job."""

        return cls("inherit")

    @classmethod
    def workspace(cls, path: str | PurePosixPath, sha256: str) -> "RunnerRef":
        """Reference one runner published in the workspace runner store."""

        return cls("workspace", str(PurePosixPath(path)), validate_sha256(sha256, "runner sha256"))

    @classmethod
    def installed(cls, path: str | PurePosixPath, sha256: str) -> "RunnerRef":
        """Reference one runner installed on the machine that runs the child."""

        return cls("installed", str(PurePosixPath(path)), validate_sha256(sha256, "runner sha256"))

    def _resolve(self, parent: JobDefinition) -> tuple[str, RunnerSource, str, str | None, tuple[str, ...]]:
        """Return ``(backend, source, path, sha256, arguments)`` for a child."""

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
            parent.runner_backend,
            # Validated against the protocol's runner sources when the job was read.
            cast(RunnerSource, parent.runner_source),
            parent.runner_path.as_posix(),
            parent.runner_sha256,
            parent.runner_arguments,
        )


@dataclass(frozen=True)
class ChildSpec:
    """A complete child job described by the step and inputs it starts with.

    Everything not given follows the spawning job: its workflow, its claim pool,
    its priority, its resources, and its runner. The child therefore differs from
    its parent in exactly what the campaign varies, which is normally only *step*
    and *inputs*.
    """

    step: str
    inputs: Mapping[str, object] = field(default_factory=dict)
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

        backend, source, path, sha256, arguments = self.runner._resolve(parent)
        return JobSpec(
            name=self.name or f"{parent.name}: {self.step} ({label})",
            workflow=self.workflow or parent.workflow,
            runner_path=path,
            initial_step=validate_step(self.step, "child step"),
            tag=self.tag or label,
            runner_backend=backend,
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
            inputs=dict(self.inputs),
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
    def _observed(cls, raw: Mapping[str, object], workspace: Path) -> "ChildResult":
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
        """Return the child spawned under *label*, or *default*."""

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
    def inputs(self) -> Mapping[str, object]:
        """The application-defined ``inputs`` object of this job."""

        return self.job.inputs

    @property
    def children(self) -> ChildrenView:
        """The children observed by the join that started this activation."""

        if self._children is None:
            observations = self.context.raw.get("children")
            if not isinstance(observations, Sequence) or isinstance(observations, (str, bytes)):
                observations = ()
            self._children = ChildrenView(
                tuple(ChildResult._observed(item, self.workspace) for item in observations if isinstance(item, Mapping))
            )
        return self._children

    @property
    def published(self) -> bool:
        """Report whether this attempt already published its outcome."""

        return self._published is not None

    def input(self, name: str, default: object = _MISSING) -> object:
        """Return one member of the job's ``inputs`` object.

        Without a *default*, a missing input is a :exc:`KeyError`: a step that
        needs an input cannot run without it, and saying so immediately is better
        than failing later on a value that was never there.
        """

        inputs = self.job.inputs
        if name in inputs:
            return inputs[name]
        if isinstance(default, _Missing):
            available = ", ".join(sorted(inputs)) or "none"
            raise KeyError(f"job input {name!r} is not defined; defined inputs: {available}")
        return default

    def setting(self, name: str, default: object = None) -> object:
        """Resolve one application setting through its layers.

        The layers are consulted most-specific first, and the first that has the
        name wins: this job's ``inputs`` object, then the environment variable
        ``HTTK_`` + the name upper-cased with dots as underscores (so
        ``vasp.command`` reads ``HTTK_VASP_COMMAND``), then the workspace's
        application settings, then *default*. This is how a step reads the VASP
        command a workspace was configured with without the operator exporting it
        for every job, while still letting one job or one shell override it.
        """

        inputs = self.job.inputs
        if name in inputs:
            return inputs[name]
        variable = "HTTK_" + name.upper().replace(".", "_")
        if variable in os.environ:
            return os.environ[variable]
        settings = self.context.settings
        if name in settings:
            return settings[name]
        return default

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
        the subject was. A harvest reports the observed document beside the
        declared one and never merges the two.
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
        """Run an argv array in the workdir and reap its process group."""

        return run_command(
            argv,
            timeout=timeout,
            cwd=self.workdir if cwd is None else cwd,
            environment=environment,
            termination_grace=termination_grace,
        )

    def workdir_batch(self) -> ReplayableWorkdirBatch:
        """Start a replayable group of workdir changes."""

        return ReplayableWorkdirBatch.create(self.workdir, durable=self.context.durable)

    def put(self, source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> str:
        """Stage one file or directory for the job's transactional data.

        The operation is applied by the manager when the outcome is committed,
        exactly once, whatever happens to this process in between. Operation
        identifiers are generated in call order, so replaying the same step
        produces the same manifest.
        """

        transaction = self._data_transaction()
        operation_id = self._next_operation()
        path = Path(source)
        if path.is_dir() and not path.is_symlink():
            transaction.put_tree(operation_id, path, destination)
        else:
            transaction.put_file(operation_id, path, destination)
        return operation_id

    def remove(self, destination: str | os.PathLike[str], *, missing_ok: bool = False) -> str:
        """Remove one path from the job's transactional data."""

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
    ) -> Path:
        """Wait for the children spawned on this attempt, then run *step*.

        The join names exactly the children :meth:`spawn` registered on this
        attempt, which is also the bundle that creates them, so the manager can
        always resolve it. *when* is one of ``all_succeeded``, ``all_terminal``,
        ``any_succeeded``, or ``at_least`` with *count*. When the condition can no
        longer be met, the job advances to *on_impossible* if one is named and
        fails with ``dependency_failure`` otherwise.
        """

        self._reject_published()
        self._check_step(step, "gather target")
        if on_impossible is not None:
            self._check_step(on_impossible, "on_impossible target")
        draft = self._draft
        if draft is None or not draft.children:
            raise ValueError("gather joins the children spawned on this attempt, and none were spawned")
        join = join_mapping(draft.children, when, count, on_impossible)
        return self._publish("wait", next_step=step, join=join)

    def succeed(self) -> Path:
        """Publish the successful completion of this job."""

        return self._publish("succeed")

    def retry(self, reason: str) -> Path:
        """Ask for another attempt of this same activation."""

        return self._publish("retry", retry={"reason": reason})

    def pause(self, reason: str) -> Path:
        """Pause this job until an operator resumes it."""

        return self._publish("pause", pause={"reason": reason})

    def fail(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        retryable: bool = False,
    ) -> Path:
        """Publish a structured terminal failure.

        ``code`` is the token a job lists in ``retry_on``. ``retryable`` declares
        that repeating this attempt could help, which the manager honours within
        the attempt budgets of the job.
        """

        failure = Failure(code, message, details=details, retryable=retryable)
        return self._publish("fail", failure=failure.as_mapping())

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
        if declared is not None:
            self._record_steps(declared)
        return published

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
                    "format_version": 1,
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
    """

    def __init__(self, workflow: str) -> None:
        self.workflow = require_string(workflow, "workflow")
        self._steps: dict[str, StepHandler] = {}

    @property
    def steps(self) -> frozenset[str]:
        """The names of every registered step."""

        return frozenset(self._steps)

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
        """Register one step handler, named after the function unless *name* is given."""

        def register(handler: StepHandler) -> StepHandler:
            step = validate_step(handler.__name__ if name is None else name, "step")
            if step in self._steps:
                raise ValueError(f"step {step!r} is already registered on the {self.workflow} runner")
            self._steps[step] = handler
            return handler

        return register if function is None else register(function)

    def description(self) -> dict[str, object]:
        """Return the machine-readable description of this runner."""

        return {
            "format": RUNNER_DESCRIPTION_FORMAT,
            "format_version": 1,
            "workflow": self.workflow,
            "steps": sorted(self._steps),
        }

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
        """

        arguments = sys.argv[1:] if argv is None else list(argv)
        if _DESCRIBE_FLAG in arguments or os.environ.get(_DESCRIBE_VARIABLE) == "1":
            print(json.dumps(self.description(), sort_keys=True))
            return 0
        attempt = Attempt.initialize(runner=self)
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
        if not attempt.published:
            attempt.fail("no_outcome", f"step {attempt.step!r} finished without publishing an outcome")
        return 0
