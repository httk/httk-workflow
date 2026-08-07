"""Builders used by native workflow runners.

The builders create protocol bundles below the attempt control directory.  A
bundle has no effect until the draft publication performs the final rename to
``outcome.ready``.

Application code composes outcomes through :class:`httk.workflow.Attempt`; the
classes here are the protocol-level building blocks that both the Python
authoring SDK and the Bash bridge publish with.
"""

import copy
import json
import os
import shutil
import uuid
from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, Self

from ._util import (
    fsync_directory,
    fsync_tree,
    json_bytes,
    read_json,
    sha256_file,
    tree_digest,
    utc_now,
    write_json_atomic,
)
from .models import (
    JOB_STATE_DIRECTORY,
    JobDefinition,
    normalize_placement,
    validate_declarations,
    validate_environment,
    validate_failure,
    validate_label,
    validate_parameters,
    validate_sha256,
    validate_step,
)
from .transactions import replay_transaction

if TYPE_CHECKING:
    from .runtime import AttemptContext

type OutcomeAction = Literal["advance", "retry", "wait", "succeed", "fail", "pause"]
type JoinCondition = Literal["all_succeeded", "all_terminal", "any_succeeded", "any_terminal", "at_least"]


def _relative(value: str | os.PathLike[str], name: str) -> PurePosixPath:
    result = PurePosixPath(os.fspath(value))
    if result.is_absolute() or not result.parts:
        raise ValueError(f"{name} must be a nonempty relative path")
    if any(part in {"", ".", "..", ".httk-workflow", ".httk-runner"} or "\x00" in part for part in result.parts):
        raise ValueError(f"{name} is not a safe normalized relative path")
    return result


def _copy_tree(source: Path, destination: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise ValueError(f"tree source is not a regular directory: {source}")
    for child in source.rglob("*"):
        if child.is_symlink() or not (child.is_file() or child.is_dir()):
            raise ValueError(f"tree contains a symlink or special file: {child}")
    shutil.copytree(source, destination)


@dataclass(frozen=True)
class ChildReference:
    """Identify one child in a native join.

    :param workspace_id: Identify the child's workspace.
    :param job_id: Identify the child job.
    :param job_key: Identify the child payload and markers.
    :param placement_hint: Locate the child within its workspace.
    """

    workspace_id: str
    job_id: str
    job_key: str
    placement_hint: str

    def as_mapping(self) -> dict[str, str]:
        """Return the protocol mapping for this child.

        :return: The serialized child reference.
        """
        return {
            "workspace_id": self.workspace_id,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "placement_hint": self.placement_hint,
        }


def join_mapping(
    children: Sequence[ChildReference],
    condition: JoinCondition = "all_succeeded",
    count: int | None = None,
    on_impossible_step: str | None = None,
    additional_children: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    """Return the validated ``join`` member of one waiting outcome.

    :param children: Identify the child jobs to join.
    :param condition: Select the condition that decides the join.
    :param count: Set the success threshold for ``at_least``.
    :param on_impossible_step: Name the step to advance to when the join is impossible.
    :param additional_children: Add already registered child references to the join.
    :return: The validated join mapping.
    :raises ValueError: If the child set or condition arguments are invalid.
    """

    serialized_children = [child.as_mapping() for child in children] + [dict(child) for child in additional_children]
    if not serialized_children:
        raise ValueError("a join requires at least one child")
    if condition == "at_least":
        if count is None or not 1 <= count <= len(serialized_children):
            raise ValueError("an at_least join requires a valid count")
    elif count is not None:
        raise ValueError("join count is valid only for at_least")
    result: dict[str, object] = {
        "children": serialized_children,
        "condition": condition,
    }
    if count is not None:
        result["count"] = count
    if on_impossible_step is not None:
        result["on_impossible"] = {"action": "advance", "next_step": validate_step(on_impossible_step)}
    return result


@dataclass(frozen=True)
class JobSpec:
    """Values needed to create an immutable native job definition.

    A ``payload`` runner is a file inside the job payload. A ``workspace`` or
    ``installed`` runner lives outside the payload and must therefore pin its own
    ``runner_sha256``, which is how one published runner serves a whole campaign
    of jobs without being copied per job.

    :param name: Set the job display name.
    :param workflow: Name the workflow.
    :param runner_path: Locate the runner.
    :param initial_step: Name the starting step.
    :param tag: Set the optional job tag.
    :param job_id: Preserve a job id when resuming or spawning.
    :param runner_executor: Select the runner executor.
    :param runner_source: Select where the runner lives.
    :param runner_sha256: Pin a runner outside the payload by digest.
    :param runner_arguments: Supply runner arguments.
    :param workdir_mode: Select the workdir mode.
    :param workdir_path: Name the workdir below the job payload.
    :param data_mode: Select the job data mode.
    :param priority: Set the scheduling priority.
    :param claim_pool: Select the claim pool.
    :param required_capabilities: Require these manager capabilities.
    :param maximum_attempts_per_activation: Bound attempts in one activation.
    :param maximum_total_attempts: Bound attempts across the job.
    :param maximum_activations: Bound job activations.
    :param retry_on: Name manager-detected failure codes eligible for retry.
    :param resources: Supply resource requirements.
    :param parameters: Supply opaque job parameters.
    :param environment: Supply declared environment metadata and overrides.
    :param declarations: Supply workflow declarations.
    :param compatibility: Supply an optional compatibility profile.
    """

    name: str
    workflow: str
    runner_path: str
    initial_step: str = "start"
    tag: str | None = None
    job_id: str | None = None
    runner_executor: str = "path"
    runner_source: Literal["payload", "workspace", "installed"] = "payload"
    runner_sha256: str | None = None
    runner_arguments: tuple[str, ...] = ()
    workdir_mode: Literal["persistent", "isolated"] = "persistent"
    workdir_path: str = "run"
    data_mode: Literal["none", "transactional"] = "none"
    priority: int = 500
    claim_pool: str = "default"
    required_capabilities: tuple[str, ...] = ()
    maximum_attempts_per_activation: int | None = None
    maximum_total_attempts: int | None = None
    maximum_activations: int | None = None
    retry_on: tuple[str, ...] = ()
    resources: Mapping[str, object] = field(default_factory=dict)
    parameters: Mapping[str, object] = field(default_factory=dict)
    environment: Mapping[str, object] = field(default_factory=dict)
    #: Workflow declarations carried verbatim into ``job.json``, keyed by name.
    declarations: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    compatibility: Mapping[str, object] | None = None

    def as_mapping(self, *, parent: Mapping[str, object] | None = None) -> dict[str, object]:
        """Return the validated job-definition mapping.

        :param parent: Identify the parent job when this is a spawned child.
        :return: The mapping written to ``job.json``.
        :raises ValueError: If runner placement or digest settings are invalid.
        """
        limits = {
            "maximum_attempts_per_activation": self.maximum_attempts_per_activation,
            "maximum_total_attempts": self.maximum_total_attempts,
            "maximum_activations": self.maximum_activations,
        }
        retry_policy: dict[str, object] = {name: value for name, value in limits.items() if value is not None}
        retry_policy["retry_on"] = list(self.retry_on)
        runner: dict[str, object] = {
            "executor": self.runner_executor,
            "source": self.runner_source,
            "path": self.runner_path,
            "arguments": list(self.runner_arguments),
        }
        if self.runner_source == "payload":
            if self.runner_sha256 is not None:
                raise ValueError("a payload runner is pinned by the job digest and cannot carry runner_sha256")
        else:
            if self.runner_sha256 is None:
                raise ValueError(f"a {self.runner_source} runner must pin runner_sha256")
            runner["sha256"] = validate_sha256(self.runner_sha256, "runner_sha256")
        result: dict[str, object] = {
            "format": "httk-workflow-job",
            "format_version": 1,
            "id": self.job_id or str(uuid.uuid4()),
            "tag": self.tag,
            "name": self.name,
            "workflow": self.workflow,
            "runner": runner,
            "workdir": {"mode": self.workdir_mode, "path": self.workdir_path},
            "data": {"mode": self.data_mode},
            "initial_step": self.initial_step,
            "priority": self.priority,
            "claim": {
                "pool": self.claim_pool,
                "required_capabilities": list(self.required_capabilities),
            },
            "retry_policy": retry_policy,
            "resources": dict(self.resources),
            "parent": None if parent is None else dict(parent),
        }
        if self.parameters:
            result["parameters"] = validate_parameters(self.parameters)
        if self.environment:
            result["environment"] = validate_environment(self.environment)
        if self.declarations:
            result["declarations"] = validate_declarations(self.declarations)
        if self.compatibility is not None:
            result["compatibility"] = copy.deepcopy(dict(self.compatibility))
        return result


def prepare_job_payload(
    destination: str | os.PathLike[str],
    spec: JobSpec,
    *,
    parent: Mapping[str, object] | None = None,
    durable: bool = False,
) -> JobDefinition:
    """Create and validate ``job.json`` in an existing prepared payload.

    *durable* synchronizes the written ``job.json`` for a caller preparing a
    payload directly on durable storage; it defaults to ``False`` because a
    payload prepared here is not yet a workspace artifact, and its submission
    is what makes it authoritative and durable.

    :param destination: Locate the prepared payload directory.
    :param spec: Supply the immutable job definition values.
    :param parent: Identify the parent job when preparing a child.
    :param durable: Synchronize ``job.json`` before returning.
    :return: The validated job definition.
    :raises FileExistsError: If ``job.json`` already exists.
    :raises ValueError: If the job definition or payload runner is invalid.
    """

    root = Path(destination)
    root.mkdir(parents=True, exist_ok=True)
    job_path = root / "job.json"
    if job_path.exists():
        raise FileExistsError(f"job definition already exists: {job_path}")
    mapping = spec.as_mapping(parent=parent)
    job = JobDefinition.from_mapping(mapping)
    if job.runner_source == "payload":
        runner = root.joinpath(*job.runner_path.parts)
        if not runner.is_file() or runner.is_symlink():
            raise ValueError(f"runner must be a regular file inside the payload: {job.runner_path}")
    write_json_atomic(job_path, mapping, durable=durable)
    return job


class TransactionBuilder:
    """Build a validated replayable transaction manifest.

    :param root: Locate the transaction staging directory.
    :param expected_generation: Pin the data generation the transaction applies to.
    :param durable: Synchronize the sealed manifest before returning it.
    """

    def __init__(self, root: Path, *, expected_generation: int, durable: bool = False) -> None:
        self.root = root
        self.expected_generation = expected_generation
        #: Whether :meth:`seal` synchronizes the manifest itself. The staged
        #: payload files this builder copies are synchronized in one batch by
        #: the owning publication — an outcome draft or a sealed workdir batch —
        #: just before its atomic rename, so an owner that batches sets this
        #: ``False`` and lets that single tree sync cover the manifest too.
        self.durable = durable
        self._operations: list[dict[str, object]] = []
        self._targets: list[PurePosixPath] = []
        (root / "payload").mkdir(parents=True, exist_ok=False)

    @classmethod
    def resume(cls, root: str | os.PathLike[str], *, expected_generation: int, durable: bool = False) -> Self:
        """Reattach to a transaction an earlier process of this attempt sealed.

        A Bash runner publishes one outcome through many short-lived processes, so
        the staged manifest on disk — not any in-memory counter — is what carries
        the operations of a draft from one call to the next. Resuming reads it
        back, so appending an operation continues the same sequence and the same
        overlap checks as the process that staged the first one.

        :param root: Locate the sealed transaction directory.
        :param expected_generation: Require this data generation in the manifest.
        :param durable: Preserve the durability setting for later sealing.
        :return: The resumed transaction builder.
        :raises ValueError: If the manifest format or generation is invalid.
        """

        target = Path(root)
        manifest = read_json(target / "manifest.json")
        if manifest.get("format") != "httk-workflow-transaction" or manifest.get("format_version") != 1:
            raise ValueError("transaction must use httk-workflow-transaction version 1")
        if manifest.get("expected_data_generation") != expected_generation:
            raise ValueError("transaction expected_data_generation does not match the attempt context")
        operations = manifest.get("operations")
        if not isinstance(operations, list):
            raise ValueError("sealed transaction manifest has no operations array")
        result = cls.__new__(cls)
        result.root = target
        result.expected_generation = expected_generation
        result.durable = durable
        result._operations = [dict(item) for item in operations if isinstance(item, Mapping)]
        if len(result._operations) != len(operations):
            raise ValueError("sealed transaction manifest has an operation that is not an object")
        result._targets = [
            PurePosixPath(str(item["path"])) for item in result._operations if item.get("op") != "make-dir"
        ]
        return result

    def __len__(self) -> int:
        """Return the number of operations staged on this transaction.

        :return: The staged operation count.
        """

        return len(self._operations)

    def _operation(
        self,
        operation_id: str,
        operation: str,
        path: str | os.PathLike[str],
        *,
        track_target: bool = True,
    ) -> dict[str, object]:
        identifier = validate_label(operation_id, "transaction operation id")
        if any(existing["id"] == identifier for existing in self._operations):
            raise ValueError(f"duplicate transaction operation id: {identifier}")
        target = _relative(path, "transaction target")
        if track_target:
            for existing in self._targets:
                if target == existing or target in existing.parents or existing in target.parents:
                    raise ValueError(f"transaction paths overlap: {existing} and {target}")
            self._targets.append(target)
        return {"id": identifier, "op": operation, "path": target.as_posix()}

    def make_dir(self, operation_id: str, path: str | os.PathLike[str]) -> None:
        """Stage creation of one directory.

        :param operation_id: Identify the transaction operation.
        :param path: Name the relative destination directory.
        """
        self._operations.append(self._operation(operation_id, "make-dir", path, track_target=False))

    def put_file(
        self,
        operation_id: str,
        source: str | os.PathLike[str],
        path: str | os.PathLike[str],
    ) -> None:
        """Stage one regular file for installation.

        :param operation_id: Identify the transaction operation.
        :param source: Locate the regular source file.
        :param path: Name the relative destination path.
        :raises ValueError: If the source is not a regular file.
        """
        item = self._operation(operation_id, "put-file", path)
        source_path = Path(source)
        if not source_path.is_file() or source_path.is_symlink():
            raise ValueError(f"put-file source must be a regular file: {source_path}")
        staged = self.root / "payload" / operation_id
        shutil.copy2(source_path, staged)
        item.update({"source": f"payload/{operation_id}", "sha256": sha256_file(staged)})
        self._operations.append(item)

    def put_tree(
        self,
        operation_id: str,
        source: str | os.PathLike[str],
        path: str | os.PathLike[str],
        *,
        replace: bool = False,
    ) -> None:
        """Stage one directory tree for installation.

        :param operation_id: Identify the transaction operation.
        :param source: Locate the regular source directory.
        :param path: Name the relative destination path.
        :param replace: Replace the destination tree instead of merging it.
        :raises ValueError: If the source tree contains an unsupported entry.
        """
        item = self._operation(operation_id, "replace-tree" if replace else "put-tree", path)
        staged = self.root / "payload" / operation_id
        _copy_tree(Path(source), staged)
        item.update({"source": f"payload/{operation_id}", "sha256": tree_digest(staged)})
        self._operations.append(item)

    def remove(self, operation_id: str, path: str | os.PathLike[str], *, missing_ok: bool = False) -> None:
        """Stage removal of one path.

        :param operation_id: Identify the transaction operation.
        :param path: Name the relative path to remove.
        :param missing_ok: Allow the destination to be absent during replay.
        """
        item = self._operation(operation_id, "remove", path)
        item["missing_ok"] = missing_ok
        self._operations.append(item)

    def seal(self) -> Path:
        """Seal the staged operations into a manifest.

        :return: The sealed manifest path.
        """
        write_json_atomic(
            self.root / "manifest.json",
            {
                "format": "httk-workflow-transaction",
                "format_version": 1,
                "id": str(uuid.uuid4()),
                "expected_data_generation": self.expected_generation,
                "operations": self._operations,
            },
            durable=self.durable,
        )
        return self.root / "manifest.json"


class OutcomeDraft:
    """One unpublished outcome bundle below an attempt control directory.

    The draft is the single place that writes the protocol shapes of an outcome:
    its transaction, its spawn set, and the atomic rename that publishes it. It
    is bound to nothing but the attempt identity and the control directory, so
    the authoring SDK and the Bash bridge publish through exactly one
    implementation.

    :param context: Provide the attempt identity and generation.
    :param control: Locate the attempt control directory.
    :param root: Locate an existing unpublished draft, when resuming one.
    :param durable: Synchronize the draft before publishing it.
    """

    def __init__(
        self,
        context: "AttemptContext",
        control: Path,
        root: Path | None = None,
        *,
        durable: bool = False,
    ) -> None:
        self.context = context
        self.control = control
        #: Whether :meth:`publish` synchronizes the draft before the atomic
        #: rename that publishes it. The whole draft is unreferenced until that
        #: rename, so nothing inside it is synchronized per write: one batched
        #: tree sync at publication makes the outcome, its transaction manifest
        #: and staged payload, and its child bundles durable together.
        self.durable = durable
        self.root = root or control / f"outcome.tmp.{uuid.uuid4()}"
        self.root.mkdir(exist_ok=False)
        self._transaction: TransactionBuilder | None = None
        self._children: list[tuple[ChildReference, dict[str, object]]] = []

    @classmethod
    def _resume(
        cls,
        context: "AttemptContext",
        control: Path,
        root: str | os.PathLike[str],
        *,
        durable: bool = False,
    ) -> Self:
        """Reattach to a draft an earlier process in this attempt created."""

        result = cls.__new__(cls)
        result.context = context
        result.control = control
        result.durable = durable
        result.root = Path(root).resolve()
        if result.root.parent != control or not result.root.name.startswith("outcome.tmp."):
            raise ValueError("outcome draft is not below this attempt control directory")
        if not result.root.is_dir():
            raise FileNotFoundError(result.root)
        result._transaction = None
        result._children = []
        spawn_path = result.root / "children" / "spawn.json"
        if spawn_path.exists():
            spawn = read_json(spawn_path)
            for raw in spawn.get("children", []):
                if isinstance(raw, Mapping):
                    reference = ChildReference(
                        str(raw["workspace_id"]),
                        str(raw["job_id"]),
                        str(raw["job_key"]),
                        str(raw["placement"]),
                    )
                    result._children.append((reference, dict(raw)))
        return result

    def transaction(self) -> TransactionBuilder:
        """Create the transaction builder for this outcome.

        :return: The outcome's transaction builder.
        :raises ValueError: If the attempt has no transactional data.
        :raises RuntimeError: If the outcome already has a transaction.
        """
        if self.context.data_generation is None:
            raise ValueError("this job does not use transactional data")
        if self._transaction is not None or (self.root / "transaction").exists():
            raise RuntimeError("an outcome can contain only one transaction")
        self._transaction = TransactionBuilder(
            self.root / "transaction",
            expected_generation=self.context.data_generation,
            # The draft's own publish-time tree sync makes the manifest and the
            # staged payload durable in one batch, so the builder does not sync
            # the manifest a second time on its own.
            durable=False,
        )
        return self._transaction

    def _child_label(self, requested: str | None, child: JobDefinition) -> str:
        """Return one child's spawn label, which must be unique in this set."""

        used = {str(item[1].get("label")) for item in self._children}
        if requested is not None:
            label = validate_label(requested, "child label")
            if label in used:
                raise ValueError(f"child label is not unique within this spawn set: {label}")
            return label
        base = child.tag or "child"
        label = base
        index = 1
        while label in used:
            index += 1
            label = f"{base}-{index}"
        return validate_label(label, "child label")

    def add_child(
        self,
        payload: str | os.PathLike[str],
        placement: str | PurePosixPath,
        *,
        label: str | None = None,
    ) -> ChildReference:
        """Register one prepared payload directory as a child of this outcome.

        :param payload: Locate the prepared child payload.
        :param placement: Place the child within the workspace.
        :param label: Set the child's unique spawn label.
        :return: The registered child reference.
        """

        source = Path(payload)
        return self._register_child(read_json(source / "job.json"), placement, label=label, source=source)

    def add_child_job(
        self,
        job: Mapping[str, object],
        placement: str | PurePosixPath,
        *,
        label: str,
    ) -> ChildReference:
        """Register one synthesized child that needs no prepared payload.

        A child whose runner lives outside the payload — a workspace or installed
        runner — is completely described by its ``job.json``, so a partitioned
        campaign can spawn children without copying a payload tree per child.

        :param job: Supply the synthesized child job definition.
        :param placement: Place the child within the workspace.
        :param label: Set the child's unique spawn label.
        :return: The registered child reference.
        """

        return self._register_child(dict(job), placement, label=label, source=None)

    def _register_child(
        self,
        child_mapping: dict[str, object],
        placement: str | PurePosixPath,
        *,
        label: str | None,
        source: Path | None,
    ) -> ChildReference:
        spawn_id = str(uuid.uuid4())
        child_mapping["parent"] = {
            "workspace_id": self.context.workspace_id,
            "job_id": self.context.job_id,
            "job_key": self.context.job_key,
            # The parent's own placement, so a manager can probe its exact state
            # set — for the decided-join revival guard — without a workspace scan.
            "placement": self.context.placement,
            "activation_id": self.context.activation_id,
            "spawn_id": spawn_id,
        }
        child = JobDefinition.from_mapping(child_mapping)
        entry_label = self._child_label(label, child)
        normalized = normalize_placement(placement)
        jobs = self.root / "children" / "jobs"
        jobs.mkdir(parents=True, exist_ok=True)
        destination = jobs / child.job_key
        if source is None:
            destination.mkdir(exist_ok=False)
        else:
            _copy_tree(source, destination)
        # Draft-internal: the publish-time tree sync of this draft synchronizes
        # every staged child bundle in one batch just before the outcome rename.
        write_json_atomic(destination / "job.json", child_mapping, durable=False)
        reference = ChildReference(
            self.context.workspace_id,
            child.id,
            child.job_key,
            normalized.as_posix(),
        )
        entry: dict[str, object] = {
            "workspace_id": reference.workspace_id,
            "job_id": reference.job_id,
            "job_key": reference.job_key,
            "label": entry_label,
            "placement": reference.placement_hint,
            "spawn_id": spawn_id,
        }
        self._children.append((reference, entry))
        self._write_spawn()
        return reference

    @property
    def children(self) -> tuple[ChildReference, ...]:
        """Return the children registered in this outcome.

        :return: The child references in registration order.
        """
        return tuple(item[0] for item in self._children)

    def _write_spawn(self) -> None:
        write_json_atomic(
            self.root / "children" / "spawn.json",
            {
                "format": "httk-workflow-spawn",
                "format_version": 1,
                "children": [item[1] for item in self._children],
            },
            # Draft-internal and rewritten once per registered child: the
            # publish-time tree sync makes the final set durable in one batch,
            # so this rewrite is never synchronized on its own.
            durable=False,
        )

    def publish(
        self,
        action: OutcomeAction,
        *,
        next_step: str | None = None,
        priority: int | None = None,
        failure: Mapping[str, object] | None = None,
        retry: Mapping[str, object] | None = None,
        join: Mapping[str, object] | None = None,
        pause: Mapping[str, object] | None = None,
        message: str | None = None,
        expected_data_generation: int | None = None,
        runner_steps: Sequence[str] | None = None,
    ) -> Path:
        """Publish this outcome atomically.

        :param action: Select the next job action.
        :param next_step: Name the next step for ``advance`` or ``wait``.
        :param priority: Override the next marker priority.
        :param failure: Supply canonical failure details for ``fail``.
        :param retry: Supply retry details for ``retry``.
        :param join: Supply join details for ``wait``.
        :param pause: Supply pause details for ``pause``.
        :param message: Attach an optional human-readable message.
        :param expected_data_generation: Confirm the transaction generation.
        :param runner_steps: Record the runner steps available to the manager.
        :return: The authoritative published outcome path.
        :raises FileExistsError: If an outcome is already published.
        :raises ValueError: If the action's required details are invalid.
        """
        ready = self.control / "outcome.ready"
        if ready.exists():
            raise FileExistsError(f"an outcome is already published: {ready}")
        if priority is not None and (isinstance(priority, bool) or not 0 <= priority <= 999):
            raise ValueError("priority must be an integer from 0 through 999")
        if action in {"advance", "wait"}:
            if next_step is None:
                raise ValueError(f"{action} requires next_step")
            validate_step(next_step)
        elif next_step is not None:
            raise ValueError(f"{action} does not accept next_step")
        if action == "wait" and join is None:
            join = join_mapping(self.children)
        if action == "fail":
            if failure is None:
                raise ValueError("fail requires failure details")
            # Normalize here so every publication path—native Python, the Bash
            # bridge, and application code composing a builder directly—emits
            # exactly the canonical failure shape.
            failure = validate_failure(failure).as_mapping()
        if action == "retry" and retry is None:
            raise ValueError("retry requires a reason")
        if action == "pause" and pause is None:
            raise ValueError("pause requires a reason")
        transaction_path = self.root / "transaction"
        if self._transaction is not None:
            self._transaction.seal()
        if transaction_path.is_dir():
            context_generation = self.context.data_generation
            manifest = read_json(transaction_path / "manifest.json")
            if manifest.get("format") != "httk-workflow-transaction" or manifest.get("format_version") != 1:
                raise ValueError("transaction must use httk-workflow-transaction version 1")
            if manifest.get("expected_data_generation") != context_generation:
                raise ValueError("transaction expected_data_generation does not match the attempt context")
            if expected_data_generation is None:
                expected_data_generation = context_generation
            elif expected_data_generation != context_generation:
                raise ValueError("expected_data_generation does not match the attempt context")
        body: dict[str, object] = {
            "format": "httk-workflow-outcome",
            "format_version": 1,
            "job_id": self.context.job_id,
            "activation_id": self.context.activation_id,
            "attempt_id": self.context.attempt_id,
            "action": action,
        }
        optional: dict[str, object | None] = {
            "next_step": next_step,
            "priority": priority,
            "failure": None if failure is None else dict(failure),
            "retry": None if retry is None else dict(retry),
            "join": join,
            "pause": None if pause is None else dict(pause),
            "message": message,
            "expected_data_generation": expected_data_generation,
            # The step set of the runner that published this outcome, recorded by
            # the manager as evidence of what this job can still be advanced to.
            "runner_steps": None if runner_steps is None else [validate_step(item) for item in runner_steps],
        }
        body.update({key: value for key, value in optional.items() if value is not None})
        # Draft-internal: durability of the whole draft is the one batched tree
        # sync below, so this final write is not synchronized on its own.
        write_json_atomic(self.root / "outcome.json", body, durable=False)
        if self.durable:
            # Everything the draft staged — the outcome, its sealed transaction
            # manifest and copied payload, its child bundles — is synchronized
            # here, before the rename that makes the outcome authoritative, so a
            # node crash can never leave a published outcome that names data its
            # storage never received.
            fsync_tree(self.root)
        os.rename(self.root, ready)
        if self.durable:
            # The rename created the ``outcome.ready`` name in the attempt
            # control directory; synchronize that directory entry too.
            fsync_directory(self.control)
        return ready


class ReplayableWorkdirBatch:
    """Build a sealed, idempotently replayable set of workdir changes.

    :param workdir: Locate the workdir receiving the changes.
    :param root: Locate the batch staging directory.
    :param durable: Synchronize staged and applied batch directories.
    """

    def __init__(self, workdir: Path, root: Path, *, durable: bool = False) -> None:
        self.workdir = workdir
        self.root = root
        #: Whether the batch synchronizes its sealed staging before publishing
        #: the ``workdir-ready`` name recovery replays from, and synchronizes the
        #: replayed workdir destinations before retiring the batch as applied.
        self.durable = durable
        self.transaction = TransactionBuilder(root, expected_generation=0, durable=False)

    @classmethod
    def create(cls, workdir: str | os.PathLike[str], *, durable: bool = False) -> "ReplayableWorkdirBatch":
        """Create a new workdir batch.

        :param workdir: Locate the workdir receiving the changes.
        :param durable: Synchronize the batch publications.
        :return: The new replayable batch.
        """
        target = Path(workdir).resolve()
        draft = target / ".httk-runner" / "workdir-drafts" / str(uuid.uuid4())
        draft.mkdir(parents=True)
        return cls(target, draft, durable=durable)

    def seal(self) -> Path:
        """Seal the batch for recovery.

        :return: The sealed ready-directory path.
        """
        self.transaction.seal()
        ready_root = self.workdir / ".httk-runner" / "workdir-ready"
        ready_root.mkdir(parents=True, exist_ok=True)
        ready = ready_root / self.root.name
        if self.durable:
            # The sealed batch is what recovery replays if this process dies
            # before the commit, so its manifest and staged payload are made
            # durable in one batched tree sync before the rename publishes it.
            fsync_tree(self.root)
        os.rename(self.root, ready)
        if self.durable:
            fsync_directory(ready_root)
        self.root = ready
        return ready

    def commit(self) -> Path:
        """Replay and retire the sealed batch.

        :return: The applied batch path.
        """
        if self.root.parent.name != "workdir-ready":
            self.seal()
        replay_transaction(self.root, self.workdir, expected_generation=0, durable=self.durable)
        applied_root = self.workdir / ".httk-runner" / "workdir-applied"
        applied_root.mkdir(parents=True, exist_ok=True)
        applied = applied_root / self.root.name
        os.rename(self.root, applied)
        if self.durable:
            fsync_directory(applied_root)
        self.root = applied
        return applied

    @staticmethod
    def recover(workdir: str | os.PathLike[str], *, durable: bool = False) -> tuple[Path, ...]:
        """Replay every ready batch found in a workdir.

        :param workdir: Locate the workdir containing ready batches.
        :param durable: Synchronize replayed publications.
        :return: The applied batch paths.
        """
        target = Path(workdir).resolve()
        ready_root = target / ".httk-runner" / "workdir-ready"
        if not ready_root.is_dir():
            return ()
        recovered: list[Path] = []
        for batch in sorted(ready_root.iterdir()):
            if not batch.is_dir():
                continue
            replay_transaction(batch, target, expected_generation=0, durable=durable)
            applied_root = target / ".httk-runner" / "workdir-applied"
            applied_root.mkdir(parents=True, exist_ok=True)
            applied = applied_root / batch.name
            os.rename(batch, applied)
            if durable:
                fsync_directory(applied_root)
            recovered.append(applied)
        return tuple(recovered)


class JobState(MutableMapping[str, object]):
    """Atomic JSON application state that belongs to one job.

    The state lives at ``.httk-job/state.json`` inside the job payload, so it
    survives every step advance, every retry, and every isolated workdir of the
    job, and it travels with the payload when the job is transferred. It is
    runner-private: the directory is excluded from every payload digest, so
    writing state never disturbs the immutability checks of the payload.

    Keys are nonempty strings and values must be JSON. Each mutation rewrites the
    whole document through an atomic replace, so a crash leaves either the
    previous state or the new one.

    :param payload: Locate the job payload containing the state directory.
    :param durable: Synchronize each atomic state replacement.
    """

    def __init__(self, payload: str | os.PathLike[str], *, durable: bool = False) -> None:
        self.path = Path(payload).resolve() / JOB_STATE_DIRECTORY / "state.json"
        #: Whether each atomic replace is synchronized. State is committed
        #: immediately rather than staged in a draft, so a durable workspace
        #: synchronizes every write here rather than deferring to a later batch.
        self.durable = durable

    def read(self) -> dict[str, object]:
        """Return the whole state document.

        :return: The current JSON state mapping.
        """

        return {} if not self.path.exists() else read_json(self.path)

    def merge(self, values: Mapping[str, object]) -> None:
        """Write several keys in one atomic replace.

        :param values: Supply the keys and values to merge.
        :raises ValueError: If a key or value is not valid JSON state.
        """

        state = self.read()
        for name, value in values.items():
            _check_state_item(name, value)
            state[name] = value
        write_json_atomic(self.path, state, durable=self.durable)

    def set(self, name: str, value: object) -> None:
        """Store one value, an alias of ``state[name] = value``.

        :param name: Name the state key.
        :param value: Supply the JSON-compatible value.
        :raises ValueError: If the key or value is invalid JSON state.
        """

        self[name] = value

    def delete(self, name: str) -> bool:
        """Remove one key, reporting whether it was present.

        :param name: Name the state key.
        :return: Whether the key was present.
        """

        state = self.read()
        if name not in state:
            return False
        del state[name]
        write_json_atomic(self.path, state, durable=self.durable)
        return True

    def __getitem__(self, name: str) -> object:
        return self.read()[name]

    def __setitem__(self, name: str, value: object) -> None:
        _check_state_item(name, value)
        state = self.read()
        state[name] = value
        write_json_atomic(self.path, state, durable=self.durable)

    def __delitem__(self, name: str) -> None:
        if not self.delete(name):
            raise KeyError(name)

    def __iter__(self) -> Iterator[str]:
        return iter(self.read())

    def __len__(self) -> int:
        return len(self.read())

    def __repr__(self) -> str:
        return f"JobState({str(self.path)!r})"


def _check_state_item(name: str, value: object) -> None:
    """Reject a key or a value that cannot be stored in JSON state."""

    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("job state keys must be nonempty strings without NUL")
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"job state value for {name!r} must be JSON: {exc}") from exc


class RunLog:
    """Append structured application evidence in a workdir.

    :param workdir: Locate the workdir receiving the run log.
    """

    def __init__(self, workdir: str | os.PathLike[str]) -> None:
        self.path = Path(workdir).resolve() / ".httk-runner" / "runlog.jsonl"

    def append(self, kind: str, message: str, *, files: Sequence[str | os.PathLike[str]] = ()) -> None:
        """Append one structured run-log event.

        :param kind: Name the event kind.
        :param message: Record the event message.
        :param files: Attach existing regular files as evidence.
        :raises ValueError: If *kind* is empty or contains NUL.
        """
        if not kind or "\x00" in kind:
            raise ValueError("run-log kind must be a nonempty string without NUL")
        attachments: list[dict[str, object]] = []
        for raw in files:
            path = Path(raw)
            if not path.is_file() or path.is_symlink():
                continue
            attachments.append(
                {
                    "path": os.fspath(raw),
                    "sha256": sha256_file(path),
                    "content": path.read_text(encoding="utf-8", errors="replace"),
                }
            )
        record = {
            "format": "httk-workflow-runlog-event",
            "format_version": 1,
            "timestamp": utc_now(),
            "kind": kind,
            "message": message,
            "files": attachments,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, json_bytes(record) + b"\n")
        finally:
            os.close(descriptor)
