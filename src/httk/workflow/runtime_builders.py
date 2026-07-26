"""Builders used by native workflow runners.

The builders create protocol bundles below the attempt control directory.  A
bundle has no effect until the draft publication performs the final rename to
``outcome.ready``.

Application code composes outcomes through :class:`httk.workflow.Attempt`; the
classes here are the protocol-level building blocks that both the Python
authoring SDK and the Bash bridge publish with.
"""

import json
import os
import shutil
import uuid
import warnings
from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, Self

from ._util import (
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
    validate_failure,
    validate_inputs,
    validate_label,
    validate_sha256,
    validate_step,
)
from .transactions import replay_transaction

if TYPE_CHECKING:
    from .runtime import AttemptContext, AttemptRuntime

type OutcomeAction = Literal["advance", "retry", "wait", "succeed", "fail", "pause"]
type JoinCondition = Literal["all_succeeded", "all_terminal", "any_succeeded", "at_least"]


def _warn_deprecated(name: str, replacement: str) -> None:
    """Announce one superseded interface that still works this release."""

    warnings.warn(
        f"{name} is deprecated and will be removed in a later release; use {replacement} instead",
        DeprecationWarning,
        stacklevel=3,
    )


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
    """The stable identity used in a native join."""

    workspace_id: str
    job_id: str
    job_key: str
    placement_hint: str

    def as_mapping(self) -> dict[str, str]:
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
) -> dict[str, object]:
    """Return the validated ``join`` member of one waiting outcome."""

    if not children:
        raise ValueError("a join requires at least one child")
    if condition == "at_least":
        if count is None or not 1 <= count <= len(children):
            raise ValueError("an at_least join requires a valid count")
    elif count is not None:
        raise ValueError("join count is valid only for at_least")
    result: dict[str, object] = {
        "children": [child.as_mapping() for child in children],
        "condition": condition,
    }
    if count is not None:
        result["count"] = count
    if on_impossible_step is not None:
        result["on_impossible"] = {"action": "advance", "next_step": validate_step(on_impossible_step)}
    return result


@dataclass(frozen=True)
class JoinSpec:
    """An explicit set of children and its completion condition.

    .. deprecated:: 0.2
       Use :meth:`httk.workflow.Attempt.gather`, which joins exactly the children
       spawned on the publishing attempt.
    """

    children: tuple[ChildReference, ...]
    condition: JoinCondition = "all_succeeded"
    count: int | None = None
    on_impossible_step: str | None = None

    def __post_init__(self) -> None:
        _warn_deprecated("JoinSpec", "Attempt.gather")

    def as_mapping(self) -> dict[str, object]:
        return join_mapping(self.children, self.condition, self.count, self.on_impossible_step)


@dataclass(frozen=True)
class JobSpec:
    """Values needed to create an immutable native job definition.

    A ``payload`` runner is a file inside the job payload. A ``workspace`` or
    ``installed`` runner lives outside the payload and must therefore pin its own
    ``runner_sha256``, which is how one published runner serves a whole campaign
    of jobs without being copied per job.
    """

    name: str
    workflow: str
    runner_path: str
    initial_step: str = "start"
    tag: str | None = None
    job_id: str | None = None
    runner_backend: str = "path"
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
    inputs: Mapping[str, object] = field(default_factory=dict)
    #: Workflow declarations carried verbatim into ``job.json``, keyed by name.
    declarations: Mapping[str, Mapping[str, object]] = field(default_factory=dict)

    def as_mapping(self, *, parent: Mapping[str, object] | None = None) -> dict[str, object]:
        limits = {
            "maximum_attempts_per_activation": self.maximum_attempts_per_activation,
            "maximum_total_attempts": self.maximum_total_attempts,
            "maximum_activations": self.maximum_activations,
        }
        retry_policy: dict[str, object] = {name: value for name, value in limits.items() if value is not None}
        retry_policy["retry_on"] = list(self.retry_on)
        runner: dict[str, object] = {
            "backend": self.runner_backend,
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
        if self.inputs:
            result["inputs"] = validate_inputs(self.inputs)
        if self.declarations:
            result["declarations"] = validate_declarations(self.declarations)
        return result


def prepare_job_payload(
    destination: str | os.PathLike[str],
    spec: JobSpec,
    *,
    parent: Mapping[str, object] | None = None,
) -> JobDefinition:
    """Create and validate ``job.json`` in an existing prepared payload."""

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
    write_json_atomic(job_path, mapping)
    return job


class TransactionBuilder:
    """Build a validated replayable transaction manifest."""

    def __init__(self, root: Path, *, expected_generation: int) -> None:
        self.root = root
        self.expected_generation = expected_generation
        self._operations: list[dict[str, object]] = []
        self._targets: list[PurePosixPath] = []
        (root / "payload").mkdir(parents=True, exist_ok=False)

    @classmethod
    def resume(cls, root: str | os.PathLike[str], *, expected_generation: int) -> Self:
        """Reattach to a transaction an earlier process of this attempt sealed.

        A Bash runner publishes one outcome through many short-lived processes, so
        the staged manifest on disk — not any in-memory counter — is what carries
        the operations of a draft from one call to the next. Resuming reads it
        back, so appending an operation continues the same sequence and the same
        overlap checks as the process that staged the first one.
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
        result._operations = [dict(item) for item in operations if isinstance(item, Mapping)]
        if len(result._operations) != len(operations):
            raise ValueError("sealed transaction manifest has an operation that is not an object")
        result._targets = [
            PurePosixPath(str(item["path"])) for item in result._operations if item.get("op") != "make-dir"
        ]
        return result

    def __len__(self) -> int:
        """The number of operations staged on this transaction so far."""

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
        self._operations.append(self._operation(operation_id, "make-dir", path, track_target=False))

    def put_file(
        self,
        operation_id: str,
        source: str | os.PathLike[str],
        path: str | os.PathLike[str],
    ) -> None:
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
        item = self._operation(operation_id, "replace-tree" if replace else "put-tree", path)
        staged = self.root / "payload" / operation_id
        _copy_tree(Path(source), staged)
        item.update({"source": f"payload/{operation_id}", "sha256": tree_digest(staged)})
        self._operations.append(item)

    def remove(self, operation_id: str, path: str | os.PathLike[str], *, missing_ok: bool = False) -> None:
        item = self._operation(operation_id, "remove", path)
        item["missing_ok"] = missing_ok
        self._operations.append(item)

    def seal(self) -> Path:
        write_json_atomic(
            self.root / "manifest.json",
            {
                "format": "httk-workflow-transaction",
                "format_version": 1,
                "id": str(uuid.uuid4()),
                "expected_data_generation": self.expected_generation,
                "operations": self._operations,
            },
        )
        return self.root / "manifest.json"


class OutcomeDraft:
    """One unpublished outcome bundle below an attempt control directory.

    The draft is the single place that writes the protocol shapes of an outcome:
    its transaction, its spawn set, and the atomic rename that publishes it. It
    is bound to nothing but the attempt identity and the control directory, so
    the authoring SDK and the Bash bridge publish through exactly one
    implementation.
    """

    def __init__(self, context: "AttemptContext", control: Path, root: Path | None = None) -> None:
        self.context = context
        self.control = control
        self.root = root or control / f"outcome.tmp.{uuid.uuid4()}"
        self.root.mkdir(exist_ok=False)
        self._transaction: TransactionBuilder | None = None
        self._children: list[tuple[ChildReference, dict[str, object]]] = []

    @classmethod
    def _resume(cls, context: "AttemptContext", control: Path, root: str | os.PathLike[str]) -> Self:
        """Reattach to a draft an earlier process in this attempt created."""

        result = cls.__new__(cls)
        result.context = context
        result.control = control
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
        if self.context.data_generation is None:
            raise ValueError("this job does not use transactional data")
        if self._transaction is not None or (self.root / "transaction").exists():
            raise RuntimeError("an outcome can contain only one transaction")
        self._transaction = TransactionBuilder(
            self.root / "transaction",
            expected_generation=self.context.data_generation,
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
        """Register one prepared payload directory as a child of this outcome."""

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
        runner — is completely described by its ``job.json``, so a campaign can
        spawn thousands of them without copying a payload tree per child.
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
        write_json_atomic(destination / "job.json", child_mapping)
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
        return tuple(item[0] for item in self._children)

    def _write_spawn(self) -> None:
        write_json_atomic(
            self.root / "children" / "spawn.json",
            {
                "format": "httk-workflow-spawn",
                "format_version": 1,
                "children": [item[1] for item in self._children],
            },
        )

    def publish(
        self,
        action: OutcomeAction,
        *,
        next_step: str | None = None,
        priority: int | None = None,
        failure: Mapping[str, object] | None = None,
        retry: Mapping[str, object] | None = None,
        join: JoinSpec | Mapping[str, object] | None = None,
        pause: Mapping[str, object] | None = None,
        message: str | None = None,
        expected_data_generation: int | None = None,
        runner_steps: Sequence[str] | None = None,
    ) -> Path:
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
            "join": join.as_mapping() if isinstance(join, JoinSpec) else join,
            "pause": None if pause is None else dict(pause),
            "message": message,
            "expected_data_generation": expected_data_generation,
            # The step set of the runner that published this outcome, recorded by
            # the manager as evidence of what this job can still be advanced to.
            "runner_steps": None if runner_steps is None else [validate_step(item) for item in runner_steps],
        }
        body.update({key: value for key, value in optional.items() if value is not None})
        write_json_atomic(self.root / "outcome.json", body)
        os.rename(self.root, ready)
        return ready


class OutcomeBuilder(OutcomeDraft):
    """Compose and atomically publish one native outcome.

    .. deprecated:: 0.2
       Use :class:`httk.workflow.Attempt`, which owns one implicit draft per
       attempt and publishes it through :meth:`~httk.workflow.Attempt.advance`,
       :meth:`~httk.workflow.Attempt.gather`, and the other outcome methods.
    """

    runtime: "AttemptRuntime"

    def __init__(self, runtime: "AttemptRuntime", root: Path | None = None) -> None:
        _warn_deprecated("OutcomeBuilder", "the outcome methods of httk.workflow.Attempt")
        self.runtime = runtime
        super().__init__(runtime.context, runtime.control, root)

    @classmethod
    def resume(cls, runtime: "AttemptRuntime", root: str | os.PathLike[str]) -> "OutcomeBuilder":
        """Reattach to a draft created earlier in this attempt."""

        _warn_deprecated("OutcomeBuilder", "the outcome methods of httk.workflow.Attempt")
        result = cls._resume(runtime.context, runtime.control, root)
        result.runtime = runtime
        return result


class ReplayableWorkdirBatch:
    """A sealed, idempotently replayable set of workdir changes."""

    def __init__(self, workdir: Path, root: Path) -> None:
        self.workdir = workdir
        self.root = root
        self.transaction = TransactionBuilder(root, expected_generation=0)

    @classmethod
    def create(cls, workdir: str | os.PathLike[str]) -> "ReplayableWorkdirBatch":
        target = Path(workdir).resolve()
        draft = target / ".httk-runner" / "workdir-drafts" / str(uuid.uuid4())
        draft.mkdir(parents=True)
        return cls(target, draft)

    def seal(self) -> Path:
        self.transaction.seal()
        ready_root = self.workdir / ".httk-runner" / "workdir-ready"
        ready_root.mkdir(parents=True, exist_ok=True)
        ready = ready_root / self.root.name
        os.rename(self.root, ready)
        self.root = ready
        return ready

    def commit(self) -> Path:
        if self.root.parent.name != "workdir-ready":
            self.seal()
        replay_transaction(self.root, self.workdir, expected_generation=0)
        applied_root = self.workdir / ".httk-runner" / "workdir-applied"
        applied_root.mkdir(parents=True, exist_ok=True)
        applied = applied_root / self.root.name
        os.rename(self.root, applied)
        self.root = applied
        return applied

    @staticmethod
    def recover(workdir: str | os.PathLike[str]) -> tuple[Path, ...]:
        target = Path(workdir).resolve()
        ready_root = target / ".httk-runner" / "workdir-ready"
        if not ready_root.is_dir():
            return ()
        recovered: list[Path] = []
        for batch in sorted(ready_root.iterdir()):
            if not batch.is_dir():
                continue
            replay_transaction(batch, target, expected_generation=0)
            applied_root = target / ".httk-runner" / "workdir-applied"
            applied_root.mkdir(parents=True, exist_ok=True)
            applied = applied_root / batch.name
            os.rename(batch, applied)
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
    """

    def __init__(self, payload: str | os.PathLike[str]) -> None:
        self.path = Path(payload).resolve() / JOB_STATE_DIRECTORY / "state.json"

    def read(self) -> dict[str, object]:
        """Return the whole state document."""

        return {} if not self.path.exists() else read_json(self.path)

    def merge(self, values: Mapping[str, object]) -> None:
        """Write several keys in one atomic replace."""

        state = self.read()
        for name, value in values.items():
            _check_state_item(name, value)
            state[name] = value
        write_json_atomic(self.path, state)

    def set(self, name: str, value: object) -> None:
        """Store one value, an alias of ``state[name] = value``."""

        self[name] = value

    def delete(self, name: str) -> bool:
        """Remove one key, reporting whether it was present."""

        state = self.read()
        if name not in state:
            return False
        del state[name]
        write_json_atomic(self.path, state)
        return True

    def __getitem__(self, name: str) -> object:
        return self.read()[name]

    def __setitem__(self, name: str, value: object) -> None:
        _check_state_item(name, value)
        state = self.read()
        state[name] = value
        write_json_atomic(self.path, state)

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


class WorkdirState:
    """Atomic JSON application state stored below the current workdir.

    .. deprecated:: 0.2
       Use :class:`JobState` through :attr:`httk.workflow.Attempt.state`, which
       survives isolated workdirs and step advances.
    """

    def __init__(self, workdir: str | os.PathLike[str]) -> None:
        self.path = Path(workdir).resolve() / ".httk-runner" / "state.json"

    def read(self) -> dict[str, object]:
        return {} if not self.path.exists() else read_json(self.path)

    def get(self, name: str, default: object = None) -> object:
        return self.read().get(name, default)

    def set(self, name: str, value: object) -> None:
        if not name or "\x00" in name:
            raise ValueError("state key must be a nonempty string without NUL")
        state = self.read()
        state[name] = value
        write_json_atomic(self.path, state)

    def delete(self, name: str) -> bool:
        state = self.read()
        if name not in state:
            return False
        del state[name]
        write_json_atomic(self.path, state)
        return True


class RunLog:
    """Append-only structured application evidence in a workdir."""

    def __init__(self, workdir: str | os.PathLike[str]) -> None:
        self.path = Path(workdir).resolve() / ".httk-runner" / "runlog.jsonl"

    def append(self, kind: str, message: str, *, files: Sequence[str | os.PathLike[str]] = ()) -> None:
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
