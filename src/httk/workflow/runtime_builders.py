"""Builders used by native workflow runners.

The builders create protocol bundles below the attempt control directory.  A
bundle has no effect until :meth:`OutcomeBuilder.publish` performs the final
rename to ``outcome.ready``.
"""

import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal

from ._util import (
    json_bytes,
    read_json,
    sha256_file,
    tree_digest,
    utc_now,
    write_json_atomic,
)
from .models import (
    JobDefinition,
    normalize_placement,
    validate_failure,
    validate_label,
    validate_step,
)
from .transactions import replay_transaction

if TYPE_CHECKING:
    from .runtime import AttemptRuntime

type OutcomeAction = Literal["advance", "retry", "wait", "succeed", "fail", "pause"]
type JoinCondition = Literal["all_succeeded", "all_terminal", "any_succeeded", "at_least"]


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


@dataclass(frozen=True)
class JoinSpec:
    """An explicit set of children and its completion condition."""

    children: tuple[ChildReference, ...]
    condition: JoinCondition = "all_succeeded"
    count: int | None = None
    on_impossible_step: str | None = None

    def as_mapping(self) -> dict[str, object]:
        if not self.children:
            raise ValueError("a join requires at least one child")
        if self.condition == "at_least":
            if self.count is None or not 1 <= self.count <= len(self.children):
                raise ValueError("an at_least join requires a valid count")
        elif self.count is not None:
            raise ValueError("join count is valid only for at_least")
        result: dict[str, object] = {
            "children": [child.as_mapping() for child in self.children],
            "condition": self.condition,
        }
        if self.count is not None:
            result["count"] = self.count
        if self.on_impossible_step is not None:
            result["on_impossible"] = {
                "action": "advance",
                "next_step": validate_step(self.on_impossible_step),
            }
        return result


@dataclass(frozen=True)
class JobSpec:
    """Values needed to create an immutable native job payload."""

    name: str
    workflow: str
    runner_path: str
    initial_step: str = "start"
    tag: str | None = None
    job_id: str | None = None
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

    def as_mapping(self, *, parent: Mapping[str, object] | None = None) -> dict[str, object]:
        limits = {
            "maximum_attempts_per_activation": self.maximum_attempts_per_activation,
            "maximum_total_attempts": self.maximum_total_attempts,
            "maximum_activations": self.maximum_activations,
        }
        retry_policy: dict[str, object] = {name: value for name, value in limits.items() if value is not None}
        retry_policy["retry_on"] = list(self.retry_on)
        return {
            "format": "httk-workflow-job",
            "format_version": 1,
            "id": self.job_id or str(uuid.uuid4()),
            "tag": self.tag,
            "name": self.name,
            "workflow": self.workflow,
            "runner": {
                "backend": "path",
                "path": self.runner_path,
                "arguments": list(self.runner_arguments),
            },
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


class OutcomeBuilder:
    """Compose and atomically publish one native outcome."""

    def __init__(self, runtime: "AttemptRuntime", root: Path | None = None) -> None:
        self.runtime = runtime
        self.root = root or runtime.control / f"outcome.tmp.{uuid.uuid4()}"
        self.root.mkdir(exist_ok=False)
        self._transaction: TransactionBuilder | None = None
        self._children: list[tuple[ChildReference, dict[str, object]]] = []

    @classmethod
    def resume(cls, runtime: "AttemptRuntime", root: str | os.PathLike[str]) -> "OutcomeBuilder":
        result = cls.__new__(cls)
        result.runtime = runtime
        result.root = Path(root).resolve()
        if result.root.parent != runtime.control or not result.root.name.startswith("outcome.tmp."):
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
        if self.runtime.context.data_generation is None:
            raise ValueError("this job does not use transactional data")
        if self._transaction is not None or (self.root / "transaction").exists():
            raise RuntimeError("an outcome can contain only one transaction")
        self._transaction = TransactionBuilder(
            self.root / "transaction",
            expected_generation=self.runtime.context.data_generation,
        )
        return self._transaction

    def add_child(
        self,
        payload: str | os.PathLike[str],
        placement: str | PurePosixPath,
    ) -> ChildReference:
        source = Path(payload)
        child_mapping = read_json(source / "job.json")
        spawn_id = str(uuid.uuid4())
        child_mapping["parent"] = {
            "workspace_id": self.runtime.context.workspace_id,
            "job_id": self.runtime.context.job_id,
            "job_key": self.runtime.context.job_key,
            "activation_id": self.runtime.context.activation_id,
            "spawn_id": spawn_id,
        }
        child = JobDefinition.from_mapping(child_mapping)
        normalized = normalize_placement(placement)
        jobs = self.root / "children" / "jobs"
        jobs.mkdir(parents=True, exist_ok=True)
        destination = jobs / child.job_key
        _copy_tree(source, destination)
        write_json_atomic(destination / "job.json", child_mapping)
        reference = ChildReference(
            self.runtime.context.workspace_id,
            child.id,
            child.job_key,
            normalized.as_posix(),
        )
        entry: dict[str, object] = {
            "workspace_id": reference.workspace_id,
            "job_id": reference.job_id,
            "job_key": reference.job_key,
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
    ) -> Path:
        ready = self.runtime.control / "outcome.ready"
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
            join = JoinSpec(self.children)
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
            context_generation = self.runtime.context.data_generation
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
            "job_id": self.runtime.context.job_id,
            "activation_id": self.runtime.context.activation_id,
            "attempt_id": self.runtime.context.attempt_id,
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
        }
        body.update({key: value for key, value in optional.items() if value is not None})
        write_json_atomic(self.root / "outcome.json", body)
        os.rename(self.root, ready)
        return ready


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


class WorkdirState:
    """Atomic JSON application state stored below the current workdir."""

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
