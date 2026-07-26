"""Compatibility support for instantiated httk v1 task templates."""

import os
import pathlib
import re
import shutil
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from importlib.resources import files
from pathlib import Path, PurePosixPath

from ._util import read_json, require_int, write_json_atomic
from .backends import AttemptLaunch, OutcomeCommit
from .errors import FormatError, WorkflowError
from .manager import TaskManager
from .models import JobDefinition, Marker, normalize_placement, validate_label
from .workspace import WorkflowWorkspace

V1_BACKEND = "httk-v1"
V1_CAPABILITY = "httk-v1"
V1_WORKFLOW = "httk.v1.ht_steps"
V1_PRIORITY_MAP = {1: 100, 2: 300, 3: 500, 4: 700, 5: 900}
V2_TO_V1_PRIORITY = {value: key for key, value in V1_PRIORITY_MAP.items()}

_TASK_PATTERN = re.compile(
    r"^ht\.task\.(?P<taskset>[^.]+)\.(?P<task_id>[^.]+)\.(?P<step>[^.]+)\."
    r"(?P<restarts>[0-9]+)\.(?P<owner>[^.]+)\.(?P<priority>[1-5])\."
    r"(?P<status>waitstart|waitstep|waitsubtasks|running|finished|broken|stopped|timeout)$"
)

type V1Materializer = Callable[[pathlib.Path], None]


def bundled_v1_root() -> Path:
    """Return the packaged compatibility ``HTTK_DIR`` root."""

    return Path(str(files("httk.workflow").joinpath("v1_runtime")))


def legacy_priority(value: int) -> int:
    """Map a legacy priority from 1 through 5 onto the v2 range."""

    try:
        return V1_PRIORITY_MAP[value]
    except KeyError as exc:
        raise ValueError("legacy priority must be 1 through 5") from exc


def _safe_tag(value: str) -> str:
    tag = re.sub(r"[^a-z0-9._-]+", "-", value.lower()).strip("-._")
    tag = re.sub(r"-{2,}", "-", tag)[:48].rstrip("-._")
    return validate_label(tag or "v1-task", "tag")


def _program_in(payload: Path) -> tuple[str, str]:
    for name, program in (("ht_steps", "ht_steps"), ("ht_run", "ht_run")):
        path = payload / name
        if path.is_file():
            if not os.access(path, os.X_OK):
                raise FormatError(f"legacy runner is not executable: {path}")
            return name, program
    raise FormatError("legacy payload requires an executable ht_steps or ht_run")


def _v1_metadata(
    *,
    program: str,
    legacy_priority_value: int,
    attempts: int,
    root_placement: str | None,
    legacy_link: Mapping[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "profile": "httk-v1-task-v1",
        "program": program,
        "legacy_priority": legacy_priority_value,
        "attempts": attempts,
    }
    if root_placement is not None:
        result["root_placement"] = root_placement
    if legacy_link is not None:
        result["legacy_link"] = dict(legacy_link)
    return result


def _job_mapping(
    payload: Path,
    *,
    job_id: str,
    tag: str,
    name: str,
    initial_step: str,
    pool: str,
    priority: int,
    attempts: int,
    parent: Mapping[str, object] | None = None,
    root_placement: str | None = None,
    legacy_link: Mapping[str, object] | None = None,
) -> dict[str, object]:
    runner_path, program = _program_in(payload)
    if attempts < 0:
        raise ValueError("attempts must be zero or greater")
    validate_label(pool, "pool")
    return {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": str(uuid.UUID(job_id)),
        "tag": _safe_tag(tag),
        "name": name,
        "workflow": V1_WORKFLOW,
        "runner": {
            "backend": V1_BACKEND,
            "path": runner_path,
            "arguments": [],
        },
        "workdir": {"mode": "persistent", "path": "ht.run.current"},
        "data": {"mode": "none"},
        "initial_step": initial_step,
        "priority": legacy_priority(priority),
        "claim": {
            "pool": pool,
            "required_capabilities": [V1_CAPABILITY],
        },
        "retry_policy": {
            "maximum_attempts_per_activation": attempts + 1,
            "maximum_total_attempts": None,
            "maximum_activations": None,
            "retry_on": ["lease_lost", "process_failure"],
        },
        "resources": {},
        "parent": None if parent is None else dict(parent),
        "compatibility": _v1_metadata(
            program=program,
            legacy_priority_value=priority,
            attempts=attempts,
            root_placement=root_placement,
            legacy_link=legacy_link,
        ),
    }


def _execute_instantiator(payload: Path, globals_: Mapping[str, object]) -> None:
    script = payload / "ht.instantiate.py"
    if not script.is_file():
        raise FormatError("instantiate_globals were supplied but ht.instantiate.py is missing")
    namespace = dict(globals_)
    namespace.setdefault("__file__", str(script))
    namespace.setdefault("__name__", "__httk_v1_instantiate__")
    old_cwd = Path.cwd()
    old_argv = sys.argv
    try:
        os.chdir(payload)
        sys.argv = [str(script)]
        code = compile(script.read_bytes(), str(script), "exec")
        exec(code, namespace, namespace)
    finally:
        sys.argv = old_argv
        os.chdir(old_cwd)
    script.unlink()


def prepare_v1_payload(
    source: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    materializer: V1Materializer | None = None,
    instantiate_globals: Mapping[str, object] | None = None,
    job_id: str | None = None,
    tag: str = "v1-task",
    name: str = "httk v1 task",
    initial_step: str = "start",
    pool: str = "default",
    priority: int = 3,
    attempts: int = 10,
    parent: Mapping[str, object] | None = None,
    root_placement: str | None = None,
    legacy_link: Mapping[str, object] | None = None,
) -> JobDefinition:
    """Copy an instantiated v1 template and add its immutable v2 job definition.

    ``materializer`` and ``instantiate_globals`` are mutually exclusive. The
    latter executes trusted template code in the current interpreter and does
    not restore removed v1 Python imports.
    """

    if materializer is not None and instantiate_globals is not None:
        raise ValueError("materializer and instantiate_globals are mutually exclusive")
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    if not source_path.is_dir():
        raise FileNotFoundError(source_path)
    if destination_path.exists():
        raise FileExistsError(destination_path)
    if destination_path.is_relative_to(source_path):
        raise ValueError("destination must not be inside the legacy source directory")
    shutil.copytree(source_path, destination_path)
    try:
        if materializer is not None:
            materializer(destination_path)
        elif instantiate_globals is not None:
            _execute_instantiator(destination_path, instantiate_globals)
        if (destination_path / "job.json").exists():
            raise FormatError("legacy source already contains job.json")
        mapping = _job_mapping(
            destination_path,
            job_id=job_id or str(uuid.uuid4()),
            tag=tag,
            name=name,
            initial_step=initial_step,
            pool=pool,
            priority=priority,
            attempts=attempts,
            parent=parent,
            root_placement=root_placement,
            legacy_link=legacy_link,
        )
        job = JobDefinition.from_mapping(mapping)
        write_json_atomic(destination_path / "job.json", mapping)
        return job
    except Exception:
        shutil.rmtree(destination_path, ignore_errors=True)
        raise


def submit_v1_task(
    workspace: WorkflowWorkspace,
    source: str | os.PathLike[str],
    placement: str | PurePosixPath,
    *,
    materializer: V1Materializer | None = None,
    instantiate_globals: Mapping[str, object] | None = None,
    job_id: str | None = None,
    tag: str = "v1-task",
    name: str = "httk v1 task",
    initial_step: str = "start",
    pool: str = "default",
    priority: int = 3,
    attempts: int = 10,
) -> Marker:
    """Prepare and atomically submit one legacy task through a v2 workspace."""

    normalized = normalize_placement(placement)
    temporary = workspace.control / "tmp" / f"v1-submit.{uuid.uuid4()}"
    prepare_v1_payload(
        source,
        temporary,
        materializer=materializer,
        instantiate_globals=instantiate_globals,
        job_id=job_id,
        tag=tag,
        name=name,
        initial_step=initial_step,
        pool=pool,
        priority=priority,
        attempts=attempts,
        root_placement=normalized.as_posix(),
    )
    return workspace.submit(temporary, normalized, move=True)


def parse_v1_task_name(value: str) -> dict[str, str] | None:
    """Parse a legacy task basename, returning ``None`` when it is unrelated."""

    match = _TASK_PATTERN.fullmatch(value)
    return None if match is None else match.groupdict()


def _compatibility(job: JobDefinition) -> Mapping[str, object]:
    value = job.raw.get("compatibility")
    if not isinstance(value, Mapping) or value.get("profile") != "httk-v1-task-v1":
        raise FormatError("httk-v1 job requires compatibility profile httk-v1-task-v1")
    return value


class V1RunnerBackend:
    """Runner backend which adapts v1 filesystem decisions to v2 outcomes."""

    name = V1_BACKEND

    def __init__(
        self,
        *,
        runtime_root: str | os.PathLike[str] | None = None,
        timeout: float = 21600.0,
        wrapper: str | None = None,
        log_compression: str = "bzip2",
        attempts: int = 10,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if attempts < 0:
            raise ValueError("attempts must be zero or greater")
        if log_compression not in {"none", "bzip2", "zstd"}:
            raise ValueError("log_compression must be none, bzip2, or zstd")
        self.runtime_root = Path(runtime_root).resolve() if runtime_root is not None else bundled_v1_root()
        self.timeout = timeout
        self.wrapper = wrapper
        self.log_compression = log_compression
        self.attempts = attempts

    def validate(self, job: JobDefinition, payload: Path) -> None:
        compatibility = _compatibility(job)
        if job.workflow != V1_WORKFLOW:
            raise FormatError(f"httk-v1 backend cannot execute workflow {job.workflow!r}")
        program = str(compatibility.get("program", ""))
        if program not in {"ht_steps", "ht_run"}:
            raise FormatError("compatibility.program must be ht_steps or ht_run")
        require_int(compatibility.get("attempts"), "compatibility.attempts")
        require_int(
            compatibility.get("legacy_priority"),
            "compatibility.legacy_priority",
            minimum=1,
            maximum=5,
        )
        runner = payload.joinpath(*job.runner_path.parts)
        if runner.name != program or not runner.is_file() or not os.access(runner, os.X_OK):
            raise FormatError(f"legacy runner is missing or not executable: {runner}")
        api = self.runtime_root / "Execution" / "tasks" / "ht_tasks_api.sh"
        if not api.is_file():
            raise FormatError(f"legacy shell runtime is incomplete: {api}")

    def command(self, launch: AttemptLaunch) -> Sequence[str]:
        compatibility = _compatibility(launch.job)
        job_attempts = require_int(compatibility.get("attempts"), "compatibility.attempts")
        effective_attempts = min(self.attempts, job_attempts)
        runner = Path(__file__).with_name("_v1_runner.py")
        command = [
            sys.executable,
            str(runner),
            "--runtime-root",
            str(self.runtime_root),
            "--timeout",
            str(self.timeout),
            "--log-compression",
            self.log_compression,
            "--attempts",
            str(effective_attempts),
        ]
        if self.wrapper is not None:
            command.extend(["--wrapper", self.wrapper])
        return command

    def commit_outcome(self, commit: OutcomeCommit) -> None:
        spawn_path = commit.outcome_path / "children" / "spawn.json"
        if not spawn_path.is_file():
            return
        spawn = read_json(spawn_path)
        entries = spawn.get("children")
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
            raise FormatError("spawn children must be an array")
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise FormatError("spawn child must be an object")
            compatibility = entry.get("compatibility")
            if not isinstance(compatibility, Mapping):
                continue
            legacy_path = _relative_legacy_path(compatibility.get("legacy_path"))
            job_key = str(entry.get("job_key", ""))
            placement = normalize_placement(str(entry.get("placement", "")))
            target = commit.payload.parents[len(commit.marker.placement.parts)].joinpath(*placement.parts, job_key)
            _replace_with_relative_symlink(commit.payload / legacy_path, target)

    def reconcile(self, workspace: WorkflowWorkspace) -> None:
        for marker in workspace.scan_markers():
            try:
                job = workspace.load_job(marker)
                compatibility = _compatibility(job)
                link = compatibility.get("legacy_link")
                if isinstance(link, Mapping):
                    _reconcile_legacy_link(workspace, marker, link)
            except (WorkflowError, OSError):
                continue

    def marker_changed(self, workspace: WorkflowWorkspace, marker: Marker) -> None:
        job = workspace.load_job(marker)
        compatibility = _compatibility(job)
        link = compatibility.get("legacy_link")
        if isinstance(link, Mapping):
            _reconcile_legacy_link(workspace, marker, link)


class V1TaskManager(TaskManager):
    """A task manager restricted to jobs using the httk v1 runner backend."""

    def __init__(
        self,
        workspace: WorkflowWorkspace,
        *,
        taskset: str = "any",
        maximum_workers: int = 1,
        lease_seconds: float | None = None,
        heartbeat_interval: float = 30.0,
        unsafe_persistent_takeover: bool = False,
        runtime_root: str | os.PathLike[str] | None = None,
        timeout: float = 21600.0,
        wrapper: str | None = None,
        log_compression: str = "bzip2",
        attempts: int = 10,
    ) -> None:
        pools = ("default",) if taskset == "any" else (validate_label(taskset, "taskset"),)
        backend = V1RunnerBackend(
            runtime_root=runtime_root,
            timeout=timeout,
            wrapper=wrapper,
            log_compression=log_compression,
            attempts=attempts,
        )
        super().__init__(
            workspace,
            pools=pools,
            capabilities=(V1_CAPABILITY,),
            maximum_workers=maximum_workers,
            lease_seconds=lease_seconds,
            heartbeat_interval=heartbeat_interval,
            unsafe_persistent_takeover=unsafe_persistent_takeover,
            runner_backends=(backend,),
            allowed_backends=(V1_BACKEND,),
            accept_any_pool=taskset == "any",
        )


def _relative_legacy_path(value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise FormatError("legacy_path must be a nonempty relative path")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise FormatError("legacy_path must remain below the parent payload")
    return Path(*relative.parts)


def _replace_with_relative_symlink(source: Path, target: Path) -> None:
    source.parent.mkdir(parents=True, exist_ok=True)
    for candidate in source.parent.iterdir():
        if (
            parse_v1_task_name(candidate.name) is not None
            and candidate.is_symlink()
            and candidate.resolve(strict=False) == target.resolve(strict=False)
        ):
            return
    if source.is_symlink():
        if source.resolve(strict=False) == target.resolve(strict=False):
            return
        source.unlink()
    elif source.exists():
        consumed = source.with_name(f".httk-v1-consumed.{target.name}")
        if consumed.exists():
            shutil.rmtree(consumed)
        os.rename(source, consumed)
    temporary = source.with_name(f".httk-v1-link.{uuid.uuid4()}")
    relative_target = os.path.relpath(target, source.parent)
    temporary.symlink_to(relative_target, target_is_directory=True)
    os.replace(temporary, source)
    consumed = source.with_name(f".httk-v1-consumed.{target.name}")
    if consumed.exists():
        shutil.rmtree(consumed)


def _legacy_status(marker: Marker, state: Mapping[str, object]) -> str:
    if marker.kind in {"submitted", "ready", "claimed"}:
        activation = require_int(state.get("activation_ordinal", 1), "state.activation_ordinal")
        attempts = require_int(state.get("total_attempts", 0), "state.total_attempts")
        return "waitstart" if activation == 1 and attempts <= 1 else "waitstep"
    if marker.kind in {"running", "committing"}:
        return "running"
    if marker.kind == "waiting":
        return "waitsubtasks"
    if marker.kind == "succeeded":
        return "finished"
    if marker.kind == "failed":
        failure = state.get("failure")
        failure_code = failure.get("code") if isinstance(failure, Mapping) else None
        if failure_code == "timeout":
            return "timeout"
        if failure_code in {"process_failure", "retry_exhausted"}:
            return "stopped"
        return "broken"
    return "stopped"


def _reconcile_legacy_link(workspace: WorkflowWorkspace, marker: Marker, link: Mapping[str, object]) -> None:
    parent_key = str(link.get("parent_job_key", ""))
    parent_placement = normalize_placement(str(link.get("parent_placement", "")))
    link_directory = _relative_legacy_path(link.get("directory"))
    fields = link.get("fields")
    if not isinstance(fields, Mapping):
        raise FormatError("legacy_link.fields must be an object")
    field_values = {
        name: str(fields.get(name, ""))
        for name in ("taskset", "task_id", "step", "restarts", "owner", "priority", "status")
    }
    original_name = (
        f"ht.task.{field_values['taskset']}.{field_values['task_id']}.{field_values['step']}."
        f"{field_values['restarts']}.{field_values['owner']}.{field_values['priority']}."
        f"{field_values['status']}"
    )
    if parse_v1_task_name(original_name) != field_values:
        raise FormatError("legacy_link.fields do not form a valid v1 task name")
    state = workspace.read_state(marker)
    step = str(
        state.get("next_step")
        if marker.kind == "waiting" and state.get("next_step") is not None
        else state.get("step", field_values["step"])
    )
    restarts = max(
        0,
        require_int(state.get("total_attempts", 0), "state.total_attempts")
        - require_int(state.get("activation_ordinal", 1), "state.activation_ordinal"),
    )
    owner = (
        str(state.get("manager_id", "unclaimed"))
        if marker.kind in {"claimed", "running", "committing"}
        else "unclaimed"
    )
    priority = V2_TO_V1_PRIORITY.get(marker.priority, int(field_values["priority"]))
    parent_payload = workspace.payload_path(parent_placement, parent_key)
    directory = parent_payload / link_directory
    prefix = f"ht.task.{field_values['taskset']}.{field_values['task_id']}.{step}." f"{restarts}.{owner}.{priority}."
    desired = directory / f"{prefix}{_legacy_status(marker, state)}"
    target = workspace.payload_path(marker.placement, marker.job_key)
    candidates = []
    if directory.is_dir():
        for candidate in directory.iterdir():
            candidate_fields = parse_v1_task_name(candidate.name)
            if (
                candidate_fields is not None
                and candidate.is_symlink()
                and candidate.resolve(strict=False) == target.resolve(strict=False)
            ):
                candidates.append(candidate)
    if desired.is_symlink() and desired.resolve(strict=False) == target.resolve(strict=False):
        return
    for candidate in candidates:
        if candidate.is_symlink() and candidate.resolve(strict=False) == target.resolve(strict=False):
            if desired.exists() or desired.is_symlink():
                desired.unlink()
            os.rename(candidate, desired)
            return
    _replace_with_relative_symlink(desired, target)
