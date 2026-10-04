"""Trusted local approval and publication for the workspace daemon."""

import hashlib
import json
import os
import re
import secrets
import selectors
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from httk.core.userdirs import data_home

from ._daemon_activation import activation_document, read_active_snapshot, verify_active_snapshot
from ._daemon_keys import initialize_response_seed, response_public_key, response_seed_path
from ._daemon_policy import (
    _HARD_LIMIT,
    MPIProfile,
    MPISettings,
    Policy,
    Profile,
    _object_without_duplicates,
    _open_nofollow,
    _parse_float,
    _read_policy_bytes,
    _reject_constant,
    load_policy,
    policy_document,
)
from ._daemon_policy import MAX_POLICY_BYTES as _MAX_RUNTIME_POLICY_BYTES
from ._daemon_policy import (
    _authorized_keys as _runtime_authorized_keys,
)
from ._daemon_policy import (
    _number as _runtime_number,
)
from ._daemon_state import Ledger
from .launchers import (
    LAUNCHER_METADATA,
    _locate_launcher,
    _validate_launcher_metadata,
    valid_launcher_name,
)
from .workspace import _validate_settings

_OPERATOR_FORMAT = "httk-workspace-daemon-policy"
_OPERATOR_VERSION = 2
_ENDPOINT_FORMAT = "httk-workspace-daemon-endpoint"
_MAX_WORKSPACE_BYTES = 1024 * 1024
_MAX_LAUNCHER_BYTES = 64 * 1024
_MAX_DISCOVERY_BYTES = 64 * 1024
_PROFILE_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_CLUSTER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_CANONICAL_INTEGER = re.compile(r"[1-9][0-9]*\Z")
_MEMORY = re.compile(r"([1-9][0-9]*)([KMGT]?)\Z")
_CPU_LIMIT = 1024
_MEMORY_LIMIT = 1_048_576
_TIME_LIMIT = 10_080
_FORCE_HINT = "; pass --force to approve it"
_SUPPORTED_SETTINGS = frozenset(
    {
        "manager.workers",
        "manager.command",
        "environment.prelude",
        "slurm.cpus_per_task",
        "slurm.mem",
        "slurm.time_limit",
        "slurm.nodes",
        "slurm.ntasks",
        "slurm.ntasks_per_node",
        "slurm.partition",
        "slurm.account",
        "slurm.mpi",
    }
)


@dataclass(frozen=True, slots=True)
class _OperatorPolicy:
    workspace: Path
    readonly_paths: tuple[Path, ...]
    broker_paths: tuple[Path, ...]
    authorized_keys: tuple[str, ...]
    allowed_launchers: tuple[str, ...]
    snapshot_root: Path
    state: Path
    requests: Path
    responses: Path
    bwrap: Path | None
    python: Path | None
    sbatch: Path | None
    squeue: Path | None
    scancel: Path | None
    scontrol: Path | None
    cluster: str | None
    slurm_conf: Path | None
    max_records: int = 4096
    max_submissions: int = 128
    poll_seconds: float = 1.0
    command_timeout: float = 30.0
    max_output_bytes: int = 65_536
    request_max_age: int = 3600
    mpi: Mapping[str, object] | None = None


def _read_bounded(path: Path, limit: int, *, protected: bool = False) -> bytes:
    descriptor = _open_nofollow(path)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError(f"expected a regular file: {path}")
        if protected and information.st_mode & 0o002:
            raise ValueError(f"protected file must not be world-writable: {path}")
        if information.st_size > limit:
            raise ValueError(f"JSON document is too large: {path}")
        data = bytearray()
        while len(data) <= limit:
            chunk = os.read(descriptor, limit + 1 - len(data))
            if not chunk:
                return bytes(data)
            data.extend(chunk)
        raise ValueError(f"JSON document is too large: {path}")
    finally:
        os.close(descriptor)


def _decode_object(data: bytes, *, description: str) -> dict[str, object]:
    try:
        value = json.loads(
            data.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
            parse_float=_parse_float,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"invalid {description} JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{description} must be a JSON object")
    return value


def _path(value: object, name: str) -> Path:
    if type(value) is not str:
        raise ValueError(f"{name} must be a string")
    result = Path(value)
    if not result.is_absolute() or ".." in result.parts or "\0" in value:
        raise ValueError(f"{name} must be an absolute path without '..' or NUL")
    return result


def _path_array(value: object, name: str) -> tuple[Path, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    result = tuple(_path(item, f"{name} entry") for item in value)
    if len(set(result)) != len(result):
        raise ValueError(f"{name} entries must be unique")
    return result


def _authorized_keys(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("authorized_keys must be a nonempty array")
    return _runtime_authorized_keys(tuple(value))


def _launcher_names(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("allowed_launchers must be a nonempty array")
    result: list[str] = []
    for item in value:
        if type(item) is not str:
            raise ValueError("allowed_launchers entries must be strings")
        try:
            name = valid_launcher_name(item)
        except RuntimeError as exc:
            raise ValueError(str(exc)) from exc
        if _PROFILE_NAME.fullmatch(name) is None:
            raise ValueError(f"launcher name cannot be used as an approved configuration: {name!r}")
        if name in result:
            raise ValueError("allowed_launchers entries must be unique")
        result.append(name)
    return tuple(result)


def _operator_integer(value: object, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


def _operator_number(value: object, name: str) -> float:
    return _runtime_number(value, name, -sys.float_info.max, sys.float_info.max)


def _state_default(workspace: Path) -> Path:
    digest = hashlib.sha256(str(workspace).encode("utf-8")).hexdigest()
    return data_home() / "workspace-daemons" / digest


def _decode_operator(path: Path) -> _OperatorPolicy:
    value = _decode_object(_read_policy_bytes(path), description="operator policy")
    required = {
        "format",
        "format_version",
        "workspace",
        "readonly_paths",
        "broker_paths",
        "authorized_keys",
        "allowed_launchers",
    }
    optional = {
        "snapshot_root",
        "state",
        "requests",
        "responses",
        "bwrap",
        "python",
        "sbatch",
        "squeue",
        "scancel",
        "scontrol",
        "cluster",
        "slurm_conf",
        "max_records",
        "max_submissions",
        "poll_seconds",
        "command_timeout",
        "max_output_bytes",
        "request_max_age",
        "mpi",
    }
    if not required <= set(value) or not set(value) <= required | optional:
        raise ValueError("operator policy fields are missing or unknown")
    if (
        value["format"] != _OPERATOR_FORMAT
        or type(value["format_version"]) is not int
        or value["format_version"] != _OPERATOR_VERSION
    ):
        raise ValueError("unsupported operator policy format or version")
    workspace = _path(value["workspace"], "workspace").resolve(strict=True)
    if not workspace.is_dir():
        raise ValueError("workspace must be an existing directory")
    default_prefix = workspace.name
    if not default_prefix:
        raise ValueError("workspace root cannot be the filesystem root")
    snapshot_root = _path(value.get("snapshot_root", str(path.parent / f"{path.stem}.daemon")), "snapshot_root")
    state = _path(value.get("state", str(_state_default(workspace))), "state")
    requests = _path(value.get("requests", str(workspace.with_name(f"{default_prefix}.daemon-requests"))), "requests")
    responses = _path(
        value.get("responses", str(workspace.with_name(f"{default_prefix}.daemon-responses"))), "responses"
    )
    cluster = value.get("cluster")
    if cluster is not None and (type(cluster) is not str or _CLUSTER_NAME.fullmatch(cluster) is None):
        raise ValueError("invalid cluster")
    mpi = value.get("mpi")
    if mpi is not None and not isinstance(mpi, dict):
        raise ValueError("mpi must be an object")
    return _OperatorPolicy(
        workspace=workspace,
        readonly_paths=_path_array(value["readonly_paths"], "readonly_paths"),
        broker_paths=_path_array(value["broker_paths"], "broker_paths"),
        authorized_keys=_authorized_keys(value["authorized_keys"]),
        allowed_launchers=_launcher_names(value["allowed_launchers"]),
        snapshot_root=snapshot_root,
        state=state,
        requests=requests,
        responses=responses,
        bwrap=_path(value["bwrap"], "bwrap") if "bwrap" in value else None,
        python=_path(value["python"], "python") if "python" in value else None,
        sbatch=_path(value["sbatch"], "sbatch") if "sbatch" in value else None,
        squeue=_path(value["squeue"], "squeue") if "squeue" in value else None,
        scancel=_path(value["scancel"], "scancel") if "scancel" in value else None,
        scontrol=_path(value["scontrol"], "scontrol") if "scontrol" in value else None,
        cluster=cluster,
        slurm_conf=_path(value["slurm_conf"], "slurm_conf") if "slurm_conf" in value else None,
        mpi=mpi,
        max_records=_operator_integer(value.get("max_records", 4096), "max_records"),
        max_submissions=_operator_integer(value.get("max_submissions", 128), "max_submissions"),
        poll_seconds=_operator_number(value.get("poll_seconds", 1.0), "poll_seconds"),
        command_timeout=_operator_number(value.get("command_timeout", 30.0), "command_timeout"),
        max_output_bytes=_operator_integer(value.get("max_output_bytes", 65_536), "max_output_bytes"),
        request_max_age=_operator_integer(value.get("request_max_age", 3600), "request_max_age"),
    )


def _operator(path: Path, workspace: Path) -> _OperatorPolicy:
    if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts or "\0" in str(path):
        raise ValueError("operator policy path must be absolute without '..' or NUL")
    operator = _decode_operator(path)
    requested = workspace.resolve(strict=True)
    if requested != operator.workspace:
        raise ValueError("workspace argument must exactly match the operator policy workspace")
    return operator


def _workspace_data(workspace: Path) -> tuple[str, dict[str, object]]:
    path = workspace / ".httk-workspace" / "format.json"
    value = _decode_object(_read_bounded(path, _MAX_WORKSPACE_BYTES), description="workspace format")
    if value.get("format") != "httk-workflow-filesystem" or value.get("format_version") != 2:
        raise ValueError("workspace must use httk-workflow-filesystem format version 2")
    workspace_id = value.get("workspace_id")
    if type(workspace_id) is not str:
        raise ValueError("workspace has no valid workspace_id")
    settings = _validate_settings(value.get("settings", {}))
    return workspace_id, settings


def _resolve_executable(value: Path | None, name: str) -> Path:
    if value is None:
        found = shutil.which(name)
    else:
        found = shutil.which(str(value))
    if found is None:
        raise ValueError(f"required executable is unavailable on the operator PATH: {name}")
    return Path(found).absolute()


def _positive_integer(value: object, name: str, maximum: int, hint: str = "") -> int:
    if type(value) is int:
        result = value
    elif type(value) is str and _CANONICAL_INTEGER.fullmatch(value) is not None:
        result = int(value)
    else:
        raise ValueError(f"{name} must be a positive integer or canonical decimal string")
    if not 1 <= result <= maximum:
        raise ValueError(f"{name} must be from 1 through {maximum}{hint}")
    return result


def _memory_mb(value: object, *, force: bool = False) -> int:
    if type(value) is int:
        result = value
    elif type(value) is str:
        match = _MEMORY.fullmatch(value)
        if match is None:
            raise ValueError("slurm.mem must be a positive integer with an optional K, M, G, or T suffix")
        amount, suffix = int(match.group(1)), match.group(2)
        if suffix == "K":
            result = (amount + 1023) // 1024
        elif suffix == "M" or not suffix:
            result = amount
        elif suffix == "G":
            result = amount * 1024
        else:
            result = amount * 1024 * 1024
    else:
        raise ValueError("slurm.mem must be a positive integer with an optional K, M, G, or T suffix")
    if not 1 <= result <= (_HARD_LIMIT if force else _MEMORY_LIMIT):
        if result >= 1:
            if force:
                raise ValueError(f"slurm.mem exceeds the {_HARD_LIMIT} MiB ceiling")
            raise ValueError(f"slurm.mem exceeds the {_MEMORY_LIMIT} MiB sanity limit{_FORCE_HINT}")
        raise ValueError("slurm.mem must normalize to at least 1 MiB")
    return result


def _time_minutes(value: object, *, force: bool = False) -> int:
    if type(value) is int:
        result = value
    elif type(value) is str and _CANONICAL_INTEGER.fullmatch(value) is not None:
        result = int(value)
    elif type(value) is str:
        days = 0
        clock = value
        if "-" in value:
            pieces = value.split("-")
            if len(pieces) != 2 or _CANONICAL_INTEGER.fullmatch(pieces[0]) is None:
                raise ValueError("invalid slurm.time_limit")
            days, clock = int(pieces[0]), pieces[1]
        fields = clock.split(":")
        if len(fields) not in (1, 2, 3) or any(not field.isdigit() or not field for field in fields):
            raise ValueError("invalid slurm.time_limit")
        numbers = [int(field) for field in fields]
        if days:
            if len(numbers) == 1:
                hours, minutes, seconds = numbers[0], 0, 0
            elif len(numbers) == 2:
                hours, minutes, seconds = numbers[0], numbers[1], 0
            else:
                hours, minutes, seconds = numbers
            if hours > 23 or minutes > 59 or seconds > 59:
                raise ValueError("invalid slurm.time_limit")
            total_seconds = ((days * 24 + hours) * 60 + minutes) * 60 + seconds
        elif len(numbers) == 2:
            minutes, seconds = numbers
            if seconds > 59:
                raise ValueError("invalid slurm.time_limit")
            total_seconds = minutes * 60 + seconds
        elif len(numbers) == 3:
            hours, minutes, seconds = numbers
            if minutes > 59 or seconds > 59:
                raise ValueError("invalid slurm.time_limit")
            total_seconds = (hours * 60 + minutes) * 60 + seconds
        else:
            raise ValueError("invalid slurm.time_limit")
        result = (total_seconds + 59) // 60
    else:
        raise ValueError("invalid slurm.time_limit")
    if not 1 <= result <= (_HARD_LIMIT if force else _TIME_LIMIT):
        if result >= 1:
            if force:
                raise ValueError(f"slurm.time_limit exceeds the {_HARD_LIMIT} minute ceiling")
            raise ValueError(f"slurm.time_limit exceeds the {_TIME_LIMIT} minute sanity limit{_FORCE_HINT}")
        raise ValueError("slurm.time_limit must normalize to at least 1 minute")
    return result


def _bounded_json_at(path: Path, limit: int, description: str) -> dict[str, object]:
    return _decode_object(_read_bounded(path, limit), description=description)


def _configuration(
    name: str,
    workspace: Path,
    workspace_settings: Mapping[str, object],
    mpi_settings: MPISettings | None,
    *,
    force: bool,
) -> Profile:
    target = _locate_launcher(name, project=workspace)
    root = target.bundle.resolve(strict=True)
    metadata = _bounded_json_at(root / LAUNCHER_METADATA, _MAX_LAUNCHER_BYTES, "launcher metadata")
    metadata = _validate_launcher_metadata(root, metadata, check_binaries=False)
    if metadata.get("kind") != "slurm":
        raise ValueError(f"approved launcher {name!r} must declare the stock Slurm kind")
    launcher_settings = metadata.get("settings", {})
    assert isinstance(launcher_settings, Mapping)
    settings = {**workspace_settings, **_validate_settings(launcher_settings)}
    unsupported = sorted(key for key in settings if key.startswith("slurm.") and key not in _SUPPORTED_SETTINGS)
    if unsupported:
        raise ValueError(f"unsupported Slurm launcher settings for {name!r}: {', '.join(unsupported)}")
    cpus = memory_mb = time_limit = None
    if "slurm.cpus_per_task" in settings:
        cpus = _positive_integer(
            settings["slurm.cpus_per_task"],
            "slurm.cpus_per_task",
            _HARD_LIMIT if force else _CPU_LIMIT,
            "" if force else _FORCE_HINT,
        )
    if "slurm.mem" in settings:
        memory_mb = _memory_mb(settings["slurm.mem"], force=force)
    if "slurm.time_limit" in settings:
        time_limit = _time_minutes(settings["slurm.time_limit"], force=force)
    workers = _positive_integer(settings.get("manager.workers", 1), "manager.workers", 1024)
    nodes = _positive_integer(settings.get("slurm.nodes", 1), "slurm.nodes", 4096)
    ntasks_per_node = None
    if "slurm.ntasks_per_node" in settings:
        ntasks_per_node = _positive_integer(settings["slurm.ntasks_per_node"], "slurm.ntasks_per_node", 65_536)
    if "slurm.ntasks" in settings:
        ranks = _positive_integer(settings["slurm.ntasks"], "slurm.ntasks", 65_536)
    else:
        ranks = nodes * ntasks_per_node if ntasks_per_node is not None else nodes
    if ranks < nodes or ranks > 65_536:
        raise ValueError("slurm.ntasks must be from slurm.nodes through 65536")
    if ntasks_per_node is not None and ranks > nodes * ntasks_per_node:
        raise ValueError("slurm.ntasks exceeds the approved node placement")
    mpi_mode = settings.get("slurm.mpi")
    if mpi_mode is not None and mpi_mode != "pmix":
        raise ValueError("slurm.mpi must be 'pmix' when set")
    mpi = mpi_mode == "pmix" or nodes > 1 or ranks > 1
    if mpi:
        if mpi_settings is None:
            raise ValueError(f"approved MPI launcher {name!r} requires operator mpi settings")
        if workers != 1:
            raise ValueError("MPI approved configurations require manager.workers=1")
        geometry = MPIProfile(nodes, ranks, ntasks_per_node)
    else:
        if nodes != 1 or ranks != 1 or ntasks_per_node not in (None, 1):
            raise ValueError("serial approved configurations require one node and one task")
        geometry = None
    partition = settings.get("slurm.partition")
    account = settings.get("slurm.account")
    for key, value in (("slurm.partition", partition), ("slurm.account", account)):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{key} must be a string")
    assert partition is None or isinstance(partition, str)
    assert account is None or isinstance(account, str)
    prelude = settings.get("environment.prelude", "")
    if type(prelude) is not str:
        raise ValueError("environment.prelude must be a string")
    raw_command = settings.get("manager.command")
    manager_command: str | None = None
    if raw_command is not None:
        if type(raw_command) is not str or not raw_command.strip() or "\0" in raw_command:
            raise ValueError("manager.command must be one nonempty executable string")
        manager_command = raw_command.strip()
    return Profile(
        name,
        cpus,
        memory_mb,
        time_limit,
        partition,
        account,
        geometry,
        workers,
        prelude,
        manager_command,
    )


def _mpi_settings(raw: Mapping[str, object] | None) -> MPISettings | None:
    if raw is None:
        return None
    required = {"control_root"}
    optional = {
        "srun",
        "pmix_roots",
        "shm_root",
        "devices",
        "environment",
        "max_steps",
        "termination_grace",
    }
    if not required <= set(raw) or not set(raw) <= required | optional:
        raise ValueError("mpi fields are missing or unknown")
    srun = _resolve_executable(_path(raw["srun"], "mpi.srun") if "srun" in raw else None, "srun")
    raw_pmix_roots = raw.get("pmix_roots", [])
    raw_devices = raw.get("devices", [])
    if not isinstance(raw_pmix_roots, list):
        raise ValueError("mpi.pmix_roots must be an array")
    if not isinstance(raw_devices, list):
        raise ValueError("mpi.devices must be an array")
    environment = raw.get("environment", {})
    if not isinstance(environment, dict):
        raise ValueError("mpi.environment must be an object")
    environment_pairs: list[tuple[str, str]] = []
    for name, setting in environment.items():
        if type(name) is not str or type(setting) is not str:
            raise ValueError("mpi.environment names and values must be strings")
        environment_pairs.append((name, setting))
    return MPISettings(
        srun=srun,
        control_root=_path(raw["control_root"], "mpi.control_root"),
        pmix_roots=tuple(_path(item, "mpi.pmix_roots entry") for item in raw_pmix_roots),
        shm_root=_path(raw.get("shm_root", "/dev/shm"), "mpi.shm_root"),
        devices=tuple(_path(item, "mpi.devices entry") for item in raw_devices),
        environment=tuple(environment_pairs),
        max_steps=_operator_integer(raw.get("max_steps", 128), "mpi.max_steps"),
        termination_grace=_operator_number(raw.get("termination_grace", 10.0), "mpi.termination_grace"),
    )


def _cluster_from_text(data: bytes, source: str, *, required: bool = True) -> str | None:
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{source} is not valid UTF-8") from exc
    names: list[str] = []
    for line in text.splitlines():
        content = line.split("#", 1)[0].strip()
        if not content:
            continue
        match = re.fullmatch(r"ClusterName\s*=\s*([^\s]+)", content)
        if match is not None:
            names.append(match.group(1))
        elif content.startswith("ClusterName"):
            raise ValueError(f"malformed ClusterName declaration in {source}")
    if not names and not required:
        return None
    if len(names) != 1 or _CLUSTER_NAME.fullmatch(names[0]) is None:
        raise ValueError(f"could not determine one valid ClusterName from {source}")
    return names[0]


def _run_scontrol(path: Path, *, slurm_conf: Path | None = None) -> bytes:
    environment = dict(os.environ)
    if slurm_conf is not None:
        environment["SLURM_CONF"] = str(slurm_conf)
    process = subprocess.Popen(
        [str(path), "show", "config"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=environment,
        close_fds=True,
    )
    assert process.stdout is not None
    os.set_blocking(process.stdout.fileno(), False)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + 10.0
    output = bytearray()
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                process.kill()
                raise ValueError("scontrol show config timed out")
            for key, _events in selector.select(remaining):
                chunk = os.read(key.fd, 4096)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                output.extend(chunk)
                if len(output) > _MAX_DISCOVERY_BYTES:
                    process.kill()
                    raise ValueError("scontrol show config output is too large")
        code = process.wait(timeout=max(0.1, deadline - time.monotonic()))
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.wait()
        raise
    finally:
        selector.close()
        process.stdout.close()
    if code != 0:
        raise ValueError(f"scontrol show config failed with status {code}")
    return bytes(output)


def _cluster(operator: _OperatorPolicy) -> str:
    if operator.cluster is not None:
        return operator.cluster
    environment = os.environ.get("SLURM_CLUSTER_NAME")
    if environment:
        if _CLUSTER_NAME.fullmatch(environment) is None:
            raise ValueError("SLURM_CLUSTER_NAME is invalid")
        return environment
    if operator.slurm_conf is not None:
        configured = _cluster_from_text(
            _read_bounded(operator.slurm_conf, _MAX_DISCOVERY_BYTES, protected=True),
            str(operator.slurm_conf),
            required=False,
        )
        if configured is not None:
            return configured
    scontrol = _resolve_executable(operator.scontrol, "scontrol")
    discovered = _cluster_from_text(_run_scontrol(scontrol, slurm_conf=operator.slurm_conf), "scontrol show config")
    assert discovered is not None
    return discovered


def _compile(operator: _OperatorPolicy, enrollment_id: str, *, force: bool = False) -> Policy:
    workspace_id, workspace_settings = _workspace_data(operator.workspace)
    bwrap = _resolve_executable(operator.bwrap, "bwrap")
    python = (
        Path(sys.executable).absolute() if operator.python is None else _resolve_executable(operator.python, "python")
    )
    sbatch = _resolve_executable(operator.sbatch, "sbatch")
    squeue = _resolve_executable(operator.squeue, "squeue")
    scancel = _resolve_executable(operator.scancel, "scancel")
    mpi = _mpi_settings(operator.mpi)
    profiles = tuple(
        _configuration(
            name,
            operator.workspace,
            workspace_settings,
            mpi,
            force=force,
        )
        for name in operator.allowed_launchers
    )
    return Policy(
        workspace=operator.workspace,
        workspace_id=workspace_id,
        enrollment_id=enrollment_id,
        requests=operator.requests,
        responses=operator.responses,
        state=operator.state,
        bwrap=bwrap,
        python=python,
        sbatch=sbatch,
        squeue=squeue,
        scancel=scancel,
        cluster=_cluster(operator),
        readonly_paths=operator.readonly_paths,
        broker_paths=operator.broker_paths,
        profiles=profiles,
        authorized_keys=operator.authorized_keys,
        slurm_conf=operator.slurm_conf,
        max_records=operator.max_records,
        max_submissions=operator.max_submissions,
        poll_seconds=operator.poll_seconds,
        command_timeout=operator.command_timeout,
        max_output_bytes=operator.max_output_bytes,
        request_max_age=operator.request_max_age,
        mpi=mpi,
    )


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _validate_setup_roots(operator: _OperatorPolicy, operator_path: Path) -> None:
    protected_policy = operator_path.resolve(strict=True)
    for writable in (operator.workspace, operator.requests, operator.responses):
        writable_resolved = writable.resolve(strict=False)
        if protected_policy == writable_resolved or protected_policy.is_relative_to(writable_resolved):
            raise ValueError("operator policy must be outside workspace and mailbox roots")
    mutable = (operator.workspace, operator.requests, operator.responses, operator.state, operator.snapshot_root)
    for index, left in enumerate(mutable):
        for right in mutable[index + 1 :]:
            if _overlap(left, right):
                raise ValueError("workspace, mailboxes, state, and snapshot_root must be pairwise disjoint")


def _mkdir_exclusive(path: Path) -> None:
    if not path.is_absolute() or ".." in path.parts or "\0" in str(path):
        raise ValueError("directory path must be absolute without '..' or NUL")
    descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    administrator_uid = os.fstat(descriptor).st_uid
    try:
        for index, component in enumerate(path.parts[1:], 1):
            final = index == len(path.parts) - 1
            try:
                os.mkdir(component, 0o700, dir_fd=descriptor)
                created = True
            except FileExistsError:
                if final:
                    raise
                created = False
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            information = os.fstat(next_descriptor)
            if created:
                os.fchmod(next_descriptor, 0o700)
                information = os.fstat(next_descriptor)
            mode = stat.S_IMODE(information.st_mode)
            sticky_administrator = information.st_uid == administrator_uid and bool(mode & stat.S_ISVTX)
            if mode & 0o002 and not sticky_administrator:
                raise ValueError(f"directory ancestry must not be world-writable: {path}")
            if information.st_uid not in (administrator_uid, os.geteuid()):
                raise ValueError(f"directory ancestry has foreign ownership: {path}")
            if final and (information.st_uid != os.geteuid() or stat.S_IMODE(information.st_mode) != 0o700):
                raise ValueError(f"private directory must be owned by the daemon user with mode 0700: {path}")
            previous, descriptor = descriptor, next_descriptor
            os.close(previous)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _open_directory_nofollow(path: Path) -> int:
    descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            previous, descriptor = descriptor, next_descriptor
            os.close(previous)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _validate_private_directory(path: Path) -> None:
    descriptor = _open_directory_nofollow(path)
    try:
        information = os.fstat(descriptor)
        if information.st_uid != os.geteuid() or stat.S_IMODE(information.st_mode) != 0o700:
            raise ValueError(f"private directory must be owned by the daemon user with mode 0700: {path}")
    finally:
        os.close(descriptor)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _runtime_policy_bytes(policy: Policy) -> bytes:
    data = _canonical_bytes(policy_document(policy))
    if len(data) > _MAX_RUNTIME_POLICY_BYTES:
        raise ValueError("compiled runtime policy exceeds the daemon policy size limit")
    return data


def _write_exclusive(path: Path, data: bytes) -> None:
    directory = _open_directory_nofollow(path.parent)
    temporary = f".{path.name}.{secrets.token_hex(16)}.tmp"
    descriptor = -1
    created = False
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        created = True
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("protected write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.link(
            temporary,
            path.name,
            src_dir_fd=directory,
            dst_dir_fd=directory,
            follow_symlinks=False,
        )
        os.fsync(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if created:
            try:
                os.unlink(temporary, dir_fd=directory)
                os.fsync(directory)
            except FileNotFoundError:
                pass
        os.close(directory)


def _write_atomic(path: Path, data: bytes) -> None:
    directory = _open_directory_nofollow(path.parent)
    temporary = f".{path.name}.{secrets.token_hex(16)}.tmp"
    descriptor = -1
    created = False
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        created = True
        os.fchmod(descriptor, 0o600)
        data = data + b"\n"
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("protected write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.rename(temporary, path.name, src_dir_fd=directory, dst_dir_fd=directory)
        created = False
        os.fsync(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if created:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass
        os.close(directory)


def _publish(operator: _OperatorPolicy, policy: Policy) -> Path:
    canonical = _runtime_policy_bytes(policy)
    digest = hashlib.sha256(canonical).hexdigest()
    snapshot = operator.snapshot_root / f"{digest}.json"
    try:
        _write_exclusive(snapshot, canonical)
    except FileExistsError:
        if _read_bounded(snapshot, _MAX_RUNTIME_POLICY_BYTES, protected=True) != canonical:
            raise ValueError("existing immutable daemon snapshot disagrees with its digest") from None
    active = activation_document(snapshot, policy)
    _write_atomic(operator.state / "active.json", _canonical_bytes(active))
    return snapshot


def initialize(workspace: Path, operator_path: Path, *, force: bool = False) -> Path:
    """Compile and publish a fresh local daemon enrollment.

    :param workspace: Uploaded workspace root.
    :param operator_path: Operator policy file.
    :param force: Approve CPU, memory and time requests above the built-in sanity limits.
    :return: Path of the published runtime snapshot.
    """

    operator = _operator(operator_path, workspace)
    _validate_setup_roots(operator, operator_path)
    policy = _compile(operator, secrets.token_hex(16), force=force)
    _runtime_policy_bytes(policy)
    for root in (operator.requests, operator.responses, operator.state, operator.snapshot_root):
        _mkdir_exclusive(root)
    initialize_response_seed(operator.state)
    with Ledger(
        operator.state,
        policy.workspace_id,
        policy.enrollment_id,
        initialize=True,
        max_records=policy.max_records,
        max_submissions=policy.max_submissions,
    ):
        return _publish(operator, policy)


def _active(operator: _OperatorPolicy) -> tuple[Path, Policy, str]:
    snapshot, digest = read_active_snapshot(operator.state)
    policy = load_policy(snapshot)
    verify_active_snapshot(operator.state, snapshot, policy)
    if policy.workspace != operator.workspace:
        raise ValueError("active daemon enrollment belongs to a different workspace")
    return snapshot, policy, digest


def active_policy_path(workspace: Path, operator_path: Path) -> Path:
    """Return the saved active runtime snapshot for ordinary daemon startup."""

    operator = _operator(operator_path, workspace)
    snapshot, _policy, _digest = _active(operator)
    return snapshot


def _fixed_connection(old: Policy, new: Policy) -> None:
    fields = (
        "workspace",
        "workspace_id",
        "enrollment_id",
        "requests",
        "responses",
        "state",
        "sbatch",
        "squeue",
        "scancel",
        "cluster",
        "slurm_conf",
        "broker_paths",
    )
    changed = [name for name in fields if getattr(old, name) != getattr(new, name)]
    if changed:
        raise ValueError(f"reload cannot change the enrollment scheduler connection: {', '.join(changed)}")


def reload(workspace: Path, operator_path: Path, *, force: bool = False) -> Path:
    """Compile and atomically approve a replacement active catalog.

    :param workspace: Uploaded workspace root.
    :param operator_path: Operator policy file.
    :param force: Approve CPU, memory and time requests above the built-in sanity limits.
    :return: Path of the newly approved runtime snapshot.
    """

    operator = _operator(operator_path, workspace)
    _validate_setup_roots(operator, operator_path)
    _validate_private_directory(operator.state)
    _validate_private_directory(operator.snapshot_root)
    old_snapshot, old, old_digest = _active(operator)
    new = _compile(operator, old.enrollment_id, force=force)
    _runtime_policy_bytes(new)
    _fixed_connection(old, new)
    with Ledger(
        old.state,
        old.workspace_id,
        old.enrollment_id,
        max_records=old.max_records,
        max_submissions=old.max_submissions,
    ):
        current_snapshot, current_digest = read_active_snapshot(operator.state)
        if current_snapshot != old_snapshot or current_digest != old_digest:
            raise ValueError("active daemon approval changed while reload was waiting for its lock")
        verify_active_snapshot(operator.state, old_snapshot, old)
        return _publish(operator, new)


def export_endpoint(workspace: Path, operator_path: Path) -> dict[str, object]:
    """Return the exact public endpoint and approved configuration catalog."""

    operator = _operator(operator_path, workspace)
    _snapshot, policy, _digest = _active(operator)
    return {
        "format": _ENDPOINT_FORMAT,
        "format_version": 1,
        "workspace_id": policy.workspace_id,
        "enrollment_id": policy.enrollment_id,
        "daemon_public_key": response_public_key(response_seed_path(policy.state)),
        "configurations": {profile.name: policy.configuration_digest(profile.name) for profile in policy.profiles},
        "request_max_age": policy.request_max_age,
    }


__all__ = ["active_policy_path", "export_endpoint", "initialize", "reload"]
