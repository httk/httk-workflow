"""Trusted local enrollment and publication for the workspace daemon."""

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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from httk.core.userdirs import data_home

from ._confine import confine_settings
from ._daemon_activation import activation_document, read_active_snapshot, verify_active_snapshot
from ._daemon_cli import _anchored
from ._daemon_keys import initialize_response_seed, response_public_key, response_seed_path
from ._daemon_policy import (
    _LAUNCHER_NAME,
    ApprovedLauncher,
    Policy,
    _object_without_duplicates,
    _open_directory,
    _open_nofollow,
    _overlap,
    _parse_float,
    _reject_constant,
    check_private_paths,
    load_policy,
    policy_document,
)
from ._daemon_policy import MAX_POLICY_BYTES as _MAX_RUNTIME_POLICY_BYTES
from ._daemon_policy import (
    _authorized_keys as _runtime_authorized_keys,
)
from ._daemon_slurm import submission
from ._daemon_state import Ledger
from ._exchange import ExchangeUnavailableError, enable_exchange, install_document
from .configuration import launchers_home
from .launchers import LAUNCHER_METADATA, _validate_launcher_metadata
from .models import EXCHANGE_DIRECTORY
from .workspace import Workspace

_DAEMON_DOCUMENT = "daemon.json"
_DAEMON_FORMAT = "httk-workspace-daemon"
_DAEMON_VERSION = 1
_MAX_WORKSPACE_BYTES = 1024 * 1024
_MAX_LAUNCHER_BYTES = 64 * 1024
_MAX_DISCOVERY_BYTES = 64 * 1024
_CLUSTER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_DEFAULT_SLURM_CONF = Path("/etc/slurm/slurm.conf")
_DEFAULT_MAX_SUBMISSIONS = 128
_CANONICAL_INTEGER = re.compile(r"[1-9][0-9]*\Z")
_MEMORY = re.compile(r"([1-9][0-9]*)([KMGT]?)\Z")
_CPU_LIMIT = 1024
_MEMORY_LIMIT = 1_048_576
_TIME_LIMIT = 10_080
_HARD_LIMIT = 2**31 - 1
_FORCE_HINT = "; set force=true to approve it"
_CONFIGURATION_FILE = "configuration.json"
_CONFIGURATION_FORMAT = "httk-workspace-daemon-configuration"
_CONFIGURATION_VERSION = 1
_MAX_CONFIGURATION_BYTES = 64 * 1024
_LIST_KEYS = ("launchers", "authorized_keys")
_EXECUTABLE_KEYS = ("bwrap", "python", "sbatch", "squeue", "scancel")
_OPTIONAL_KEYS = ("sacct", "slurm_conf")
#: The daemon configuration keys, in their stored and displayed order.
CONFIGURATION_KEYS = (*_LIST_KEYS, *_EXECUTABLE_KEYS, *_OPTIONAL_KEYS, "max_submissions", "force")
#: Keys accepted only by ``init``: the cluster is part of the fixed enrollment, scontrol only discovers it.
INITIALIZE_KEYS = ("cluster", "scontrol")


@dataclass(frozen=True, slots=True)
class _Configuration:
    """The operator's daemon configuration, fully resolved; ``CONFIGURATION_KEYS`` names the fields."""

    launchers: tuple[str, ...]
    authorized_keys: tuple[str, ...]
    bwrap: Path
    python: Path
    sbatch: Path
    squeue: Path
    scancel: Path
    sacct: Path | None
    slurm_conf: Path | None
    max_submissions: int
    force: bool


def _read_bounded(path: Path, limit: int, *, protected: bool = False) -> bytes:
    descriptor = _open_nofollow(path)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError(f"expected a regular file: {path}")
        if protected and information.st_mode & 0o002:
            raise ValueError(f"protected file must not be world-writable: {path}")
        if information.st_size > limit:
            raise ValueError(f"file is too large: {path}")
        data = bytearray()
        while len(data) <= limit:
            chunk = os.read(descriptor, limit + 1 - len(data))
            if not chunk:
                return bytes(data)
            data.extend(chunk)
        raise ValueError(f"file is too large: {path}")
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


def _state_default(workspace: Path) -> Path:
    digest = hashlib.sha256(str(workspace).encode("utf-8")).hexdigest()
    return data_home() / "workspace-daemons" / digest


def _workspace_id(workspace: Path) -> str:
    path = workspace / ".httk-workspace" / "format.json"
    value = _decode_object(_read_bounded(path, _MAX_WORKSPACE_BYTES), description="workspace format")
    if value.get("format") != "httk-workflow-filesystem" or value.get("format_version") != 3:
        raise ValueError("workspace must use httk-workflow-filesystem format version 3")
    workspace_id = value.get("workspace_id")
    if type(workspace_id) is not str:
        raise ValueError("workspace has no valid workspace_id")
    return workspace_id


def _resolve_executable(value: object, name: str) -> Path:
    found = shutil.which(name if value is None else str(value))
    if found is None:
        raise ValueError(f"required executable is unavailable on the operator PATH: {name}")
    return Path(found).absolute()


def _running_python() -> Path:
    # Rebase onto the resolved prefix, the path that compute nodes share with this host.
    executable = Path(sys.executable).absolute()
    try:
        return Path(sys.prefix).resolve() / executable.relative_to(sys.prefix)
    except ValueError:
        return executable


def _effective_slurm_conf() -> Path | None:
    # The operator environment is trusted at setup; the broker later uses only this fixed file.
    environment = os.environ.get("SLURM_CONF")
    if environment and Path(environment).is_absolute() and os.path.isfile(environment):
        return Path(environment)
    return _DEFAULT_SLURM_CONF if _DEFAULT_SLURM_CONF.is_file() else None


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


def _check_resources(settings: Mapping[str, object], *, force: bool) -> None:
    cpus = settings.get("slurm.cpus_per_task")
    if cpus is not None:
        _positive_integer(
            cpus, "slurm.cpus_per_task", _HARD_LIMIT if force else _CPU_LIMIT, "" if force else _FORCE_HINT
        )
    memory = settings.get("slurm.mem")
    if memory is not None:
        _memory_mb(memory, force=force)
    time_limit = settings.get("slurm.time_limit")
    if time_limit is not None:
        _time_minutes(time_limit, force=force)


def _approved_launcher(name: str, forbidden: tuple[Path, ...], *, force: bool) -> ApprovedLauncher:
    """Read one global ``slurm`` launcher bundle and freeze its approved settings; never execute it."""

    if type(name) is not str or _LAUNCHER_NAME.fullmatch(name) is None:
        raise ValueError(f"daemon launcher names must match {_LAUNCHER_NAME.pattern}: {name!r}")
    try:
        bundle = (launchers_home() / name).resolve(strict=True)
    except FileNotFoundError:
        raise ValueError(f"unknown global launcher: {name!r}") from None
    if any(bundle.is_relative_to(path.resolve()) for path in forbidden):
        raise ValueError(f"daemon launcher {name!r} must not lie inside the workspace, state or snapshots")
    metadata = _decode_object(
        _read_bounded(bundle / LAUNCHER_METADATA, _MAX_LAUNCHER_BYTES, protected=True), description="launcher metadata"
    )
    kind = metadata.get("kind")
    if kind != "slurm":
        raise ValueError(
            f"launcher {name!r} is a {kind!r} launcher; the workspace daemon approves only global slurm launchers "
            "('httk launcher add --template slurm --global')"
        )
    settings = _validate_launcher_metadata(bundle, metadata, check_binaries=False).get("settings", {})
    assert isinstance(settings, Mapping)
    if settings.get("manager.confine") != "bwrap":
        raise ValueError(f"launcher {name!r} must set manager.confine=bwrap: the daemon starts only confined managers")
    confine_settings(settings)
    _check_resources(settings, force=force)
    # The broker submits through the installed launch runtime and never runs the bundle's executable.
    return ApprovedLauncher(
        name, tuple(sorted(settings.items())), hashlib.sha256(_canonical_bytes(metadata)).hexdigest()
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


def _cluster(cluster: str | None, slurm_conf: Path | None, scontrol: Path | None) -> str:
    if cluster is not None:
        return cluster
    environment = os.environ.get("SLURM_CLUSTER_NAME")
    if environment:
        if _CLUSTER_NAME.fullmatch(environment) is None:
            raise ValueError("SLURM_CLUSTER_NAME is invalid")
        return environment
    if slurm_conf is not None:
        configured = _cluster_from_text(
            _read_bounded(slurm_conf, _MAX_DISCOVERY_BYTES, protected=True), str(slurm_conf), required=False
        )
        if configured is not None:
            return configured
    discovered = _cluster_from_text(
        _run_scontrol(_resolve_executable(scontrol, "scontrol"), slurm_conf=slurm_conf), "scontrol show config"
    )
    assert discovered is not None
    return discovered


def _setting(key: str, value: str) -> object:
    """Return the stored form of one ``--set`` value; executables resolve and relative paths anchor to the cwd."""

    if key in _LIST_KEYS:
        return value.split(",") if value else []
    if key in _OPTIONAL_KEYS and not value:
        return None
    if not value:
        raise ValueError(f"daemon configuration {key} requires a value")
    if key == "max_submissions":
        return _positive_integer(value, key, _HARD_LIMIT)
    if key == "force":
        if value not in ("true", "false"):
            raise ValueError(f"daemon configuration force must be true or false: {value!r}")
        return value == "true"
    if key == "cluster":
        return value
    path = _anchored(Path(value), key)
    return str(path if key in ("slurm_conf", "scontrol") else _resolve_executable(path, key))


def _apply(document: dict[str, object], changes: Sequence[tuple[str, str]], keys: tuple[str, ...]) -> None:
    """Apply ordered ``set``, ``add``, ``remove`` or ``unset`` configuration changes."""

    for operation, item in changes:
        key, separator, value = item.partition("=")
        if operation == "unset" and separator:
            raise ValueError(f"--unset expects KEY, not KEY=VALUE: {item!r}")
        if operation != "unset" and not separator:
            raise ValueError(f"--{operation} expects KEY=VALUE: {item!r}")
        if key not in keys:
            if key in INITIALIZE_KEYS:
                raise ValueError(f"{key} is fixed by the enrollment; changing it requires a new enrollment")
            raise ValueError(f"unknown daemon configuration key {key!r}; valid keys: {', '.join(keys)}")
        if operation == "unset":
            defaults = {"sacct": None, "slurm_conf": None, "max_submissions": _DEFAULT_MAX_SUBMISSIONS, "force": False}
            if key not in defaults:
                raise ValueError(f"daemon configuration {key} is required; use --set to replace it")
            document[key] = defaults[key]
            continue
        if operation == "set":
            document[key] = _setting(key, value)
            continue
        if key not in _LIST_KEYS:
            raise ValueError(f"--{operation} applies only to the list keys {' and '.join(_LIST_KEYS)}: {key}")
        current = document[key]
        assert isinstance(current, list)
        if operation == "add" and value not in current:
            current.append(value)
        elif operation == "remove":
            if value not in current:
                raise ValueError(f"daemon configuration {key} does not contain {value!r}")
            current.remove(value)


def _discover(document: dict[str, object]) -> None:
    """Fill the configuration values ``init`` was not given from the operator environment."""

    for key in _EXECUTABLE_KEYS:
        if key not in document:
            document[key] = str(_running_python() if key == "python" else _resolve_executable(None, key))
    if "sacct" not in document:
        # Reporting only: no sacct on PATH is not an error, but a given sacct must exist.
        document["sacct"] = None if shutil.which("sacct") is None else str(_resolve_executable(None, "sacct"))
    if "slurm_conf" not in document:
        slurm_conf = _effective_slurm_conf()
        document["slurm_conf"] = None if slurm_conf is None else str(slurm_conf)
    document.setdefault("max_submissions", _DEFAULT_MAX_SUBMISSIONS)
    document.setdefault("force", False)


def _configuration_document(configuration: _Configuration) -> dict[str, object]:
    document: dict[str, object] = {"format": _CONFIGURATION_FORMAT, "format_version": _CONFIGURATION_VERSION}
    for key in CONFIGURATION_KEYS:
        value = getattr(configuration, key)
        document[key] = list(value) if isinstance(value, tuple) else str(value) if isinstance(value, Path) else value
    return document


def _stored_path(document: Mapping[str, object], key: str) -> Path:
    value = document[key]
    if type(value) is not str or "\0" in value or not os.path.isabs(value):
        raise ValueError(f"daemon configuration {key} must be an absolute path")
    return Path(value)


def _stored_list(document: Mapping[str, object], key: str) -> tuple[str, ...]:
    value = document[key]
    if type(value) is not list or any(type(item) is not str for item in value):
        raise ValueError(f"daemon configuration {key} must be a list of strings")
    return tuple(value)


def _configuration(document: Mapping[str, object]) -> _Configuration:
    """Validate one configuration document, as stored or as ``init`` and ``configure`` assembled it."""

    if set(document) != {"format", "format_version", *CONFIGURATION_KEYS}:
        raise ValueError("daemon configuration fields are missing or unknown")
    if document["format"] != _CONFIGURATION_FORMAT or document["format_version"] != _CONFIGURATION_VERSION:
        raise ValueError("unsupported daemon configuration format or version")
    max_submissions, force = document["max_submissions"], document["force"]
    if type(max_submissions) is not int or type(force) is not bool:
        raise ValueError("daemon configuration max_submissions must be an integer and force a boolean")
    return _Configuration(
        launchers=_stored_list(document, "launchers"),
        authorized_keys=_stored_list(document, "authorized_keys"),
        bwrap=_stored_path(document, "bwrap"),
        python=_stored_path(document, "python"),
        sbatch=_stored_path(document, "sbatch"),
        squeue=_stored_path(document, "squeue"),
        scancel=_stored_path(document, "scancel"),
        sacct=None if document["sacct"] is None else _stored_path(document, "sacct"),
        slurm_conf=None if document["slurm_conf"] is None else _stored_path(document, "slurm_conf"),
        max_submissions=max_submissions,
        force=force,
    )


def _compile(
    workspace: Path,
    state: Path,
    snapshots: Path,
    enrollment_id: str,
    cluster: str,
    configuration: _Configuration,
) -> Policy:
    launchers = configuration.launchers
    if not launchers:
        raise ValueError("at least one daemon launcher is required")
    if not configuration.authorized_keys:
        raise ValueError("at least one authorized key is required")
    if len(set(launchers)) != len(launchers):
        raise ValueError("daemon launcher names must be unique")
    # In name order, the order of the canonical snapshot document.
    approved = tuple(
        _approved_launcher(name, (workspace, state, snapshots), force=configuration.force) for name in sorted(launchers)
    )
    policy = Policy(
        workspace=workspace,
        workspace_id=_workspace_id(workspace),
        enrollment_id=enrollment_id,
        state=state,
        snapshots=snapshots,
        launchers=approved,
        authorized_keys=_runtime_authorized_keys(configuration.authorized_keys),
        bwrap=configuration.bwrap,
        python=configuration.python,
        sbatch=configuration.sbatch,
        squeue=configuration.squeue,
        scancel=configuration.scancel,
        sacct=configuration.sacct,
        cluster=cluster,
        slurm_conf=configuration.slurm_conf,
        max_submissions=configuration.max_submissions,
    )
    for launcher in approved:
        # Build each submission once now, so a setting the packaged dispatcher refuses fails at approval.
        submission(policy, launcher, "0" * 32)
    return policy


def _mkdir_exclusive(path: Path, *, exist_ok: bool = False) -> None:
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
                if final and not exist_ok:
                    raise
                created = False
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            information = os.fstat(next_descriptor)
            if created or final and information.st_uid == os.geteuid():
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


def _validate_private_directory(path: Path) -> None:
    descriptor = _open_directory(path)
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
    directory = _open_directory(path.parent)
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
    directory = _open_directory(path.parent)
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


def _publish(policy: Policy) -> Path:
    canonical = _runtime_policy_bytes(policy)
    digest = hashlib.sha256(canonical).hexdigest()
    snapshot = policy.snapshots / f"{digest}.json"
    try:
        _write_exclusive(snapshot, canonical)
    except FileExistsError:
        if _read_bounded(snapshot, _MAX_RUNTIME_POLICY_BYTES, protected=True) != canonical:
            raise ValueError("existing immutable daemon snapshot disagrees with its digest") from None
    active = activation_document(snapshot, policy)
    _write_atomic(policy.state / "active.json", _canonical_bytes(active))
    return snapshot


def _write_daemon_document(policy: Policy) -> None:
    """Install ``exchange/daemon.json``: the unsigned menu a client reads, never a trust anchor.

    The exchange is opened from a workspace-root descriptor without following symlinks, and the document
    is installed by an exclusive temporary and a rename, so whatever the client left at the name is
    replaced rather than followed.
    """

    document = {
        "format": _DAEMON_FORMAT,
        "format_version": _DAEMON_VERSION,
        "workspace_id": policy.workspace_id,
        "enrollment_id": policy.enrollment_id,
        "daemon_public_key": response_public_key(response_seed_path(policy.state)),
        "configurations": {launcher.name: policy.configuration_digest(launcher.name) for launcher in policy.launchers},
        "request_max_age": policy.request_max_age,
    }
    try:
        install_document(policy.workspace, _DAEMON_DOCUMENT, _canonical_bytes(document) + b"\n")
    except (OSError, ExchangeUnavailableError) as exc:
        raise ValueError(
            f"cannot publish {policy.exchange / _DAEMON_DOCUMENT}: {exc}; the workspace's exchange must be "
            "a real directory ('httk workspace exchange enable' creates it)"
        ) from exc


def _snapshots_default(state: Path) -> Path:
    return state.with_name(state.name + ".snapshots")


def _save_configuration(state: Path, configuration: _Configuration) -> None:
    _write_atomic(state / _CONFIGURATION_FILE, _canonical_bytes(_configuration_document(configuration)))


def _load_configuration(policy: Policy) -> _Configuration:
    try:
        data = _read_bounded(policy.state / _CONFIGURATION_FILE, _MAX_CONFIGURATION_BYTES, protected=True)
    except FileNotFoundError:
        raise ValueError(
            f"the daemon state {policy.state} has no {_CONFIGURATION_FILE}; this enrollment predates the saved "
            "configuration and is not migrated. Remove its state and snapshots directories and run "
            "'httk workspace daemon init' again"
        ) from None
    return _configuration(_decode_object(data, description="daemon configuration"))


def _description(policy: Policy, configuration: _Configuration) -> dict[str, object]:
    document = _configuration_document(configuration)
    del document["format"], document["format_version"]
    return {
        "workspace": str(policy.workspace),
        "workspace_id": policy.workspace_id,
        "enrollment_id": policy.enrollment_id,
        "exchange": str(policy.exchange),
        "state": str(policy.state),
        "snapshots": str(policy.snapshots),
        "cluster": policy.cluster,
        "configuration": document,
    }


def initialize(
    workspace: Path,
    *,
    changes: Sequence[tuple[str, str]] = (),
    state: Path | None = None,
    snapshots: Path | None = None,
    report: Callable[[str], None] | None = None,
) -> Path:
    """Compile and publish a fresh enrollment from approved global ``slurm`` launchers.

    The exchange is the workspace's own ``exchange`` directory. The workspace's exchange extension is enabled
    when it is absent (and *report* told so) before ``exchange/daemon.json`` is written: from then on every
    manager of the workspace must confine its attempts. *state* and *snapshots* must lie outside the workspace.

    Configuration values that *changes* does not set are discovered: executables on ``PATH``, the running
    interpreter, the effective Slurm configuration and its cluster, 128 submissions, and ``force=false``.

    :param workspace: Workspace data root.
    :param changes: ``(operation, "KEY=VALUE")`` pairs with operation ``set`` or ``add``, applied in order to
        ``CONFIGURATION_KEYS`` and ``INITIALIZE_KEYS``; at least one launcher and one key must result.
    :param state: Broker state directory outside the workspace, by default under the httk data home.
    :param snapshots: Snapshot directory outside the workspace, by default ``<state>.snapshots``.
    :param report: Called with one line when this call enables the workspace's exchange extension.
    :return: Path of the published runtime snapshot.
    :raises ValueError: If a launcher, key, configuration value or the layout is refused.
    :raises httk.workflow.errors.WorkflowError: If the exchange extension cannot be enabled.
    """

    workspace = workspace.resolve(strict=True)
    state = _state_default(workspace) if state is None else state
    snapshots = _snapshots_default(state) if snapshots is None else snapshots
    document: dict[str, object] = {
        "format": _CONFIGURATION_FORMAT,
        "format_version": _CONFIGURATION_VERSION,
        "launchers": [],
        "authorized_keys": [],
    }
    _apply(document, changes, (*CONFIGURATION_KEYS, *INITIALIZE_KEYS))
    cluster, scontrol = document.pop("cluster", None), document.pop("scontrol", None)
    assert (cluster is None or isinstance(cluster, str)) and (scontrol is None or isinstance(scontrol, str))
    _discover(document)
    configuration = _configuration(document)
    cluster = _cluster(cluster, configuration.slurm_conf, None if scontrol is None else Path(scontrol))
    policy = _compile(workspace, state, snapshots, secrets.token_hex(16), cluster, configuration)
    home = data_home().resolve()
    if _overlap(policy.workspace, home):
        raise ValueError(f"workspace {policy.workspace} must be disjoint from the httk data home {home}")
    check_private_paths(policy)
    _runtime_policy_bytes(policy)
    leftover = [str(path) for path in (state, snapshots) if os.path.lexists(path)]
    if leftover:
        raise ValueError(
            f"daemon state already exists: {', '.join(leftover)}; remove an earlier enrollment's state "
            "or give a different --state"
        )
    for directory in (state, snapshots, policy.jobs):
        _mkdir_exclusive(directory)
    _save_configuration(state, configuration)
    initialize_response_seed(state)
    with Ledger(
        state,
        policy.workspace_id,
        policy.enrollment_id,
        initialize=True,
        max_records=policy.max_records,
        max_submissions=policy.max_submissions,
    ):
        snapshot = _publish(policy)
        # From here on every manager of the workspace must be confined. The exchange must exist before the
        # daemon document can be installed in it.
        if enable_exchange(Workspace(workspace)) and report is not None:
            report(f"enabled the exchange extension of {workspace}: {workspace / EXCHANGE_DIRECTORY}")
        _write_daemon_document(policy)
        return snapshot


def _active(workspace: Path, state: Path | None, snapshots: Path | None) -> tuple[Path, Policy, str]:
    workspace = workspace.resolve(strict=True)
    state = _state_default(workspace) if state is None else state
    _validate_private_directory(state)
    snapshot, digest = read_active_snapshot(state)
    policy = load_policy(snapshot)
    verify_active_snapshot(state, snapshot, policy)
    if policy.workspace != workspace:
        raise ValueError("active daemon enrollment belongs to a different workspace")
    if snapshots is not None and policy.snapshots != snapshots:
        raise ValueError(f"active daemon enrollment keeps its snapshots in {policy.snapshots}, not {snapshots}")
    return snapshot, policy, digest


def _recompile(active: Policy, configuration: _Configuration) -> Policy:
    """Compile *configuration* against the current launcher bundles and the fixed enrollment connection."""

    policy = _compile(
        active.workspace,
        active.state,
        active.snapshots,
        active.enrollment_id,
        active.cluster,
        configuration,
    )
    if policy.workspace_id != active.workspace_id:
        raise ValueError("the workspace identity changed since the enrollment; a new enrollment is required")
    _runtime_policy_bytes(policy)
    return policy


def describe(workspace: Path, *, state: Path | None = None, snapshots: Path | None = None) -> dict[str, object]:
    """Describe the fixed enrollment connection and the saved daemon configuration.

    :param workspace: Workspace data root.
    :param state: Broker state directory when not the default.
    :param snapshots: Expected snapshot directory, checked when given.
    :return: JSON-compatible description with the configuration under ``configuration``.
    """

    _snapshot, active, _digest = _active(workspace, state, snapshots)
    return _description(active, _load_configuration(active))


def configure(
    workspace: Path,
    changes: Sequence[tuple[str, str]],
    *,
    state: Path | None = None,
    snapshots: Path | None = None,
) -> dict[str, object]:
    """Change the saved daemon configuration after compiling the result as activation would.

    Nothing is published: the daemon activates the configuration when it next starts.

    :param workspace: Workspace data root.
    :param changes: ``(operation, "KEY=VALUE")`` pairs with operation ``set``, ``add`` or ``remove``,
        or ``("unset", "KEY")`` to clear an optional value or restore a default, applied in order.
    :param state: Broker state directory when not the default.
    :param snapshots: Expected snapshot directory, checked when given.
    :return: The resulting description, as ``describe`` returns it.
    :raises ValueError: If a change or the resulting configuration is refused; nothing is then saved.
    """

    _snapshot, active, _digest = _active(workspace, state, snapshots)
    document = _configuration_document(_load_configuration(active))
    _apply(document, changes, CONFIGURATION_KEYS)
    configuration = _configuration(document)
    _recompile(active, configuration)
    _save_configuration(active.state, configuration)
    return _description(active, configuration)


def activate(
    workspace: Path, *, state: Path | None = None, snapshots: Path | None = None
) -> tuple[Path, tuple[str, ...] | None]:
    """Compile the saved configuration from the current launcher bundles and activate it when it changed.

    The job-output directory is recreated when missing and ``exchange/daemon.json`` is reinstalled every
    time, since the client may have replaced it. A changed compilation is published as a new active
    snapshot even while daemon instances run: the ledger holds no lock, and every running instance re-reads
    the active snapshot before each admission and decision and stops once it changed (never between
    deciding a submission and submitting it), so the change takes effect when the daemon is started again.

    :param workspace: Workspace data root.
    :param state: Broker state directory when not the default.
    :param snapshots: Expected snapshot directory, checked when given.
    :return: The active runtime snapshot, and the names of the launchers whose approval changed, or
        ``None`` when the active snapshot already matched.
    :raises ValueError: If the configuration is refused, or the active snapshot changed during activation.
    :raises httk.workflow._daemon_state.LedgerError: If the enrollment's ledger is missing, corrupt, or the
        SQLite ledger of an earlier version.
    """

    old_snapshot, old, old_digest = _active(workspace, state, snapshots)
    new = _recompile(old, _load_configuration(old))
    _validate_private_directory(old.snapshots)
    _mkdir_exclusive(new.jobs, exist_ok=True)
    check_private_paths(new)
    # Opening the ledger refuses an earlier version's SQLite ledger and an identity mismatch; it takes no lock.
    with Ledger(
        old.state,
        old.workspace_id,
        old.enrollment_id,
        max_records=old.max_records,
        max_submissions=old.max_submissions,
    ):
        pass
    if _runtime_policy_bytes(new) == _runtime_policy_bytes(old):
        _write_daemon_document(new)
        return old_snapshot, None
    if read_active_snapshot(old.state) != (old_snapshot, old_digest):
        raise ValueError("the active daemon snapshot changed while activation was compiling; activate again")
    verify_active_snapshot(old.state, old_snapshot, old)
    snapshot = _publish(new)
    _write_daemon_document(new)
    return snapshot, tuple(sorted({launcher.name for launcher in set(old.launchers) ^ set(new.launchers)}))


__all__ = ["CONFIGURATION_KEYS", "INITIALIZE_KEYS", "activate", "configure", "describe", "initialize"]
