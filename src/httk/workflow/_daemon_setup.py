"""Trusted local enrollment and publication for the workspace daemon."""

import hashlib
import importlib.resources
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

from httk.core.userdirs import data_home

from ._confine import confine_settings
from ._daemon_activation import activation_document, read_active_snapshot, verify_active_snapshot
from ._daemon_keys import initialize_response_seed, response_public_key, response_seed_path
from ._daemon_policy import (
    _LAUNCHER_NAME,
    ApprovedLauncher,
    Policy,
    _check_parent_names,
    _object_without_duplicates,
    _open_directory,
    _open_nofollow,
    _overlap,
    _parse_float,
    _reject_constant,
    check_layout,
    load_policy,
    policy_document,
)
from ._daemon_policy import MAX_POLICY_BYTES as _MAX_RUNTIME_POLICY_BYTES
from ._daemon_policy import (
    _authorized_keys as _runtime_authorized_keys,
)
from ._daemon_slurm import submission
from ._daemon_state import Ledger
from ._exchange_staging import ENROLLMENT_MARKER
from .configuration import launchers_home
from .launchers import LAUNCHER_EXECUTABLE, LAUNCHER_METADATA, _validate_launcher_metadata

_ENDPOINT_FORMAT = "httk-workspace-daemon-endpoint"
_ENDPOINT_VERSION = 2
_MAX_WORKSPACE_BYTES = 1024 * 1024
_MAX_LAUNCHER_BYTES = 64 * 1024
_MAX_DISCOVERY_BYTES = 64 * 1024
_CLUSTER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_REPORT_DIRECTORIES = ("outbox/managers", "outbox/withdrawn")
_EXCHANGE_DIRECTORIES = ("requests", "responses", "inbox", "outbox", "outbox/rejected", *_REPORT_DIRECTORIES)
_STAGING_DIRECTORIES = ("inbox", "outbox", "outbox/rejected", "records")
_ENROLLMENT_FORMAT = "httk-workspace-daemon-enrollment"
_ENROLLMENT_VERSION = 1
_DEFAULT_SLURM_CONF = Path("/etc/slurm/slurm.conf")
_DEFAULT_MAX_SUBMISSIONS = 128
_CANONICAL_INTEGER = re.compile(r"[1-9][0-9]*\Z")
_MEMORY = re.compile(r"([1-9][0-9]*)([KMGT]?)\Z")
_CPU_LIMIT = 1024
_MEMORY_LIMIT = 1_048_576
_TIME_LIMIT = 10_080
_HARD_LIMIT = 2**31 - 1
_FORCE_HINT = "; pass --force to approve it"


@dataclass(frozen=True, slots=True)
class BrokerOptions:
    """Broker configuration given to daemon setup; each unset value is discovered or kept.

    ``--initialize`` discovers unset values (executables on ``PATH``, the running interpreter, the effective
    Slurm configuration and its cluster, 128 submissions); ``--reload`` keeps the enrollment's stored values.

    :param bwrap: Bubblewrap executable for the broker sandbox.
    :param python: Python executable that runs the broker and the submitted managers.
    :param sbatch: Slurm submission executable.
    :param squeue: Slurm query executable.
    :param scancel: Slurm cancellation executable.
    :param sacct: Optional Slurm accounting executable.
    :param scontrol: Slurm control executable, used only to discover the cluster name.
    :param cluster: Fixed Slurm cluster name.
    :param slurm_conf: Fixed Slurm configuration file.
    :param max_submissions: Maximum accepted manager submissions of the enrollment.
    """

    bwrap: Path | None = None
    python: Path | None = None
    sbatch: Path | None = None
    squeue: Path | None = None
    scancel: Path | None = None
    sacct: Path | None = None
    scontrol: Path | None = None
    cluster: str | None = None
    slurm_conf: Path | None = None
    max_submissions: int | None = None


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
    if value.get("format") != "httk-workflow-filesystem" or value.get("format_version") != 2:
        raise ValueError("workspace must use httk-workflow-filesystem format version 2")
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


def _effective_slurm_conf(declared: Path | None) -> Path | None:
    # The operator environment is trusted at setup; the broker later uses only this fixed file.
    if declared is not None:
        return declared
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


def _packaged_slurm_launcher() -> bytes:
    source = importlib.resources.files("httk.workflow").joinpath("launch_templates", "slurm", LAUNCHER_EXECUTABLE)
    return source.read_bytes()


def _approved_launcher(name: str, forbidden: tuple[Path, ...], *, force: bool) -> ApprovedLauncher:
    """Read one global ``slurm`` launcher bundle and freeze its approved content; never execute it."""

    if type(name) is not str or _LAUNCHER_NAME.fullmatch(name) is None:
        raise ValueError(f"daemon launcher names must match {_LAUNCHER_NAME.pattern}: {name!r}")
    try:
        bundle = (launchers_home() / name).resolve(strict=True)
    except FileNotFoundError:
        raise ValueError(f"unknown global launcher: {name!r}") from None
    if any(bundle.is_relative_to(path.resolve()) for path in forbidden):
        raise ValueError(f"daemon launcher {name!r} must not lie inside the daemon parent, state or snapshots")
    metadata = _decode_object(
        _read_bounded(bundle / LAUNCHER_METADATA, _MAX_LAUNCHER_BYTES, protected=True), description="launcher metadata"
    )
    kind = metadata.get("kind")
    if kind != "slurm":
        raise ValueError(
            f"launcher {name!r} is a {kind!r} launcher; the workspace daemon approves only global slurm launchers "
            "('httk workflow launcher add --template slurm --global')"
        )
    settings = _validate_launcher_metadata(bundle, metadata, check_binaries=False).get("settings", {})
    assert isinstance(settings, Mapping)
    executable = _read_bounded(bundle / LAUNCHER_EXECUTABLE, _MAX_LAUNCHER_BYTES, protected=True)
    if executable != _packaged_slurm_launcher():
        raise ValueError(f"launcher {name!r} must keep the packaged slurm launcher executable unchanged")
    if settings.get("manager.confine") != "bwrap":
        raise ValueError(f"launcher {name!r} must set manager.confine=bwrap: the daemon starts only confined managers")
    confine_settings(settings)
    _check_resources(settings, force=force)
    content = {"launcher_json": metadata, "launcher_sha256": hashlib.sha256(executable).hexdigest()}
    return ApprovedLauncher(
        name, tuple(sorted(settings.items())), hashlib.sha256(_canonical_bytes(content)).hexdigest()
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


class _BrokerFields(TypedDict):
    bwrap: Path
    python: Path
    sbatch: Path
    squeue: Path
    scancel: Path
    sacct: Path | None
    cluster: str
    slurm_conf: Path | None
    max_submissions: int


def _broker_fields(options: BrokerOptions, stored: Policy | None) -> _BrokerFields:
    """Resolve the enrollment-held broker configuration: given values win, then stored ones, then discovery."""

    if stored is not None:
        slurm_conf = stored.slurm_conf if options.slurm_conf is None else options.slurm_conf
        # Only a new cluster or Slurm configuration rediscovers the cluster, which reload refuses to change.
        rediscover = options.cluster is not None or options.slurm_conf is not None
        cluster = _cluster(options.cluster, slurm_conf, options.scontrol) if rediscover else stored.cluster
        return {
            "bwrap": stored.bwrap if options.bwrap is None else _resolve_executable(options.bwrap, "bwrap"),
            "python": stored.python if options.python is None else _resolve_executable(options.python, "python"),
            "sbatch": stored.sbatch if options.sbatch is None else _resolve_executable(options.sbatch, "sbatch"),
            "squeue": stored.squeue if options.squeue is None else _resolve_executable(options.squeue, "squeue"),
            "scancel": stored.scancel if options.scancel is None else _resolve_executable(options.scancel, "scancel"),
            "sacct": stored.sacct if options.sacct is None else _resolve_executable(options.sacct, "sacct"),
            "cluster": cluster,
            "slurm_conf": slurm_conf,
            "max_submissions": stored.max_submissions if options.max_submissions is None else options.max_submissions,
        }
    slurm_conf = _effective_slurm_conf(options.slurm_conf)
    return {
        "bwrap": _resolve_executable(options.bwrap, "bwrap"),
        "python": _running_python() if options.python is None else _resolve_executable(options.python, "python"),
        "sbatch": _resolve_executable(options.sbatch, "sbatch"),
        "squeue": _resolve_executable(options.squeue, "squeue"),
        "scancel": _resolve_executable(options.scancel, "scancel"),
        # Reporting only: no sacct on PATH is not an error, but a given --sacct must exist.
        "sacct": None
        if options.sacct is None and shutil.which("sacct") is None
        else _resolve_executable(options.sacct, "sacct"),
        "cluster": _cluster(options.cluster, slurm_conf, options.scontrol),
        "slurm_conf": slurm_conf,
        "max_submissions": _DEFAULT_MAX_SUBMISSIONS if options.max_submissions is None else options.max_submissions,
    }


def _compile(
    workspace: Path,
    exchange: Path,
    state: Path,
    snapshots: Path,
    enrollment_id: str,
    launchers: Sequence[str],
    authorized_keys: Sequence[str],
    *,
    options: BrokerOptions,
    stored: Policy | None = None,
    force: bool,
) -> Policy:
    if not launchers:
        raise ValueError("at least one daemon launcher is required")
    if not authorized_keys:
        raise ValueError("at least one authorized key is required")
    if len(set(launchers)) != len(launchers):
        raise ValueError("daemon launcher names must be unique")
    # In name order, the order of the canonical snapshot document.
    approved = tuple(
        _approved_launcher(name, (exchange.parent, state, snapshots), force=force) for name in sorted(launchers)
    )
    policy = Policy(
        workspace=workspace,
        workspace_id=_workspace_id(workspace),
        enrollment_id=enrollment_id,
        exchange=exchange,
        state=state,
        snapshots=snapshots,
        launchers=approved,
        authorized_keys=_runtime_authorized_keys(tuple(authorized_keys)),
        **_broker_fields(options, stored),
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


def _write_endpoint(policy: Policy) -> None:
    endpoint = {
        "format": _ENDPOINT_FORMAT,
        "format_version": _ENDPOINT_VERSION,
        "workspace_id": policy.workspace_id,
        "enrollment_id": policy.enrollment_id,
        "daemon_public_key": response_public_key(response_seed_path(policy.state)),
        "configurations": {launcher.name: policy.configuration_digest(launcher.name) for launcher in policy.launchers},
        "request_max_age": policy.request_max_age,
    }
    _write_atomic(policy.exchange / "endpoint.json", _canonical_bytes(endpoint))


def _create_staging(workspace: Path) -> None:
    for name in _STAGING_DIRECTORIES:
        _mkdir_exclusive(workspace / ".httk-workspace" / "exchange" / name, exist_ok=True)


def _write_enrollment(workspace: Path, policy: Policy, *, replace: bool = True) -> None:
    """Mark the workspace enrolled; with *replace* false, only when the marker is missing."""

    marker = workspace / ".httk-workspace" / "exchange" / ENROLLMENT_MARKER
    if not replace and os.path.lexists(marker):
        return
    document = {
        "format": _ENROLLMENT_FORMAT,
        "format_version": _ENROLLMENT_VERSION,
        "enrollment_id": policy.enrollment_id,
        "workspace_id": policy.workspace_id,
    }
    _write_atomic(marker, _canonical_bytes(document))


def _snapshots_default(state: Path) -> Path:
    return state.with_name(state.name + ".snapshots")


def initialize(
    workspace: Path,
    *,
    exchange: Path,
    launchers: Sequence[str],
    authorized_keys: Sequence[str],
    state: Path | None = None,
    snapshots: Path | None = None,
    force: bool = False,
    broker: BrokerOptions | None = None,
) -> Path:
    """Compile and publish a fresh enrollment from approved global ``slurm`` launchers.

    :param workspace: Workspace data root.
    :param exchange: Client exchange directory; a missing or empty sibling of the workspace.
    :param launchers: Names of the approved global ``slurm`` launchers, which become the configurations.
    :param authorized_keys: Canonical Ed25519 keys authorized to issue requests.
    :param state: Broker state directory, by default under the httk data home.
    :param snapshots: Snapshot directory, by default ``<state>.snapshots``.
    :param force: Approve CPU, memory and time requests above the built-in sanity limits.
    :param broker: Broker configuration; unset values are discovered.
    :return: Path of the published runtime snapshot.
    :raises ValueError: If a launcher, key or the layout is refused.
    """

    workspace = workspace.resolve(strict=True)
    exchange = exchange.parent.resolve(strict=True) / exchange.name
    state = _state_default(workspace) if state is None else state
    snapshots = _snapshots_default(state) if snapshots is None else snapshots
    policy = _compile(
        workspace,
        exchange,
        state,
        snapshots,
        secrets.token_hex(16),
        launchers,
        authorized_keys,
        options=BrokerOptions() if broker is None else broker,
        force=force,
    )
    home = data_home().resolve()
    if _overlap(policy.root, home):
        raise ValueError(f"daemon parent {policy.root} must be disjoint from the httk data home {home}")
    _runtime_policy_bytes(policy)
    _check_parent_names(policy, os.listdir(policy.root))
    if os.path.lexists(exchange) and (exchange.is_symlink() or not exchange.is_dir() or os.listdir(exchange)):
        raise ValueError(f"exchange must not exist or must be an empty directory: {exchange}")
    leftover = [str(path) for path in (state, snapshots) if os.path.lexists(path)]
    if leftover:
        raise ValueError(
            f"daemon state already exists: {', '.join(leftover)}; remove an earlier enrollment's state "
            "or give a different --state"
        )
    # The probe runs while the exchange is still empty, so a refused layout can be fixed and retried.
    _mkdir_exclusive(exchange, exist_ok=True)
    _create_staging(workspace)
    check_layout(policy)
    for name in _EXCHANGE_DIRECTORIES:
        _mkdir_exclusive(exchange / name)
    for directory in (state, snapshots, policy.jobs):
        _mkdir_exclusive(directory)
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
        _write_endpoint(policy)
        # Last: a failed --initialize leaves no enrolled workspace without a daemon. From here on every
        # manager of the workspace must be confined.
        _write_enrollment(workspace, policy)
        return snapshot


def _active(workspace: Path, state: Path, snapshots: Path | None = None) -> tuple[Path, Policy, str]:
    snapshot, digest = read_active_snapshot(state)
    policy = load_policy(snapshot)
    verify_active_snapshot(state, snapshot, policy)
    if policy.workspace != workspace:
        raise ValueError("active daemon enrollment belongs to a different workspace")
    if snapshots is not None and policy.snapshots != snapshots:
        raise ValueError(f"active daemon enrollment keeps its snapshots in {policy.snapshots}, not {snapshots}")
    return snapshot, policy, digest


def active_policy_path(workspace: Path, *, state: Path | None = None, snapshots: Path | None = None) -> Path:
    """Return the saved active runtime snapshot for ordinary daemon startup.

    :param workspace: Workspace data root.
    :param state: Broker state directory when not the default.
    :param snapshots: Expected snapshot directory, checked when given.
    :return: Path of the active runtime snapshot.
    """

    workspace = workspace.resolve(strict=True)
    return _active(workspace, _state_default(workspace) if state is None else state, snapshots)[0]


def _fixed_connection(old: Policy, new: Policy) -> None:
    fields = (
        "workspace",
        "workspace_id",
        "enrollment_id",
        "exchange",
        "state",
        "snapshots",
        # Recorded jobs are bound to the cluster; Slurm client paths and slurm.conf may change.
        "cluster",
    )
    changed = [name for name in fields if getattr(old, name) != getattr(new, name)]
    if changed:
        raise ValueError(
            f"reload cannot change the fixed enrollment connection: {', '.join(changed)}; a new enrollment is required"
        )


def reload(
    workspace: Path,
    *,
    launchers: Sequence[str] | None = None,
    authorized_keys: Sequence[str] | None = None,
    state: Path | None = None,
    snapshots: Path | None = None,
    force: bool = False,
    broker: BrokerOptions | None = None,
) -> Path:
    """Recompile the enrollment and atomically activate the replacement snapshot.

    Every approved launcher is read and frozen again, so edits to a bundle take effect only here.

    :param workspace: Workspace data root.
    :param launchers: Replacement launcher names, or ``None`` to keep the active launcher names.
    :param authorized_keys: Replacement authorized keys, or ``None`` to keep the active keys.
    :param state: Broker state directory when not the default.
    :param snapshots: Snapshot directory when not ``<state>.snapshots``.
    :param force: Approve CPU, memory and time requests above the built-in sanity limits.
    :param broker: Replacement broker configuration; unset values keep the active ones.
    :return: Path of the newly activated runtime snapshot.
    :raises ValueError: If a launcher or key is refused or the fixed connection would change.
    """

    workspace = workspace.resolve(strict=True)
    state = _state_default(workspace) if state is None else state
    snapshots = _snapshots_default(state) if snapshots is None else snapshots
    _validate_private_directory(state)
    old_snapshot, old, old_digest = _active(workspace, state)
    new = _compile(
        workspace,
        old.exchange,
        state,
        snapshots,
        old.enrollment_id,
        [launcher.name for launcher in old.launchers] if launchers is None else launchers,
        old.authorized_keys if authorized_keys is None else authorized_keys,
        options=BrokerOptions() if broker is None else broker,
        stored=old,
        force=force,
    )
    _runtime_policy_bytes(new)
    _fixed_connection(old, new)
    _validate_private_directory(snapshots)
    _mkdir_exclusive(new.jobs, exist_ok=True)
    for name in _REPORT_DIRECTORIES:
        _mkdir_exclusive(new.exchange / name, exist_ok=True)
    _create_staging(workspace)
    check_layout(new)
    _write_enrollment(workspace, new, replace=False)
    with Ledger(
        old.state,
        old.workspace_id,
        old.enrollment_id,
        max_records=old.max_records,
        max_submissions=old.max_submissions,
    ):
        current_snapshot, current_digest = read_active_snapshot(state)
        if current_snapshot != old_snapshot or current_digest != old_digest:
            raise ValueError("active daemon approval changed while reload was waiting for its lock")
        verify_active_snapshot(state, old_snapshot, old)
        snapshot = _publish(new)
        _write_endpoint(new)
        return snapshot


__all__ = ["BrokerOptions", "active_policy_path", "initialize", "reload"]
