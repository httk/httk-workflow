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
from collections.abc import Mapping, Sequence
from pathlib import Path

from httk.core.userdirs import data_home

from ._daemon_activation import activation_document, read_active_snapshot, verify_active_snapshot
from ._daemon_keys import initialize_response_seed, response_public_key, response_seed_path
from ._daemon_launcher import DAEMON_KIND, DAEMON_LAUNCHER_NAME, DaemonSettings, parse_daemon_settings
from ._daemon_policy import MAX_POLICY_BYTES as _MAX_RUNTIME_POLICY_BYTES
from ._daemon_policy import (
    MPIProfile,
    MPISettings,
    Policy,
    Profile,
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
from ._daemon_policy import (
    _authorized_keys as _runtime_authorized_keys,
)
from ._daemon_state import Ledger
from .configuration import launchers_home
from .launchers import LAUNCHER_METADATA, _validate_launcher_metadata

_ENDPOINT_FORMAT = "httk-workspace-daemon-endpoint"
_ENDPOINT_VERSION = 2
_MAX_WORKSPACE_BYTES = 1024 * 1024
_MAX_LAUNCHER_BYTES = 64 * 1024
_MAX_DISCOVERY_BYTES = 64 * 1024
_CLUSTER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_DEFAULT_READONLY = ("/usr", "/bin", "/lib", "/lib64")
_EXCHANGE_DIRECTORIES = ("requests", "responses", "inbox", "outbox", "outbox/rejected")
_STAGING_DIRECTORIES = ("inbox", "outbox", "outbox/rejected", "records")
_MPI_ENVIRONMENT_PREFIX = "daemon.mpi.environment."
_DEFAULT_SLURM_CONF = Path("/etc/slurm/slurm.conf")


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
    # Rebase onto the resolved prefix: readonly defaults are resolved, and the sandbox binds them only there.
    executable = Path(sys.executable).absolute()
    try:
        return Path(sys.prefix).resolve() / executable.relative_to(sys.prefix)
    except ValueError:
        return executable


def _effective_slurm_conf(declared: Path | None) -> Path | None:
    # The operator environment is trusted at setup; the broker sandbox later sees only this fixed file.
    if declared is not None:
        return declared
    environment = os.environ.get("SLURM_CONF")
    if environment and Path(environment).is_absolute() and os.path.isfile(environment):
        return Path(environment)
    return _DEFAULT_SLURM_CONF if _DEFAULT_SLURM_CONF.is_file() else None


def _site[T](site: Mapping[str, object], key: str, kind: type[T]) -> T | None:
    value = site.get(key)
    if value is None or isinstance(value, kind):
        return value
    raise ValueError(f"invalid {key}")


def _daemon_launcher(name: str, forbidden: tuple[Path, ...], *, force: bool) -> DaemonSettings:
    if type(name) is not str or DAEMON_LAUNCHER_NAME.fullmatch(name) is None:
        raise ValueError(f"daemon launcher names must match {DAEMON_LAUNCHER_NAME.pattern}: {name!r}")
    try:
        bundle = (launchers_home() / name).resolve(strict=True)
    except FileNotFoundError:
        raise ValueError(f"unknown global daemon launcher: {name!r}") from None
    if any(bundle.is_relative_to(path.resolve()) for path in forbidden):
        raise ValueError(f"daemon launcher {name!r} must not lie inside the daemon parent, state or snapshots")
    metadata = _decode_object(
        _read_bounded(bundle / LAUNCHER_METADATA, _MAX_LAUNCHER_BYTES, protected=True), description="launcher metadata"
    )
    kind = metadata.get("kind")
    if kind != DAEMON_KIND:
        raise ValueError(
            f"launcher {name!r} is a {kind!r} launcher; create a daemon launcher with "
            "'httk workflow launcher add --template daemon --global'"
        )
    settings = _validate_launcher_metadata(bundle, metadata, check_binaries=False).get("settings", {})
    assert isinstance(settings, Mapping)
    return parse_daemon_settings(settings, force=force)


def _combined_site(launchers: Mapping[str, DaemonSettings]) -> dict[str, object]:
    site: dict[str, object] = {}
    owners: dict[str, str] = {}
    for name, settings in launchers.items():
        environment = {_MPI_ENVIRONMENT_PREFIX + key: value for key, value in settings.mpi_environment.items()}
        for key, value in (*settings.site.items(), *environment.items()):
            if key in site and site[key] != value:
                raise ValueError(f"daemon launchers {owners[key]!r} and {name!r} set {key} to different values")
            site[key] = value
            owners.setdefault(key, name)
    return site


def _nested_dropped(paths: set[Path]) -> tuple[Path, ...]:
    return tuple(
        sorted(path for path in paths if not any(path != other and path.is_relative_to(other) for other in paths))
    )


def _mpi_settings(site: Mapping[str, object]) -> MPISettings:
    control_root = _site(site, "daemon.mpi.control_root", Path)
    if control_root is None:
        raise ValueError("MPI daemon launchers require daemon.mpi.control_root")
    environment = tuple(
        (key.removeprefix(_MPI_ENVIRONMENT_PREFIX), value)
        for key, value in sorted(site.items())
        if key.startswith(_MPI_ENVIRONMENT_PREFIX) and isinstance(value, str)
    )
    grace = _site(site, "daemon.mpi.termination_grace", float)
    steps = _site(site, "daemon.mpi.max_steps", int)
    return MPISettings(
        srun=_resolve_executable(site.get("daemon.mpi.srun"), "srun"),
        control_root=control_root,
        pmix_roots=_site(site, "daemon.mpi.pmix_roots", tuple) or (),
        shm_root=_site(site, "daemon.mpi.shm_root", Path) or Path("/dev/shm"),
        devices=_site(site, "daemon.mpi.devices", tuple) or (),
        environment=environment,
        max_steps=128 if steps is None else steps,
        termination_grace=10.0 if grace is None else grace,
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


def _compile(
    workspace: Path,
    exchange: Path,
    state: Path,
    snapshots: Path,
    enrollment_id: str,
    launchers: Sequence[str],
    authorized_keys: Sequence[str],
    *,
    force: bool,
) -> Policy:
    if not launchers:
        raise ValueError("at least one daemon launcher is required")
    if not authorized_keys:
        raise ValueError("at least one authorized key is required")
    parsed = {name: _daemon_launcher(name, (exchange.parent, state, snapshots), force=force) for name in launchers}
    if len(parsed) != len(launchers):
        raise ValueError("daemon launcher names must be unique")
    site = _combined_site(parsed)
    readonly = _site(site, "daemon.readonly_paths", tuple)
    if readonly is None:
        defaults = {Path(path).resolve() for path in _DEFAULT_READONLY if os.path.exists(path)}
        readonly = _nested_dropped(defaults | {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve()})
    slurm_conf = _effective_slurm_conf(_site(site, "daemon.slurm_conf", Path))
    broker = _site(site, "daemon.broker_paths", tuple)
    if broker is None:
        munge = Path("/run/munge")
        broker = (*(() if slurm_conf is None else (slurm_conf.parent,)), *((munge,) if munge.exists() else ()))
    python = site.get("daemon.python")
    max_submissions = _site(site, "daemon.max_submissions", int)
    return Policy(
        workspace=workspace,
        workspace_id=_workspace_id(workspace),
        enrollment_id=enrollment_id,
        exchange=exchange,
        state=state,
        snapshots=snapshots,
        bwrap=_resolve_executable(site.get("daemon.bwrap"), "bwrap"),
        python=_running_python() if python is None else _resolve_executable(python, "python"),
        sbatch=_resolve_executable(site.get("daemon.sbatch"), "sbatch"),
        squeue=_resolve_executable(site.get("daemon.squeue"), "squeue"),
        scancel=_resolve_executable(site.get("daemon.scancel"), "scancel"),
        cluster=_cluster(_site(site, "daemon.cluster", str), slurm_conf, _site(site, "daemon.scontrol", Path)),
        readonly_paths=readonly,
        broker_paths=broker,
        profiles=tuple(
            Profile(
                name,
                settings.cpus,
                settings.memory_mb,
                settings.time_minutes,
                settings.partition,
                settings.account,
                MPIProfile(settings.nodes, settings.ranks, settings.ntasks_per_node) if settings.mpi else None,
                settings.workers,
                settings.prelude,
                settings.manager_command,
                settings.gres,
                settings.reservation,
            )
            for name, settings in parsed.items()
        ),
        authorized_keys=_runtime_authorized_keys(tuple(authorized_keys)),
        slurm_conf=slurm_conf,
        max_submissions=128 if max_submissions is None else max_submissions,
        mpi=_mpi_settings(site) if any(settings.mpi for settings in parsed.values()) else None,
    )


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
        "configurations": {profile.name: policy.configuration_digest(profile.name) for profile in policy.profiles},
        "request_max_age": policy.request_max_age,
    }
    _write_atomic(policy.exchange / "endpoint.json", _canonical_bytes(endpoint))


def _create_staging(workspace: Path) -> None:
    for name in _STAGING_DIRECTORIES:
        _mkdir_exclusive(workspace / ".httk-workspace" / "exchange" / name, exist_ok=True)


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
) -> Path:
    """Compile and publish a fresh enrollment from approved global daemon launchers.

    :param workspace: Workspace data root.
    :param exchange: Client exchange directory; a missing or empty sibling of the workspace.
    :param launchers: Names of approved global ``daemon`` launchers, which become the profiles.
    :param authorized_keys: Canonical Ed25519 keys authorized to issue requests.
    :param state: Broker state directory, by default under the httk data home.
    :param snapshots: Snapshot directory, by default ``<state>.snapshots``.
    :param force: Approve CPU, memory and time requests above the built-in sanity limits.
    :return: Path of the published runtime snapshot.
    :raises ValueError: If a launcher, key or the layout is refused.
    """

    workspace = workspace.resolve(strict=True)
    exchange = exchange.parent.resolve(strict=True) / exchange.name
    state = _state_default(workspace) if state is None else state
    snapshots = _snapshots_default(state) if snapshots is None else snapshots
    policy = _compile(
        workspace, exchange, state, snapshots, secrets.token_hex(16), launchers, authorized_keys, force=force
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
    for directory in (state, snapshots):
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
        "sbatch",
        "squeue",
        "scancel",
        "cluster",
        "slurm_conf",
        "broker_paths",
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
) -> Path:
    """Recompile the enrollment and atomically activate the replacement snapshot.

    :param workspace: Workspace data root.
    :param launchers: Replacement launcher names, or ``None`` to keep the active profile names.
    :param authorized_keys: Replacement authorized keys, or ``None`` to keep the active keys.
    :param state: Broker state directory when not the default.
    :param snapshots: Snapshot directory when not ``<state>.snapshots``.
    :param force: Approve CPU, memory and time requests above the built-in sanity limits.
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
        [profile.name for profile in old.profiles] if launchers is None else launchers,
        old.authorized_keys if authorized_keys is None else authorized_keys,
        force=force,
    )
    _runtime_policy_bytes(new)
    _fixed_connection(old, new)
    _validate_private_directory(snapshots)
    _create_staging(workspace)
    check_layout(new)
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


__all__ = ["active_policy_path", "initialize", "reload"]
