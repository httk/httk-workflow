"""Isolated Bubblewrap bootstrap for the confined workspace daemon."""

import argparse
import fcntl
import os
import re
import runpy
import secrets
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

_HANDLE = re.compile(r"[0-9a-f]{32}\Z")
_PROFILE = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_HOSTNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}\Z")
_MPI_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_MPI_EXACT_ENVIRONMENT = frozenset(
    {
        "SLURM_PROCID",
        "SLURM_LOCALID",
        "SLURM_NODEID",
        "SLURM_NTASKS",
        "SLURM_JOB_ID",
        "SLURMD_NODENAME",
    }
)
_MPI_ENVIRONMENT_PREFIXES = ("PMI_", "PMIX_", "OMPI_", "OPAL_")
_MPI_ENVIRONMENT_LIMIT = 128
_MPI_ENVIRONMENT_VALUE_BYTES = 4096
_MPI_ENVIRONMENT_TOTAL_BYTES = 64 * 1024
_CONTROL_DESTINATION = Path("/run/httk-mpi")
_BWRAP_OPTIONS = frozenset(
    {
        "--assert-userns-disabled",
        "--bind-fd",
        "--clearenv",
        "--disable-userns",
        "--new-session",
        "--ro-bind-data",
        "--ro-bind-fd",
        "--unshare-ipc",
        "--unshare-net",
        "--unshare-pid",
        "--unshare-user",
        "--unshare-uts",
    }
)
_FIXED_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin",
    "HOME": "/tmp/home",
    "TMPDIR": "/tmp",
    "LANG": "C.UTF-8",
}


@dataclass(slots=True)
class _PreparedSandbox:
    argv: list[str]
    descriptors: tuple[int, ...]

    def close(self) -> None:
        descriptors, self.descriptors = self.descriptors, ()
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


@dataclass(frozen=True, slots=True)
class _ResolvedRoots:
    mutable: tuple[Path, ...]
    readonly: tuple[Path, ...]
    broker: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class _AllocationIdentity:
    job_id: str
    node: str


def _source_path() -> Path:
    return Path(__file__).absolute()


def _is_within(path: Path, roots: tuple[Path, ...]) -> bool:
    return any(path == root or path.is_relative_to(root) for root in roots)


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _check_protected_file(path: Path, mutable_roots: tuple[Path, ...] = ()) -> os.stat_result:
    try:
        information = path.lstat()
    except OSError as exc:
        raise ValueError(f"trusted source is unavailable: {path}") from exc
    if not stat.S_ISREG(information.st_mode):
        raise ValueError(f"trusted source must be a regular non-symlink file: {path}")
    if information.st_mode & 0o022:
        raise ValueError(f"trusted source must not be writable by group or other: {path}")
    if _is_within(path, mutable_roots):
        raise ValueError(f"trusted source must be outside daemon mutable roots: {path}")
    return information


def _policy_api() -> dict[str, Any]:
    source = _source_path().with_name("_daemon_policy.py")
    _check_protected_file(source)
    return runpy.run_path(str(source))


def _load_policy_once(path: Path) -> tuple[Any, bytes]:
    api = _policy_api()
    loader: Any = api.get("_load_policy_with_bytes")
    if not callable(loader):
        raise RuntimeError("installed daemon policy module has no isolated loader")
    policy, data = cast(tuple[Any, bytes], loader(path))
    return policy, data


def _open_directory_nofollow(path: Path) -> int:
    descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            previous_descriptor, descriptor = descriptor, next_descriptor
            os.close(previous_descriptor)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _validate_directory(
    descriptor: int,
    path: Path,
    *,
    owner: int | None = None,
    exact_mode: int | None = None,
    allow_root_sticky_parent: bool = False,
) -> os.stat_result:
    information = os.fstat(descriptor)
    if not stat.S_ISDIR(information.st_mode):
        raise ValueError(f"protected path must be a directory: {path}")
    mode = stat.S_IMODE(information.st_mode)
    if owner is not None and information.st_uid != owner:
        raise ValueError(f"protected directory has foreign ownership: {path}")
    if exact_mode is not None and mode != exact_mode:
        raise ValueError(f"protected directory must have mode {exact_mode:04o}: {path}")
    if exact_mode is None and mode & 0o022:
        sticky_root = allow_root_sticky_parent and information.st_uid == 0 and bool(mode & stat.S_ISVTX)
        if not sticky_root:
            raise ValueError(f"protected directory must not be writable by group or other: {path}")
    return information


def _open_private_child(control_root: Path, child: str) -> tuple[Path, int]:
    root_fd = _open_directory_nofollow(control_root)
    try:
        information = _validate_directory(root_fd, control_root)
        if information.st_uid not in (0, os.geteuid()):
            raise ValueError(f"protected directory has foreign ownership: {control_root}")
        descriptor = os.open(
            child,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
    finally:
        os.close(root_fd)
    path = control_root / child
    try:
        _validate_directory(descriptor, path, owner=os.geteuid(), exact_mode=0o700)
    except BaseException:
        os.close(descriptor)
        raise
    return path, descriptor


def _create_control_directory(control_root: Path) -> tuple[Path, int]:
    root_fd = _open_directory_nofollow(control_root)
    try:
        information = _validate_directory(root_fd, control_root)
        if information.st_uid not in (0, os.geteuid()):
            raise ValueError(f"protected directory has foreign ownership: {control_root}")
        for _ in range(128):
            child = f"httk-{secrets.token_hex(16)}"
            try:
                os.mkdir(child, mode=0o700, dir_fd=root_fd)
            except FileExistsError:
                continue
            descriptor = os.open(
                child,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=root_fd,
            )
            path = control_root / child
            try:
                _validate_directory(descriptor, path, owner=os.geteuid(), exact_mode=0o700)
            except BaseException:
                os.close(descriptor)
                raise
            return path, descriptor
    finally:
        os.close(root_fd)
    raise RuntimeError("could not allocate a private MPI control directory")


def _open_control_source(control_root: Path, source: Path) -> int:
    if source.parent != control_root or source.name in ("", ".", ".."):
        raise ValueError("--control-source must be a direct child of mpi.control_root")
    _, descriptor = _open_private_child(control_root, source.name)
    return descriptor


def _create_shared_memory_directory(policy: Any, handle: str) -> tuple[Path, int]:
    assert policy.mpi is not None
    root = policy.mpi.shm_root
    root_fd = _open_directory_nofollow(root)
    try:
        information = _validate_directory(root_fd, root, allow_root_sticky_parent=True)
        if information.st_uid not in (0, os.geteuid()):
            raise ValueError(f"protected directory has foreign ownership: {root}")
        child = f"httk-{policy.enrollment_id}-{handle}"
        try:
            os.mkdir(child, mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        descriptor = os.open(
            child,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
    finally:
        os.close(root_fd)
    path = root / child
    try:
        _validate_directory(descriptor, path, owner=os.geteuid(), exact_mode=0o700)
    except BaseException:
        os.close(descriptor)
        raise
    return path, descriptor


def _open_device_nofollow(path: Path) -> int:
    parent = _open_directory_nofollow(path.parent)
    try:
        descriptor = os.open(path.name, os.O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent)
    finally:
        os.close(parent)
    information = os.fstat(descriptor)
    if not (stat.S_ISCHR(information.st_mode) or stat.S_ISBLK(information.st_mode)):
        os.close(descriptor)
        raise ValueError(f"approved MPI device is not a device node: {path}")
    return descriptor


def _resolve_declared_path(path: Path, *, strict: bool) -> Path:
    try:
        return path.resolve(strict=strict)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"approved runtime path is unavailable: {path}") from exc


def _resolved_roots(policy: Any, mode: str) -> _ResolvedRoots:
    mutable = tuple(
        _resolve_declared_path(path, strict=False)
        for path in (policy.workspace, policy.requests, policy.responses, policy.state)
    )
    readonly = tuple(_resolve_declared_path(path, strict=True) for path in policy.readonly_paths)
    broker = tuple(
        _resolve_declared_path(path, strict=mode in ("broker", "allocation")) for path in policy.broker_paths
    )
    if Path("/") in {*readonly, *broker}:
        raise ValueError("approved runtime paths must not resolve to the filesystem root")
    if any(_overlap(runtime_root, mutable_root) for runtime_root in (*readonly, *broker) for mutable_root in mutable):
        raise ValueError("resolved runtime paths must be disjoint from mutable roots")
    if any(_overlap(readonly_root, broker_root) for readonly_root in readonly for broker_root in broker):
        raise ValueError("resolved readonly and broker roots must be pairwise disjoint")
    return _ResolvedRoots(mutable, readonly, broker)


def _validate_resolved_mpi_roots(
    policy: Any, resolved_roots: _ResolvedRoots, mode: str, *, mpi_payload: bool = False
) -> None:
    if policy.mpi is None:
        return
    mpi_paths: list[Path] = []
    if mode == "allocation" or mpi_payload:
        mpi_paths.append(_resolve_declared_path(policy.mpi.control_root, strict=True))
    if mode == "mpi-rank":
        mpi_paths.extend(_resolve_declared_path(path, strict=True) for path in policy.mpi.pmix_roots)
        mpi_paths.append(_resolve_declared_path(policy.mpi.shm_root, strict=True))
    for root in mpi_paths:
        if root == Path("/"):
            raise ValueError("MPI roots must not resolve to the filesystem root")
        if any(
            _overlap(root, item) for item in (*resolved_roots.mutable, *resolved_roots.readonly, *resolved_roots.broker)
        ):
            raise ValueError("resolved MPI roots must be disjoint from daemon and runtime roots")
    for index, left in enumerate(mpi_paths):
        for right in mpi_paths[index + 1 :]:
            if _overlap(left, right):
                raise ValueError("resolved MPI roots must be pairwise disjoint")


def _allocation_identity(policy: Any) -> _AllocationIdentity:
    restart = os.environ.get("SLURM_RESTART_COUNT", "0")
    if restart != "0":
        raise ValueError("allocation mode refuses a malformed or nonzero SLURM_RESTART_COUNT")
    job_id = os.environ.get("SLURM_JOB_ID")
    if job_id is None or re.fullmatch(r"[1-9][0-9]{0,19}", job_id) is None:
        raise ValueError("allocation mode requires a numeric SLURM_JOB_ID")
    node = os.environ.get("SLURMD_NODENAME")
    if node is None or _HOSTNAME.fullmatch(node) is None:
        raise ValueError("allocation mode requires a valid SLURMD_NODENAME")
    cluster = os.environ.get("SLURM_CLUSTER_NAME")
    if cluster is not None and cluster != policy.cluster:
        raise ValueError("SLURM_CLUSTER_NAME does not match the protected cluster")
    return _AllocationIdentity(job_id, node)


def _capture_rank_environment(policy: Any, profile: Any) -> tuple[tuple[str, str], ...]:
    captured: list[tuple[str, str]] = []
    total = 0
    for name, value in os.environ.items():
        if name not in _MPI_EXACT_ENVIRONMENT and not name.startswith(_MPI_ENVIRONMENT_PREFIXES):
            continue
        if _MPI_ENVIRONMENT_NAME.fullmatch(name) is None:
            raise ValueError("invalid MPI environment name")
        encoded = value.encode("utf-8")
        if "\0" in value or len(encoded) > _MPI_ENVIRONMENT_VALUE_BYTES:
            raise ValueError(f"MPI environment value is invalid or too large: {name}")
        total += len(name) + len(encoded)
        captured.append((name, value))
    if len(captured) > _MPI_ENVIRONMENT_LIMIT or total > _MPI_ENVIRONMENT_TOTAL_BYTES:
        raise ValueError("MPI environment exceeds the protected bound")

    values = dict(captured)
    missing = _MPI_EXACT_ENVIRONMENT - values.keys()
    if missing:
        raise ValueError(f"MPI rank environment is missing {min(missing)}")
    numeric = {
        name: values[name] for name in ("SLURM_PROCID", "SLURM_LOCALID", "SLURM_NODEID", "SLURM_NTASKS", "SLURM_JOB_ID")
    }
    if any(not value.isascii() or not value.isdecimal() for value in numeric.values()):
        raise ValueError("MPI rank identity values must be numeric")
    ranks = profile.mpi.ranks
    nodes = profile.mpi.nodes
    if int(numeric["SLURM_NTASKS"]) != ranks:
        raise ValueError("SLURM_NTASKS does not match the protected MPI profile")
    if not 0 <= int(numeric["SLURM_PROCID"]) < ranks:
        raise ValueError("SLURM_PROCID is outside the protected MPI profile")
    if not 0 <= int(numeric["SLURM_LOCALID"]) < ranks:
        raise ValueError("SLURM_LOCALID is outside the protected MPI profile")
    if not 0 <= int(numeric["SLURM_NODEID"]) < nodes:
        raise ValueError("SLURM_NODEID is outside the protected MPI profile")
    if (
        re.fullmatch(r"[1-9][0-9]{0,19}", numeric["SLURM_JOB_ID"]) is None
        or _HOSTNAME.fullmatch(values["SLURMD_NODENAME"]) is None
    ):
        raise ValueError("MPI rank Slurm identity is invalid")
    cluster = os.environ.get("SLURM_CLUSTER_NAME")
    if cluster is not None and cluster != policy.cluster:
        raise ValueError("SLURM_CLUSTER_NAME does not match the protected cluster")
    return tuple(captured)


def _open_pmix_directory(policy: Any, environment: tuple[tuple[str, str], ...]) -> tuple[Path, int] | None:
    values = dict(environment)
    raw_path = values.get("PMIX_SERVER_TMPDIR")
    if raw_path is None:
        return None
    path = Path(raw_path)
    if not path.is_absolute() or ".." in path.parts or "\0" in raw_path:
        raise ValueError("PMIX_SERVER_TMPDIR must be an absolute path without '..' or NUL")
    resolved_path = _resolve_declared_path(path, strict=True)
    resolved_roots = tuple(_resolve_declared_path(root, strict=True) for root in policy.mpi.pmix_roots)
    if not any(resolved_path != root and resolved_path.is_relative_to(root) for root in resolved_roots):
        raise ValueError("PMIX_SERVER_TMPDIR is outside approved PMIx roots")
    reserved_destinations = (
        Path("/workspace"),
        Path("/requests"),
        Path("/responses"),
        Path("/control"),
        _CONTROL_DESTINATION,
        Path("/proc"),
        Path("/dev"),
        Path("/daemon-policy.json"),
        Path("/tmp/home"),
    )
    if any(_overlap(path, destination) for destination in reserved_destinations):
        raise ValueError("PMIX_SERVER_TMPDIR overlaps a reserved sandbox destination")
    forbidden = (
        policy.workspace,
        policy.requests,
        policy.responses,
        policy.state,
        policy.mpi.control_root,
        policy.mpi.shm_root,
        *policy.readonly_paths,
        *policy.broker_paths,
    )
    if any(_overlap(resolved_path, _resolve_declared_path(item, strict=False)) for item in forbidden):
        raise ValueError("PMIX_SERVER_TMPDIR overlaps a protected daemon root")
    descriptor = _open_directory_nofollow(path)
    try:
        _validate_directory(descriptor, path, owner=os.geteuid())
    except BaseException:
        os.close(descriptor)
        raise
    return path, descriptor


def _check_command(path: Path, approved: tuple[Path, ...], mutable_roots: tuple[Path, ...]) -> None:
    resolved = _resolve_declared_path(path, strict=True)
    if any(_overlap(resolved, mutable_root) for mutable_root in mutable_roots):
        raise ValueError(f"trusted command resolves across a mutable root: {path}")
    if not _is_within(resolved, approved):
        raise ValueError(f"trusted command resolves outside its approved roots: {path}")
    information = resolved.stat()
    if not stat.S_ISREG(information.st_mode) or information.st_mode & 0o111 == 0:
        raise ValueError(f"trusted command must be an executable regular file: {path}")
    if information.st_uid not in (0, os.geteuid()) or information.st_mode & 0o022:
        raise ValueError(f"trusted command has unprotected ownership or mode: {path}")


def _check_bwrap(path: Path) -> None:
    environment = dict(_FIXED_ENVIRONMENT)
    try:
        version = subprocess.run(
            [str(path), "--version"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=environment,
        )
        help_result = subprocess.run(
            [str(path), "--help"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("Bubblewrap feature check failed") from exc
    match = re.search(r"(?:bubblewrap\s+)?(\d+)\.(\d+)(?:\.(\d+))?", version.stdout)
    if version.returncode != 0 or match is None or tuple(int(item or 0) for item in match.groups()) < (0, 9, 0):
        raise ValueError("Bubblewrap 0.9.0 or newer is required")
    help_text = help_result.stdout + help_result.stderr
    if help_result.returncode != 0 or any(option not in help_text for option in _BWRAP_OPTIONS):
        raise ValueError("Bubblewrap lacks required confinement features")


def _policy_snapshot(data: bytes) -> int:
    if not hasattr(os, "memfd_create"):
        raise RuntimeError("the daemon bootstrap requires Linux memfd support")
    descriptor = os.memfd_create("httk-daemon-policy", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("policy snapshot write made no progress")
            offset += written
        os.lseek(descriptor, 0, os.SEEK_SET)
        seals = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_WRITE
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS, seals)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _base_bwrap_argv(policy: Any, mode: str, rank_environment: tuple[tuple[str, str], ...] = ()) -> list[str]:
    argv = [
        str(policy.bwrap),
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
    ]
    if mode == "payload":
        argv.append("--unshare-net")
    argv += [
        "--disable-userns",
        "--assert-userns-disabled",
        "--cap-drop",
        "ALL",
        "--new-session",
        "--die-with-parent",
        "--clearenv",
    ]
    for name, value in _FIXED_ENVIRONMENT.items():
        argv += ["--setenv", name, value]
    if mode == "mpi-rank":
        assert policy.mpi is not None
        environment = dict(rank_environment)
        environment.update(policy.mpi.environment)
        for name, value in environment.items():
            argv += ["--setenv", name, value]
    if mode == "broker" and policy.slurm_conf is not None:
        argv += ["--setenv", "SLURM_CONF", str(policy.slurm_conf)]
    # Bubblewrap starts from its own empty tmpfs root; only these private paths
    # and the descriptor-backed mounts below are then added to it.
    argv += ["--dir", "/workspace", "--dir", "/run", "--tmpfs", "/tmp", "--dir", "/tmp/home"]
    return argv


def _inside_command(arguments: argparse.Namespace, policy: Any, policy_source: Path) -> list[str]:
    if arguments.mode == "payload":
        return [
            str(policy.python),
            "-I",
            "-m",
            "httk.workflow._daemon_payload",
            "--profile",
            arguments.profile,
            "--handle",
            arguments.handle,
        ]
    if arguments.mode == "allocation":
        return [
            str(policy.python),
            "-I",
            "-m",
            "httk.workflow._daemon_mpi_service",
            "--policy-source",
            str(policy_source),
            "--profile",
            arguments.profile,
            "--handle",
            arguments.handle,
            "--job-id",
            arguments.allocation_identity.job_id,
            "--node",
            arguments.allocation_identity.node,
            "--control-source",
            str(arguments.control_source),
        ]
    if arguments.mode == "mpi-rank":
        return [
            str(policy.python),
            "-I",
            "-m",
            "httk.workflow._daemon_mpi_rank",
            "--profile",
            arguments.profile,
            "--handle",
            arguments.handle,
            "--request-id",
            arguments.request_id,
        ]
    result = [
        str(policy.python),
        "-I",
        "-m",
        "httk.workflow._daemon_service",
        "--policy",
        "/daemon-policy.json",
        "--policy-source",
        str(policy_source),
    ]
    if arguments.check:
        result.append("--check")
    elif arguments.once:
        result.append("--once")
    return result


def _prepare_sandbox(
    arguments: argparse.Namespace, policy: Any, policy_data: bytes, policy_source: Path
) -> _PreparedSandbox:
    mutable_roots = (policy.workspace, policy.requests, policy.responses, policy.state)
    resolved_roots = _resolved_roots(policy, arguments.mode)
    mpi_payload = arguments.mode == "payload" and policy.profile(arguments.profile).mpi is not None
    _validate_resolved_mpi_roots(policy, resolved_roots, arguments.mode, mpi_payload=mpi_payload)
    source = _source_path()
    _check_protected_file(source, mutable_roots)
    _check_protected_file(source.with_name("_daemon_policy.py"), mutable_roots)
    _check_protected_file(policy_source, mutable_roots)
    _check_command(policy.python, resolved_roots.readonly, resolved_roots.mutable)
    _check_command(policy.bwrap, (*resolved_roots.readonly, *resolved_roots.broker), resolved_roots.mutable)
    if arguments.mode == "broker":
        for command in (policy.sbatch, policy.squeue, policy.scancel):
            _check_command(command, (*resolved_roots.readonly, *resolved_roots.broker), resolved_roots.mutable)
        if policy.mpi is not None:
            _check_command(policy.mpi.srun, (*resolved_roots.readonly, *resolved_roots.broker), resolved_roots.mutable)
    if arguments.mode in ("broker", "allocation") and policy.slurm_conf is not None:
        resolved_slurm_conf = _resolve_declared_path(policy.slurm_conf, strict=True)
        if not _is_within(resolved_slurm_conf, resolved_roots.broker) or any(
            _overlap(resolved_slurm_conf, mutable_root) for mutable_root in resolved_roots.mutable
        ):
            raise ValueError("Slurm configuration resolves outside broker-only runtime roots")
    if arguments.mode == "allocation":
        assert policy.mpi is not None
        _check_command(policy.mpi.srun, (*resolved_roots.readonly, *resolved_roots.broker), resolved_roots.mutable)
    _check_bwrap(policy.bwrap)

    descriptors: list[int] = []
    rank_environment = getattr(arguments, "rank_environment", ())
    argv = _base_bwrap_argv(policy, arguments.mode, rank_environment)
    try:
        workspace_fd = _open_directory_nofollow(policy.workspace)
        descriptors.append(workspace_fd)
        writable_workspace = arguments.mode in ("payload", "mpi-rank")
        argv += ["--bind-fd" if writable_workspace else "--ro-bind-fd", str(workspace_fd), "/workspace"]
        if arguments.mode == "broker":
            for source_path, destination in (
                (policy.requests, "/requests"),
                (policy.responses, "/responses"),
                (policy.state, "/control"),
            ):
                descriptor = _open_directory_nofollow(source_path)
                descriptors.append(descriptor)
                argv += ["--dir", destination, "--bind-fd", str(descriptor), destination]

        runtime_paths = list(zip(policy.readonly_paths, resolved_roots.readonly, strict=True))
        if arguments.mode in ("broker", "allocation"):
            runtime_paths.extend(zip(policy.broker_paths, resolved_roots.broker, strict=True))
        for runtime_path, resolved_path in runtime_paths:
            descriptor = os.open(resolved_path, os.O_PATH | os.O_CLOEXEC)
            descriptors.append(descriptor)
            argv += ["--ro-bind-fd", str(descriptor), str(runtime_path)]

        policy_fd = _policy_snapshot(policy_data)
        descriptors.append(policy_fd)
        argv += ["--ro-bind-data", str(policy_fd), "/daemon-policy.json"]
        if arguments.mode == "allocation":
            assert policy.mpi is not None
            control_source, control_fd = _create_control_directory(policy.mpi.control_root)
            arguments.control_source = control_source
            descriptors.append(control_fd)
            argv += ["--dir", str(_CONTROL_DESTINATION), "--bind-fd", str(control_fd), str(_CONTROL_DESTINATION)]
        elif arguments.mode == "payload" and policy.profile(arguments.profile).mpi is not None:
            assert policy.mpi is not None
            control_fd = _open_control_source(policy.mpi.control_root, Path(arguments.control_source))
            descriptors.append(control_fd)
            argv += ["--dir", str(_CONTROL_DESTINATION), "--ro-bind-fd", str(control_fd), str(_CONTROL_DESTINATION)]
            argv += ["--setenv", "HTTK_DAEMON_MPI_HANDLE", arguments.handle]
            argv += ["--setenv", "HTTK_DAEMON_MPI_PROFILE", arguments.profile]

        argv += ["--proc", "/proc", "--dev", "/dev"]
        if arguments.mode == "mpi-rank":
            assert policy.mpi is not None
            _, shm_fd = _create_shared_memory_directory(policy, arguments.handle)
            descriptors.append(shm_fd)
            argv += ["--bind-fd", str(shm_fd), "/dev/shm"]
            pmix = _open_pmix_directory(policy, rank_environment)
            if pmix is not None:
                pmix_path, pmix_fd = pmix
                descriptors.append(pmix_fd)
                pmix_parent = pmix_path.parent
                pmix_parents: list[Path] = []
                while pmix_parent not in (Path("/"), Path("/tmp"), Path("/run")):
                    pmix_parents.append(pmix_parent)
                    pmix_parent = pmix_parent.parent
                for path_destination in reversed(pmix_parents):
                    argv += ["--dir", str(path_destination)]
                argv += ["--bind-fd", str(pmix_fd), str(pmix_path)]
            created_device_parents: set[Path] = set()
            for device in policy.mpi.devices:
                device_parent = device.parent
                device_parents: list[Path] = []
                while device_parent != Path("/dev"):
                    device_parents.append(device_parent)
                    device_parent = device_parent.parent
                for path_destination in reversed(device_parents):
                    if path_destination not in created_device_parents:
                        argv += ["--dir", str(path_destination)]
                        created_device_parents.add(path_destination)
                device_fd = _open_device_nofollow(device)
                descriptors.append(device_fd)
                argv += ["--bind-fd", str(device_fd), str(device)]
        chdir = "/workspace" if arguments.mode in ("payload", "mpi-rank") else "/"
        argv += ["--chdir", chdir]
        argv += ["--", *_inside_command(arguments, policy, policy_source)]
        return _PreparedSandbox(argv, tuple(descriptors))
    except BaseException:
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _open_descriptor_numbers() -> set[int]:
    try:
        return {int(name) for name in os.listdir("/proc/self/fd") if name.isdigit()}
    except OSError as exc:
        raise RuntimeError("cannot enumerate inherited descriptors through /proc/self/fd") from exc


def _exec_prepared(prepared: _PreparedSandbox) -> None:
    try:
        preserved = {0, 1, 2, *prepared.descriptors}
        for descriptor in _open_descriptor_numbers() - preserved:
            try:
                os.close(descriptor)
            except OSError:
                pass
        for descriptor in prepared.descriptors:
            os.set_inheritable(descriptor, True)
        os.execve(prepared.argv[0], prepared.argv, {})
    except BaseException:
        prepared.close()
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-workspace-daemon-bootstrap")
    parser.add_argument("--policy", required=True, metavar="ABS")
    parser.add_argument("--workspace", metavar="ABS")
    parser.add_argument("--mode", required=True, choices=("broker", "payload", "allocation", "mpi-rank"))
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--once", action="store_true")
    parser.add_argument("--profile")
    parser.add_argument("--handle")
    parser.add_argument("--control-source")
    parser.add_argument("--request-id")
    return parser


def _validate_arguments(arguments: argparse.Namespace, policy: Any) -> Path:
    policy_source = Path(arguments.policy)
    if not policy_source.is_absolute() or ".." in policy_source.parts or "\0" in str(policy_source):
        raise ValueError("--policy must be an absolute local path")
    if arguments.mode == "broker":
        if arguments.workspace is None:
            raise ValueError("broker mode requires --workspace")
        workspace = Path(arguments.workspace)
        if not workspace.is_absolute() or ".." in workspace.parts or "\0" in str(workspace):
            raise ValueError("--workspace must be an absolute local path")
        if workspace != policy.workspace:
            raise ValueError("--workspace must exactly match the policy workspace")
        if (
            arguments.profile is not None
            or arguments.handle is not None
            or arguments.control_source is not None
            or arguments.request_id is not None
        ):
            raise ValueError("broker mode forbids payload arguments")
    else:
        if arguments.workspace is not None or arguments.check or arguments.once:
            raise ValueError(f"{arguments.mode} mode forbids broker arguments")
        if type(arguments.profile) is not str or _PROFILE.fullmatch(arguments.profile) is None:
            raise ValueError(f"{arguments.mode} mode requires a valid --profile")
        profile = policy.profile(arguments.profile)
        if type(arguments.handle) is not str or _HANDLE.fullmatch(arguments.handle) is None:
            raise ValueError(f"{arguments.mode} mode requires a valid --handle")
        if arguments.mode == "payload":
            if arguments.request_id is not None:
                raise ValueError("payload mode forbids --request-id")
            if profile.mpi is None:
                if arguments.control_source is not None:
                    raise ValueError("serial payload mode forbids --control-source")
            else:
                if policy.mpi is None or type(arguments.control_source) is not str:
                    raise ValueError("MPI payload mode requires --control-source")
                control_source = Path(arguments.control_source)
                if (
                    not control_source.is_absolute()
                    or ".." in control_source.parts
                    or "\0" in str(control_source)
                    or control_source.parent != policy.mpi.control_root
                ):
                    raise ValueError("--control-source must be an absolute direct child of mpi.control_root")
        elif arguments.mode == "allocation":
            if profile.mpi is None or policy.mpi is None:
                raise ValueError("allocation mode requires an MPI profile")
            if arguments.control_source is not None or arguments.request_id is not None:
                raise ValueError("allocation mode creates its control source and forbids --request-id")
            arguments.allocation_identity = _allocation_identity(policy)
        else:
            if profile.mpi is None or policy.mpi is None:
                raise ValueError("mpi-rank mode requires an MPI profile")
            if arguments.control_source is not None:
                raise ValueError("mpi-rank mode forbids --control-source")
            if type(arguments.request_id) is not str or _HANDLE.fullmatch(arguments.request_id) is None:
                raise ValueError("mpi-rank mode requires a valid --request-id")
            arguments.rank_environment = _capture_rank_environment(policy, profile)
    return policy_source


def main(argv: list[str] | None = None) -> int:
    """Validate policy and replace this process with Bubblewrap.

    :param argv: Bootstrap arguments, or process arguments when omitted.
    :return: Two on a validation or startup refusal; success replaces the process.
    """

    arguments = _parser().parse_args(sys.argv[1:] if argv is None else argv)
    try:
        policy_path = Path(arguments.policy)
        if not policy_path.is_absolute():
            raise ValueError("--policy must be an absolute local path")
        policy, policy_data = _load_policy_once(policy_path)
        policy_source = _validate_arguments(arguments, policy)
        prepared = _prepare_sandbox(arguments, policy, policy_data, policy_source)
        try:
            _exec_prepared(prepared)
        finally:
            prepared.close()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"daemon bootstrap: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
