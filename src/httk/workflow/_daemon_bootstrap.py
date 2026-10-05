"""Isolated Bubblewrap bootstrap for the confined workspace daemon."""

import argparse
import fcntl
import os
import re
import runpy
import secrets
import signal
import stat
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

# Conda-built Pythons may use glibc headers that predate O_PATH and memfd; the kernel ABI values are fixed.
_O_PATH = getattr(os, "O_PATH", 0o10000000)
_MFD_CLOEXEC = getattr(os, "MFD_CLOEXEC", 1)
_MFD_ALLOW_SEALING = getattr(os, "MFD_ALLOW_SEALING", 2)
_F_ADD_SEALS = getattr(fcntl, "F_ADD_SEALS", 1033)
# F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE (Linux ABI values 1, 2, 4, 8).
_SEALS = sum(
    getattr(fcntl, name, value)
    for name, value in (("F_SEAL_SEAL", 1), ("F_SEAL_SHRINK", 2), ("F_SEAL_GROW", 4), ("F_SEAL_WRITE", 8))
)
_HANDLE = re.compile(r"[0-9a-f]{32}\Z")
_PROFILE = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_HOSTNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}\Z")
_COUNT = re.compile(r"(?:0|[1-9][0-9]{0,18})\Z")
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_JOB_LOG_BYTES = 1024 * 1024
_COUNT_MAX = 2**63 - 1
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
_POLICY_DESTINATION = "/daemon-policy.json"
# The broker and the MPI allocation service run only trusted code, so they see the host read-only;
# their writable mounts and policy data sit on the private /tmp, since a read-only / takes no new directories.
_HOST_VIEW_MODES = ("broker", "allocation")
_HOST_ROOT_DESTINATION = "/tmp/daemon-root"
_HOST_STATE_DESTINATION = "/tmp/control"
_HOST_CONTROL_DESTINATION = "/tmp/httk-mpi"
_HOST_POLICY_DESTINATION = "/tmp/daemon-policy.json"
_BWRAP_REQUIRED = frozenset(
    {
        "--bind-fd",
        "--clearenv",
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
_BWRAP_USERNS_BLOCK = ("--disable-userns", "--assert-userns-disabled")
_FIXED_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin",
    "HOME": "/tmp/home",
    "TMPDIR": "/tmp",
    "LANG": "C.UTF-8",
}
# The trusted operator environment reaches the broker; scheduler input variables would change submissions,
# and interpreter or loader variables would change the broker's Python or the host Slurm clients.
_OPERATOR_DROPPED_PREFIXES = ("SBATCH_", "SALLOC_", "SRUN_", "SLURM_", "PYTHON")
_OPERATOR_DROPPED = frozenset({"BASH_ENV", "ENV", "LD_PRELOAD", "LD_LIBRARY_PATH"})
_OPERATOR_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_OPERATOR_LIMIT = 512
_OPERATOR_BYTES = 256 * 1024
# The broker's home and cache lie on its private /tmp: the host view, including the passwd home, is read-only.
_BROKER_ENVIRONMENT = {"HOME": "/tmp/home", "XDG_CACHE_HOME": "/tmp/home/.cache", "TMPDIR": "/tmp"}
_BROKER_UNSET = ("XDG_RUNTIME_DIR", "XDG_CONFIG_HOME")


def _operator_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """Filter the trusted operator environment passed to the broker and its Slurm clients.

    :param environ: The operator environment, usually ``os.environ``.
    :return: The kept variables in name order, bounded in count and size.
    """

    kept: dict[str, str] = {}
    total = 0
    for name in sorted(environ):
        value = environ[name]
        if (
            (name.startswith(_OPERATOR_DROPPED_PREFIXES) and name != "SLURM_CONF")
            or name in _OPERATOR_DROPPED
            or not _OPERATOR_NAME.match(name)
            or "\0" in value
        ):
            continue
        total += len(os.fsencode(name)) + len(os.fsencode(value)) + 2
        # ponytail: a full bound drops the alphabetically last variables; rank them if a site ever hits it.
        if len(kept) == _OPERATOR_LIMIT or total > _OPERATOR_BYTES:
            print(
                f"httk workspace daemon: operator environment beyond {_OPERATOR_LIMIT} variables or "
                f"{_OPERATOR_BYTES} bytes was dropped from {name}",
                file=sys.stderr,
            )
            break
        kept[name] = value
    return kept


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
    if information.st_mode & 0o002:
        raise ValueError(f"trusted source must not be world-writable: {path}")
    if _is_within(path, mutable_roots):
        raise ValueError(f"trusted source must be outside daemon mutable roots: {path}")
    return information


def _policy_api() -> dict[str, Any]:
    source = _source_path().with_name("_daemon_policy.py")
    _check_protected_file(source)
    return runpy.run_path(str(source))


def _load_policy_once(path: Path) -> tuple[Any, bytes, Any]:
    api = _policy_api()
    loader: Any = api.get("_load_policy_with_bytes")
    check_layout: Any = api.get("check_layout")
    if not callable(loader) or not callable(check_layout):
        raise RuntimeError("installed daemon policy module has no isolated loader or layout check")
    policy, data = cast(tuple[Any, bytes], loader(path))
    return policy, data, check_layout


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
    if exact_mode is None and mode & 0o002:
        sticky_root = allow_root_sticky_parent and information.st_uid == 0 and bool(mode & stat.S_ISVTX)
        if not sticky_root:
            raise ValueError(f"protected directory must not be world-writable: {path}")
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
        descriptor = os.open(path.name, _O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent)
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


def _resolved_roots(policy: Any) -> _ResolvedRoots:
    # The dedicated parent holds the workspace and the exchange, and the broker binds it read-write.
    mutable = tuple(_resolve_declared_path(path, strict=False) for path in (policy.root, policy.state))
    # Only payload and rank sandboxes mount these; every mode resolves them so a broker check reports them early.
    readonly = tuple(_resolve_declared_path(path, strict=True) for path in policy.readonly_paths)
    if Path("/") in readonly:
        raise ValueError("approved runtime paths must not resolve to the filesystem root")
    if any(_overlap(runtime_root, mutable_root) for runtime_root in readonly for mutable_root in mutable):
        raise ValueError("resolved runtime paths must be disjoint from mutable roots")
    return _ResolvedRoots(mutable, readonly)


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
        if any(_overlap(root, item) for item in (*resolved_roots.mutable, *resolved_roots.readonly)):
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
    if job_id is None or _JOB_ID.fullmatch(job_id) is None:
        raise ValueError("allocation mode requires a numeric SLURM_JOB_ID")
    node = os.environ.get("SLURMD_NODENAME")
    if node is None or _HOSTNAME.fullmatch(node) is None:
        raise ValueError("allocation mode requires a valid SLURMD_NODENAME")
    cluster = os.environ.get("SLURM_CLUSTER_NAME")
    if cluster is not None and cluster != policy.cluster:
        raise ValueError("SLURM_CLUSTER_NAME does not match the protected cluster")
    return _AllocationIdentity(job_id, node)


def _count(value: str, name: str, *, positive: bool = False) -> int:
    if _COUNT.fullmatch(value) is None or int(value) > _COUNT_MAX or (positive and value == "0"):
        raise ValueError(f"{name} must be a canonical decimal integer from {int(positive)} through {_COUNT_MAX}")
    return int(value)


def _capacity_arguments(capacity: tuple[int, int | None]) -> list[str]:
    procs, mem_mb = capacity
    return ["--procs", str(procs), *(() if mem_mb is None else ("--mem-mb", str(mem_mb)))]


def _allocation_capacity(profile: Any) -> tuple[int, int | None]:
    """Read the worker capacity of the trusted batch-job environment, before any payload runs."""

    def value(name: str) -> int | None:
        return None if name not in os.environ else _count(os.environ[name], name)

    per_node, per_cpu = value("SLURM_MEM_PER_NODE"), value("SLURM_MEM_PER_CPU")
    if profile.mpi is None:
        nodes = 1
        cpus = value("SLURM_CPUS_ON_NODE")
        procs = profile.cpus if cpus is None else cpus
        if not procs:
            raise ValueError("Slurm did not report the allocated CPUs")
    else:
        # With --export=NIL the job's CPUs per task are exactly the submitted value, or Slurm's default of one.
        nodes = profile.mpi.nodes
        procs = (profile.cpus or 1) * profile.mpi.ranks
    # A Slurm memory value of zero means "not reported"; fall through to the next source.
    if per_node:
        mem_mb = per_node * nodes
    elif per_cpu:
        mem_mb = per_cpu * procs
    elif profile.memory_mb is not None:
        mem_mb = profile.memory_mb * nodes
    else:
        mem_mb = None
    if max(procs, mem_mb or 0) > _COUNT_MAX:
        raise ValueError(f"allocation capacity exceeds {_COUNT_MAX}")
    return procs, mem_mb


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
    if _JOB_ID.fullmatch(numeric["SLURM_JOB_ID"]) is None or _HOSTNAME.fullmatch(values["SLURMD_NODENAME"]) is None:
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
        _CONTROL_DESTINATION,
        Path("/proc"),
        Path("/dev"),
        Path(_POLICY_DESTINATION),
        Path("/tmp/home"),
    )
    if any(_overlap(path, destination) for destination in reserved_destinations):
        raise ValueError("PMIX_SERVER_TMPDIR overlaps a reserved sandbox destination")
    forbidden = (
        policy.root,
        policy.state,
        policy.mpi.control_root,
        policy.mpi.shm_root,
        *policy.readonly_paths,
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


def _check_command(path: Path, mutable_roots: tuple[Path, ...], approved: tuple[Path, ...] | None = None) -> None:
    # ``approved`` applies only to commands run inside a payload sandbox; host-view commands need no containment.
    resolved = _resolve_declared_path(path, strict=True)
    if any(_overlap(resolved, mutable_root) for mutable_root in mutable_roots):
        raise ValueError(f"trusted command resolves across a mutable root: {path}")
    if approved is not None and not _is_within(resolved, approved):
        raise ValueError(f"trusted command resolves outside its approved roots: {path}")
    information = resolved.stat()
    if not stat.S_ISREG(information.st_mode) or information.st_mode & 0o111 == 0:
        raise ValueError(f"trusted command must be an executable regular file: {path}")
    if information.st_uid not in (0, os.geteuid()) or information.st_mode & 0o002:
        raise ValueError(f"trusted command has unprotected ownership or mode: {path}")


def _check_bwrap(path: Path) -> bool:
    """Check the Bubblewrap options from its help text; return whether it can block nested user namespaces."""

    try:
        help_result = subprocess.run(
            [str(path), "--help"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=dict(_FIXED_ENVIRONMENT),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("Bubblewrap feature check failed") from exc
    if help_result.returncode != 0:
        raise ValueError("Bubblewrap feature check failed")
    options = set(re.findall(r"--[A-Za-z0-9-]+", help_result.stdout + help_result.stderr))
    missing = sorted(_BWRAP_REQUIRED - options)
    if missing:
        raise ValueError(f"Bubblewrap lacks required confinement features: {', '.join(missing)}")
    return all(option in options for option in _BWRAP_USERNS_BLOCK)


def _memfd_create(name: str) -> int:
    if hasattr(os, "memfd_create"):
        return os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    # Pythons built against old glibc headers lack the wrapper; the running libc usually has it.
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    try:
        function = libc.memfd_create
    except AttributeError as exc:
        raise RuntimeError("the daemon bootstrap requires Linux memfd support") from exc
    descriptor = int(function(name.encode(), _MFD_CLOEXEC | _MFD_ALLOW_SEALING))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def _policy_snapshot(data: bytes) -> int:
    descriptor = _memfd_create("httk-daemon-policy")
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("policy snapshot write made no progress")
            offset += written
        os.lseek(descriptor, 0, os.SEEK_SET)
        fcntl.fcntl(descriptor, _F_ADD_SEALS, _SEALS)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


_MERGED_USR_LINKS = (Path("/bin"), Path("/lib"), Path("/lib64"), Path("/sbin"))


def _merged_usr_symlinks(
    resolved_roots: tuple[Path, ...], declared: set[Path], links: tuple[Path, ...] = _MERGED_USR_LINKS
) -> list[str]:
    # On merged-/usr hosts /lib64 etc. are symlinks into /usr; the defaults mount only /usr, so recreate
    # the links, or the ELF loader (/lib64/ld-linux-*.so.2) and /bin/bash are missing in the sandbox.
    argv: list[str] = []
    for link in links:
        if link in declared or not link.is_symlink():
            continue
        target = link.resolve()
        if any(target == root or target.is_relative_to(root) for root in resolved_roots):
            argv += ["--symlink", os.readlink(link), str(link)]
    return argv


def _base_bwrap_argv(
    policy: Any, mode: str, rank_environment: tuple[tuple[str, str], ...] = (), *, block_userns: bool
) -> list[str]:
    argv = [
        str(policy.bwrap),
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
    ]
    if mode == "payload" and policy.isolate_network:
        argv.append("--unshare-net")
    if block_userns:
        argv += _BWRAP_USERNS_BLOCK
    argv += [
        "--cap-drop",
        "ALL",
        "--new-session",
        "--die-with-parent",
        "--clearenv",
    ]
    environment = dict(_FIXED_ENVIRONMENT)
    if mode == "broker":
        environment |= _operator_environment(os.environ) | _BROKER_ENVIRONMENT
        for name in _BROKER_UNSET:
            environment.pop(name, None)
        if policy.slurm_conf is not None:
            environment["SLURM_CONF"] = str(policy.slurm_conf)
    for name, value in environment.items():
        argv += ["--setenv", name, value]
    if mode == "mpi-rank":
        assert policy.mpi is not None
        environment = dict(rank_environment)
        environment.update(policy.mpi.environment)
        for name, value in environment.items():
            argv += ["--setenv", name, value]
    if mode in _HOST_VIEW_MODES:
        # Recursive, so host submounts (/software, /etc, the munge socket) come in read-only as well.
        argv += ["--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--dir", "/tmp/home"]
        if mode == "broker":
            argv += ["--dir", "/tmp/home/.cache"]
        return argv
    # Payload sandboxes start from Bubblewrap's own empty tmpfs root; only these private paths
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
            *_capacity_arguments(arguments.capacity),
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
            *_capacity_arguments(arguments.capacity),
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
        _HOST_POLICY_DESTINATION,
        "--policy-source",
        str(policy_source),
    ]
    if arguments.check:
        result.append("--check")
    elif arguments.once:
        result.append("--once")
    return result


def _prepare_sandbox(
    arguments: argparse.Namespace, policy: Any, policy_data: bytes, policy_source: Path, check_layout: Any
) -> _PreparedSandbox:
    mutable_roots = (policy.root, policy.state)
    resolved_roots = _resolved_roots(policy)
    host_view = arguments.mode in _HOST_VIEW_MODES
    mpi_payload = arguments.mode == "payload" and policy.profile(arguments.profile).mpi is not None
    _validate_resolved_mpi_roots(policy, resolved_roots, arguments.mode, mpi_payload=mpi_payload)
    source = _source_path()
    _check_protected_file(source, mutable_roots)
    _check_protected_file(source.with_name("_daemon_policy.py"), mutable_roots)
    _check_protected_file(policy_source, mutable_roots)
    # Every entry rechecks the layout; only the broker, which owns the exchange, runs the rename probe.
    check_layout(policy, probe=arguments.mode == "broker")
    # Payloads run python inside their allow-list; bwrap and the Slurm clients run with a host view or on the host.
    _check_command(policy.python, resolved_roots.mutable, resolved_roots.readonly)
    _check_command(policy.bwrap, resolved_roots.mutable)
    if arguments.mode == "broker":
        for command in (policy.sbatch, policy.squeue, policy.scancel):
            _check_command(command, resolved_roots.mutable)
    if host_view and policy.mpi is not None:
        _check_command(policy.mpi.srun, resolved_roots.mutable)
    if host_view and policy.slurm_conf is not None:
        resolved_slurm_conf = _resolve_declared_path(policy.slurm_conf, strict=True)
        if any(_overlap(resolved_slurm_conf, mutable_root) for mutable_root in resolved_roots.mutable):
            raise ValueError("Slurm configuration resolves across a mutable root")
    block_userns = _check_bwrap(policy.bwrap)
    if not block_userns and arguments.mode == "broker":
        # Mounts stay locked either way; only nested user namespaces (kernel attack surface) remain open.
        print(
            "daemon bootstrap: warning: this Bubblewrap lacks --disable-userns (0.8.0+); "
            "sandboxed code can create nested user namespaces",
            file=sys.stderr,
            flush=True,
        )
    if arguments.mode == "payload" and not policy.isolate_network:
        print(
            "daemon bootstrap: note: job network isolation is disabled (daemon.isolate_network=false)",
            file=sys.stderr,
            flush=True,
        )

    descriptors: list[int] = []
    rank_environment = getattr(arguments, "rank_environment", ())
    argv = _base_bwrap_argv(policy, arguments.mode, rank_environment, block_userns=block_userns)
    try:
        if arguments.mode == "broker":
            # One writable bind of the dedicated parent; the service reaches the exchange and the
            # workspace staging area only through descriptor-anchored no-follow opens below it.
            for source_path, destination in (
                (policy.root, _HOST_ROOT_DESTINATION),
                (policy.state, _HOST_STATE_DESTINATION),
            ):
                descriptor = _open_directory_nofollow(source_path)
                descriptors.append(descriptor)
                argv += ["--dir", destination, "--bind-fd", str(descriptor), destination]
        elif not host_view:
            workspace_fd = _open_directory_nofollow(policy.workspace)
            descriptors.append(workspace_fd)
            argv += ["--bind-fd", str(workspace_fd), "/workspace"]
            runtime_paths = list(zip(policy.readonly_paths, resolved_roots.readonly, strict=True))
            for runtime_path, resolved_path in runtime_paths:
                descriptor = os.open(resolved_path, _O_PATH | os.O_CLOEXEC)
                descriptors.append(descriptor)
                argv += ["--ro-bind-fd", str(descriptor), str(runtime_path)]
            argv += _merged_usr_symlinks(
                tuple(resolved for _path, resolved in runtime_paths), {path for path, _ in runtime_paths}
            )

        policy_fd = _policy_snapshot(policy_data)
        descriptors.append(policy_fd)
        argv += ["--ro-bind-data", str(policy_fd), _HOST_POLICY_DESTINATION if host_view else _POLICY_DESTINATION]
        if arguments.mode == "allocation":
            assert policy.mpi is not None
            control_source, control_fd = _create_control_directory(policy.mpi.control_root)
            arguments.control_source = control_source
            descriptors.append(control_fd)
            argv += ["--dir", _HOST_CONTROL_DESTINATION, "--bind-fd", str(control_fd), _HOST_CONTROL_DESTINATION]
        elif arguments.mode == "payload" and policy.profile(arguments.profile).mpi is not None:
            assert policy.mpi is not None
            control_fd = _open_control_source(policy.mpi.control_root, Path(arguments.control_source))
            descriptors.append(control_fd)
            argv += ["--dir", str(_CONTROL_DESTINATION), "--ro-bind-fd", str(control_fd), str(_CONTROL_DESTINATION)]
            argv += ["--setenv", "HTTK_DAEMON_MPI_HANDLE", arguments.handle]
            argv += ["--setenv", "HTTK_DAEMON_MPI_PROFILE", arguments.profile]

        if not host_view:
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


def _make_inheritable(descriptors: tuple[int, ...]) -> None:
    for descriptor in descriptors:
        # Not os.set_inheritable: its ioctl fails with EBADF on O_PATH descriptors, and
        # Pythons built without O_PATH also lack CPython's fcntl fallback for that.
        flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        fcntl.fcntl(descriptor, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)


def _exec_prepared(prepared: _PreparedSandbox) -> None:
    try:
        preserved = {0, 1, 2, *prepared.descriptors}
        for descriptor in _open_descriptor_numbers() - preserved:
            try:
                os.close(descriptor)
            except OSError:
                pass
        _make_inheritable(prepared.descriptors)
        os.execve(prepared.argv[0], prepared.argv, {})
    except BaseException:
        prepared.close()
        raise


def _run_prepared(prepared: _PreparedSandbox) -> int:
    """Run Bubblewrap as a child that keeps the descriptor numbers, forwarding SIGTERM and SIGINT to it."""

    _make_inheritable(prepared.descriptors)
    process = subprocess.Popen(prepared.argv, env={}, pass_fds=prepared.descriptors, close_fds=True)
    # ponytail: a signal before these handlers exist takes the default action; --die-with-parent then ends
    # Bubblewrap and that job's log stays uncopied, so block signals across the spawn if that ever matters.
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda received, _frame: process.send_signal(received))
    status = process.wait()
    return 128 - status if status < 0 else status


def _copy_job_log(policy: Any, handle: str) -> None:
    """Copy the bounded tail of this batch job's private Slurm output into the workspace, replacing any entry."""

    job_id = os.environ.get("SLURM_JOB_ID")
    if job_id is None or _JOB_ID.fullmatch(job_id) is None or _HANDLE.fullmatch(handle) is None:
        return
    try:
        source = os.open(policy.jobs / f"httk-{job_id}.out", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        try:
            information = os.fstat(source)
            if not stat.S_ISREG(information.st_mode):
                raise ValueError("job output is not a regular file")
            data = os.pread(source, _JOB_LOG_BYTES, max(0, information.st_size - _JOB_LOG_BYTES))
        finally:
            os.close(source)
        workspace = _open_directory_nofollow(policy.workspace)
        try:
            directory = os.open(
                ".httk-workspace", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=workspace
            )
        finally:
            os.close(workspace)
        try:
            temporary = f".daemon-job-{secrets.token_hex(16)}"
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory
            )
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as stream:
                    stream.write(data)
                # The payload may still race on this directory; rename replaces, and never follows, a planted entry.
                os.rename(temporary, f"daemon-job-{handle}.log", src_dir_fd=directory, dst_dir_fd=directory)
            except BaseException:
                try:
                    os.unlink(temporary, dir_fd=directory)
                except OSError:
                    pass
                raise
            finally:
                os.close(descriptor)
        finally:
            os.close(directory)
    except (OSError, ValueError) as exc:
        print(f"daemon bootstrap: could not copy the job log into the workspace: {exc}", file=sys.stderr, flush=True)


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
    parser.add_argument("--procs")
    parser.add_argument("--mem-mb")
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
            or arguments.procs is not None
            or arguments.mem_mb is not None
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
        # Only the protected MPI service, which already supplies --control-source, may state the capacity.
        if (arguments.mode != "payload" or profile.mpi is None) and (
            arguments.procs is not None or arguments.mem_mb is not None
        ):
            raise ValueError(f"{arguments.mode} mode forbids --procs and --mem-mb")
        if arguments.mode == "payload":
            if arguments.request_id is not None:
                raise ValueError("payload mode forbids --request-id")
            if profile.mpi is None:
                if arguments.control_source is not None:
                    raise ValueError("serial payload mode forbids --control-source")
                arguments.capacity = _allocation_capacity(profile)
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
                if type(arguments.procs) is not str:
                    raise ValueError("MPI payload mode requires --procs")
                mem_mb = None if arguments.mem_mb is None else _count(arguments.mem_mb, "--mem-mb", positive=True)
                arguments.capacity = (_count(arguments.procs, "--procs", positive=True), mem_mb)
        elif arguments.mode == "allocation":
            if profile.mpi is None or policy.mpi is None:
                raise ValueError("allocation mode requires an MPI profile")
            if arguments.control_source is not None or arguments.request_id is not None:
                raise ValueError("allocation mode creates its control source and forbids --request-id")
            arguments.allocation_identity = _allocation_identity(policy)
            arguments.capacity = _allocation_capacity(profile)
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
    """Validate policy and enter Bubblewrap.

    The Slurm batch-script modes, serial ``payload`` and ``allocation``, run Bubblewrap as a child and
    afterwards copy the job's Slurm output tail into the workspace; every other mode replaces this process.

    :param argv: Bootstrap arguments, or process arguments when omitted.
    :return: Bubblewrap's exit status in a batch-script mode, or two on a validation or startup refusal.
    """

    arguments = _parser().parse_args(sys.argv[1:] if argv is None else argv)
    # MPI payload managers are srun steps with a --control-source; only the batch script itself copies a log.
    batch_script = arguments.mode in ("payload", "allocation") and arguments.control_source is None
    policy = None
    status = 2
    try:
        policy_path = Path(arguments.policy)
        if not policy_path.is_absolute():
            raise ValueError("--policy must be an absolute local path")
        policy, policy_data, check_layout = _load_policy_once(policy_path)
        policy_source = _validate_arguments(arguments, policy)
        prepared = _prepare_sandbox(arguments, policy, policy_data, policy_source, check_layout)
        try:
            if batch_script:
                status = _run_prepared(prepared)
            else:
                _exec_prepared(prepared)
        finally:
            prepared.close()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"daemon bootstrap: {exc}", file=sys.stderr, flush=True)
    if batch_script and policy is not None and type(arguments.handle) is str:
        _copy_job_log(policy, arguments.handle)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
