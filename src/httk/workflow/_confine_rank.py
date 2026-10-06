"""Rank helper of a confined launch: the trusted program the launch template starts on every rank.

Run as ``python -I -m httk.workflow._confine_rank --launch-dir T`` with ``T`` the manager-owned launch
directory ``<workspace>/.httk-workspace/managers/<manager_id>/launches/<attempt_id>.<request_id>``. Jobs see
``.httk-workspace`` read-only, so ``T/launch.json`` is trusted input; it is still read without following a
symlink below ``.httk-workspace`` and bounded.

The helper opens the workspace and the job directory, joins the per-launch shared-memory directory
``<confine.shm_root>/httk-<token>`` (the manager's random launch token; the last rank on a node removes
it), opens the PMIx directory of
the step when it lies below ``confine.pmix_roots`` and the approved devices, and runs the inner exec
(:mod:`httk.workflow._confine_exec`) in a Bubblewrap sandbox with host networking in which only the job
directory is writable. Bubblewrap runs as a child in its own process group, which every process in the
sandbox keeps; SIGTERM, SIGINT, SIGHUP and SIGCONT delivered to the helper are forwarded to that whole group,
and ``--die-with-parent`` ends the sandbox when the helper is killed. Under ``srun --mpi=pmi2`` Slurm's
``PMI_FD`` socket never enters the sandbox while the launch's ``manager.confine.block_mpi_spawn`` setting is in effect: the rank gets one
end of a socket pair as its ``PMI_FD``, and a :mod:`~httk.workflow._pmi_proxy` thread relays PMI-1 to Slurm
and refuses ``MPI_Comm_spawn``. The helper
exits with the sandbox's status, ``128+N`` for signal ``N``, and with 2 when it refuses.
"""

import argparse
import errno
import fcntl
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import FrameType

from . import _pmi_proxy
from ._jobdir import JobDirectory
from ._launch_protocol import (
    MAX_ENVIRONMENT_ENTRIES,
    MAX_ENVIRONMENT_VALUE_BYTES,
    MAX_TRUSTED_BYTES,
    TrustedLaunch,
    decode_trusted,
    is_environment_name,
    read_bounded,
    trusted_name,
)
from ._sandbox import (
    BWRAP_USERNS_BLOCK,
    O_PATH,
    PreparedSandbox,
    device_parent_dirs,
    make_inheritable,
    merged_usr_symlinks,
    open_device_nofollow,
    open_directory_nofollow,
)
from .errors import FormatError
from .models import (
    EXCHANGE_DIRECTORY,
    JOBS_DIRECTORY,
    LOGS_DIRECTORY,
    POSTPROCESS_DIRECTORY,
    check_job_placement,
    normalize_placement,
    parse_job_key,
)

#: The trusted launch description inside the launch directory.
LAUNCH_FILE = "launch.json"
_WORKSPACE_DIRECTORY = ".httk-workspace"
# The rank's process-manager variables, and the Slurm step identity the old daemon rank sandbox passed on.
_RANK_EXACT_ENVIRONMENT = frozenset(
    {"SLURM_PROCID", "SLURM_LOCALID", "SLURM_NODEID", "SLURM_NTASKS", "SLURM_JOB_ID", "SLURMD_NODENAME"}
)
_RANK_ENVIRONMENT_PREFIXES = ("PMI_", "PMIX_", "OMPI_", "OPAL_")
_SANDBOX_ENVIRONMENT = {"HOME": "/tmp/home", "TMPDIR": "/tmp"}
_FORWARDED_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGCONT)
_ANCHOR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
_DIRECTORY_FLAGS = _ANCHOR_FLAGS | os.O_NOFOLLOW
_LOCK_FLAGS = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_SHM_LOCK = ".lock"
_SHM_ATTEMPTS = 16
_GROUP_POLL_SECONDS = 0.05
_RELAY_JOIN_SECONDS = 1.0
# Destinations the sandbox owns: a PMIx directory must not be bound over or into them.
_RESERVED_DESTINATIONS = (Path("/proc"), Path("/dev"), Path("/tmp/home"))


def _close(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        pass


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _walk(anchor: int, parts: Sequence[str]) -> int:
    """Return a new descriptor for *parts* below *anchor*, opening every component without following it."""

    descriptor = os.dup(anchor)
    try:
        for part in parts:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def read_launch(launch_dir: Path) -> tuple[TrustedLaunch, Path, int]:
    """Read the trusted launch description and open its workspace.

    The workspace above ``.httk-workspace`` is operator layout and followed; the launch directory below it is
    opened without following any symlink. The description must name this launch directory's request and
    a workspace root that is the same directory.

    :param launch_dir: The absolute launch directory
        ``<workspace>/.httk-workspace/managers/<id>/launches/<attempt_id>.<request_id>``.
    :return: The description, the workspace's real path and a descriptor of the workspace, owned by the caller.
    :raises ValueError: If the path, the description or the workspace does not match.
    :raises OSError: If a path cannot be opened or read.
    """

    parts = launch_dir.parts
    if (
        not launch_dir.is_absolute()
        or ".." in parts
        or len(parts) < 6
        or parts[-5] != _WORKSPACE_DIRECTORY
        or parts[-4] != "managers"
        or parts[-2] != "launches"
    ):
        raise ValueError(
            f"--launch-dir must be <workspace>/{_WORKSPACE_DIRECTORY}/managers/<id>/launches/<attempt_id>.<request_id>"
        )
    workspace_fd = os.open(Path(*parts[:-5]), _ANCHOR_FLAGS)
    try:
        launch_fd = _walk(workspace_fd, parts[-5:])
        try:
            launch = decode_trusted(read_bounded(launch_fd, LAUNCH_FILE, MAX_TRUSTED_BYTES))
        finally:
            os.close(launch_fd)
        if trusted_name(launch.attempt_id, launch.request_id) != parts[-1]:
            raise ValueError("the trusted launch names another attempt or request than its directory")
        workspace = Path(os.path.realpath(launch.workspace_root))
        if not os.path.samestat(os.fstat(workspace_fd), os.stat(workspace)):
            raise ValueError(f"the trusted launch's workspace {launch.workspace_root} is not {Path(*parts[:-5])}")
    except BaseException:
        os.close(workspace_fd)
        raise
    return launch, workspace, workspace_fd


def open_job(launch: TrustedLaunch, workspace: Path) -> tuple[Path, int]:
    """Open the launch's job directory: placement directories followed, the job key and below not.

    :param launch: The trusted launch description.
    :param workspace: The workspace's real path.
    :return: The job directory's real path and a descriptor of it, owned by the caller.
    :raises ValueError: If the placement or job key is invalid, or the job directory is the workspace or
        lies in its control directory.
    :raises OSError: If the job directory cannot be opened.
    """

    try:
        placement = normalize_placement(launch.placement)
        check_job_placement(placement)
        parse_job_key(launch.job_key)
        with JobDirectory.open(jobs=workspace / JOBS_DIRECTORY, placement=placement, job_key=launch.job_key) as job:
            descriptor = os.dup(job.fd)
            shown = job.path
    except FormatError as exc:
        raise ValueError(str(exc)) from exc
    try:
        real = Path(os.path.realpath(shown))
        if not os.path.samestat(os.fstat(descriptor), os.stat(real)):
            raise ValueError(f"the job directory {shown} changed while it was opened")
        # Placement directories may be symlinks anywhere, so no positive "below jobs/" check.
        refused = (_WORKSPACE_DIRECTORY, EXCHANGE_DIRECTORY, LOGS_DIRECTORY, POSTPROCESS_DIRECTORY)
        if real == workspace or any(real.is_relative_to(workspace / name) for name in refused):
            raise ValueError(f"the job directory {real} is the workspace or lies in its control directory")
    except BaseException:
        os.close(descriptor)
        raise
    return real, descriptor


@dataclass(slots=True)
class SharedMemory:
    """A joined per-launch shared-memory directory and the shared lock that marks this rank's use.

    :param root_fd: A descriptor of ``confine.shm_root``.
    :param name: The directory's name, ``httk-<token>``.
    :param fd: A descriptor of the directory.
    :param lock_fd: A descriptor of its ``.lock`` file, holding ``flock(LOCK_SH)``.
    """

    root_fd: int
    name: str
    fd: int
    lock_fd: int


def _check_shm_root(descriptor: int, path: Path) -> None:
    information = os.fstat(descriptor)
    mode = stat.S_IMODE(information.st_mode)
    if not stat.S_ISDIR(information.st_mode) or information.st_uid not in (0, os.geteuid()):
        raise ValueError(f"confine.shm_root {path} must be a directory owned by root or this user")
    if mode & 0o002 and not (information.st_uid == 0 and mode & stat.S_ISVTX):
        raise ValueError(f"confine.shm_root {path} is world-writable without being a sticky root directory")


def _linked(root_fd: int, name: str, descriptor: int) -> bool:
    try:
        return os.path.samestat(os.stat(name, dir_fd=root_fd, follow_symlinks=False), os.fstat(descriptor))
    except OSError:
        return False


def join_shared_memory(shm_root: Path, token: str) -> SharedMemory:
    """Create or join the per-launch shared-memory directory and take a shared lock in it.

    An existing directory must be a real directory owned by this user with mode 0700. A directory the
    last rank removed while this one waited for the lock is created afresh.

    :param shm_root: The node-local parent, ``confine.shm_root``.
    :param token: The launch token of the trusted launch description.
    :return: The joined directory; release it with :func:`leave_shared_memory`.
    :raises ValueError: If the root or the directory is unsafe.
    :raises OSError: If the directory cannot be created, opened or locked.
    """

    name = f"httk-{token}"
    root_fd = open_directory_nofollow(shm_root)
    try:
        _check_shm_root(root_fd, shm_root)
        for _attempt in range(_SHM_ATTEMPTS):
            created = True
            try:
                os.mkdir(name, 0o700, dir_fd=root_fd)
            except FileExistsError:
                created = False
            try:
                descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=root_fd)
            except FileNotFoundError:
                continue
            except OSError as exc:
                if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise ValueError(f"shared-memory directory {shm_root / name} is not a real directory") from exc
                raise
            lock_fd = -1
            try:
                if created:
                    os.fchmod(descriptor, 0o700)
                information = os.fstat(descriptor)
                if information.st_uid != os.geteuid() or stat.S_IMODE(information.st_mode) != 0o700:
                    raise ValueError(
                        f"shared-memory directory {shm_root / name} must be owned by this user with mode 0700"
                    )
                try:
                    lock_fd = os.open(_SHM_LOCK, _LOCK_FLAGS, 0o600, dir_fd=descriptor)
                except FileNotFoundError:
                    os.close(descriptor)
                    continue
                if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
                    raise ValueError(f"shared-memory lock {shm_root / name / _SHM_LOCK} is not a regular file")
                fcntl.flock(lock_fd, fcntl.LOCK_SH)
            except BaseException:
                if lock_fd >= 0:
                    _close(lock_fd)
                _close(descriptor)
                raise
            if _linked(root_fd, name, descriptor) and _linked(descriptor, _SHM_LOCK, lock_fd):
                return SharedMemory(root_fd, name, descriptor, lock_fd)
            _close(lock_fd)
            _close(descriptor)
        raise ValueError(f"cannot join the shared-memory directory {shm_root / name}: it keeps being removed")
    except BaseException:
        _close(root_fd)
        raise


def leave_shared_memory(shared: SharedMemory) -> None:
    """Release a joined shared-memory directory, removing it when no other rank on the node holds it.

    :param shared: The joined directory; its descriptors are closed.
    """

    try:
        try:
            fcntl.flock(shared.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return
        if _linked(shared.root_fd, shared.name, shared.fd):
            try:
                shutil.rmtree(shared.name, dir_fd=shared.root_fd)
            except OSError:
                pass
    finally:
        for descriptor in (shared.lock_fd, shared.fd, shared.root_fd):
            _close(descriptor)


def rank_environment(environ: Mapping[str, str], launch: TrustedLaunch) -> dict[str, str]:
    """Return the environment of a rank sandbox.

    It holds the rank's ``PMI_*``, ``PMIX_*``, ``OMPI_*`` and ``OPAL_*`` variables and its Slurm step
    identity (``SLURM_PROCID``, ``SLURM_LOCALID``, ``SLURM_NODEID``, ``SLURM_NTASKS``, ``SLURM_JOB_ID``,
    ``SLURMD_NODENAME``), then ``confine.environment``, then the sandbox's ``HOME`` and ``TMPDIR``.

    :param environ: The helper's environment, as the launcher set it.
    :param launch: The trusted launch description.
    :return: The rank sandbox environment.
    :raises ValueError: If a captured variable has an invalid name or an oversized value, or too many are set.
    """

    captured: dict[str, str] = {}
    for name, value in environ.items():
        if name not in _RANK_EXACT_ENVIRONMENT and not name.startswith(_RANK_ENVIRONMENT_PREFIXES):
            continue
        if not is_environment_name(name):
            raise ValueError(f"invalid rank environment name {name!r}")
        if "\0" in value or len(value.encode("utf-8", errors="surrogateescape")) > MAX_ENVIRONMENT_VALUE_BYTES:
            raise ValueError(
                f"rank environment value of {name} is invalid or exceeds {MAX_ENVIRONMENT_VALUE_BYTES} bytes"
            )
        captured[name] = value
    if len(captured) > MAX_ENVIRONMENT_ENTRIES:
        raise ValueError(f"the rank environment has more than {MAX_ENVIRONMENT_ENTRIES} process-manager variables")
    return captured | dict(launch.confine.environment) | _SANDBOX_ENVIRONMENT


def _resolve(path: Path, what: str) -> Path:
    try:
        return path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"{what} is unavailable: {path}") from exc


def open_pmix_directory(
    environment: Mapping[str, str], launch: TrustedLaunch, *, workspace: Path, job: Path
) -> tuple[Path, int] | None:
    """Open the step's PMIx directory named by ``PMIX_SERVER_TMPDIR``, when set.

    It must resolve strictly below one of ``confine.pmix_roots``, must not overlap the workspace, the job
    directory, the shared-memory root, a read-only path or a sandbox-owned destination, and must be a
    directory of this user that is not world-writable, reached without following a symlink.

    :param environment: The rank environment.
    :param launch: The trusted launch description.
    :param workspace: The workspace's real path.
    :param job: The job directory's real path.
    :return: The declared path and a descriptor of it owned by the caller, or ``None`` without a PMIx directory.
    :raises ValueError: If the directory is not approved or unsafe.
    :raises OSError: If it cannot be opened.
    """

    raw = environment.get("PMIX_SERVER_TMPDIR")
    if raw is None:
        return None
    path = Path(raw)
    if not raw or "\0" in raw or not path.is_absolute() or ".." in path.parts:
        raise ValueError("PMIX_SERVER_TMPDIR must be an absolute path without '..' or NUL")
    resolved = _resolve(path, "PMIX_SERVER_TMPDIR")
    roots = tuple(_resolve(root, "confine.pmix_roots entry") for root in launch.confine.pmix_roots)
    if not any(resolved != root and resolved.is_relative_to(root) for root in roots):
        raise ValueError(
            f"PMIX_SERVER_TMPDIR {path} is not below an approved PMIx root; add its parent to confine.pmix_roots"
        )
    if path in (Path("/"), Path("/tmp")) or any(_overlap(path, item) for item in _RESERVED_DESTINATIONS):
        raise ValueError(f"PMIX_SERVER_TMPDIR {path} overlaps a sandbox-owned path")
    forbidden = [workspace, job, launch.confine.shm_root]
    for item in launch.confine.readonly_paths:
        forbidden += [item, _resolve(item, "confine.readonly_paths entry")]
    if any(_overlap(candidate, item) for candidate in (path, resolved) for item in forbidden):
        raise ValueError(
            f"PMIX_SERVER_TMPDIR {path} overlaps the workspace, the job, confine.shm_root or confine.readonly_paths"
        )
    descriptor = open_directory_nofollow(path)
    try:
        information = os.fstat(descriptor)
        if information.st_uid != os.geteuid() or stat.S_IMODE(information.st_mode) & 0o002:
            raise ValueError(f"PMIX_SERVER_TMPDIR {path} must be owned by this user and not world-writable")
    except BaseException:
        os.close(descriptor)
        raise
    return path, descriptor


def blocks_mpi_spawn(launch: TrustedLaunch, environ: Mapping[str, str]) -> bool:
    """Return whether the rank's PMI goes through the spawn-refusing relay.

    :param launch: The trusted launch description, whose ``confine.block_mpi_spawn`` is ``on``, ``off`` or
        ``auto`` (``on`` exactly when ``PMI_FD`` is set).
    :param environ: The helper's environment, as the launcher set it.
    :return: Whether to relay ``PMI_FD`` and drop ``PMI_PORT``.
    """

    mode = launch.confine.block_mpi_spawn
    return mode == "on" or (mode == "auto" and bool(environ.get("PMI_FD")))


def slurm_pmi_fd(environ: Mapping[str, str]) -> int | None:
    """Return the descriptor number of Slurm's ``PMI_FD``, to pass it to the rank unrelayed.

    :param environ: The helper's environment, as the launcher set it.
    :return: The descriptor number, or ``None`` when ``PMI_FD`` is unset or empty.
    :raises ValueError: If ``PMI_FD`` is not the decimal number of an open descriptor.
    """

    raw = environ.get("PMI_FD", "")
    if not raw:
        return None
    if not (len(raw) <= 9 and raw.isascii() and raw.isdigit()):
        raise ValueError(f"PMI_FD {raw!r} is not a descriptor number")
    try:
        os.fstat(int(raw))
    except OSError:
        raise ValueError(f"PMI_FD {raw!r} is not an open descriptor") from None
    return int(raw)


def open_pmi_relay(environ: Mapping[str, str]) -> tuple[int, int, int] | None:
    """Return ``(server, relay_end, rank_end)`` for the rank's ``PMI_FD``, or ``None`` without one.

    ``server`` is Slurm's socket named by ``PMI_FD``, made close-on-exec so that no child inherits it;
    ``relay_end`` and ``rank_end`` are the two close-on-exec ends of a new Unix stream socket pair, the
    first for :func:`httk.workflow._pmi_proxy.relay` and the second for the sandbox. The caller owns all three.

    :param environ: The helper's environment, as the launcher set it.
    :return: The three descriptors, or ``None`` when ``PMI_FD`` is unset or empty.
    :raises ValueError: If ``PMI_FD`` is not the decimal number of an open socket.
    :raises OSError: If the socket pair cannot be created.
    """

    raw = environ.get("PMI_FD", "")
    if not raw:
        return None
    mode = 0
    if len(raw) <= 9 and raw.isascii() and raw.isdigit():
        try:
            mode = os.fstat(int(raw)).st_mode
        except OSError:
            pass
    if not stat.S_ISSOCK(mode):
        raise ValueError(f"PMI_FD {raw!r} is not an open socket")
    server = int(raw)
    os.set_inheritable(server, False)
    relay_end, rank_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    return server, relay_end.detach(), rank_end.detach()


def _pmix_parent_dirs(path: Path) -> list[str]:
    parents: list[Path] = []
    parent = path.parent
    while parent not in (Path("/"), Path("/tmp")):
        parents.append(parent)
        parent = parent.parent
    return [option for destination in reversed(parents) for option in ("--dir", str(destination))]


def inner_command(launch: TrustedLaunch, job: Path) -> list[str]:
    """Return the inner exec command run inside the rank sandbox.

    :param launch: The trusted launch description.
    :param job: The job directory's real path, at which the sandbox binds it.
    :return: The command.
    """

    return [
        str(launch.python),
        "-I",
        "-m",
        "httk.workflow._confine_exec",
        "--request",
        str(job / launch.request),
        "--request-id",
        launch.request_id,
        "--attempt-id",
        launch.attempt_id,
    ]


def prepare_rank_sandbox(
    launch: TrustedLaunch,
    *,
    workspace: Path,
    workspace_fd: int,
    job: Path,
    job_fd: int,
    shm_fd: int,
    environ: Mapping[str, str],
    pmi_fd: int | None = None,
) -> tuple[PreparedSandbox, dict[str, str]]:
    """Build the Bubblewrap argument vector, descriptor set and environment of one rank sandbox.

    The sandbox unshares the user, PID, IPC and UTS namespaces but keeps the host network, dies with its
    helper (``--die-with-parent``) and drops all capabilities. Its environment is not in the argument
    vector, which any user can read in ``/proc/<pid>/cmdline``: Bubblewrap is started with exactly the
    rank environment and passes it through. It binds the read-only paths, the workspace read-only and the
    job directory writable at their real paths, a private ``/proc`` and ``/dev`` with the shared-memory
    directory at ``/dev/shm``, the PMIx directory at its own path and the approved devices, and runs
    :func:`inner_command` in the job directory.

    :param launch: The trusted launch description.
    :param workspace: The workspace's real path.
    :param workspace_fd: A descriptor of the workspace; duplicated, not consumed.
    :param job: The job directory's real path.
    :param job_fd: A descriptor of the job directory; duplicated, not consumed.
    :param shm_fd: A descriptor of the per-launch shared-memory directory; duplicated, not consumed.
    :param environ: The helper's environment, as the launcher set it.
    :param pmi_fd: The rank's PMI descriptor, consumed: it is among the returned descriptors, and closed here
        on failure. When :func:`blocks_mpi_spawn` holds, it is the sandbox end from :func:`open_pmi_relay`,
        which replaces ``PMI_FD``, and ``PMI_PORT`` is dropped; otherwise it is Slurm's own ``PMI_FD``.
    :return: The complete argument vector with the inheritable descriptors to pass with ``pass_fds``, which
        the caller closes after the spawn, and the whole environment to start Bubblewrap with.
    :raises ValueError: If a path or the environment is not acceptable.
    :raises OSError: If a path cannot be opened.
    """

    confine = launch.confine
    descriptors: list[int] = [] if pmi_fd is None else [pmi_fd]
    try:
        environment = rank_environment(environ, launch)
    except BaseException:
        PreparedSandbox([], tuple(descriptors)).close()
        raise
    if blocks_mpi_spawn(launch, environ):
        environment.pop("PMI_PORT", None)
        if pmi_fd is not None:
            environment["PMI_FD"] = str(pmi_fd)
    argv = [str(confine.bwrap), "--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts"]
    if confine.block_userns:
        argv += BWRAP_USERNS_BLOCK
    # Unlike an attempt, a rank never outlives its helper, which holds its shared memory and is its only reaper.
    argv += ["--die-with-parent", "--cap-drop", "ALL"]
    argv += ["--tmpfs", "/tmp", "--dir", "/tmp/home"]
    try:
        resolved: list[Path] = []
        for path in confine.readonly_paths:
            real = _resolve(path, "confine.readonly_paths entry")
            if any(item.is_relative_to(workspace) for item in (path, real)):
                raise ValueError(f"confine.readonly_paths entry {path} lies inside the workspace {workspace}")
            descriptors.append(os.open(real, O_PATH | os.O_CLOEXEC))
            argv += ["--ro-bind-fd", str(descriptors[-1]), str(path)]
            resolved.append(real)
        argv += merged_usr_symlinks(tuple(resolved), set(confine.readonly_paths))
        descriptors.append(os.dup(workspace_fd))
        argv += ["--ro-bind-fd", str(descriptors[-1]), str(workspace)]
        descriptors.append(os.dup(job_fd))
        argv += ["--bind-fd", str(descriptors[-1]), str(job)]
        argv += ["--proc", "/proc", "--dev", "/dev"]
        descriptors.append(os.dup(shm_fd))
        argv += ["--bind-fd", str(descriptors[-1]), "/dev/shm"]
        pmix = open_pmix_directory(environment, launch, workspace=workspace, job=job)
        if pmix is not None:
            pmix_path, pmix_fd = pmix
            descriptors.append(pmix_fd)
            argv += _pmix_parent_dirs(pmix_path)
            argv += ["--bind-fd", str(pmix_fd), str(pmix_path)]
        created: set[Path] = set()
        for device in confine.devices:
            argv += device_parent_dirs(device, created)
            descriptors.append(open_device_nofollow(device))
            argv += ["--bind-fd", str(descriptors[-1]), str(device)]
        argv += ["--chdir", str(job), "--", *inner_command(launch, job)]
        make_inheritable(descriptors)
        return PreparedSandbox(argv, tuple(descriptors)), environment
    except BaseException:
        PreparedSandbox([], tuple(descriptors)).close()
        raise


def _group_alive(group: int) -> bool:
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _log_pmi(line: str) -> None:
    print(f"httk-workflow rank: PMI {line}", file=sys.stderr, flush=True)


def _run(prepared: PreparedSandbox, environment: Mapping[str, str], pmi: tuple[int, int] | None = None) -> int:
    """Run Bubblewrap in its own process group, forward signals to the group, and return its status.

    With *pmi*, ``(server, relay_end)`` from :func:`open_pmi_relay`, a daemon thread relays PMI-1 between
    them once Bubblewrap runs and the helper has closed its copy of the sandbox end; *pmi* is consumed. If
    the thread cannot start, the rank's process group is killed and the launch is refused with status 2.

    Bubblewrap does not forward signals into its namespace, and a launcher such as ``mpirun`` signals only
    its direct children (the helpers), so the helper signals the whole group: the rank sandbox has no
    ``--new-session``, so every process inside keeps the group. The helper returns only after the group is
    gone, so the shared-memory directory is never removed under a live rank.
    """

    group: int | None = None
    pending: list[int] = []

    def forward(signum: int, _frame: FrameType | None) -> None:
        if group is None:
            pending.append(signum)
            return
        try:
            os.killpg(group, signum)
        except OSError:
            pass

    for signum in _FORWARDED_SIGNALS:
        signal.signal(signum, forward)
    try:
        process = subprocess.Popen(prepared.argv, env=dict(environment), pass_fds=prepared.descriptors, process_group=0)
    except BaseException:
        for descriptor in pmi or ():
            _close(descriptor)
        raise
    prepared.close()
    group = process.pid
    relay: threading.Thread | None = None
    refused = False
    if pmi is not None:
        relay = threading.Thread(
            target=_pmi_proxy.relay, args=pmi, kwargs={"log": _log_pmi}, daemon=True, name="pmi-relay"
        )
        try:
            relay.start()
        except Exception as exc:  # the rank must never run without its relay
            relay = None
            refused = True
            try:
                os.killpg(group, signal.SIGKILL)
            except OSError:
                pass
            for descriptor in pmi:
                _close(descriptor)
            print(f"httk-workflow rank: refused: cannot start the PMI relay: {exc}", file=sys.stderr, flush=True)
    for received in pending:
        forward(received, None)
    status = process.wait()
    while _group_alive(group):
        time.sleep(_GROUP_POLL_SECONDS)
    if relay is not None:
        relay.join(_RELAY_JOIN_SECONDS)  # the rank end is gone; a daemon thread still blocked dies with the helper
    if refused:
        return 2
    return 128 - status if status < 0 else status


def main(argv: Sequence[str] | None = None) -> int:
    """Run one rank of a confined launch in its Bubblewrap sandbox.

    :param argv: The helper arguments, or the process arguments when omitted.
    :return: The sandbox's exit status, ``128+N`` for signal ``N``, or 2 when the launch is refused.
    """

    parser = argparse.ArgumentParser(prog="python -I -m httk.workflow._confine_rank", description=__doc__)
    parser.add_argument("--launch-dir", required=True, type=Path)
    arguments = parser.parse_args(argv)
    descriptors: list[int] = []
    relay_descriptors: list[int] = []  # (server, relay_end) until handed to _run
    shared: SharedMemory | None = None
    try:
        try:
            launch, workspace, workspace_fd = read_launch(arguments.launch_dir)
            descriptors.append(workspace_fd)
            job, job_fd = open_job(launch, workspace)
            descriptors.append(job_fd)
            shared = join_shared_memory(launch.confine.shm_root, launch.token)
            pmi_fd = None
            if blocks_mpi_spawn(launch, os.environ):
                pmi = open_pmi_relay(os.environ)
                if pmi is not None:
                    relay_descriptors += pmi[:2]
                    pmi_fd = pmi[2]
            else:
                pmi_fd = slurm_pmi_fd(os.environ)
            prepared, environment = prepare_rank_sandbox(
                launch,
                workspace=workspace,
                workspace_fd=workspace_fd,
                job=job,
                job_fd=job_fd,
                shm_fd=shared.fd,
                environ=os.environ,
                pmi_fd=pmi_fd,
            )
        except (OSError, ValueError) as exc:
            print(f"httk-workflow rank: refused: {exc}", file=sys.stderr, flush=True)
            return 2
        for descriptor in descriptors:
            _close(descriptor)
        descriptors.clear()
        relay_pair = (relay_descriptors[0], relay_descriptors[1]) if relay_descriptors else None
        relay_descriptors.clear()
        try:
            return _run(prepared, environment, relay_pair)
        except OSError as exc:
            prepared.close()
            print(
                f"httk-workflow rank: cannot run Bubblewrap {launch.confine.bwrap}: {exc}", file=sys.stderr, flush=True
            )
            return 2
    finally:
        for descriptor in descriptors + relay_descriptors:
            _close(descriptor)
        if shared is not None:
            leave_shared_memory(shared)


if __name__ == "__main__":
    raise SystemExit(main())
