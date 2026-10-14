"""Generic Bubblewrap helpers shared by the daemon bootstrap and attempt confinement.

The isolated daemon bootstrap (``python -I -S``) loads this file by path with :mod:`runpy` after a
protected-file check, so it uses the standard library only and no package-relative imports.
"""

import errno
import fcntl
import os
import re
import signal
import stat
import subprocess
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

# Conda-built Pythons may use glibc headers that predate O_PATH and memfd; the kernel ABI values are fixed.
O_PATH = getattr(os, "O_PATH", 0o10000000)
MFD_CLOEXEC = getattr(os, "MFD_CLOEXEC", 1)
MFD_ALLOW_SEALING = getattr(os, "MFD_ALLOW_SEALING", 2)
F_ADD_SEALS = getattr(fcntl, "F_ADD_SEALS", 1033)
# F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE (Linux ABI values 1, 2, 4, 8).
SEALS = sum(
    getattr(fcntl, name, value)
    for name, value in (("F_SEAL_SEAL", 1), ("F_SEAL_SHRINK", 2), ("F_SEAL_GROW", 4), ("F_SEAL_WRITE", 8))
)
BWRAP_REQUIRED = frozenset(
    {
        "--bind-fd",
        "--info-fd",
        "--new-session",
        "--ro-bind-data",
        "--ro-bind-fd",
        "--remount-ro",
        "--unshare-ipc",
        "--unshare-net",
        "--unshare-pid",
        "--unshare-user",
        "--unshare-uts",
    }
)
BWRAP_USERNS_BLOCK = ("--disable-userns", "--assert-userns-disabled")
MERGED_USR_LINKS = (Path("/bin"), Path("/lib"), Path("/lib64"), Path("/sbin"))
_HELP_ENVIRONMENT = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}


def open_regular_file_nofollow(dir_fd: int, name: str) -> int | None:
    """Open a regular file relative to a directory without following its final symlink.

    :param dir_fd: The parent directory descriptor.
    :param name: The file name relative to the parent.
    :return: A readable descriptor, or ``None`` when absent.
    :raises ValueError: If the entry is not a regular file.
    """

    try:
        descriptor = os.open(name, O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=dir_fd)
    except FileNotFoundError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{name} is not a regular file")
        readable = os.open(f"/proc/self/fd/{descriptor}", os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)
    except BaseException:
        os.close(descriptor)
        raise
    os.close(descriptor)
    return readable


def workspace_view(workspace_fd: int, workspace_root: Path) -> tuple[list[str], list[int]]:
    """Open and describe the three workspace paths visible in a confined job.

    :param workspace_fd: The workspace directory descriptor.
    :param workspace_root: The workspace's real path.
    :return: Bubblewrap options and the descriptors they name; the caller owns the descriptors.
    :raises ValueError: If a required path is absent, a symlink or the wrong kind.
    """

    if workspace_root in (Path("/"), Path("/tmp"), Path("/tmp/home")):
        raise ValueError(f"a confined workspace may not be rooted at {workspace_root}; confinement refused")

    descriptors: list[int] = []
    argv = ["--tmpfs", str(workspace_root)]
    try:
        for name in ("jobs", "workflows"):
            path = workspace_root / name
            try:
                descriptor = os.open(name, O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=workspace_fd)
            except OSError as exc:
                if exc.errno not in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
                    raise
                raise ValueError(f"{path} must be a real directory; confinement refused") from exc
            descriptors.append(descriptor)
            argv += ["--ro-bind-fd", str(descriptor), str(path)]

        control_path = workspace_root / ".httk-workspace"
        try:
            control_fd = os.open(
                ".httk-workspace", O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=workspace_fd
            )
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
                raise
            raise ValueError(f"{control_path} must be a real directory; confinement refused") from exc
        format_path = control_path / "format.json"
        try:
            try:
                format_fd = open_regular_file_nofollow(control_fd, "format.json")
            finally:
                os.close(control_fd)
        except ValueError as exc:
            raise ValueError(f"{format_path} must be a regular file; confinement refused") from exc
        if format_fd is None:
            raise ValueError(f"{format_path} must be a regular file; confinement refused")
        descriptors.append(format_fd)
        argv += ["--ro-bind-data", str(format_fd), str(format_path)]
        return argv, descriptors
    except BaseException:
        for descriptor in descriptors:
            os.close(descriptor)
        raise


@dataclass(slots=True)
class PreparedSandbox:
    """A Bubblewrap argument vector and the descriptors it names.

    :param argv: The Bubblewrap argument vector.
    :param descriptors: The descriptors the argument vector refers to, owned by this object.
    """

    argv: list[str]
    descriptors: tuple[int, ...]

    def close(self) -> None:
        """Close every owned descriptor once, ignoring close errors."""

        descriptors, self.descriptors = self.descriptors, ()
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


def check_bwrap(path: Path, *, environment: Mapping[str, str] | None = None) -> bool:
    """Check the Bubblewrap options from its help text.

    :param path: The Bubblewrap executable.
    :param environment: The environment of the help run, or a fixed minimal one when omitted.
    :return: Whether Bubblewrap lists the options that block nested user namespaces.
    :raises ValueError: If the help run fails or a required option is missing.
    """

    try:
        help_result = subprocess.run(
            [str(path), "--help"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=dict(_HELP_ENVIRONMENT if environment is None else environment),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("Bubblewrap feature check failed") from exc
    if help_result.returncode != 0:
        raise ValueError("Bubblewrap feature check failed")
    options = set(re.findall(r"--[A-Za-z0-9-]+", help_result.stdout + help_result.stderr))
    missing = sorted(BWRAP_REQUIRED - options)
    if missing:
        raise ValueError(f"Bubblewrap lacks required confinement features: {', '.join(missing)}")
    return all(option in options for option in BWRAP_USERNS_BLOCK)


def memfd_create(name: str) -> int:
    """Create a close-on-exec, sealable memory file.

    :param name: The memory file's debugging name.
    :return: The new descriptor.
    :raises RuntimeError: If the C library has no ``memfd_create``.
    """

    if hasattr(os, "memfd_create"):
        return os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    # Pythons built against old glibc headers lack the wrapper; the running libc usually has it.
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    try:
        function = libc.memfd_create
    except AttributeError as exc:
        raise RuntimeError("the daemon bootstrap requires Linux memfd support") from exc
    descriptor = int(function(name.encode(), MFD_CLOEXEC | MFD_ALLOW_SEALING))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


# ponytail: 434 and 424 cover x86_64 and Linux's generic syscall table; add an ABI map if other architectures arise.
def pidfd_open(pid: int) -> int:
    """Open a pidfd for a process.

    :param pid: The process identifier.
    :return: Its pidfd.
    :raises OSError: If the process cannot be opened.
    """

    if hasattr(os, "pidfd_open"):
        return os.pidfd_open(pid)
    import ctypes

    syscall = ctypes.CDLL(None, use_errno=True).syscall
    syscall.restype = ctypes.c_long
    descriptor = int(syscall(ctypes.c_long(434), ctypes.c_long(pid), ctypes.c_long(0)))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def pidfd_send_signal(pidfd: int, signum: int) -> None:
    """Send a signal through a pidfd.

    :param pidfd: The process descriptor.
    :param signum: The signal number.
    :return: ``None``.
    :raises OSError: If the process cannot be signalled.
    """

    if hasattr(signal, "pidfd_send_signal"):
        signal.pidfd_send_signal(pidfd, signum)
        return
    import ctypes

    syscall = ctypes.CDLL(None, use_errno=True).syscall
    syscall.restype = ctypes.c_long
    result = syscall(ctypes.c_long(424), ctypes.c_long(pidfd), ctypes.c_long(signum), None, ctypes.c_long(0))
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def merged_usr_symlinks(
    resolved_roots: tuple[Path, ...], declared: set[Path], links: tuple[Path, ...] = MERGED_USR_LINKS
) -> list[str]:
    """Return ``--symlink`` options recreating merged-``/usr`` links whose targets are bound.

    On merged-``/usr`` hosts ``/lib64`` and friends are symlinks into ``/usr``; a sandbox that mounts
    only ``/usr`` would otherwise miss the ELF loader (``/lib64/ld-linux-*.so.2``) and ``/bin/bash``.

    :param resolved_roots: The resolved read-only paths bound into the sandbox.
    :param declared: The declared read-only paths, which are bound themselves and need no link.
    :param links: The candidate link paths.
    :return: The Bubblewrap options.
    """

    argv: list[str] = []
    for link in links:
        if link in declared or not link.is_symlink():
            continue
        target = link.resolve()
        if any(target == root or target.is_relative_to(root) for root in resolved_roots):
            argv += ["--symlink", os.readlink(link), str(link)]
    return argv


def open_directory_nofollow(path: Path) -> int:
    """Open an absolute directory component-wise without following any symlink.

    :param path: The absolute directory path.
    :return: A close-on-exec read-only directory descriptor.
    """

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


def open_device_nofollow(path: Path) -> int:
    """Open a device node with ``O_PATH`` through a no-follow parent walk.

    :param path: The absolute device path.
    :return: A close-on-exec ``O_PATH`` descriptor of the device node.
    :raises ValueError: If the path is not a character or block device.
    """

    parent = open_directory_nofollow(path.parent)
    try:
        descriptor = os.open(path.name, O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent)
    finally:
        os.close(parent)
    information = os.fstat(descriptor)
    if not (stat.S_ISCHR(information.st_mode) or stat.S_ISBLK(information.st_mode)):
        os.close(descriptor)
        raise ValueError(f"approved device is not a device node: {path}")
    return descriptor


def device_parent_dirs(device: Path, created: set[Path]) -> list[str]:
    """Return the ``--dir`` options creating a device's missing parents on the private ``/dev``.

    :param device: The device path below ``/dev``.
    :param created: Parents already created; updated in place.
    :return: The Bubblewrap options, outermost parent first.
    """

    parent = device.parent
    parents: list[Path] = []
    while parent not in (Path("/dev"), Path("/")):
        parents.append(parent)
        parent = parent.parent
    argv: list[str] = []
    for destination in reversed(parents):
        if destination not in created:
            argv += ["--dir", str(destination)]
            created.add(destination)
    return argv


def device_and_proc_mounts(devices: Iterable[Path], descriptors: list[int]) -> list[str]:
    """Return the Bubblewrap options binding approved devices with device access, ending in the ``/proc`` mount.

    Bubblewrap has no descriptor form of ``--dev-bind``, and ``--bind-fd`` mounts ``nodev``, which refuses every
    open of a device. Each device is opened by :func:`open_device_nofollow` and bound with
    ``--dev-bind /proc/self/fd/N``, which mounts exactly the opened node without resolving its path again.
    Bubblewrap closes only the descriptors its ``--bind-fd`` options name, so each descriptor is also bound
    read-only and ``nodev`` below ``/proc``. The ``--proc /proc`` mount that ends the options hides those binds,
    and the sandboxed command inherits no descriptor of a host device. The options belong after ``--dev /dev``.

    :param devices: The approved device paths below ``/dev``.
    :param descriptors: Receives each opened descriptor; the caller owns and closes them.
    :return: The Bubblewrap options.
    :raises ValueError: If a path is not a character or block device.
    """

    argv: list[str] = []
    created: set[Path] = set()
    for index, device in enumerate(devices):
        argv += device_parent_dirs(device, created)
        descriptors.append(open_device_nofollow(device))
        argv += ["--dev-bind", f"/proc/self/fd/{descriptors[-1]}", str(device)]
        argv += ["--ro-bind-fd", str(descriptors[-1]), f"/proc/.httk-devices/{index}"]
    return [*argv, "--proc", "/proc"]


def make_inheritable(descriptors: Iterable[int]) -> None:
    """Clear close-on-exec on descriptors, including ``O_PATH`` ones.

    :param descriptors: The descriptors.
    """

    for descriptor in descriptors:
        # Not os.set_inheritable: its ioctl fails with EBADF on O_PATH descriptors, and
        # Pythons built without O_PATH also lack CPython's fcntl fallback for that.
        flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        fcntl.fcntl(descriptor, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)
