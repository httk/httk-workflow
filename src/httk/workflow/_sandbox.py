"""Generic Bubblewrap helpers shared by the daemon bootstrap and attempt confinement.

The isolated daemon bootstrap (``python -I -S``) loads this file by path with :mod:`runpy` after a
protected-file check, so it uses the standard library only and no package-relative imports.
"""

import fcntl
import os
import re
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
BWRAP_USERNS_BLOCK = ("--disable-userns", "--assert-userns-disabled")
MERGED_USR_LINKS = (Path("/bin"), Path("/lib"), Path("/lib64"), Path("/sbin"))
_HELP_ENVIRONMENT = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}


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


def make_inheritable(descriptors: Iterable[int]) -> None:
    """Clear close-on-exec on descriptors, including ``O_PATH`` ones.

    :param descriptors: The descriptors.
    """

    for descriptor in descriptors:
        # Not os.set_inheritable: its ioctl fails with EBADF on O_PATH descriptors, and
        # Pythons built without O_PATH also lack CPython's fcntl fallback for that.
        flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        fcntl.fcntl(descriptor, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)
