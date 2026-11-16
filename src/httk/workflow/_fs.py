"""Filesystem primitives: the only module that renames, replaces, unlinks or exclusively creates.

Every outcome is decided by observation (``lstat`` of the source and the
destination), never by a return code: NFS can report an error for a rename that
happened (a retransmission), and ``ENOENT`` can come from a concurrently pruned
parent directory. Only atomic rename and basic POSIX are used: no file locks,
no hard links, and no symlink is created except where a trusted :func:`copy_tree`
copies one as a symlink and where :func:`extract_tar` extracts a symlink member
of an untrusted tar stream (its text kept, never followed). Plain :class:`Loc`
values address trusted directories; anchored ones address a name below a
directory descriptor that an untrusted party may write; :func:`extract_tar`
reaches every member of a stream through such descriptors.
"""

import base64
import enum
import errno
import hashlib
import logging
import os
import re
import shutil
import stat
import sys
import tarfile
from collections import Counter, OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import cache, partial
from pathlib import Path, PurePosixPath
from typing import IO, Literal, Protocol, Self

from httk.workflow._pool import Feed
from httk.workflow.errors import FormatError, WorkflowError

__all__ = [
    "DEFAULT_LIMITS",
    "STAGING_MARK",
    "SYNC_SETTINGS",
    "Barrier",
    "CrossDevice",
    "Delivered",
    "DeliveryUncertain",
    "Entry",
    "Fault",
    "FilesystemUnsupported",
    "Loc",
    "MoveFailed",
    "Moved",
    "SyncFailed",
    "SyncStrategy",
    "SyncUnreadable",
    "TooLarge",
    "UnsafePath",
    "UntrustedContentError",
    "WalkLimits",
    "anchored",
    "carries",
    "copy_file",
    "copy_tree",
    "create_exclusive",
    "deliver",
    "digest_regular",
    "discard",
    "exists",
    "extract_tar",
    "fresh_token",
    "fsync_landing",
    "loc",
    "lstat",
    "make_dirs",
    "move_once",
    "move_owned",
    "open_append",
    "open_dir",
    "open_dir_under",
    "publish_dir",
    "publish_record",
    "read_bounded",
    "read_tail",
    "remove_empty_dir",
    "remove_file",
    "rename_probe",
    "resolve_sync",
    "set_fault_injector",
    "sync_written",
    "walk_untrusted",
    "write_all",
    "write_file",
]

_LOGGER = logging.getLogger(__name__)

_ATTEMPTS = 8
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_OCCUPIED_ERRNOS = frozenset({errno.EEXIST, errno.ENOTEMPTY, errno.ENOTDIR, errno.EISDIR})
#: Marks the dot-named staging directory of :func:`publish_record`: ``.<name>.stage-<token>``.
STAGING_MARK = ".stage-"


class CrossDevice(WorkflowError):
    """A rename crossed a filesystem boundary (``EXDEV``); it is never turned into a copy."""


class MoveFailed(WorkflowError):
    """A move could not be made: the source stayed in place after every attempt.

    :param message: Human-readable failure description.
    :param error_number: The ``errno`` of the last failed attempt, if one was reported.
    """

    def __init__(self, message: str, error_number: int | None = None) -> None:
        super().__init__(message)
        self.errno = error_number


class DeliveryUncertain(WorkflowError):
    """A delivered source is gone, but the destination does not carry the caller's token."""


class UnsafePath(WorkflowError):
    """A path is a symlink, a special file or otherwise not the plain entry expected.

    :param message: Human-readable failure description.
    :param kind: ``"symlink"`` for a symlink where a file was opened, ``"not_regular"`` for a special file or a
        directory where a regular file was expected, ``"not_directory"`` for a component that is a symlink or
        not a directory where a directory was opened (Linux does not tell those two apart).
    """

    def __init__(self, message: str, kind: Literal["symlink", "not_regular", "not_directory"]) -> None:
        super().__init__(message)
        self.kind = kind


class TooLarge(FormatError):
    """A bounded read found more content than its limit."""


class UntrustedContentError(FormatError):
    """An untrusted tree holds an entry that validation refuses.

    :param relative_path: The refused entry, relative to the walked root.
    :param reason: Why the entry is refused.
    """

    def __init__(self, relative_path: PurePosixPath, reason: str) -> None:
        super().__init__(f"{relative_path}: {reason}")
        self.relative_path = relative_path
        self.reason = reason


class FilesystemUnsupported(WorkflowError):
    """The filesystem lacks the rename semantics the workspace protocol relies on."""


def _check_name(name: str) -> None:
    if name in ("", ".", "..") or "/" in name or "\0" in name:
        raise ValueError(f"not a plain entry name: {name!r}")


@dataclass(frozen=True)
class Loc:
    """A filesystem location: an absolute path, or one plain name below a directory descriptor.

    :param path: An absolute path, or one plain name when *at* is given.
    :param at: A directory descriptor opened with ``O_DIRECTORY|O_NOFOLLOW`` that *path* is relative to.
    """

    path: Path
    at: int | None = None

    def __post_init__(self) -> None:
        if self.at is not None:
            _check_name(str(self.path))
        elif not self.path.is_absolute():
            raise ValueError(f"a plain location must be absolute: {self.path}")

    def name(self) -> str:
        """Return the final name of the location."""

        return self.path.name

    def parent(self) -> "Loc":
        """Return the plain location of the containing directory.

        :return: The parent location.
        :raises ValueError: For an anchored location, whose parent is only a descriptor.
        """

        if self.at is not None:
            raise ValueError("an anchored location has no parent path")
        return Loc(self.path.parent)


def loc(path: Path) -> Loc:
    """Return the plain location of an absolute path.

    :param path: The absolute path.
    :return: The location.
    """

    return Loc(Path(path))


def anchored(at: int, name: str) -> Loc:
    """Return the location of one plain name below a directory descriptor.

    :param at: The directory descriptor.
    :param name: One entry name; ``""``, ``"."``, ``".."`` and names with ``/`` or NUL are refused.
    :return: The anchored location.
    """

    _check_name(name)
    return Loc(Path(name), at)


def open_dir(path: Path) -> int:
    """Open a directory descriptor for anchoring, refusing a symlink at *path*.

    :param path: The directory to open.
    :return: A descriptor opened ``O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC``; the caller closes it.
    :raises UnsafePath: When *path* is a symlink or not a directory.
    """

    try:
        return os.open(path, _DIR_FLAGS)
    except OSError as exc:
        # Linux reports a symlink opened O_DIRECTORY|O_NOFOLLOW as ENOTDIR, not ELOOP.
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise UnsafePath(f"{path} is a symlink or not a directory", "not_directory") from exc
        raise


def open_dir_under(
    root: Path | int, relative: str | PurePosixPath, *, create: bool = False, mode: int = 0o700, durable: bool = False
) -> int:
    """Open a directory below a trusted root, never following a symlink below the root.

    *root* is a trusted anchor and is followed; each component of *relative* is
    opened ``O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`` relative to the previous one.
    No ``O_NONBLOCK`` is needed: ``O_DIRECTORY`` refuses a FIFO before an open could wait on it.

    :param root: The trusted directory to start from: a path, or an open directory descriptor (left open).
    :param relative: The directory below *root*; ``..``, absolute paths and empty components are refused.
    :param create: Create a missing component with *mode*, like ``mkdir(parents=True)``.
    :param mode: The permission bits of a created directory (before the umask).
    :param durable: Fsync the parent of every directory created.
    :return: The descriptor of the final directory; the caller closes it.
    :raises ValueError: For an invalid *relative*.
    :raises UnsafePath: When a component is a symlink or not a directory.
    :raises FileNotFoundError: When a component is missing and *create* is false.
    """

    parts = relative.parts if isinstance(relative, PurePosixPath) else tuple(relative.split("/"))
    if not parts:
        raise ValueError(f"not a relative directory path: {relative!r}")
    for part in parts:
        _check_name(part)
    if isinstance(root, int):
        descriptor = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC, dir_fd=root)
    else:
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for name in parts:
            if create:
                try:
                    os.mkdir(name, mode, dir_fd=descriptor)
                    if durable:
                        os.fsync(descriptor)
                except FileExistsError:
                    pass
            try:
                child = os.open(name, _DIR_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                # Linux reports a symlink opened O_DIRECTORY|O_NOFOLLOW as ENOTDIR, not ELOOP.
                if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise UnsafePath(
                        f"{root}/{relative}: {name} is a symlink or not a directory", "not_directory"
                    ) from exc
                raise
            descriptor, previous = child, descriptor
            os.close(previous)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


class Fault(Protocol):
    """A test hook called at the documented crash points of this module."""

    def __call__(self, op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        """Observe or interrupt one operation; raising simulates a crash at that point.

        :param op: ``"rename"``, ``"write"``, ``"publish"`` or ``"remove"``.
        :param phase: ``"before"``, ``"after"`` or ``"before_replace"``.
        :param src: The operation's source, if any.
        :param dst: The operation's destination, if any.
        """


_fault: Fault | None = None


def set_fault_injector(fault: Fault | None) -> None:
    """Install (or with ``None`` remove) the fault injector; for tests only.

    :param fault: The hook, or ``None``.
    """

    global _fault
    _fault = fault


class Moved(enum.Enum):
    """The outcome of a contested move."""

    WON = "won"
    LOST = "lost"


class Delivered(enum.Enum):
    """The outcome of a delivery to a name that is not unique to the caller."""

    DONE = "done"
    OCCUPIED = "occupied"


def lstat(target: Loc) -> os.stat_result | None:
    """Observe an entry without following a symlink at its final name.

    A plain *target* follows symlinks in its intermediate components; anchor a name below a directory an
    untrusted party may write.

    :param target: The location to observe.
    :return: Its ``lstat`` result, or ``None`` when no entry has the name (or a component is not a directory).
    """

    try:
        return os.lstat(target.path, dir_fd=target.at)
    except (FileNotFoundError, NotADirectoryError):
        return None


def exists(target: Loc) -> bool:
    """Report whether an entry exists, without following a symlink at its final name (see :func:`lstat`).

    :param target: The location to observe.
    :return: Whether ``lstat`` finds an entry.
    """

    return lstat(target) is not None


def fresh_token() -> str:
    """Return 80 random bits as 16 lowercase base32 characters."""

    return base64.b32encode(os.urandom(10)).decode("ascii").lower()


def _fsync_parent(target: Loc) -> None:
    if target.at is not None:
        os.fsync(target.at)
        return
    try:
        # Following a symlink is harmless here: fsync changes nothing.
        descriptor = os.open(target.parent().path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except FileNotFoundError:
        # A pruner removed the parent: the removal implies the entry left it.
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_parents(src: Loc, dst: Loc) -> None:
    # Durable mode: both directories whose entries a move changed.
    _fsync_parent(src)
    _fsync_parent(dst)


def make_dirs(path: Path, *, durable: bool, mode: int = 0o777) -> None:
    """Create *path* and its missing ancestors as real directories.

    A concurrent creation of the same directory is success. An ancestor removed
    concurrently (by a pruner) makes the walk start again. Only the components
    this call creates and the deepest existing ancestor are checked: a symlink
    further up is followed, so *path* must lie below directories only trusted
    parties write (anchor with :func:`open_dir_under` otherwise).

    :param path: The absolute directory path.
    :param durable: fsync the parent of every directory created.
    :param mode: The permission bits of the created directories (before the umask).
    :raises UnsafePath: When the deepest existing ancestor, or a component created concurrently, is a symlink or
        not a directory.
    :raises MoveFailed: When concurrent removals defeat every attempt.
    """

    for _ in range(_ATTEMPTS):
        # Walk up to the deepest existing ancestor; each component examined must be a real directory.
        missing: list[Path] = []
        current = path
        while (info := lstat(Loc(current))) is None:
            missing.append(current)
            current = current.parent
        if not stat.S_ISDIR(info.st_mode):
            raise UnsafePath(f"{current} is a symlink or not a directory", "not_directory")
        try:
            for directory in reversed(missing):
                try:
                    os.mkdir(directory, mode)
                except FileExistsError:
                    # A concurrent mkdir is success, provided it made a directory and not a symlink.
                    created = lstat(Loc(directory))
                    if created is not None and not stat.S_ISDIR(created.st_mode):
                        raise UnsafePath(f"{directory} is a symlink or not a directory", "not_directory") from None
                    continue
                if durable:
                    _fsync_parent(Loc(directory))
        except FileNotFoundError:
            # A pruner removed an ancestor between our mkdirs: walk again.
            continue
        return
    raise MoveFailed(f"could not create {path} in {_ATTEMPTS} rounds", errno.ENOENT)


def _ensure_parent(dst: Loc, *, durable: bool) -> None:
    # An anchored destination's parent is its descriptor, which exists.
    if dst.at is None:
        make_dirs(dst.path.parent, durable=durable)


def _rename(src: Loc, dst: Loc) -> OSError | None:
    if _fault is not None:
        _fault("rename", "before", src, dst)
    error: OSError | None = None
    try:
        os.rename(src.path, dst.path, src_dir_fd=src.at, dst_dir_fd=dst.at)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise CrossDevice(f"{src.path} and {dst.path} are on different filesystems") from exc
        # Not a result: the caller decides by observation.
        _LOGGER.debug("rename %s -> %s reported %s", src.path, dst.path, exc)
        error = exc
    if _fault is not None:
        _fault("rename", "after", src, dst)
    return error


def move_once(src: Loc, dst: Loc, *, durable: bool, create_parents: bool = True) -> Moved:
    """Move a contested entry to a fresh name that, once there, only the caller can move.

    When both names look absent, the move is lost: callers settle for zero seconds, because a win misjudged as
    lost leaves *dst* below a name only the caller reaches, where self-healing or a reconciler finds it.

    :param src: The entry other actors may also try to move.
    :param dst: A fresh name (it contains a fresh token or an owner id); nothing may exist there yet.
    :param durable: Fsync both parent directories after a win.
    :param create_parents: Create *dst*'s missing ancestors; with ``False`` a missing parent fails the move.
    :return: :attr:`Moved.WON` when *dst* was observed, else :attr:`Moved.LOST`.
    :raises ValueError: When *dst* already exists: an existing name would be observed as a win.
    :raises MoveFailed: When the source stayed in place after every attempt.
    :raises CrossDevice: For a rename across filesystems.
    """

    if lstat(dst) is not None:
        raise ValueError(f"{dst.path} is not a fresh name: it already exists")
    error: OSError | None = None
    for _ in range(_ATTEMPTS):
        if create_parents:
            _ensure_parent(dst, durable=durable)
        error = _rename(src, dst)
        if exists(dst):
            # dst exists: our rename happened even if rename() reported an error (NFS retransmit);
            # nobody else can create the fresh name.
            if durable:
                _fsync_parents(src, dst)
            return Moved.WON
        if exists(src):
            # src still here: the rename did not happen (e.g. a pruner removed dst's parent); retry.
            continue
        # Both absent: another actor moved src first, or our dst is not yet visible here (NFS
        # attribute caching); the caller's self-healing finds a misjudged win.
        return Moved.LOST
    raise MoveFailed(
        f"{src.path} stayed in place after {_ATTEMPTS} attempts", error.errno if error else None
    ) from error


def move_owned(src: Loc, dst: Loc, *, durable: bool, create_parents: bool = True) -> None:
    """Move an entry only the caller can move, possibly replacing a file or symlink at *dst*.

    The outcome is decided by the source alone: *dst* may have existed before
    (a replacing rename) or a successor may already have moved it on.

    :param src: The caller-owned entry.
    :param dst: The destination name; when its parent is writable by an untrusted party it MUST be anchored
        (a plain path follows a symlink planted on the way, and the created parents are checked only partly).
    :param durable: Fsync both parent directories after the move.
    :param create_parents: Create *dst*'s missing ancestors; with ``False`` a missing parent fails the move.
    :raises MoveFailed: When the source stayed in place after every attempt; ``errno`` carries the last error.
    :raises CrossDevice: For a rename across filesystems.
    """

    error: OSError | None = None
    for _ in range(_ATTEMPTS):
        if create_parents:
            _ensure_parent(dst, durable=durable)
        error = _rename(src, dst)
        if not exists(src):
            # src gone: only we could move it, so the move happened even if rename() reported an error.
            if durable:
                _fsync_parents(src, dst)
            return
        # src still here: the rename did not happen; retry (the parent may have been pruned).
    raise MoveFailed(
        f"{src.path} stayed in place after {_ATTEMPTS} attempts", error.errno if error else None
    ) from error


def _read_child(parent: Loc, name: str, limit: int) -> bytes | None:
    # Open the directory itself without following a symlink, then read the name anchored at it.
    try:
        descriptor = os.open(parent.path, _DIR_FLAGS, dir_fd=parent.at)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
            return None
        raise
    try:
        return read_bounded(anchored(descriptor, name), limit, nonblock=True)
    except (UnsafePath, TooLarge):
        return None
    finally:
        os.close(descriptor)


def carries(dst: Loc, token_name: str, token: bytes) -> bool:
    """Report whether the directory *dst* holds the regular file *token_name* with exactly *token*.

    *dst* itself is opened without following a symlink, and the token is read anchored at it, bounded and
    without blocking, so a name another party may write can never pass by pointing elsewhere.

    :param dst: The directory to check.
    :param token_name: The token file's name inside it.
    :param token: The expected content.
    :return: Whether *dst* carries the token.
    """

    return _read_child(dst, token_name, len(token)) == token


def deliver(src: Loc, dst: Loc, *, token_name: str, token: bytes, durable: bool) -> Delivered:
    """Move a directory carrying a token file to a name another party may also occupy.

    Precondition: the destination's parent is never pruned; unlike :func:`move_once`, there is no
    pruned-parent retry.

    :param src: The caller-owned directory holding ``token_name``.
    :param dst: The destination name; when its parent is writable by an untrusted party (the exchange outbox) it
        MUST be anchored, since a plain path follows a symlink planted on the way.
    :param token_name: The name of the token file inside the directory.
    :param token: The token the delivered directory carries.
    :param durable: Fsync both parent directories after a delivery.
    :return: :attr:`Delivered.DONE`, or :attr:`Delivered.OCCUPIED` when *dst* is taken and *src* remains.
    :raises DeliveryUncertain: When *src* is gone but *dst* does not carry *token*.
    :raises OSError: When *src* remains after an error that does not mean "occupied".
    :raises CrossDevice: For a rename across filesystems.
    """

    _ensure_parent(dst, durable=durable)
    error = _rename(src, dst)
    if exists(src):
        # The source stayed: an occupied destination only for the errnos that mean it.
        if error is not None and error.errno in _OCCUPIED_ERRNOS:
            return Delivered.OCCUPIED
        if error is not None:
            raise error
        raise MoveFailed(f"{src.path} stayed in place although the rename reported success")
    if carries(dst, token_name, token):
        # Our token at dst: the delivery happened, even if rename() reported an error.
        if durable:
            _fsync_parents(src, dst)
        return Delivered.DONE
    raise DeliveryUncertain(f"{src.path} is gone but {dst.path} does not carry its token")


def publish_dir(staging: Loc, dst: Loc, *, nonce: bytes, durable: bool) -> bool:
    """Create the name *dst* at most once by renaming a staged directory onto it.

    Precondition: the destination's parent is never pruned; unlike :func:`move_once`, there is no
    pruned-parent retry.

    :param staging: A caller-owned directory holding a regular file ``.nonce`` with *nonce*.
    :param dst: The name to create.
    :param nonce: The staging directory's unique nonce.
    :param durable: Fsync both parent directories after a win.
    :return: Whether this call created *dst*.
    :raises ValueError: When *staging* does not hold ``.nonce`` with *nonce*.
    :raises CrossDevice: For a rename across filesystems.
    """

    if _read_child(staging, ".nonce", len(nonce)) != nonce:
        raise ValueError(f"{staging.path} does not hold its .nonce")
    _ensure_parent(dst, durable=durable)
    _rename(staging, dst)
    # Our nonce at dst decides the win, whatever rename() reported.
    won = _read_child(dst, ".nonce", len(nonce)) == nonce
    if _fault is not None:
        _fault("publish", "after", staging, dst)
    if exists(staging):
        _remove_tree(staging)
    if won and durable:
        _fsync_parents(staging, dst)
    return won


def write_all(descriptor: int, data: bytes) -> None:
    """Write all of *data* to *descriptor*, however many short writes that takes.

    :param descriptor: An open, writable descriptor.
    :param data: The bytes.
    :raises OSError: ``EIO`` when a write makes no progress, and any error of :func:`os.write`.
    """

    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        if written == 0:
            raise OSError(errno.EIO, f"a write to descriptor {descriptor} made no progress")
        view = view[written:]


def write_file(dst: Loc, data: bytes, *, durable: bool, mode: int = 0o644) -> None:
    """Replace the file *dst* atomically with *data* through a fresh temporary beside it.

    One writer per path, or every writer writes equivalent content.

    :param dst: The file to write.
    :param data: The complete new content.
    :param durable: Fsync the file and its directory.
    :param mode: The permission bits of a newly created file (before the umask).
    """

    temporary = Loc(dst.path.with_name(f".{dst.name()}.{fresh_token()}.tmp"), dst.at)
    descriptor = os.open(temporary.path, _CREATE_FLAGS, mode, dir_fd=temporary.at)
    try:
        try:
            written = os.fstat(descriptor)
            write_all(descriptor, data)
            if durable:
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if _fault is not None:
            _fault("write", "before_replace", temporary, dst)
        try:
            os.replace(temporary.path, dst.path, src_dir_fd=temporary.at, dst_dir_fd=dst.at)
        except OSError:
            # Our temporary's inode at dst means the replace happened (NFS retransmit); a missing
            # temporary alone proves nothing, since the whole directory may have been moved away.
            landed = lstat(dst)
            if landed is None or (landed.st_dev, landed.st_ino) != (written.st_dev, written.st_ino):
                raise
            _LOGGER.debug("replace onto %s reported an error but took effect", dst.path)
    except Exception:
        try:
            os.unlink(temporary.path, dir_fd=temporary.at)
        except FileNotFoundError:
            pass
        raise
    if durable:
        _fsync_parent(dst)


def create_exclusive(dst: Loc, data: bytes = b"", *, durable: bool, mode: int = 0o600) -> int:
    """Create a new file that must not exist, not even as a symlink, and write *data*.

    :param dst: The file to create.
    :param data: The initial content.
    :param durable: Fsync the file and its directory.
    :param mode: The permission bits (before the umask).
    :return: The open ``O_WRONLY`` descriptor; the caller closes it.
    """

    descriptor = os.open(dst.path, _CREATE_FLAGS, mode, dir_fd=dst.at)
    try:
        write_all(descriptor, data)
        if durable:
            os.fsync(descriptor)
            _fsync_parent(dst)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def open_append(target: Loc, *, durable: bool, mode: int = 0o644) -> int:
    """Open a regular file for appending, creating it when absent; nothing at *target* can block the call.

    :param target: The file.
    :param durable: Fsync its directory when this call created it.
    :param mode: The permission bits of a newly created file (before the umask).
    :return: An ``O_WRONLY|O_APPEND`` descriptor; the caller closes it.
    :raises UnsafePath: When *target* is a symlink, a FIFO, a directory or any other non-regular file.
    """

    created = True
    try:
        # Exclusive first: success proves this call created the entry, whose directory then needs an fsync.
        descriptor = os.open(target.path, _CREATE_FLAGS | os.O_APPEND | os.O_NONBLOCK, mode, dir_fd=target.at)
    except FileExistsError:
        created = False
        flags = os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
        try:
            descriptor = os.open(target.path, flags, dir_fd=target.at)
        except OSError as exc:
            # ELOOP: a symlink. ENXIO: a FIFO without a reader (O_NONBLOCK never waits for one). EISDIR.
            if exc.errno == errno.ELOOP:
                raise UnsafePath(f"{target.path} is a symlink", "symlink") from exc
            if exc.errno in (errno.ENXIO, errno.EISDIR):
                raise UnsafePath(f"{target.path} is not a regular file", "not_regular") from exc
            raise
    try:
        # A FIFO with a reader, or a device, opens: only a regular file is accepted.
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise UnsafePath(f"{target.path} is not a regular file", "not_regular")
        if durable and created:
            _fsync_parent(target)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def read_bounded(src: Loc, limit: int, *, nonblock: bool = False) -> bytes | None:
    """Read a regular file of at most *limit* bytes without following a symlink.

    :param src: The file to read.
    :param limit: The largest accepted size in bytes.
    :param nonblock: Open with ``O_NONBLOCK`` so a FIFO planted by an untrusted party cannot block.
    :return: The content, or ``None`` when *src* does not exist.
    :raises UnsafePath: For a symlink or anything but a regular file.
    :raises TooLarge: For content longer than *limit*.
    """

    descriptor = _open_regular(src, nonblock)
    if descriptor is None:
        return None
    try:
        data = bytearray()
        while len(data) <= limit and (chunk := os.read(descriptor, min(1 << 20, limit + 1 - len(data)))):
            data += chunk
    finally:
        os.close(descriptor)
    if len(data) > limit:
        raise TooLarge(f"{src.path} is larger than {limit} bytes")
    return bytes(data)


def digest_regular(src: Loc, keep: int, *, nonblock: bool = False) -> tuple[int, str, bytes | None] | None:
    """Stream the SHA-256 of a regular file of any size without following a symlink.

    The file is read once, so content returned is exactly the content hashed.

    :param src: The file to hash.
    :param keep: Also return the content when it is at most this many bytes.
    :param nonblock: Open with ``O_NONBLOCK`` so a FIFO planted by an untrusted party cannot block.
    :return: The size, the lowercase hex digest and the content (``None`` past *keep*),
        or ``None`` when *src* does not exist.
    :raises UnsafePath: For a symlink or anything but a regular file.
    """

    descriptor = _open_regular(src, nonblock)
    if descriptor is None:
        return None
    digest, size, data = hashlib.sha256(), 0, bytearray()
    try:
        while chunk := os.read(descriptor, 1 << 20):
            digest.update(chunk)
            size += len(chunk)
            if size <= keep:
                data += chunk
            else:
                data.clear()
    finally:
        os.close(descriptor)
    return size, digest.hexdigest(), bytes(data) if size <= keep else None


def _open_regular(src: Loc, nonblock: bool) -> int | None:
    """Open a regular file for reading without following a symlink, or return ``None`` when it does not exist."""

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | (os.O_NONBLOCK if nonblock else 0)
    try:
        descriptor = os.open(src.path, flags, dir_fd=src.at)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise UnsafePath(f"{src.path} is a symlink", "symlink") from exc
        raise
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise UnsafePath(f"{src.path} is not a regular file", "not_regular")
    return descriptor


def read_tail(anchor: Path, relative: str | PurePosixPath, size: int, offset: int | None = None) -> tuple[bytes, int]:
    """Read a slice of a growing regular file below a trusted directory, never following a symlink or blocking.

    :param anchor: The trusted directory.
    :param relative: The file below it, such as ``logs/runlog.jsonl``.
    :param size: The most bytes to read.
    :param offset: Where to start; ``None`` reads the last *size* bytes, and an offset past the end restarts at 0.
    :return: The bytes and the offset after them; no bytes and *offset* (or 0) when the file is absent.
    :raises UnsafePath: If the file is not a regular file, or a directory on its way is a symlink.
    :raises OSError: If the file is a symlink (``ELOOP``) or cannot be read.
    """

    path = PurePosixPath(relative)
    try:
        directory = open_dir_under(anchor, path.parent)
    except (FileNotFoundError, NotADirectoryError):
        return b"", offset or 0
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory)
    except FileNotFoundError:
        return b"", offset or 0
    finally:
        os.close(directory)
    try:
        length = os.fstat(descriptor)
        if not stat.S_ISREG(length.st_mode):
            raise UnsafePath(f"{anchor / path} is not a regular file", "not_regular")
        if offset is None:
            start = max(0, length.st_size - size)
        else:
            start = offset if offset <= length.st_size else 0
        data = os.pread(descriptor, size, start)
    finally:
        os.close(descriptor)
    return data, start + len(data)


def _remove_leaf(at: int | None, name: str) -> bool:
    # True when the entry is gone; False when it is a directory to walk.
    try:
        info = os.lstat(name, dir_fd=at)
    except FileNotFoundError:
        return True
    if stat.S_ISDIR(info.st_mode):
        return False
    if _fault is not None:
        _fault("remove", "before", Loc(Path(name), at), None)
    try:
        os.unlink(name, dir_fd=at)
    except FileNotFoundError:
        pass
    return True


def _remove_listed(at: int, name: str) -> bool:
    # A listed non-directory: unlink without a lstat; False when it became a directory to walk meanwhile.
    if _fault is not None:
        _fault("remove", "before", Loc(Path(name), at), None)
    try:
        os.unlink(name, dir_fd=at)
    except FileNotFoundError:
        pass
    except OSError as exc:
        if exc.errno not in (errno.EISDIR, errno.EPERM):
            raise
        return _remove_leaf(at, name)
    return True


def _open_listing(parent: int | None, name: str) -> tuple[int, list[tuple[str, bool]]] | None:
    # None when the directory vanished or was swapped for a non-directory: the next pass sees it.
    try:
        descriptor = os.open(name, _DIR_FLAGS, dir_fd=parent)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
            return None
        raise
    try:
        # is_dir answers from the listing's d_type; a DT_UNKNOWN entry costs one internal lstat.
        with os.scandir(descriptor) as listing:
            return descriptor, [(entry.name, entry.is_dir(follow_symlinks=False)) for entry in listing]
    except BaseException:
        os.close(descriptor)
        raise


def _remove_pass(at: int | None, name: str) -> bool:
    # One bottom-up pass over descriptors opened O_NOFOLLOW; False when it must be repeated.
    if _remove_leaf(at, name):
        return True
    stack: list[tuple[int | None, str, int, list[tuple[str, bool]]]] = []
    complete = True
    try:
        opened = _open_listing(at, name)
        if opened is None:
            return False
        stack.append((at, name, *opened))
        while stack:
            parent, child, descriptor, entries = stack[-1]
            if entries:
                entry, is_dir = entries.pop()
                if is_dir or not _remove_listed(descriptor, entry):
                    opened = _open_listing(descriptor, entry)
                    if opened is None:
                        # Gone or no longer a directory since the listing: settle it now, or in the next pass.
                        complete = _remove_leaf(descriptor, entry) and complete
                    else:
                        stack.append((descriptor, entry, *opened))
                continue
            stack.pop()
            os.close(descriptor)
            if _fault is not None:
                _fault("remove", "before", Loc(Path(child), parent), None)
            try:
                os.rmdir(child, dir_fd=parent)
            except OSError as exc:
                # ENOENT: someone else removed it. ENOTEMPTY: it refilled during the walk.
                if exc.errno in (errno.ENOTEMPTY, errno.EEXIST):
                    complete = False
                elif exc.errno != errno.ENOENT:
                    raise
    finally:
        for _, _, descriptor, _ in stack:
            os.close(descriptor)
    return complete


def _remove_tree(target: Loc) -> None:
    for _ in range(_ATTEMPTS):
        if _remove_pass(target.at, str(target.path)):
            return
    raise OSError(errno.ENOTEMPTY, f"kept refilling during {_ATTEMPTS} removal passes", str(target.path))


def discard(target: Loc, *, trash_dir: Path, durable: bool) -> None:
    """Remove a caller-owned tree: move it into the trash, then remove it bottom-up.

    Symlinks are removed, never followed. An absent *target* is not an error.

    :param target: The location to discard; an anchored one is moved out of its descriptor's directory.
    :param trash_dir: A directory on the same filesystem for the intermediate trash name.
    :param durable: Make the move out of *target*'s directory durable.
    """

    trash = loc(trash_dir / f"trash.{fresh_token()}")
    move_owned(target, trash, durable=durable)
    _remove_tree(trash)


def remove_empty_dir(target: Loc) -> bool:
    """Remove an empty directory.

    :param target: The directory.
    :return: ``True`` when removed; ``False`` when it is not empty or absent.
    """

    if _fault is not None:
        _fault("remove", "before", target, None)
    try:
        os.rmdir(target.path, dir_fd=target.at)
    except OSError as exc:
        if exc.errno in (errno.ENOTEMPTY, errno.EEXIST, errno.ENOENT):
            return False
        raise
    return True


def remove_file(target: Loc, *, durable: bool) -> None:
    """Remove a file or symlink; an absent *target* counts as already removed.

    :param target: The entry to remove.
    :param durable: Fsync the parent directory after the removal.
    :raises IsADirectoryError: When *target* is a directory.
    """

    if _fault is not None:
        _fault("remove", "before", target, None)
    try:
        os.unlink(target.path, dir_fd=target.at)
    except FileNotFoundError:
        # Already removed: the outcome the caller wants.
        pass
    if durable:
        _fsync_parent(target)


@dataclass(frozen=True)
class WalkLimits:
    """Bounds of :func:`walk_untrusted` and of an untrusted :func:`copy_tree`.

    :param entries: The largest accepted number of entries.
    :param depth: The deepest accepted entry, counted in path components below the root.
    :param max_bytes: The largest accepted total size of the regular files; it also ends the copy of a file an
        untrusted party keeps growing.
    """

    entries: int = 1_000_000
    depth: int = 256
    max_bytes: int = 1 << 40


DEFAULT_LIMITS = WalkLimits()


def _open_child_dir(at: int | None, name: str) -> int:
    try:
        return os.open(name, _DIR_FLAGS, dir_fd=at)
    except OSError as exc:
        # Linux reports a symlink opened O_DIRECTORY|O_NOFOLLOW as ENOTDIR, not ELOOP.
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise UnsafePath(f"{name} is a symlink or not a directory", "not_directory") from exc
        raise


def _finish_copy(descriptor: int, source: os.stat_result, *, durable: bool) -> None:
    # As shutil.copy2 and copytree: the permission bits and times travel with the copy.
    os.fchmod(descriptor, stat.S_IMODE(source.st_mode))
    os.utime(descriptor, ns=(source.st_atime_ns, source.st_mtime_ns))
    if durable:
        os.fsync(descriptor)


def _copy_file(
    src: int,
    dst: int,
    name: str,
    relative: PurePosixPath,
    *,
    durable: bool,
    limits: WalkLimits | None,
    count: list[int],
) -> None:
    try:
        source = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=src)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise UnsafePath(f"{relative} became a symlink while it was copied", "symlink") from exc
        raise
    try:
        opened = os.fstat(source)
        if not stat.S_ISREG(opened.st_mode):
            raise UnsafePath(f"{relative} is not a regular file", "not_regular")
        if limits is not None and opened.st_nlink > 1:
            raise UntrustedContentError(relative, "a hard-linked file")
        target = os.open(name, _CREATE_FLAGS, 0o600, dir_fd=dst)
        try:
            while chunk := os.read(source, 1 << 20):
                count[1] += len(chunk)
                if limits is not None and count[1] > limits.max_bytes:
                    raise UntrustedContentError(relative, f"more than {limits.max_bytes} bytes in all")
                write_all(target, chunk)
            _finish_copy(target, opened, durable=durable)
        finally:
            os.close(target)
    finally:
        os.close(source)


def _copy_dir(
    src: int, dst: int, relative: PurePosixPath, *, durable: bool, limits: WalkLimits | None, count: list[int]
) -> None:
    for name in sorted(os.listdir(src)):
        path = relative / name
        if limits is not None:
            count[0] += 1
            if len(path.parts) > limits.depth:
                raise UntrustedContentError(path, f"deeper than {limits.depth} levels")
            if count[0] > limits.entries:
                raise UntrustedContentError(path, f"more than {limits.entries} entries")
        mode = os.lstat(name, dir_fd=src).st_mode
        if stat.S_ISDIR(mode):
            child = _open_child_dir(src, name)
            try:
                os.mkdir(name, 0o700, dir_fd=dst)
                made = _open_child_dir(dst, name)
                try:
                    _copy_dir(child, made, path, durable=durable, limits=limits, count=count)
                    _finish_copy(made, os.fstat(child), durable=durable)
                finally:
                    os.close(made)
            finally:
                os.close(child)
        elif stat.S_ISLNK(mode):
            if limits is not None:
                raise UntrustedContentError(path, "a symlink")
            # The one place a symlink is created: a trusted copy keeps a link a link, and never follows it.
            os.symlink(os.readlink(name, dir_fd=src), name, dir_fd=dst)  # noqa: TID251
        elif stat.S_ISREG(mode):
            _copy_file(src, dst, name, path, durable=durable, limits=limits, count=count)
        else:
            raise UnsafePath(f"{path} is a special file", "not_regular")


def copy_tree(src: Path | Loc, dst: Path, *, durable: bool, limits: WalkLimits | None = None) -> None:
    """Copy a tree to a fresh name, possibly on another filesystem, never following a symlink in *src*.

    Every directory of *src* is opened ``O_DIRECTORY|O_NOFOLLOW`` relative to its parent's descriptor, and every
    regular file is read through an anchored ``O_NOFOLLOW|O_NONBLOCK`` open, so a party that renames or replaces
    entries of *src* during the copy changes what is copied, never where it is read from. In a trusted copy,
    symlinks are recreated as symlinks (their targets are never examined); an untrusted copy refuses them.
    Permission bits and times are kept, and anything else is refused. The copy is not atomic: callers copy to a private or partial name and move or deliver it afterwards.

    :param src: The directory to copy: an absolute path (a symlink there is refused) or an anchored location.
    :param dst: The destination, which must not exist; its parent is created.
    :param durable: Fsync every copied file and directory, and the parent of *dst*.
    :param limits: For a source an untrusted party writes: refuse symlinks, hard-linked files, and more, deeper or
        larger entries than these bounds.
    :raises FileExistsError: When *dst* exists.
    :raises UnsafePath: For a special file, or an entry replaced by another kind of entry during the copy.
    :raises UntrustedContentError: With *limits*, for a symlink, a hard-linked file, or too many, too deep or too
        large entries.
    """

    if exists(loc(dst)):
        raise FileExistsError(errno.EEXIST, "the copy destination exists", str(dst))
    make_dirs(dst.parent, durable=durable)
    source = _open_child_dir(src.at, str(src.path)) if isinstance(src, Loc) else open_dir(src)
    try:
        os.mkdir(dst, 0o700)
        target = open_dir(dst)
        try:
            _copy_dir(source, target, PurePosixPath(), durable=durable, limits=limits, count=[0, 0])
            _finish_copy(target, os.fstat(source), durable=durable)
        finally:
            os.close(target)
    finally:
        os.close(source)
    if durable:
        _fsync_parent(loc(dst))


#: What :func:`shutil.copy2` ignores when extended attributes cannot be listed, read or (with ``EPERM``) set.
_XATTR_IGNORED = frozenset({errno.ENOTSUP, errno.ENODATA, errno.EINVAL})


def _copy_xattrs(source: int, target: int) -> None:
    # As shutil.copy2: every extended attribute the source has and the target's filesystem takes (none off Linux).
    if not hasattr(os, "listxattr"):
        return
    try:
        names = os.listxattr(source)
    except OSError as exc:
        if exc.errno not in _XATTR_IGNORED:
            raise
        return
    for name in names:
        try:
            os.setxattr(target, name, os.getxattr(source, name))
        except OSError as exc:
            if exc.errno not in _XATTR_IGNORED | {errno.EPERM}:
                raise


def copy_file(
    src: str | os.PathLike[str], dst: str | os.PathLike[str], *, durable: bool, metadata: bool = False
) -> None:
    """Copy one regular file's bytes to *dst*, as :func:`shutil.copyfile` does, fsyncing it on its writing descriptor.

    *src* is a file its caller trusts, opened following symlinks as :func:`shutil.copyfile` does; *dst* is created,
    or truncated when it exists (never when it is *src* itself), never through a symlink. Its directory is not
    fsynced: a sync barrier's walk, or the caller, does that.

    :param src: The file to copy.
    :param dst: The copy.
    :param durable: Fsync the copy before closing it.
    :param metadata: Also keep the permission bits, times and extended attributes, as :func:`shutil.copy2` does (and
        so usable as the ``copy_function`` of :func:`shutil.copytree`).
    :raises UnsafePath: When *src* is not a regular file.
    :raises shutil.SameFileError: When *dst* is *src*; neither is changed.
    :raises shutil.SpecialFileError: When *dst* exists and is not a regular file (a device, say); it is not
        written.
    :raises OSError: When *src* cannot be read or *dst* written (``ELOOP`` for a symlink at *dst*).
    """

    source = os.open(src, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        opened = os.fstat(source)
        if not stat.S_ISREG(opened.st_mode):
            raise UnsafePath(f"{os.fspath(src)} is not a regular file", "not_regular")
        # Truncated only once it is known to be a regular file other than the source; never blocking on a FIFO.
        target = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, 0o666)
        try:
            written = os.fstat(target)
            if not stat.S_ISREG(written.st_mode):
                raise shutil.SpecialFileError(f"{os.fspath(dst)!r} is not a regular file")
            if (written.st_dev, written.st_ino) == (opened.st_dev, opened.st_ino):
                raise shutil.SameFileError(f"{os.fspath(src)!r} and {os.fspath(dst)!r} are the same file")
            os.ftruncate(target, 0)
            while chunk := os.read(source, 1 << 20):
                write_all(target, chunk)
            if metadata:
                _copy_xattrs(source, target)
                _finish_copy(target, opened, durable=durable)
            elif durable:
                os.fsync(target)
        finally:
            os.close(target)
    finally:
        os.close(source)


#: Directory descriptors one :func:`extract_tar` keeps open below its target, least recently used closed first.
_EXTRACT_DIRECTORIES = 64
#: Linux's ``PATH_MAX``, which counts the separator after *target* and the terminating NUL: *target*, ``/`` and
#: the member name together must stay below it.
_EXTRACT_PATH_MAX = 4096
#: The deepest accepted member, in path components below *target*.
_EXTRACT_DEPTH = 256
_EXTRACT_CHUNK = 1 << 20
#: Hard-link copies may write at most this many times the regular-file bytes the stream itself carried so far,
#: plus the allowance: a file under a few names passes, a small stream copied without end does not.
_HARD_LINK_FACTOR = 8
_HARD_LINK_ALLOWANCE = 1 << 30
_EXTRACT_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW | os.O_CLOEXEC
_SOURCE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
#: What an anchored open or create reports for an entry of the wrong kind in the way: a symlink (``ELOOP``, or
#: ``ENOTDIR`` for an ``O_DIRECTORY`` open), a non-directory component, or a directory where a file goes.
_IN_THE_WAY = frozenset({errno.ELOOP, errno.ENOTDIR, errno.EISDIR})
#: A hostile time is not fatal either: ``ValueError`` for NaN, ``OverflowError`` for one out of range.
_TIME_ERRORS = (OSError, OverflowError, ValueError)
#: A pooled extraction hands a regular member of at most this many bytes, read into memory, to a worker.
_POOLED_SIZE = 1 << 20
#: The members a pooled extraction keeps handed out and unwritten, per worker, and their bytes in all.
_POOLED_TASKS = 4
_POOLED_BYTES = 64 << 20

type _Parts = tuple[str, ...]


def _in_the_way(member: tarfile.TarInfo, parent: int, name: str, path: str, what: str) -> tarfile.FilterError:
    # Only on a refusal: a symlink in the way could lead out of the landing; anything else is just in the way.
    try:
        found = os.lstat(name, dir_fd=parent)
    except OSError:
        found = None
    if found is not None and stat.S_ISLNK(found.st_mode):
        return tarfile.OutsideDestinationError(member, path)
    return tarfile.FilterError(f"{member.name!r} would be extracted to {path!r}, {what}")


def _data_mode(mode: int) -> int:
    # tarfile's data filter: no high bits, no group/other write, exec bits only when owner-executable, owner rw.
    mode &= 0o755
    if not mode & 0o100:
        mode &= ~0o111
    return mode | 0o600


def _finish_member(descriptor: int, member: tarfile.TarInfo) -> None:
    # Mode and time are not fatal, as in tarfile, whose chmod and utime failures are non-fatal ExtractErrors.
    try:
        os.fchmod(descriptor, _data_mode(member.mode))
    except OSError:
        pass
    try:
        os.utime(descriptor, (member.mtime, member.mtime))
    except _TIME_ERRORS:
        pass


class _Landing:
    """The state of one extraction: *target*, opened once following symlinks, and cached directories below.

    Every directory below *target* is opened ``O_DIRECTORY|O_NOFOLLOW`` relative to its parent's descriptor, so a
    symlink at any component makes the open fail; a path is never resolved by name.

    :param target: The directory extracted into; a symlink to a directory is followed.
    :param durable: Fsync every regular file on its writing descriptor and, after the last member, every directory.
    """

    def __init__(self, target: Path, *, durable: bool = False) -> None:
        self.target = str(target)
        self.durable = durable
        self.root = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        self._cache: OrderedDict[_Parts, int] = OrderedDict()
        #: Every directory below the target this extraction opened or created (fsynced when durable).
        self.resolved: set[_Parts] = set()
        #: The regular files this stream wrote, by normalized path: the only names a hard link may copy.
        self.written: set[_Parts] = set()
        #: The directory members, whose times are restored after the last member.
        self.directories: dict[_Parts, tarfile.TarInfo] = {}
        #: Regular-file bytes read from the stream, and bytes written by hard-link copies.
        self.received = 0
        self.copied = 0

    def close(self) -> None:
        """Close every descriptor."""

        try:
            for descriptor in self._cache.values():
                os.close(descriptor)
        finally:
            self._cache.clear()
            os.close(self.root)

    def path(self, parts: _Parts) -> str:
        """Return the display path of *parts* below the target.

        :param parts: The components below the target.
        :return: The joined path, for messages only.
        """

        return os.path.join(self.target, *parts)

    def directory(self, member: tarfile.TarInfo, parts: _Parts, *, create: bool) -> int:
        """Return a descriptor of the directory *parts* below the target; the landing keeps and closes it.

        The descriptor stays valid only until the next call, which may close it to make room.

        :param member: The member being extracted, for a refusal.
        :param parts: The components below the target.
        :param create: Create a missing component with the default mode.
        :return: The directory descriptor.
        :raises tarfile.FilterError: When a component is a symlink or not a directory.
        :raises FileNotFoundError: When a component is missing and *create* is false.
        """

        if not parts:
            return self.root
        cached = self._cache.get(parts)
        if cached is not None:
            self._cache.move_to_end(parts)
            return cached
        known = len(parts) - 1
        while known and parts[:known] not in self._cache:
            known -= 1
        descriptor = self.directory(member, parts[:known], create=False)
        for depth in range(known + 1, len(parts) + 1):
            descriptor = self._open(member, descriptor, parts[:depth], create=create)
        return descriptor

    def _open(self, member: tarfile.TarInfo, parent: int, parts: _Parts, *, create: bool) -> int:
        name = parts[-1]
        try:
            child = os.open(name, _DIR_FLAGS, dir_fd=parent)
        except FileNotFoundError:
            if not create:
                raise
            try:
                os.mkdir(name, dir_fd=parent)
            except FileExistsError:
                pass
            child = self._open_existing(member, parent, parts)
        except OSError as exc:
            if exc.errno in _IN_THE_WAY:
                raise _in_the_way(member, parent, name, self.path(parts), "which is not a directory") from exc
            raise
        self._cache[parts] = child
        self.resolved.add(parts)
        while len(self._cache) > _EXTRACT_DIRECTORIES:
            os.close(self._cache.popitem(last=False)[1])
        return child

    def _open_existing(self, member: tarfile.TarInfo, parent: int, parts: _Parts) -> int:
        try:
            return os.open(parts[-1], _DIR_FLAGS, dir_fd=parent)
        except OSError as exc:
            # Linux reports a symlink opened O_DIRECTORY|O_NOFOLLOW as ENOTDIR, not ELOOP.
            if exc.errno in _IN_THE_WAY:
                raise _in_the_way(member, parent, parts[-1], self.path(parts), "which is not a directory") from exc
            raise

    def remove_in_the_way(self, member: tarfile.TarInfo, parent: int, parts: _Parts) -> None:
        """Unlink the non-directory entry at *parts*, never following it; an absent entry is fine.

        :param member: The member being extracted, for a refusal.
        :param parent: The descriptor of the entry's directory.
        :param parts: The entry's components below the target.
        :raises tarfile.FilterError: When the entry is a directory.
        """

        try:
            os.unlink(parts[-1], dir_fd=parent)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # Linux reports unlink of a directory as EISDIR, POSIX as EPERM, which can also be a real denial.
            if exc.errno == errno.EISDIR or (exc.errno == errno.EPERM and self._is_directory(parent, parts[-1])):
                raise tarfile.FilterError(
                    f"{member.name!r} would be extracted to {self.path(parts)!r}, which is a directory"
                ) from exc
            raise

    @staticmethod
    def _is_directory(parent: int, name: str) -> bool:
        try:
            return stat.S_ISDIR(os.lstat(name, dir_fd=parent).st_mode)
        except OSError:
            return False

    def create(self, member: tarfile.TarInfo, parent: int, parts: _Parts, flags: int) -> int:
        """Open the regular file *parts* for writing with *flags*, which include ``O_NOFOLLOW``.

        :param member: The member being extracted, for a refusal.
        :param parent: The descriptor of the file's directory.
        :param parts: The file's components below the target.
        :param flags: The open flags.
        :return: The open descriptor; the caller closes it.
        :raises tarfile.FilterError: When a symlink or a directory is at the name.
        """

        try:
            return os.open(parts[-1], flags, 0o600, dir_fd=parent)
        except OSError as exc:
            if exc.errno in _IN_THE_WAY or (exc.errno == errno.EEXIST and flags & os.O_EXCL):
                raise _in_the_way(member, parent, parts[-1], self.path(parts), "where another entry is") from exc
            raise


def _member_parts(member: tarfile.TarInfo, name: str, target: str) -> _Parts:
    # Lexical only: a leading "/" is stripped, empty and "." components dropped, and ".." refused outright.
    relative = name.lstrip("/")
    parts = tuple(part for part in relative.split("/") if part not in ("", "."))
    if ".." in parts:
        raise tarfile.OutsideDestinationError(member, os.path.normpath(os.path.join(target, relative)))
    if "\0" in name:
        raise tarfile.FilterError(f"{member.name!r} has a NUL character in its name")
    if len(parts) > _EXTRACT_DEPTH:
        raise tarfile.FilterError(f"{member.name!r} is deeper than {_EXTRACT_DEPTH} components")
    if len(os.fsencode(target)) + 1 + len(os.fsencode("/".join(parts))) >= _EXTRACT_PATH_MAX:
        raise tarfile.FilterError(f"{member.name!r} below {target!r} reaches {_EXTRACT_PATH_MAX} bytes")
    if not member.isdir() and name.endswith(("/", "/.")):
        raise tarfile.FilterError(f"{member.name!r} is not a directory, but its name ends like one")
    return parts


def _sparse(member: tarfile.TarInfo) -> bool:
    return member.sparse is not None or member.type == tarfile.GNUTYPE_SPARSE


def _link_parts(linkname: str) -> _Parts:
    # A hard link's source, lexically, as _member_parts treats a name (``..`` is refused by the caller).
    return tuple(part for part in linkname.split("/") if part not in ("", "."))


def _extract_regular(landing: _Landing, archive: tarfile.TarFile, member: tarfile.TarInfo, parts: _Parts) -> None:
    if not parts:
        raise tarfile.OutsideDestinationError(member, landing.target)
    if _sparse(member):
        # Its map could claim any size (a dense write fills it, a sparse one leaves a huge apparent file).
        raise tarfile.FilterError(f"{member.name!r} is a sparse member; sparse members are not accepted")
    parent = landing.directory(member, parts[:-1], create=True)
    # Without O_EXCL: an existing regular file at the name is written in place, as tarfile does.
    descriptor = landing.create(member, parent, parts, _EXTRACT_FLAGS)
    try:
        source = archive.extractfile(member)
        if source is None:  # pragma: no cover - tarfile returns a reader for every regular member
            raise tarfile.FilterError(f"{member.name!r} has no data")
        with source:
            while chunk := source.read(_EXTRACT_CHUNK):
                landing.received += len(chunk)
                write_all(descriptor, chunk)
        _finish_member(descriptor, member)
        if landing.durable:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    landing.written.add(parts)


def _extract_directory(landing: _Landing, member: tarfile.TarInfo, parts: _Parts) -> None:
    if parts:
        parent = landing.directory(member, parts[:-1], create=True)
        try:
            # The default mode: a member's own bits (setuid, sticky, world-writable) are never applied.
            os.mkdir(parts[-1], dir_fd=parent)
        except FileExistsError:
            # An existing directory is accepted (and now cached); anything else is refused.
            landing.directory(member, parts, create=False)
    landing.directories[parts] = member


def _extract_symlink(landing: _Landing, member: tarfile.TarInfo, parts: _Parts) -> None:
    if not parts:
        raise tarfile.OutsideDestinationError(member, landing.target)
    if not member.linkname or "\0" in member.linkname:
        raise tarfile.FilterError(f"{member.name!r} is a symlink with an empty link text or a NUL in it")
    parent = landing.directory(member, parts[:-1], create=True)
    # The link text is kept verbatim (job trees legitimately carry absolute targets) and never followed here.
    try:
        os.symlink(member.linkname, parts[-1], dir_fd=parent)  # noqa: TID251
    except FileExistsError:
        landing.remove_in_the_way(member, parent, parts)
        os.symlink(member.linkname, parts[-1], dir_fd=parent)  # noqa: TID251
    landing.written.discard(parts)


def _extract_hard_link(landing: _Landing, member: tarfile.TarInfo, parts: _Parts) -> None:
    if not parts:
        raise tarfile.OutsideDestinationError(member, landing.target)
    if member.linkname.startswith("/"):
        raise tarfile.AbsoluteLinkError(member)
    source_parts = _link_parts(member.linkname)
    if ".." in source_parts:
        raise tarfile.LinkOutsideDestinationError(
            member, os.path.normpath(os.path.join(landing.target, member.linkname))
        )
    if source_parts == parts and parts in landing.written:
        return  # GNU tar links a name it was given twice to itself: the file is already there.
    if source_parts == parts or source_parts not in landing.written:
        raise tarfile.FilterError(
            f"{member.name!r} is a hard link to {member.linkname!r}, which is not a regular file this stream "
            "extracted under another name"
        )
    # Copied, never linked: the source is opened first, so an existing destination (even one already
    # hard-linked to the source) can then be unlinked and created afresh.
    source_parent = landing.directory(member, source_parts[:-1], create=False)
    try:
        source = os.open(source_parts[-1], _SOURCE_FLAGS, dir_fd=source_parent)
    except OSError as exc:
        if exc.errno in _IN_THE_WAY:
            raise _in_the_way(
                member, source_parent, source_parts[-1], landing.path(source_parts), "which is not a regular file"
            ) from exc
        raise
    try:
        found = os.fstat(source)
        if not stat.S_ISREG(found.st_mode):
            raise tarfile.FilterError(
                f"{member.name!r} is a hard link to {member.linkname!r}, which is not a regular file"
            )
        budget = _HARD_LINK_FACTOR * landing.received + _HARD_LINK_ALLOWANCE
        if landing.copied + found.st_size > budget:
            raise _amplified(member, budget)
        parent = landing.directory(member, parts[:-1], create=True)
        landing.remove_in_the_way(member, parent, parts)
        descriptor = landing.create(member, parent, parts, _CREATE_FLAGS)
        try:
            while chunk := os.read(source, _EXTRACT_CHUNK):
                landing.copied += len(chunk)
                if landing.copied > budget:
                    raise _amplified(member, budget)
                write_all(descriptor, chunk)
            _finish_member(descriptor, member)
            if landing.durable:
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        os.close(source)
    landing.written.add(parts)


def _amplified(member: tarfile.TarInfo, budget: int) -> tarfile.FilterError:
    return tarfile.FilterError(
        f"{member.name!r}: hard-link copies would write more than {budget} bytes, {_HARD_LINK_FACTOR} times the "
        f"stream's own file bytes plus {_HARD_LINK_ALLOWANCE}"
    )


def _restore_directory_times(landing: _Landing) -> None:
    # Deepest first, after every member, as tarfile does; a failure is not fatal, as in tarfile.
    for parts, member in sorted(landing.directories.items(), key=lambda item: len(item[0]), reverse=True):
        times = (member.mtime, member.mtime)
        try:
            if parts:
                parent = landing.directory(member, parts[:-1], create=False)
                os.utime(parts[-1], times, dir_fd=parent, follow_symlinks=False)
            else:
                os.utime(landing.root, times)
        except (*_TIME_ERRORS, tarfile.FilterError):
            pass


def _fsync_directories(landing: _Landing) -> None:
    # A durable extraction, after every member and worker and the times: each directory the stream created or
    # wrote into, deepest first, then the target. Unlike the times, a failure here is fatal.
    for parts in sorted((landing.resolved | set(landing.directories)) - {()}, key=len, reverse=True):
        os.fsync(landing.directory(tarfile.TarInfo("/".join(parts)), parts, create=False))
    os.fsync(landing.root)


def _extract_member(
    landing: _Landing, archive: tarfile.TarFile, member: tarfile.TarInfo, rename: tuple[str, str] | None
) -> None:
    name = member.name
    if rename is not None:
        # The landing directory holds the user's other files, so only one regular file may arrive there.
        if name != rename[0] or not member.isreg():
            raise tarfile.FilterError(f"unexpected member in a single-file pull: {name!r}")
        name = rename[1]
    _dispatch(landing, archive, member, _member_parts(member, name, landing.target))


def _dispatch(landing: _Landing, archive: tarfile.TarFile, member: tarfile.TarInfo, parts: _Parts) -> None:
    if member.isdir():
        _extract_directory(landing, member, parts)
    elif member.isreg():
        _extract_regular(landing, archive, member, parts)
    elif member.issym():
        _extract_symlink(landing, member, parts)
    elif member.islnk():
        _extract_hard_link(landing, member, parts)
    else:
        raise tarfile.SpecialFileError(member)


@dataclass(frozen=True)
class _Write:
    """A regular member handed to a worker: its data, and a duplicate of its directory's descriptor, which it owns."""

    member: tarfile.TarInfo
    parent: int
    parts: _Parts
    data: bytes


def _write_member(landing: _Landing, task: _Write) -> None:
    # A worker's part, exactly the sequential path's for a regular file: anchored no-follow open, write, the data
    # filter's mode and the member's time (not fatal), close; a refusal is translated as there.
    try:
        descriptor = landing.create(task.member, task.parent, task.parts, _EXTRACT_FLAGS)
        try:
            write_all(descriptor, task.data)
            _finish_member(descriptor, task.member)
            if landing.durable:
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        os.close(task.parent)


def _close_write(task: _Write) -> None:
    os.close(task.parent)  # a task no worker took


def _folded(parts: _Parts) -> _Parts:
    # Case-folded, so that names a case-insensitive filesystem confuses also wait for each other.
    return tuple(part.casefold() for part in parts)


class _Reader:
    """The reading thread of a pooled extraction: the only reader of the stream and writer of the landing's state.

    It resolves and creates every directory, handles every member but the small regular ones itself, and updates
    the received bytes and the written names (a small regular member counts as written once it is handed out).
    Before acting on a member whose name, an ancestor or a descendant of it, or whose hard-link source, has a write
    in flight (names compared normalized and case-folded) it waits for those writes.

    :param landing: The extraction's state.
    :param archive: The stream, opened ``r|``.
    :param feed: The workers' feed.
    """

    def __init__(self, landing: _Landing, archive: tarfile.TarFile, feed: Feed[_Write]) -> None:
        self.landing = landing
        self.archive = archive
        self.feed = feed
        self.members = iter(archive)
        #: A member taken from the stream but not acted on, because the feed stopped while it waited.
        self.held: tarfile.TarInfo | None = None
        # Under the feed's condition: the task writing each folded name, and how many of them are at or below each
        # folded name (pruned of finished tasks lazily).
        self.tasks: dict[_Parts, int] = {}
        self.below: Counter[_Parts] = Counter()

    def read(self) -> bool:
        """Act on every member in stream order; stop before the next one once the feed stopped.

        :return: Whether the stream's members are exhausted.
        """

        while True:
            member = self.held
            if member is None:
                if self.feed.stopped:
                    return False
                member = next(self.members, None)
                if member is None:
                    return True
                self.held = member
            if not self.handle(member):
                return False
            self.held = None

    def handle(self, member: tarfile.TarInfo) -> bool:
        """Act on one member, after its waits; nothing is done when the feed stops during them.

        :param member: The member.
        :return: Whether it was acted on.
        """

        landing = self.landing
        parts = _member_parts(member, member.name, landing.target)
        keys = [_folded(parts)]
        if (
            member.islnk()
            and not member.linkname.startswith("/")
            and ".." not in (source := _link_parts(member.linkname))
        ):
            keys.append(_folded(source))
        if not self.feed.wait(lambda: all(self.clear(key) for key in keys)):
            return False
        if not (member.isreg() and parts and not _sparse(member) and member.size <= _POOLED_SIZE):
            _dispatch(landing, self.archive, member, parts)
            return True
        if not self.feed.wait(lambda: self.feed.room(member.size)):
            return False
        parent = landing.directory(member, parts[:-1], create=True)
        source_file = self.archive.extractfile(member)
        if source_file is None:  # pragma: no cover - tarfile returns a reader for every regular member
            raise tarfile.FilterError(f"{member.name!r} has no data")
        with source_file:
            data = source_file.read()
        landing.received += len(data)
        submitted = self.feed.submitted
        descriptor = os.dup(parent)  # the cached one may be closed before a worker takes the task
        try:
            index = self.feed.submit(_Write(member, descriptor, parts, data), len(data))
        except BaseException:
            if self.feed.submitted == submitted:
                os.close(descriptor)  # never handed over
            raise
        del data  # the feed holds the only reference, until a worker takes the task
        key = keys[0]
        with self.feed.condition:
            self.tasks[key] = index
            for depth in range(1, len(key) + 1):
                self.below[key[:depth]] += 1
        landing.written.add(parts)
        return True

    def clear(self, key: _Parts) -> bool:
        """Whether no write in flight is at, above or below *key*; call it under the feed's condition.

        :param key: A folded name.
        :return: Whether the member may be acted on.
        """

        if len(self.tasks) > 2 * self.feed.max_tasks:
            self.prune()  # keeps the bookkeeping as small as the feed's bound
        if not self.conflicts(key):
            return True
        self.prune()
        return not self.conflicts(key)

    def conflicts(self, key: _Parts) -> bool:
        """Whether a recorded write is at, below or above *key*.

        :param key: A folded name.
        :return: Whether one is.
        """

        return self.below[key] > 0 or any(key[:depth] in self.tasks for depth in range(1, len(key)))

    def prune(self) -> None:
        """Forget the writes that finished."""

        for key, index in list(self.tasks.items()):
            if self.feed.finished(index):
                del self.tasks[key]
                for depth in range(1, len(key) + 1):
                    self.below[key[:depth]] -= 1
                    if not self.below[key[:depth]]:
                        del self.below[key[:depth]]


def extract_tar(
    stream: IO[bytes],
    target: Path,
    *,
    rename: tuple[str, str] | None = None,
    workers: int = 1,
    durable: bool = False,
) -> int:
    """Extract an untrusted tar stream under *target*, never resolving a path by name.

    *target* is opened once, following a symlink there; every member below it is reached through directory
    descriptors opened component by component ``O_DIRECTORY|O_NOFOLLOW``, so a symlink at any component (one this
    stream wrote or one an earlier extraction left) refuses the member. Missing directories are created with the
    default mode. A leading ``/`` is stripped from names, empty and ``.`` components are dropped, and a ``..``
    component is refused, as are a NUL, more than 256 components, a path that with *target* and a separator
    reaches ``PATH_MAX`` (4096 bytes), and a non-directory whose name ends in ``/`` or ``/.``. Members, in stream
    order:

    - A directory is created with the default mode (an existing one accepted); a ``.`` member names *target*
      itself, every other type naming *target* is refused. Directory times are restored after the last member,
      deepest first.
    - A regular file is written through an ``O_NOFOLLOW`` open (a symlink at its name is refused; an existing
      regular file is written in place) with tarfile's ``data`` filter mode (no high bits, no group or other
      write, exec bits only when owner-executable, owner read and write) and its time. A sparse member is refused.
    - A symlink keeps its link text, absolute or not (an empty one or one with a NUL is refused); an existing
      non-directory entry at its name is removed first, never followed.
    - A hard link must name a regular file this stream extracted under another name; it is copied (no hard link is
      ever made), with its own member's mode and time; one naming itself, after that file, is a no-op. An absolute
      link name is refused, and so is a copy that would take the bytes copied for hard links beyond 8 times the
      regular-file bytes the stream carried so far plus 1 GiB.
    - Devices, FIFOs and other types are refused; ownership is ignored.

    Mode and time failures are not fatal. Without *durable* nothing is fsynced (the caller's
    :class:`Barrier` makes the landing durable). A refused member stops the extraction and leaves
    what was extracted before it (see *workers* for what more it may leave). A stream cut short, even exactly at a
    member boundary, is refused after its last complete member; a damaged (non-zero, invalid) header after the
    members ends the stream as tarfile reads it and is not detected.

    Threat model: the stream is untrusted; *target* is this process's own directory (not always a fresh one: a
    reused push token, a single-file pull's chosen directory), which only trusted local actors write. A concurrent
    local writer that moves a directory of *target* during the extraction redirects every later member of that
    directory through a cached descriptor; that is outside the threat model, as it is for tarfile's filters.

    :param stream: The binary tar stream, read once.
    :param target: The existing directory to extract into.
    :param rename: ``(member, name)`` for a single file: the only member accepted, a regular file written as *name*.
    :param workers: With more than one (and no *rename*), regular members of at most 1 MiB are read into memory
        by this thread and written by up to that many worker threads (at most 4 per worker or 64 MiB handed out
        at once), each through a duplicate of its directory's descriptor; everything else, and every directory
        resolution, stays in this thread, which waits for the writes in flight below, at or above a member's name
        (compared case-folded, not Unicode-normalized: names a filesystem such as macOS's treats as equal only
        after normalization are not serialized) before acting on it. On a failure or an interrupt no write is
        running any more when this raises. The failure raised is the first one this thread or a worker met, which
        in rare interleavings is not the first in stream order, and a member a worker took just as another write
        failed is then not written. Members after the refused one may have been applied too, symlink and
        hard-link members replacing earlier entries included (the caller discards the landing). A ``SIGINT`` is
        acted on between members only, not inside a stalled read of the stream or a large member this thread
        writes. Under a custom ``SIGINT`` handler that returns, a refusal this thread meets may be raised before
        the members already handed out are written, so the refusal reported may be a later one than the
        sequential extraction reports.
    :param durable: Fsync every regular file on the descriptor that wrote it, before closing it (in this thread or
        the worker that wrote it), and after the last member and the directory times every directory the stream
        created or wrote into, deepest first, then *target*: the strategy ``fsync`` of a :class:`Barrier`. A
        failed fsync is an ``OSError`` like any other write error.
    :return: How many regular files the stream wrote (hard-link copies included).
    :raises tarfile.FilterError: For a refused member; a subclass (``OutsideDestinationError``,
        ``SpecialFileError``, ``AbsoluteLinkError``, ``LinkOutsideDestinationError``) where one applies.
    :raises tarfile.TarError: If the stream is malformed or cut short.
    :raises OSError: For an error of the filesystem itself (``ENOSPC``, ``EIO``, ``EACCES``, ...).
    """

    landing = _Landing(target, durable=durable)
    try:
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            if workers > 1 and rename is None:
                feed = Feed(
                    partial(_write_member, landing),
                    _close_write,
                    max_tasks=_POOLED_TASKS * workers,
                    max_weight=_POOLED_BYTES,
                )
                # Returns, or raises, only once no write runs any more (directory times are restored after it).
                feed.run(_Reader(landing, archive, feed).read, workers)
            else:
                for member in archive:
                    _extract_member(landing, archive, member, rename)
            # tarfile ends the iteration quietly when the stream stops at a member boundary (nothing of the next
            # header read) or inside a header (part of it read): a whole block read where the next header starts
            # tells a cut stream from one that reached its end. Not detected: tarfile also ends quietly on a damaged
            # (non-zero, invalid) header after the first member, which reads a whole block too.
            consumed = getattr(archive.fileobj, "pos", None)
            if not isinstance(consumed, int) or consumed - archive.offset != tarfile.BLOCKSIZE:
                raise tarfile.ReadError("the stream ended before its end-of-archive marker")
        _restore_directory_times(landing)
        if durable:
            _fsync_directories(landing)
    finally:
        landing.close()
    return len(landing.written)


def publish_record(directory: Path, name: str, data: bytes, *, nonce: bytes, durable: bool) -> bool:
    """Create the record directory ``<directory>/<name>/`` holding the file ``record`` with *data*, at most once.

    The record is staged in a fresh dot-named directory beside it, ``.<name>.stage-<token>``, holding ``record``
    and ``.nonce``, and renamed onto *name* by :func:`publish_dir`. A staging an error left is removed; one a crash
    left is the caller's to sweep by its name.

    :param directory: The directory of the record; :func:`publish_dir`'s precondition holds: it is never pruned.
    :param name: The record's name.
    :param data: The exact content of ``record``.
    :param nonce: The publisher's unique nonce, which decides the win.
    :param durable: Fsync the files and directories.
    :return: Whether this call created the record.
    :raises FileNotFoundError: When *directory* is gone, or the staging vanished before the rename.
    :raises ValueError: When the staging's ``.nonce`` vanished before the rename.
    """

    _check_name(name)
    staging = loc(directory / f".{name[:64]}{STAGING_MARK}{fresh_token()}")
    os.mkdir(staging.path, 0o700)
    try:
        write_file(loc(staging.path / ".nonce"), nonce, durable=durable, mode=0o600)
        write_file(loc(staging.path / "record"), data, durable=durable, mode=0o600)
        return publish_dir(staging, loc(directory / name), nonce=nonce, durable=durable)
    except BaseException:
        if exists(staging):
            _remove_tree(staging)
        raise


@dataclass(frozen=True)
class Entry:
    """One entry of a validated tree.

    :param relative: The path below the walked root.
    :param kind: ``"dir"``, ``"file"`` or ``"symlink"``.
    :param size: The size of a file; ``0`` for a directory or a symlink.
    """

    relative: PurePosixPath
    kind: Literal["dir", "file", "symlink"]
    size: int


def walk_untrusted(root: Path, *, limits: WalkLimits = DEFAULT_LIMITS) -> tuple[Entry, ...]:
    """Validate and list a tree written by an untrusted party, never following a symlink.

    :param root: The directory to walk.
    :param limits: The accepted entry count, depth and total size.
    :return: Every entry below *root* in sorted, top-down order.
    :raises UntrustedContentError: For a special file, a hard-linked file, a name with NUL,
        too many, too deep or too large entries, or a root that is a symlink or not a directory.
    """

    top = PurePosixPath(".")
    info = os.lstat(root)
    if stat.S_ISLNK(info.st_mode):
        raise UntrustedContentError(top, "the root is a symlink")
    if not stat.S_ISDIR(info.st_mode):
        raise UntrustedContentError(top, "the root is not a directory")
    entries: list[Entry] = []
    total = 0
    pending: list[tuple[Path, PurePosixPath]] = [(root, PurePosixPath())]
    while pending:
        directory, relative = pending.pop()
        with os.scandir(directory) as listing:
            for item in listing:
                path = relative / item.name
                if "\0" in item.name:
                    raise UntrustedContentError(path, "the name contains NUL")
                if len(path.parts) > limits.depth:
                    raise UntrustedContentError(path, f"deeper than {limits.depth} levels")
                info = item.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    entries.append(Entry(path, "symlink", 0))
                elif stat.S_ISDIR(info.st_mode):
                    entries.append(Entry(path, "dir", 0))
                    pending.append((Path(item.path), path))
                elif stat.S_ISREG(info.st_mode):
                    if info.st_nlink > 1:
                        raise UntrustedContentError(path, "a hard-linked file")
                    entries.append(Entry(path, "file", info.st_size))
                    total += info.st_size
                    if total > limits.max_bytes:
                        raise UntrustedContentError(path, f"more than {limits.max_bytes} bytes in all")
                else:
                    raise UntrustedContentError(path, "a special file")
                if len(entries) > limits.entries:
                    raise UntrustedContentError(path, f"more than {limits.entries} entries")
    return tuple(sorted(entries, key=lambda entry: entry.relative))


def rename_probe(a: Path, b: Path) -> None:
    """Check that renames between two directories have the semantics the protocol relies on.

    A non-empty directory must move from *a* to *b* keeping its inode, and a
    non-empty directory renamed onto a non-empty directory must fail with both
    left in place. The probe entries are removed afterwards.

    :param a: The source directory.
    :param b: The destination directory.
    :raises FilesystemUnsupported: When the directories are on different filesystems or a check fails.
    """

    token = fresh_token()
    # Three distinct names, so that a and b may be the same directory.
    first, second, target = a / f".probe.{token}", a / f".probe.{token}.occupant", b / f".probe.{token}.moved"
    try:
        for directory in (first, second):
            os.mkdir(directory)
            os.close(os.open(directory / "probe", _CREATE_FLAGS, 0o600))
        inode = os.lstat(first).st_ino
        try:
            os.rename(first, target)
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                raise FilesystemUnsupported(f"{a} and {b} are on different filesystems") from exc
            raise FilesystemUnsupported(f"renaming a directory from {a} to {b} failed: {exc}") from exc
        if exists(loc(first)) or os.lstat(target).st_ino != inode:
            raise FilesystemUnsupported(f"a rename from {a} to {b} did not move the directory with its inode")
        try:
            os.rename(second, target)
        except OSError:
            # Expected: the outcome is checked by observation below.
            pass
        if not exists(loc(second)) or os.lstat(target).st_ino != inode:
            raise FilesystemUnsupported(f"renaming a non-empty directory onto a non-empty one succeeded in {b}")
    finally:
        for leftover in (first, second, target):
            _remove_tree(loc(leftover))


# -- the sync barrier (plan WORKFLOW_SYNC_BARRIER §8) ----------------------------------------------------------------

#: The values of the ``io.sync`` application setting; an absent setting is ``auto``.
SYNC_SETTINGS = ("auto", "syncfs", "fsync")
#: The filesystem types ``auto`` trusts ``syncfs`` on (Linux 5.8 or later): Lustre by the user's decision, the
#: local journalling filesystems whose writeback errors reach the superblock, and tmpfs (no persistence either way).
_SYNCFS_VETTED = frozenset({"lustre", "ext4", "xfs", "tmpfs"})
#: The ``errno`` values of a ``syncfs`` that is not there or not allowed (seccomp): the strategy becomes ``fsync``.
_SYNCFS_UNSUPPORTED = frozenset({errno.ENOSYS, errno.EPERM, errno.EINVAL})
#: The oldest kernel whose ``syncfs`` reports a writeback error (recorded on the superblock since the open).
_SYNCFS_KERNEL = (5, 8)
_MOUNTINFO = Path("/proc/self/mountinfo")

type SyncStrategy = Literal["syncfs", "fsync"]

#: The deepest directory the late walk descends into, in path components below the landing (as extract_tar).
_LANDING_DEPTH = 256
#: Whether ``syncfs`` works, by ``st_dev``, as the first prime or probe on that filesystem found.
_syncfs_works: dict[int, bool] = {}


class SyncFailed(OSError):
    """A barrier's commit failed: the landing is not known to be durable and must not be published.

    A subclass of :class:`OSError`, so that every handler of filesystem errors treats it as one.
    """


class SyncUnreadable(SyncFailed):
    """A walk of strategy ``fsync`` could not list or open an entry (its permissions, its type, its depth).

    No writeback error was reported: the entry, which the message names, was never synced.
    """


@cache
def _syncfs_function() -> Callable[[int], int] | None:
    # The libc function with its argument types, or None when libc has no syncfs (resolved once).
    try:
        import ctypes

        found = ctypes.CDLL(None, use_errno=True).syncfs
    except (ImportError, OSError, AttributeError):
        # No _ctypes in this Python, no libc to load, or a libc without syncfs.
        return None
    found.argtypes = (ctypes.c_int,)
    found.restype = ctypes.c_int
    return found


def _syncfs(descriptor: int) -> None:
    """Call ``syncfs`` on *descriptor*: write back and wait for every dirty page and inode of its filesystem.

    The call releases the GIL. From Linux 5.8 it reports a writeback error recorded on the filesystem since
    *descriptor* was opened.

    :param descriptor: Any open descriptor on the filesystem.
    :raises OSError: With the call's ``errno``; ``ENOSYS`` when libc has no ``syncfs``.
    """

    function = _syncfs_function()
    if function is None:
        raise OSError(errno.ENOSYS, "libc has no syncfs")
    import ctypes

    if function(descriptor) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _uname() -> tuple[str, str]:
    # The system name and kernel release (a seam for tests).
    system = os.uname()
    return system.sysname, system.release


def _read_mountinfo() -> str | None:
    # This process's mount table, or None when it cannot be read (a seam for tests).
    try:
        return _MOUNTINFO.read_text(encoding="utf-8", errors="surrogateescape")
    except OSError:
        return None


def _kernel_version(release: str) -> tuple[int, int] | None:
    # The leading major.minor of a release string ("5.14.0-362.el9.x86_64" -> (5, 14)); None when it has none.
    found = re.match(r"(\d+)\.(\d+)(?!\d)", release)
    return None if found is None else (int(found[1]), int(found[2]))


def _unescape(field: str) -> str:
    # mountinfo writes space, tab, newline and backslash in a mount point as a backslash and three octal digits.
    return re.sub(r"\\([0-7]{3})", lambda escape: chr(int(escape[1], 8)), field)


def _filesystem_type(path: Path, device: int) -> str | None:
    """The type of the filesystem *path* (on *device*) is on, from ``/proc/self/mountinfo``; ``None`` if unknown.

    The entry whose ``major:minor`` is *device*; when none is (a btrfs subvolume reports an anonymous device), the
    entry with the longest mount point that is a path-component prefix of the resolved *path*. Read afresh each
    time: an anonymous device number is reused after an unmount.
    """

    text = _read_mountinfo()
    if text is None:
        return None
    wanted = f"{os.major(device)}:{os.minor(device)}"
    resolved = PurePosixPath(os.path.realpath(path))
    found: str | None = None
    longest = -1
    for line in text.splitlines():
        fields = line.split(" ")
        if len(fields) < 7 or "-" not in fields[6:]:
            continue
        separator = fields.index("-", 6)
        if separator + 1 >= len(fields):
            continue
        kind = fields[separator + 1]
        if fields[2] == wanted:
            found = kind
            break
        mount_point = PurePosixPath(_unescape(fields[4]))
        if resolved.is_relative_to(mount_point) and len(mount_point.parts) > longest:
            found, longest = kind, len(mount_point.parts)
    return found


def resolve_sync(path: Path, setting: object) -> tuple[SyncStrategy, str]:
    """Choose how a landing below *path* is made durable: ``syncfs`` of its filesystem, or ``fsync`` per file.

    *setting* is the ``io.sync`` value: ``auto`` gives ``syncfs`` only on Linux 5.8 or later, with libc's
    ``syncfs`` working on that filesystem (not refused by a prime or probe before) and a vetted filesystem type
    (``lustre``, ``ext4``, ``xfs``, ``tmpfs``); ``syncfs`` forces it on any filesystem type, but still gives
    ``fsync``, with one warning, below Linux 5.8, off Linux, or where ``syncfs`` does not work; ``fsync`` gives
    ``fsync``; any other value gives ``fsync`` with one warning. The setting is the caller's fresh read; only the
    filesystem type is read afresh from the mount table each time.

    :param path: An existing directory on the filesystem written.
    :param setting: The ``io.sync`` value.
    :return: The strategy and a short reason for display, such as ``"syncfs (lustre, Linux 5.14)"`` or
        ``"fsync (zfs is not vetted for auto)"``.
    :raises OSError: When *path* cannot be examined.
    """

    if setting not in SYNC_SETTINGS:
        _LOGGER.warning("io.sync is %r, not one of auto, syncfs or fsync: using fsync", setting)
        return "fsync", f"fsync (io.sync {setting!r} is not auto, syncfs or fsync)"
    if setting == "fsync":
        return "fsync", "fsync (io.sync is fsync)"
    forced = setting == "syncfs"
    device = os.stat(path).st_dev

    def fallback(why: str) -> tuple[SyncStrategy, str]:
        if forced:
            _LOGGER.warning("io.sync is syncfs, but %s: using fsync", why)
        return "fsync", f"fsync ({why})"

    system, release = _uname()
    if system != "Linux":
        return fallback(f"{system} is not Linux")
    version = _kernel_version(release)
    if version is None:
        return fallback(f"the kernel release {release!r} is not recognized")
    kernel = f"Linux {version[0]}.{version[1]}"
    if version < _SYNCFS_KERNEL:
        return fallback(f"{kernel} is older than 5.8")
    if _syncfs_function() is None:
        return fallback("libc has no syncfs")
    if _syncfs_works.get(device) is False:
        return fallback("syncfs is not supported on this filesystem here")
    kind = _filesystem_type(path, device)
    if forced:
        return "syncfs", f"syncfs (forced, {kind or 'unknown filesystem'}, {kernel})"
    if kind is None:
        return "fsync", "fsync (the filesystem type is unknown)"
    if kind not in _SYNCFS_VETTED:
        return "fsync", f"fsync ({kind} is not vetted for auto)"
    return "syncfs", f"syncfs ({kind}, {kernel})"


def _prime(descriptor: int, device: int) -> bool:
    # One syncfs on a fresh descriptor: whether syncfs works on the filesystem (recorded by st_dev). Any other
    # error is logged and ignored: nothing of the landing is written yet, so it is not ours.
    try:
        _syncfs(descriptor)
    except OSError as exc:
        if exc.errno in _SYNCFS_UNSUPPORTED:
            _syncfs_works[device] = False
            return False
        _LOGGER.warning("the sync barrier's first syncfs failed before anything was landed (ignored): %s", exc)
    _syncfs_works[device] = True
    return True


def _probe_syncfs(path: Path) -> bool:
    # Whether syncfs works on the filesystem of *path*, recorded for resolve_sync as a barrier's prime records it.

    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        return _prime(descriptor, os.fstat(descriptor).st_dev)
    finally:
        os.close(descriptor)


def fsync_landing(anchor: int, *, label: str = "") -> int:
    """Fsync a landed tree after the fact: every regular file, then every directory bottom-up, then *anchor*.

    Each directory is opened ``O_DIRECTORY|O_NOFOLLOW`` relative to its parent's descriptor and each file through
    an anchored ``O_NOFOLLOW|O_NONBLOCK`` open; symlinks and special files are skipped, never followed. A later
    opener than the writer misses a writeback error already reported to someone else or lost when the inode was
    evicted (the accepted limit of the strategy ``fsync`` for a landing another process wrote).

    :param anchor: The directory descriptor of the landing (left open).
    :param label: The landing's path, for messages.
    :return: How many regular files were fsynced.
    :raises SyncUnreadable: When an entry cannot be listed or opened, or a directory lies deeper than 256 levels
        (which bounds the descriptors the walk keeps open).
    :raises SyncFailed: When an fsync reports an error.
    """

    def unreadable(error: int | None, where: str, why: str | None) -> SyncUnreadable:
        return SyncUnreadable(error, f"cannot read {where} to sync it to disk: {why}")

    count = 0
    try:
        listing = sorted(os.listdir(anchor))
    except OSError as exc:
        raise unreadable(exc.errno, label, exc.strerror) from exc
    # Each directory open below the anchor (the caller owns the anchor): its descriptor, path and names left.
    pending: list[tuple[int, PurePosixPath, list[str]]] = [(anchor, PurePosixPath(), listing)]
    try:
        while pending:
            directory, relative, names = pending[-1]
            if not names:
                pending.pop()
                try:
                    os.fsync(directory)
                except OSError as exc:
                    raise SyncFailed(exc.errno, f"cannot sync {label}/{relative} to disk: {exc.strerror}") from exc
                finally:
                    if directory != anchor:
                        os.close(directory)
                continue
            name = names.pop()
            path = relative / name
            try:
                mode = os.lstat(name, dir_fd=directory).st_mode
                if stat.S_ISDIR(mode):
                    if len(path.parts) > _LANDING_DEPTH:
                        raise unreadable(errno.ELOOP, f"{label}/{path}", f"deeper than {_LANDING_DEPTH} levels")
                    child = os.open(name, _DIR_FLAGS, dir_fd=directory)
                    try:
                        listing = sorted(os.listdir(child))
                    except BaseException:
                        os.close(child)
                        raise
                    pending.append((child, path, listing))
                    continue
                descriptor = os.open(name, _SOURCE_FLAGS, dir_fd=directory) if stat.S_ISREG(mode) else None
            except SyncFailed:
                raise
            except OSError as exc:
                raise unreadable(exc.errno, f"{label}/{path}", exc.strerror) from exc
            if descriptor is None:
                continue  # symlinks and special files: nothing of theirs to sync
            try:
                os.fsync(descriptor)
            except OSError as exc:
                raise SyncFailed(exc.errno, f"cannot sync {label}/{path} to disk: {exc.strerror}") from exc
            finally:
                os.close(descriptor)
            count += 1
    finally:
        for directory, _relative, _names in pending:
            if directory != anchor:
                os.close(directory)
    return count


class Barrier:
    """Make everything landed below *anchor* durable once, before any of it is published (plan §8.1).

    Entered before the first byte is landed: the strategy is resolved (:func:`resolve_sync`) and *anchor* opened.
    For ``syncfs`` a first ``syncfs`` on that fresh descriptor ("prime") flushes what was dirty already and
    consumes an unseen writeback error of someone else; its error is logged and ignored, and a ``syncfs`` that is
    not supported there makes the strategy ``fsync``. :meth:`commit` then calls ``syncfs`` on the same descriptor,
    so any writeback error since the open, ours or not, fails it: an error is never attributed, the landing is
    not published. Under ``fsync``, a caller whose writes fsync themselves (:attr:`fsync_inline`) passes
    *inline*, and the commit has nothing left to do but say so; otherwise it fsyncs the landing after the fact
    (:func:`fsync_landing`). Leaving without a commit syncs nothing. A running ``syncfs`` cannot be interrupted.

    Trees written before the barrier could be entered (operators' bundles unpacked by scp or tar) take *prime*
    false (:func:`sync_written`): no prime consumes a writeback error of their files, so the commit's ``syncfs``
    reports one that nobody has seen yet (best effort: one already reported to someone else, or lost with an
    evicted inode, is not), and also any unseen one of someone else on that filesystem; where ``syncfs`` turns
    out not to be supported, the commit walks its *tree* instead.

    :param anchor: The landing directory, which the caller has created durably.
    :param setting: The ``io.sync`` value, read fresh by the caller.
    :param inline: The caller writes every file of the landing with fsync on its writing descriptor when
        :attr:`fsync_inline` is true (an extraction or a copy), rather than another process writing it.
    :param quiet: Print no progress line.
    :param prime: Prime the barrier at entry; false only for a tree written before it was entered.
    """

    def __init__(
        self, anchor: Path, setting: object, *, inline: bool = False, quiet: bool = False, prime: bool = True
    ) -> None:
        self.anchor = anchor
        self.setting = setting
        self.inline = inline
        self.quiet = quiet
        self.prime = prime
        self.strategy: SyncStrategy = "fsync"
        self.reason = ""
        self._descriptor: int | None = None

    @property
    def fsync_inline(self) -> bool:
        """Whether the caller must fsync each file as it writes it (the strategy is ``fsync``)."""

        return self.strategy == "fsync"

    def __enter__(self) -> Self:
        self.strategy, self.reason = resolve_sync(self.anchor, self.setting)
        self._descriptor = os.open(self.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            if (
                self.strategy == "syncfs"
                and self.prime
                and not _prime(self._descriptor, os.fstat(self._descriptor).st_dev)
            ):
                if self.setting == "syncfs":
                    _LOGGER.warning("io.sync is syncfs, but syncfs is not supported here: using fsync")
                self.strategy, self.reason = "fsync", "fsync (syncfs is not supported on this filesystem here)"
        except BaseException:
            self._close()
            raise
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._close()

    def _close(self) -> None:
        if self._descriptor is not None:
            descriptor, self._descriptor = self._descriptor, None
            os.close(descriptor)

    def _say(self, verb: str, count_hint: int | None, unit: str) -> None:
        if self.quiet:
            return
        what = f"the landed {unit}s" if count_hint is None else f"{count_hint} landed {unit}(s)"
        strategy, _, detail = self.reason.partition(" (")
        print(f"{verb.format(what)} ({strategy}: {detail.removesuffix(')')})", file=sys.stderr, flush=True)

    def commit(self, count_hint: int | None = None, *, unit: str = "file", tree: "Path | Loc | None" = None) -> None:
        """Make the landing durable: ``syncfs`` on the anchor's descriptor, or the late walk of strategy ``fsync``.

        A barrier may commit several times, one landing below *anchor* after another, each before its own
        publication: every ``syncfs`` on the one descriptor reports a writeback error recorded since the previous
        one (or since the prime), so each commit covers what was written since the last, and a failed one fails
        only what it was to publish. Under ``fsync`` the walk covers *tree*, that landing alone.

        Unless *quiet*, a progress line naming the strategy goes to standard error: ``syncing N landed file(s) to
        disk (syncfs: lustre, Linux 5.14)`` before a ``syncfs`` or a walk, and ``made N landed file(s) durable
        (fsync: ...)`` for an *inline* landing under ``fsync``, whose files were fsynced as they were written (so
        nothing is left to do).

        :param count_hint: How many were landed, for the progress line.
        :param unit: What *count_hint* counts, for the progress line: ``file`` or ``bundle``.
        :param tree: The directory (or anchored location of one) below *anchor*, or *anchor* itself (the default),
            that the walk of strategy ``fsync`` covers.
        :raises SyncFailed: When the ``syncfs`` or the walk fails (:class:`SyncUnreadable` when the walk cannot
            read an entry); it is logged once as a warning naming the strategy.
        :raises ValueError: Outside the context.
        """

        if self._descriptor is None:
            raise ValueError("a barrier commits only inside its context")
        if self.strategy == "fsync" and self.inline:
            self._say("made {} durable", count_hint, unit)
            return
        self._say("syncing {} to disk", count_hint, unit)
        if self.strategy == "fsync":
            self._walk(tree)
            return
        try:
            _syncfs(self._descriptor)
        except OSError as exc:
            if not self.prime and exc.errno in _SYNCFS_UNSUPPORTED:
                # No prime probed it: syncfs does not work here, so the tree is walked instead.
                _syncfs_works[os.fstat(self._descriptor).st_dev] = False
                self.strategy, self.reason = "fsync", "fsync (syncfs is not supported on this filesystem here)"
                self._walk(tree)
                return
            _LOGGER.warning("the sync barrier (%s) of %s failed: %s", self.reason, self.anchor, exc)
            raise SyncFailed(
                exc.errno, f"syncfs of {self.anchor} failed, so what landed there is not durable: {exc.strerror}"
            ) from exc

    def _walk(self, tree: "Path | Loc | None") -> None:
        # The late walk of strategy fsync over *tree* (the anchor by default), logged once when it fails.
        assert self._descriptor is not None
        label = str(self.anchor if tree is None else tree.path if isinstance(tree, Loc) else tree)
        try:
            if tree is None:
                fsync_landing(self._descriptor, label=label)
                return
            try:
                if isinstance(tree, Loc):
                    descriptor = os.open(str(tree.path), _DIR_FLAGS, dir_fd=tree.at)
                else:
                    descriptor = os.open(tree, _DIR_FLAGS)
            except OSError as exc:
                raise SyncUnreadable(exc.errno, f"cannot read {label} to sync it to disk: {exc.strerror}") from exc
            try:
                fsync_landing(descriptor, label=label)
            finally:
                os.close(descriptor)
        except SyncFailed as exc:
            _LOGGER.warning("the sync barrier (%s) of %s failed: %s", self.reason, label, exc)
            raise


def sync_written(anchor: Path, trees: Sequence[Path | Loc], setting: object) -> list[Path | Loc]:
    """Make trees that were written before httk ran (operators' bundles or payloads) durable, together.

    Only the trees that are directories on the filesystem of *anchor* are synced (another filesystem's are copied
    by their caller, which fsyncs the copy): one ``syncfs`` on a descriptor opened on *anchor* now, with no prime,
    for all of them, or under ``fsync`` a walk of each. No barrier could precede their first byte, so this is best
    effort: a writeback error is reported only while nobody has seen it yet, and an unseen one of someone else on
    that filesystem fails the call too. An error cannot be attributed to one tree; once reported, it is seen, so
    the failed pages may read back intact from memory while not on disk: every tree must then be re-created from
    its source, never merely synced again.

    :param anchor: An existing directory on the filesystem written (the workspace's control directory).
    :param trees: The trees: absolute paths, or anchored locations (a trusted name below a directory descriptor).
    :param setting: The ``io.sync`` value, read fresh by the caller.
    :return: The trees synced (those on *anchor*'s filesystem).
    :raises SyncFailed: When the ``syncfs`` or a walk fails (:class:`SyncUnreadable` when a walk cannot read an
        entry).
    :raises OSError: When *anchor* cannot be opened.
    """

    device = os.stat(anchor).st_dev
    synced: list[Path | Loc] = []
    for tree in trees:
        try:
            status = lstat(tree) if isinstance(tree, Loc) else os.lstat(tree)
        except OSError:
            continue  # gone or unreachable (ENOTDIR, EACCES, ELOOP, ...): its caller's take reports it
        if status is None:
            continue
        if stat.S_ISDIR(status.st_mode) and status.st_dev == device:
            synced.append(tree)
    if not synced:
        return synced
    with Barrier(anchor, setting, quiet=True, prime=False) as barrier:
        pending = synced
        if barrier.strategy == "syncfs":
            # One syncfs for every tree; where it turns out not to be supported, the first tree is walked instead.
            barrier.commit(tree=synced[0])
            if barrier.strategy == "syncfs":
                return synced
            pending = synced[1:]
        for tree in pending:
            barrier.commit(tree=tree)
    return synced
