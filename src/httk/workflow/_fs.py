"""Filesystem primitives: the only module that renames, replaces, unlinks or exclusively creates.

Every outcome is decided by observation (``lstat`` of the source and the
destination), never by a return code: NFS can report an error for a rename that
happened (a retransmission), and ``ENOENT`` can come from a concurrently pruned
parent directory. Only atomic rename and basic POSIX are used: no file locks,
no hard links, no symlinks created. Plain :class:`Loc` values address trusted
directories; anchored ones address a name below a directory descriptor that an
untrusted party may write.
"""

import base64
import enum
import errno
import logging
import os
import re
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, Protocol

from httk.workflow.errors import FormatError, WorkflowError

_LOGGER = logging.getLogger(__name__)

_ATTEMPTS = 8
_SETTLE_STEP = 0.05
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_OCCUPIED_ERRNOS = frozenset({errno.EEXIST, errno.ENOTEMPTY, errno.ENOTDIR, errno.EISDIR})


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
    """A path is a symlink, a special file or otherwise not the plain entry expected."""


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
            raise UnsafePath(f"{path} is a symlink or not a directory") from exc
        raise


def open_dir_under(root: Path, relative: str | PurePosixPath, *, create: bool = False, mode: int = 0o700) -> int:
    """Open a directory below a trusted root, never following a symlink below the root.

    *root* is a trusted anchor and is followed; each component of *relative* is
    opened ``O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`` relative to the previous one.

    :param root: The trusted directory to start from.
    :param relative: The directory below *root*; ``..``, absolute paths and empty components are refused.
    :param create: Create a missing component with *mode*, like ``mkdir(parents=True)``.
    :param mode: The permission bits of a created directory (before the umask).
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
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for name in parts:
            if create:
                try:
                    os.mkdir(name, mode, dir_fd=descriptor)
                except FileExistsError:
                    pass
            try:
                child = os.open(name, _DIR_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                # Linux reports a symlink opened O_DIRECTORY|O_NOFOLLOW as ENOTDIR, not ELOOP.
                if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise UnsafePath(f"{root / relative}: {name} is a symlink or not a directory") from exc
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


def _lstat(target: Loc) -> os.stat_result | None:
    try:
        return os.lstat(target.path, dir_fd=target.at)
    except (FileNotFoundError, NotADirectoryError):
        return None


def exists(target: Loc) -> bool:
    """Report whether an entry exists, without following a symlink.

    :param target: The location to observe.
    :return: Whether ``lstat`` finds an entry.
    """

    return _lstat(target) is not None


def fresh_token() -> str:
    """Return 80 random bits as 16 lowercase base32 characters."""

    return base64.b32encode(os.urandom(10)).decode("ascii").lower()


def remove_write_temporaries(directory: Path, name: str, *, durable: bool) -> int:
    """Remove the temporaries an interrupted :func:`write_file` of *name* left in *directory*.

    Only regular files named ``.<name>.<token>.tmp`` are removed; call this only where every such
    name is the caller's own.

    :param directory: The directory holding *name*.
    :param name: The target file name.
    :param durable: Durability of the removals.
    :return: The number of temporaries removed.
    """

    pattern = re.compile(rf"\.{re.escape(name)}\.[a-z2-7]{{16}}\.tmp")
    removed = 0
    for entry in os.listdir(directory):
        info = _lstat(Loc(directory / entry)) if pattern.fullmatch(entry) else None
        if info is not None and stat.S_ISREG(info.st_mode):
            remove_file(Loc(directory / entry), durable=durable)
            removed += 1
    return removed


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
    concurrently (by a pruner) makes the walk start again.

    :param path: The absolute directory path.
    :param durable: fsync the parent of every directory created.
    :param mode: The permission bits of the created directories (before the umask).
    :raises UnsafePath: When a component is a symlink or not a directory.
    :raises MoveFailed: When concurrent removals defeat every attempt.
    """

    for _ in range(_ATTEMPTS):
        # Walk up to the deepest existing ancestor; each component examined must be a real directory.
        missing: list[Path] = []
        current = path
        while (info := _lstat(Loc(current))) is None:
            missing.append(current)
            current = current.parent
        if not stat.S_ISDIR(info.st_mode):
            raise UnsafePath(f"{current} is a symlink or not a directory")
        try:
            for directory in reversed(missing):
                try:
                    os.mkdir(directory, mode)
                except FileExistsError:
                    # A concurrent mkdir is success, provided it made a directory and not a symlink.
                    created = _lstat(Loc(directory))
                    if created is not None and not stat.S_ISDIR(created.st_mode):
                        raise UnsafePath(f"{directory} is a symlink or not a directory") from None
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


def move_once(src: Loc, dst: Loc, *, durable: bool, settle: float = 0.0) -> Moved:
    """Move a contested entry to a fresh name that, once there, only the caller can move.

    :param src: The entry other actors may also try to move.
    :param dst: A fresh name (it contains a fresh token or an owner id).
    :param durable: Fsync both parent directories after a win.
    :param settle: Seconds to keep re-observing *dst* when both names look absent.
    :return: :attr:`Moved.WON` when *dst* was observed, else :attr:`Moved.LOST`.
    :raises MoveFailed: When the source stayed in place after every attempt.
    :raises CrossDevice: For a rename across filesystems.
    """

    error: OSError | None = None
    for _ in range(_ATTEMPTS):
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
        # attribute caching). Only dst can decide; the name src cannot legitimately come back.
        deadline = time.monotonic() + settle
        while (remaining := deadline - time.monotonic()) > 0:
            time.sleep(min(_SETTLE_STEP, remaining))
            if exists(dst):
                if durable:
                    _fsync_parents(src, dst)
                return Moved.WON
        return Moved.LOST
    raise MoveFailed(
        f"{src.path} stayed in place after {_ATTEMPTS} attempts", error.errno if error else None
    ) from error


def move_owned(src: Loc, dst: Loc, *, durable: bool) -> None:
    """Move an entry only the caller can move, possibly replacing a file or symlink at *dst*.

    The outcome is decided by the source alone: *dst* may have existed before
    (a replacing rename) or a successor may already have moved it on.

    :param src: The caller-owned entry.
    :param dst: The destination name.
    :param durable: Fsync both parent directories after the move.
    :raises MoveFailed: When the source stayed in place after every attempt; ``errno`` carries the last error.
    :raises CrossDevice: For a rename across filesystems.
    """

    error: OSError | None = None
    for _ in range(_ATTEMPTS):
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


def deliver(src: Loc, dst: Loc, *, token_name: str, token: bytes, durable: bool) -> Delivered:
    """Move a directory carrying a token file to a name another party may also occupy.

    Precondition: the destination's parent is never pruned; unlike :func:`move_once`, there is no
    pruned-parent retry.

    :param src: The caller-owned directory holding ``token_name``.
    :param dst: The destination name.
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
    if _read_child(dst, token_name, len(token)) == token:
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


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(descriptor, view) :]


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
            _write_all(descriptor, data)
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
            landed = _lstat(dst)
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
        _write_all(descriptor, data)
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
            if exc.errno in (errno.ELOOP, errno.ENXIO, errno.EISDIR):
                raise UnsafePath(f"{target.path} is not a regular file") from exc
            raise
    try:
        # A FIFO with a reader, or a device, opens: only a regular file is accepted.
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise UnsafePath(f"{target.path} is not a regular file")
        if durable and created:
            _fsync_parent(target)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def append_file(target: Loc, data: bytes, *, durable: bool, mode: int = 0o644) -> None:
    """Append *data* to a regular file, creating it when absent; one appender per file.

    :param target: The file to append to.
    :param data: The bytes to append.
    :param durable: Fsync the file, and its directory when this call created it.
    :param mode: The permission bits of a newly created file (before the umask).
    :raises UnsafePath: When *target* is a symlink, a FIFO, a directory or any other non-regular file.
    """

    descriptor = open_append(target, durable=durable, mode=mode)
    try:
        _write_all(descriptor, data)
        if durable:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_bounded(src: Loc, limit: int, *, nonblock: bool = False) -> bytes | None:
    """Read a regular file of at most *limit* bytes without following a symlink.

    :param src: The file to read.
    :param limit: The largest accepted size in bytes.
    :param nonblock: Open with ``O_NONBLOCK`` so a FIFO planted by an untrusted party cannot block.
    :return: The content, or ``None`` when *src* does not exist.
    :raises UnsafePath: For a symlink or anything but a regular file.
    :raises TooLarge: For content longer than *limit*.
    """

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | (os.O_NONBLOCK if nonblock else 0)
    try:
        descriptor = os.open(src.path, flags, dir_fd=src.at)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise UnsafePath(f"{src.path} is a symlink") from exc
        raise
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise UnsafePath(f"{src.path} is not a regular file")
        data = bytearray()
        while len(data) <= limit and (chunk := os.read(descriptor, min(1 << 20, limit + 1 - len(data)))):
            data += chunk
    finally:
        os.close(descriptor)
    if len(data) > limit:
        raise TooLarge(f"{src.path} is larger than {limit} bytes")
    return bytes(data)


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


def _open_listing(parent: int | None, name: str) -> tuple[int, list[str]] | None:
    # None when the directory vanished or was swapped for a non-directory: the next pass sees it.
    try:
        descriptor = os.open(name, _DIR_FLAGS, dir_fd=parent)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
            return None
        raise
    return descriptor, os.listdir(descriptor)


def _remove_pass(at: int | None, name: str) -> bool:
    # One bottom-up pass over descriptors opened O_NOFOLLOW; False when it must be repeated.
    if _remove_leaf(at, name):
        return True
    stack: list[tuple[int | None, str, int, list[str]]] = []
    complete = True
    try:
        opened = _open_listing(at, name)
        if opened is None:
            return False
        stack.append((at, name, *opened))
        while stack:
            parent, child, descriptor, names = stack[-1]
            if names:
                entry = names.pop()
                if not _remove_leaf(descriptor, entry):
                    opened = _open_listing(descriptor, entry)
                    if opened is None:
                        complete = False
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

    :param target: The plain location to discard.
    :param trash_dir: A directory on the same filesystem for the intermediate trash name.
    :param durable: Make the move out of *target*'s directory durable.
    :raises ValueError: For an anchored *target*.
    """

    if target.at is not None:
        raise ValueError("discard takes a plain location")
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


def _copy_regular(src: str, dst: str) -> None:
    # copytree hands over every non-directory; a FIFO or device would block or leak, so only regular files copy.
    if not stat.S_ISREG(os.lstat(src).st_mode):
        raise UnsafePath(f"{src} is a special file")
    shutil.copy2(src, dst)


def copy_tree(src: Path, dst: Path, *, durable: bool) -> None:
    """Copy a tree to a fresh name, possibly on another filesystem; symlinks are copied as symlinks.

    The copy is not atomic: callers copy to a private or partial name and move or deliver it afterwards.

    :param src: The directory to copy.
    :param dst: The destination, which must not exist; its parent is created.
    :param durable: Fsync every copied file and directory, and the parent of *dst*.
    :raises FileExistsError: When *dst* exists.
    :raises UnsafePath: For a special file in *src*.
    """

    if exists(loc(dst)):
        raise FileExistsError(errno.EEXIST, "the copy destination exists", str(dst))
    make_dirs(dst.parent, durable=durable)
    shutil.copytree(src, dst, symlinks=True, copy_function=_copy_regular)
    if not durable:
        return
    for directory, _, files in os.walk(dst):
        for name in files:
            path = Path(directory, name)
            if not path.is_symlink():
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        descriptor = os.open(directory, _DIR_FLAGS)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    _fsync_parent(loc(dst))


@dataclass(frozen=True)
class WalkLimits:
    """Bounds of :func:`walk_untrusted`.

    :param entries: The largest accepted number of entries.
    :param depth: The deepest accepted entry, counted in path components below the root.
    """

    entries: int = 1_000_000
    depth: int = 256


DEFAULT_LIMITS = WalkLimits()


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
    :param limits: The accepted entry count and depth.
    :return: Every entry below *root* in sorted, top-down order.
    :raises UntrustedContentError: For a special file, a hard-linked file, a name with NUL,
        too many or too deep entries, or a root that is a symlink or not a directory.
    """

    top = PurePosixPath(".")
    info = os.lstat(root)
    if stat.S_ISLNK(info.st_mode):
        raise UntrustedContentError(top, "the root is a symlink")
    if not stat.S_ISDIR(info.st_mode):
        raise UntrustedContentError(top, "the root is not a directory")
    entries: list[Entry] = []
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
