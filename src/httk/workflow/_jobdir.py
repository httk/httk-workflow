"""Descriptor-anchored access to the manager's control paths in one job directory.

A job directory is written by the job it holds. Whatever the job leaves there
between attempts, or while a fenced process is still running after it
published, must neither redirect a manager write outside the directory nor
hang or crash the manager. :class:`JobDirectory` therefore pins the directory
once through a chain of no-follow directory descriptors and anchors every later
operation on a descriptor:

- every component below the anchor is opened with
  ``O_DIRECTORY|O_NOFOLLOW``, so a symlink planted anywhere on a control path
  is refused rather than followed;
- files are opened ``O_NOFOLLOW|O_NONBLOCK`` and checked with ``fstat``, so a
  symlink, FIFO, device or other special file is refused without blocking;
- reads are bounded, so an oversized control document is refused rather than
  loaded;
- replacements are written to an ``O_EXCL`` temporary entry and renamed into
  place within the same directory descriptor, which never follows a symlink at
  the destination name.

A refusal is a :class:`JobDirectoryError`, a
:class:`~httk.workflow.errors.FormatError`, which the manager records as a
``protocol_error`` of that job alone. A missing entry keeps raising the
ordinary :class:`FileNotFoundError`.

The anchor itself is trusted: :meth:`JobDirectory.open` opens the (resolved)
workspace root and the placement directories, which are operator layout and
are followed as the workspace follows them, and then the job directory without
following it; :meth:`JobDirectory.at` trusts a whole path supplied by a caller
that only holds a payload path.
"""

import errno
import fcntl
import hashlib
import json
import os
import secrets
import shutil
import stat
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any, Self

from httk.core.digests import tree_digest

from .errors import FormatError

__all__ = ["CONTROL_DOCUMENT_LIMIT", "JobDirectory", "JobDirectoryError"]

#: The largest JSON control document the manager reads from a job directory:
#: ``outcome.json``, ``spawn.json``, the environment-resolution marker,
#: transaction manifests, spawn records, ``detached.json`` and seals.
CONTROL_DOCUMENT_LIMIT = 16 * 1024 * 1024

_READ_CHUNK = 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_ANCHOR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_APPEND_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_EXCLUSIVE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
#: What an open or rename reports when a component is a symlink, is not a
#: directory, is a directory where a file belongs, or is a FIFO without a reader.
_REFUSED_ERRNOS = frozenset({errno.ELOOP, errno.ENOTDIR, errno.EISDIR, errno.ENXIO})


class JobDirectoryError(FormatError):
    """A control path in a job directory is a symlink, special file, or oversized."""


def _components(relative: str | PurePosixPath) -> tuple[str, ...]:
    """Validate one relative control path and return its components."""

    if isinstance(relative, PurePosixPath):
        if relative.is_absolute():
            raise JobDirectoryError(f"control path must be relative: {relative}")
        parts = relative.parts
    elif isinstance(relative, str):
        if relative.startswith("/"):
            raise JobDirectoryError(f"control path must be relative: {relative}")
        parts = tuple(relative.split("/"))
    else:
        raise JobDirectoryError(f"control path must be a string or a POSIX path: {relative!r}")
    if not parts:
        raise JobDirectoryError("control path is empty")
    for part in parts:
        _check_component(part, relative)
    return parts


def _check_component(part: str, whole: object) -> None:
    if part in ("", ".", "..") or "\0" in part or "/" in part:
        raise JobDirectoryError(f"control path has an invalid component {part!r}: {whole}")


def _refused(exc: OSError, what: str) -> Exception:
    """Map a refusal errno to :class:`JobDirectoryError`; keep other errors."""

    if exc.errno in _REFUSED_ERRNOS:
        return JobDirectoryError(f"{what} is a symlink or not of the expected type")
    return exc


def _close(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        pass


def _open_child(
    parent: int, name: str, *, create: bool, exclusive: bool, mode: int, shown: Path, follow: bool = False
) -> int:
    """Open (and create) one directory component below *parent*, without following it unless *follow*."""

    if create:
        try:
            os.mkdir(name, mode, dir_fd=parent)
        except FileExistsError:
            if exclusive:
                raise
    try:
        return os.open(name, _DIRECTORY_FLAGS & ~os.O_NOFOLLOW if follow else _DIRECTORY_FLAGS, dir_fd=parent)
    except OSError as exc:
        raise _refused(exc, f"job directory component {shown}") from exc


class JobDirectory:
    """One job (or control) directory pinned through a no-follow descriptor chain.

    Instances own their descriptor and are context managers; use
    :meth:`open`, :meth:`at` or :meth:`directory` to obtain one. Relative paths
    given to the methods are strings or :class:`~pathlib.PurePosixPath` values
    without absolute, empty, ``.``, ``..`` or NUL components; every intermediate
    component is opened ``O_DIRECTORY|O_NOFOLLOW``.

    A handle opened with ``follow_symlinks=True`` (only through :meth:`at`,
    for a caller acting in a tree it owns, such as a runner replaying a batch
    into its own workdir) follows symlinks in directory walks, :meth:`stat`
    and reads instead; its writes, removals and renames still never follow the
    final name.

    :param descriptor: An open directory descriptor this instance takes over.
    :param path: The path the descriptor was opened at, for messages and for
        paths handed to processes.
    :param follow_symlinks: Follow symlinks below the anchor (see above).
    """

    def __init__(self, descriptor: int, path: Path, *, follow_symlinks: bool = False) -> None:
        self._fd = descriptor
        self._path = path
        self._follow = follow_symlinks

    @classmethod
    def open(cls, *, jobs: Path, placement: PurePosixPath, job_key: str) -> Self:
        """Open one job directory of a workspace without following a link at or below it.

        The workspace root and the placement directories are operator layout
        and may be symlinks (``project -> /scratch/project``), so they are
        followed; the job directory itself, the job-key component, is opened
        ``O_NOFOLLOW``, and so is everything the handle reaches below it.

        :param jobs: The workspace ``jobs/`` directory, trusted and followed as the
            workspace resolves it.
        :param placement: The job's relative placement, followed.
        :param job_key: The job key naming the job directory.
        :return: The pinned job directory.
        :raises JobDirectoryError: If the job directory is a symlink or not a
            directory, or a component is invalid.
        :raises OSError: If the jobs root or a placement directory cannot be
            opened, including :class:`FileNotFoundError` for a missing job directory.
        """

        if placement.is_absolute():
            raise JobDirectoryError(f"placement must be relative: {placement}")
        for name in (*placement.parts, job_key):
            _check_component(name, placement / job_key)
        anchor = jobs.joinpath(*placement.parts)
        descriptor = os.open(anchor, _ANCHOR_FLAGS)
        shown = anchor / job_key
        try:
            child = _open_child(descriptor, job_key, create=False, exclusive=False, mode=0, shown=shown)
        finally:
            _close(descriptor)
        return cls(child, shown)

    @classmethod
    def at(cls, path: str | os.PathLike[str], *, follow_symlinks: bool = False) -> Self:
        """Open a directory the caller trusts as the anchor, following its path.

        This is for callers that only hold a payload path; everything below
        the anchor is still opened without following links unless
        *follow_symlinks* is set.

        :param path: The directory to anchor at.
        :param follow_symlinks: Follow symlinks below the anchor too, for a
            caller acting in a tree it owns.
        :return: The pinned directory.
        :raises OSError: If *path* cannot be opened as a directory.
        """

        location = Path(path)
        return cls(os.open(location, _ANCHOR_FLAGS), location, follow_symlinks=follow_symlinks)

    @property
    def fd(self) -> int:
        """The open directory descriptor; owned by this instance."""

        return self._require_open()

    @property
    def path(self) -> Path:
        """The path this directory was opened at."""

        return self._path

    def __enter__(self) -> Self:
        """Return this open directory."""

        self._require_open()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Close the descriptor after a context-managed use."""

        self.close()

    def __repr__(self) -> str:
        return f"JobDirectory({str(self._path)!r})"

    def close(self) -> None:
        """Close the directory descriptor; repeated calls are harmless."""

        descriptor = self._fd
        self._fd = -1
        if descriptor >= 0:
            os.close(descriptor)

    def _require_open(self) -> int:
        if self._fd < 0:
            raise ValueError("job directory handle is closed")
        return self._fd

    def _walk(self, parts: tuple[str, ...], *, create: bool = False, exclusive: bool = False, mode: int = 0o777) -> int:
        """Return a new descriptor for the directory at *parts* below this one."""

        descriptor = os.dup(self._require_open())
        shown = self._path
        try:
            for index, name in enumerate(parts):
                shown = shown / name
                last = index == len(parts) - 1
                child = _open_child(
                    descriptor,
                    name,
                    create=create,
                    exclusive=exclusive and last,
                    mode=mode,
                    shown=shown,
                    follow=self._follow,
                )
                _close(descriptor)
                descriptor = child
        except BaseException:
            _close(descriptor)
            raise
        return descriptor

    def _parent(self, relative: str | PurePosixPath) -> tuple[int, str, Path]:
        """Return a new descriptor for the parent of *relative*, its last name and display path."""

        parts = _components(relative)
        return self._walk(parts[:-1]), parts[-1], self._path.joinpath(*parts)

    def directory(
        self, relative: str | PurePosixPath, *, create: bool = False, exclusive: bool = False, mode: int = 0o777
    ) -> "JobDirectory":
        """Open a subdirectory as its own handle, creating it when asked.

        :param relative: The subdirectory below this directory.
        :param create: Create every missing component with *mode*, like
            ``mkdir(parents=True)``.
        :param exclusive: Require that this call creates the last component.
        :param mode: The mode of created directories, filtered by the umask as
            for :meth:`pathlib.Path.mkdir`.
        :return: The pinned subdirectory.
        :raises JobDirectoryError: If a component is a symlink or not a directory.
        :raises FileExistsError: If *exclusive* and the last component exists.
        :raises OSError: If a component is missing (without *create*) or cannot be opened.
        """

        parts = _components(relative)
        return JobDirectory(
            self._walk(parts, create=create, exclusive=exclusive, mode=mode),
            self._path.joinpath(*parts),
            follow_symlinks=self._follow,
        )

    def open_parent(self, relative: str | PurePosixPath, *, create: bool = False) -> tuple["JobDirectory", str]:
        """Open the parent directory of an entry as its own handle.

        :param relative: The entry below this directory.
        :param create: Create missing parent components, like ``mkdir(parents=True)``.
        :return: The pinned parent directory (a duplicate of this handle for a
            single-component path) and the entry's own name.
        :raises JobDirectoryError: If a parent component is a symlink or not a directory.
        :raises OSError: If a parent component is missing (without *create*) or cannot be opened.
        """

        parts = _components(relative)
        parent = JobDirectory(
            self._walk(parts[:-1], create=create), self._path.joinpath(*parts[:-1]), follow_symlinks=self._follow
        )
        return parent, parts[-1]

    def digest_file(self, relative: str | PurePosixPath) -> str:
        """Return the SHA-256 of a regular file read through a no-follow, non-blocking descriptor.

        :param relative: The file below this directory.
        :return: The lowercase hexadecimal digest.
        :raises JobDirectoryError: If the file or a component is a symlink or the file is not regular.
        :raises OSError: If the file cannot be read.
        """

        descriptor = self.open_read(relative)
        try:
            digest = hashlib.sha256()
            offset = 0
            while chunk := os.pread(descriptor, _READ_CHUNK, offset):
                digest.update(chunk)
                offset += len(chunk)
            return digest.hexdigest()
        finally:
            _close(descriptor)

    def digest_tree(
        self,
        relative: str | PurePosixPath | None = None,
        *,
        skip: Callable[[str], bool] | None = None,
        digest: Callable[..., str] = tree_digest,
    ) -> str:
        """Return the tree digest of a directory, anchored at its descriptor.

        The directory is opened without following links and hashed through its
        ``/dev/fd`` alias, so the tree hashed is the one pinned here even if a
        component above it is swapped for a symlink meanwhile.

        :param relative: The directory below this one, or ``None`` for this directory.
        :param skip: Top-level entry names to leave out, as for :func:`~httk.core.digests.tree_digest`.
        :param digest: The tree-digest function, :func:`~httk.core.digests.tree_digest` by default.
        :return: The lowercase hexadecimal digest.
        :raises JobDirectoryError: If a component is a symlink or not a directory,
            or the tree holds a symlink or special file.
        :raises OSError: If the tree cannot be read.
        """

        handle = self if relative is None else self.directory(relative)
        try:
            alias = Path(f"/dev/fd/{handle.fd}")
            try:
                return digest(alias, skip=skip)
            except FormatError:
                raise
            except ValueError as exc:
                raise JobDirectoryError(str(exc).replace(str(alias), str(handle.path))) from exc
        finally:
            if handle is not self:
                handle.close()

    def fsync_tree(self, relative: str | PurePosixPath | None = None) -> None:
        """Synchronize every regular file and directory of a tree, without following links.

        Symlinks and special files are skipped, as :func:`~httk.workflow._util.fsync_tree`
        skips symlinks; nothing is opened in a way that could block.

        :param relative: The directory below this one, or ``None`` for this directory.
        :raises JobDirectoryError: If a component is a symlink or not a directory.
        :raises OSError: If an entry cannot be synchronized.
        """

        handle = self if relative is None else self.directory(relative)
        try:
            for name in sorted(os.listdir(handle.fd)):
                try:
                    mode = os.stat(name, dir_fd=handle.fd, follow_symlinks=False).st_mode
                except FileNotFoundError:
                    continue
                if stat.S_ISDIR(mode):
                    handle.fsync_tree(name)
                elif stat.S_ISREG(mode):
                    descriptor = os.open(name, _READ_FLAGS, dir_fd=handle.fd)
                    try:
                        os.fsync(descriptor)
                    finally:
                        _close(descriptor)
            os.fsync(handle.fd)
        finally:
            if handle is not self:
                handle.close()

    def stat(self, relative: str | PurePosixPath) -> os.stat_result | None:
        """Return the no-follow status of an entry, or ``None`` when it is absent.

        :param relative: The entry below this directory.
        :return: The entry's own status (a symlink reports itself), or ``None``.
        :raises JobDirectoryError: If an intermediate component is a symlink or not a directory.
        :raises OSError: If the status cannot be read.
        """

        try:
            parent, name, _shown = self._parent(relative)
        except FileNotFoundError:
            return None
        try:
            return os.stat(name, dir_fd=parent, follow_symlinks=self._follow)
        except FileNotFoundError:
            return None
        finally:
            _close(parent)

    def exists_dir(self, relative: str | PurePosixPath) -> bool:
        """Report whether a real directory exists at *relative*.

        Absence is ``False``. Anything else than a real directory there — a
        symlink, even one to a directory, a regular file, or a special file — is
        not something the manager or a well-behaved job puts on a control path,
        so it is refused as tampering rather than reported as absent.

        :param relative: The entry below this directory.
        :return: Whether a real directory is there.
        :raises JobDirectoryError: If the entry or an intermediate component is
            a symlink or not a directory.
        :raises OSError: If the status cannot be read.
        """

        information = self.stat(relative)
        if information is None:
            return False
        if not stat.S_ISDIR(information.st_mode):
            raise JobDirectoryError(f"{self._path.joinpath(*_components(relative))} is not a real directory")
        return True

    def open_read(self, relative: str | PurePosixPath, *, limit: int | None = None) -> int:
        """Open a regular file for reading without following or blocking.

        The entry is checked with a no-follow status before it is opened, so a
        device is never opened, and with ``fstat`` after, so a swapped entry is
        refused. The descriptor is returned in blocking mode; the caller owns it.

        :param relative: The file below this directory.
        :param limit: Refuse a file larger than this many bytes, when given.
        :return: The open descriptor.
        :raises JobDirectoryError: If the file or a component is a symlink, the
            file is not regular, or it exceeds *limit*.
        :raises OSError: If the file is missing or cannot be opened.
        """

        parent, name, shown = self._parent(relative)
        try:
            information = os.stat(name, dir_fd=parent, follow_symlinks=self._follow)
            if not stat.S_ISREG(information.st_mode):
                raise JobDirectoryError(f"{shown} is not a regular file")
            try:
                descriptor = os.open(name, _READ_FLAGS & ~os.O_NOFOLLOW if self._follow else _READ_FLAGS, dir_fd=parent)
            except OSError as exc:
                raise _refused(exc, str(shown)) from exc
        finally:
            _close(parent)
        try:
            information = os.fstat(descriptor)
            if not stat.S_ISREG(information.st_mode):
                raise JobDirectoryError(f"{shown} is not a regular file")
            if limit is not None and information.st_size > limit:
                raise JobDirectoryError(f"{shown} exceeds {limit} bytes")
            os.set_blocking(descriptor, True)
        except BaseException:
            _close(descriptor)
            raise
        return descriptor

    def read(self, relative: str | PurePosixPath, limit: int) -> bytes:
        """Read a bounded regular file.

        :param relative: The file below this directory.
        :param limit: The largest accepted size in bytes.
        :return: The bytes read.
        :raises JobDirectoryError: If the file or a component is a symlink, the
            file is not regular, or it holds more than *limit* bytes.
        :raises OSError: If the file is missing or cannot be read.
        """

        descriptor = self.open_read(relative, limit=limit)
        try:
            data = bytearray()
            while len(data) <= limit:
                chunk = os.read(descriptor, min(_READ_CHUNK, limit + 1 - len(data)))
                if not chunk:
                    return bytes(data)
                data.extend(chunk)
            raise JobDirectoryError(f"{self._path.joinpath(*_components(relative))} exceeds {limit} bytes")
        finally:
            _close(descriptor)

    def read_json(self, relative: str | PurePosixPath, limit: int = CONTROL_DOCUMENT_LIMIT) -> dict[str, Any]:
        """Read a bounded JSON object document.

        :param relative: The document below this directory.
        :param limit: The largest accepted size in bytes.
        :return: The decoded object.
        :raises JobDirectoryError: If the document or a component is a symlink,
            special file, or oversized.
        :raises httk.workflow.errors.FormatError: If the document is missing,
            unreadable, or not a JSON object.
        """

        shown = self._path.joinpath(*_components(relative))
        try:
            value = json.loads(self.read(relative, limit).decode("utf-8"))
        except JobDirectoryError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise FormatError(f"cannot read JSON object {shown}: {exc}") from exc
        if not isinstance(value, dict):
            raise FormatError(f"expected JSON object in {shown}")
        return value

    def open_append(self, relative: str | PurePosixPath, mode: int = 0o600) -> int:
        """Open (creating) a regular file for appending without following or blocking.

        A FIFO without a reader is refused instead of blocking the manager, and
        the descriptor is switched back to blocking mode once it is known to be
        a regular file. The caller owns the descriptor.

        :param relative: The file below this directory; its parent must exist.
        :param mode: The mode of a created file.
        :return: The open descriptor.
        :raises JobDirectoryError: If the file or a component is a symlink, or
            the file is not regular.
        :raises OSError: If the file cannot be opened.
        """

        parent, name, shown = self._parent(relative)
        try:
            information = None
            try:
                information = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                pass
            if information is not None and not stat.S_ISREG(information.st_mode):
                raise JobDirectoryError(f"{shown} is not a regular file")
            try:
                descriptor = os.open(name, _APPEND_FLAGS, mode, dir_fd=parent)
            except OSError as exc:
                raise _refused(exc, str(shown)) from exc
        finally:
            _close(parent)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise JobDirectoryError(f"{shown} is not a regular file")
            flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
            fcntl.fcntl(descriptor, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)
        except BaseException:
            _close(descriptor)
            raise
        return descriptor

    def append(self, relative: str | PurePosixPath, data: bytes, mode: int = 0o600) -> None:
        """Append *data* to a regular file with one write.

        :param relative: The file below this directory; its parent must exist.
        :param data: The bytes to append.
        :param mode: The mode of a created file.
        :raises JobDirectoryError: If the file or a component is a symlink, or
            the file is not regular.
        :raises OSError: If the file cannot be opened or written.
        """

        descriptor = self.open_append(relative, mode)
        try:
            os.write(descriptor, data)
        finally:
            _close(descriptor)

    @staticmethod
    def _write_all(descriptor: int, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("write made no progress")
            view = view[written:]

    def write_atomic(
        self, relative: str | PurePosixPath, data: bytes, *, durable: bool = False, mode: int = 0o600
    ) -> None:
        """Replace a file atomically through an exclusive temporary entry.

        The temporary entry is created ``O_EXCL|O_NOFOLLOW`` beside the target
        and renamed over it within one directory descriptor, so a symlink at
        the target name is replaced, never followed.

        :param relative: The file below this directory; its parent must exist.
        :param data: The complete new content.
        :param durable: Synchronize the file and its directory.
        :param mode: The mode of the written file.
        :raises JobDirectoryError: If a component is a symlink or the target is a directory.
        :raises OSError: If the file cannot be written or renamed.
        """

        parent, name, shown = self._parent(relative)
        temporary = f".{secrets.token_hex(16)}.tmp"
        created = False
        renamed = False
        try:
            descriptor = os.open(temporary, _EXCLUSIVE_FLAGS, mode, dir_fd=parent)
            created = True
            try:
                self._write_all(descriptor, data)
                if durable:
                    os.fsync(descriptor)
            finally:
                os.close(descriptor)
            try:
                os.rename(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
            except OSError as exc:
                raise _refused(exc, str(shown)) from exc
            renamed = True
            if durable:
                os.fsync(parent)
        finally:
            if created and not renamed:
                try:
                    os.unlink(temporary, dir_fd=parent)
                except OSError:
                    pass
            _close(parent)

    def create_exclusive(self, relative: str | PurePosixPath, data: bytes, mode: int = 0o600) -> None:
        """Create a new file that must not exist yet, and write *data* to it.

        :param relative: The file below this directory; its parent must exist.
        :param data: The file content.
        :param mode: The mode of the created file.
        :raises JobDirectoryError: If a component or the name is a symlink.
        :raises FileExistsError: If an entry already has the name.
        :raises OSError: If the file cannot be created or written.
        """

        parent, name, shown = self._parent(relative)
        try:
            try:
                descriptor = os.open(name, _EXCLUSIVE_FLAGS, mode, dir_fd=parent)
            except FileExistsError as exc:
                if stat.S_ISLNK(os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode):
                    raise JobDirectoryError(f"{shown} is a symlink") from exc
                raise
            except OSError as exc:
                raise _refused(exc, str(shown)) from exc
        finally:
            _close(parent)
        try:
            self._write_all(descriptor, data)
        finally:
            os.close(descriptor)

    def remove_tree(self, relative: str | PurePosixPath) -> None:
        """Remove an entry and, for a real directory, everything below it.

        A symlink is unlinked, never followed. A directory is removed with the
        descriptor-based :func:`shutil.rmtree`, which does not follow a symlink
        planted anywhere below it while it runs. A missing entry is not an error.

        :param relative: The entry below this directory.
        :raises JobDirectoryError: If an intermediate component is a symlink or not a directory.
        :raises OSError: If the entry cannot be removed.
        """

        try:
            parent, name, _shown = self._parent(relative)
        except FileNotFoundError:
            return
        try:
            try:
                information = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return
            if stat.S_ISDIR(information.st_mode):
                shutil.rmtree(name, dir_fd=parent)
            else:
                try:
                    os.unlink(name, dir_fd=parent)
                except FileNotFoundError:
                    return
        finally:
            _close(parent)

    def unlink(self, relative: str | PurePosixPath, *, missing_ok: bool = True) -> None:
        """Remove one non-directory entry without following it.

        :param relative: The entry below this directory.
        :param missing_ok: Treat an absent entry (or parent) as removed.
        :raises JobDirectoryError: If an intermediate component is a symlink or
            not a directory, or the entry is a directory.
        :raises OSError: If the entry cannot be removed.
        """

        try:
            parent, name, shown = self._parent(relative)
        except FileNotFoundError:
            if missing_ok:
                return
            raise
        try:
            os.unlink(name, dir_fd=parent)
        except FileNotFoundError:
            if not missing_ok:
                raise
        except OSError as exc:
            raise _refused(exc, str(shown)) from exc
        finally:
            _close(parent)

    def rename_out(
        self, relative: str | PurePosixPath, target_dir_fd: int, name: str, *, directory: bool = True
    ) -> None:
        """Rename an entry of this directory into another directory descriptor.

        The source is checked without following it before the rename, and the
        renamed entry is checked again at its destination, because the job may
        swap the source in between: a mismatch found afterwards is refused with
        the entry left at the destination, where the caller owns its removal.

        :param relative: The entry below this directory.
        :param target_dir_fd: The destination directory descriptor.
        :param name: The destination name, a single component.
        :param directory: Require a real directory (otherwise a regular file).
        :raises JobDirectoryError: If the entry or a component is a symlink or
            the entry is not of the required type.
        :raises OSError: If the rename fails.
        """

        _check_component(name, name)
        wanted = stat.S_ISDIR if directory else stat.S_ISREG
        kind = "a real directory" if directory else "a regular file"
        parent, source, shown = self._parent(relative)
        try:
            if not wanted(os.stat(source, dir_fd=parent, follow_symlinks=False).st_mode):
                raise JobDirectoryError(f"{shown} is not {kind}")
            try:
                os.rename(source, name, src_dir_fd=parent, dst_dir_fd=target_dir_fd)
            except OSError as exc:
                raise _refused(exc, str(shown)) from exc
        finally:
            _close(parent)
        if not wanted(os.stat(name, dir_fd=target_dir_fd, follow_symlinks=False).st_mode):
            raise JobDirectoryError(f"{shown} was replaced by something that is not {kind} while it was renamed")
