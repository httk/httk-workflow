"""Bounded, descriptor-anchored files for the daemon request mailbox.

This module deliberately contains no request parser or daemon loop.  It only
provides the small filesystem primitive used by those later layers: an
already-created directory is opened once, and every operation is then
anchored to the descriptor for that directory.

Scans are bounded (:meth:`MailboxDirectory.scan`): only publication names count
toward a batch, and a publication-named entry that is not a request and cannot
be removed is set aside under a dot name (:meth:`MailboxDirectory.set_aside`),
which scans skip, so such entries cannot fill every batch. Accepted residual:
more than :data:`MAX_SCANNED_ENTRIES` entries with other names placed before a
request in directory order hide it from every scan.
"""

import errno
import os
import re
import secrets
import stat
from collections.abc import Collection
from pathlib import Path
from typing import Self

from . import _fs

MAX_DOCUMENT_BYTES = 16 * 1024
#: The most publications one scan returns; the rest wait for a later scan, after these were consumed.
MAX_DIRECTORY_ENTRIES = 4096
#: The most directory entries one scan examines, whatever their names.
MAX_SCANNED_ENTRIES = 1_000_000
_NAME_PATTERN = re.compile(r"[0-9a-f]{32}\.json\Z")
_O_DIRECTORY = os.O_DIRECTORY
_O_CLOEXEC = os.O_CLOEXEC
_O_NOFOLLOW = os.O_NOFOLLOW
_O_NONBLOCK = os.O_NONBLOCK


def _directory_flags() -> int:
    return os.O_RDONLY | _O_DIRECTORY | _O_CLOEXEC | _O_NOFOLLOW


def _publication_name(name: str) -> bool:
    return isinstance(name, str) and _NAME_PATTERN.fullmatch(name) is not None


def _validate_name(name: str) -> None:
    if not _publication_name(name):
        raise ValueError("mailbox name must be 32 lower-case hexadecimal characters followed by .json")


class MailboxDirectory:
    """Operate on one pre-existing mailbox directory through a pinned fd.

    :param path: An absolute path to an existing directory.  Every component is
        opened with ``O_NOFOLLOW``; no component or parent directory is created.
    :raises ValueError: If *path* is relative, contains ``..`` components, or
        the mailbox has already been closed.
    :raises OSError: If the path cannot be opened as a directory.
    """

    def __init__(self, path: Path | str) -> None:
        candidate = Path(path)
        if not candidate.is_absolute():
            raise ValueError("mailbox path must be absolute")

        descriptor = -1
        try:
            descriptor = os.open(candidate.anchor or "/", _directory_flags())
            for component in candidate.parts[1:]:
                if component == "..":
                    raise ValueError("mailbox path must not contain .. components")
                next_descriptor = os.open(component, _directory_flags(), dir_fd=descriptor)
                previous_descriptor = descriptor
                descriptor = next_descriptor
                os.close(previous_descriptor)
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        self._fd = descriptor

    @classmethod
    def child(cls, parent_fd: int, name: str) -> Self:
        """Open the mailbox directory *name* inside an open directory, without following a symlink.

        :param parent_fd: Descriptor of the directory that holds the mailbox.
        :param name: The mailbox directory's name: one path component.
        :return: The open mailbox.
        :raises ValueError: If *name* is not one plain path component.
        :raises OSError: If *name* is absent, a symlink (``ELOOP``) or not a directory.
        """

        if not name or name in {".", ".."} or "/" in name or "\0" in name:
            raise ValueError("mailbox name must be one path component")
        mailbox = cls.__new__(cls)
        mailbox._fd = os.open(name, _directory_flags(), dir_fd=parent_fd)
        return mailbox

    def __enter__(self) -> Self:
        """Return this open mailbox."""

        self._require_open()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Close the mailbox after a context-managed use."""

        self.close()

    def _require_open(self) -> int:
        if self._fd < 0:
            raise ValueError("mailbox is closed")
        return self._fd

    @staticmethod
    def _publication_name() -> str:
        return f"{secrets.token_hex(16)}.json"

    def publish(self, data: bytes) -> str:
        """Publish *data* as one fresh mailbox document.

        :param data: Bytes no larger than 16 KiB.
        :return: The independently generated publication filename.
        :raises ValueError: If *data* is not bytes or exceeds the size limit.
        :raises OSError: If a filesystem operation fails.  An error after the
            rename may leave the publication visible.

        The final name has 128 random bits.  The filesystem rename has no
        atomic no-replace form here: a random collision can replace an existing
        entry. Concurrent writers can also replace or remove publications.
        """

        descriptor = self._require_open()
        if not isinstance(data, bytes):
            raise ValueError("mailbox data must be bytes")
        if len(data) > MAX_DOCUMENT_BYTES:
            raise ValueError(f"mailbox document exceeds {MAX_DOCUMENT_BYTES} bytes")

        publication = self._publication_name()
        self._write_atomic(descriptor, publication, data)
        return publication

    @staticmethod
    def _validate_data(data: bytes) -> None:
        if not isinstance(data, bytes):
            raise ValueError("mailbox data must be bytes")
        if len(data) > MAX_DOCUMENT_BYTES:
            raise ValueError(f"mailbox document exceeds {MAX_DOCUMENT_BYTES} bytes")

    def _write_atomic(self, descriptor: int, publication: str, data: bytes) -> None:
        """Write and atomically install bytes through an exclusive temporary (:func:`httk.workflow._fs.write_file`)."""

        _fs.write_file(_fs.anchored(descriptor, publication), data, durable=True, mode=0o600)

    def replace(self, name: str, data: bytes) -> None:
        """Atomically replace one flat publication with bounded bytes.

        :param name: A 32-character lower-case hexadecimal publication name.
        :param data: Bytes no larger than 16 KiB.
        :raises ValueError: If the name or data is invalid or exceeds the size limit.
        :raises OSError: If a filesystem operation fails. An error after rename may
            leave the replacement visible.
        """

        descriptor = self._require_open()
        _validate_name(name)
        self._validate_data(data)
        self._write_atomic(descriptor, name, data)

    def remove(self, name: str) -> None:
        """Remove one flat publication and sync the containing directory; a missing one counts as removed.

        :param name: A 32-character lower-case hexadecimal publication name.
        :raises ValueError: If the name is invalid.
        :raises OSError: If unlinking or syncing the directory fails (``IsADirectoryError`` for a directory).
        """

        descriptor = self._require_open()
        _validate_name(name)
        _fs.remove_file(_fs.anchored(descriptor, name), durable=True)

    def lstat(self, name: str) -> os.stat_result | None:
        """Look up one flat publication by name, without following a symlink.

        :param name: A 32-character lower-case hexadecimal publication name.
        :return: The entry's ``lstat`` result, or ``None`` when no entry has the name.
        :raises ValueError: If the name is invalid.
        :raises OSError: If the lookup fails for another reason than absence.
        """

        descriptor = self._require_open()
        _validate_name(name)
        try:
            return os.lstat(name, dir_fd=descriptor)
        except FileNotFoundError:
            return None

    def read(self, name: str) -> bytes:
        """Read one bounded regular publication by its flat filename.

        :param name: A 32-character lower-case hexadecimal publication name.
        :return: The bytes observed while reading the file.
        :raises ValueError: If *name* is malformed, the entry is not regular,
            or the document exceeds 16 KiB.
        :raises OSError: If the entry cannot be opened or read.
        """

        descriptor = self._require_open()
        _validate_name(name)
        file_descriptor = -1
        try:
            file_descriptor = os.open(
                name,
                os.O_RDONLY | _O_NOFOLLOW | _O_NONBLOCK | _O_CLOEXEC,
                dir_fd=descriptor,
            )
            information = os.fstat(file_descriptor)
            if not stat.S_ISREG(information.st_mode):
                raise ValueError(f"mailbox entry is not a regular file: {name}")
            if information.st_size > MAX_DOCUMENT_BYTES:
                raise ValueError(f"mailbox document exceeds {MAX_DOCUMENT_BYTES} bytes")

            result = bytearray()
            while len(result) <= MAX_DOCUMENT_BYTES:
                chunk = os.read(file_descriptor, MAX_DOCUMENT_BYTES + 1 - len(result))
                if not chunk:
                    return bytes(result)
                result.extend(chunk)
                if len(result) > MAX_DOCUMENT_BYTES:
                    raise ValueError(f"mailbox document exceeds {MAX_DOCUMENT_BYTES} bytes")
            raise ValueError(f"mailbox document exceeds {MAX_DOCUMENT_BYTES} bytes")
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)

    def identity(self) -> tuple[int, int]:
        """Return the ``(st_dev, st_ino)`` of the open directory, comparable within this process only.

        :return: The directory's device and inode numbers.
        """

        information = os.fstat(self._require_open())
        return information.st_dev, information.st_ino

    def set_aside(self, name: str) -> str:
        """Rename one publication-named entry to a fresh dot name that scans skip, whatever its type.

        :param name: A 32-character lower-case hexadecimal publication name.
        :return: The new name, ``.invalid-<name>-<rand>``.
        :raises ValueError: If the name is invalid.
        :raises OSError: If the rename or the directory sync fails.
        """

        descriptor = self._require_open()
        _validate_name(name)
        aside = f".invalid-{name}-{secrets.token_hex(8)}"
        if self.lstat(name) is None:
            raise FileNotFoundError(errno.ENOENT, "no such publication", name)
        try:
            _fs.move_owned(_fs.anchored(descriptor, name), _fs.anchored(descriptor, aside), durable=True)
        except _fs.MoveFailed as exc:
            raise OSError(exc.errno or errno.EIO, str(exc)) from exc
        return aside

    def scan(self, skip: Collection[str] = ()) -> tuple[tuple[str, ...], bool]:
        """Return a bounded batch of valid publication names in sorted order, and whether more were left.

        Only publication names count toward :data:`MAX_DIRECTORY_ENTRIES`; temporaries, dot names and other
        entries are skipped, and so are the publication names in *skip*. A full mailbox never raises: the
        batch is what fit, and since consumed publications leave the directory, later scans reach the rest.
        At most :data:`MAX_SCANNED_ENTRIES` entries are examined.

        :param skip: Publication names to leave out without counting them.
        :return: The names, sorted lexicographically, and ``True`` when the scan stopped at a bound.
        :raises OSError: If the directory cannot be scanned.
        """

        descriptor = self._require_open()
        names: list[str] = []
        truncated = False
        with os.scandir(descriptor) as entries:
            for count, entry in enumerate(entries, 1):
                if count > MAX_SCANNED_ENTRIES or len(names) >= MAX_DIRECTORY_ENTRIES:
                    truncated = True
                    break
                if _publication_name(entry.name) and entry.name not in skip:
                    names.append(entry.name)
        return tuple(sorted(names)), truncated

    def names(self) -> tuple[str, ...]:
        """Return a bounded batch of valid publication names in sorted order (see :meth:`scan`).

        :return: Valid names, sorted lexicographically.
        :raises OSError: If the directory cannot be scanned.
        """

        return self.scan()[0]

    def close(self) -> None:
        """Close the pinned directory descriptor; repeated calls are harmless."""

        descriptor = self._fd
        self._fd = -1
        if descriptor >= 0:
            os.close(descriptor)
