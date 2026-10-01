"""Bounded, descriptor-anchored files for the daemon request mailbox.

This module deliberately contains no request parser or daemon loop.  It only
provides the small filesystem primitive used by those later layers: an
already-created directory is opened once, and every operation is then
anchored to the descriptor for that directory.
"""

import os
import re
import secrets
import stat
from pathlib import Path
from typing import Self

MAX_DOCUMENT_BYTES = 16 * 1024
MAX_DIRECTORY_ENTRIES = 4096
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
    def _temporary_name() -> str:
        return f".{secrets.token_hex(16)}.tmp"

    @staticmethod
    def _publication_name() -> str:
        return f"{secrets.token_hex(16)}.json"

    @staticmethod
    def _remove_temporary(descriptor: int, name: str) -> None:
        try:
            os.unlink(name, dir_fd=descriptor)
        except FileNotFoundError:
            pass
        except OSError:
            pass

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

        temporary = self._temporary_name()
        temporary_fd = -1
        temporary_created = False
        renamed = False
        try:
            temporary_fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC,
                0o600,
                dir_fd=descriptor,
            )
            temporary_created = True
            offset = 0
            while offset < len(data):
                written = os.write(temporary_fd, data[offset:])
                if written <= 0:
                    raise OSError("mailbox write made no progress")
                offset += written
            os.fsync(temporary_fd)
            descriptor_to_close = temporary_fd
            temporary_fd = -1
            os.close(descriptor_to_close)

            publication = self._publication_name()
            os.rename(temporary, publication, src_dir_fd=descriptor, dst_dir_fd=descriptor)
            renamed = True
            os.fsync(descriptor)
            return publication
        finally:
            if temporary_fd >= 0:
                descriptor_to_close = temporary_fd
                temporary_fd = -1
                try:
                    os.close(descriptor_to_close)
                finally:
                    if temporary_created and not renamed:
                        self._remove_temporary(descriptor, temporary)
            elif temporary_created and not renamed:
                self._remove_temporary(descriptor, temporary)

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

    def names(self) -> tuple[str, ...]:
        """Return valid publication names in sorted order.

        :return: Valid names, sorted lexicographically.
        :raises ValueError: If more than 4096 directory entries are present.
        :raises OSError: If the directory cannot be scanned.
        """

        descriptor = self._require_open()
        names: list[str] = []
        with os.scandir(descriptor) as entries:
            for count, entry in enumerate(entries, 1):
                if count > MAX_DIRECTORY_ENTRIES:
                    raise ValueError(f"mailbox directory exceeds {MAX_DIRECTORY_ENTRIES} entries")
                if _publication_name(entry.name):
                    names.append(entry.name)
        return tuple(sorted(names))

    def close(self) -> None:
        """Close the pinned directory descriptor; repeated calls are harmless."""

        descriptor = self._fd
        self._fd = -1
        if descriptor >= 0:
            os.close(descriptor)
