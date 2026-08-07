"""Packed append-only transition journal."""

import hashlib
import json
import os
import re
import struct
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Self

from ._util import fsync_directory, json_bytes, visibility_attempts
from .errors import FormatError, WorkspaceCorruptionError, WorkspaceUnavailableError
from .models import DEFAULT_JOURNAL_SEGMENT_BYTES, to_base36

SEGMENT_HEADER = b"HTTK-HWJ-V1\n"
_LENGTH = struct.Struct(">Q")
_REF_PATTERN = re.compile(
    r"w(?P<writer>[0-9a-f]{32})-s(?P<segment>[0-9a-z]{1,7})-o(?P<offset>[0-9a-z]{1,13})"
    r"-l(?P<length>[0-9a-z]{1,13})-h(?P<checksum>[0-9a-f]{32})"
)
#: Problem codes a frame read reports. A transient code may simply mean that
#: the extended segment has not become visible on this client yet, so a reader
#: retries it until the visibility deadline expires; the rest are damage.
TRANSIENT_FRAME_PROBLEMS = frozenset({"missing_segment", "short_read", "undecodable_frame"})
#: The largest frame a segment walk will believe a length prefix about. It
#: keeps a corrupted length from turning a repair walk into a huge allocation.
MAXIMUM_FRAME_BYTES = 16 * 1024 * 1024


class _FrameProblem(Exception):
    """One frame could not be read, with a stable machine-readable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code

    @property
    def transient(self) -> bool:
        """Report whether waiting for the filesystem could still fix this."""

        return self.code in TRANSIENT_FRAME_PROBLEMS


class JournalWriter:
    """Open the exclusive journal writer for one manager incarnation.

    :param control_dir: Locate the workspace control directory.
    :param writer_id: Reuse a canonical writer identity when supplied.
    :param durable: Synchronize journal writes before returning from append.
    :param maximum_segment_bytes: Set the maximum size of a journal segment.
    :raises ValueError: If the writer identity is not canonical.
    """

    def __init__(
        self,
        control_dir: Path,
        *,
        writer_id: str | None = None,
        durable: bool = True,
        maximum_segment_bytes: int = DEFAULT_JOURNAL_SEGMENT_BYTES,
    ) -> None:
        self.control_dir = control_dir
        self.writer_id = writer_id or str(uuid.uuid4())
        writer_uuid = uuid.UUID(self.writer_id)
        if str(writer_uuid) != self.writer_id:
            raise ValueError("writer_id must be a canonical UUID")
        self.durable = durable
        self.maximum_segment_bytes = maximum_segment_bytes
        self._writer_dir = control_dir / "journal" / self.writer_id
        self._writer_dir.mkdir(parents=True, exist_ok=False)
        self._segment_number = 0
        self._handle = self._open_segment(self._segment_number)

    def _segment_path(self, number: int) -> Path:
        return self._writer_dir / f"{to_base36(number)}.hwj"

    def _open_segment(self, number: int) -> BinaryIO:
        if number > (1 << 32) - 1:
            raise WorkspaceCorruptionError("journal segment number exhausted")
        path = self._segment_path(number)
        handle = path.open("x+b")
        handle.write(SEGMENT_HEADER)
        handle.flush()
        if self.durable:
            os.fsync(handle.fileno())
            fsync_directory(path.parent)
        return handle

    def _rotate_if_needed(self, frame_bytes: int) -> None:
        if self._handle.tell() == len(SEGMENT_HEADER):
            return
        if self._handle.tell() + frame_bytes <= self.maximum_segment_bytes:
            return
        self._handle.close()
        self._segment_number += 1
        self._handle = self._open_segment(self._segment_number)

    def append(self, record: Mapping[str, object]) -> str:
        """Append *record* and return its canonical ``hwref-v1`` reference.

        :param record: Supply the journal record to append.
        :return: The canonical record reference.
        """

        payload = json_bytes(dict(record))
        length_bytes = _LENGTH.pack(len(payload))
        checksum = hashlib.sha256(length_bytes + payload).digest()
        frame = length_bytes + payload + checksum + length_bytes
        self._rotate_if_needed(len(frame))
        offset = self._handle.tell()
        self._handle.write(frame)
        self._handle.flush()
        if self.durable:
            os.fsync(self._handle.fileno())
        return encode_record_ref(self.writer_id, self._segment_number, offset, len(payload), checksum)

    def close(self) -> None:
        """Close the current journal segment."""

        self._handle.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def encode_record_ref(writer_id: str, segment: int, offset: int, length: int, checksum: bytes) -> str:
    """Encode one canonical ``hwref-v1`` reference.

    :param writer_id: Identify the journal writer.
    :param segment: Identify the journal segment.
    :param offset: Locate the frame within the segment.
    :param length: Record the frame payload length.
    :param checksum: Supply the frame checksum.
    :return: The canonical record reference.
    """

    return (
        f"w{writer_id.replace('-', '')}"
        f"-s{to_base36(segment)}"
        f"-o{to_base36(offset)}"
        f"-l{to_base36(length)}"
        f"-h{checksum[:16].hex()}"
    )


def parse_record_ref(record_ref: str) -> tuple[str, int, int, int, str]:
    """Parse one canonical ``hwref-v1`` reference.

    :param record_ref: Supply the record reference to parse.
    :return: The writer, segment, offset, length, and checksum components.
    :raises httk.workflow.errors.FormatError: If the reference is not canonical.
    """

    match = _REF_PATTERN.fullmatch(record_ref)
    if match is None:
        raise FormatError(f"invalid hwref-v1 record reference: {record_ref!r}")
    writer_hex = match.group("writer")
    writer_id = str(uuid.UUID(hex=writer_hex))
    segment = int(match.group("segment"), 36)
    offset = int(match.group("offset"), 36)
    length = int(match.group("length"), 36)
    return writer_id, segment, offset, length, match.group("checksum")


def segment_path(control_dir: Path, writer_id: str, segment: int) -> Path:
    """Return the segment file one record reference names.

    :param control_dir: Locate the workspace control directory.
    :param writer_id: Identify the journal writer.
    :param segment: Identify the journal segment.
    :return: The segment file path.
    """

    return control_dir / "journal" / writer_id / f"{to_base36(segment)}.hwj"


def _read_frame(path: Path, offset: int, expected_length: int, expected_prefix: str) -> dict[str, Any]:
    """Read and verify one referenced frame, or report why it is unreadable."""

    try:
        handle = path.open("rb")
    except FileNotFoundError as exc:
        raise _FrameProblem("missing_segment", f"journal segment is not present: {path}") from exc
    with handle:
        header = handle.read(len(SEGMENT_HEADER))
        if len(header) != len(SEGMENT_HEADER):
            raise _FrameProblem("short_read", "short journal segment header")
        if header != SEGMENT_HEADER:
            raise _FrameProblem("invalid_header", f"invalid journal segment header: {path}")
        handle.seek(offset)
        length_bytes = handle.read(_LENGTH.size)
        if len(length_bytes) != _LENGTH.size:
            raise _FrameProblem("short_read", "short frame length")
        (length,) = _LENGTH.unpack(length_bytes)
        if length != expected_length:
            raise _FrameProblem("length_mismatch", "record reference length disagrees with journal frame")
        payload = handle.read(length)
        checksum = handle.read(32)
        trailer = handle.read(_LENGTH.size)
        if len(payload) != length or len(checksum) != 32 or len(trailer) != _LENGTH.size:
            raise _FrameProblem("short_read", "short journal frame")
        if trailer != length_bytes:
            raise _FrameProblem("trailer_mismatch", "journal frame trailer disagrees with header")
        if hashlib.sha256(length_bytes + payload).digest() != checksum:
            raise _FrameProblem("checksum_mismatch", "journal frame checksum mismatch")
        if checksum[:16].hex() != expected_prefix:
            raise _FrameProblem("reference_mismatch", "record reference checksum mismatch")
        try:
            value = json.loads(payload)
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise _FrameProblem("undecodable_frame", f"journal frame is not decodable JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise _FrameProblem("not_an_object", "journal record is not a JSON object")
        return value


def read_record(control_dir: Path, record_ref: str, *, deadline_seconds: float | None = None) -> dict[str, Any]:
    """Read and verify a journal record, retrying visibility-short reads.

    A frame that is absent, short, or undecodable may be an extension of a
    segment that has not reached this client yet, so it is retried with bounded
    backoff until *deadline_seconds* — the workspace's configured visibility
    deadline — expires. Damage that no amount of waiting can repair is reported
    at once.

    :param control_dir: Locate the workspace control directory.
    :param record_ref: Identify the journal record to read.
    :param deadline_seconds: Bound retries for metadata visibility.
    :return: The verified journal record.
    :raises httk.workflow.errors.FormatError: If the record reference is invalid.
    :raises httk.workflow.errors.WorkspaceCorruptionError: If the record is permanently damaged.
    :raises httk.workflow.errors.WorkspaceUnavailableError: If the record remains incoherently visible.
    """

    writer_id, segment, offset, expected_length, expected_prefix = parse_record_ref(record_ref)
    path = segment_path(control_dir, writer_id, segment)
    last_error: _FrameProblem | None = None
    for _ in visibility_attempts(deadline_seconds):
        try:
            return _read_frame(path, offset, expected_length, expected_prefix)
        except _FrameProblem as exc:
            if not exc.transient:
                raise WorkspaceCorruptionError(str(exc)) from exc
            last_error = exc
    raise WorkspaceUnavailableError(f"journal record is not coherently visible: {record_ref}") from last_error


@dataclass(frozen=True)
class RecordVerification:
    """Report the outcome of reading one referenced frame without raising.

    :param record_ref: Identify the record that was checked.
    :param frame: Hold the verified frame when reading succeeded.
    :param problem: Name the verification problem when reading failed.
    :param detail: Explain the verification result.
    """

    record_ref: str
    frame: dict[str, Any] | None
    problem: str | None
    detail: str

    @property
    def ok(self) -> bool:
        """Report whether the referenced frame was read and verified.

        :return: ``True`` when the frame is present and valid.
        """

        return self.frame is not None


def verify_record(control_dir: Path, record_ref: str, *, deadline_seconds: float | None = None) -> RecordVerification:
    """Read one referenced frame, reporting damage rather than raising.

    This is the reading half of a workspace check: it distinguishes a segment
    that is gone from one that is truncated, corrupt, or simply not holding the
    frame the reference names, which is what a repair decision needs.

    :param control_dir: Locate the workspace control directory.
    :param record_ref: Identify the journal record to verify.
    :param deadline_seconds: Bound retries for metadata visibility.
    :return: The verification result.
    """

    try:
        writer_id, segment, offset, expected_length, expected_prefix = parse_record_ref(record_ref)
    except (FormatError, ValueError) as exc:
        return RecordVerification(record_ref, None, "invalid_record_ref", str(exc))
    path = segment_path(control_dir, writer_id, segment)
    last_error: _FrameProblem | None = None
    for _ in visibility_attempts(deadline_seconds):
        try:
            frame = _read_frame(path, offset, expected_length, expected_prefix)
        except _FrameProblem as exc:
            last_error = exc
            if exc.transient:
                continue
            break
        except OSError as exc:
            return RecordVerification(record_ref, None, "unreadable_segment", str(exc))
        return RecordVerification(record_ref, frame, None, "")
    if last_error is None:  # pragma: no cover - a schedule always has one attempt
        return RecordVerification(record_ref, None, "unreadable_frame", "the frame was never probed")
    return RecordVerification(record_ref, None, last_error.code, str(last_error))


@dataclass(frozen=True)
class JournalFrame:
    """Describe one intact frame found by walking a segment from its header.

    :param record_ref: Identify the canonical record reference.
    :param writer_id: Identify the journal writer.
    :param segment: Identify the journal segment.
    :param offset: Locate the frame within the segment.
    :param frame: Hold the decoded journal record.
    """

    record_ref: str
    writer_id: str
    segment: int
    offset: int
    frame: dict[str, Any]


def iter_segment_frames(path: Path, writer_id: str, segment: int) -> Iterator[JournalFrame]:
    """Yield every intact frame of one segment.

    The walk is deliberately forgiving. A damaged frame whose framing is still
    intact is skipped, because the frames behind it remain locatable and are
    exactly what a repair is looking for; a torn or partially visible tail is
    the normal state of a segment a live writer is appending to and simply ends
    the walk.

    :param path: Locate the journal segment.
    :param writer_id: Identify the journal writer.
    :param segment: Identify the journal segment number.
    :yield: Each intact frame found in the segment.
    """

    try:
        handle = path.open("rb")
    except OSError:
        return
    with handle:
        if handle.read(len(SEGMENT_HEADER)) != SEGMENT_HEADER:
            return
        while True:
            offset = handle.tell()
            length_bytes = handle.read(_LENGTH.size)
            if len(length_bytes) != _LENGTH.size:
                return
            (length,) = _LENGTH.unpack(length_bytes)
            if length > MAXIMUM_FRAME_BYTES:
                return
            payload = handle.read(length)
            checksum = handle.read(32)
            trailer = handle.read(_LENGTH.size)
            if len(payload) != length or len(checksum) != 32 or trailer != length_bytes:
                return
            if hashlib.sha256(length_bytes + payload).digest() != checksum:
                continue
            try:
                value = json.loads(payload)
            except (json.JSONDecodeError, UnicodeError):
                continue
            if not isinstance(value, dict):
                continue
            yield JournalFrame(
                record_ref=encode_record_ref(writer_id, segment, offset, length, checksum),
                writer_id=writer_id,
                segment=segment,
                offset=offset,
                frame=value,
            )


def iter_journal_frames(control_dir: Path) -> Iterator[JournalFrame]:
    """Yield every intact frame of every segment of every writer.

    :param control_dir: Locate the workspace control directory.
    :yield: Each intact journal frame.
    """

    journal = control_dir / "journal"
    if not journal.is_dir():
        return
    for writer_dir in sorted(journal.iterdir()):
        if not writer_dir.is_dir():
            continue
        try:
            writer_id = str(uuid.UUID(writer_dir.name))
        except ValueError:
            continue
        if writer_id != writer_dir.name:
            continue
        for path in sorted(writer_dir.glob("*.hwj")):
            try:
                segment = int(path.stem, 36)
            except ValueError:
                continue
            yield from iter_segment_frames(path, writer_id, segment)
