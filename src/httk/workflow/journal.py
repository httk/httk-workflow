"""Packed append-only transition journal."""

import hashlib
import json
import os
import re
import struct
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, BinaryIO

from ._util import fsync_directory, json_bytes, retry_delay
from .errors import FormatError, WorkspaceCorruptionError, WorkspaceUnavailableError
from .models import to_base36

SEGMENT_HEADER = b"HTTK-HWJ-V1\n"
_LENGTH = struct.Struct(">Q")
_REF_PATTERN = re.compile(
    r"w(?P<writer>[0-9a-f]{32})-s(?P<segment>[0-9a-z]{1,7})-o(?P<offset>[0-9a-z]{1,13})"
    r"-l(?P<length>[0-9a-z]{1,13})-h(?P<checksum>[0-9a-f]{32})"
)


class JournalWriter:
    """The exclusive journal writer for one manager incarnation."""

    def __init__(
        self,
        control_dir: Path,
        *,
        writer_id: str | None = None,
        durable: bool = False,
        maximum_segment_bytes: int = 64 * 1024 * 1024,
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
        """Append *record* and return its canonical ``hwref-v1`` reference."""

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
        return (
            f"w{self.writer_id.replace('-', '')}"
            f"-s{to_base36(self._segment_number)}"
            f"-o{to_base36(offset)}"
            f"-l{to_base36(len(payload))}"
            f"-h{checksum[:16].hex()}"
        )

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "JournalWriter":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def parse_record_ref(record_ref: str) -> tuple[str, int, int, int, str]:
    """Parse one canonical ``hwref-v1`` reference."""

    match = _REF_PATTERN.fullmatch(record_ref)
    if match is None:
        raise FormatError(f"invalid hwref-v1 record reference: {record_ref!r}")
    writer_hex = match.group("writer")
    writer_id = str(uuid.UUID(hex=writer_hex))
    segment = int(match.group("segment"), 36)
    offset = int(match.group("offset"), 36)
    length = int(match.group("length"), 36)
    return writer_id, segment, offset, length, match.group("checksum")


def read_record(control_dir: Path, record_ref: str, *, attempts: int = 7) -> dict[str, Any]:
    """Read and verify a journal record, retrying visibility-short reads."""

    writer_id, segment, offset, expected_length, expected_prefix = parse_record_ref(record_ref)
    path = control_dir / "journal" / writer_id / f"{to_base36(segment)}.hwj"
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with path.open("rb") as handle:
                header = handle.read(len(SEGMENT_HEADER))
                if len(header) != len(SEGMENT_HEADER):
                    raise EOFError("short journal segment header")
                if header != SEGMENT_HEADER:
                    raise WorkspaceCorruptionError(f"invalid journal segment header: {path}")
                handle.seek(offset)
                length_bytes = handle.read(_LENGTH.size)
                if len(length_bytes) != _LENGTH.size:
                    raise EOFError("short frame length")
                (length,) = _LENGTH.unpack(length_bytes)
                if length != expected_length:
                    raise WorkspaceCorruptionError("record reference length disagrees with journal frame")
                payload = handle.read(length)
                checksum = handle.read(32)
                trailer = handle.read(_LENGTH.size)
                if len(payload) != length or len(checksum) != 32 or len(trailer) != _LENGTH.size:
                    raise EOFError("short journal frame")
                if trailer != length_bytes:
                    raise WorkspaceCorruptionError("journal frame trailer disagrees with header")
                actual_checksum = hashlib.sha256(length_bytes + payload).digest()
                if actual_checksum != checksum:
                    raise WorkspaceCorruptionError("journal frame checksum mismatch")
                if checksum[:16].hex() != expected_prefix:
                    raise WorkspaceCorruptionError("record reference checksum mismatch")
                value = json.loads(payload)
                if not isinstance(value, dict):
                    raise WorkspaceCorruptionError("journal record is not a JSON object")
                return value
        except (FileNotFoundError, EOFError, json.JSONDecodeError, UnicodeError) as exc:
            last_error = exc
            time.sleep(retry_delay(attempt))
    raise WorkspaceUnavailableError(f"journal record is not coherently visible: {record_ref}") from last_error
