"""Binary frames of the transfer session, and the streams carried in them.

After its hello line, a transfer session speaks frames in both directions. A frame header is one byte of kind,
the request id (four bytes, big-endian unsigned) and the payload length (four bytes, big-endian), then the payload.
The header's id is authoritative: every frame belongs to the request it names. ``Q`` (request), ``E`` (stream
terminal), ``P`` (progress) and ``R`` (response) carry one compact UTF-8 JSON object; ``D`` carries raw stream
bytes. Version 1 is half-duplex per request: the client sends ``Q``, then, for an upload, ``D`` frames and exactly
one ``E``; the far side sends only ``P`` frames until it has read that terminal, then the request's download
(``D`` frames and one ``E``), if any, and exactly one ``R``.

A frame that breaks these rules (an unknown kind, an oversized length, undecodable JSON, a frame of another request)
is a :class:`~httk.workflow.errors.TransferProtocolError`, after which the session is unusable; decoders latch it. A
stream is complete only when its terminal says so: a reader that meets an aborting terminal or the end of its input
raises :class:`~httk.workflow.errors.StreamAborted` instead of reporting the end of the stream, so a truncated
stream is never mistaken for a whole one.

This module is the codec only: it imports nothing of the command line and nothing heavy.
"""

import contextlib
import io
import json
import struct
import threading
from collections.abc import Buffer, Callable, Mapping
from dataclasses import dataclass
from typing import IO, Any, BinaryIO, Self

from .errors import StreamAborted, TransferProtocolError

__all__ = [
    "DATA",
    "DATA_LIMIT",
    "END",
    "HEADER_SIZE",
    "JSON_LIMIT",
    "KINDS",
    "PROGRESS",
    "REQUEST",
    "RESPONSE",
    "TICK",
    "Frame",
    "FrameReader",
    "FrameWriter",
    "StreamReader",
    "StreamWriter",
    "encode_frame",
    "encode_header",
    "encode_json",
]

#: A request: ``{"op", ...}``, with ``"upload": true`` when stream data follows.
REQUEST = "Q"
#: Stream data: raw bytes of the request's upload or download.
DATA = "D"
#: A stream terminal: ``{"ok": true}``, or ``{"ok": false, "abort": why}``.
END = "E"
#: Progress of the request the far side executes: ``{"phase", "count"}``.
PROGRESS = "P"
#: The one final response to a request: ``{"ok": bool, ...}``.
RESPONSE = "R"
#: Every frame kind.
KINDS = frozenset({REQUEST, DATA, END, PROGRESS, RESPONSE})
#: The largest ``D`` payload, in bytes.
DATA_LIMIT = 1 << 20
#: The largest JSON payload, in bytes.
JSON_LIMIT = 64 << 20
#: Seconds between two progress frames of a request the far side executes. A client's bound is at least three.
TICK = 10.0

_HEADER = struct.Struct(">cII")
#: The bytes of a frame header.
HEADER_SIZE = _HEADER.size
_ID_LIMIT = (1 << 32) - 1


@dataclass(frozen=True)
class Frame:
    """One decoded frame.

    :param kind: The frame kind, one of :data:`KINDS`.
    :param id: The id of the request the frame belongs to.
    :param data: The raw payload of a ``D`` frame; empty for the other kinds.
    :param document: The JSON payload of a ``Q``, ``E``, ``P`` or ``R`` frame; ``None`` for ``D``.
    :param size: The payload length on the wire.
    """

    kind: str
    id: int
    data: bytes = b""
    document: dict[str, Any] | None = None
    size: int = 0


def _limit(kind: str) -> int:
    return DATA_LIMIT if kind == DATA else JSON_LIMIT


def encode_header(kind: str, request_id: int, length: int) -> bytes:
    """Return one frame header.

    :param kind: The frame kind.
    :param request_id: The request id, 1 to 2**32-1.
    :param length: The payload length.
    :return: The header bytes.
    :raises TransferProtocolError: If the kind or id is invalid or the length exceeds the kind's limit.
    """

    if kind not in KINDS:
        raise TransferProtocolError(f"unknown frame kind {kind!r}")
    if isinstance(request_id, bool) or not isinstance(request_id, int) or not 0 < request_id <= _ID_LIMIT:
        raise TransferProtocolError(f"invalid request id {request_id!r}")
    if length > _limit(kind):
        raise TransferProtocolError(f"a {kind} frame of {length} bytes exceeds {_limit(kind)} bytes")
    return _HEADER.pack(kind.encode("ascii"), request_id, length)


def encode_frame(kind: str, request_id: int, payload: Buffer) -> bytes:
    """Return one whole frame.

    :param kind: The frame kind.
    :param request_id: The request id.
    :param payload: The frame payload.
    :return: The header followed by the payload.
    """

    view = memoryview(payload).cast("B")
    return encode_header(kind, request_id, len(view)) + view


def encode_json(kind: str, request_id: int, document: Mapping[str, Any]) -> bytes:
    """Return one whole JSON frame.

    :param kind: The frame kind; not ``D``.
    :param request_id: The request id.
    :param document: The JSON object.
    :return: The encoded frame.
    :raises TransferProtocolError: If the kind is ``D`` or the frame is invalid.
    """

    if kind == DATA:
        raise TransferProtocolError("a D frame carries raw bytes, not JSON")
    return encode_frame(kind, request_id, json.dumps(document, separators=(",", ":")).encode("utf-8"))


class FrameReader:
    """The incremental frame decoder of one direction, reading whole frames from a binary stream.

    A decoding failure is latched: every later read raises it again.

    :param stream: The binary input; ``read`` may return short counts.
    """

    def __init__(self, stream: IO[bytes]) -> None:
        self._stream = stream
        #: Whether the input ended (at a frame boundary or inside a frame).
        self.ended = False
        #: The protocol error this decoder raised, if any.
        self.failure: TransferProtocolError | None = None

    def _exactly(self, size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = self._stream.read(remaining)
            if not chunk:
                self.ended = True
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def read_frame(self) -> Frame | None:
        """Read the next whole frame.

        :return: The frame, or ``None`` when the input ends at a frame boundary.
        :raises TransferProtocolError: If the input ends inside a frame or the frame breaks the protocol.
        """

        if self.failure is not None:
            raise self.failure
        try:
            return self._read()
        except TransferProtocolError as exc:
            self.failure = exc
            raise

    def _read(self) -> Frame | None:
        if self.ended:
            return None
        header = self._exactly(_HEADER.size)
        if not header:
            return None
        if len(header) < _HEADER.size:
            raise TransferProtocolError("the input ended inside a frame header")
        raw_kind, request_id, length = _HEADER.unpack(header)
        kind = raw_kind.decode("latin-1")
        if kind not in KINDS:
            raise TransferProtocolError(f"unknown frame kind {raw_kind!r}")
        if request_id == 0:
            raise TransferProtocolError("a frame of request id 0")
        if length > _limit(kind):
            raise TransferProtocolError(f"a {kind} frame of {length} bytes exceeds {_limit(kind)} bytes")
        payload = self._exactly(length)
        if len(payload) < length:
            raise TransferProtocolError(f"the input ended inside a {kind} frame")
        if kind == DATA:
            return Frame(kind, request_id, data=payload, size=length)
        try:
            document = json.loads(payload.decode("utf-8"))
        except Exception as exc:  # a ValueError, but also a RecursionError for a deeply nested document
            raise TransferProtocolError(f"undecodable {kind} frame: {type(exc).__name__}: {exc}") from None
        if not isinstance(document, dict):
            raise TransferProtocolError(f"a {kind} frame must carry a JSON object")
        return Frame(kind, request_id, document=document, size=length)


class FrameWriter:
    """Send whole frames through one sink, serialized by :attr:`lock`.

    Each frame's header and payload are written back to back under the lock, then flushed, so frames from several
    threads (a far side's progress ticker and its download) never interleave. The payload is written as given,
    never copied into a joined buffer.

    :param write: Writes all of the bytes it is given.
    :param flush: Pushes written bytes out, called after each frame; nothing when omitted.
    """

    def __init__(self, write: Callable[[Buffer], object], flush: Callable[[], object] | None = None) -> None:
        self._write = write
        self._flush = flush
        #: Held for each frame; hold it across several sends to keep them together.
        self.lock = threading.RLock()

    def send(self, kind: str, request_id: int, payload: Buffer = b"") -> None:
        """Send one frame.

        :param kind: The frame kind.
        :param request_id: The request id.
        :param payload: The payload.
        """

        view = memoryview(payload).cast("B")
        header = encode_header(kind, request_id, len(view))
        with self.lock:
            self._write(header)
            if view:
                self._write(view)
            if self._flush is not None:
                self._flush()

    def send_json(self, kind: str, request_id: int, document: Mapping[str, Any]) -> None:
        """Send one JSON frame.

        :param kind: The frame kind; not ``D``.
        :param request_id: The request id.
        :param document: The JSON object.
        :raises TransferProtocolError: If the kind is ``D``.
        """

        if kind == DATA:
            raise TransferProtocolError("a D frame carries raw bytes, not JSON")
        self.send(kind, request_id, json.dumps(document, separators=(",", ":")).encode("utf-8"))


class StreamWriter:
    """The sending side of one stream: ``D`` frames, then exactly one ``E``.

    Small writes are gathered into ``D`` frames of up to :data:`DATA_LIMIT` bytes; whole frames of a large write are
    sent straight from it, without copying. Used as a context manager, it ends the stream successfully on a normal
    exit and with an abort when the block raises. It is deliberately not an :class:`io.IOBase`, whose finalizer
    would close (and so complete) a stream its owner abandoned.

    :param frames: The writer the frames go through.
    :param request_id: The id of the request the stream belongs to.
    """

    def __init__(self, frames: FrameWriter, request_id: int) -> None:
        self._frames = frames
        self._id = request_id
        self._pending = bytearray()
        #: Whether the terminal was sent (or sending it was attempted).
        self.closed = False
        #: How many stream bytes were written.
        self.bytes = 0

    def __enter__(self) -> Self:
        """Return this writer."""

        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, _traceback: object) -> None:
        """End the stream: successfully, or with an abort naming the exception that left the block."""

        if exc is None:
            self.close()
        elif not self.closed:
            # The exception leaving the block is the one to report, not a failure to send the abort.
            with contextlib.suppress(Exception):
                self.close(ok=False, abort=f"{type(exc).__name__}: {exc}")

    def write(self, data: Buffer) -> int:
        """Append bytes to the stream.

        :param data: The bytes.
        :return: How many bytes were taken (all of them).
        :raises ValueError: If the stream already ended.
        """

        if self.closed:
            raise ValueError("write to an ended stream")
        view = memoryview(data).cast("B")
        size = len(view)
        self.bytes += size
        if self._pending:
            room = DATA_LIMIT - len(self._pending)
            self._pending += view[:room]
            view = view[room:]
            if len(self._pending) < DATA_LIMIT:
                return size
            self._frames.send(DATA, self._id, self._pending)
            self._pending = bytearray()
        while len(view) >= DATA_LIMIT:
            self._frames.send(DATA, self._id, view[:DATA_LIMIT])
            view = view[DATA_LIMIT:]
        self._pending += view
        return size

    def flush(self) -> None:
        """Send the gathered bytes now."""

        if self._pending and not self.closed:
            self._frames.send(DATA, self._id, self._pending)
            self._pending = bytearray()

    def close(self, ok: bool = True, abort: str | None = None) -> None:
        """Send the stream's terminal, once; later calls do nothing.

        A successful terminal first sends the gathered bytes; an abort drops them.

        :param ok: Whether the stream is complete.
        :param abort: Why the stream was aborted, when *ok* is false.
        """

        if self.closed:
            return
        if ok:
            self.flush()
        self.closed = True
        self._pending = bytearray()
        terminal: dict[str, Any] = {"ok": ok}
        if not ok:
            terminal["abort"] = abort or "aborted"
        self._frames.send_json(END, self._id, terminal)


class StreamReader(io.RawIOBase, BinaryIO):
    """The receiving side of one stream: the ``D`` frames of a request, up to its terminal.

    Readable as a binary file (``tarfile`` stream input, ``_fs.extract_tar``): a successful terminal reads as the end
    of the stream, while an aborting terminal or the end of the input raises
    :class:`~httk.workflow.errors.StreamAborted`. Data past what the consumer read is not validated by the stream
    itself: call :meth:`finish` to read up to the terminal and require success, or :meth:`discard` after a refusal.
    A protocol error is latched.

    :param next_frame: Returns the next frame of the stream's direction, or ``None`` at the end of the input.
    :param request_id: The id of the request the stream belongs to.
    :param first: A frame of the stream already taken from *next_frame*.
    """

    def __init__(self, next_frame: Callable[[], Frame | None], request_id: int, first: Frame | None = None) -> None:
        super().__init__()
        self._next_frame = next_frame
        self._id = request_id
        self._first = first
        self._chunk = b""
        self._offset = 0
        #: The protocol error this stream raised, if any; latched.
        self.failure: TransferProtocolError | None = None
        #: The terminal document once read, else ``None``.
        self.terminal: dict[str, Any] | None = None
        #: Whether the input ended before the terminal.
        self.cut = False
        #: How many stream bytes arrived.
        self.bytes = 0

    @property
    def done(self) -> bool:
        """Whether nothing more of this stream can arrive (its terminal was read, or the input ended)."""

        return self.terminal is not None or self.cut

    @property
    def succeeded(self) -> bool:
        """Whether the stream ended with a successful terminal."""

        return self.terminal is not None and self.terminal["ok"] is True

    def readable(self) -> bool:
        """Return true: the stream is readable."""

        return True

    def _failure(self) -> StreamAborted:
        if self.cut:
            return StreamAborted("the input ended inside the stream")
        assert self.terminal is not None
        return StreamAborted(str(self.terminal.get("abort", "the stream was aborted")))

    def _next(self) -> bool:
        """Read the next frame of the stream; return false at its end."""

        if self.failure is not None:
            raise self.failure
        if self.done:
            return False
        frame, self._first = (self._first, None) if self._first is not None else (self._next_frame(), None)
        try:
            return self._take(frame)
        except TransferProtocolError as exc:
            self.failure = exc
            raise

    def _take(self, frame: Frame | None) -> bool:
        if frame is None:
            self.cut = True
            return False
        if frame.id != self._id:
            raise TransferProtocolError(f"a {frame.kind} frame of request {frame.id} inside the stream of {self._id}")
        if frame.kind == DATA:
            self._chunk, self._offset = frame.data, 0
            self.bytes += len(frame.data)
            return True
        if frame.kind != END:
            raise TransferProtocolError(f"a {frame.kind} frame inside a stream")
        assert frame.document is not None
        abort = frame.document.get("abort")
        if not isinstance(frame.document.get("ok"), bool) or (abort is not None and not isinstance(abort, str)):
            raise TransferProtocolError(f"malformed stream terminal {frame.document!r}")
        self.terminal = frame.document
        return False

    def readinto(self, buffer: Buffer, /) -> int:
        """Read stream bytes into *buffer*.

        :param buffer: The writable buffer.
        :return: The number of bytes read; 0 only at a successful terminal.
        :raises ValueError: If the stream was closed.
        :raises StreamAborted: If the stream was aborted or the input ended first.
        """

        if self.closed:
            raise ValueError("read from a closed stream")
        view = memoryview(buffer).cast("B")
        if not view:
            return 0
        while self._offset >= len(self._chunk):
            if not self._next():
                if self.succeeded:
                    return 0
                raise self._failure()
        count = min(len(view), len(self._chunk) - self._offset)
        view[:count] = self._chunk[self._offset : self._offset + count]
        self._offset += count
        return count

    def _drain(self) -> None:
        self._chunk, self._offset = b"", 0
        while self._next():
            self._chunk, self._offset = b"", 0

    def finish(self) -> None:
        """Read up to the terminal, dropping unread data, and require a successful one.

        :raises StreamAborted: If the stream was aborted or the input ended before its terminal.
        """

        self._drain()
        if not self.succeeded:
            raise self._failure()

    def discard(self) -> None:
        """Read and drop the rest of the stream up to its terminal (or the end of the input), whatever it says."""

        self._drain()
