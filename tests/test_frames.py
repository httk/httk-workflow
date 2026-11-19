"""The transfer session's binary frames and the streams carried in them (``httk.workflow._frames``)."""

import io
import json
import struct
import tarfile
import threading
import time
from collections.abc import Buffer
from pathlib import Path
from typing import BinaryIO

import pytest

from httk.workflow import _fs
from httk.workflow._frames import (
    DATA,
    DATA_LIMIT,
    END,
    HEADER_SIZE,
    JSON_LIMIT,
    KINDS,
    PROGRESS,
    REQUEST,
    RESPONSE,
    FrameReader,
    FrameWriter,
    StreamReader,
    StreamWriter,
    encode_frame,
    encode_json,
)
from httk.workflow.errors import StreamAborted, TransferProtocolError


def _reader(*frames: bytes) -> FrameReader:
    return FrameReader(io.BytesIO(b"".join(frames)))


def _stream(request_id: int, *frames: bytes) -> tuple[StreamReader, FrameReader]:
    reader = _reader(*frames)
    return StreamReader(reader.read_frame, request_id), reader


class _Trickle(io.RawIOBase, BinaryIO):
    """A stream that returns at most one byte per read, as a pipe may."""

    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._data = io.BytesIO(data)

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer, /) -> int:
        chunk = self._data.read(1)
        memoryview(buffer).cast("B")[: len(chunk)] = chunk
        return len(chunk)


def _tar(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w|") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def _data_frames(request_id: int, data: bytes, size: int = 4096) -> list[bytes]:
    return [encode_frame(DATA, request_id, data[offset : offset + size]) for offset in range(0, len(data), size)]


def test_every_kind_round_trips_with_its_header_id_even_through_short_reads() -> None:
    documents = {
        REQUEST: {"op": "echo", "upload": True},
        END: {"ok": False, "abort": "why"},
        PROGRESS: {"phase": "extract", "count": 7},
        RESPONSE: {"ok": True, "names": ["ä", "b"]},
    }
    wire = b"".join(encode_json(kind, 7, document) for kind, document in documents.items())
    payload = bytes(range(256)) * 3
    wire += encode_frame(DATA, 2**32 - 1, payload) + encode_frame(DATA, 1, b"")
    assert set(documents) | {DATA} == KINDS

    reader = FrameReader(_Trickle(wire))
    for kind, document in documents.items():
        frame = reader.read_frame()
        assert frame is not None and (frame.kind, frame.id, frame.document) == (kind, 7, document)
    data = reader.read_frame()
    assert data is not None and (data.kind, data.id, data.data, data.size) == (DATA, 2**32 - 1, payload, 768)
    empty = reader.read_frame()
    assert empty is not None and empty.data == b"" and empty.id == 1
    assert reader.read_frame() is None and reader.ended
    assert HEADER_SIZE == 9


def test_a_frame_writer_sends_what_the_reader_decodes() -> None:
    sink = io.BytesIO()
    writer = FrameWriter(sink.write)
    writer.send_json(PROGRESS, 3, {"phase": "sync", "count": None})
    writer.send(DATA, 3, memoryview(b"xabc")[1:])
    reader = _reader(sink.getvalue())
    progress, data = reader.read_frame(), reader.read_frame()
    assert progress is not None and progress.document == {"phase": "sync", "count": None} and progress.id == 3
    assert data is not None and data.data == b"abc"


def _header(kind: bytes, request_id: int, length: int) -> bytes:
    return struct.pack(">cII", kind, request_id, length)


@pytest.mark.parametrize(
    ("wire", "message"),
    [
        (_header(b"D", 1, DATA_LIMIT + 1), "exceeds"),
        (_header(b"R", 1, JSON_LIMIT + 1), "exceeds"),
        (_header(b"X", 1, 2) + b"{}", "unknown frame kind"),
        (_header(b"R", 0, 2) + b"{}", "request id 0"),
        (_header(b"R", 1, 5) + b"{nope", "undecodable"),
        (_header(b"R", 1, 4) + b"\xff\xfe{}", "undecodable"),
        (_header(b"R", 1, 4) + "{}".encode("utf-16-le"), "undecodable"),  # JSON, but not UTF-8
        (_header(b"R", 1, 2) + b"[]", "JSON object"),
        pytest.param(
            _header(b"R", 1, 200_006) + b'{"a":' + b"[" * 100_000 + b"]" * 100_000 + b"}",
            "RecursionError",
            id="nested",
        ),
        (b"Q\x00\x00\x00\x01\x00", "inside a frame header"),
        (_header(b"D", 1, 10) + b"short", "inside a D frame"),
    ],
)
def test_a_broken_frame_is_a_latched_protocol_error(wire: bytes, message: str) -> None:
    reader = _reader(wire)
    with pytest.raises(TransferProtocolError, match=message):
        reader.read_frame()
    with pytest.raises(TransferProtocolError, match=message):
        reader.read_frame()  # latched: nothing after a broken frame is trusted


def test_json_limit_is_512_mib() -> None:
    # Transfer protocol 2 (plan WORKFLOW_TRANSFER_SESSION §7.12): a hold or an adoption of a tree of about a million
    # jobs fits one message.
    assert JSON_LIMIT == 512 << 20


def test_encoding_refuses_what_decoding_would() -> None:
    with pytest.raises(TransferProtocolError, match="exceeds"):
        encode_frame(DATA, 1, bytes(DATA_LIMIT + 1))
    with pytest.raises(TransferProtocolError, match="unknown frame kind"):
        encode_frame("X", 1, b"")
    for bad in (0, 2**32, True, -1):
        with pytest.raises(TransferProtocolError, match="request id"):
            encode_frame(DATA, bad, b"")
    with pytest.raises(TransferProtocolError, match="raw bytes"):
        encode_json(DATA, 1, {})


def test_an_upload_feeds_tarfile_and_finishes_at_its_terminal(tmp_path: Path) -> None:
    archive = _tar({"a/one.txt": b"one\n", "a/two.txt": b"two" * 5000})
    upload, _ = _stream(4, *_data_frames(4, archive, 777), encode_json(END, 4, {"ok": True}))
    target = tmp_path / "landing"
    target.mkdir()

    assert _fs.extract_tar(upload, target) == 2
    upload.finish()
    assert (target / "a" / "two.txt").read_bytes() == b"two" * 5000
    assert upload.terminal == {"ok": True} and upload.bytes == len(archive) and upload.succeeded
    assert upload.read() == b""  # a successful terminal reads as the end of the stream


def test_finish_refuses_an_aborted_stream() -> None:
    archive = _tar({"x": b"x"})
    upload, _ = _stream(1, *_data_frames(1, archive), encode_json(END, 1, {"ok": False, "abort": "EDQUOT"}))
    with tarfile.open(fileobj=upload, mode="r|") as stream:
        assert [member.name for member in stream] == ["x"]
    with pytest.raises(StreamAborted, match="EDQUOT"):
        upload.finish()


def test_finish_refuses_a_complete_archive_without_its_terminal() -> None:
    # The archive is whole (its end-of-archive blocks included), but the input ends before E: not a success.
    archive = _tar({"x": b"x" * 2000})
    upload, _ = _stream(1, *_data_frames(1, archive))
    with tarfile.open(fileobj=upload, mode="r|") as stream:
        assert [member.name for member in stream] == ["x"]
    with pytest.raises(StreamAborted, match="input ended"):
        upload.finish()
    assert upload.cut and upload.terminal is None


def test_reading_into_an_abort_or_the_end_of_input_raises() -> None:
    aborted, _ = _stream(2, encode_frame(DATA, 2, b"abc"), encode_json(END, 2, {"ok": False}))
    assert aborted.read(3) == b"abc"
    with pytest.raises(StreamAborted):
        aborted.read(1)
    cut, _ = _stream(2, encode_frame(DATA, 2, b"abc"))
    assert cut.read(10) == b"abc"
    with pytest.raises(StreamAborted, match="input ended"):
        cut.read(1)
    cut.close()
    with pytest.raises(ValueError, match="closed"):
        cut.read(1)


def test_discard_drains_to_the_terminal_and_leaves_the_next_frame() -> None:
    upload, reader = _stream(
        5,
        *_data_frames(5, bytes(50_000), 1000),
        encode_json(END, 5, {"ok": True}),
        encode_json(REQUEST, 6, {"op": "next"}),
    )
    assert upload.read(10) == bytes(10)
    upload.discard()
    assert upload.terminal == {"ok": True}
    following = reader.read_frame()
    assert following is not None and following.document == {"op": "next"} and following.id == 6


def test_a_stream_can_start_from_a_frame_already_taken() -> None:
    first = encode_frame(DATA, 3, b"head")
    reader = _reader(first, encode_frame(DATA, 3, b"tail"), encode_json(END, 3, {"ok": True}))
    taken = reader.read_frame()
    stream = StreamReader(reader.read_frame, 3, taken)
    assert stream.read() == b"headtail"


@pytest.mark.parametrize(
    "intruder",
    [
        encode_json(END, 9, {"ok": True}),
        encode_frame(DATA, 9, b"stale"),
        encode_json(RESPONSE, 1, {"ok": True}),
        encode_json(END, 1, {}),
        encode_json(END, 1, {"ok": False, "abort": 3}),
    ],
)
def test_a_foreign_frame_inside_a_stream_is_a_latched_protocol_error(intruder: bytes) -> None:
    upload, _ = _stream(1, encode_frame(DATA, 1, b"a"), intruder, encode_json(END, 1, {"ok": True}))
    with pytest.raises(TransferProtocolError):
        upload.finish()
    with pytest.raises(TransferProtocolError):
        upload.discard()


def _decoded(wire: bytes) -> list[tuple[str, int, object]]:
    reader = _reader(wire)
    frames: list[tuple[str, int, object]] = []
    while (frame := reader.read_frame()) is not None:
        frames.append((frame.kind, frame.id, frame.data if frame.kind == DATA else frame.document))
    return frames


class _Recorder:
    """A sink that records each write's length, to show that large writes are not copied into one buffer."""

    def __init__(self) -> None:
        self.buffer = io.BytesIO()
        self.writes: list[int] = []

    def write(self, data: Buffer) -> int:
        size = memoryview(data).nbytes
        self.writes.append(size)
        return self.buffer.write(data)


def test_a_stream_writer_chunks_and_ends_once() -> None:
    sink = _Recorder()
    with StreamWriter(FrameWriter(sink.write), 7) as stream:
        stream.write(b"head")
        stream.write(bytes(2 * DATA_LIMIT + 10))
        stream.write(b"tail")
    stream.close()  # a second close sends nothing
    frames = _decoded(sink.buffer.getvalue())
    assert [kind for kind, _, _ in frames] == [DATA, DATA, DATA, END]
    assert frames[0][2] == b"head" + bytes(DATA_LIMIT - 4)
    assert frames[1][2] == bytes(DATA_LIMIT)
    assert frames[2][2] == bytes(14) + b"tail"
    assert frames[3][1:] == (7, {"ok": True})
    with pytest.raises(ValueError, match="ended"):
        stream.write(b"late")


def test_a_large_write_is_sent_without_copying() -> None:
    sources: list[object] = []

    def write(data: Buffer) -> None:
        sources.append(memoryview(data).obj)

    original = bytearray(3 * DATA_LIMIT)
    with StreamWriter(FrameWriter(write), 1) as stream:
        stream.write(original)
    payloads = sources[1::2][:3]  # header, payload, header, payload, ...
    assert len(payloads) == 3 and all(source is original for source in payloads)


def test_a_frame_writer_writes_header_and_payload_separately() -> None:
    sink = _Recorder()
    FrameWriter(sink.write).send(DATA, 1, bytes(DATA_LIMIT))
    assert sink.writes == [HEADER_SIZE, DATA_LIMIT]


def test_a_stream_writer_left_by_an_exception_sends_exactly_one_abort() -> None:
    sink = io.BytesIO()
    with pytest.raises(OSError, match="disk full"), StreamWriter(FrameWriter(sink.write), 8) as stream:
        stream.write(b"partial")
        raise OSError("disk full")
    stream.close()
    assert _decoded(sink.getvalue()) == [(END, 8, {"ok": False, "abort": "OSError: disk full"})]


def test_a_reader_round_trips_what_a_stream_writer_wrote() -> None:
    sink = io.BytesIO()
    payload = json.dumps({"k": list(range(1000))}).encode() * 300
    with StreamWriter(FrameWriter(sink.write), 1) as stream:
        stream.write(payload)
    upload, _ = _stream(1, sink.getvalue())
    assert upload.read() == payload
    upload.finish()


def test_a_frame_writer_keeps_frames_whole_under_concurrent_writers() -> None:
    sink = io.BytesIO()

    def write(data: Buffer) -> None:
        sink.write(data)
        time.sleep(0)  # invite a switch between a frame's header and its payload

    writer = FrameWriter(write)

    def send(request_id: int) -> None:
        for index in range(200):
            writer.send(DATA, request_id, bytes([request_id]) * (index + 1))

    threads = [threading.Thread(target=send, args=(request_id,)) for request_id in range(1, 9)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    frames = _decoded(sink.getvalue())
    assert len(frames) == 8 * 200
    for _kind, request_id, data in frames:
        assert isinstance(data, bytes) and set(data) == {request_id}
