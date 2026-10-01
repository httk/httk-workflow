"""Strict codec, manifest-path and frame tests for confined MPI steps."""

import json
import os
import socket
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from httk.workflow._daemon_mpi_protocol import (
    Manifest,
    decode_manifest,
    decode_request,
    decode_terminal,
    encode_manifest,
    encode_request,
    encode_terminal,
    manifest_path,
    read_manifest,
    recv_frame,
    send_frame,
)

REQUEST_ID = "1" * 32
HANDLE = "2" * 32
WORKSPACE_ID = "12345678-1234-4234-8234-123456789abc"


def _manifest(**changes: object) -> Manifest:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "manager_handle": HANDLE,
        "workspace_id": WORKSPACE_ID,
        "argv": ("solver", "--input", "value with spaces"),
        "cwd": "jobs/example/run",
        "environment": (("PATH", "/usr/bin"), ("EMPTY", "")),
    }
    values.update(changes)
    return Manifest(**values)  # type: ignore[arg-type]


def test_manifest_round_trip_is_canonical_and_immutable() -> None:
    manifest = _manifest(environment=(("ZED", "2"), ("ALPHA", "1")))

    data = encode_manifest(manifest)

    assert data == encode_manifest(decode_manifest(data))
    assert decode_manifest(data) == manifest
    assert manifest.environment == (("ALPHA", "1"), ("ZED", "2"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("request_id", "A" * 32),
        ("manager_handle", "2" * 31),
        ("workspace_id", "not-a-uuid"),
        ("argv", ()),
        ("argv", ("",)),
        ("argv", ("solver\0bad",)),
        ("argv", tuple("x" for _ in range(257))),
        ("cwd", "/workspace"),
        ("cwd", "../escape"),
        ("cwd", "jobs/./run"),
        ("environment", (("PMIX_SERVER_URI", "attacker"),)),
        ("environment", (("SLURM_JWT", "attacker"),)),
        ("environment", (("SLURMD_NODENAME", "attacker"),)),
        ("environment", (("HTTK_DAEMON_MPI_HANDLE", "attacker"),)),
        ("environment", (("BAD-NAME", "value"),)),
        ("environment", (("DUP", "one"), ("DUP", "two"))),
        ("environment", (("NUL", "bad\0value"),)),
    ],
)
def test_manifest_rejects_invalid_authority_and_bounds(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _manifest(**{field: value})


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(extra=True),
        lambda value: value.pop("argv"),
        lambda value: value.update(format="wrong"),
        lambda value: value.update(format_version=True),
        lambda value: value.update(argv="solver"),
        lambda value: value.update(environment=[]),
    ],
)
def test_manifest_decoder_refuses_missing_unknown_and_wrong_types(
    mutate: Callable[[dict[str, object]], object],
) -> None:
    value = json.loads(encode_manifest(_manifest()))
    mutate(value)

    with pytest.raises(ValueError):
        decode_manifest(json.dumps(value).encode())


@pytest.mark.parametrize(
    "data",
    [
        b'{"format":"httk-workspace-mpi-manifest","format":"duplicate"}',
        b"[]",
        b"\xef\xbb\xbf{}",
        b'{"x":NaN}',
        b"{" + b" " * (16 * 1024),
        b'"\xed\xa0\x80"',
    ],
)
def test_manifest_decoder_refuses_malformed_documents(data: bytes) -> None:
    with pytest.raises(ValueError):
        decode_manifest(data)


def test_request_codec_contains_only_run_and_request_id() -> None:
    encoded = encode_request(REQUEST_ID)

    assert decode_request(encoded) == REQUEST_ID
    assert json.loads(encoded) == {
        "format": "httk-workspace-mpi-request",
        "format_version": 1,
        "operation": "run",
        "request_id": REQUEST_ID,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"request_id": "z" * 32},
        {"operation": "exec"},
        {"format_version": 2},
        {"extra": "argv"},
    ],
)
def test_request_decoder_rejects_invalid_fields(change: dict[str, object]) -> None:
    value = json.loads(encode_request(REQUEST_ID))
    value.update(change)
    with pytest.raises(ValueError):
        decode_request(json.dumps(value).encode())


@pytest.mark.parametrize("code", [0, 1, 127, 255])
def test_terminal_codec_round_trip(code: int) -> None:
    assert decode_terminal(encode_terminal(code)) == (code, None)
    assert decode_terminal(encode_terminal(code, "bounded failure")) == (code, "bounded failure")


@pytest.mark.parametrize("code", [True, -1, 256, 1.5])
def test_terminal_codec_rejects_invalid_status(code: object) -> None:
    with pytest.raises(ValueError):
        encode_terminal(code)  # type: ignore[arg-type]


def test_terminal_codec_rejects_oversize_nul_duplicate_and_unknown_error() -> None:
    with pytest.raises(ValueError):
        encode_terminal(2, "x" * 1025)
    with pytest.raises(ValueError):
        encode_terminal(2, "bad\0error")
    with pytest.raises(ValueError):
        decode_terminal(b'{"format":"httk-workspace-mpi-result","format_version":1,"code":1,"code":2}')
    value = json.loads(encode_terminal(2))
    value["extra"] = True
    with pytest.raises(ValueError):
        decode_terminal(json.dumps(value).encode())


@pytest.mark.parametrize("kind", [b"Q", b"O", b"E", b"X"])
def test_frames_handle_short_socket_reads(kind: bytes) -> None:
    left, right = socket.socketpair()
    payload = b"payload\x00bytes"
    try:
        send_frame(left, kind, payload)
        wrapper = _ShortReadSocket(right)
        assert recv_frame(wrapper) == (kind, payload)  # type: ignore[arg-type]
    finally:
        left.close()
        right.close()


class _ShortReadSocket:
    def __init__(self, wrapped: socket.socket) -> None:
        self.wrapped = wrapped

    def recv(self, size: int) -> bytes:
        return self.wrapped.recv(min(size, 2))

    def gettimeout(self) -> float | None:
        return self.wrapped.gettimeout()

    def settimeout(self, value: float | None) -> None:
        self.wrapped.settimeout(value)


def test_recv_frame_uses_one_total_deadline_across_partial_reads() -> None:
    left, right = socket.socketpair()
    right.settimeout(0.12)
    wire = b"Q" + (8).to_bytes(4, "big") + b"12345678"

    def drip() -> None:
        try:
            for byte in wire:
                left.sendall(bytes([byte]))
                time.sleep(0.04)
        except OSError:
            pass

    sender = threading.Thread(target=drip)
    sender.start()
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            recv_frame(right)
        assert time.monotonic() - started < 0.3
        assert right.gettimeout() == 0.12
    finally:
        right.close()
        left.close()
        sender.join(1)
        assert not sender.is_alive()


@pytest.mark.parametrize("kind", [b"", b"Z", b"QQ", "Q"])
def test_send_frame_rejects_invalid_kind(kind: object) -> None:
    left, right = socket.socketpair()
    try:
        with pytest.raises(ValueError):
            send_frame(left, kind, b"")  # type: ignore[arg-type]
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize(
    ("kind", "size"),
    [(b"Q", 4097), (b"X", 4097), (b"O", 65_537), (b"E", 65_537)],
)
def test_send_frame_rejects_kind_specific_output_bounds(kind: bytes, size: int) -> None:
    left, right = socket.socketpair()
    try:
        with pytest.raises(ValueError):
            send_frame(left, kind, b"x" * size)
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize(
    "wire",
    [
        b"",
        b"Q\x00",
        b"Q\x00\x00\x00\x04ab",
        b"Z\x00\x00\x00\x00",
        b"Q\x00\x00\x10\x01",
        b"O\x00\x01\x00\x01",
    ],
)
def test_recv_frame_rejects_eof_unknown_kind_and_oversize(wire: bytes) -> None:
    left, right = socket.socketpair()
    try:
        left.sendall(wire)
        left.shutdown(socket.SHUT_WR)
        with pytest.raises((EOFError, ValueError)):
            recv_frame(right)
    finally:
        left.close()
        right.close()


def test_manifest_path_and_descriptor_read_bind_path_identities(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    directory = workspace / ".httk-workspace" / "mpi" / HANDLE
    directory.mkdir(parents=True)
    path = manifest_path(workspace, HANDLE, REQUEST_ID)
    path.write_bytes(encode_manifest(_manifest()))

    assert path == directory / f"{REQUEST_ID}.json"
    assert read_manifest(workspace, HANDLE, REQUEST_ID) == _manifest()

    path.write_bytes(encode_manifest(_manifest(request_id="3" * 32)))
    with pytest.raises(ValueError, match="identity"):
        read_manifest(workspace, HANDLE, REQUEST_ID)


def test_manifest_read_refuses_symlink_fifo_and_oversize(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    directory = workspace / ".httk-workspace" / "mpi" / HANDLE
    directory.mkdir(parents=True)
    path = directory / f"{REQUEST_ID}.json"
    outside = tmp_path / "outside"
    outside.write_bytes(encode_manifest(_manifest()))
    path.symlink_to(outside)
    with pytest.raises(OSError):
        read_manifest(workspace, HANDLE, REQUEST_ID)
    path.unlink()
    os.mkfifo(path)
    with pytest.raises(ValueError):
        read_manifest(workspace, HANDLE, REQUEST_ID)
    path.unlink()
    path.write_bytes(b"x" * (16 * 1024 + 1))
    with pytest.raises(ValueError):
        read_manifest(workspace, HANDLE, REQUEST_ID)
