"""Strict local protocol for confined daemon MPI application steps."""

import json
import re
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ._daemon_mailbox import MailboxDirectory

_MANIFEST_FORMAT = "httk-workspace-mpi-manifest"
_REQUEST_FORMAT = "httk-workspace-mpi-request"
_RESULT_FORMAT = "httk-workspace-mpi-result"
_FORMAT_VERSION = 1
_MAX_MANIFEST_BYTES = 16 * 1024
_MAX_CONTROL_BYTES = 4 * 1024
_MAX_STREAM_BYTES = 65_536
_MAX_ARGUMENTS = 256
_MAX_ARGUMENT_BYTES = 4096
_MAX_CWD_BYTES = 4096
_MAX_ENVIRONMENT_ENTRIES = 256
_MAX_ENVIRONMENT_NAME_BYTES = 128
_MAX_ENVIRONMENT_VALUE_BYTES = 4096
_MAX_ERROR_BYTES = 1024
_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_RESERVED_ENVIRONMENT_PREFIXES = ("PMI_", "PMIX_", "OMPI_", "OPAL_", "SLURM_", "SLURMD_", "HTTK_DAEMON_MPI_")
_FRAME_LIMITS = {b"Q": _MAX_CONTROL_BYTES, b"O": _MAX_STREAM_BYTES, b"E": _MAX_STREAM_BYTES, b"X": _MAX_CONTROL_BYTES}
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    raise ValueError("nonfinite JSON value")


def _validate_unicode(value: object) -> None:
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, list):
        for item in value:
            _validate_unicode(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _validate_unicode(key)
            _validate_unicode(item)


def _decode_object(data: bytes, kind: str, limit: int) -> dict[str, object]:
    if type(data) is not bytes or len(data) > limit:
        raise ValueError(f"invalid {kind} document")
    if any(data.startswith(bom) for bom in _BOMS):
        raise ValueError(f"{kind} must be UTF-8 without a BOM")
    try:
        value = json.loads(
            data.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"invalid {kind} document") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{kind} must be a JSON object")
    return value


def _encode_object(value: dict[str, object], kind: str, limit: int) -> bytes:
    try:
        data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError(f"{kind} cannot be encoded") from exc
    if len(data) > limit:
        raise ValueError(f"{kind} document exceeds {limit} bytes")
    return data


def _identifier(value: object, name: str) -> str:
    if type(value) is not str or _ID_PATTERN.fullmatch(value) is None:
        raise ValueError(f"invalid {name}")
    return value


def _workspace_id(value: object) -> str:
    if type(value) is not str:
        raise ValueError("invalid workspace_id")
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except (AttributeError, ValueError) as exc:
        raise ValueError("invalid workspace_id") from exc
    return value


def _bounded_string(value: object, name: str, limit: int, *, empty: bool = True) -> str:
    if type(value) is not str or "\0" in value or (not empty and not value):
        raise ValueError(f"invalid {name}")
    try:
        size = len(value.encode("utf-8", errors="strict"))
    except UnicodeError as exc:
        raise ValueError(f"invalid {name}") from exc
    if size > limit:
        raise ValueError(f"{name} exceeds {limit} bytes")
    return value


def _relative_path(value: object) -> str:
    cwd = _bounded_string(value, "cwd", _MAX_CWD_BYTES, empty=False)
    path = PurePosixPath(cwd)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != cwd:
        raise ValueError("cwd must be a canonical workspace-relative path")
    return cwd


def _reserved_environment(name: str) -> bool:
    return name.startswith(_RESERVED_ENVIRONMENT_PREFIXES)


def _environment(entries: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(entries, tuple) or len(entries) > _MAX_ENVIRONMENT_ENTRIES:
        raise ValueError("environment must be a bounded tuple of pairs")
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise ValueError("environment entries must be pairs")
        name = _bounded_string(entry[0], "environment name", _MAX_ENVIRONMENT_NAME_BYTES, empty=False)
        value = _bounded_string(entry[1], "environment value", _MAX_ENVIRONMENT_VALUE_BYTES)
        if _ENVIRONMENT_NAME.fullmatch(name) is None or _reserved_environment(name):
            raise ValueError("invalid or reserved environment name")
        if name in seen:
            raise ValueError("duplicate environment name")
        seen.add(name)
        result.append((name, value))
    return tuple(sorted(result))


@dataclass(frozen=True, slots=True)
class Manifest:
    """Describe one application after all outer launch authority is fixed.

    :param request_id: Random application request identifier.
    :param manager_handle: Protected daemon manager handle.
    :param workspace_id: Canonical workspace UUID.
    :param argv: Nonempty application argument vector.
    :param cwd: Canonical path relative to ``/workspace``.
    :param environment: Nonreserved application environment entries.
    """

    request_id: str
    manager_handle: str
    workspace_id: str
    argv: tuple[str, ...]
    cwd: str
    environment: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        """Validate and canonicalize manifest members."""

        _identifier(self.request_id, "request_id")
        _identifier(self.manager_handle, "manager_handle")
        _workspace_id(self.workspace_id)
        if not isinstance(self.argv, tuple) or not self.argv or len(self.argv) > _MAX_ARGUMENTS:
            raise ValueError("argv must be a nonempty bounded tuple")
        for index, argument in enumerate(self.argv):
            _bounded_string(argument, "argv entry", _MAX_ARGUMENT_BYTES, empty=index != 0)
        _relative_path(self.cwd)
        object.__setattr__(self, "environment", _environment(self.environment))


def encode_manifest(manifest: Manifest) -> bytes:
    """Encode one canonical bounded application manifest.

    :param manifest: Validated manifest value.
    :return: Canonical ASCII JSON bytes.
    :raises ValueError: If the value is not a manifest or exceeds the bound.
    """

    if type(manifest) is not Manifest:
        raise ValueError("manifest must be a Manifest")
    return _encode_object(
        {
            "format": _MANIFEST_FORMAT,
            "format_version": _FORMAT_VERSION,
            "request_id": manifest.request_id,
            "manager_handle": manifest.manager_handle,
            "workspace_id": manifest.workspace_id,
            "argv": list(manifest.argv),
            "cwd": manifest.cwd,
            "environment": dict(manifest.environment),
        },
        "manifest",
        _MAX_MANIFEST_BYTES,
    )


def decode_manifest(data: bytes) -> Manifest:
    """Decode one strict bounded application manifest.

    :param data: Raw manifest bytes.
    :return: Validated immutable manifest.
    :raises ValueError: If the document is malformed or outside the protocol.
    """

    value = _decode_object(data, "manifest", _MAX_MANIFEST_BYTES)
    fields = {"format", "format_version", "request_id", "manager_handle", "workspace_id", "argv", "cwd", "environment"}
    if set(value) != fields:
        raise ValueError("manifest fields are missing or unknown")
    if value["format"] != _MANIFEST_FORMAT or type(value["format_version"]) is not int or value["format_version"] != 1:
        raise ValueError("unsupported manifest format or version")
    raw_argv = value["argv"]
    raw_environment = value["environment"]
    if not isinstance(raw_argv, list) or not isinstance(raw_environment, dict):
        raise ValueError("invalid manifest fields")
    return Manifest(
        request_id=value["request_id"],  # type: ignore[arg-type]
        manager_handle=value["manager_handle"],  # type: ignore[arg-type]
        workspace_id=value["workspace_id"],  # type: ignore[arg-type]
        argv=tuple(raw_argv),
        cwd=value["cwd"],  # type: ignore[arg-type]
        environment=tuple(raw_environment.items()),
    )


def encode_request(request_id: str) -> bytes:
    """Encode one bounded run request containing only its identifier.

    :param request_id: Application request identifier.
    :return: Canonical ASCII JSON bytes.
    """

    request_id = _identifier(request_id, "request_id")
    return _encode_object(
        {
            "format": _REQUEST_FORMAT,
            "format_version": _FORMAT_VERSION,
            "operation": "run",
            "request_id": request_id,
        },
        "request",
        _MAX_CONTROL_BYTES,
    )


def decode_request(data: bytes) -> str:
    """Decode one exact bounded run request.

    :param data: Raw request bytes.
    :return: Validated request identifier.
    """

    value = _decode_object(data, "request", _MAX_CONTROL_BYTES)
    if set(value) != {"format", "format_version", "operation", "request_id"}:
        raise ValueError("request fields are missing or unknown")
    if value["format"] != _REQUEST_FORMAT or type(value["format_version"]) is not int or value["format_version"] != 1:
        raise ValueError("unsupported request format or version")
    if value["operation"] != "run":
        raise ValueError("unsupported MPI request operation")
    return _identifier(value["request_id"], "request_id")


def encode_terminal(code: int, error: str | None = None) -> bytes:
    """Encode one terminal application result.

    :param code: Process exit status from zero through 255.
    :param error: Optional bounded diagnostic.
    :return: Canonical ASCII JSON bytes.
    """

    if type(code) is not int or not 0 <= code <= 255:
        raise ValueError("terminal code must be an integer from 0 through 255")
    value: dict[str, object] = {"format": _RESULT_FORMAT, "format_version": _FORMAT_VERSION, "code": code}
    if error is not None:
        value["error"] = _bounded_string(error, "terminal error", _MAX_ERROR_BYTES)
    return _encode_object(value, "terminal result", _MAX_CONTROL_BYTES)


def decode_terminal(data: bytes) -> tuple[int, str | None]:
    """Decode one exact bounded terminal application result.

    :param data: Raw result bytes.
    :return: Exit status and optional diagnostic.
    """

    value = _decode_object(data, "terminal result", _MAX_CONTROL_BYTES)
    required = {"format", "format_version", "code"}
    if not required <= set(value) or not set(value) <= required | {"error"}:
        raise ValueError("terminal result fields are missing or unknown")
    if value["format"] != _RESULT_FORMAT or type(value["format_version"]) is not int or value["format_version"] != 1:
        raise ValueError("unsupported terminal result format or version")
    code = value["code"]
    if type(code) is not int or not 0 <= code <= 255:
        raise ValueError("terminal code must be an integer from 0 through 255")
    error = value.get("error")
    if error is not None:
        error = _bounded_string(error, "terminal error", _MAX_ERROR_BYTES)
    return code, error


def _frame_limit(kind: bytes) -> int:
    if type(kind) is not bytes or kind not in _FRAME_LIMITS:
        raise ValueError("invalid MPI frame kind")
    return _FRAME_LIMITS[kind]


def send_frame(sock: socket.socket, kind: bytes, payload: bytes) -> None:
    """Send one bounded length-prefixed protocol frame.

    :param sock: Connected Unix stream socket.
    :param kind: One of ``Q``, ``O``, ``E`` or ``X`` as one byte.
    :param payload: Frame bytes within the kind-specific bound.
    """

    limit = _frame_limit(kind)
    if type(payload) is not bytes or len(payload) > limit:
        raise ValueError(f"MPI frame payload exceeds {limit} bytes")
    sock.sendall(kind + len(payload).to_bytes(4, "big") + payload)


def _receive_exact(sock: socket.socket, size: int, deadline: float | None) -> bytes:
    result = bytearray()
    while len(result) < size:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out while receiving MPI frame")
            sock.settimeout(remaining)
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise EOFError("EOF before complete MPI frame")
        result.extend(chunk)
    return bytes(result)


def recv_frame(sock: socket.socket) -> tuple[bytes, bytes]:
    """Receive one exact bounded length-prefixed protocol frame.

    :param sock: Connected Unix stream socket.
    :return: Frame kind and payload.
    :raises EOFError: If EOF arrives before the frame is complete.
    :raises TimeoutError: If the socket's configured total receive deadline expires.
    :raises ValueError: If the kind or declared length is invalid.
    """

    timeout = sock.gettimeout()
    deadline = None if timeout is None else time.monotonic() + timeout
    try:
        header = _receive_exact(sock, 5, deadline)
        kind = header[:1]
        length = int.from_bytes(header[1:], "big")
        limit = _frame_limit(kind)
        if length > limit:
            raise ValueError(f"MPI frame payload exceeds {limit} bytes")
        return kind, _receive_exact(sock, length, deadline)
    finally:
        sock.settimeout(timeout)


def manifest_path(workspace: Path, handle: str, request_id: str) -> Path:
    """Return the fixed manifest path for one manager and request.

    :param workspace: Absolute workspace root.
    :param handle: Protected manager handle.
    :param request_id: Application request identifier.
    :return: Manifest path below the workspace control directory.
    """

    if not isinstance(workspace, Path) or not workspace.is_absolute() or ".." in workspace.parts:
        raise ValueError("workspace must be an absolute Path without parent traversal")
    return (
        workspace
        / ".httk-workspace"
        / "mpi"
        / _identifier(handle, "manager_handle")
        / f"{_identifier(request_id, 'request_id')}.json"
    )


def read_manifest(workspace: Path, handle: str, request_id: str) -> Manifest:
    """Read and bind one manifest through the descriptor-anchored mailbox primitive.

    :param workspace: Absolute workspace root visible inside the rank sandbox.
    :param handle: Expected protected manager handle.
    :param request_id: Expected application request identifier.
    :return: Strict decoded manifest with matching path identities.
    """

    path = manifest_path(workspace, handle, request_id)
    with MailboxDirectory(path.parent) as mailbox:
        manifest = decode_manifest(mailbox.read(path.name))
    if manifest.manager_handle != handle or manifest.request_id != request_id:
        raise ValueError("manifest identity does not match its path")
    return manifest


__all__ = [
    "Manifest",
    "decode_manifest",
    "decode_request",
    "decode_terminal",
    "encode_manifest",
    "encode_request",
    "encode_terminal",
    "manifest_path",
    "read_manifest",
    "recv_frame",
    "send_frame",
]
