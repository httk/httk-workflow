"""Strict request documents for the confined workspace daemon."""

import hashlib
import json
import re
import uuid
from dataclasses import dataclass

_MAX_REQUEST_SIZE = 16 * 1024
_FORMAT = "httk-workspace-command"
_FORMAT_VERSION = 1
_OPERATIONS = frozenset({"health", "start_manager", "manager_status", "cancel_manager"})
_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_PROFILE_PATTERN = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")


@dataclass(frozen=True, slots=True)
class Request:
    """An immutable, validated daemon request.

    :param request_id: Identify this request with 32 lowercase hexadecimal digits.
    :param workspace_id: Identify the workspace with its canonical UUID.
    :param operation: Select one supported daemon operation.
    :param profile: Select a configured profile for ``start_manager``.
    :param handle: Identify a broker-issued manager handle.
    :raises ValueError: If a field is invalid or conflicts with the operation.
    """

    request_id: str
    workspace_id: str
    operation: str
    profile: str | None = None
    handle: str | None = None

    def __post_init__(self) -> None:
        """Refuse invalid fields and operation-specific combinations."""

        if type(self.request_id) is not str or _ID_PATTERN.fullmatch(self.request_id) is None:
            raise ValueError("invalid request_id")
        if type(self.workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        if type(self.operation) is not str or self.operation not in _OPERATIONS:
            raise ValueError("invalid operation")
        if self.profile is not None and type(self.profile) is not str:
            raise ValueError("invalid profile")
        if self.handle is not None and type(self.handle) is not str:
            raise ValueError("invalid handle")

        if self.operation == "health":
            if self.profile is not None or self.handle is not None:
                raise ValueError("health forbids operation fields")
        elif self.operation == "start_manager":
            if self.handle is not None or self.profile is None:
                raise ValueError("start_manager requires profile only")
            if _PROFILE_PATTERN.fullmatch(self.profile) is None:
                raise ValueError("invalid profile")
        elif self.operation in {"manager_status", "cancel_manager"}:
            if self.profile is not None or self.handle is None:
                raise ValueError("manager operation requires handle only")
            if _ID_PATTERN.fullmatch(self.handle) is None:
                raise ValueError("invalid handle")


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while rejecting repeated decoded keys."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    """Reject JSON extensions such as NaN and Infinity."""

    raise ValueError("nonfinite JSON value")


def _request_fields(request: Request) -> dict[str, object]:
    """Return the exact wire fields for a validated request."""

    fields: dict[str, object] = {
        "format": _FORMAT,
        "format_version": _FORMAT_VERSION,
        "request_id": request.request_id,
        "workspace_id": request.workspace_id,
        "operation": request.operation,
    }
    if request.operation == "start_manager":
        fields["profile"] = request.profile
    elif request.operation in {"manager_status", "cancel_manager"}:
        fields["handle"] = request.handle
    return fields


def decode_request(data: bytes) -> Request:
    """Decode and validate one bounded UTF-8 request document.

    :param data: Supply the raw request bytes.
    :return: The immutable validated request.
    :raises ValueError: If the document is malformed or outside the protocol.
    """

    if type(data) is not bytes:
        raise ValueError("request must be bytes")
    if len(data) > _MAX_REQUEST_SIZE:
        raise ValueError("request is too large")
    if any(data.startswith(bom) for bom in _BOMS):
        raise ValueError("request must be UTF-8 without a BOM")
    try:
        text = data.decode("utf-8", errors="strict")
        value = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid request document") from exc
    if not isinstance(value, dict):
        raise ValueError("invalid request document")
    if value.get("format") != _FORMAT:
        raise ValueError("invalid request format")
    version = value.get("format_version")
    if type(version) is not int or version != _FORMAT_VERSION:
        raise ValueError("unsupported request version")

    operation = value.get("operation")
    if type(operation) is not str or operation not in _OPERATIONS:
        raise ValueError("invalid operation")
    keys = {"format", "format_version", "request_id", "workspace_id", "operation"}
    if operation == "start_manager":
        keys.add("profile")
    elif operation in {"manager_status", "cancel_manager"}:
        keys.add("handle")
    if set(value) != keys:
        raise ValueError("invalid request fields")
    request_id = value["request_id"]
    workspace_id = value["workspace_id"]
    profile = value.get("profile")
    handle = value.get("handle")
    if type(request_id) is not str or type(workspace_id) is not str:
        raise ValueError("invalid request fields")
    if "profile" in value and type(profile) is not str:
        raise ValueError("invalid request fields")
    if "handle" in value and type(handle) is not str:
        raise ValueError("invalid request fields")
    try:
        return Request(request_id, workspace_id, operation, profile, handle)
    except ValueError as exc:
        raise ValueError("invalid request fields") from exc


def encode_request(request: Request) -> bytes:
    """Encode a request in its canonical compact JSON representation.

    :param request: Provide a validated request.
    :return: Canonical ASCII JSON bytes without a trailing newline.
    :raises ValueError: If the value is not a ``Request``.
    """

    if type(request) is not Request:
        raise ValueError("request must be a Request")
    try:
        encoded = json.dumps(_request_fields(request), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        data = encoded.encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("request cannot be encoded") from exc
    if len(data) > _MAX_REQUEST_SIZE:
        raise ValueError("request is too large")
    return data


def request_digest(request: Request) -> str:
    """Return the SHA-256 digest of a request's canonical encoding.

    :param request: Provide a validated request.
    :return: The lowercase hexadecimal SHA-256 digest.
    :raises ValueError: If the value is not a ``Request``.
    """

    return hashlib.sha256(encode_request(request)).hexdigest()


__all__ = ["Request", "decode_request", "encode_request", "request_digest"]
