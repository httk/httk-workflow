"""Strict request documents for the confined workspace daemon."""

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import cast

from .models import validate_step

_MAX_REQUEST_SIZE = 16 * 1024
_REQUEST_FORMAT = "httk-workspace-command"
_RESPONSE_FORMAT = "httk-workspace-response"
_FORMAT_VERSION = 5
#: Authenticated job control and fresh status observations handled by the confined daemon.
JOB_OPERATIONS = frozenset({"job_request", "job_request_status"})
_OPERATIONS = frozenset({"health", "start_manager", "manager_status", "cancel_manager"}) | JOB_OPERATIONS
_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_PROFILE_PATTERN = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_SCHEDULER_STATE_PATTERN = re.compile(r"[A-Z_]{1,64}\Z")
_REASON_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_DETAIL_PATTERN = re.compile(r"[ -~]{1,1000}\Z")
_UUID_TEXT = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
_JOB_STATES = frozenset({"ready", "waiting", "paused", "failed", "succeeded", "cancelled", "running", "owned"})
_JOB_ACTIONS = frozenset(
    {
        "cancel",
        "pause",
        "continue",
        "restart",
        "override_step",
        "set_priority",
        "detach",
        "eject",
        "delete",
        "seal",
        "unseal",
    }
)
_JOB_RESULTS = frozenset({"applied", "queued", "refused", "uncertain"})
_OUTCOMES = frozenset(
    {
        "ready",
        "submitted",
        "status",
        "cancel_requested",
        "refused",
        "uncertain",
        "busy",
        "accepted",
        "applied",
        "queued",
    }
)
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")


@dataclass(frozen=True, slots=True)
class Request:
    """An immutable, validated daemon request.

    :param request_id: Identify this request with 32 lowercase hexadecimal digits.
    :param workspace_id: Identify the workspace with its canonical UUID.
    :param operation: Select one supported daemon operation.
    :param profile: Select a configured profile for ``start_manager``.
    :param handle: Identify a broker-issued manager handle.
    :param configuration_digest: Pin the selected approved configuration.
    :param selector: Name a job by a canonical UUID or hexadecimal UUID prefix.
    :param action: Select one internal job-request action.
    :param reason: Explain why the action was requested.
    :param priority: Set priority for ``set_priority``.
    :param step: Select the runner step for ``override_step``.
    :param destination: Select an outbox-relative destination for ``eject``.
    :param command_id: Identify the original daemon command for ``job_request_status``.
    :raises ValueError: If a field is invalid or conflicts with the operation.
    """

    request_id: str
    workspace_id: str
    operation: str
    profile: str | None = None
    handle: str | None = None
    enrollment_id: str = field(kw_only=True)
    created_at: int = field(default=0, kw_only=True)
    expires_at: int = field(default=0, kw_only=True)
    operator_key: str | None = field(default=None, kw_only=True)
    signature: str | None = field(default=None, kw_only=True)
    configuration_digest: str | None = field(default=None, kw_only=True)
    selector: str | None = field(default=None, kw_only=True)
    action: str | None = field(default=None, kw_only=True)
    reason: str | None = field(default=None, kw_only=True)
    priority: int | None = field(default=None, kw_only=True)
    step: str | None = field(default=None, kw_only=True)
    destination: str | None = field(default=None, kw_only=True)
    command_id: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        """Refuse invalid fields and operation-specific combinations."""

        if type(self.request_id) is not str or _ID_PATTERN.fullmatch(self.request_id) is None:
            raise ValueError("invalid request_id")
        if type(self.enrollment_id) is not str or _ID_PATTERN.fullmatch(self.enrollment_id) is None:
            raise ValueError("invalid enrollment_id")
        if type(self.workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        if type(self.operation) is not str or self.operation not in _OPERATIONS:
            raise ValueError("invalid operation")
        if type(self.created_at) is not int or self.created_at < 0:
            raise ValueError("invalid created_at")
        if type(self.expires_at) is not int or self.expires_at < 0:
            raise ValueError("invalid expires_at")
        if self.operator_key is not None and type(self.operator_key) is not str:
            raise ValueError("invalid operator_key")
        if self.signature is not None and type(self.signature) is not str:
            raise ValueError("invalid signature")
        if self.configuration_digest is not None and (
            type(self.configuration_digest) is not str or _DIGEST_PATTERN.fullmatch(self.configuration_digest) is None
        ):
            raise ValueError("invalid configuration_digest")
        if self.profile is not None and type(self.profile) is not str:
            raise ValueError("invalid profile")
        if self.handle is not None and type(self.handle) is not str:
            raise ValueError("invalid handle")
        if self.operation not in JOB_OPERATIONS and any(
            item is not None
            for item in (
                self.selector,
                self.action,
                self.reason,
                self.priority,
                self.step,
                self.destination,
                self.command_id,
            )
        ):
            raise ValueError("job request fields require a job operation")
        if self.operation in JOB_OPERATIONS:
            if self.profile is not None or self.handle is not None:
                raise ValueError("job operations forbid manager fields")
            if self.operation == "job_request":
                if self.configuration_digest is None:
                    raise ValueError("job_request requires configuration_digest")
                if self.command_id is not None:
                    raise ValueError("job_request forbids command_id")
                if not _valid_selector(self.selector):
                    raise ValueError("invalid selector")
                if type(self.action) is not str or self.action not in _JOB_ACTIONS:
                    raise ValueError("invalid action")
                if type(self.reason) is not str or len(self.reason.encode("utf-8")) > 1000:
                    raise ValueError("invalid reason")
                if self.priority is not None and (type(self.priority) is not int or not 0 <= self.priority <= 999):
                    raise ValueError("invalid priority")
                if (self.action == "set_priority") != (self.priority is not None):
                    raise ValueError("priority is required only for set_priority")
                if self.step is not None:
                    if type(self.step) is not str:
                        raise ValueError("invalid step")
                    try:
                        validate_step(self.step)
                    except ValueError as exc:
                        raise ValueError("invalid step") from exc
                if (self.action == "override_step") != (self.step is not None):
                    raise ValueError("step is required only for override_step")
                if self.destination is not None:
                    if (
                        type(self.destination) is not str
                        or "\\" in self.destination
                        or len(self.destination.encode("utf-8")) > 1024
                    ):
                        raise ValueError("invalid destination")
                    parts = self.destination.split("/")
                    if any(part in {"", ".", ".."} or len(part.encode("utf-8")) > 255 or ":" in part for part in parts):
                        raise ValueError("invalid destination")
                if (self.action == "eject") != (self.destination is not None):
                    raise ValueError("destination is required only for eject")
            else:
                if self.configuration_digest is not None or any(
                    item is not None
                    for item in (self.selector, self.action, self.reason, self.priority, self.step, self.destination)
                ):
                    raise ValueError("job_request_status requires command_id only")
                if type(self.command_id) is not str or _ID_PATTERN.fullmatch(self.command_id) is None:
                    raise ValueError("invalid command_id")
        elif self.operation == "health":
            if self.profile is not None or self.handle is not None or self.configuration_digest is not None:
                raise ValueError("health forbids operation fields")
        elif self.operation == "start_manager":
            if self.handle is not None or self.profile is None:
                raise ValueError("start_manager requires profile only")
            if _PROFILE_PATTERN.fullmatch(self.profile) is None:
                raise ValueError("invalid profile")
        elif self.operation in {"manager_status", "cancel_manager"}:
            if self.profile is not None or self.handle is None or self.configuration_digest is not None:
                raise ValueError("manager operation requires handle only")
            if _ID_PATTERN.fullmatch(self.handle) is None:
                raise ValueError("invalid handle")


def _version_message(kind: str, version: object) -> str:
    """Return the refusal for a *kind* document of an unsupported protocol *version*."""

    if type(version) is int and version < _FORMAT_VERSION:
        return (
            f"unsupported {kind} version {version}: this daemon speaks protocol {_FORMAT_VERSION}; "
            "update the client and re-enroll the daemon; old protocol requests are not migrated"
        )
    return f"unsupported {kind} version {version!r}: this daemon speaks protocol {_FORMAT_VERSION}; update the client"


def _valid_selector(value: object) -> bool:
    """Report whether a selector is a lowercase textual UUID prefix."""

    if type(value) is not str or not value or len(value) > len(_UUID_TEXT):
        return False
    return all(
        character == "-" if _UUID_TEXT[index] == "-" else character in "0123456789abcdef"
        for index, character in enumerate(value)
    )


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


def _decode_object(data: bytes, kind: str) -> dict[str, object]:
    """Parse one bounded UTF-8 JSON object with strict duplicate handling."""

    if type(data) is not bytes:
        raise ValueError(f"{kind} must be bytes")
    if len(data) > _MAX_REQUEST_SIZE:
        raise ValueError(f"{kind} is too large")
    if any(data.startswith(bom) for bom in _BOMS):
        raise ValueError(f"{kind} must be UTF-8 without a BOM")
    try:
        text = data.decode("utf-8", errors="strict")
        value = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"invalid {kind} document") from exc
    if not isinstance(value, dict):
        raise ValueError(f"invalid {kind} document")
    return value


def _encode_fields(fields: dict[str, object], kind: str) -> bytes:
    """Encode bounded canonical ASCII JSON fields."""

    try:
        encoded = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        data = encoded.encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError(f"{kind} cannot be encoded") from exc
    if len(data) > _MAX_REQUEST_SIZE:
        raise ValueError(f"{kind} is too large")
    return data


def _request_fields(request: Request) -> dict[str, object]:
    """Return the exact wire fields for a validated request."""

    fields: dict[str, object] = {
        "format": _REQUEST_FORMAT,
        "format_version": _FORMAT_VERSION,
        "request_id": request.request_id,
        "workspace_id": request.workspace_id,
        "enrollment_id": request.enrollment_id,
        "operation": request.operation,
        "created_at": request.created_at,
        "expires_at": request.expires_at,
        "operator_key": request.operator_key,
        "signature": request.signature,
    }
    if request.operation == "start_manager":
        if request.configuration_digest is None:
            raise ValueError("start_manager requires configuration_digest on the wire")
        fields["configuration"] = request.profile
        fields["configuration_digest"] = request.configuration_digest
    elif request.operation in {"manager_status", "cancel_manager"}:
        fields["handle"] = request.handle
    elif request.operation == "job_request":
        fields.update(
            {
                "configuration_digest": request.configuration_digest,
                "selector": request.selector,
                "action": request.action,
                "reason": request.reason,
            }
        )
        for name in ("priority", "step", "destination"):
            value = getattr(request, name)
            if value is not None:
                fields[name] = value
    elif request.operation == "job_request_status":
        fields["command_id"] = request.command_id
    return fields


def decode_request(data: bytes) -> Request:
    """Decode and validate one bounded UTF-8 request document.

    :param data: Supply the raw request bytes.
    :return: The immutable validated request.
    :raises ValueError: If the document is malformed or outside the protocol.
    """

    value = _decode_object(data, "request")
    if value.get("format") != _REQUEST_FORMAT:
        raise ValueError("invalid request format")
    version = value.get("format_version")
    if type(version) is not int or version != _FORMAT_VERSION:
        raise ValueError(_version_message("request", version))

    operation = value.get("operation")
    if type(operation) is not str or operation not in _OPERATIONS:
        raise ValueError("invalid operation")
    keys = {
        "format",
        "format_version",
        "request_id",
        "workspace_id",
        "enrollment_id",
        "operation",
        "created_at",
        "expires_at",
        "operator_key",
        "signature",
    }
    if operation == "start_manager":
        keys.update({"configuration", "configuration_digest"})
    elif operation in {"manager_status", "cancel_manager"}:
        keys.add("handle")
    elif operation == "job_request":
        keys.update({"configuration_digest", "selector", "action", "reason"})
        keys.update(name for name in ("priority", "step", "destination") if name in value)
    elif operation == "job_request_status":
        keys.add("command_id")
    if set(value) != keys:
        raise ValueError("invalid request fields")
    request_id = value["request_id"]
    workspace_id = value["workspace_id"]
    enrollment_id = value["enrollment_id"]
    profile = value.get("configuration")
    configuration_digest = value.get("configuration_digest")
    handle = value.get("handle")
    created_at = value["created_at"]
    expires_at = value["expires_at"]
    operator_key = value["operator_key"]
    signature = value["signature"]
    if type(request_id) is not str or type(workspace_id) is not str or type(enrollment_id) is not str:
        raise ValueError("invalid request fields")
    if "configuration" in value and type(profile) is not str:
        raise ValueError("invalid request fields")
    if "configuration_digest" in value and type(configuration_digest) is not str:
        raise ValueError("invalid request fields")
    if "handle" in value and type(handle) is not str:
        raise ValueError("invalid request fields")
    if type(created_at) is not int or type(expires_at) is not int:
        raise ValueError("invalid request fields")
    if operator_key is not None and type(operator_key) is not str:
        raise ValueError("invalid request fields")
    if signature is not None and type(signature) is not str:
        raise ValueError("invalid request fields")
    try:
        return Request(
            request_id,
            workspace_id,
            operation,
            cast(str | None, profile),
            cast(str | None, handle),
            enrollment_id=enrollment_id,
            created_at=created_at,
            expires_at=expires_at,
            operator_key=cast(str | None, operator_key),
            signature=cast(str | None, signature),
            configuration_digest=cast(str | None, configuration_digest),
            selector=cast(str | None, value.get("selector")),
            action=cast(str | None, value.get("action")),
            reason=cast(str | None, value.get("reason")),
            priority=cast(int | None, value.get("priority")),
            step=cast(str | None, value.get("step")),
            destination=cast(str | None, value.get("destination")),
            command_id=cast(str | None, value.get("command_id")),
        )
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
    return _encode_fields(_request_fields(request), "request")


@dataclass(frozen=True, slots=True)
class Response:
    """An immutable, validated daemon response.

    :param request_id: Identify the corresponding request with 32 lowercase hexadecimal digits.
    :param workspace_id: Identify the workspace with its canonical UUID.
    :param enrollment_id: Identify the protected daemon enrollment.
    :param request_digest: Give the canonical request's lowercase SHA-256 digest.
    :param outcome: Report one protocol-defined daemon outcome.
    :param handle: Identify a broker-issued manager handle when applicable.
    :param scheduler_state: Give a bounded normalized scheduler state.
    :param reason: Give a bounded normalized reason code.
    :param detail: Explain ``reason`` in at most 1000 printable ASCII characters.
    :param job_id: Identify the resolved job with its canonical UUID.
    :param internal_request_id: Identify the filesystem request with its canonical UUID.
    :param current_state: Report the resolved job's bounded current state when available.
    :param command_id: Identify the original daemon command with 32 lowercase hexadecimal digits.
    :raises ValueError: If a field is invalid or conflicts with the outcome.
    """

    request_id: str
    workspace_id: str
    enrollment_id: str
    request_digest: str
    outcome: str
    handle: str | None = None
    scheduler_state: str | None = None
    reason: str | None = None
    detail: str | None = None
    operator_key: str | None = field(default=None, kw_only=True)
    signature: str | None = field(default=None, kw_only=True)
    job_id: str | None = field(default=None, kw_only=True)
    internal_request_id: str | None = field(default=None, kw_only=True)
    current_state: str | None = field(default=None, kw_only=True)
    command_id: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        """Refuse invalid identifiers and outcome-specific combinations."""

        if type(self.request_id) is not str or _ID_PATTERN.fullmatch(self.request_id) is None:
            raise ValueError("invalid request_id")
        if type(self.workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        if type(self.enrollment_id) is not str or _ID_PATTERN.fullmatch(self.enrollment_id) is None:
            raise ValueError("invalid enrollment_id")
        if type(self.request_digest) is not str or _DIGEST_PATTERN.fullmatch(self.request_digest) is None:
            raise ValueError("invalid request_digest")
        if type(self.outcome) is not str or self.outcome not in _OUTCOMES:
            raise ValueError("invalid outcome")
        if self.handle is not None and (type(self.handle) is not str or _ID_PATTERN.fullmatch(self.handle) is None):
            raise ValueError("invalid handle")
        if self.scheduler_state is not None and (
            type(self.scheduler_state) is not str or _SCHEDULER_STATE_PATTERN.fullmatch(self.scheduler_state) is None
        ):
            raise ValueError("invalid scheduler_state")
        if self.reason is not None and (type(self.reason) is not str or _REASON_PATTERN.fullmatch(self.reason) is None):
            raise ValueError("invalid reason")
        if self.detail is not None and (type(self.detail) is not str or _DETAIL_PATTERN.fullmatch(self.detail) is None):
            raise ValueError("invalid detail")
        if self.detail is not None and self.reason is None:
            raise ValueError("detail requires reason")
        if self.operator_key is not None and type(self.operator_key) is not str:
            raise ValueError("invalid operator_key")
        if self.signature is not None and type(self.signature) is not str:
            raise ValueError("invalid signature")
        for name in ("job_id", "internal_request_id"):
            value = getattr(self, name)
            if value is not None:
                try:
                    if type(value) is not str or str(uuid.UUID(value)) != value:
                        raise ValueError
                except (ValueError, AttributeError) as exc:
                    raise ValueError(f"invalid {name}") from exc
        if self.command_id is not None and (
            type(self.command_id) is not str or _ID_PATTERN.fullmatch(self.command_id) is None
        ):
            raise ValueError("invalid command_id")
        if self.current_state is not None and (
            type(self.current_state) is not str or self.current_state not in _JOB_STATES
        ):
            raise ValueError("invalid current_state")

        has_handle = self.handle is not None
        has_state = self.scheduler_state is not None
        has_reason = self.reason is not None
        valid = {
            "ready": not has_handle and not has_state and not has_reason,
            "submitted": has_handle and not has_state and not has_reason,
            "status": has_handle and has_state and not has_reason,
            "cancel_requested": has_handle and not has_state and not has_reason,
            "uncertain": has_handle and not has_state and has_reason,
            "refused": not has_state and has_reason,
            "busy": not has_handle and not has_state and has_reason,
            "accepted": not has_handle and not has_state and not has_reason,
            "applied": not has_handle and not has_state and not has_reason,
            "queued": not has_handle and not has_state and not has_reason,
        }[self.outcome]
        if self.command_id is not None:
            valid = self.outcome in _JOB_RESULTS and not has_handle and not has_state
            if (self.job_id is None) != (self.internal_request_id is None):
                valid = False
            if self.outcome in {"applied", "queued"} and self.job_id is None:
                valid = False
            if self.outcome == "refused" and not has_reason:
                valid = False
        elif any(item is not None for item in (self.job_id, self.internal_request_id, self.current_state)):
            valid = False
        if not valid:
            raise ValueError("invalid outcome fields")


def _response_fields(response: Response) -> dict[str, object]:
    """Return the exact wire fields for a validated response."""

    fields: dict[str, object] = {
        "format": _RESPONSE_FORMAT,
        "format_version": _FORMAT_VERSION,
        "request_id": response.request_id,
        "workspace_id": response.workspace_id,
        "enrollment_id": response.enrollment_id,
        "request_digest": response.request_digest,
        "outcome": response.outcome,
        "operator_key": response.operator_key,
        "signature": response.signature,
    }
    if response.handle is not None:
        fields["handle"] = response.handle
    if response.scheduler_state is not None:
        fields["scheduler_state"] = response.scheduler_state
    if response.reason is not None:
        fields["reason"] = response.reason
    if response.detail is not None:
        fields["detail"] = response.detail
    for name in ("job_id", "internal_request_id", "current_state", "command_id"):
        value = getattr(response, name)
        if value is not None:
            fields[name] = value
    return fields


def decode_response(data: bytes) -> Response:
    """Decode and validate one bounded UTF-8 response document.

    :param data: Supply the raw response bytes.
    :return: The immutable validated response.
    :raises ValueError: If the document is malformed or outside the protocol.
    """

    value = _decode_object(data, "response")
    if value.get("format") != _RESPONSE_FORMAT:
        raise ValueError("invalid response format")
    version = value.get("format_version")
    if type(version) is not int or version != _FORMAT_VERSION:
        raise ValueError(_version_message("response", version))
    keys = {
        "format",
        "format_version",
        "request_id",
        "workspace_id",
        "enrollment_id",
        "request_digest",
        "outcome",
        "operator_key",
        "signature",
    }
    optional = {
        "handle",
        "scheduler_state",
        "reason",
        "detail",
        "job_id",
        "internal_request_id",
        "current_state",
        "command_id",
    }
    if not keys <= set(value) or not set(value) <= keys | optional:
        raise ValueError("invalid response fields")
    if any(name in value and value[name] is None for name in optional):
        raise ValueError("invalid response fields")
    try:
        return Response(
            request_id=value["request_id"],  # type: ignore[arg-type]
            workspace_id=value["workspace_id"],  # type: ignore[arg-type]
            enrollment_id=value["enrollment_id"],  # type: ignore[arg-type]
            request_digest=value["request_digest"],  # type: ignore[arg-type]
            outcome=value["outcome"],  # type: ignore[arg-type]
            handle=value.get("handle"),  # type: ignore[arg-type]
            scheduler_state=value.get("scheduler_state"),  # type: ignore[arg-type]
            reason=value.get("reason"),  # type: ignore[arg-type]
            detail=value.get("detail"),  # type: ignore[arg-type]
            operator_key=value["operator_key"],  # type: ignore[arg-type]
            signature=value["signature"],  # type: ignore[arg-type]
            job_id=value.get("job_id"),  # type: ignore[arg-type]
            internal_request_id=value.get("internal_request_id"),  # type: ignore[arg-type]
            current_state=value.get("current_state"),  # type: ignore[arg-type]
            command_id=value.get("command_id"),  # type: ignore[arg-type]
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid response fields") from exc


def encode_response(response: Response) -> bytes:
    """Encode a response in its canonical compact JSON representation.

    :param response: Provide a validated response.
    :return: Canonical ASCII JSON bytes without a trailing newline.
    :raises ValueError: If the value is not a ``Response``.
    """

    if type(response) is not Response:
        raise ValueError("response must be a Response")
    return _encode_fields(_response_fields(response), "response")


def request_digest(request: Request) -> str:
    """Return the SHA-256 digest of a request's canonical encoding.

    :param request: Provide a validated request.
    :return: The lowercase hexadecimal SHA-256 digest.
    :raises ValueError: If the value is not a ``Request``.
    """

    return hashlib.sha256(encode_request(request)).hexdigest()


__all__ = [
    "JOB_OPERATIONS",
    "Request",
    "Response",
    "decode_request",
    "decode_response",
    "encode_request",
    "encode_response",
    "request_digest",
]
