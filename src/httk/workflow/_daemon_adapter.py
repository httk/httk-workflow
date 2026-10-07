"""Dedicated adapter dispatcher for mounted workspace daemon controls."""

import json
import math
import os
import secrets
import stat
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from ._daemon_client import Endpoint, exchange, prepare_request
from ._daemon_protocol import Request, decode_request, encode_response

_REQUEST_FORMAT = "httk-computer-request"
_RESULT_FORMAT = "httk-computer-result"
_METADATA_FILE = "remote.json"
_MAX_ADAPTER_DOCUMENT_BYTES = 64 * 1024
_COMMON_FIELDS = frozenset({"format", "format_version", "operation", "adapter_dir", "remote_settings"})
_POSITIVE_OUTCOMES = frozenset({"ready", "submitted", "status", "cancel_requested"})
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")


def _result(operation: str, **values: object) -> None:
    """Print one successful adapter result envelope."""

    print(
        json.dumps(
            {
                "format": _RESULT_FORMAT,
                "format_version": 2,
                "operation": operation,
                "ok": True,
                **values,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _refusal(operation: str, message: str) -> None:
    """Print one adapter-level refusal envelope."""

    print(
        json.dumps(
            {
                "error": message,
                "format": _RESULT_FORMAT,
                "format_version": 2,
                "operation": operation,
                "ok": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while refusing duplicate decoded keys."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    """Refuse nonfinite JSON extensions."""

    raise ValueError("nonfinite JSON value")


def _validate_unicode(value: object) -> None:
    """Refuse decoded strings that cannot be encoded as UTF-8."""

    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, list):
        for item in value:
            _validate_unicode(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _validate_unicode(key)
            _validate_unicode(item)


def _read_document(path: Path, kind: str) -> dict[str, object]:
    """Read one bounded regular UTF-8 JSON object without following a symlink."""

    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError(f"{kind} is not a regular file")
        if information.st_size > _MAX_ADAPTER_DOCUMENT_BYTES:
            raise ValueError(f"{kind} is too large")
        data = bytearray()
        while len(data) <= _MAX_ADAPTER_DOCUMENT_BYTES:
            chunk = os.read(descriptor, _MAX_ADAPTER_DOCUMENT_BYTES + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _MAX_ADAPTER_DOCUMENT_BYTES:
                raise ValueError(f"{kind} is too large")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    raw = bytes(data)
    if any(raw.startswith(bom) for bom in _BOMS):
        raise ValueError(f"{kind} must be UTF-8 without a BOM")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"invalid {kind}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{kind} must be a JSON object")
    return value


def _operation(request: Mapping[str, object]) -> str:
    """Return the nonempty adapter operation."""

    operation = request.get("operation")
    if type(operation) is not str or not operation:
        raise ValueError("adapter request must carry a nonempty string operation")
    return operation


def _require_envelope(request: Mapping[str, object], fields: frozenset[str]) -> None:
    """Validate the fixed adapter envelope and exact operation fields."""

    if set(request) != _COMMON_FIELDS | fields:
        raise ValueError("invalid mount-daemon adapter request fields")
    if request.get("format") != _REQUEST_FORMAT:
        raise ValueError("invalid adapter request format")
    version = request.get("format_version")
    if type(version) is not int or version != 2:
        raise ValueError("unsupported adapter request version")
    if type(request.get("adapter_dir")) is not str or not request["adapter_dir"]:
        raise ValueError("invalid adapter_dir")
    if not isinstance(request.get("remote_settings"), Mapping):
        raise ValueError("remote_settings must be a JSON object")


def _adapter_kind(request: Mapping[str, object]) -> str | None:
    """Read the adapter kind before dispatching any operation."""

    adapter_dir = request.get("adapter_dir")
    if type(adapter_dir) is not str or not adapter_dir:
        return None
    metadata = _read_document(Path(adapter_dir) / _METADATA_FILE, "adapter metadata")
    kind = metadata.get("kind")
    return kind if type(kind) is str else None


def _mapping(value: object, name: str) -> dict[str, object]:
    """Copy one string-keyed JSON mapping."""

    if not isinstance(value, Mapping) or not all(type(key) is str for key in value):
        raise ValueError(f"{name} must be a JSON object")
    return dict(value)


def _endpoint(request: Mapping[str, object], *, pending: bool) -> Endpoint:
    """Build an endpoint from configured and pending settings."""

    settings = _mapping(request["remote_settings"], "remote_settings")
    if pending:
        settings.update(_mapping(request["settings"], "settings"))
        settings = {name: value for name, value in settings.items() if value is not None}  # None unpins
    return Endpoint.from_settings(settings)


def _wait_seconds(value: object) -> float:
    """Return a finite daemon wait interval."""

    if type(value) not in {int, float}:
        raise ValueError("wait_seconds must be a finite number from 0.05 through 120")
    try:
        wait = float(cast(int | float, value))
    except OverflowError as exc:
        raise ValueError("wait_seconds must be a finite number from 0.05 through 120") from exc
    if not math.isfinite(wait) or not 0.05 <= wait <= 120.0:
        raise ValueError("wait_seconds must be a finite number from 0.05 through 120")
    return wait


def _request(value: object) -> Request:
    """Decode an exact daemon wire object with the strict request codec."""

    if not isinstance(value, Mapping):
        raise ValueError("daemon_request must be a JSON object")
    try:
        data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("daemon_request cannot be encoded") from exc
    return decode_request(data)


def _render_exchange(endpoint: Endpoint, daemon_request: Request, wait_seconds: float) -> None:
    """Exchange one request and preserve confirmed negative outcomes."""

    response = exchange(endpoint, daemon_request, wait_seconds=wait_seconds)
    _result(
        "daemon",
        returncode=0 if response.outcome in _POSITIVE_OUTCOMES else 2,
        stdout=encode_response(response).decode("ascii"),
        stderr="",
    )


def _configure(request: Mapping[str, object]) -> None:
    """Validate merged settings and local endpoint paths without publication."""

    _require_envelope(request, frozenset({"settings"}))
    _endpoint(request, pending=True).check()
    _result("configure", configured=True)


def _install(request: Mapping[str, object]) -> None:
    """Check the exchange and, when the daemon is pinned, its health without accepting pending configuration."""

    fields = frozenset({"settings"}) if "settings" in request else frozenset()
    _require_envelope(request, fields)
    if "settings" in request and _mapping(request["settings"], "settings"):
        raise ValueError("mount-daemon check refuses pending settings; configure them first")
    endpoint = _endpoint(request, pending=False)
    if endpoint.enrollment_id is None or endpoint.public_key is None:
        endpoint.check()  # exchange.json only: without daemon pins there is nothing to sign for
        _result("install", returncode=0, stdout="", stderr="")
        return
    health = prepare_request(
        endpoint,
        Request(
            secrets.token_hex(16),
            endpoint.workspace_id,
            "health",
            enrollment_id=endpoint.enrollment_id,
        ),
    )
    response = exchange(endpoint, health)
    _result(
        "install",
        returncode=0 if response.outcome == "ready" else 2,
        stdout=encode_response(response).decode("ascii"),
        stderr="",
    )


def _daemon(request: Mapping[str, object]) -> None:
    """Run the typed daemon exchange operation."""

    optional_wait = "wait_seconds" in request
    fields = frozenset({"daemon_request", "wait_seconds"} if optional_wait else {"daemon_request"})
    _require_envelope(request, fields)
    endpoint = _endpoint(request, pending=False)
    daemon_request = _request(request["daemon_request"])
    wait = _wait_seconds(request.get("wait_seconds", 10))
    _render_exchange(endpoint, daemon_request, wait)


def main(argv: list[str] | None = None) -> int:
    """Dispatch one dedicated mount-daemon adapter request file.

    :param argv: Dispatcher arguments, or process arguments when omitted.
    :return: Zero after an adapter result, or two for a malformed exchange.
    """

    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        print("mount-daemon adapter expects one REQUEST.json path", file=sys.stderr)
        return 2
    try:
        request = _read_document(Path(arguments[0]), "adapter request")
        operation = _operation(request)
        kind = _adapter_kind(request)
        if kind != "mount-daemon":
            _refusal(operation, "adapter kind is not 'mount-daemon'; refusing all operations")
            return 0
        if operation == "configure":
            _configure(request)
        elif operation == "install":
            _install(request)
        elif operation == "daemon":
            _daemon(request)
        elif operation in {"invoke", "status", "push", "pull"}:
            _refusal(
                operation,
                "mount-daemon supports typed daemon controls only; move jobs with "
                "'httk job eject JOB EXCHANGE/inbox' and 'httk job adopt EXCHANGE/outbox/<job_key>'",
            )
        else:
            _refusal(operation, "unsupported mount-daemon adapter operation")
    except (OSError, RuntimeError, TimeoutError, ValueError, KeyError) as exc:
        print(f"mount-daemon adapter: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
