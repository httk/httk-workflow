"""Mandatory identity authentication for workspace daemon documents."""

import base64
import json
import time
from collections.abc import Collection
from dataclasses import replace
from pathlib import Path

from httk.core.identity import sign_document, verify_document

from ._daemon_protocol import Request, Response, encode_request, encode_response

CLOCK_SKEW_SECONDS = 7800
#: The longest request lifetime :func:`check_request_time` accepts unless told otherwise, in seconds.
DEFAULT_REQUEST_MAX_AGE = 3600


def _document(data: bytes) -> dict[str, object]:
    """Return the already validated canonical document as a mapping."""

    value = json.loads(data)
    if not isinstance(value, dict):  # pragma: no cover - encoders always emit objects
        raise ValueError("daemon document must be an object")
    return value


def _canonical_key(value: object, name: str) -> str:
    """Return one canonical encoded Ed25519 public key."""

    if not isinstance(value, str) or not value.startswith("ed25519:"):
        raise ValueError(f"{name} must be a canonical Ed25519 public key")
    encoded = value.removeprefix("ed25519:")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError(f"{name} must be a canonical Ed25519 public key") from exc
    if len(raw) != 32 or base64.b64encode(raw).decode("ascii") != encoded:
        raise ValueError(f"{name} must be a canonical Ed25519 public key")
    return value


def _canonical_signature(value: object) -> str:
    """Return one canonical encoded Ed25519 signature."""

    if not isinstance(value, str):
        raise ValueError("signature must be a canonical Ed25519 signature")
    try:
        raw = base64.b64decode(value, validate=True)
    except ValueError as exc:
        raise ValueError("signature must be a canonical Ed25519 signature") from exc
    if len(raw) != 64 or base64.b64encode(raw).decode("ascii") != value:
        raise ValueError("signature must be a canonical Ed25519 signature")
    return value


def _signed_values(document: dict[str, object], kind: str) -> tuple[str, str]:
    """Require a valid, canonically encoded signature block."""

    operator_key = _canonical_key(document.get("operator_key"), f"{kind} operator_key")
    signature = _canonical_signature(document.get("signature"))
    result = verify_document(document)
    if not result.present or not result.valid or result.operator_key != operator_key:
        raise ValueError(f"invalid {kind} signature")
    return operator_key, signature


def _positive_integer(value: object, name: str) -> int:
    """Return one positive integer while refusing booleans."""

    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def sign_request(
    request: Request,
    *,
    seed_path: Path | None = None,
    now: int | None = None,
    lifetime: int = 3600,
) -> Request:
    """Timestamp and sign one request with the local operator identity.

    :param request: Unsigned typed request to authenticate.
    :param seed_path: Explicit identity seed, or the configured default identity.
    :param now: Positive Unix time, or the current system time.
    :param lifetime: Positive request lifetime in seconds.
    :return: A signed request covering every request field.
    :raises ValueError: If timestamps or signing identity are unavailable or invalid.
    """

    if type(request) is not Request:
        raise ValueError("request must be a Request")
    issued = int(time.time()) if now is None else _positive_integer(now, "now")
    _positive_integer(issued, "now")
    duration = _positive_integer(lifetime, "lifetime")
    unsigned = replace(
        request,
        created_at=issued,
        expires_at=issued + duration,
        operator_key=None,
        signature=None,
    )
    signed = sign_document(_document(encode_request(unsigned)), seed_path=seed_path)
    operator_key, signature = _signed_values(signed, "request")
    return replace(unsigned, operator_key=operator_key, signature=signature)


def verify_request(request: Request, authorized_keys: Collection[str]) -> None:
    """Require a valid request signature made by an authorized operator key.

    :param request: Typed request to authenticate.
    :param authorized_keys: Exact canonical public keys permitted by policy.
    :raises ValueError: If the signature is missing, malformed, forged, or unauthorized.
    """

    if type(request) is not Request:
        raise ValueError("request must be a Request")
    if isinstance(authorized_keys, (str, bytes)) or not isinstance(authorized_keys, Collection):
        raise ValueError("authorized_keys must be a collection of public keys")
    operator_key, _ = _signed_values(_document(encode_request(request)), "request")
    if operator_key not in authorized_keys:
        raise ValueError("request operator key is not authorized")


def check_request_time(request: Request, *, now: int | None = None, max_age: int = DEFAULT_REQUEST_MAX_AGE) -> None:
    """Require a bounded request lifetime containing now with allowed clock skew.

    :param request: Typed request carrying positive integer Unix timestamps.
    :param now: Positive Unix time, or the current system time.
    :param max_age: Largest permitted signed lifetime in seconds.
    :raises ValueError: If timestamps, lifetime, or the skew-adjusted window are invalid.
    """

    if type(request) is not Request:
        raise ValueError("request must be a Request")
    checked_at = int(time.time()) if now is None else _positive_integer(now, "now")
    _positive_integer(checked_at, "now")
    maximum = _positive_integer(max_age, "max_age")
    created_at = _positive_integer(request.created_at, "created_at")
    expires_at = _positive_integer(request.expires_at, "expires_at")
    lifetime = expires_at - created_at
    if lifetime <= 0 or lifetime > maximum:
        raise ValueError("request lifetime is invalid")
    if checked_at < created_at - CLOCK_SKEW_SECONDS or checked_at > expires_at + CLOCK_SKEW_SECONDS:
        raise ValueError("request is outside its accepted time window")


def sign_response(response: Response, *, seed_path: Path) -> Response:
    """Sign one response with the broker's explicit response seed.

    :param response: Unsigned typed response to authenticate.
    :param seed_path: Broker response-signing seed path.
    :return: A signed response covering every response field.
    :raises ValueError: If the response or signing seed is unavailable or invalid.
    """

    if type(response) is not Response:
        raise ValueError("response must be a Response")
    unsigned = replace(response, operator_key=None, signature=None)
    signed = sign_document(_document(encode_response(unsigned)), seed_path=seed_path)
    operator_key, signature = _signed_values(signed, "response")
    return replace(unsigned, operator_key=operator_key, signature=signature)


def verify_response(response: Response, public_key: str) -> None:
    """Require a valid response signature made by the pinned broker key.

    :param response: Typed response to authenticate.
    :param public_key: Exact canonical broker public-key pin.
    :raises ValueError: If the signature is missing, malformed, forged, or uses another key.
    """

    if type(response) is not Response:
        raise ValueError("response must be a Response")
    expected = _canonical_key(public_key, "public_key")
    operator_key, _ = _signed_values(_document(encode_response(response)), "response")
    if operator_key != expected:
        raise ValueError("response operator key does not match pinned public key")


__all__ = [
    "CLOCK_SKEW_SECONDS",
    "DEFAULT_REQUEST_MAX_AGE",
    "check_request_time",
    "sign_request",
    "sign_response",
    "verify_request",
    "verify_response",
]
