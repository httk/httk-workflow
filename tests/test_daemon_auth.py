"""Mandatory request and response authentication tests."""

import base64
from dataclasses import replace
from pathlib import Path

import pytest
from httk.core.identity import identity_public_key

from httk.workflow._daemon_auth import (
    CLOCK_SKEW_SECONDS,
    check_request_time,
    sign_request,
    sign_response,
    verify_request,
    verify_response,
)
from httk.workflow._daemon_protocol import Request, Response, request_digest

REQUEST_ID = "0123456789abcdef0123456789abcdef"
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"
CONFIGURATION_DIGEST = "b" * 64


def _seed(tmp_path: Path, byte: int = 1) -> Path:
    path = tmp_path / f"{byte}.seed"
    path.write_text(base64.b64encode(bytes([byte]) * 32).decode("ascii") + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _request() -> Request:
    return Request(REQUEST_ID, WORKSPACE_ID, "health", enrollment_id=ENROLLMENT_ID)


def _response(request: Request) -> Response:
    return Response(REQUEST_ID, WORKSPACE_ID, ENROLLMENT_ID, request_digest(request), "ready")


def test_sign_request_requires_an_existing_identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / "config"))

    with pytest.raises(ValueError, match="operator_key"):
        sign_request(_request(), now=100)
    with pytest.raises(ValueError):
        sign_request(_request(), seed_path=tmp_path / "missing.seed", now=100)


def test_request_signature_covers_every_field_and_requires_allowlist(tmp_path: Path) -> None:
    seed = _seed(tmp_path)
    signed = sign_request(_request(), seed_path=seed, now=100, lifetime=60)
    public_key = identity_public_key(seed)
    assert public_key is not None

    assert (signed.created_at, signed.expires_at, signed.operator_key) == (100, 160, public_key)
    verify_request(signed, [public_key])
    with pytest.raises(ValueError, match="not authorized"):
        verify_request(signed, [])
    for tampered in (
        replace(signed, request_id="f" * 32),
        replace(signed, workspace_id="00000000-0000-0000-0000-000000000000"),
        replace(signed, enrollment_id="e" * 32),
        replace(signed, expires_at=161),
    ):
        with pytest.raises(ValueError, match="signature"):
            verify_request(tampered, [public_key])


def test_request_signature_covers_configuration_digest(tmp_path: Path) -> None:
    seed = _seed(tmp_path)
    request = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )
    signed = sign_request(request, seed_path=seed, now=100)
    public_key = identity_public_key(seed)
    assert public_key is not None

    verify_request(signed, [public_key])
    with pytest.raises(ValueError, match="signature"):
        verify_request(replace(signed, configuration_digest="c" * 64), [public_key])


def test_request_signature_refuses_forged_unlisted_and_noncanonical_encodings(tmp_path: Path) -> None:
    seed = _seed(tmp_path)
    other_seed = _seed(tmp_path, 2)
    signed = sign_request(_request(), seed_path=seed, now=100)
    public_key = identity_public_key(seed)
    other_key = identity_public_key(other_seed)
    assert public_key is not None and other_key is not None

    with pytest.raises(ValueError, match="not authorized"):
        verify_request(signed, [other_key])
    with pytest.raises(ValueError, match="signature"):
        verify_request(replace(signed, signature="A" * 88), [public_key])
    with pytest.raises(ValueError, match="canonical"):
        verify_request(replace(signed, operator_key=public_key.removeprefix("ed25519:")), [public_key])
    with pytest.raises(ValueError, match="canonical"):
        verify_request(replace(signed, operator_key=public_key + "="), [public_key])


def test_request_time_accepts_inclusive_skew_boundaries(tmp_path: Path) -> None:
    request = sign_request(_request(), seed_path=_seed(tmp_path), now=10_000, lifetime=3600)

    check_request_time(request, now=10_000 - CLOCK_SKEW_SECONDS)
    check_request_time(request, now=13_600 + CLOCK_SKEW_SECONDS)
    with pytest.raises(ValueError, match="time window"):
        check_request_time(request, now=10_000 - CLOCK_SKEW_SECONDS - 1)
    with pytest.raises(ValueError, match="time window"):
        check_request_time(request, now=13_600 + CLOCK_SKEW_SECONDS + 1)


@pytest.mark.parametrize(
    "daemon_request",
    [
        replace(_request(), created_at=0, expires_at=1),
        replace(_request(), created_at=1, expires_at=0),
        replace(_request(), created_at=10, expires_at=10),
        replace(_request(), created_at=10, expires_at=3611),
    ],
)
def test_request_time_refuses_invalid_intervals(daemon_request: Request) -> None:
    with pytest.raises(ValueError):
        check_request_time(daemon_request, now=10, max_age=3600)


@pytest.mark.parametrize(("field", "value"), [("created_at", True), ("expires_at", False)])
def test_request_timestamp_fields_refuse_booleans(field: str, value: bool) -> None:
    with pytest.raises(ValueError):
        if field == "created_at":
            replace(_request(), created_at=value)
        else:
            replace(_request(), expires_at=value)


def test_time_arguments_refuse_booleans_and_nonpositive_values(tmp_path: Path) -> None:
    seed = _seed(tmp_path)
    with pytest.raises(ValueError):
        sign_request(_request(), seed_path=seed, now=True)
    with pytest.raises(ValueError):
        sign_request(_request(), seed_path=seed, now=1, lifetime=False)
    valid = sign_request(_request(), seed_path=seed, now=1)
    with pytest.raises(ValueError):
        check_request_time(valid, now=True)
    with pytest.raises(ValueError):
        check_request_time(valid, now=1, max_age=False)


def test_response_signature_requires_seed_pin_and_untampered_document(tmp_path: Path) -> None:
    request = sign_request(_request(), seed_path=_seed(tmp_path), now=100)
    response_seed = _seed(tmp_path, 3)
    other_seed = _seed(tmp_path, 4)
    response = sign_response(_response(request), seed_path=response_seed)
    public_key = identity_public_key(response_seed)
    other_key = identity_public_key(other_seed)
    assert public_key is not None and other_key is not None

    verify_response(response, public_key)
    with pytest.raises(ValueError, match="pinned"):
        verify_response(response, other_key)
    with pytest.raises(ValueError, match="signature"):
        verify_response(replace(response, reason="tampered", outcome="refused"), public_key)
    with pytest.raises(ValueError):
        sign_response(_response(request), seed_path=tmp_path / "missing.seed")
