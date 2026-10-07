"""Strict request codec and validation tests for the workspace daemon."""

import base64
import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest
from httk.core.identity import identity_public_key

from httk.workflow._daemon_auth import sign_response, verify_response
from httk.workflow._daemon_protocol import (
    Request,
    Response,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
    request_digest,
)

REQUEST_ID = "0123456789abcdef0123456789abcdef"
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
HANDLE = "abcdef0123456789abcdef0123456789"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"
REQUEST_DIGEST = "a" * 64
CONFIGURATION_DIGEST = "b" * 64


def _document(operation: str, **fields: object) -> bytes:
    value: dict[str, object] = {
        "format": "httk-workspace-command",
        "format_version": 4,
        "request_id": REQUEST_ID,
        "workspace_id": WORKSPACE_ID,
        "enrollment_id": ENROLLMENT_ID,
        "operation": operation,
        "created_at": 0,
        "expires_at": 0,
        "operator_key": None,
        "signature": None,
    }
    value.update(fields)
    return json.dumps(value).encode("utf-8")


@pytest.mark.parametrize(
    ("operation", "fields"),
    [
        ("health", {}),
        ("start_manager", {"configuration": "cpu-1", "configuration_digest": CONFIGURATION_DIGEST}),
        ("manager_status", {"handle": HANDLE}),
        ("cancel_manager", {"handle": HANDLE}),
    ],
)
def test_each_operation_round_trips(operation: str, fields: dict[str, str]) -> None:
    request = decode_request(_document(operation, **fields))

    assert request == Request(
        REQUEST_ID,
        WORKSPACE_ID,
        operation,
        profile=fields.get("configuration"),
        handle=fields.get("handle"),
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=fields.get("configuration_digest"),
    )
    encoded = encode_request(request)
    assert encoded.isascii()
    assert not encoded.endswith(b"\n")
    assert decode_request(encoded) == request


def test_request_is_frozen_and_retains_only_immutable_strings() -> None:
    request = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )

    with pytest.raises(FrozenInstanceError):
        request.profile = "other"  # type: ignore[misc]
    assert (request.request_id, request.workspace_id, request.operation, request.profile, request.handle) == (
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        "cpu",
        None,
    )


def test_encoding_and_digest_ignore_input_key_order_and_whitespace() -> None:
    ordered = _document("start_manager", configuration="cpu", configuration_digest=CONFIGURATION_DIGEST)
    reordered = (
        b'{ "configuration" : "cpu", "configuration_digest":"' + CONFIGURATION_DIGEST.encode() + b'",'
        b'"operation":"start_manager",'
        b'"workspace_id":"12345678-1234-1234-1234-123456789abc",'
        b'"request_id":"0123456789abcdef0123456789abcdef",'
        b'"enrollment_id":"fedcba9876543210fedcba9876543210",'
        b'"created_at":0,"expires_at":0,"operator_key":null,"signature":null,'
        b'"format_version":4,"format":"httk-workspace-command" }'
    )
    first = decode_request(ordered)
    second = decode_request(reordered)

    assert first == second
    assert encode_request(first) == encode_request(second)
    assert request_digest(first) == request_digest(second)


def test_digest_changes_with_request_identity_and_profile() -> None:
    base = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )
    other_id = Request(
        "f" * 32,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )
    other_profile = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="gpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )
    other_enrollment = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id="e" * 32,
        configuration_digest=CONFIGURATION_DIGEST,
    )
    other_digest = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest="c" * 64,
    )

    assert request_digest(base) != request_digest(other_id)
    assert request_digest(base) != request_digest(other_profile)
    assert request_digest(base) != request_digest(other_enrollment)
    assert request_digest(base) != request_digest(other_digest)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"request_id": "A" * 32},
        {"request_id": "g" * 32},
        {"request_id": "a" * 31},
        {"request_id": "a" * 33},
        {"request_id": "a" * 31 + "\n"},
        {"request_id": None},
        {"enrollment_id": "A" * 32},
        {"enrollment_id": "f" * 31},
        {"enrollment_id": None},
        {"workspace_id": "12345678-1234-1234-1234-123456789ABC"},
        {"workspace_id": "not-a-uuid"},
        {"workspace_id": 5},
        {"operation": "exec"},
        {"operation": None},
        {"operation": []},
        {"operation": "health", "profile": "cpu"},
        {"operation": "health", "handle": HANDLE},
        {"operation": "health", "configuration_digest": CONFIGURATION_DIGEST},
        {"operation": "start_manager"},
        {"operation": "start_manager", "profile": "CPU"},
        {"operation": "start_manager", "profile": "a" * 65},
        {"operation": "start_manager", "profile": ""},
        {"operation": "start_manager", "profile": None},
        {"operation": "start_manager", "profile": 7},
        {"operation": "start_manager", "profile": []},
        {"operation": "start_manager", "profile": "cpu", "handle": HANDLE},
        {"operation": "start_manager", "profile": "cpu", "configuration_digest": "A" * 64},
        {"operation": "manager_status"},
        {"operation": "manager_status", "handle": "x" * 32},
        {"operation": "manager_status", "handle": []},
        {"operation": "manager_status", "handle": HANDLE, "profile": "cpu"},
        {"operation": "manager_status", "handle": HANDLE, "configuration_digest": CONFIGURATION_DIGEST},
        {"operation": "cancel_manager", "handle": None},
    ],
)
def test_direct_construction_validates_all_fields(kwargs: dict[str, object]) -> None:
    fields: dict[str, object] = {
        "request_id": REQUEST_ID,
        "workspace_id": WORKSPACE_ID,
        "enrollment_id": ENROLLMENT_ID,
        "operation": "health",
    }
    fields.update(kwargs)

    with pytest.raises(ValueError) as error:
        Request(**fields)  # type: ignore[arg-type]
    assert len(str(error.value)) < 256


@pytest.mark.parametrize("version", ["true", "4.0", "4e0", "2", "null", '"4"'])
def test_version_must_be_exact_integer_four(version: str) -> None:
    data = _document("health").replace(b'"format_version": 4', f'"format_version": {version}'.encode())

    with pytest.raises(ValueError):
        decode_request(data)


def test_a_version_three_request_is_refused_with_a_teaching_message() -> None:
    data = _document("withdraw").replace(b'"format_version": 4', b'"format_version": 3')

    with pytest.raises(ValueError, match=r"unsupported request version 3.*protocol 4.*update the client"):
        decode_request(data)


def test_withdraw_is_no_longer_an_operation() -> None:
    with pytest.raises(ValueError, match="invalid operation"):
        Request(REQUEST_ID, WORKSPACE_ID, "withdraw", enrollment_id=ENROLLMENT_ID)
    with pytest.raises(ValueError, match="invalid operation"):
        decode_request(_document("withdraw"))
    with pytest.raises(ValueError, match="invalid outcome"):
        Response(REQUEST_ID, WORKSPACE_ID, ENROLLMENT_ID, REQUEST_DIGEST, "withdrawn")


def test_start_requires_configuration_digest_when_encoded() -> None:
    request = Request(REQUEST_ID, WORKSPACE_ID, "start_manager", profile="cpu", enrollment_id=ENROLLMENT_ID)

    with pytest.raises(ValueError, match="configuration_digest"):
        encode_request(request)


def test_profile_length_64_is_valid() -> None:
    profile = "a" + "x" * 63

    assert (
        decode_request(
            _document("start_manager", configuration=profile, configuration_digest=CONFIGURATION_DIGEST)
        ).profile
        == profile
    )


@pytest.mark.parametrize("request_id", ["a" * 31, "a" * 33, "a" * 31 + "\n"])
def test_wire_request_id_rejects_wrong_length_or_newline(request_id: str) -> None:
    with pytest.raises(ValueError):
        decode_request(_document("health", request_id=request_id))


def test_duplicate_keys_reject_escaped_equivalent_names() -> None:
    data = (
        b'{"format":"httk-workspace-command","format_version":4,'
        b'"request_id":"0123456789abcdef0123456789abcdef",'
        b'"workspace_id":"12345678-1234-1234-1234-123456789abc",'
        b'"enrollment_id":"fedcba9876543210fedcba9876543210",'
        b'"created_at":0,"expires_at":0,"operator_key":null,"signature":null,'
        b'"operation":"health","\\u006fperation":"cancel_manager"}'
    )

    with pytest.raises(ValueError):
        decode_request(data)


@pytest.mark.parametrize(
    "data",
    [
        b"\xef\xbb\xbf" + _document("health"),
        _document("health").decode().encode("utf-16"),
        _document("health").decode().encode("utf-16-le"),
        _document("health").decode().encode("utf-16-be"),
        _document("health").decode().encode("utf-32"),
        _document("health").decode().encode("utf-32-le"),
        _document("health").decode().encode("utf-32-be"),
        b"\xfe\xff" + _document("health").decode().encode("utf-16-be"),
        b"\x80" + _document("health"),
    ],
)
def test_non_utf8_or_bom_documents_are_refused(data: bytes) -> None:
    with pytest.raises(ValueError):
        decode_request(data)


@pytest.mark.parametrize(
    "data",
    [
        b"[]",
        b"null",
        b"{}",
        _document("health", profile=None),
        _document("health", argv=["rm", "-rf", "/"]),
        _document("health", command="whoami"),
        _document("health", cwd="/tmp"),
        _document("health", env={"PATH": "/tmp"}),
        _document("health", extra=None),
        _document("start_manager", configuration=None, configuration_digest=CONFIGURATION_DIGEST),
        _document("start_manager", configuration=[], configuration_digest=CONFIGURATION_DIGEST),
        _document("start_manager", configuration="", configuration_digest=CONFIGURATION_DIGEST),
        _document("start_manager", configuration="a" * 65, configuration_digest=CONFIGURATION_DIGEST),
        _document("start_manager", configuration="cpu"),
        _document("start_manager", configuration="cpu", configuration_digest="A" * 64),
        _document("start_manager", configuration="cpu", configuration_digest=CONFIGURATION_DIGEST, profile="cpu"),
        _document("manager_status", handle=""),
        _document("manager_status", handle=7),
        _document("unknown", payload="ignored"),
        _document("health", nested={"duplicate": 1}),
    ],
)
def test_invalid_envelopes_and_execution_fields_are_refused(data: bytes) -> None:
    with pytest.raises(ValueError) as error:
        decode_request(data)
    assert len(str(error.value)) < 256
    assert "whoami" not in str(error.value)


@pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
def test_nonfinite_json_constants_are_refused(constant: bytes) -> None:
    data = _document("health").replace(b'"health"', constant)

    with pytest.raises(ValueError):
        decode_request(data)


def test_lone_surrogate_is_refused_without_echoing_document() -> None:
    data = _document("health").replace(b'"health"', b'"\\ud800"')

    with pytest.raises(ValueError) as error:
        decode_request(data)
    assert len(str(error.value)) < 256
    assert "d800" not in str(error.value)


def test_nested_surrogate_in_unknown_field_is_refused() -> None:
    data = _document("health", unknown={"nested": "\ud800"})

    with pytest.raises(ValueError):
        decode_request(data)


def test_huge_integer_and_deep_nesting_become_bounded_value_errors() -> None:
    huge_integer = b'{"unknown":' + b"9" * 5000 + b"}"
    deep = b'{"x":' + b"[" * 7000 + b"]" * 7000 + b"}"

    for data in (huge_integer, deep):
        with pytest.raises(ValueError) as error:
            decode_request(data)
        assert len(str(error.value)) < 256
        assert "9999" not in str(error.value)


def test_input_size_and_types_are_bounded() -> None:
    valid = _document("health")
    exact_size = valid + b" " * (16 * 1024 - len(valid))

    assert len(exact_size) == 16 * 1024
    assert decode_request(exact_size).operation == "health"
    with pytest.raises(ValueError):
        decode_request(exact_size + b" ")
    for value in (None, "{}", bytearray(b"{}"), memoryview(b"{}")):
        with pytest.raises(ValueError):
            decode_request(value)  # type: ignore[arg-type]
    encode_values: tuple[object, ...] = (None, {}, "request")
    for encode_value in encode_values:
        with pytest.raises(ValueError):
            encode_request(encode_value)  # type: ignore[arg-type]


def test_request_requires_enrollment_on_wire_and_refuses_unknown_fields() -> None:
    missing_fields = json.loads(_document("health"))
    del missing_fields["enrollment_id"]
    missing = json.dumps(missing_fields).encode()
    with pytest.raises(ValueError):
        decode_request(missing)
    with pytest.raises(TypeError):
        Request(REQUEST_ID, WORKSPACE_ID, "health")  # type: ignore[call-arg]
    with pytest.raises(ValueError):
        decode_request(_document("health", old_schema_field="ignored"))


@pytest.mark.parametrize(
    ("outcome", "fields"),
    [
        ("ready", {}),
        ("submitted", {"handle": HANDLE}),
        ("status", {"handle": HANDLE, "scheduler_state": "RUNNING"}),
        ("cancel_requested", {"handle": HANDLE}),
        ("uncertain", {"handle": HANDLE, "reason": "submission_unknown"}),
        ("refused", {"reason": "policy_refused"}),
        ("refused", {"handle": HANDLE, "reason": "policy_refused"}),
        ("busy", {"reason": "capacity"}),
        ("uncertain", {"handle": HANDLE, "reason": "submission_unconfirmed", "detail": "sbatch exited 1: no"}),
        ("refused", {"reason": "scheduler_unavailable", "detail": '"\\ ~' * 250}),
    ],
)
def test_response_outcomes_round_trip(outcome: str, fields: dict[str, str]) -> None:
    response = Response(REQUEST_ID, WORKSPACE_ID, ENROLLMENT_ID, REQUEST_DIGEST, outcome, **fields)
    encoded = encode_response(response)

    assert encoded.isascii()
    assert not encoded.endswith(b"\n")
    assert decode_response(encoded) == response


def test_response_encoding_is_canonical_and_omits_absent_fields() -> None:
    response = Response(REQUEST_ID, WORKSPACE_ID, ENROLLMENT_ID, REQUEST_DIGEST, "ready")
    encoded = encode_response(response)
    alternate = (
        b'{ "outcome":"ready", "request_digest":"' + REQUEST_DIGEST.encode() + b'",'
        b'"enrollment_id":"' + ENROLLMENT_ID.encode() + b'",'
        b'"workspace_id":"' + WORKSPACE_ID.encode() + b'",'
        b'"request_id":"' + REQUEST_ID.encode() + b'", "format_version":4,'
        b'"operator_key":null,"signature":null,'
        b'"format":"httk-workspace-response" }'
    )
    assert decode_response(alternate) == response
    assert encode_response(decode_response(alternate)) == encoded
    assert b'"handle"' not in encoded
    assert b'"scheduler_state"' not in encoded
    assert b'"reason"' not in encoded
    assert b'"detail"' not in encoded


@pytest.mark.parametrize(
    "kwargs",
    [
        {"request_id": "g" * 32},
        {"workspace_id": "not-a-uuid"},
        {"enrollment_id": "g" * 32},
        {"request_digest": "a" * 63},
        {"request_digest": "A" * 64},
        {"outcome": "unknown"},
        {"handle": "x" * 32},
        {"scheduler_state": "RUNNING1"},
        {"scheduler_state": "a"},
        {"scheduler_state": "A" * 65},
        {"reason": "Bad"},
        {"reason": "a" * 65},
        {"outcome": "submitted", "handle": None},
        {"outcome": "status", "handle": HANDLE},
        {"outcome": "status", "handle": HANDLE, "scheduler_state": "RUNNING", "reason": "bad"},
        {"outcome": "uncertain", "handle": None, "reason": "timeout"},
        {"outcome": "busy", "reason": None},
        {"outcome": "ready", "reason": "ok"},
        {"outcome": "ready", "detail": "no reason"},
        {"outcome": "status", "handle": HANDLE, "scheduler_state": "UNKNOWN", "detail": "no reason"},
        {"outcome": "busy", "reason": "capacity", "detail": ""},
        {"outcome": "busy", "reason": "capacity", "detail": "a" * 1001},
        {"outcome": "busy", "reason": "capacity", "detail": "line\nbreak"},
        {"outcome": "busy", "reason": "capacity", "detail": "\x7f"},
        {"outcome": "busy", "reason": "capacity", "detail": "caf\u00e9"},
        {"outcome": "busy", "reason": "capacity", "detail": 1},
    ],
)
def test_response_constructor_rejects_invalid_fields(kwargs: dict[str, object]) -> None:
    fields: dict[str, object] = {
        "request_id": REQUEST_ID,
        "workspace_id": WORKSPACE_ID,
        "enrollment_id": ENROLLMENT_ID,
        "request_digest": REQUEST_DIGEST,
        "outcome": "ready",
    }
    fields.update(kwargs)
    with pytest.raises(ValueError):
        Response(**fields)  # type: ignore[arg-type]


def test_response_decoder_rejects_schema_and_malformed_documents() -> None:
    valid = encode_response(Response(REQUEST_ID, WORKSPACE_ID, ENROLLMENT_ID, REQUEST_DIGEST, "ready"))
    invalid_documents = (
        valid.replace(b"httk-workspace-response", b"httk-workspace-command"),
        valid.replace(b'"format_version":4', b'"format_version":true'),
        valid[:-1] + b',"old_field":1}',
        valid[:-1] + b',"handle":null}',
        valid[:-1] + b',"detail":"no reason"}',
        valid.replace(b'"request_id":', b'"request_id":"' + REQUEST_ID.encode() + b'","request_id":'),
        valid.replace(b'"outcome":"ready"', b'"outcome":"ready","outcome":"busy"'),
        valid.replace(b'"outcome":"ready"', b'"outcome":NaN'),
        valid.decode().encode("utf-16"),
        valid + b" " * (16 * 1024),
        b"\xef\xbb\xbf" + valid,
        valid.replace(b'"outcome":"ready"', b'"outcome":"busy"'),
    )
    for invalid in invalid_documents:
        with pytest.raises(ValueError):
            decode_response(invalid)


def test_response_signature_covers_a_maximal_detail(tmp_path: Path) -> None:
    seed = tmp_path / "response.seed"
    seed.write_text(base64.b64encode(bytes([3]) * 32).decode("ascii") + "\n", encoding="ascii")
    seed.chmod(0o600)
    public_key = identity_public_key(seed)
    assert public_key is not None
    unsigned = Response(REQUEST_ID, WORKSPACE_ID, ENROLLMENT_ID, REQUEST_DIGEST, "refused", HANDLE, None, "a" * 64)
    response = sign_response(replace(unsigned, detail='"' * 1000), seed_path=seed)
    assert decode_response(encode_response(response)) == response
    verify_response(response, public_key)
    with pytest.raises(ValueError, match="signature"):
        verify_response(replace(response, detail="'" * 1000), public_key)


def test_response_without_detail_keeps_its_version_four_encoding() -> None:
    stored = (
        b'{"enrollment_id":"' + ENROLLMENT_ID.encode() + b'","format":"httk-workspace-response","format_version":4,'
        b'"handle":"' + HANDLE.encode() + b'","operator_key":null,"outcome":"uncertain",'
        b'"reason":"submission_unconfirmed","request_digest":"' + REQUEST_DIGEST.encode() + b'",'
        b'"request_id":"' + REQUEST_ID.encode() + b'","signature":null,"workspace_id":"' + WORKSPACE_ID.encode() + b'"}'
    )
    response = decode_response(stored)
    assert response.detail is None
    assert encode_response(response) == stored
