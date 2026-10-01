"""Strict request codec and validation tests for the workspace daemon."""

import json
from dataclasses import FrozenInstanceError

import pytest

from httk.workflow._daemon_protocol import Request, decode_request, encode_request, request_digest

REQUEST_ID = "0123456789abcdef0123456789abcdef"
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
HANDLE = "abcdef0123456789abcdef0123456789"


def _document(operation: str, **fields: object) -> bytes:
    value: dict[str, object] = {
        "format": "httk-workspace-command",
        "format_version": 1,
        "request_id": REQUEST_ID,
        "workspace_id": WORKSPACE_ID,
        "operation": operation,
    }
    value.update(fields)
    return json.dumps(value).encode("utf-8")


@pytest.mark.parametrize(
    ("operation", "fields"),
    [
        ("health", {}),
        ("start_manager", {"profile": "cpu-1"}),
        ("manager_status", {"handle": HANDLE}),
        ("cancel_manager", {"handle": HANDLE}),
    ],
)
def test_each_operation_round_trips(operation: str, fields: dict[str, str]) -> None:
    request = decode_request(_document(operation, **fields))

    assert request == Request(REQUEST_ID, WORKSPACE_ID, operation, **fields)
    encoded = encode_request(request)
    assert encoded.isascii()
    assert not encoded.endswith(b"\n")
    assert decode_request(encoded) == request


def test_request_is_frozen_and_retains_only_immutable_strings() -> None:
    request = Request(REQUEST_ID, WORKSPACE_ID, "start_manager", profile="cpu")

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
    ordered = _document("start_manager", profile="cpu")
    reordered = (
        b'{ "profile" : "cpu", "operation":"start_manager",'
        b'"workspace_id":"12345678-1234-1234-1234-123456789abc",'
        b'"request_id":"0123456789abcdef0123456789abcdef",'
        b'"format_version":1,"format":"httk-workspace-command" }'
    )
    first = decode_request(ordered)
    second = decode_request(reordered)

    assert first == second
    assert encode_request(first) == encode_request(second)
    assert request_digest(first) == request_digest(second)


def test_digest_changes_with_request_identity_and_profile() -> None:
    base = Request(REQUEST_ID, WORKSPACE_ID, "start_manager", profile="cpu")
    other_id = Request("f" * 32, WORKSPACE_ID, "start_manager", profile="cpu")
    other_profile = Request(REQUEST_ID, WORKSPACE_ID, "start_manager", profile="gpu")

    assert request_digest(base) != request_digest(other_id)
    assert request_digest(base) != request_digest(other_profile)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"request_id": "A" * 32},
        {"request_id": "g" * 32},
        {"request_id": "a" * 31},
        {"request_id": "a" * 33},
        {"request_id": "a" * 31 + "\n"},
        {"request_id": None},
        {"workspace_id": "12345678-1234-1234-1234-123456789ABC"},
        {"workspace_id": "not-a-uuid"},
        {"workspace_id": 5},
        {"operation": "exec"},
        {"operation": None},
        {"operation": []},
        {"operation": "health", "profile": "cpu"},
        {"operation": "health", "handle": HANDLE},
        {"operation": "start_manager"},
        {"operation": "start_manager", "profile": "CPU"},
        {"operation": "start_manager", "profile": "a" * 65},
        {"operation": "start_manager", "profile": ""},
        {"operation": "start_manager", "profile": None},
        {"operation": "start_manager", "profile": 7},
        {"operation": "start_manager", "profile": []},
        {"operation": "start_manager", "profile": "cpu", "handle": HANDLE},
        {"operation": "manager_status"},
        {"operation": "manager_status", "handle": "x" * 32},
        {"operation": "manager_status", "handle": []},
        {"operation": "manager_status", "handle": HANDLE, "profile": "cpu"},
        {"operation": "cancel_manager", "handle": None},
    ],
)
def test_direct_construction_validates_all_fields(kwargs: dict[str, object]) -> None:
    fields: dict[str, object] = {
        "request_id": REQUEST_ID,
        "workspace_id": WORKSPACE_ID,
        "operation": "health",
    }
    fields.update(kwargs)

    with pytest.raises(ValueError) as error:
        Request(**fields)  # type: ignore[arg-type]
    assert len(str(error.value)) < 256


@pytest.mark.parametrize("version", ["true", "1.0", "1e0", "2", "null", '"1"'])
def test_version_must_be_exact_integer_one(version: str) -> None:
    data = _document("health").replace(b'"format_version": 1', f'"format_version": {version}'.encode())

    with pytest.raises(ValueError):
        decode_request(data)


def test_profile_length_64_is_valid() -> None:
    profile = "a" + "x" * 63

    assert decode_request(_document("start_manager", profile=profile)).profile == profile


@pytest.mark.parametrize("request_id", ["a" * 31, "a" * 33, "a" * 31 + "\n"])
def test_wire_request_id_rejects_wrong_length_or_newline(request_id: str) -> None:
    with pytest.raises(ValueError):
        decode_request(_document("health", request_id=request_id))


def test_duplicate_keys_reject_escaped_equivalent_names() -> None:
    data = (
        b'{"format":"httk-workspace-command","format_version":1,'
        b'"request_id":"0123456789abcdef0123456789abcdef",'
        b'"workspace_id":"12345678-1234-1234-1234-123456789abc",'
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
        _document("start_manager", profile=None),
        _document("start_manager", profile=[]),
        _document("start_manager", profile=""),
        _document("start_manager", profile="a" * 65),
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
