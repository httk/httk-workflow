"""Mounted daemon client identity, race, and filesystem-boundary tests."""

import base64
import json
import os
import threading
import time
from pathlib import Path

import pytest
from httk.core.identity import identity_public_key

import httk.workflow._daemon_client as client_module
from httk.workflow._daemon_auth import sign_request, sign_response
from httk.workflow._daemon_client import Endpoint, decode_matching_response, exchange
from httk.workflow._daemon_mailbox import MAX_DOCUMENT_BYTES, MailboxDirectory
from httk.workflow._daemon_protocol import (
    Request,
    Response,
    decode_request,
    encode_request,
    encode_response,
    request_digest,
)

REQUEST_ID = "0123456789abcdef0123456789abcdef"
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"
HANDLE = "abcdef0123456789abcdef0123456789"
CONFIGURATION_DIGEST = "b" * 64


@pytest.fixture(autouse=True)
def _private_data_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / "data-home"))


def _seed(path: Path, byte: int) -> Path:
    path.write_text(base64.b64encode(bytes([byte]) * 32).decode("ascii") + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _document(public_key: str, **changes: object) -> dict[str, object]:
    document: dict[str, object] = {
        "format": "httk-workspace-daemon-endpoint",
        "format_version": 2,
        "workspace_id": WORKSPACE_ID,
        "enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": public_key,
        "configurations": {"cpu": CONFIGURATION_DIGEST},
        "request_max_age": 3600,
    }
    document.update(changes)
    return document


def _write_endpoint(exchange: Path, document: dict[str, object]) -> None:
    (exchange / "endpoint.json").write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )


def _endpoint(tmp_path: Path) -> Endpoint:
    exchange = tmp_path / "exchange"
    for name in ("requests", "responses", "inbox", "outbox"):
        (exchange / name).mkdir(parents=True)
    response_seed = _seed(tmp_path / "response.seed", 2)
    public_key = identity_public_key(response_seed)
    assert public_key is not None
    _write_endpoint(exchange, _document(public_key))
    return Endpoint(exchange, WORKSPACE_ID, ENROLLMENT_ID, public_key)


def _settings(endpoint: Endpoint) -> dict[str, object]:
    return {
        "exchange": str(endpoint.exchange),
        "daemon_workspace_id": endpoint.workspace_id,
        "daemon_enrollment_id": endpoint.enrollment_id,
        "daemon_public_key": endpoint.public_key,
    }


def _request(operation: str = "health", *, profile: str | None = None, handle: str | None = None) -> Request:
    return Request(
        REQUEST_ID,
        WORKSPACE_ID,
        operation,
        profile=profile,
        handle=handle,
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST if operation == "start_manager" else None,
    )


def _signed(request: Request, tmp_path: Path) -> Request:
    return sign_request(request, seed_path=_seed(tmp_path / "client.seed", 1), now=1_000_000)


def _response(request: Request, outcome: str, **fields: str) -> Response:
    return Response(
        request.request_id,
        request.workspace_id,
        request.enrollment_id,
        request_digest(request),
        outcome,
        **fields,
    )


def _publish_response(endpoint: Endpoint, request: Request, outcome: str, **fields: str) -> None:
    name = f"{request.request_id}.json"
    with MailboxDirectory(endpoint.responses) as responses:
        responses.replace(
            name,
            encode_response(
                sign_response(
                    _response(request, outcome, **fields), seed_path=endpoint.exchange.parent / "response.seed"
                )
            ),
        )


def _broker_once(endpoint: Endpoint, outcome: str, **fields: str) -> threading.Thread:
    def broker() -> None:
        path = endpoint.requests / f"{REQUEST_ID}.json"
        deadline = time.monotonic() + 10
        while not path.exists():
            if time.monotonic() >= deadline:
                return
            time.sleep(0.005)
        request = decode_request(path.read_bytes())
        _publish_response(endpoint, request, outcome, **fields)
        path.unlink(missing_ok=True)

    thread = threading.Thread(target=broker)
    thread.start()
    return thread


def test_endpoint_accepts_exact_settings_and_checks_workspace(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)

    decoded = Endpoint.from_settings(_settings(endpoint))

    assert decoded == endpoint
    decoded.check()


def test_read_endpoint_returns_validated_live_document(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    document = _document(endpoint.public_key, configurations={"cpu": CONFIGURATION_DIGEST, "gpu": "c" * 64})
    document["request_max_age"] = 900
    _write_endpoint(endpoint.exchange, document)

    value = client_module.read_endpoint(endpoint.exchange)

    assert dict(value["configurations"]) == {"cpu": CONFIGURATION_DIGEST, "gpu": "c" * 64}  # type: ignore[call-overload]
    assert value["request_max_age"] == 900


@pytest.mark.parametrize(
    "change",
    [
        {"unknown": "x"},
        {"exchange": "relative"},
        {"exchange": "/tmp/../tmp/exchange"},
        {"daemon_workspace_id": "not-a-uuid"},
        {"daemon_enrollment_id": "A" * 32},
        {"mount_root": "/tmp/x"},
    ],
)
def test_endpoint_refuses_unknown_or_malformed_settings(tmp_path: Path, change: dict[str, object]) -> None:
    endpoint = _endpoint(tmp_path)
    settings = _settings(endpoint)
    settings.update(change)

    with pytest.raises(ValueError):
        Endpoint.from_settings(settings)


def test_endpoint_refuses_missing_setting(tmp_path: Path) -> None:
    settings = _settings(_endpoint(tmp_path))
    del settings["exchange"]

    with pytest.raises(ValueError, match="four"):
        Endpoint.from_settings(settings)


@pytest.mark.parametrize(
    "change",
    [
        {"configurations": []},
        {"configurations": {"CPU": CONFIGURATION_DIGEST}},
        {"configurations": {"cpu": "B" * 64}},
        {"configurations": {"cpu": True}},
        {"request_max_age": True},
        {"request_max_age": 0},
        {"request_max_age": 86_401},
        {"request_max_age": "3600"},
        {"format": "other"},
        {"format_version": 1},
        {"workspace_id": "not-a-uuid"},
        {"enrollment_id": "A" * 32},
        {"daemon_public_key": "ed25519:AAAA"},
    ],
)
def test_read_endpoint_refuses_invalid_values(tmp_path: Path, change: dict[str, object]) -> None:
    endpoint = _endpoint(tmp_path)
    _write_endpoint(endpoint.exchange, _document(endpoint.public_key, **change))

    with pytest.raises(ValueError):
        client_module.read_endpoint(endpoint.exchange)


@pytest.mark.parametrize("kind", ["extra", "duplicate", "bom", "nonfinite"])
def test_read_endpoint_refuses_extra_keys_duplicates_boms_and_nonfinite(tmp_path: Path, kind: str) -> None:
    endpoint = _endpoint(tmp_path)
    path = endpoint.exchange / "endpoint.json"
    text = path.read_text(encoding="utf-8")
    if kind == "extra":
        path.write_text(text[:-1] + ',"extra":1}', encoding="utf-8")
    elif kind == "duplicate":
        path.write_text(text[:-1] + ',"request_max_age":5}', encoding="utf-8")
    elif kind == "bom":
        path.write_bytes(b"\xef\xbb\xbf" + text.encode("utf-8"))
    else:
        path.write_text(text.replace("3600", "NaN"), encoding="utf-8")

    with pytest.raises(ValueError):
        client_module.read_endpoint(endpoint.exchange)


@pytest.mark.parametrize("kind", ["symlink", "fifo", "oversize"])
def test_read_endpoint_refuses_adversarial_files(tmp_path: Path, kind: str) -> None:
    endpoint = _endpoint(tmp_path)
    path = endpoint.exchange / "endpoint.json"
    original = path.read_bytes()
    path.unlink()
    if kind == "symlink":
        outside = tmp_path / "outside.json"
        outside.write_bytes(original)
        path.symlink_to(outside)
    elif kind == "fifo":
        os.mkfifo(path)  # open must not block waiting for a writer
    else:
        path.write_bytes(b" " * (64 * 1024 + 1))

    with pytest.raises((OSError, ValueError)):
        endpoint.check()


def test_endpoint_check_accepts_exchange_and_reads_only_endpoint(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    (endpoint.exchange / "outbox" / "status.json").write_bytes(b"\xff not json")

    Endpoint.from_settings(_settings(endpoint)).check()


@pytest.mark.parametrize("name", ["requests", "responses", "inbox", "outbox"])
def test_endpoint_check_refuses_missing_or_symlinked_subdirectory(tmp_path: Path, name: str) -> None:
    endpoint = _endpoint(tmp_path)
    (endpoint.exchange / name).rmdir()
    with pytest.raises(OSError):
        endpoint.check()
    (endpoint.exchange / name).symlink_to(tmp_path)
    with pytest.raises(OSError):
        endpoint.check()


def test_endpoint_check_refuses_symlinked_exchange(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    linked = tmp_path / "linked"
    linked.symlink_to(endpoint.exchange)

    with pytest.raises(OSError):
        Endpoint(linked, WORKSPACE_ID, ENROLLMENT_ID, endpoint.public_key).check()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workspace_id", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        ("enrollment_id", "0" * 32),
        ("daemon_public_key", "ed25519:" + base64.b64encode(b"\x07" * 32).decode("ascii")),
    ],
)
def test_endpoint_check_refuses_changed_identity(tmp_path: Path, field: str, value: str) -> None:
    endpoint = _endpoint(tmp_path)
    _write_endpoint(endpoint.exchange, _document(endpoint.public_key, **{field: value}))

    with pytest.raises(ValueError, match=f"{field}.*enrollment changed"):
        endpoint.check()


@pytest.mark.parametrize(
    ("daemon_request", "outcome", "fields"),
    [
        (_request(), "ready", {}),
        (_request("start_manager", profile="cpu"), "submitted", {"handle": HANDLE}),
        (_request("manager_status", handle=HANDLE), "status", {"handle": HANDLE, "scheduler_state": "UNKNOWN"}),
        (_request("cancel_manager", handle=HANDLE), "cancel_requested", {"handle": HANDLE}),
        (_request("manager_status", handle=HANDLE), "refused", {"reason": "policy_refused"}),
        (_request("cancel_manager", handle=HANDLE), "busy", {"reason": "capacity"}),
    ],
)
def test_decode_matching_response_accepts_operation_outcomes(
    tmp_path: Path, daemon_request: Request, outcome: str, fields: dict[str, str]
) -> None:
    endpoint = _endpoint(tmp_path)
    daemon_request = _signed(daemon_request, tmp_path)
    response = sign_response(
        _response(daemon_request, outcome, **fields),
        seed_path=tmp_path / "response.seed",
    )

    assert (
        decode_matching_response(encode_response(response), daemon_request, public_key=endpoint.public_key) == response
    )


@pytest.mark.parametrize("mismatch", ["request_id", "workspace_id", "enrollment_id", "digest", "outcome", "handle"])
def test_decode_matching_response_refuses_every_binding_mismatch(tmp_path: Path, mismatch: str) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request("manager_status", handle=HANDLE), tmp_path)
    values: dict[str, object] = {
        "request_id": request.request_id,
        "workspace_id": request.workspace_id,
        "enrollment_id": request.enrollment_id,
        "request_digest": request_digest(request),
        "outcome": "status",
        "handle": HANDLE,
        "scheduler_state": "RUNNING",
    }
    replacements: dict[str, object] = {
        "request_id": "1" * 32,
        "workspace_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "enrollment_id": "2" * 32,
        "digest": "3" * 64,
        "handle": "4" * 32,
    }
    if mismatch == "outcome":
        values["outcome"] = "cancel_requested"
        del values["scheduler_state"]
    else:
        values["request_digest" if mismatch == "digest" else mismatch] = replacements[mismatch]
    response = sign_response(Response(**values), seed_path=tmp_path / "response.seed")  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        decode_matching_response(encode_response(response), request, public_key=endpoint.public_key)


def test_decode_matching_response_refuses_health_handle(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    response = sign_response(
        _response(request, "refused", handle=HANDLE, reason="policy_refused"),
        seed_path=tmp_path / "response.seed",
    )

    with pytest.raises(ValueError, match="handle"):
        decode_matching_response(encode_response(response), request, public_key=endpoint.public_key)


def test_cached_terminal_response_returns_without_publication_and_is_cleaned(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request("start_manager", profile="cpu"), tmp_path)
    _publish_response(endpoint, request, "submitted", handle=HANDLE)

    response = exchange(endpoint, request, wait_seconds=0.2)

    assert response.outcome == "submitted"
    assert not (endpoint.requests / f"{REQUEST_ID}.json").exists()
    assert not (endpoint.responses / f"{REQUEST_ID}.json").exists()


def test_existing_conflicting_request_is_never_replaced(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request("start_manager", profile="cpu"), tmp_path)
    conflict = Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="other",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest="c" * 64,
    )
    path = endpoint.requests / f"{REQUEST_ID}.json"
    path.write_bytes(encode_request(conflict))

    with pytest.raises(ValueError, match="conflicts"):
        exchange(endpoint, request, wait_seconds=0.1)
    assert path.read_bytes() == encode_request(conflict)


def test_existing_matching_request_waits_for_response_without_republication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    path = endpoint.requests / f"{REQUEST_ID}.json"
    path.write_bytes(encode_request(request))
    publications = 0
    original_replace = MailboxDirectory.replace

    def counted_replace(mailbox: MailboxDirectory, name: str, data: bytes) -> None:
        nonlocal publications
        if name == path.name:
            publications += 1
        original_replace(mailbox, name, data)

    monkeypatch.setattr(MailboxDirectory, "replace", counted_replace)
    thread = _broker_once(endpoint, "ready")
    try:
        assert exchange(endpoint, request, wait_seconds=5).outcome == "ready"
    finally:
        thread.join(timeout=10)
    # Only the broker's response replacement ran; the request was not republished.
    assert publications == 1


def test_response_before_request_unlink_is_returned(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    request_path = endpoint.requests / f"{REQUEST_ID}.json"
    request_path.write_bytes(encode_request(request))
    _publish_response(endpoint, request, "ready")

    assert exchange(endpoint, request, wait_seconds=0.2).outcome == "ready"
    assert request_path.exists()


def test_disappearance_gets_a_final_response_check_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    response = _response(request, "ready")
    request_reads = iter((True, False))
    response_reads = iter((None, None, response))
    published = False

    monkeypatch.setattr(client_module, "_read_request", lambda *_args: next(request_reads))
    monkeypatch.setattr(client_module, "_read_response", lambda *_args: next(response_reads))
    monkeypatch.setattr(client_module, "_remove_returned_response", lambda *_args: None)
    monkeypatch.setattr(client_module, "_sleep_until_poll", lambda _deadline: None)

    def fail_publish(*_args: object) -> None:
        nonlocal published
        published = True

    monkeypatch.setattr(MailboxDirectory, "replace", fail_publish)

    assert exchange(endpoint, request, wait_seconds=1) == response
    assert published is False


def test_stale_busy_is_cleared_before_one_new_publication(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    _publish_response(endpoint, request, "busy", reason="capacity")
    thread = _broker_once(endpoint, "ready")
    try:
        assert exchange(endpoint, request, wait_seconds=5).outcome == "ready"
    finally:
        thread.join(timeout=10)


def test_stale_busy_before_request_unlink_waits_then_republishes_once(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    name = f"{REQUEST_ID}.json"
    request_path = endpoint.requests / name
    response_path = endpoint.responses / name
    request_path.write_bytes(encode_request(request))
    _publish_response(endpoint, request, "busy", reason="capacity")
    errors: list[BaseException] = []

    def broker() -> None:
        try:
            deadline = time.monotonic() + 10
            while response_path.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            request_path.unlink()
            while not request_path.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            published = decode_request(request_path.read_bytes())
            _publish_response(endpoint, published, "ready")
            request_path.unlink(missing_ok=True)
        except BaseException as exc:  # pragma: no cover - asserted in the caller
            errors.append(exc)

    thread = threading.Thread(target=broker)
    thread.start()
    try:
        assert exchange(endpoint, request, wait_seconds=5).outcome == "ready"
    finally:
        thread.join(timeout=10)
    assert not thread.is_alive()
    assert errors == []


def test_failed_stale_busy_cleanup_blocks_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    _publish_response(endpoint, request, "busy", reason="capacity")
    monkeypatch.setattr(MailboxDirectory, "remove", lambda *_args: (_ for _ in ()).throw(OSError("injected")))

    with pytest.raises(RuntimeError, match="stale busy"):
        exchange(endpoint, request, wait_seconds=0.2)
    assert not (endpoint.requests / f"{REQUEST_ID}.json").exists()


def test_confirmed_cleanup_error_preserves_known_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    _publish_response(endpoint, request, "ready")
    monkeypatch.setattr(MailboxDirectory, "remove", lambda *_args: (_ for _ in ()).throw(OSError("injected")))

    assert exchange(endpoint, request, wait_seconds=0.2).outcome == "ready"
    assert "cleanup failed" in caplog.text


def test_timeout_leaves_matching_request_for_same_id_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request("start_manager", profile="cpu"), tmp_path)
    clock = [100.0]

    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(client_module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    with pytest.raises(TimeoutError):
        exchange(endpoint, request, wait_seconds=0.05)
    assert (endpoint.requests / f"{REQUEST_ID}.json").read_bytes() == encode_request(request)


def test_workspace_check_spends_the_same_deadline_used_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint = _endpoint(tmp_path)
    clock = [100.0]

    def delayed_check(_endpoint: Endpoint) -> None:
        clock[0] = 100.06

    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(Endpoint, "check", delayed_check)

    with pytest.raises(TimeoutError):
        exchange(endpoint, _signed(_request(), tmp_path), wait_seconds=0.05)
    assert not list(endpoint.requests.iterdir())


def test_error_after_rename_leaves_published_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request("start_manager", profile="cpu"), tmp_path)
    original = MailboxDirectory.replace

    def replace_then_error(mailbox: MailboxDirectory, name: str, data: bytes) -> None:
        original(mailbox, name, data)
        raise OSError("injected post-rename error")

    monkeypatch.setattr(MailboxDirectory, "replace", replace_then_error)
    with pytest.raises(OSError, match="post-rename"):
        exchange(endpoint, request, wait_seconds=0.2)
    assert (endpoint.requests / f"{REQUEST_ID}.json").read_bytes() == encode_request(request)


@pytest.mark.parametrize("kind", ["symlink", "fifo", "oversize"])
def test_exchange_fails_closed_on_adversarial_response_entries(tmp_path: Path, kind: str) -> None:
    endpoint = _endpoint(tmp_path)
    request = _signed(_request(), tmp_path)
    path = endpoint.responses / f"{REQUEST_ID}.json"
    if kind == "symlink":
        outside = tmp_path / "outside-response.json"
        outside.write_bytes(encode_response(_response(request, "ready")))
        path.symlink_to(outside)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes(b"x" * (MAX_DOCUMENT_BYTES + 1))

    with pytest.raises((OSError, ValueError)):
        exchange(endpoint, request, wait_seconds=0.2)
    assert not (endpoint.requests / f"{REQUEST_ID}.json").exists()


@pytest.mark.parametrize("wait", [True, 0, 0.049, 121, 10**400, float("inf"), float("nan")])
def test_exchange_refuses_invalid_wait_without_publication(tmp_path: Path, wait: object) -> None:
    endpoint = _endpoint(tmp_path)

    with pytest.raises(ValueError, match="wait_seconds"):
        exchange(endpoint, _signed(_request(), tmp_path), wait_seconds=wait)  # type: ignore[arg-type]
    assert not list(endpoint.requests.iterdir())


def _passive_documents(endpoint: Endpoint) -> tuple[dict[str, object], dict[str, object]]:
    status: dict[str, object] = {
        "format": "httk-workspace-daemon-status",
        "format_version": 1,
        "workspace_id": endpoint.workspace_id,
        "generated_at": "2026-01-01T00:00:00.000000Z",
        "jobs": [{"job_id": "j", "job_key": "k", "state": "submitted"}],
        "rejected": [{"name": "n", "reason": "r"}],
        "eject_errors": [],
        "truncated": False,
    }
    managers: dict[str, object] = {
        "format": "httk-workspace-daemon-managers",
        "format_version": 1,
        "enrollment_id": endpoint.enrollment_id,
        "generated_at": "2026-01-01T00:00:00.000000Z",
        "managers": [{"handle": HANDLE, "profile": "cpu", "request_id": REQUEST_ID, "state": "RUNNING"}],
    }
    return status, managers


def _publish(endpoint: Endpoint, name: str, document: object) -> Path:
    path = endpoint.exchange / "outbox" / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_passive_status_reads_both_files_or_none(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    assert client_module.read_passive_status(endpoint) == {"status": None, "managers": None}
    status, managers = _passive_documents(endpoint)
    _publish(endpoint, "status.json", status)
    assert client_module.read_passive_status(endpoint) == {"status": status, "managers": None}
    _publish(endpoint, "managers.json", managers)
    assert client_module.read_passive_status(endpoint) == {"status": status, "managers": managers}


@pytest.mark.parametrize(
    ("name", "change"),
    [
        ("status.json", {"format": "other"}),
        ("status.json", {"extra": 1}),
        ("status.json", {"format_version": 2}),
        ("status.json", {"truncated": "no"}),
        ("status.json", {"jobs": [{"job_id": 1, "job_key": "k", "state": "s"}]}),
        ("status.json", {"jobs": [{"job_id": "j"}]}),
        ("status.json", {"workspace_id": "12345678-1234-1234-1234-000000000000"}),
        ("managers.json", {"enrollment_id": "0" * 32}),
        ("managers.json", {"managers": [{"handle": 1, "profile": "p", "request_id": "r", "state": "s"}]}),
        ("managers.json", {"format": "httk-workspace-daemon-status"}),
    ],
)
def test_passive_status_refuses_invalid_documents(tmp_path: Path, name: str, change: dict[str, object]) -> None:
    endpoint = _endpoint(tmp_path)
    status, managers = _passive_documents(endpoint)
    document = status if name == "status.json" else managers
    document.update(change)
    _publish(endpoint, name, document)
    with pytest.raises(ValueError, match=name):
        client_module.read_passive_status(endpoint)


def test_passive_status_refuses_unsafe_files(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    outbox = endpoint.exchange / "outbox"
    os.mkfifo(outbox / "status.json")
    with pytest.raises(ValueError, match="status.json"):  # returns without blocking
        client_module.read_passive_status(endpoint)
    (outbox / "status.json").unlink()
    (outbox / "status.json").write_text(" " * (1024 * 1024 + 1), encoding="utf-8")
    with pytest.raises(ValueError, match="exceeds"):
        client_module.read_passive_status(endpoint)
    (outbox / "status.json").unlink()
    status, _managers = _passive_documents(endpoint)
    (tmp_path / "elsewhere.json").write_text(json.dumps(status), encoding="utf-8")
    (outbox / "status.json").symlink_to(tmp_path / "elsewhere.json")
    with pytest.raises(OSError):
        client_module.read_passive_status(endpoint)


def test_passive_status_fails_closed_when_the_enrollment_changed(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    _write_endpoint(endpoint.exchange, _document(endpoint.public_key, enrollment_id="0" * 32))
    with pytest.raises(ValueError, match="enrollment"):
        client_module.read_passive_status(endpoint)
