"""Durable signed-request client tests."""

import base64
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest
from httk.core.identity import add_identity, identity_public_key, set_default_identity

import httk.workflow._daemon_client as client_module
from httk.workflow._daemon_auth import sign_response
from httk.workflow._daemon_client import Endpoint, decode_matching_response, prepare_request
from httk.workflow._daemon_protocol import Request, Response, encode_request, encode_response, request_digest

REQUEST_ID = "0123456789abcdef0123456789abcdef"
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"
CONFIGURATION_DIGEST = "b" * 64


def _seed(path: Path, byte: int) -> Path:
    path.write_text(base64.b64encode(bytes([byte]) * 32).decode("ascii") + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


@pytest.fixture
def endpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Endpoint:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / "data"))
    add_identity("first", "First Operator", "first@example.test")
    exchange = tmp_path / "exchange"
    for name in ("requests", "responses", "inbox", "outbox"):
        (exchange / name).mkdir(parents=True)
    response_seed = _seed(tmp_path / "response.seed", 8)
    public_key = identity_public_key(response_seed)
    assert public_key is not None
    _write_endpoint(exchange, public_key)
    return Endpoint(exchange, WORKSPACE_ID, ENROLLMENT_ID, public_key)


def _write_endpoint(exchange: Path, public_key: str, **changes: object) -> None:
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
    (exchange / "endpoint.json").write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )


def _intent() -> Request:
    return Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )


def _settings(endpoint: Endpoint) -> dict[str, object]:
    return {
        "exchange": str(endpoint.exchange),
        "daemon_workspace_id": endpoint.workspace_id,
        "daemon_enrollment_id": endpoint.enrollment_id,
        "daemon_public_key": endpoint.public_key,
    }


def test_prepare_request_reuses_exact_signature_without_resigning(
    endpoint: Endpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    original = client_module.sign_request

    def counted(request: Request, *, lifetime: int = 3600) -> Request:
        nonlocal calls
        calls += 1
        return original(request, now=1_000 + calls * 100, lifetime=lifetime)

    monkeypatch.setattr(client_module, "sign_request", counted)
    first = prepare_request(endpoint, _intent())
    second = prepare_request(endpoint, _intent())

    assert calls == 1
    assert encode_request(first) == encode_request(second)
    assert (first.created_at, first.expires_at) == (1_100, 4_700)
    cache = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID / f"{REQUEST_ID}.json"
    envelope = json.loads(cache.read_bytes())
    assert envelope["request"] == json.loads(encode_request(first))
    assert envelope["binding"]["daemon_public_key"] == endpoint.public_key


def test_prepare_request_retry_is_exact_across_processes(endpoint: Endpoint, tmp_path: Path) -> None:
    settings_path = tmp_path / "endpoint.json"
    settings_path.write_text(json.dumps(_settings(endpoint)), encoding="utf-8")
    script = (
        "import json,sys; from pathlib import Path; "
        "from httk.workflow._daemon_client import Endpoint,prepare_request; "
        "from httk.workflow._daemon_protocol import Request,encode_request; "
        "e=Endpoint.from_settings(json.loads(Path(sys.argv[1]).read_text())); "
        "r=prepare_request(e,Request(sys.argv[2],sys.argv[3],'start_manager',profile='cpu',"
        "enrollment_id=sys.argv[4],configuration_digest=sys.argv[5])); "
        "sys.stdout.buffer.write(encode_request(r))"
    )
    command = [
        sys.executable,
        "-c",
        script,
        str(settings_path),
        REQUEST_ID,
        WORKSPACE_ID,
        ENROLLMENT_ID,
        CONFIGURATION_DIGEST,
    ]

    first = subprocess.run(command, check=True, capture_output=True).stdout
    time.sleep(0.01)
    second = subprocess.run(command, check=True, capture_output=True).stdout

    assert first == second


def test_withdraw_bundle_is_signed_cached_and_part_of_the_intent(endpoint: Endpoint) -> None:
    intent = Request(REQUEST_ID, WORKSPACE_ID, "withdraw", enrollment_id=ENROLLMENT_ID, bundle="alpha")
    saved = prepare_request(endpoint, intent)
    assert saved.bundle == "alpha" and saved.signature is not None
    assert prepare_request(endpoint, intent) == saved
    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_request(endpoint, replace(intent, bundle="beta"))
    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_request(endpoint, replace(intent, bundle=None))


def test_prepare_request_refuses_changed_intent_endpoint_pin_and_signer(endpoint: Endpoint, tmp_path: Path) -> None:
    saved = prepare_request(endpoint, _intent())

    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_request(endpoint, replace(_intent(), profile="other"))

    other_seed = _seed(tmp_path / "other-response.seed", 9)
    other_public = identity_public_key(other_seed)
    assert other_public is not None
    changed_endpoint = replace(endpoint, public_key=other_public)
    with pytest.raises(ValueError, match="endpoint binding conflicts"):
        prepare_request(changed_endpoint, _intent())

    add_identity("second", "Second Operator", "second@example.test")
    set_default_identity("second")
    with pytest.raises(ValueError, match="signer conflicts"):
        prepare_request(endpoint, _intent())
    assert saved.operator_key is not None


def test_prepare_request_uses_live_max_age(endpoint: Endpoint) -> None:
    _write_endpoint(endpoint.exchange, endpoint.public_key, request_max_age=900)

    signed = prepare_request(endpoint, _intent())

    assert signed.expires_at - signed.created_at == 900


def test_new_start_uses_live_configuration_digest(endpoint: Endpoint) -> None:
    changed_digest = "c" * 64
    _write_endpoint(endpoint.exchange, endpoint.public_key, configurations={"cpu": changed_digest})

    with pytest.raises(ValueError, match="does not match"):
        prepare_request(endpoint, _intent())
    signed = prepare_request(endpoint, replace(_intent(), configuration_digest=changed_digest))

    assert signed.configuration_digest == changed_digest


def test_prepare_request_refuses_changed_enrollment(endpoint: Endpoint) -> None:
    _write_endpoint(endpoint.exchange, endpoint.public_key, enrollment_id="0" * 32)

    with pytest.raises(ValueError, match="enrollment changed"):
        prepare_request(endpoint, _intent())


def test_reload_cannot_retarget_cached_request_id(endpoint: Endpoint) -> None:
    signed = prepare_request(endpoint, _intent())
    changed_digest = "c" * 64
    _write_endpoint(endpoint.exchange, endpoint.public_key, configurations={"cpu": changed_digest})
    changed_intent = replace(_intent(), configuration_digest=changed_digest)

    # The original intent still replays its exact cached bytes; a request under the new digest is a conflict.
    assert encode_request(prepare_request(endpoint, _intent())) == encode_request(signed)
    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_request(endpoint, changed_intent)

    cache = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID / f"{REQUEST_ID}.json"
    assert json.loads(cache.read_bytes())["request"] == json.loads(encode_request(signed))


def test_new_start_requires_approved_configuration(endpoint: Endpoint) -> None:
    unknown = replace(_intent(), request_id="f" * 32, profile="other")

    with pytest.raises(ValueError, match="not approved"):
        prepare_request(endpoint, unknown)


def test_prepare_request_refuses_partial_and_corrupt_cache(endpoint: Endpoint) -> None:
    prepare_request(endpoint, _intent())
    cache_root = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID
    request_path = cache_root / f"{REQUEST_ID}.json"
    envelope = request_path.read_bytes()
    request_path.write_bytes(envelope[: len(envelope) // 2])
    with pytest.raises(ValueError, match="corrupt"):
        prepare_request(endpoint, _intent())

    request_path.write_bytes(envelope + b"\n")
    with pytest.raises(ValueError, match="canonical"):
        prepare_request(endpoint, _intent())


def test_failed_publication_leaves_no_cache_and_retry_signs_fresh(
    endpoint: Endpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    original_sign = client_module.sign_request
    original_link = client_module.os.link

    def counted(request: Request, *, lifetime: int = 3600) -> Request:
        nonlocal calls
        calls += 1
        return original_sign(request, now=3_000 + calls, lifetime=lifetime)

    def fail_link(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(client_module, "sign_request", counted)
    monkeypatch.setattr(client_module.os, "link", fail_link)
    with pytest.raises(OSError, match="publication failure"):
        prepare_request(endpoint, _intent())
    cache = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID / f"{REQUEST_ID}.json"
    assert not cache.exists()

    monkeypatch.setattr(client_module.os, "link", original_link)
    retried = prepare_request(endpoint, _intent())
    assert calls == 2
    assert retried.created_at == 3_002


def test_fsync_failure_after_install_reuses_exact_request(endpoint: Endpoint, monkeypatch: pytest.MonkeyPatch) -> None:
    signed: list[Request] = []
    original_sign = client_module.sign_request
    original_fsync = client_module.os.fsync
    fsync_calls = 0

    def captured(request: Request, *, lifetime: int = 3600) -> Request:
        result = original_sign(request, now=4_000, lifetime=lifetime)
        signed.append(result)
        return result

    def fail_directory_fsync(descriptor: int) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 2:
            raise OSError("injected directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(client_module, "sign_request", captured)
    monkeypatch.setattr(client_module.os, "fsync", fail_directory_fsync)
    with pytest.raises(OSError, match="directory fsync failure"):
        prepare_request(endpoint, _intent())

    monkeypatch.setattr(client_module.os, "fsync", original_fsync)
    retried = prepare_request(endpoint, _intent())
    assert len(signed) == 1
    assert encode_request(retried) == encode_request(signed[0])


def test_concurrent_prepare_has_one_writer_and_no_overwrite(
    endpoint: Endpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    calls_lock = threading.Lock()
    original = client_module.sign_request

    def delayed(request: Request, *, lifetime: int = 3600) -> Request:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return original(request, now=2_000, lifetime=lifetime)

    monkeypatch.setattr(client_module, "sign_request", delayed)
    results: list[Request] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            results.append(prepare_request(endpoint, _intent()))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert len(errors) == 3
    assert all(isinstance(error, RuntimeError) and "already active" in str(error) for error in errors)
    assert calls == 1
    assert len(results) == 1
    assert prepare_request(endpoint, _intent()) == results[0]


def test_active_request_lock_fails_immediately_without_publication(endpoint: Endpoint) -> None:
    signed = prepare_request(endpoint, _intent())

    with client_module._request_lock(endpoint, signed.request_id):
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="already active"):
            client_module.exchange(endpoint, signed, wait_seconds=120)
        elapsed = time.monotonic() - started

    assert elapsed < 1
    assert not list(endpoint.requests.iterdir())


def test_response_authentication_precedes_binding_and_cleanup(endpoint: Endpoint, tmp_path: Path) -> None:
    request = prepare_request(endpoint, _intent())
    unsigned = Response(
        request.request_id,
        request.workspace_id,
        request.enrollment_id,
        request_digest(request),
        "submitted",
        handle="a" * 32,
    )
    signed = sign_response(unsigned, seed_path=tmp_path / "response.seed")

    assert decode_matching_response(encode_response(signed), request, public_key=endpoint.public_key) == signed
    with pytest.raises(ValueError, match="signature"):
        decode_matching_response(
            encode_response(replace(signed, handle="b" * 32)),
            request,
            public_key=endpoint.public_key,
        )
    other_seed = _seed(tmp_path / "wrong.seed", 10)
    wrong_key = identity_public_key(other_seed)
    assert wrong_key is not None
    with pytest.raises(ValueError, match="pinned"):
        decode_matching_response(encode_response(signed), request, public_key=wrong_key)
