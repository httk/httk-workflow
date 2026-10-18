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
from httk.core.identity import add_identity, identity_key_paths, identity_public_key, set_default_identity

import httk.workflow._daemon_client as client_module
from httk.workflow._daemon_auth import sign_response
from httk.workflow._daemon_client import Endpoint, decode_matching_response, prepare_job_request, prepare_request
from httk.workflow._daemon_protocol import Request, Response, encode_request, encode_response, request_digest

REQUEST_ID = "0123456789abcdef0123456789abcdef"
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"
CONFIGURATION_DIGEST = "b" * 64
ACTIVE_CONFIGURATION_DIGEST = "a" * 64


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
        "format": "httk-workspace-daemon",
        "format_version": 3,
        "protocol_version": 5,
        "workspace_id": WORKSPACE_ID,
        "enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": public_key,
        "configuration_digest": ACTIVE_CONFIGURATION_DIGEST,
        "configurations": {"cpu": {"digest": CONFIGURATION_DIGEST, "settings": {}}},
        "request_max_age": 3600,
    }
    document.update(changes)
    (exchange / "daemon.json").write_text(json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8")


def _intent() -> Request:
    return Request(
        REQUEST_ID,
        WORKSPACE_ID,
        "start_manager",
        profile="cpu",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )


def _job_intent(**changes: object) -> Request:
    fields: dict[str, object] = {
        "request_id": "1" * 32,
        "workspace_id": WORKSPACE_ID,
        "operation": "job_request",
        "enrollment_id": ENROLLMENT_ID,
        "configuration_digest": ACTIVE_CONFIGURATION_DIGEST,
        "selector": "12345678-1234-1234-1234-123456789abc",
        "action": "cancel",
        "reason": "operator requested cancellation",
    }
    fields.update(changes)
    return Request(**fields)  # type: ignore[arg-type]


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

    def counted(request: Request, *, seed_path: Path | None = None, lifetime: int = 3600) -> Request:
        nonlocal calls
        calls += 1
        return original(request, seed_path=seed_path, now=1_000 + calls * 100, lifetime=lifetime)

    monkeypatch.setattr(client_module, "sign_request", counted)
    first = prepare_request(endpoint, _intent())
    second = prepare_request(endpoint, _intent())

    assert calls == 1
    assert encode_request(first) == encode_request(second)
    assert (first.created_at, first.expires_at) == (1_100, 4_700)
    cache = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID / REQUEST_ID / "record"
    envelope = json.loads(cache.read_bytes())
    assert envelope["request"] == json.loads(encode_request(first))
    assert envelope["binding"]["daemon_public_key"] == endpoint.public_key


def test_prepare_request_retry_is_exact_across_processes(endpoint: Endpoint, tmp_path: Path) -> None:
    settings_path = tmp_path / "daemon.json"
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
    _write_endpoint(endpoint.exchange, endpoint.require_daemon()[1], request_max_age=900)

    signed = prepare_request(endpoint, _intent())

    assert signed.expires_at - signed.created_at == 900


def test_prepare_request_can_use_selected_operator_seed(endpoint: Endpoint) -> None:
    add_identity("second", "Second Operator", "second@example.test")
    seed_path, _ = identity_key_paths("second")

    signed = prepare_request(endpoint, _job_intent(), seed_path=seed_path)

    assert signed.operator_key == identity_public_key(seed_path)


def test_new_start_uses_live_configuration_digest(endpoint: Endpoint) -> None:
    changed_digest = "c" * 64
    _write_endpoint(
        endpoint.exchange,
        endpoint.require_daemon()[1],
        configurations={"cpu": {"digest": changed_digest, "settings": {}}},
    )

    with pytest.raises(ValueError, match="does not match"):
        prepare_request(endpoint, _intent())
    signed = prepare_request(endpoint, replace(_intent(), configuration_digest=changed_digest))

    assert signed.configuration_digest == changed_digest


def test_prepare_request_refuses_changed_enrollment(endpoint: Endpoint) -> None:
    _write_endpoint(endpoint.exchange, endpoint.require_daemon()[1], enrollment_id="0" * 32)

    with pytest.raises(ValueError, match="enrollment changed"):
        prepare_request(endpoint, _intent())


def test_reload_cannot_retarget_cached_request_id(endpoint: Endpoint) -> None:
    signed = prepare_request(endpoint, _intent())
    changed_digest = "c" * 64
    _write_endpoint(
        endpoint.exchange,
        endpoint.require_daemon()[1],
        configurations={"cpu": {"digest": changed_digest, "settings": {}}},
    )
    changed_intent = replace(_intent(), configuration_digest=changed_digest)

    # The original intent still replays its exact cached bytes; a request under the new digest is a conflict.
    assert encode_request(prepare_request(endpoint, _intent())) == encode_request(signed)
    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_request(endpoint, changed_intent)

    cache = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID / REQUEST_ID / "record"
    assert json.loads(cache.read_bytes())["request"] == json.loads(encode_request(signed))


def test_new_start_requires_approved_configuration(endpoint: Endpoint) -> None:
    unknown = replace(_intent(), request_id="f" * 32, profile="other")

    with pytest.raises(ValueError, match="not approved"):
        prepare_request(endpoint, unknown)


def test_job_retry_keeps_cached_configuration_snapshot_when_active_changes(endpoint: Endpoint) -> None:
    saved = prepare_job_request(endpoint, _job_intent())
    changed_digest = "c" * 64
    _write_endpoint(endpoint.exchange, endpoint.require_daemon()[1], configuration_digest=changed_digest)

    retried = prepare_job_request(endpoint, _job_intent(configuration_digest=changed_digest))

    assert encode_request(retried) == encode_request(saved)
    assert retried.configuration_digest == ACTIVE_CONFIGURATION_DIGEST


@pytest.mark.parametrize(
    "change",
    [
        {"selector": "12345678-1234-1234-1234-123456789abd"},
        {"action": "pause"},
        {"reason": "a different reason"},
        {"priority": 3, "action": "set_priority"},
        {"step": "relax", "action": "override_step"},
        {"destination": "outbox", "action": "eject"},
    ],
)
def test_job_retry_refuses_changed_semantic_intent(endpoint: Endpoint, change: dict[str, object]) -> None:
    prepare_job_request(endpoint, _job_intent())

    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_job_request(endpoint, _job_intent(**change))


def test_job_request_builder_rejects_explicitly_changed_configuration_pin(endpoint: Endpoint) -> None:
    prepare_request(endpoint, _job_intent())

    with pytest.raises(ValueError, match="intent conflicts"):
        prepare_request(endpoint, _job_intent(configuration_digest="c" * 64))


def test_prepare_request_refuses_partial_and_corrupt_cache(endpoint: Endpoint) -> None:
    prepare_request(endpoint, _intent())
    cache_root = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID
    request_path = cache_root / REQUEST_ID / "record"
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
    original_rename = os.rename

    def counted(request: Request, *, seed_path: Path | None = None, lifetime: int = 3600) -> Request:
        nonlocal calls
        calls += 1
        return original_sign(request, seed_path=seed_path, now=3_000 + calls, lifetime=lifetime)

    def fail_publication(src: object, dst: object, **kwargs: object) -> None:
        if str(dst).endswith(REQUEST_ID):
            raise OSError("injected publication failure")
        original_rename(src, dst, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(client_module, "sign_request", counted)
    monkeypatch.setattr(os, "rename", fail_publication)
    with pytest.raises(OSError, match="could not be published"):
        prepare_request(endpoint, _intent())
    cache_root = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID
    assert list(cache_root.iterdir()) == []

    monkeypatch.setattr(os, "rename", original_rename)
    retried = prepare_request(endpoint, _intent())
    assert calls == 2
    assert retried.created_at == 3_002


def test_fsync_failure_after_install_reuses_exact_request(endpoint: Endpoint, monkeypatch: pytest.MonkeyPatch) -> None:
    signed: list[Request] = []
    original_sign = client_module.sign_request
    original_fsync = client_module.os.fsync
    fsync_calls = 0

    def captured(request: Request, *, seed_path: Path | None = None, lifetime: int = 3600) -> Request:
        result = original_sign(request, seed_path=seed_path, now=4_000, lifetime=lifetime)
        signed.append(result)
        return result

    cache_root = Path(os.environ["HTTK_DATA_HOME"]) / "daemon-requests" / ENROLLMENT_ID

    def fail_directory_fsync(descriptor: int) -> None:
        # The cache directory is first synchronized right after the entry was published into it.
        nonlocal fsync_calls
        if os.readlink(f"/proc/self/fd/{descriptor}") == str(cache_root):
            fsync_calls += 1
            if fsync_calls == 1:
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


def test_concurrent_prepare_converges_on_one_cached_request(
    endpoint: Endpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(4)
    stamps = iter(range(2_000, 2_100))
    stamp_lock = threading.Lock()
    original = client_module.sign_request

    def delayed(request: Request, *, seed_path: Path | None = None, lifetime: int = 3600) -> Request:
        barrier.wait(timeout=5)  # every preparer has missed the cache before any installs
        with stamp_lock:
            now = next(stamps)
        return original(request, seed_path=seed_path, now=now, lifetime=lifetime)

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
        thread.join(timeout=10)

    assert errors == []
    assert len(results) == 4
    assert len({encode_request(result) for result in results}) == 1
    assert len(list(client_module._cache_directory(endpoint.require_daemon()[0]).glob(REQUEST_ID))) == 1
    assert prepare_request(endpoint, _intent()) == results[0]


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

    assert decode_matching_response(encode_response(signed), request, public_key=endpoint.require_daemon()[1]) == signed
    with pytest.raises(ValueError, match="signature"):
        decode_matching_response(
            encode_response(replace(signed, handle="b" * 32)),
            request,
            public_key=endpoint.require_daemon()[1],
        )
    other_seed = _seed(tmp_path / "wrong.seed", 10)
    wrong_key = identity_public_key(other_seed)
    assert wrong_key is not None
    with pytest.raises(ValueError, match="pinned"):
        decode_matching_response(encode_response(signed), request, public_key=wrong_key)


@pytest.mark.parametrize("operation", ["job_request", "job_request_status"])
def test_job_response_must_name_the_original_command(endpoint: Endpoint, tmp_path: Path, operation: str) -> None:
    original_id = "1" * 32
    request = (
        _job_intent(request_id=original_id)
        if operation == "job_request"
        else Request(
            "2" * 32,
            WORKSPACE_ID,
            "job_request_status",
            enrollment_id=ENROLLMENT_ID,
            command_id=original_id,
        )
    )
    response = sign_response(
        Response(
            request.request_id,
            request.workspace_id,
            request.enrollment_id,
            request_digest(request),
            "queued",
            job_id="12345678-1234-1234-1234-123456789abc",
            internal_request_id="12345678-1234-1234-1234-123456789abd",
            command_id="3" * 32,
        ),
        seed_path=tmp_path / "response.seed",
    )

    with pytest.raises(ValueError, match="command_id"):
        decode_matching_response(encode_response(response), request, public_key=endpoint.require_daemon()[1])
