"""Dedicated mount-daemon adapter envelope and subprocess tests."""

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from httk.core.identity import add_identity, identity_public_key

from httk.workflow._daemon_auth import sign_request, sign_response
from httk.workflow._daemon_client import Endpoint
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_protocol import (
    Request,
    Response,
    decode_request,
    encode_request,
    encode_response,
    request_digest,
)
from httk.workflow.adapters import run_adapter

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"


@pytest.fixture(autouse=True)
def _identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / "data"))
    add_identity("tester", "Test Operator", "test@example.test")


def _endpoint(tmp_path: Path) -> Endpoint:
    workspace = tmp_path / "workspace"
    requests = tmp_path / "requests"
    responses = tmp_path / "responses"
    (workspace / ".httk-workspace").mkdir(parents=True)
    requests.mkdir()
    responses.mkdir()
    (workspace / ".httk-workspace" / "format.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-filesystem",
                "format_version": 2,
                "workspace_id": WORKSPACE_ID,
            }
        ),
        encoding="utf-8",
    )
    response_seed = tmp_path / "response.seed"
    response_seed.write_text("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=\n", encoding="ascii")
    public_key = identity_public_key(response_seed)
    assert public_key is not None
    return Endpoint(workspace, requests, responses, WORKSPACE_ID, ENROLLMENT_ID, public_key)


def _settings(endpoint: Endpoint) -> dict[str, object]:
    return {
        "mount_root": str(endpoint.workspace),
        "daemon_requests": str(endpoint.requests),
        "daemon_responses": str(endpoint.responses),
        "daemon_workspace_id": endpoint.workspace_id,
        "daemon_enrollment_id": endpoint.enrollment_id,
        "daemon_public_key": endpoint.daemon_public_key,
    }


def _bundle(tmp_path: Path, endpoint: Endpoint, *, kind: str = "mount-daemon") -> Path:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "remote.json").write_text(
        json.dumps(
            {
                "adapter_version": 2,
                "format": "httk-computer-adapter",
                "format_version": 2,
                "kind": kind,
                "settings": _settings(endpoint),
                "required_binaries": [],
                "timeout_seconds": 15,
            }
        ),
        encoding="utf-8",
    )
    adapter = bundle / "adapter"
    adapter.write_text("#!/bin/sh\nexec python3 -m httk.workflow._daemon_adapter \"$@\"\n", encoding="utf-8")
    adapter.chmod(0o755)
    return bundle


def _outer_request(bundle: Path, operation: str, **fields: object) -> dict[str, object]:
    return {
        "format": "httk-computer-request",
        "format_version": 2,
        "operation": operation,
        "adapter_dir": str(bundle),
        "remote_settings": json.loads((bundle / "remote.json").read_text(encoding="utf-8"))["settings"],
        **fields,
    }


def _run_direct(tmp_path: Path, request: dict[str, object]) -> subprocess.CompletedProcess[str]:
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-m", "httk.workflow._daemon_adapter", str(path)],
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )


def _broker(endpoint: Endpoint, outcome: str, **fields: str) -> tuple[threading.Thread, list[BaseException]]:
    errors: list[BaseException] = []

    def run() -> None:
        try:
            deadline = time.monotonic() + 15
            with MailboxDirectory(endpoint.requests) as requests:
                while time.monotonic() < deadline:
                    names = requests.names()
                    if names:
                        request = decode_request(requests.read(names[0]))
                        response = sign_response(
                            Response(
                                request.request_id,
                                request.workspace_id,
                                request.enrollment_id,
                                request_digest(request),
                                outcome,
                                **fields,
                            ),
                            seed_path=endpoint.responses.parent / "response.seed",
                        )
                        with MailboxDirectory(endpoint.responses) as responses:
                            responses.replace(names[0], encode_response(response))
                        requests.remove(names[0])
                        return
                    time.sleep(0.005)
            raise TimeoutError("test broker did not receive a request")
        except BaseException as exc:  # pragma: no cover - asserted in the caller
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, errors


def test_configure_merges_known_settings_and_does_not_publish(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"] = {}
    (bundle / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")

    result = run_adapter(bundle, "configure", {"settings": _settings(endpoint)})

    assert result["configured"] is True
    assert not list(endpoint.requests.iterdir())


def test_configure_rejects_unknown_settings(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)

    with pytest.raises(RuntimeError, match="six endpoint settings"):
        run_adapter(bundle, "configure", {"settings": {"exec_command": "touch /tmp/no"}})


def test_install_rejects_pending_settings_before_publication(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)

    with pytest.raises(RuntimeError, match="pending settings"):
        run_adapter(bundle, "install", {"settings": {"mount_root": str(endpoint.workspace)}})
    assert not list(endpoint.requests.iterdir())


def test_install_sends_only_health_and_reports_ready(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    observed: list[str] = []
    errors: list[BaseException] = []

    def health_broker() -> None:
        try:
            deadline = time.monotonic() + 15
            with MailboxDirectory(endpoint.requests) as requests:
                while time.monotonic() < deadline:
                    names = requests.names()
                    if names:
                        request = decode_request(requests.read(names[0]))
                        observed.append(request.operation)
                        response = sign_response(
                            Response(
                                request.request_id,
                                request.workspace_id,
                                request.enrollment_id,
                                request_digest(request),
                                "ready",
                            ),
                            seed_path=endpoint.responses.parent / "response.seed",
                        )
                        with MailboxDirectory(endpoint.responses) as responses:
                            responses.replace(names[0], encode_response(response))
                        requests.remove(names[0])
                        return
                    time.sleep(0.005)
            raise TimeoutError("test broker did not receive a health request")
        except BaseException as exc:  # pragma: no cover - asserted in the caller
            errors.append(exc)

    thread = threading.Thread(target=health_broker)
    thread.start()
    try:
        result = run_adapter(bundle, "install", {"settings": {}}, timeout=15)
    finally:
        thread.join(timeout=15)
    assert not thread.is_alive()
    assert errors == []
    assert observed == ["health"]
    assert result["returncode"] == 0


def test_real_adapter_subprocess_preserves_confirmed_refusal(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    request = sign_request(
        Request("0" * 32, WORKSPACE_ID, "start_manager", profile="cpu", enrollment_id=ENROLLMENT_ID),
        now=1_000_000,
    )
    thread, errors = _broker(endpoint, "refused", reason="policy_refused")
    try:
        result = run_adapter(
            bundle,
            "daemon",
            {"daemon_request": json.loads(encode_request(request)), "wait_seconds": 10},
            timeout=15,
        )
    finally:
        thread.join(timeout=15)

    assert not thread.is_alive()
    assert errors == []
    assert result["ok"] is True
    assert result["returncode"] == 2
    assert json.loads(result["stdout"])["outcome"] == "refused"
    assert result["stderr"] == ""


@pytest.mark.parametrize("operation", ["invoke", "status", "push", "pull"])
def test_generic_operations_refuse_without_mutation(tmp_path: Path, operation: str) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    marker = tmp_path / "must-not-exist"
    request = _outer_request(
        bundle,
        operation,
        argv=[sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
        source=str(tmp_path / "source"),
        destination=str(marker),
    )

    completed = _run_direct(tmp_path, request)

    assert completed.returncode == 0
    assert json.loads(completed.stdout)["ok"] is False
    assert not marker.exists()


@pytest.mark.parametrize("kind", [None, "local", "mount", "changed"])
def test_missing_or_changed_kind_cannot_fall_back_to_local_execution(tmp_path: Path, kind: str | None) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    if kind is None:
        del metadata["kind"]
    else:
        metadata["kind"] = kind
    (bundle / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    marker = tmp_path / "executed"
    request = _outer_request(
        bundle,
        "invoke",
        argv=[sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
    )

    completed = _run_direct(tmp_path, request)

    assert completed.returncode == 0
    assert json.loads(completed.stdout)["ok"] is False
    assert not marker.exists()


@pytest.mark.parametrize("field", ["argv", "cwd", "environment", "unknown"])
def test_daemon_operation_rejects_extra_envelope_fields(tmp_path: Path, field: str) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    daemon_request = Request("1" * 32, WORKSPACE_ID, "health", enrollment_id=ENROLLMENT_ID)
    request = _outer_request(bundle, "daemon", daemon_request=json.loads(encode_request(daemon_request)))
    request[field] = ["true"] if field == "argv" else "/tmp"

    completed = _run_direct(tmp_path, request)

    assert completed.returncode == 2
    assert "invalid mount-daemon adapter request fields" in completed.stderr
    assert not list(endpoint.requests.iterdir())


def test_daemon_operation_rejects_unknown_typed_request_fields(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    daemon_request = json.loads(encode_request(Request("2" * 32, WORKSPACE_ID, "health", enrollment_id=ENROLLMENT_ID)))
    daemon_request["argv"] = ["touch", str(tmp_path / "executed")]

    completed = _run_direct(tmp_path, _outer_request(bundle, "daemon", daemon_request=daemon_request))

    assert completed.returncode == 2
    assert not (tmp_path / "executed").exists()


def test_daemon_operation_bounds_huge_wait_without_publication_or_traceback(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    daemon_request = Request("3" * 32, WORKSPACE_ID, "health", enrollment_id=ENROLLMENT_ID)
    request = _outer_request(
        bundle,
        "daemon",
        daemon_request=json.loads(encode_request(daemon_request)),
        wait_seconds=10**400,
    )

    completed = _run_direct(tmp_path, request)

    assert completed.returncode == 2
    assert "wait_seconds must be a finite number" in completed.stderr
    assert "Traceback" not in completed.stderr
    assert not list(endpoint.requests.iterdir())


def test_request_file_symlink_fifo_and_oversize_are_refused(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path)
    bundle = _bundle(tmp_path, endpoint)
    valid = _outer_request(bundle, "configure", settings={})
    target = tmp_path / "target.json"
    target.write_text(json.dumps(valid), encoding="utf-8")
    symlink = tmp_path / "symlink.json"
    symlink.symlink_to(target)
    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)
    oversize = tmp_path / "oversize.json"
    oversize.write_bytes(b"{" + b" " * (64 * 1024 + 1))

    for path in (symlink, fifo, oversize):
        completed = subprocess.run(
            [sys.executable, "-m", "httk.workflow._daemon_adapter", str(path)],
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )
        assert completed.returncode == 2
