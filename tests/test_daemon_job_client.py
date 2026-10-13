"""Typed remote job commands through the mounted daemon."""

import argparse
import base64
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import OperatorIdentity, add_identity, identity_public_key

from httk.workflow._daemon_auth import sign_response
from httk.workflow._daemon_client import Endpoint
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_protocol import Request, Response, decode_request, encode_response, request_digest
from httk.workflow.monitor import actions as monitor_actions
from httk.workflow.monitor.data import WorkspaceView
from httk.workflow.registry import WorkspaceBinding
from httk.workflow.workflow_cli import _daemon_jobs, command
from httk.workflow.workflow_cli import _job as job_cli

WORKSPACE_ID = "12345678-1234-4234-8234-123456789abc"
ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"
ACTIVE_DIGEST = "a" * 64
JOB_ID = "12345678-1234-1234-1234-123456789abc"
INTERNAL_ID = "12345678-1234-1234-1234-123456789abd"


def _seed(path: Path, byte: int) -> Path:
    path.write_text(base64.b64encode(bytes([byte]) * 32).decode("ascii") + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


@pytest.fixture
def remote(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Endpoint, OperatorIdentity, Path]:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / "data"))
    add_identity("operator", "Operator", "operator@example.test")
    exchange = tmp_path / "exchange"
    for name in ("requests", "responses", "inbox", "outbox", "managers"):
        (exchange / name).mkdir(parents=True)
    (exchange / "exchange.json").write_text(
        json.dumps({"format": "httk-workspace-exchange", "format_version": 1, "workspace_id": WORKSPACE_ID}),
        encoding="utf-8",
    )
    response_seed = _seed(tmp_path / "response.seed", 8)
    public_key = identity_public_key(response_seed)
    assert public_key is not None
    (exchange / "daemon.json").write_text(
        json.dumps(
            {
                "format": "httk-workspace-daemon",
                "format_version": 2,
                "protocol_version": 5,
                "workspace_id": WORKSPACE_ID,
                "enrollment_id": ENROLLMENT_ID,
                "daemon_public_key": public_key,
                "configuration_digest": ACTIVE_DIGEST,
                "configurations": {},
                "request_max_age": 3600,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    bundle = tmp_path / "remote"
    bundle.mkdir()
    (bundle / "remote.json").write_text(
        json.dumps(
            {
                "kind": "mount-daemon",
                "settings": {
                    "exchange": str(exchange),
                    "daemon_workspace_id": WORKSPACE_ID,
                    "daemon_enrollment_id": ENROLLMENT_ID,
                    "daemon_public_key": public_key,
                },
            }
        ),
        encoding="utf-8",
    )
    from httk.core.identity import configured_operator_identity

    selected = configured_operator_identity()
    assert selected is not None
    monkeypatch.setattr(
        _daemon_jobs,
        "resolve_remote",
        lambda _name, **_kwargs: SimpleNamespace(name="cluster", bundle=bundle),
    )
    return bundle, Endpoint(exchange, WORKSPACE_ID, ENROLLMENT_ID, public_key), selected, response_seed


def _response(request: Request, outcome: str, seed_path: Path, *, state: str | None = None) -> Response:
    fields: dict[str, object] = {
        "command_id": request.request_id if request.operation == "job_request" else request.command_id
    }
    if outcome in {"applied", "queued"}:
        fields.update(job_id=JOB_ID, internal_request_id=INTERNAL_ID)
    if state is not None:
        fields["current_state"] = state
    if outcome == "refused":
        fields.update(reason="request_refused", detail="operator request refused")
    elif outcome == "uncertain":
        fields.update(reason="execution_uncertain", detail="execution remains uncertain")
    return sign_response(
        Response(
            request.request_id,
            request.workspace_id,
            request.enrollment_id,
            request_digest(request),
            outcome,
            **cast(Any, fields),
        ),
        seed_path=seed_path,
    )


def _mock_adapter(
    monkeypatch: pytest.MonkeyPatch,
    seed_path: Path,
    outcomes: list[tuple[str, str | None]],
) -> list[Request]:
    seen: list[Request] = []

    def run_adapter(_bundle: Path, operation: str, payload: dict[str, object], *, timeout: float | None):
        assert operation == "daemon"
        value = json.dumps(payload["daemon_request"], sort_keys=True, separators=(",", ":")).encode("ascii")
        request = decode_request(value)
        seen.append(request)
        outcome, state = outcomes.pop(0)
        response = _response(request, outcome, seed_path, state=state)
        return {
            "returncode": 1 if outcome in {"refused", "uncertain"} else 0,
            "stdout": encode_response(response).decode("ascii"),
            "stderr": "",
        }

    monkeypatch.setattr(_daemon_jobs, "run_adapter", run_adapter)
    return seen


def _arguments(**changes: object) -> argparse.Namespace:
    fields: dict[str, object] = {
        "action": "cancel",
        "job_id": [JOB_ID],
        "request_id": None,
        "force": False,
        "wait": False,
        "timeout": None,
        "adapter_timeout": None,
        "reason": "operator requested cancellation",
        "priority": None,
        "step": None,
        "destination": None,
    }
    fields.update(changes)
    return argparse.Namespace(**fields)


def test_remote_batch_sends_one_typed_command_per_selector(remote, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, _endpoint, identity, response_seed = remote
    selectors = [JOB_ID, "12345678-1234-1234"]
    seen = _mock_adapter(monkeypatch, response_seed, [("queued", "running"), ("applied", "paused")])

    status, stdout = _daemon_jobs.request_remote_jobs(
        WorkspaceBinding("cluster:runs", "cluster", None),
        CLIContext("httk", bundle.parent),
        _arguments(job_id=selectors),
        identity,
    )

    assert status == 0
    assert len(seen) == 2
    assert all(request.operation == "job_request" for request in seen)
    assert [request.selector for request in seen] == selectors
    assert all(request.configuration_digest == ACTIVE_DIGEST for request in seen)
    assert all(request.operator_key == identity_public_key(identity.seed_path) for request in seen)
    rows = [line.split("\t") for line in stdout.splitlines()]
    assert len(rows) == 2 and all(len(row) == 6 for row in rows)
    assert [row[3] for row in rows] == [request.request_id for request in seen]


def test_force_and_non_uuid_selectors_are_refused_before_publication(remote, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, _endpoint, identity, response_seed = remote
    seen = _mock_adapter(monkeypatch, response_seed, [])
    binding = WorkspaceBinding("cluster:runs", "cluster", None)
    context = CLIContext("httk", bundle.parent)

    with pytest.raises(ValueError, match="--force"):
        _daemon_jobs.request_remote_jobs(binding, context, _arguments(force=True), identity)
    with pytest.raises(ValueError, match="invalid selector"):
        _daemon_jobs.request_remote_jobs(binding, context, _arguments(job_id=["latest"]), identity)
    assert seen == []


def test_changed_reason_retry_is_invalid_cli_input_before_any_publish(
    remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bundle, _endpoint, identity, response_seed = remote
    command_id = "d" * 32
    _mock_adapter(monkeypatch, response_seed, [("queued", "running")])
    binding = WorkspaceBinding("cluster:runs", "cluster", None)
    context = CLIContext("httk", bundle.parent)
    first = _arguments(request_id=command_id)
    status, _stdout = _daemon_jobs.request_remote_jobs(binding, context, first, identity)
    assert status == 0

    calls: list[str] = []

    def unexpected_dispatch(*_args: object, **_kwargs: object) -> dict[str, object]:
        calls.append("published")
        raise RuntimeError("unexpected adapter call")

    monkeypatch.setattr(_daemon_jobs, "run_adapter", unexpected_dispatch)
    monkeypatch.setattr(job_cli, "_resolve_binding", lambda *_args, **_kwargs: (binding, None))
    monkeypatch.setattr(job_cli, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle=bundle))

    result = command(
        [
            "job",
            "request",
            "cancel",
            "--workspace",
            "cluster:runs",
            "--reason",
            "changed reason",
            "--request-id",
            command_id,
            JOB_ID,
        ],
        context,
    )

    assert result == 2
    assert "cached daemon request intent conflicts" in capsys.readouterr().err
    assert calls == []


def test_pause_wait_polls_with_fresh_signed_command_status(remote, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, _endpoint, identity, response_seed = remote
    seen = _mock_adapter(monkeypatch, response_seed, [("queued", "running"), ("applied", "paused")])
    sleeps: list[float] = []
    monkeypatch.setattr(_daemon_jobs.time, "sleep", sleeps.append)
    arguments = _arguments(action="pause", wait=True, timeout=10.0, reason="pause for review")

    status, stdout = _daemon_jobs.request_remote_jobs(
        WorkspaceBinding("cluster:runs", "cluster", None), CLIContext("httk", bundle.parent), arguments, identity
    )

    assert status == 0 and len(sleeps) == 1
    original, observation = seen
    assert original.operation == "job_request" and original.action == "pause"
    assert observation.operation == "job_request_status"
    assert observation.command_id == original.request_id
    assert observation.request_id != original.request_id
    assert stdout.split("\t")[1] == "applied"


@pytest.mark.parametrize(
    ("outcome", "state"),
    [("refused", "running"), ("refused", "paused"), ("uncertain", "paused"), ("applied", "succeeded")],
)
def test_pause_wait_reports_refusal_or_supersession(
    remote, monkeypatch: pytest.MonkeyPatch, outcome: str, state: str
) -> None:
    bundle, _endpoint, identity, response_seed = remote
    seen = _mock_adapter(monkeypatch, response_seed, [("queued", "running"), (outcome, state)])
    monkeypatch.setattr(_daemon_jobs.time, "sleep", lambda _seconds: None)

    status, stdout = _daemon_jobs.request_remote_jobs(
        WorkspaceBinding("cluster:runs", "cluster", None),
        CLIContext("httk", bundle.parent),
        _arguments(action="pause", wait=True, timeout=10.0, reason="pause for review"),
        identity,
    )

    assert status == 1
    expected = "refused" if outcome == "applied" else outcome
    assert stdout.split("\t")[1] == expected
    assert len(seen) == 2 and seen[1].command_id == seen[0].request_id


def test_pause_wait_timeout_bounds_the_status_polls(remote, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, _endpoint, identity, response_seed = remote
    seen = _mock_adapter(monkeypatch, response_seed, [("queued", "running")])

    status, stdout = _daemon_jobs.request_remote_jobs(
        WorkspaceBinding("cluster:runs", "cluster", None),
        CLIContext("httk", bundle.parent),
        _arguments(action="pause", wait=True, timeout=0.0, reason="pause for review"),
        identity,
    )

    assert status == 1 and len(seen) == 1
    assert stdout.split("\t")[1:5:3] == ["refused", "pause request timed out and remains pending"]


def test_monitor_mount_daemon_request_uses_complete_argument_namespace(remote, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, _endpoint, identity, response_seed = remote
    _mock_adapter(monkeypatch, response_seed, [("queued", "running")])
    monkeypatch.setattr(monitor_actions, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle=bundle))
    monkeypatch.setattr(monitor_actions, "_resolve_request_identity", lambda _selector: identity)
    view = WorkspaceView(
        WorkspaceBinding("cluster:runs", "cluster", None),
        CLIContext("httk", bundle.parent),
    )

    result = monitor_actions.request(view, "cancel", [JOB_ID], "cancel through monitor")

    assert result.startswith(f"{JOB_ID}\tqueued\t{INTERNAL_ID}\t")


def test_real_mounted_exchange_runs_signed_command_through_adapter(remote) -> None:
    bundle, endpoint, identity, response_seed = remote
    remote_document = {
        "adapter_version": 2,
        "format": "httk-computer-adapter",
        "format_version": 2,
        "kind": "mount-daemon",
        "settings": {
            "exchange": str(endpoint.exchange),
            "daemon_workspace_id": WORKSPACE_ID,
            "daemon_enrollment_id": ENROLLMENT_ID,
            "daemon_public_key": endpoint.public_key,
        },
        "required_binaries": [],
        "timeout_seconds": 15,
    }
    (bundle / "remote.json").write_text(json.dumps(remote_document), encoding="utf-8")
    adapter = bundle / "adapter"
    adapter.write_text('#!/bin/sh\nexec python3 -m httk.workflow._daemon_adapter "$@"\n', encoding="utf-8")
    adapter.chmod(0o755)
    errors: list[BaseException] = []
    received: list[Request] = []

    def broker() -> None:
        try:
            deadline = time.monotonic() + 15
            with MailboxDirectory(endpoint.requests) as requests:
                while time.monotonic() < deadline:
                    names = requests.names()
                    if not names:
                        time.sleep(0.005)
                        continue
                    request = decode_request(requests.read(names[0]))
                    received.append(request)
                    assert request.operation == "job_request"
                    response = sign_response(
                        Response(
                            request.request_id,
                            request.workspace_id,
                            request.enrollment_id,
                            request_digest(request),
                            "queued",
                            job_id=JOB_ID,
                            internal_request_id=INTERNAL_ID,
                            command_id=request.request_id,
                            current_state="running",
                        ),
                        seed_path=response_seed,
                    )
                    with MailboxDirectory(endpoint.responses) as responses:
                        responses.replace(names[0], encode_response(response))
                    requests.remove(names[0])
                    return
            raise TimeoutError("test broker did not receive a request")
        except BaseException as exc:  # pragma: no cover - asserted in caller
            errors.append(exc)

    thread = threading.Thread(target=broker)
    thread.start()
    status, stdout = _daemon_jobs.request_remote_jobs(
        WorkspaceBinding("cluster:runs", "cluster", None),
        CLIContext("httk", bundle.parent),
        _arguments(action="pause", reason="integration test", adapter_timeout=10),
        identity,
    )
    thread.join(timeout=15)

    assert not thread.is_alive()
    assert errors == []
    assert len(received) == 1
    assert received[0].operator_key == identity_public_key(identity.seed_path)
    assert status == 0
    row = stdout.rstrip("\n").split("\t")
    assert (row[0], row[1], row[2], row[3], row[5]) == (
        JOB_ID,
        "queued",
        INTERNAL_ID,
        received[0].request_id,
        "running",
    )
