"""Typed mounted-daemon command and template boundaries."""

import json
import sys
import uuid
from collections.abc import Mapping
from io import StringIO
from pathlib import Path
from typing import cast

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import add_identity, identity_public_key

from httk.workflow._daemon_auth import sign_response
from httk.workflow.adapters import PERSISTABLE_REMOTE_SETTINGS, add_remote
from httk.workflow.projects import PROJECT_DIRECTORY, initialize_project
from httk.workflow.workflow_cli import build_parser, command

WORKSPACE_ID = "12345678-1234-4234-8234-123456789abc"
ENROLLMENT_ID = "a" * 32


def _remote(project: Path) -> Path:
    bundle = add_remote("cluster", template="mount-daemon", project=project)
    path = bundle / "remote.json"
    metadata = json.loads(path.read_text(encoding="utf-8"))
    response_seed = project / "response.seed"
    response_seed.write_text("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=\n", encoding="ascii")
    public_key = identity_public_key(response_seed)
    assert public_key is not None
    metadata["settings"] = {
        "mount_root": "/mnt/workspace/data",
        "daemon_requests": "/mnt/workspace/requests",
        "daemon_responses": "/mnt/workspace/responses",
        "daemon_workspace_id": WORKSPACE_ID,
        "daemon_enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": public_key,
    }
    path.write_text(json.dumps(metadata), encoding="utf-8")
    return bundle


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / "data"))
    add_identity("tester", "Test Operator", "test@example.test")
    root = tmp_path / "project"
    initialize_project(root, name="daemon-cli")
    _remote(root)
    return root


def _invoke(project: Path, args: list[str]) -> tuple[int, str, str]:
    return _command(project, ["remote", "daemon", *args])


def _command(project: Path, args: list[str]) -> tuple[int, str, str]:
    from contextlib import redirect_stderr, redirect_stdout
    from io import StringIO

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        code = command(args, CLIContext("httk", project))
    return code, stdout.getvalue(), stderr.getvalue()


def _observed_daemon_request(observed: Mapping[str, object]) -> Mapping[str, object]:
    """Narrow one adapter observation to its typed daemon request."""

    payload = observed.get("payload")
    assert isinstance(payload, Mapping)
    request = payload.get("daemon_request")
    assert isinstance(request, Mapping)
    return request


def test_template_and_shareable_daemon_settings(project: Path) -> None:
    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    adapter = (bundle / "adapter").read_text(encoding="utf-8")
    assert metadata["kind"] == "mount-daemon"
    assert metadata["adapter_version"] == 2
    assert metadata["timeout_seconds"] == 130
    assert adapter == '#!/bin/sh\nexec python3 -m httk.workflow._daemon_adapter "$@"\n'
    assert {
        "daemon_requests",
        "daemon_responses",
        "daemon_workspace_id",
        "daemon_enrollment_id",
        "daemon_public_key",
        "mount_root",
    } <= PERSISTABLE_REMOTE_SETTINGS


def test_configure_persists_only_successful_shareable_daemon_settings(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _transfer

    settings = {
        "mount_root": "/mnt/w/data",
        "daemon_requests": "/mnt/w/requests",
        "daemon_responses": "/mnt/w/responses",
        "daemon_workspace_id": WORKSPACE_ID,
        "daemon_enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": json.loads(
            (project / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json").read_text(encoding="utf-8")
        )["settings"]["daemon_public_key"],
    }
    monkeypatch.setattr(_transfer, "run_adapter", lambda *_args, **_kwargs: {"returncode": 0, "ok": True})
    args = ["configure", "cluster"]
    for key, value in settings.items():
        args.extend(("--set", f"{key}={value}"))
    code, _stdout, _stderr = _command(project, ["remote", *args])
    assert code == 0
    metadata = json.loads(
        (project / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json").read_text(encoding="utf-8")
    )
    assert metadata["settings"] == settings


def test_credentials_are_merged_for_endpoint_validation(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.adapters import store_credentials
    from httk.workflow.workflow_cli import _daemon_remote as cli

    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    replacement_id = "87654321-4321-4876-8876-cba987654321"
    store_credentials(bundle, {"daemon_workspace_id": replacement_id})
    observed: dict[str, object] = {}

    def run_adapter(_bundle, _operation, payload, *, timeout):
        observed.update(payload=payload)
        raise TimeoutError("stop after endpoint construction")

    monkeypatch.setattr(cli, "run_adapter", run_adapter)
    code, _stdout, stderr = _invoke(project, ["health", "cluster"])
    assert code == 2
    assert _observed_daemon_request(observed)["workspace_id"] == replacement_id
    assert "daemon request ID:" in stderr


def test_cli_leaves_mounted_endpoint_check_to_adapter(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow._daemon_client import Endpoint
    from httk.workflow.workflow_cli import _daemon_remote as cli

    called: list[bool] = []
    monkeypatch.setattr(Endpoint, "check", lambda _self: pytest.fail("CLI must not check mounted roots"))

    def run_adapter(_bundle, operation, payload, *, timeout):
        assert operation == "daemon"
        assert "daemon request ID:" in cast(StringIO, sys.stderr).getvalue()
        called.append(True)
        raise TimeoutError("adapter timeout")

    monkeypatch.setattr(cli, "run_adapter", run_adapter)
    code, stdout, stderr = _invoke(project, ["health", "cluster"])
    assert code == 2
    assert stdout == ""
    assert called == [True]
    assert "daemon request ID:" in stderr


def test_unknown_credential_settings_fail_before_dispatch(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.adapters import store_credentials
    from httk.workflow.workflow_cli import _daemon_remote as cli

    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    store_credentials(bundle, {"exec_command": ""})
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))
    code, stdout, stderr = _invoke(project, ["health", "cluster"])
    assert code == 2
    assert stdout == ""
    assert "settings" in stderr
    assert "daemon request ID:" not in stderr


def test_parser_exposes_only_typed_daemon_controls(project: Path) -> None:
    parser = build_parser("httk workflow", CLIContext("httk", project))
    parsed = parser.parse_args(
        ["remote", "daemon", "start", "cluster", "--profile", "serial", "--request-id", "a" * 32]
    )
    assert parsed.daemon_verb == "start"
    assert parsed.request_id == "a" * 32
    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "daemon", "start", "cluster", "--profile", "serial"])
    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "daemon", "health", "cluster", "--", "true"])


@pytest.mark.parametrize(
    ("verb_args", "outcome", "status"),
    [
        (["health"], "ready", 0),
        (["health"], "refused", 2),
        (["health"], "busy", 2),
        (["start", "--profile", "serial", "--request-id", "1" * 32], "submitted", 0),
        (["start", "--profile", "serial", "--request-id", "1" * 32], "uncertain", 2),
        (["start", "--profile", "serial", "--request-id", "1" * 32], "refused", 2),
        (["status", "--handle", "2" * 32], "status", 0),
        (["status", "--handle", "2" * 32], "busy", 2),
        (["cancel", "--handle", "2" * 32, "--request-id", "1" * 32], "cancel_requested", 0),
        (["cancel", "--handle", "2" * 32, "--request-id", "1" * 32], "refused", 2),
    ],
)
def test_cli_sends_exact_request_and_renders_confirmed_outcomes(
    project: Path, monkeypatch: pytest.MonkeyPatch, verb_args: list[str], outcome: str, status: int
) -> None:
    from httk.workflow import _daemon_protocol as protocol
    from httk.workflow.workflow_cli import _daemon_remote as cli

    observed: dict[str, object] = {}

    def run_adapter(_bundle, operation, payload, *, timeout):
        observed["stderr_before_call"] = cast(StringIO, sys.stderr).getvalue()
        observed["operation"] = operation
        observed["payload"] = payload
        observed["timeout"] = timeout
        request = protocol.decode_request(json.dumps(payload["daemon_request"]).encode("utf-8"))
        handle = "2" * 32 if outcome in {"submitted", "status", "cancel_requested", "uncertain"} else None
        state = "PENDING" if outcome == "status" else None
        reason = "busy" if outcome == "busy" else "policy" if outcome in {"refused", "uncertain"} else None
        response = sign_response(
            protocol.Response(
                request.request_id,
                WORKSPACE_ID,
                ENROLLMENT_ID,
                protocol.request_digest(request),
                outcome,
                handle=handle,
                scheduler_state=state,
                reason=reason,
            ),
            seed_path=project / "response.seed",
        )
        encoded = protocol.encode_response(response).decode("ascii")
        return {
            "returncode": 0 if status == 0 else 2,
            "stdout": encoded,
            "stderr": "",
        }

    monkeypatch.setattr(cli, "run_adapter", run_adapter)
    code, stdout, stderr = _invoke(project, [*verb_args, "cluster"])
    assert code == status
    assert json.loads(stdout)["outcome"] == outcome
    stderr_before_call = observed["stderr_before_call"]
    assert isinstance(stderr_before_call, str)
    assert "daemon request ID:" in stderr_before_call
    payload_observed = observed["payload"]
    assert isinstance(payload_observed, Mapping)
    assert "daemon_request" in payload_observed
    assert observed["operation"] == "daemon"
    timeout = observed["timeout"]
    wait_seconds = payload_observed["wait_seconds"]
    assert isinstance(timeout, (int, float))
    assert isinstance(wait_seconds, (int, float))
    assert timeout == wait_seconds + 5
    if verb_args[0] in {"start", "cancel"}:
        expected_id = verb_args[verb_args.index("--request-id") + 1]
        assert expected_id in stderr_before_call
    assert stderr == stderr_before_call


def test_wrong_kind_refuses_before_request_id_output_or_adapter_call(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    metadata_path = bundle / "remote.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["kind"] = "mount"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))
    code, stdout, stderr = _invoke(project, ["health", "cluster", "--request-id", "1" * 32])
    assert code == 2
    assert stdout == ""
    assert "not a mount-daemon remote" in stderr
    assert "daemon request ID:" not in stderr


@pytest.mark.parametrize("wait", ["nan", "inf", "0.049", "120.1"])
def test_invalid_wait_is_rejected_before_adapter_call(
    project: Path, monkeypatch: pytest.MonkeyPatch, wait: str
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))
    code, stdout, stderr = _invoke(project, ["health", "cluster", "--wait-seconds", wait])
    assert code == 2
    assert stdout == ""
    assert "finite and between 0.05 and 120" in stderr


def test_adapter_failure_preserves_request_id_retry_guidance(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("late")))
    request_id = "f" * 32
    code, stdout, stderr = _invoke(
        project,
        ["start", "cluster", "--profile", "serial", "--request-id", request_id],
    )
    assert code == 2
    assert stdout == ""
    assert request_id in stderr
    assert "reuse this request ID with identical fields" in stderr


def test_random_read_request_id_is_canonical(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    seen: list[str] = []

    def failed(_bundle, _operation, payload, *, timeout):
        seen.append(payload["daemon_request"]["request_id"])
        raise TimeoutError("offline")

    monkeypatch.setattr(cli, "run_adapter", failed)
    code, _stdout, _stderr = _invoke(project, ["health", "cluster"])
    assert code == 2
    assert len(seen) == 1 and uuid.UUID(hex=seen[0]).hex == seen[0]


def test_cli_rejects_mismatched_response_identity(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import _daemon_protocol as protocol
    from httk.workflow.workflow_cli import _daemon_remote as cli

    def run_adapter(_bundle, _operation, payload, *, timeout):
        request = protocol.decode_request(json.dumps(payload["daemon_request"]).encode("utf-8"))
        response = sign_response(
            protocol.Response(
                "0" * 32,
                request.workspace_id,
                request.enrollment_id,
                protocol.request_digest(request),
                "ready",
            ),
            seed_path=project / "response.seed",
        )
        return {"returncode": 0, "stdout": protocol.encode_response(response).decode("ascii"), "stderr": ""}

    monkeypatch.setattr(cli, "run_adapter", run_adapter)
    code, stdout, stderr = _invoke(project, ["health", "cluster"])
    assert code == 2
    assert stdout == ""
    assert "has no acknowledged result" in stderr
    assert "reuse this request ID with identical fields" in stderr
