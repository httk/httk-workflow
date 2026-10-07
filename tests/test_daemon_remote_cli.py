"""Typed mounted-daemon command and template boundaries."""

import json
import os
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
CONFIGURATION_DIGEST = "b" * 64


def _write_exchange(exchange: Path, public_key: str, **changes: object) -> Path:
    """Create (or rewrite the endpoint of) one exchange directory fixture."""

    for name in ("requests", "responses", "inbox", "outbox", "managers"):
        (exchange / name).mkdir(parents=True, exist_ok=True)
    (exchange / "exchange.json").write_text(
        json.dumps({"format": "httk-workspace-exchange", "format_version": 1, "workspace_id": WORKSPACE_ID}),
        encoding="utf-8",
    )
    document: dict[str, object] = {
        "format": "httk-workspace-daemon",
        "format_version": 1,
        "workspace_id": WORKSPACE_ID,
        "enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": public_key,
        "configurations": {"serial": CONFIGURATION_DIGEST},
        "request_max_age": 1800,
    }
    document.update(changes)
    (exchange / "daemon.json").write_text(json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    return exchange


def _remote(project: Path) -> Path:
    bundle = add_remote("cluster", template="mount-daemon", project=project)
    path = bundle / "remote.json"
    metadata = json.loads(path.read_text(encoding="utf-8"))
    response_seed = project / "response.seed"
    response_seed.write_text("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=\n", encoding="ascii")
    public_key = identity_public_key(response_seed)
    assert public_key is not None
    exchange = _write_exchange(project.parent / "exchange", public_key)
    metadata["settings"] = {
        "exchange": str(exchange),
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


def _public_key(project: Path) -> str:
    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    return json.loads((bundle / "remote.json").read_text(encoding="utf-8"))["settings"]["daemon_public_key"]


def _remote_settings(project: Path) -> dict[str, object]:
    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    return json.loads((bundle / "remote.json").read_text(encoding="utf-8"))["settings"]


def _configure_args(exchange: Path | str) -> list[str]:
    """Return CLI arguments for one exchange pin."""

    return ["configure", "cluster", "--exchange", str(exchange)]


def test_template_and_shareable_daemon_settings(project: Path) -> None:
    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    adapter = (bundle / "adapter").read_text(encoding="utf-8")
    assert metadata["kind"] == "mount-daemon"
    assert metadata["adapter_version"] == 2
    assert metadata["timeout_seconds"] == 130
    assert adapter == '#!/bin/sh\nexec python3 -m httk.workflow._daemon_adapter "$@"\n'
    assert {
        "exchange",
        "daemon_workspace_id",
        "daemon_enrollment_id",
        "daemon_public_key",
    } <= PERSISTABLE_REMOTE_SETTINGS


def test_configure_persists_only_successful_shareable_daemon_settings(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _transfer

    settings = {
        "exchange": "/mnt/w/exchange",
        "daemon_workspace_id": WORKSPACE_ID,
        "daemon_enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": _public_key(project),
    }
    monkeypatch.setattr(_transfer, "run_adapter", lambda *_args, **_kwargs: {"returncode": 0, "ok": True})
    args = ["configure", "cluster"]
    for key, value in settings.items():
        args.extend(("--set", f"{key}={value}"))
    code, _stdout, _stderr = _command(project, ["remote", *args])
    assert code == 0
    assert _remote_settings(project) == settings


def test_daemon_configure_pins_identities_from_exchange(project: Path, tmp_path: Path) -> None:
    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))

    code, stdout, stderr = _invoke(project, _configure_args(exchange))

    assert code == 0
    assert json.loads(stdout)["configured"] is True
    assert stderr == ""
    assert _remote_settings(project) == {
        "exchange": str(exchange),
        "daemon_workspace_id": WORKSPACE_ID,
        "daemon_enrollment_id": ENROLLMENT_ID,
        "daemon_public_key": _public_key(project),
    }
    assert sorted(path.name for path in exchange.iterdir()) == [
        "daemon.json",
        "exchange.json",
        "inbox",
        "managers",
        "outbox",
        "requests",
        "responses",
    ]
    assert not list((exchange / "requests").iterdir())


def test_daemon_configure_anchors_relative_exchange_to_cwd(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    monkeypatch.chdir(tmp_path)

    code, _stdout, _stderr = _invoke(project, _configure_args("mounted-exchange"))

    assert code == 0
    assert _remote_settings(project)["exchange"] == str(Path(os.getcwd()) / "mounted-exchange")
    assert exchange.is_dir()


def test_daemon_configure_failure_does_not_change_settings(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    metadata_path = project / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json"
    before = metadata_path.read_bytes()
    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("no")))

    code, stdout, stderr = _invoke(project, _configure_args(exchange))

    assert code == 2
    assert stdout == ""
    assert "no" in stderr
    assert metadata_path.read_bytes() == before


@pytest.mark.parametrize(
    "kind", ["missing_subdirectory", "symlink", "fifo", "oversize", "duplicate", "extra", "missing_field", "format"]
)
def test_daemon_configure_refuses_invalid_exchange(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    path = exchange / "daemon.json"
    text = path.read_text(encoding="utf-8")
    if kind == "missing_subdirectory":
        (exchange / "inbox").rmdir()
    elif kind == "symlink":
        target = tmp_path / "target.json"
        target.write_text(text, encoding="utf-8")
        path.unlink()
        path.symlink_to(target)
    elif kind == "fifo":
        path.unlink()
        os.mkfifo(path)
    elif kind == "oversize":
        path.write_bytes(b" " * (64 * 1024 + 1))
    elif kind == "duplicate":
        path.write_text(text[:-1] + ',"request_max_age":5}', encoding="utf-8")
    elif kind == "extra":
        path.write_text(text[:-1] + ',"private_state":"/secret"}', encoding="utf-8")
    elif kind == "missing_field":
        document = json.loads(text)
        del document["enrollment_id"]
        path.write_text(json.dumps(document), encoding="utf-8")
    else:
        path.write_text(text.replace("httk-workspace-daemon", "wrong"), encoding="utf-8")
    metadata_path = project / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json"
    before = metadata_path.read_bytes()
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))

    code, stdout, _stderr = _invoke(project, _configure_args(exchange))

    assert code == 2
    assert stdout == ""
    assert metadata_path.read_bytes() == before


def test_daemon_configure_refuses_wrong_remote_kind_without_persistence(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    metadata_path = project / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["kind"] = "mount"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    before = metadata_path.read_bytes()
    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))

    code, stdout, stderr = _invoke(project, _configure_args(exchange))

    assert code == 2
    assert stdout == ""
    assert "not a mount-daemon remote" in stderr
    assert metadata_path.read_bytes() == before


def test_identity_change_after_configure_is_refused_before_signing(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    assert _invoke(project, _configure_args(exchange))[0] == 0
    _write_exchange(exchange, _public_key(project), enrollment_id="c" * 32)
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))

    code, stdout, stderr = _invoke(project, ["health", "cluster"])

    assert code == 2
    assert stdout == ""
    assert "enrollment changed" in stderr


def test_start_uses_live_configuration_digest(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    exchange = project.parent / "exchange"
    reloaded = "d" * 64
    _write_exchange(exchange, _public_key(project), configurations={"serial": reloaded})
    observed: dict[str, object] = {}

    def run_adapter(_bundle, _operation, payload, *, timeout):
        observed.update(payload=payload)
        raise TimeoutError("stop after request construction")

    monkeypatch.setattr(cli, "run_adapter", run_adapter)
    code, _stdout, _stderr = _invoke(
        project, ["start", "cluster", "--configuration", "serial", "--request-id", "e" * 32]
    )

    assert code == 2
    assert _observed_daemon_request(observed)["configuration_digest"] == reloaded


def test_start_after_reload_cannot_replay_cached_request_under_new_digest(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    exchange = project.parent / "exchange"
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("stop")))
    args = ["start", "cluster", "--configuration", "serial", "--request-id", "e" * 32]
    assert _invoke(project, args)[0] == 2
    _write_exchange(exchange, _public_key(project), configurations={"serial": "d" * 64})

    code, _stdout, stderr = _invoke(project, args)

    assert code == 2
    assert "intent conflicts" in stderr


def test_credentials_are_merged_for_endpoint_validation(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.adapters import store_credentials
    from httk.workflow.workflow_cli import _daemon_remote as cli

    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    store_credentials(bundle, {"daemon_workspace_id": "87654321-4321-4876-8876-cba987654321"})
    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))

    code, _stdout, stderr = _invoke(project, ["health", "cluster"])

    assert code == 2
    assert "workspace_id" in stderr
    assert "enrollment changed" in stderr


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
        ["remote", "daemon", "start", "cluster", "--configuration", "serial", "--request-id", "a" * 32]
    )
    assert parsed.daemon_verb == "start"
    assert parsed.request_id == "a" * 32
    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "daemon", "start", "cluster", "--configuration", "serial"])
    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "daemon", "start", "cluster", "--profile", "serial", "--request-id", "a" * 32])
    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "daemon", "health", "cluster", "--", "true"])


@pytest.mark.parametrize(
    ("verb_args", "outcome", "status"),
    [
        (["health"], "ready", 0),
        (["health"], "refused", 2),
        (["health"], "busy", 2),
        (["start", "--configuration", "serial", "--request-id", "1" * 32], "submitted", 0),
        (["start", "--configuration", "serial", "--request-id", "1" * 32], "uncertain", 2),
        (["start", "--configuration", "serial", "--request-id", "1" * 32], "refused", 2),
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
    if verb_args[0] == "start":
        daemon_request = payload_observed["daemon_request"]
        assert isinstance(daemon_request, Mapping)
        assert daemon_request["configuration"] == "serial"
        assert daemon_request["configuration_digest"] == CONFIGURATION_DIGEST
        assert "profile" not in daemon_request
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


def test_unknown_configuration_refuses_before_signing_or_adapter_call(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    monkeypatch.setattr(cli, "run_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))
    code, stdout, stderr = _invoke(
        project,
        ["start", "cluster", "--configuration", "unknown", "--request-id", "f" * 32],
    )

    assert code == 2
    assert stdout == ""
    assert "unknown approved daemon configuration 'unknown'" in stderr
    assert "available: serial" in stderr
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
        ["start", "cluster", "--configuration", "serial", "--request-id", request_id],
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


def _passive(project: Path) -> tuple[dict[str, object], Path]:
    settings = _remote_settings(project)
    outbox = Path(str(settings["exchange"]))
    status = {
        "format": "httk-workspace-exchange-status",
        "format_version": 1,
        "workspace_id": WORKSPACE_ID,
        "updated_at": "2026-01-01T00:00:00.000000Z",
        "jobs": [],
        "truncated": False,
    }
    (outbox / "status.json").write_text(json.dumps(status), encoding="utf-8")
    return status, outbox


def test_status_without_handle_prints_the_passive_files(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.workflow_cli import _daemon_remote as cli

    monkeypatch.setattr(cli, "run_adapter", lambda *_a, **_k: pytest.fail("no request may be sent"))
    status, _outbox = _passive(project)
    code, stdout, _stderr = _invoke(project, ["status", "cluster"])
    assert code == 0
    assert json.loads(stdout) == {"status": status, "managers": None}


def test_status_without_handle_refuses_bad_files_and_request_id(project: Path) -> None:
    _status, outbox = _passive(project)
    (outbox / "status.json").write_text("{}", encoding="utf-8")
    code, stdout, stderr = _invoke(project, ["status", "cluster"])
    assert (code, stdout) == (2, "") and "status.json" in stderr
    code, stdout, stderr = _invoke(project, ["status", "cluster", "--request-id", "1" * 32])
    assert (code, stdout) == (2, "") and "--handle" in stderr


def test_log_prints_the_published_bytes_and_refuses_errors(
    project: Path, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    exchange = Path(str(_remote_settings(project)["exchange"]))
    (exchange / "managers" / f"{'2' * 32}.log").write_bytes(b"out\n\xff")
    bundle = project / PROJECT_DIRECTORY / "remotes" / "cluster"
    assert bundle.is_dir()
    code = command(["remote", "daemon", "log", "cluster", "--handle", "2" * 32], CLIContext("httk", project))
    assert code == 0 and capsysbinary.readouterr().out == b"out\n\xff"
    code = command(["remote", "daemon", "log", "cluster", "--handle", "3" * 32], CLIContext("httk", project))
    captured = capsysbinary.readouterr()
    assert code == 2 and captured.out == b"" and b"no log has been published" in captured.err
    code = command(["remote", "daemon", "log", "cluster", "--handle", "zz"], CLIContext("httk", project))
    assert code == 2


def test_status_without_handle_passes_managers_v3_through(project: Path) -> None:
    _status, outbox = _passive(project)
    managers = {
        "format": "httk-workspace-daemon-managers",
        "format_version": 3,
        "enrollment_id": ENROLLMENT_ID,
        "generated_at": "2026-01-01T00:00:00.000000Z",
        "managers": [
            {
                "handle": "2" * 32,
                "profile": "cpu",
                "request_id": "3" * 32,
                "state": "ENDED",
                "job_id": None,
                "scheduler_state": "COMPLETED",
                "exit_code": "0:0",
                "started_at": None,
                "ended_at": None,
                "log": f"managers/{'2' * 32}.log",
            }
        ],
    }
    (outbox / "managers.json").write_text(json.dumps(managers), encoding="utf-8")
    code, stdout, _stderr = _invoke(project, ["status", "cluster"])
    assert code == 0 and json.loads(stdout)["managers"] == managers


def test_daemon_configure_without_daemon_json_pins_the_workspace_only(project: Path, tmp_path: Path) -> None:
    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    (exchange / "daemon.json").unlink()

    code, _stdout, stderr = _invoke(project, _configure_args(exchange))

    assert code == 0 and "pinned the workspace only" in stderr
    assert _remote_settings(project)["daemon_workspace_id"] == WORKSPACE_ID


def test_signed_verbs_without_daemon_pins_say_how_to_pin(project: Path, tmp_path: Path) -> None:
    exchange = _write_exchange(tmp_path / "mounted-exchange", _public_key(project))
    (exchange / "daemon.json").unlink()
    assert _invoke(project, _configure_args(exchange))[0] == 0
    assert "daemon_enrollment_id" not in _remote_settings(project)

    code, stdout, stderr = _invoke(project, ["health", "cluster"])

    assert (code, stdout) == (2, "") and "httk remote daemon configure" in stderr


def test_take_back_verb_copies_the_inbox_entry_out(project: Path, tmp_path: Path) -> None:
    exchange = Path(str(_remote_settings(project)["exchange"]))
    (exchange / "inbox" / "job-1").mkdir()
    (exchange / "inbox" / "job-1" / "f").write_text("x")
    destination = tmp_path / "back"

    code, stdout, _stderr = _invoke(project, ["take-back", "cluster", "job-1", str(destination)])

    assert code == 0 and stdout.strip() == str(destination)
    assert (destination / "f").read_text() == "x"
    assert not list((exchange / "inbox").iterdir())
    code, _stdout, stderr = _invoke(project, ["take-back", "cluster", "job-1", str(tmp_path / "again")])
    assert code == 2 and "manager has taken it" in stderr
