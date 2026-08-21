"""CLI behavior for repeatable operator requests and pause waiting."""

import json
import time
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace
from httk.workflow.configuration import ensure_identity_key, identity_key_paths, identity_public_key, write_config
from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def _payload(root: Path, tag: str, runner: str = _SUCCEED_RUNNER) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    files = root / tag / "files"
    files.mkdir(parents=True)
    runner_path = files / "runner"
    runner_path.write_text(runner)
    runner_path.chmod(0o755)
    (files.parent / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": tag,
                "name": tag,
                "workflow": "tests.cli_request",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "only",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
                "parent": None,
            }
        )
    )
    return files.parent, job_id


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


@pytest.fixture(autouse=True)
def _configured_default_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "identities": {"tester": {"name": "Test User", "email": "tester@example.test"}},
            "default_identity": "tester",
        }
    )
    ensure_identity_key("tester")


def _new_workspace(tmp_path: Path) -> tuple[Workspace, str]:
    root = tmp_path / "workspace"
    name = f"cli-{uuid.uuid4().hex}"
    create_workspace(name, root)
    return Workspace(root), name


def _request_args(workspace_name: str, *job_ids: str, action: str = "pause") -> list[str]:
    return [
        "job",
        "request",
        workspace_name,
        *job_ids,
        action,
        "--reason",
        "test request",
    ]


def test_repeatable_job_ids_publish_one_request_and_path_each(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    source = tmp_path / "source"
    job_ids = []
    for tag in ("first", "second"):
        payload, job_id = _payload(source, tag)
        workspace.submit(payload, f"project/{tag}")
        job_ids.append(job_id)

    assert command(_request_args(workspace_name, *job_ids), _context(tmp_path)) == 0
    output = capsys.readouterr().out.splitlines()
    assert len(output) == 2
    assert all(Path(line).is_file() for line in output)
    assert len(list((workspace.control / "requests" / "ready").iterdir())) == 2


def test_default_operator_identity_is_recorded_and_signs_request(tmp_path: Path) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "default")
    workspace.submit(payload, "project/default")

    assert command(_request_args(workspace_name, job_id), _context(tmp_path)) == 0
    request = json.loads(next((workspace.control / "requests" / "ready").iterdir()).read_text(encoding="utf-8"))
    assert request["operator"] == "Test User <tester@example.test>"
    assert request["operator_key"] == identity_public_key(identity_key_paths("tester")[0])
    assert "signature" in request


def test_named_operator_identity_selects_its_key(tmp_path: Path) -> None:
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "identities": {
                "tester": {"name": "Test User", "email": "tester@example.test"},
                "bot": {"name": "Build Bot", "email": "bot@example.test"},
            },
            "default_identity": "tester",
        }
    )
    ensure_identity_key("bot")
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "named")
    workspace.submit(payload, "project/named")

    assert command(_request_args(workspace_name, job_id) + ["--operator", "bot"], _context(tmp_path)) == 0
    request = json.loads(next((workspace.control / "requests" / "ready").iterdir()).read_text(encoding="utf-8"))
    assert request["operator"] == "Build Bot <bot@example.test>"
    assert request["operator_key"] == identity_public_key(identity_key_paths("bot")[0])


def test_unknown_operator_identity_publishes_nothing(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "unknown")
    workspace.submit(payload, "project/unknown")

    assert command(_request_args(workspace_name, job_id) + ["--operator", "missing"], _context(tmp_path)) == 2
    assert "configured identities: tester" in capsys.readouterr().err
    assert not list((workspace.control / "requests" / "ready").iterdir())


def test_request_without_any_identity_names_the_config_command(tmp_path: Path, capsys) -> None:
    write_config({"format": "httk-config", "format_version": 2})
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "no-identity")
    workspace.submit(payload, "project/no-identity")

    assert command(_request_args(workspace_name, job_id), _context(tmp_path)) == 2
    assert "config identity add" in capsys.readouterr().err
    assert not list((workspace.control / "requests" / "ready").iterdir())


def test_literal_operator_label_is_passed_through(tmp_path: Path) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "literal")
    workspace.submit(payload, "project/literal")

    assert (
        command(_request_args(workspace_name, job_id) + ["--operator", "Ext Person <ext@x>"], _context(tmp_path)) == 0
    )
    request = json.loads(next((workspace.control / "requests" / "ready").iterdir()).read_text(encoding="utf-8"))
    assert request["operator"] == "Ext Person <ext@x>"


def test_literal_operator_on_empty_machine_publishes_unsigned_request(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "empty-config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "empty-data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    ensure_identity_key()
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "literal-empty")
    workspace.submit(payload, "project/literal-empty")

    assert command(_request_args(workspace_name, job_id) + ["--operator", "Ext <e@x>"], _context(tmp_path)) == 0
    request = json.loads(next((workspace.control / "requests" / "ready").iterdir()).read_text(encoding="utf-8"))
    assert request["operator"] == "Ext <e@x>"
    assert "operator_key" not in request and "signature" not in request


def test_legacy_operator_identity_is_used_without_named_identities(tmp_path: Path) -> None:
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "name": "Legacy User",
            "email": "legacy@example.test",
        }
    )
    ensure_identity_key()
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "legacy")
    workspace.submit(payload, "project/legacy")

    assert command(_request_args(workspace_name, job_id), _context(tmp_path)) == 0
    request = json.loads(next((workspace.control / "requests" / "ready").iterdir()).read_text(encoding="utf-8"))
    assert request["operator"] == "Legacy User <legacy@example.test>"
    assert request["operator_key"] == identity_public_key(identity_key_paths()[0])


def test_wait_returns_zero_when_manager_pauses_jobs(tmp_path: Path, capsys, monkeypatch) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    source = tmp_path / "source"
    job_ids = []
    for tag in ("first", "second"):
        payload, job_id = _payload(source, tag)
        workspace.submit(payload, f"project/{tag}")
        job_ids.append(job_id)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        monkeypatch.setattr(job_cli.time, "sleep", lambda _: manager.tick())
        assert command(_request_args(workspace_name, *job_ids) + ["--wait"], _context(tmp_path)) == 0

    assert "paused" in capsys.readouterr().out


def test_wait_reports_terminal_state_and_exits_one(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "finished")
    workspace.submit(payload, "project/finished")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            manager.tick()
            marker = workspace.find_marker_by_id(job_id)
            if marker is not None and marker.kind == "succeeded":
                break
            time.sleep(0.02)
        else:
            raise AssertionError("job did not succeed")
        assert command(_request_args(workspace_name, job_id) + ["--wait"], _context(tmp_path)) == 1

    assert "succeeded" in capsys.readouterr().out


def test_wait_without_live_manager_fails_after_publishing(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "unserved")
    workspace.submit(payload, "project/unserved")

    assert command(_request_args(workspace_name, job_id) + ["--wait"], _context(tmp_path)) == 1
    captured = capsys.readouterr()
    assert "no live manager currently serves" in captured.err
    assert "waiting is pointless until a manager starts" in captured.err
    assert len(list((workspace.control / "requests" / "ready").iterdir())) == 1


def test_wait_timeout_names_pending_job_and_keeps_request(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "pending")
    workspace.submit(payload, "project/pending")

    with TaskManager(workspace, heartbeat_interval=0.01):
        assert command(_request_args(workspace_name, job_id) + ["--wait", "--timeout", "0.01"], _context(tmp_path)) == 1

    captured = capsys.readouterr()
    assert "timeout" in captured.out and job_id in captured.out
    assert list((workspace.control / "requests" / "ready").iterdir())


def test_wait_is_rejected_for_non_pause_action(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "cancel")
    workspace.submit(payload, "project/cancel")

    assert command(_request_args(workspace_name, job_id, action="cancel") + ["--wait"], _context(tmp_path)) == 2
    assert "--wait is only valid with the pause action" in capsys.readouterr().err
