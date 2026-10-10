"""Workspace configuration edits preserve policy defaults, format sections, and seals."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow.adapters import REMOTE_WORKSPACE_SETTINGS_COMMAND
from httk.workflow.errors import SealedError
from httk.workflow.models import WorkspacePolicy
from httk.workflow.registry import WorkspaceBinding, create_workspace
from httk.workflow.workflow_cli import _common, _workspace, command
from httk.workflow.workspace import Workspace


def test_configuration_round_trip_and_invalid_batch_preserves_document(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "configure", "ws", "--set", "app.command=run", "--set", "app.count=2"], context) == 0
    capsys.readouterr()
    workspace = Workspace(tmp_path / "ws")
    path = workspace.control / "format.json"
    before = path.read_bytes()
    assert command(["workspace", "configure", "ws", "--unset", "app.count", "--set", "bad key=2"], context) == 1
    assert path.read_bytes() == before
    capsys.readouterr()
    assert (
        command(["workspace", "configure", "ws", "--unset", "app.command", "--set", "app.count=3", "--json"], context)
        == 0
    )
    assert json.loads(capsys.readouterr().out) == [{"app.count": 3}]
    assert command(["workspace", "show", "ws", "--json"], context) == 0
    assert json.loads(capsys.readouterr().out) == [{"app.count": 3}]
    assert command(["workspace", "configure", "ws", "--unset", "app.count"], context) == 0
    document = json.loads(path.read_bytes())
    assert document["settings"] == {}
    assert "policy" in document and "workflow_preludes" in document


def test_policy_unset_restores_defaults_and_rejects_unknown_keys(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"lease_seconds": 72, "retention": {"owner_tombstone_days": 7, "trash_days": 9}})
    for key in ("lease_seconds", "retention.owner_tombstone_days"):
        assert command(["workspace", "policy", "unset", "ws", "--key", key], context) == 0
    policy = Workspace(workspace.root).policy
    assert policy.lease_seconds == WorkspacePolicy().lease_seconds
    assert policy.retention.owner_tombstone_days == WorkspacePolicy().retention.owner_tombstone_days
    assert policy.retention.trash_days == 9
    for key in ("retention.typo", "typo"):
        before = (workspace.control / "format.json").read_bytes()
        assert command(["workspace", "policy", "unset", "ws", "--key", key], context) == 1
        assert (workspace.control / "format.json").read_bytes() == before
    assert command(["workspace", "policy", "unset", "ws", "--key", "retention"], context) == 0
    assert Workspace(workspace.root).policy.retention == WorkspacePolicy().retention


def test_configuration_and_prelude_mutations_obey_workspace_seals(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    workspace.set_setting("app.command", "run")
    workspace.set_workflow_prelude("test.flow", "module load test")
    (workspace.control / "seal.json").write_text("{}")
    before = (workspace.control / "format.json").read_bytes()
    with pytest.raises(SealedError):
        workspace.configure_settings({"app.command": "other"})
    with pytest.raises(SealedError):
        workspace.unset_setting("app.command")
    with pytest.raises(SealedError):
        workspace.set_workflow_prelude("test.flow", "other")
    with pytest.raises(SealedError):
        workspace.unset_workflow_prelude("test.flow")
    assert (workspace.control / "format.json").read_bytes() == before


def test_remote_configuration_uses_existing_settings_commands(tmp_path: Path, monkeypatch, capsys) -> None:
    binding = WorkspaceBinding("remote:ws", "remote", None)
    monkeypatch.setattr(_workspace, "_resolve_binding", lambda *_args: (binding, None))
    calls = []

    def remote_output(_binding, _context, argv, **_kwargs):
        calls.append(argv)
        mode = argv[len(REMOTE_WORKSPACE_SETTINGS_COMMAND)]
        return 0, '[{"app.command": "new"}]' if mode == "show" else '"new"' if mode == "set" else "", ""

    monkeypatch.setattr(_common, "remote_workspace_output", remote_output)
    assert (
        command(
            ["workspace", "configure", "remote:ws", "--unset", "app.old", "--set", "app.command=new", "--json"],
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == [{"app.command": "new"}]
    assert calls == [
        [*REMOTE_WORKSPACE_SETTINGS_COMMAND, "unset", "--key", "app.old", "ws"],
        [*REMOTE_WORKSPACE_SETTINGS_COMMAND, "set", "--key", "app.command", "--value", "new", "ws"],
        [*REMOTE_WORKSPACE_SETTINGS_COMMAND, "show", "--json", "ws"],
    ]
    calls.clear()
    assert (
        command(
            ["workspace", "configure", "remote:ws", "--unset", "app.old", "--set", "bad key=new"],
            CLIContext("httk", tmp_path),
        )
        == 1
    )
    assert not calls
