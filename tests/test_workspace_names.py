"""Workspace path initialization, machine-owned remote names, and move."""

import errno
import json
import os
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace
from httk.workflow._util import utc_now
from httk.workflow.projects import initialize_project
from httk.workflow.registry import resolve_workspace, workspaces_path
from httk.workflow.workflow_cli import _common as workflow_common
from httk.workflow.workflow_cli import _workspace as workspace_cli
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._transfer import _protocol_workspace


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


def test_plain_init_uses_path_basename_and_explicit_name(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    assert command(["workspace", "init", "runs"], context) == 0
    capsys.readouterr()
    assert (tmp_path / "runs" / ".httk-workspace" / "format.json").is_file()

    deep = tmp_path / "deep" / "dir"
    deep.mkdir(parents=True)
    assert command(["workspace", "init", "--name", "named", str(deep)], context) == 0
    assert command(["workspace", "status", "named"], context) == 0
    capsys.readouterr()


def test_machine_name_init_uses_local_path_remainder(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    assert command(["config", "set", "machine_names", "kappa"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "init", "kappa:runs"], context) == 0
    capsys.readouterr()
    assert (tmp_path / "runs" / ".httk-workspace" / "format.json").is_file()

    binding = resolve_workspace("kappa:runs")
    assert binding.name == "runs"
    assert binding.remote == "local"
    assert binding.path is not None
    assert Path(binding.path) == tmp_path / "runs"
    assert command(["workspace", "status", "kappa:runs"], context) == 0
    capsys.readouterr()


def test_existing_workspace_is_adopted_and_settings_are_refused(tmp_path: Path, capsys) -> None:
    root = tmp_path / "existing"
    Workspace.initialize(root)
    context = _context(tmp_path)
    assert command(["workspace", "init", "--name", "adopted", str(root)], context) == 0
    assert command(["workspace", "init", "--name", "other", "--setting", "answer=1", str(root)], context) == 1
    assert "adopted unchanged" in capsys.readouterr().err


def test_init_refuses_a_second_name_for_the_same_canonical_path(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    assert command(["workspace", "init", "--name", "first", "runs"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "init", "--name", "second", str((tmp_path / "runs").resolve())], context) == 1
    assert "already registered as 'first'" in capsys.readouterr().err


def test_remote_init_sends_path_command_and_far_side_registers_name(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-init")
    fake_remote(project)
    context = _context(project)
    assert command(["workspace", "init", "--name", "runs", "cluster:runs/named"], context) == 0
    capsys.readouterr()
    assert any("workspace init --name runs runs/named" in item for item in remote.commands())
    assert (remote.root / "runs" / "named" / ".httk-workspace" / "format.json").is_file()
    assert command(["workspace", "status", "cluster:runs"], context) == 0
    capsys.readouterr()
    assert any(row["name"] == "runs" for row in _local_rows(context, capsys))


def _local_rows(context: CLIContext, capsys) -> list[dict[str, object]]:
    assert command(["workspace", "list", "--json"], context) == 0
    return json.loads(capsys.readouterr().out)


def test_remote_list_is_prefixed_and_local_list_has_paths(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-list")
    fake_remote(project)
    context = _context(project)
    command(["workspace", "init", "cluster:runs"], context)
    capsys.readouterr()
    command(["workspace", "list", "--json", "cluster:"], context)
    rows = json.loads(capsys.readouterr().out)
    assert any(row["name"] == "cluster:runs" for row in rows)
    assert any("workspace list --json" in item for item in remote.commands())


def test_remote_move_emits_the_pinned_workspace_move_vector(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-move")
    fake_remote(project)
    context = _context(project)
    assert command(["workspace", "init", "cluster:runs"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "move", "cluster:runs", "moved"], context) == 0
    capsys.readouterr()
    assert any("workspace move runs moved" in item for item in remote.commands())


def test_remote_workspace_adapter_requests_are_exact_argv(tmp_path: Path, monkeypatch, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="exact-workspace-argv")
    fake_remote(project)
    context = _context(project)
    captured: list[tuple[str, ...]] = []

    def adapter(_bundle, operation: str, request: dict[str, object], **_kwargs):
        assert operation == "invoke"
        captured.append(tuple(request["argv"]))  # type: ignore[arg-type]
        return {"returncode": 0, "stdout": "[]", "stderr": ""}

    monkeypatch.setattr(workspace_cli, "_run_adapter", adapter)
    monkeypatch.setattr(workflow_common, "_run_adapter", adapter)
    commands = (
        ["workspace", "init", "--name", "runs", "cluster:runs"],
        ["workspace", "status", "--json", "cluster:runs"],
        ["workspace", "fsck", "--json", "cluster:runs"],
        ["workspace", "gc", "--dry-run", "--json", "cluster:runs"],
        ["workspace", "settings", "show", "--json", "cluster:runs"],
        ["workspace", "list", "--json", "cluster:"],
        ["workspace", "move", "cluster:runs", "moved"],
        ["workspace", "delete", "--force", "cluster:runs"],
    )
    for argv in commands:
        assert command(argv, context) == 0
        capsys.readouterr()

    assert captured == [
        ("httk", "workflow", "workspace", "init", "--name", "runs", "runs"),
        ("httk", "workflow", "workspace", "status", "--json", "runs"),
        ("httk", "workflow", "workspace", "fsck", "--json", "runs"),
        ("httk", "workflow", "workspace", "gc", "--dry-run", "--json", "runs"),
        ("httk", "workflow", "workspace", "settings", "show", "--json", "runs"),
        ("httk", "workflow", "workspace", "list", "--json"),
        ("httk", "workflow", "workspace", "move", "runs", "moved"),
        ("httk", "workflow", "workspace", "delete", "--force", "runs"),
    ]


def test_protocol_path_fallback_does_not_hide_a_corrupt_registry(tmp_path: Path) -> None:
    directory = tmp_path / "workspace"
    directory.mkdir()
    path = workspaces_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json", encoding="utf-8")
    with pytest.raises(ValueError, match="cannot read workspace registry"):
        _protocol_workspace(str(directory), _context(tmp_path))


def test_protocol_path_fallback_does_not_hide_a_broken_remote_adapter(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="broken-adapter")
    bundle = fake_remote(project)
    (bundle / "adapter").unlink()
    Workspace.initialize(project / "cluster:runs")

    with pytest.raises(ValueError, match="adapter executable is not runnable"):
        _protocol_workspace("cluster:runs", _context(project))


def test_protocol_accepts_dot_relative_and_colon_paths(tmp_path: Path) -> None:
    root = Workspace.initialize(tmp_path / "workspace")
    child = root.root / "child"
    child.mkdir()
    relative = Workspace.initialize(tmp_path / "relative" / "path")
    colon = Workspace.initialize(tmp_path / "colon:workspace")
    cases = (
        (".", _context(root.root), root.root),
        ("..", _context(child), root.root),
        ("relative/path", _context(tmp_path), relative.root),
        ("colon:workspace", _context(tmp_path), colon.root),
    )
    for value, context, expected in cases:
        assert _protocol_workspace(value, context).root == expected


@pytest.mark.parametrize(
    ("value", "context_root"),
    [(".", "workspace"), ("..", "workspace/child"), ("relative/path", "."), ("colon:workspace", ".")],
)
def test_protocol_path_forms_all_surface_a_corrupt_registry(tmp_path: Path, value: str, context_root: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    (workspace.root / "child").mkdir()
    Workspace.initialize(tmp_path / "relative" / "path")
    Workspace.initialize(tmp_path / "colon:workspace")
    path = workspaces_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json", encoding="utf-8")
    context = _context(workspace.root / context_root)
    with pytest.raises(ValueError, match="cannot read workspace registry"):
        _protocol_workspace(value, context)


def test_remote_delete_requires_force_before_contacting_remote(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-delete")
    fake_remote(project)
    context = _context(project)
    assert command(["workspace", "init", "cluster:runs"], context) == 0
    capsys.readouterr()
    before = remote.commands()

    assert command(["workspace", "delete", "cluster:runs"], context) == 1
    assert "requires --force" in capsys.readouterr().err
    assert remote.commands() == before

    assert command(["workspace", "delete", "--force", "cluster:runs"], context) == 0
    capsys.readouterr()
    assert any("workspace delete --force runs" in item for item in remote.commands())


def test_move_updates_the_registry_and_refuses_a_fresh_manager(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    command(["workspace", "init", "old"], context)
    capsys.readouterr()
    root = tmp_path / "old"
    managers = root / ".httk-workspace" / "managers" / "live"
    managers.mkdir(parents=True)
    (managers / "manager.json").write_text(json.dumps({"manager_id": "live"}), encoding="utf-8")
    (managers / "heartbeat.json").write_text(json.dumps({"updated_at": utc_now()}), encoding="utf-8")
    assert command(["workspace", "move", "old", "new"], context) == 2
    assert "fresh heartbeat" in capsys.readouterr().err
    (managers / "heartbeat.json").unlink()
    assert command(["workspace", "move", "old", "new"], context) == 0
    capsys.readouterr()
    assert _local_rows(context, capsys)[0]["path"] == str(tmp_path / "new")
    assert not (tmp_path / "new" / ".httk-workspace" / "maintenance.lock").exists()
    assert command(["workspace", "fsck", "old"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "gc", "--dry-run", "old"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "move", "old", "newer"], context) == 0
    capsys.readouterr()
    assert not (tmp_path / "newer" / ".httk-workspace" / "maintenance.lock").exists()


def test_move_refuses_cross_filesystem_without_copying(tmp_path: Path, monkeypatch, capsys) -> None:
    context = _context(tmp_path)
    assert command(["workspace", "init", "old"], context) == 0
    capsys.readouterr()
    root = (tmp_path / "old").resolve()
    real_rename = os.rename

    def cross_filesystem(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        if Path(source).resolve() == root:
            raise OSError(errno.EXDEV, "cross-device link")
        real_rename(source, destination)

    monkeypatch.setattr(os, "rename", cross_filesystem)
    assert command(["workspace", "move", "old", "new"], context) == 2
    assert "one filesystem" in capsys.readouterr().err
    assert root.is_dir() and not (tmp_path / "new").exists()


def test_move_holds_the_lock_at_the_destination_until_registry_update(tmp_path: Path, monkeypatch, capsys) -> None:
    context = _context(tmp_path)
    assert command(["workspace", "init", "old"], context) == 0
    capsys.readouterr()
    destination = (tmp_path / "new").resolve()
    observed: list[bool] = []
    real_update = workspace_cli._update_workspace_path

    def observe_update(name: str, path: Path, *, durable: bool = True):
        observed.append((path / ".httk-workspace" / "maintenance.lock").is_file())
        return real_update(name, path, durable=durable)

    monkeypatch.setattr(workspace_cli, "_update_workspace_path", observe_update)
    assert command(["workspace", "move", "old", "new"], context) == 0
    capsys.readouterr()
    assert observed == [True]
    assert not (destination / ".httk-workspace" / "maintenance.lock").exists()
