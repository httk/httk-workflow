"""Project-recorded defaults and the per-user fallback."""

import json
from pathlib import Path

from httk.core.cli import CLIContext
from httk.core.project import initialize_project, write_project_section

from httk.workflow import Workspace
from httk.workflow.configuration import data_home
from httk.workflow.registry import create_workspace, default_workspace, resolve_workspace
from httk.workflow.workflow_cli import command


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


def test_workspace_default_records_and_unsets_a_project_name(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="cli-project")
    named = tmp_path / "named"
    create_workspace("named", named)
    context = _context(project)

    assert command(["workspace", "default", "named"], context) == 0
    assert capsys.readouterr().out.strip() == "named"
    assert default_workspace(project=project).path == str(named.resolve())
    assert command(["workspace", "default"], context) == 0
    assert capsys.readouterr().out.strip() == "named"

    assert command(["workspace", "default", "--unset"], context) == 0
    assert command(["workspace", "default"], context) == 0
    assert capsys.readouterr().out.strip() == "none recorded; the per-user default applies"
    assert default_workspace(project=project).path == str((data_home() / "workspace").resolve())


def test_workspace_default_requires_a_project(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "default"], _context(tmp_path)) == 2
    assert "project" in capsys.readouterr().err


def test_omitted_workspace_uses_the_global_default_outside_a_project(tmp_path: Path, capsys) -> None:
    assert command(["job", "list"], _context(tmp_path)) == 0
    capsys.readouterr()
    binding = resolve_workspace("default")
    assert binding.path is not None
    assert Path(binding.path) == data_home() / "workspace"


def test_workspace_default_class_still_uses_the_per_user_workspace() -> None:
    workspace = Workspace.default()
    assert workspace.root == data_home() / "workspace"


def test_workspace_discover_walks_from_nested_directories_and_files(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    nested = workspace.root / "project" / "jobs"
    nested.mkdir(parents=True)
    marker_file = nested / "input.json"
    marker_file.write_text("{}", encoding="utf-8")

    assert Workspace.discover(nested) == workspace.root.resolve()
    assert Workspace.discover(marker_file) == workspace.root.resolve()
    assert Workspace.discover(tmp_path / "outside") is None


def test_enclosing_workspace_wins_over_project_and_registry_defaults(tmp_path: Path, capsys) -> None:
    enclosing = Workspace.initialize(tmp_path / "enclosing")
    project = enclosing.root / "project"
    initialize_project(project, name="defaults")
    project_default = tmp_path / "project-default"
    create_workspace("project-default", project_default)
    create_workspace("default", tmp_path / "registry-default")
    write_project_section(project, "workspace", {"default": "project-default"})
    nested = project / "subdir"
    nested.mkdir()

    assert command(["workspace", "status", "--json"], _context(nested)) == 0
    status = json.loads(capsys.readouterr().out)
    assert status[0]["root"] == str(enclosing.root)


def test_by_path_requires_an_explicit_path(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "status", "--by-path"], _context(tmp_path)) == 2
    assert "--by-path requires an explicit path" in capsys.readouterr().err


def test_settings_show_uses_explicit_key_option(tmp_path: Path, capsys) -> None:
    create_workspace("named", tmp_path / "named", settings={"named.value": 7})
    assert command(["workspace", "settings", "show", "named"], _context(tmp_path)) == 0
    assert "named.value" in capsys.readouterr().out
    default = default_workspace(project=tmp_path)
    assert default.path is not None
    Workspace(default.path).set_setting("answer", 42)
    assert command(["workspace", "settings", "show", "--key", "answer"], _context(tmp_path)) == 0
    assert capsys.readouterr().out.strip() == "42"


def test_mutating_workspace_commands_resolve_the_enclosing_workspace(tmp_path: Path, capsys) -> None:
    enclosing = Workspace.initialize(tmp_path / "enclosing")
    nested = enclosing.root / "subdir"
    nested.mkdir()

    assert command(["workspace", "settings", "set", "--key", "vasp.command", "--value", "mock"], _context(nested)) == 0
    capsys.readouterr()
    assert command(["workspace", "settings", "show", "--json"], _context(nested)) == 0
    settings = json.loads(capsys.readouterr().out)
    assert settings[0]["vasp.command"] == "mock"
    assert (
        command(["workspace", "policy", "set", "--key", "retention.journal_days", "--value", "keep"], _context(nested))
        == 0
    )
    capsys.readouterr()
    assert Workspace(enclosing.root).policy.retention.journal_days is None
