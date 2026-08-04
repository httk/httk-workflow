"""Optional CLI workspace arguments resolve the reserved default workspace."""

from pathlib import Path

from httk.core.cli import CLIContext
from httk.core.project import initialize_project as initialize_anchor

from httk.workflow import Workspace
from httk.workflow.configuration import data_home
from httk.workflow.registry import create_workspace, default_workspace, resolve_workspace
from httk.workflow.workflow_cli import command


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


def test_omitted_workspace_uses_a_project_default(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_anchor(project, name="cli-project")

    assert command(["job", "list"], _context(project)) == 0
    capsys.readouterr()

    binding = resolve_workspace("default", project=project)
    assert binding.scope == "project"
    assert Path(binding.path) == project
    assert (project / ".httk-workflow" / "format.json").is_file()


def test_omitted_workspace_uses_the_global_default_outside_a_project(tmp_path: Path, capsys) -> None:
    assert command(["job", "list"], _context(tmp_path)) == 0
    capsys.readouterr()

    binding = resolve_workspace("default")
    assert binding.scope == "global"
    assert Path(binding.path) == data_home() / "workspace"
    assert (Path(binding.path) / ".httk-workflow" / "format.json").is_file()


def test_by_path_requires_an_explicit_path(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "status", "--by-path"], _context(tmp_path)) == 2
    assert "--by-path requires an explicit path" in capsys.readouterr().err


def test_an_explicit_registered_name_still_resolves_as_before(tmp_path: Path, capsys) -> None:
    root = tmp_path / "named"
    create_workspace("named", remote="local", path=root)

    assert command(["workspace", "status", "named"], _context(tmp_path)) == 0
    capsys.readouterr()
    assert resolve_workspace("named").path == str(root)
    assert not (data_home() / "workspace").exists()


def test_settings_show_distinguishes_a_name_from_a_setting_key(tmp_path: Path, capsys) -> None:
    named = tmp_path / "named"
    create_workspace("named", remote="local", path=named, settings={"named.value": 7})
    assert command(["workspace", "settings", "show", "named"], _context(tmp_path)) == 0
    assert "named.value" in capsys.readouterr().out

    default = default_workspace(project=tmp_path)
    Workspace(default.path).set_setting("answer", 42)
    assert command(["workspace", "settings", "show", "answer"], _context(tmp_path)) == 0
    assert capsys.readouterr().out.strip() == "42"
