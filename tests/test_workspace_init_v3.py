"""Format v3 workspace init: a workspace is a directory of its own."""

from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace
from httk.workflow.errors import FormatError
from httk.workflow.projects import initialize_project, read_project_section, write_project_section
from httk.workflow.workflow_cli import command


def _init(project: Path, *arguments: str) -> int:
    return command(["workspace", "init", *arguments], CLIContext("httk", project))


def test_initialize_refuses_a_non_empty_directory_and_a_project_root(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="p")
    with pytest.raises(FormatError, match="directory of its own"):
        Workspace.initialize(project)
    stray = tmp_path / "stray"
    stray.mkdir()
    (stray / "file.txt").write_text("x", encoding="utf-8")
    with pytest.raises(FormatError, match="not empty"):
        Workspace.initialize(stray)
    assert not (stray / ".httk-workspace").exists()


def test_initialize_accepts_an_empty_or_absent_directory(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    assert Workspace.initialize(tmp_path / "empty").root == (tmp_path / "empty").resolve()
    assert (Workspace.initialize(tmp_path / "absent" / "deep").root / "jobs").is_dir()


def test_cli_init_adopts_an_existing_workspace(tmp_path: Path) -> None:
    Workspace.initialize(tmp_path / "existing")
    assert _init(tmp_path, "--name", "adopted", "existing") == 0
    assert command(["workspace", "status", "adopted"], CLIContext("httk", tmp_path)) == 0


def test_cli_init_inside_a_project_records_the_default_once(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="p")
    assert _init(project, "--name", "first", "workspace") == 0
    assert "default workspace" in capsys.readouterr().out
    assert read_project_section(project, "workspace")["default"] == "first"
    assert _init(project, "--name", "second", "other") == 0
    assert "default workspace" not in capsys.readouterr().out
    assert read_project_section(project, "workspace")["default"] == "first"


def test_cli_init_leaves_an_existing_default_alone(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="p")
    write_project_section(project, "workspace", {"default": "elsewhere"})
    assert _init(project, "--name", "mine", "workspace") == 0
    assert read_project_section(project, "workspace")["default"] == "elsewhere"


def test_a_workspace_that_is_a_project_root_is_refused(tmp_path: Path) -> None:
    from httk.workflow.project_member import WorkspaceMemberHandler
    from httk.workflow.registry import register_workspace

    workspace = Workspace.initialize(tmp_path / "ws")
    initialize_project(workspace.root, name="inside")  # a project made inside an existing workspace
    with pytest.raises(ValueError, match="project root"):
        register_workspace("inside", workspace.root)
    for relpath in (".", ""):
        with pytest.raises(ValueError, match="project root"):
            WorkspaceMemberHandler().manifest_exclusions(workspace.root, relpath)


def test_repair_with_adopt_reports_a_project_root_workspace_without_raising(tmp_path: Path) -> None:
    from httk.core.project.cli import project_repair

    workspace = Workspace.initialize(tmp_path / "ws")
    initialize_project(workspace.root, name="inside")
    report = project_repair(workspace.root, apply=True, adopt=True)
    findings = report["findings"]
    assert isinstance(findings, list)
    members = next(f for f in findings if f["check"] == "workspace_members")
    assert members["status"] == "error" and "project root" in members["message"]
