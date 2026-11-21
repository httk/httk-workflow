"""The ``httk workflow list``/``show``/``install``/``fetch`` commands and the workspace they default to."""

import json
import shutil
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.project import initialize_project, write_project_section

from httk.workflow.configuration import data_home
from httk.workflow.registry import workspaces_path
from httk.workflow.scaffold import workflow_provider
from httk.workflow.workflow_cli import _describe, command
from v3_helpers import install, workspace


def _context(tmp_path: Path) -> CLIContext:
    return CLIContext("httk", tmp_path)


@pytest.mark.usefixtures("relax_workflow")
def test_list_reports_registered_workflows(tmp_path: Path, capsys) -> None:
    assert command(["list", "--available"], _context(tmp_path)) == 0
    lines = capsys.readouterr().out.splitlines()
    relax = next(line for line in lines if line.startswith("tests.relax\t"))
    assert relax == "tests.relax\ttest-relax\tregistered\trelax one test structure"


@pytest.mark.usefixtures("relax_workflow")
def test_list_json_carries_the_source_and_summary(tmp_path: Path, capsys) -> None:
    assert command(["list", "--available", "--json"], _context(tmp_path)) == 0
    rows = json.loads(capsys.readouterr().out)
    by_id = {row["workflow"]: row for row in rows}
    entry = by_id["tests.relax"]
    assert entry["alias"] == "test-relax"
    assert entry["source"] == {"kind": "registered", "plugin": None}
    assert entry["summary"] == "relax one test structure"


@pytest.mark.usefixtures("relax_workflow")
def test_list_marks_a_plugin_workflow_with_its_owner(tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = workflow_provider("tests.relax")
    assert provider is not None
    monkeypatch.setattr(_describe, "registered_workflows", lambda: ("tests.relax",))
    monkeypatch.setattr(_describe, "workflow_provider", lambda _id: provider)
    monkeypatch.setattr(_describe, "installed_plugin_workflows", lambda: {"tests.relax": provider})
    monkeypatch.setattr(_describe, "installed_plugin_workflow_owners", lambda: {"tests.relax": "demo-plugin"})

    assert command(["list", "--available"], _context(tmp_path)) == 0
    assert capsys.readouterr().out.splitlines()[0].split("\t")[2] == "plugin demo-plugin"

    assert command(["list", "--available", "--json"], _context(tmp_path)) == 0
    assert json.loads(capsys.readouterr().out)[0]["source"] == {"kind": "plugin", "plugin": "demo-plugin"}


def test_list_reports_nothing_registered(tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_describe, "registered_workflows", lambda: ())
    assert command(["list", "--available"], _context(tmp_path)) == 0
    assert capsys.readouterr().out == "no workflows registered\n"
    assert command(["list", "--available", "--json"], _context(tmp_path)) == 0
    assert capsys.readouterr().out.strip() == "[]"


# -- the workspace default, like every other command --------------------------------------------------------


def _unresolvable_project(tmp_path: Path) -> Path:
    """A project whose recorded default workspace is not registered, so no workspace resolves from it."""

    project = tmp_path / "project"
    initialize_project(project, name="no-workspace")
    write_project_section(project, "workspace", {"default": "gone"})
    return project


def test_list_lists_the_default_workspace_and_available_the_machine(tmp_path: Path, capsys) -> None:
    ws = workspace(tmp_path / "ws")
    inside = _context(ws.root)
    assert command(["list"], inside) == 0
    assert capsys.readouterr().out == f"no workflows installed in {ws.root.resolve()}\n"
    installed = install(ws, tmp_path / "package")
    assert command(["list", "--json"], inside) == 0
    (row,) = json.loads(capsys.readouterr().out)
    assert (row["id"], row["name"]) == (installed.id, "demo")
    # --available reads this machine, never a workspace store.
    assert command(["list", "--available", "--json"], inside) == 0
    assert installed.id not in {row["workflow"] for row in json.loads(capsys.readouterr().out)}
    assert command(["list", "--available", "-w", str(ws.root)], inside) == 2
    assert "takes no --workspace" in capsys.readouterr().err
    # The old duplicate is gone.
    assert command(["workspace", "workflows"], inside) == 2
    assert "invalid choice" in capsys.readouterr().err


@pytest.mark.usefixtures("relax_workflow")
def test_show_describes_the_installation_else_what_this_machine_resolves(tmp_path: Path, capsys) -> None:
    ws = workspace(tmp_path / "ws")
    install(ws, tmp_path / "package")
    inside = _context(ws.root)
    assert command(["show", "demo"], inside) == 0
    assert capsys.readouterr().out.startswith(f"demo:\ndescribed from: workspace {ws.root.resolve()}\n")
    assert command(["show", "--json", "demo", "tests.relax"], inside) == 0
    installed, machine = json.loads(capsys.readouterr().out)
    assert (installed["described_from"], installed["workspace"]) == ("workspace", str(ws.root.resolve()))
    assert installed["source"]["kind"] == "workspace"
    assert (machine["described_from"], machine["workspace"]) == ("machine", None)
    # An uninstalled package directory is described as this machine resolves it.
    package = tmp_path / "other"
    shutil.copytree(tmp_path / "package", package)
    assert command(["show", "--json", str(package)], inside) == 0
    (directory,) = json.loads(capsys.readouterr().out)
    assert directory["described_from"] == "machine" and directory["source"]["kind"] == "directory"
    # Without a resolvable workspace, every target falls back to this machine.
    assert command(["show", "tests.relax"], _context(_unresolvable_project(tmp_path))) == 0
    assert capsys.readouterr().out.startswith("tests.relax:\ndescribed from: this machine\n")
    # An explicit workspace that does not resolve is an error, and describe is gone.
    assert command(["show", "-w", "gone", "tests.relax"], inside) == 2
    assert "unknown workspace: gone" in capsys.readouterr().err
    assert command(["describe", "tests.relax"], inside) == 2
    assert "invalid choice" in capsys.readouterr().err


def test_list_and_show_never_create_the_per_user_default(tmp_path: Path, capsys) -> None:
    fresh = _context(tmp_path)
    package = tmp_path / "package"
    install(workspace(tmp_path / "scratch"), package)
    assert command(["show", "--json", str(package)], fresh) == 0
    (described,) = json.loads(capsys.readouterr().out)
    assert (described["described_from"], described["workspace"]) == ("machine", None)
    assert command(["list"], fresh) == 2
    error = capsys.readouterr().err
    assert "no workspace to list" in error and "httk workflow list --available" in error
    assert "httk workspace init PATH" in error
    assert not (data_home() / "workspace").exists() and not workspaces_path().exists()
    # An existing per-user default is used; install creates it, as job new does.
    assert command(["install", str(package)], fresh) == 0
    capsys.readouterr()
    assert command(["list", "--json"], fresh) == 0
    assert [row["name"] for row in json.loads(capsys.readouterr().out)] == ["demo"]
    assert command(["show", "demo"], fresh) == 0
    assert capsys.readouterr().out.startswith("demo:\ndescribed from: workspace default\n")


def test_install_without_a_resolvable_workspace_names_fetch_and_init(tmp_path: Path, capsys) -> None:
    assert command(["install", "pkg"], _context(_unresolvable_project(tmp_path))) == 2
    error = capsys.readouterr().err
    assert "no workspace to install into: unknown workspace: gone" in error
    assert "httk workflow fetch URI" in error and "httk workspace init PATH" in error


def test_fetch_takes_only_git_uris(tmp_path: Path, capsys) -> None:
    assert command(["fetch", "--json", "./pkg"], _context(tmp_path)) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == [] and "fetch takes a git+https://" in captured.err
    assert "httk workflow install" in captured.err
    assert command(["forget", "--workspace", "x", "tests.relax"], _context(tmp_path)) == 2
    assert "unrecognized arguments: --workspace" in capsys.readouterr().err
