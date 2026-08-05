"""The machine-owned v2 workspace registry."""

import json
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core.project import initialize_project, write_project_section

from httk.workflow import Workspace
from httk.workflow.adapters import add_remote
from httk.workflow.configuration import data_home, set_config_key
from httk.workflow.registry import (
    LOCAL_REMOTE,
    WorkspaceBinding,
    create_workspace,
    default_workspace,
    delete_workspace,
    forget_workspace,
    list_workspaces,
    register_workspace,
    resolve_workspace,
    valid_workspace_name,
    workspaces_path,
)


def test_register_forget_and_list_use_one_absolute_local_registry(tmp_path: Path) -> None:
    path = tmp_path / "runs"
    binding = register_workspace("home", path)
    assert binding == WorkspaceBinding("home", LOCAL_REMOTE, str(path.resolve()))
    assert list_workspaces() == [binding]
    assert resolve_workspace("home") == binding
    assert forget_workspace("home") == binding
    with pytest.raises(ValueError, match="unknown workspace: home"):
        resolve_workspace("home")


def test_forget_refuses_unretired_outbound_transfer(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "runs")
    binding = register_workspace("home", workspace.root)
    ledger = workspace.control / "transfers" / "transfer.json"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(json.dumps({"status": "sealed"}), encoding="utf-8")
    with pytest.raises(ValueError, match="fetch or retire.*workspace forget --force"):
        forget_workspace("home")
    assert resolve_workspace("home") == binding
    forget_workspace("home", force=True)


def test_register_workspace_rejects_the_retired_remote_positional_shape(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        cast(Any, register_workspace)("home", "local", tmp_path / "runs")


def test_registry_rejects_v1_files_with_a_teaching_error() -> None:
    path = workspaces_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "format": "httk-workspaces",
                "format_version": 1,
                "workspaces": {"runs": {"remote": "cluster", "path": "/tmp/runs"}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"predates machine-owned.*remove it.*workspace init") as error:
        list_workspaces()
    assert str(path) in str(error.value)


def test_machine_name_resolves_a_qualified_reference_locally(tmp_path: Path) -> None:
    set_config_key("machine_names", " kappa, kappa.hpc.example.org ")
    local = register_workspace("runs", tmp_path / "runs")
    assert resolve_workspace("kappa:runs") == local


def test_remote_reference_is_pathless_and_validates_the_remote(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="registry")
    add_remote("cluster", template="local", project=project)
    binding = resolve_workspace("cluster:runs", project=project)
    assert binding == WorkspaceBinding("cluster:runs", "cluster", None)


@pytest.mark.parametrize("name", ["", "a/b", ".", "..", "runs/ws", "a:b"])
def test_invalid_workspace_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="invalid workspace name"):
        valid_workspace_name(name)


def test_default_workspace_uses_project_record_then_global_default(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="defaults")
    chosen = tmp_path / "chosen"
    Workspace.initialize(chosen)
    register_workspace("chosen", chosen)
    write_project_section(project, "workspace", {"default": "chosen"})
    assert default_workspace(project=project).path == str(chosen.resolve())

    write_project_section(project, "workspace", {})
    global_default = default_workspace(project=tmp_path)
    assert default_workspace(project=project) == global_default


def test_default_workspace_lazily_uses_data_home(tmp_path: Path) -> None:
    binding = default_workspace(project=tmp_path)
    assert binding == WorkspaceBinding("default", "local", str((data_home() / "workspace").resolve()))
    assert binding.path is not None
    assert Path(binding.path).joinpath(".httk-workflow", "format.json").is_file()


def test_create_and_delete_are_local_only(tmp_path: Path) -> None:
    binding = create_workspace("home", tmp_path / "runs", settings={"answer": 42})
    assert binding.path is not None
    assert Workspace(binding.path).settings["answer"] == 42
    with pytest.raises(ValueError, match="requires force"):
        delete_workspace("home")
    delete_workspace("home", force=True)
    assert not (tmp_path / "runs").exists()
