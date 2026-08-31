"""The workflow workspace as a core project member: registration and verbs."""

import json
import uuid
from pathlib import Path

from httk.core.cli import CLIContext
from httk.core.identity import ensure_identity_key
from httk.core.project.cli import command as project_command
from httk.core.project.cli import project_doctor
from httk.core.project.members import project_members, unregister_project_member

from httk.workflow import Workspace
from httk.workflow.projects import initialize_project
from httk.workflow.registry import create_workspace, delete_workspace, move_project_member
from httk.workflow.seals import is_project_sealed, seal_job, seal_workspace


def _payload(root: Path, tag: str) -> Path:
    payload = root / tag
    (payload / "files").mkdir(parents=True)
    (payload / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": str(uuid.uuid4()),
                "tag": tag,
                "name": tag,
                "workflow": "tests.member",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "start",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload


def test_workspace_init_registers_a_project_member(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="member")
    Workspace.initialize(project / "work")
    assert [(member.path, member.kind) for member in project_members(project)] == [("work", "workspace")]


def test_workspace_outside_a_project_registers_nothing(tmp_path: Path) -> None:
    Workspace.initialize(tmp_path / "loose")
    assert project_members(tmp_path) == ()


def test_project_doctor_detects_and_repairs_an_unregistered_workspace(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="member")
    Workspace.initialize(project / "a")
    Workspace.initialize(project / "b")
    unregister_project_member(project, project / "b")  # b now exists but is not a member

    report = project_doctor(project)
    findings = report["findings"]
    assert isinstance(findings, list)
    unregistered = [f for f in findings if f["check"] == "workspace_members" and f["status"] == "error"]
    assert unregistered and "b" in str(unregistered[0]["message"])

    project_doctor(project, repair=True)
    assert "b" in {member.path for member in project_members(project)}


def test_project_seal_end_to_end_via_the_core_cli(tmp_path: Path) -> None:
    ensure_identity_key()
    project = tmp_path / "project"
    initialize_project(project, name="member")
    workspace = Workspace.initialize(project)
    marker = workspace.submit(_payload(tmp_path / "src", "job"), "jobs")
    seal_job(workspace, marker)
    seal_workspace(workspace)

    context = CLIContext("httk", project)
    assert project_command(["seal"], context) == 0
    assert is_project_sealed(project)
    assert project_command(["verify-seal"], context) == 0


def test_delete_workspace_unregisters_the_member(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / "cfg"))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / "data"))
    project = tmp_path / "project"
    initialize_project(project, name="member")
    create_workspace("w", project / "work")
    assert [member.path for member in project_members(project)] == ["work"]
    delete_workspace("w", force=True)
    assert project_members(project) == ()


def test_move_updates_the_member_path(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="member")
    Workspace.initialize(project / "old")
    move_project_member(project / "old", project / "new")
    assert [member.path for member in project_members(project)] == ["new"]

    # A move out of the project unregisters the member.
    move_project_member(project / "new", tmp_path / "outside")
    assert project_members(project) == ()
