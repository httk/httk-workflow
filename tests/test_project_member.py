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
from httk.workflow.registry import (
    adopt_workspace,
    create_workspace,
    delete_workspace,
    move_project_member,
    resolve_workspace,
)
from httk.workflow.seals import is_project_sealed, seal_job, seal_workspace
from httk.workflow.workflow_cli import command


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


def test_project_doctor_recovers_a_deleted_members_registry(tmp_path: Path) -> None:
    # CASE B: the workspace is on disk but members.json is gone entirely, so the
    # registry is empty; scan_project must still surface and re-register it.
    from httk.core.project.members import members_path

    project = tmp_path / "project"
    initialize_project(project, name="member")
    Workspace.initialize(project)  # the root itself is the workspace, member "."
    members_path(project).unlink()
    assert project_members(project) == ()

    report = project_doctor(project)
    unregistered = [
        finding
        for finding in report["findings"]  # type: ignore[union-attr]
        if finding["check"] == "workspace_members" and finding["status"] == "error"
    ]
    assert unregistered and "." in str(unregistered[0]["details"]["workspaces"])

    project_doctor(project, repair=True)
    assert [member.path for member in project_members(project)] == ["."]
    # A second run is clean: the workspace is registered again.
    clean = project_doctor(project)
    assert all(
        finding["status"] == "ok"
        for finding in clean["findings"]  # type: ignore[union-attr]
        if finding["check"] == "workspace_members"
    )


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


def _fresh_home(tmp_path: Path, monkeypatch, suffix: str) -> None:
    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path / f"cfg-{suffix}"))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path / f"data-{suffix}"))


def _copied_named_project(tmp_path: Path, monkeypatch) -> Path:
    """Build a project with a named workspace, then copy it to a fresh machine."""
    import shutil

    _fresh_home(tmp_path, monkeypatch, "origin")
    origin = tmp_path / "origin-proj"
    initialize_project(origin, name="p")
    create_workspace("myws", origin / "work")
    assert [(m.path, m.name) for m in project_members(origin)] == [("work", "myws")]

    _fresh_home(tmp_path, monkeypatch, "fresh")  # empty registry on the new machine
    copied = tmp_path / "copied-proj"
    shutil.copytree(origin, copied)
    return copied


def test_workspace_init_with_name_records_it_in_members(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="p")
    create_workspace("named", project / "work")
    assert [(m.path, m.name) for m in project_members(project)] == [("work", "named")]


def test_copied_workspace_resolves_by_name_without_writing_the_registry(tmp_path: Path, monkeypatch) -> None:
    copied = _copied_named_project(tmp_path, monkeypatch)
    from httk.workflow.registry import _read_global

    binding = resolve_workspace("myws", project=copied)
    assert binding.path is not None
    assert Path(binding.path).resolve() == (copied / "work").resolve()
    assert _read_global() == {}  # resolution never wrote the central registry


def test_adopt_registers_the_copied_workspace_under_its_recorded_name(tmp_path: Path, monkeypatch) -> None:
    copied = _copied_named_project(tmp_path, monkeypatch)
    context = CLIContext("httk", copied / "work")
    assert command(["workspace", "adopt"], context) == 0
    binding = resolve_workspace("myws")
    assert binding.path is not None
    assert Path(binding.path).resolve() == (copied / "work").resolve()


def test_project_adopt_adopts_every_member(tmp_path: Path, monkeypatch) -> None:
    copied = _copied_named_project(tmp_path, monkeypatch)
    context = CLIContext("httk", copied)
    assert project_command(["adopt"], context) == 0
    adopted = resolve_workspace("myws")
    assert adopted.path is not None
    assert Path(adopted.path).resolve() == (copied / "work").resolve()


def test_adopt_refuses_a_name_already_registered_to_a_different_path(tmp_path: Path, monkeypatch) -> None:
    copied = _copied_named_project(tmp_path, monkeypatch)
    # "myws" already points elsewhere on this machine.
    create_workspace("myws", tmp_path / "other")
    findings = adopt_workspace(copied / "work")
    registry = [f for f in findings if f["check"] == "registry"]
    assert registry and registry[0]["status"] == "error"
    assert "different path" in str(registry[0]["message"])


def test_project_doctor_repair_leaves_workspace_default_clean(tmp_path: Path, monkeypatch) -> None:
    from httk.workflow.projects import write_project_section

    copied = _copied_named_project(tmp_path, monkeypatch)
    write_project_section(copied, "workspace", {"default": "myws"})  # default unresolvable centrally yet
    assert int(project_doctor(copied)["problems"]) >= 1  # type: ignore[arg-type]
    project_doctor(copied, repair=True)
    final = project_doctor(copied)
    by_check = {str(f["check"]): f for f in final["findings"]}  # type: ignore[union-attr]
    assert by_check["workspace_default"]["status"] == "ok"
