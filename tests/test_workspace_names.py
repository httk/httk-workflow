"""Every workspace-taking CLI command takes a registered name, never a bare path.

The registry is the CLI contract: a command names a workspace the operator has
``init``:ed, and a bare filesystem path is refused with the guidance to register
it first. This sweep drives one job all the way through — init, submit, run,
harvest — addressing the workspace only by name, proves a path is refused, and
checks that a remote-bound name reaches its far side through the adapter.
"""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command

_SUCCEED = """#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("tests.names")


@run.step
def only(a):
    (a.workdir / "done.txt").write_text("ok", encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
"""


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


def _runner(tmp_path: Path) -> Path:
    path = tmp_path / "succeed.py"
    path.write_text(_SUCCEED, encoding="utf-8")
    path.chmod(0o755)
    return path


def test_a_job_runs_end_to_end_addressed_only_by_name(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """init, job new, manager run, and harvest all take the registered name and
    operate on the workspace it resolves to."""

    context = _context(tmp_path)
    runner = _runner(tmp_path)
    root = tmp_path / "runs"

    assert command(["workspace", "init", "sweep", "--path", str(root)], context) == 0
    capsys.readouterr()

    assert (
        command(["job", "new", "sweep", "--template", str(runner), "--step", "only", "--tag", "silicon"], context) == 0
    )
    key = capsys.readouterr().out.strip().split("\t")[0]
    assert key.startswith("silicon--")

    assert command(["manager", "run", "sweep"], context) == 0
    capsys.readouterr()

    assert command(["harvest", "sweep", "--state", "succeeded"], context) == 0
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert len(records) == 1 and records[0]["state"] == "succeeded"

    # The name really resolved to that directory, and that is where the work ran.
    workspace = Workspace(root, mutable=False)
    assert [marker.kind for marker in workspace.scan_markers()] == ["succeeded"]


def test_a_bare_path_is_refused_with_the_registration_guidance(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A path where a name belongs is not a workspace the CLI will touch; it points
    the operator at ``workspace init`` and ``workspace list`` instead."""

    workspace = Workspace.initialize(tmp_path / "unregistered")
    context = _context(tmp_path)

    assert command(["workspace", "status", str(workspace.root)], context) == 2
    error = capsys.readouterr().err
    assert "unknown workspace" in error
    assert "workspace init" in error
    assert "workspace list" in error


def test_status_of_a_remote_bound_name_reaches_the_far_side(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A name bound to a remote is created on that remote and read back over the
    adapter; the client resolves the name to the remote path and the far side sees
    only the path."""

    project = tmp_path / "project"
    initialize_project(project, name="named-remote")
    fake_remote(project, workspace=str(remote.root / "runs" / "station"))
    context = _context(project)
    remote_path = str(remote.root / "runs" / "station")

    assert (
        command(
            ["workspace", "init", "cluster:station", "--path", remote_path, "--scope", "project"],
            context,
        )
        == 0
    )
    capsys.readouterr()
    # The workspace really was created on the far side, through the transport.
    assert (remote.root / "runs" / "station" / ".httk-workflow" / "format.json").is_file()

    assert command(["workspace", "list"], context) == 0
    listing = capsys.readouterr().out
    assert "cluster:station" in listing and "cluster" in listing

    assert command(["workspace", "status", "cluster:station"], context) == 0
    capsys.readouterr()
    assert any("workspace status" in item for item in remote.commands())


def test_remote_init_derives_workspace_root_and_registers_colon_binding(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="derived-remote")
    root = remote.root / "Runs"
    fake_remote(project, workspace_root=str(root))
    context = _context(project)

    assert command(["workspace", "init", "cluster:runs", "--scope", "project"], context) == 0
    capsys.readouterr()
    assert (root / "runs" / ".httk-workflow" / "format.json").is_file()
    assert command(["workspace", "list"], context) == 0
    assert "cluster:runs" in capsys.readouterr().out


def test_plain_init_defaults_to_cwd_name_and_local_colon_is_refused(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    context = _context(tmp_path)
    assert command(["workspace", "init", "plain"], context) == 0
    capsys.readouterr()
    assert (tmp_path / "plain" / ".httk-workflow" / "format.json").is_file()

    assert command(["workspace", "init", "local:x"], context) == 2
    assert "plain NAME" in capsys.readouterr().err


def test_settings_round_trip_on_a_remote_bound_name(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-settings")
    remote_path = str(remote.root / "runs" / "settings")
    fake_remote(project, workspace=remote_path)
    context = _context(project)

    assert (
        command(
            ["workspace", "init", "cluster:station", "--path", remote_path, "--scope", "project"],
            context,
        )
        == 0
    )
    capsys.readouterr()

    assert (
        command(["workspace", "settings", "set", "cluster:station", "vasp.command", "srun -n 8 vasp_std"], context) == 0
    )
    assert json.loads(capsys.readouterr().out) == "srun -n 8 vasp_std"
    assert command(["workspace", "settings", "show", "cluster:station", "vasp.command"], context) == 0
    assert json.loads(capsys.readouterr().out) == "srun -n 8 vasp_std"
    assert command(["workspace", "settings", "unset", "cluster:station", "vasp.command"], context) == 0
    capsys.readouterr()
    assert Workspace(remote_path, mutable=False).settings == {}
    assert any("workspace settings set" in item for item in remote.commands())
    assert any("workspace settings show" in item for item in remote.commands())
    assert any("workspace settings unset" in item for item in remote.commands())
