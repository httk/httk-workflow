"""Remote job list, show, log and why relays through the adapter (split out of ``test_job_paging.py``)."""

import json
import shlex
from pathlib import Path

from httk.core.cli import CLIContext

from conftest import fake_remote, register_ws
from httk.workflow import Workspace
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command
from test_job_paging import _job as _marker


def test_remote_job_list_forwards_every_flag_and_json_is_optional(tmp_path: Path, remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="paged-remote")
    fake_remote(project)
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    _marker(workspace, "ready", "jobs", tag="remote")
    # A cursor names the job directory: <state>:<placement>/<key>~p<NNN>~<token>.
    cursor = f"ready:jobs/{next((workspace.jobs / 'ready' / 'jobs').iterdir()).name}"
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    common = [
        "job",
        "list",
        "--workspace",
        "cluster:station",
        "--kind",
        "ready",
        "--placement",
        "jobs",
        "--after",
        cursor,
        "--tag-contains",
        "remote",
        "--counts",
        "--limit",
        "5",
        "--adapter-timeout",
        "17",
    ]
    assert command(common, context) == 0
    human = capsys.readouterr().out
    assert not human.lstrip().startswith("{")
    assert shlex.split(remote.commands()[-1]) == [
        "httk",
        "job",
        "list",
        "--counts",
        "--kind",
        "ready",
        "--placement",
        "jobs",
        "--after",
        cursor,
        "--limit",
        "5",
        "--tag-contains",
        "remote",
        "--workspace",
        "station",
    ]

    assert command([*common, "--json"], context) == 0
    document = json.loads(capsys.readouterr().out)

    assert document["format"] == "httk-workflow-job-list"
    assert document["jobs"] == []
    assert document["counts"] == {"ready": 1}
    assert shlex.split(remote.commands()[-1]) == [
        "httk",
        "job",
        "list",
        "--json",
        "--counts",
        "--kind",
        "ready",
        "--placement",
        "jobs",
        "--after",
        cursor,
        "--limit",
        "5",
        "--tag-contains",
        "remote",
        "--workspace",
        "station",
    ]


def test_remote_job_detail_commands_relay_and_forward_canonical_ids(tmp_path: Path, remote, capsys) -> None:
    """Remote detail readers use their pinned vectors and require canonical ids."""

    project = tmp_path / "project"
    initialize_project(project, name="paged-remote-details")
    fake_remote(project)
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    job_id = _marker(workspace, "ready", "jobs", tag="remote")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert (
        command(["job", "show", "--workspace", "cluster:station", "--json", "--adapter-timeout", "17", job_id], context)
        == 0
    )
    assert (
        command(
            [
                "job",
                "log",
                "--workspace",
                "cluster:station",
                "--json",
                "--limit",
                "2",
                "--adapter-timeout",
                "17",
                job_id,
            ],
            context,
        )
        == 0
    )
    assert (
        command(["job", "why", "--workspace", "cluster:station", "--json", "--adapter-timeout", "17", job_id], context)
        == 0
    )
    capsys.readouterr()

    commands = remote.commands()
    assert any("httk job show --json" in item and job_id in item for item in commands)
    assert any("httk job log --json --limit 2" in item and job_id in item for item in commands)
    assert any("httk job why --json" in item and job_id in item for item in commands)

    for action in ("show", "log", "why"):
        args = ["job", action, "--workspace", "cluster:station", "remote--" + job_id]
        assert command(args, context) == 2
        assert "canonical job ids" in capsys.readouterr().err
