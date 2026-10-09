"""Campaigns: a thin partition map over many registered workspaces.

A campaign spreads a project's work across many ordinary workspaces without a new
scheduler or graph. These tests pin the convention: a root job is assigned to a
partition by policy, everything it spawns inherits its workspace, and collect and
management simply cross the partition list. Nothing here is engine sharding — a
partition is just a name pointing at a registered workspace a manager serves and
a collect reads exactly as any other.
"""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

import v3_helpers as v3
from conftest import Remote, fake_remote
from httk.workflow import Workspace
from httk.workflow.campaigns import (
    assign_partition,
    campaign_collect,
    campaign_managers,
    read_campaign,
    write_campaign,
)
from httk.workflow.introspection import iter_jobs
from httk.workflow.projects import initialize_project
from httk.workflow.registry import register_workspace
from httk.workflow.workflow_cli import command

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("campaign-manager-timing")]


def _run_one(tmp_path: Path, workspace: Workspace, tag: str, *, manifest: str = "") -> None:
    """Install a workflow into one partition workspace and run one job of it to success."""

    installed = v3.install(workspace, tmp_path / f"package-{tag}", manifest=manifest)
    v3.submit(workspace, installed, {"start": "succeed"}, tag=tag)
    v3.run(workspace)


def _campaign_project(tmp_path: Path, assignment: str) -> tuple[Path, dict[str, Workspace]]:
    """A project with two local partitions, ``north`` and ``south``."""

    root = tmp_path / "project"
    initialize_project(root, name="campaign")
    workspaces: dict[str, Workspace] = {}
    for partition in ("north", "south"):
        workspace = Workspace.initialize(tmp_path / partition)
        register_workspace(partition, workspace.root)
        workspaces[partition] = workspace
    write_campaign({"north": "north", "south": "south"}, assignment=assignment, project=root)
    return root, workspaces


def test_hash_assignment_is_deterministic_and_stable(tmp_path: Path) -> None:
    """The same key always lands in the same partition, run after run."""

    root, _ = _campaign_project(tmp_path, "hash")
    first = assign_partition("silicon", project=root)
    assert first in {"north", "south"}
    assert assign_partition("silicon", project=root) == first
    # Some key lands in each partition, so the hash actually spreads.
    landed = {assign_partition(f"structure-{index}", project=root) for index in range(50)}
    assert landed == {"north", "south"}


def test_round_robin_assignment_spreads_by_index(tmp_path: Path) -> None:
    """The batch position selects the partition, so a batch fans out in order."""

    root, _ = _campaign_project(tmp_path, "round-robin")
    # Partitions are visited in stable (sorted) order: north, south.
    assert assign_partition("anything", index=0, project=root) == "north"
    assert assign_partition("anything", index=1, project=root) == "south"
    assert assign_partition("anything", index=2, project=root) == "north"


def test_explicit_assignment_names_the_partition_and_rejects_an_unknown_one(tmp_path: Path) -> None:
    """The key is the partition name itself, and an unknown one is refused."""

    root, _ = _campaign_project(tmp_path, "explicit")
    assert assign_partition("south", project=root) == "south"
    with pytest.raises(ValueError, match="unknown campaign partition"):
        assign_partition("east", project=root)


def test_the_written_campaign_reads_back(tmp_path: Path) -> None:
    """The partition map round-trips through the project configuration."""

    root, _ = _campaign_project(tmp_path, "hash")
    config = read_campaign(root)
    assert config.assignment == "hash"
    assert config.partitions == {"north": "north", "south": "south"}
    assert config.ordered_partitions() == ("north", "south")


def test_campaign_cli_rejects_path_workflows_with_a_job_new_hint(tmp_path: Path, capsys) -> None:
    root, _ = _campaign_project(tmp_path, "hash")
    assert (
        command(
            ["campaign", "submit", "--workflow", "./runner.py", "--key", "silicon"],
            CLIContext("httk", root),
        )
        == 2
    )
    error = capsys.readouterr().err
    assert "workflow names only" in error
    assert "job new" in error


def test_children_stay_in_their_parents_workspace(tmp_path: Path) -> None:
    """A spawned child is published into its parent's workspace, so a campaign
    never has to re-route a subtree — the whole tree stays in the partition its
    root was assigned."""

    _, workspaces = _campaign_project(tmp_path, "explicit")
    north = workspaces["north"]
    installed = v3.install(north, tmp_path / "package")
    spawn = {"children": [{"label": "kid", "script": {"start": "succeed"}}], "next_step": "finish"}
    v3.submit(north, installed, {"start": "spawn", "finish": "succeed"}, tag="tree", parameters={"spawn": spawn})
    v3.run(north)

    states = {ref.job_key.split("--")[0]: ref.state for ref in iter_jobs(north)}
    assert states == {"tree": "succeeded", "kid": "succeeded"}
    # The other partition was never touched: the child did not leak across.
    assert list(iter_jobs(workspaces["south"])) == []


def test_collect_crosses_the_partitions_lazily_in_stable_order(tmp_path: Path) -> None:
    """A campaign spread over many workspaces collects as one stream, visiting the
    partitions in stable order."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    for partition in ("north", "south"):
        _run_one(tmp_path, workspaces[partition], partition)

    records = list(campaign_collect(states=["succeeded"], project=root))
    by_workspace = [record.workspace_id for record in records]
    assert by_workspace == [workspaces["north"].workspace_id, workspaces["south"].workspace_id]

    # A subset selection collects only the named partition.
    south_only = list(campaign_collect(states=["succeeded"], partitions=["south"], project=root))
    assert [record.workspace_id for record in south_only] == [workspaces["south"].workspace_id]


def test_campaign_collect_into_skips_degraded_jobs_and_exits_nonzero(tmp_path: Path, capsys) -> None:
    """Campaign collect passes --into and --allow-job-collector through, stores no
    empty Run for a degraded job, and exits nonzero with an aggregated summary."""

    pytest.importorskip("httk.store")
    root, workspaces = _campaign_project(tmp_path, "explicit")
    # The workflow declares an output but has no collector, so every job degrades.
    for partition in ("north", "south"):
        _run_one(
            tmp_path, workspaces[partition], partition, manifest='\n[workflow.outputs.energy]\nentry_type = "records"\n'
        )

    store = tmp_path / "campaign.sqlite"
    context = CLIContext("httk", root)
    assert (
        command(
            [
                "campaign",
                "collect",
                "--allow-job-collector",
                "--into",
                str(store),
                "--id-base",
                "httk.campaign",
            ],
            context,
        )
        == 1
    )

    lines = capsys.readouterr().out.splitlines()
    reports = [json.loads(line) for line in lines[:-1]]
    summary = json.loads(lines[-1])
    assert len(reports) == 2
    assert all(report["stored"] is None and report["skipped"] == "degraded" for report in reports)
    assert summary["format"] == "httk-workflow-collect-summary"
    assert summary["degraded"] == 2 and summary["collected"] == 0 and summary["storage_errors"] == 0


def test_campaign_collect_rejects_into_with_raw(tmp_path: Path, capsys) -> None:
    root, _ = _campaign_project(tmp_path, "explicit")
    context = CLIContext("httk", root)
    assert command(["campaign", "collect", "--raw", "--into", str(tmp_path / "x.sqlite")], context) == 2
    assert "--into cannot be combined with --raw" in capsys.readouterr().err


def test_collect_refuses_and_names_a_remote_partition(tmp_path: Path) -> None:
    """A remote workspace is collected where it runs; the campaign collect names any
    remote partition rather than silently skipping it."""

    root = tmp_path / "project"
    initialize_project(root, name="remote-campaign")
    from httk.workflow.adapters import add_remote

    add_remote("cluster", template="local", project=root)
    write_campaign({"far": "cluster:far"}, assignment="explicit", project=root)

    with pytest.raises(ValueError, match="remote workspace"):
        list(campaign_collect(project=root))


def test_start_managers_reports_the_qualified_remote_workspace_name(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "project"
    initialize_project(root, name="remote-campaign-managers")
    fake_remote(root)
    context = CLIContext("httk", root)
    assert command(["workspace", "init", "cluster:runs"], context) == 0
    capsys.readouterr()
    write_campaign({"kappa": "cluster:runs"}, assignment="explicit", project=root)

    report = campaign_managers(project=root)

    assert report[0]["partition"] == "kappa"
    assert report[0]["workspace"] == "cluster:runs"
    assert report[0]["mode"] == "remote"


def test_campaign_start_managers_forwards_launcher_to_remote_partition(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "project"
    initialize_project(root, name="remote-campaign-launcher")
    fake_remote(root)
    context = CLIContext("httk", root)
    assert command(["workspace", "init", "cluster:runs"], context) == 0
    write_campaign({"kappa": "cluster:runs"}, assignment="explicit", project=root)

    from httk.workflow import adapters

    seen: list[list[str]] = []

    def invoked(_bundle, _operation, payload, *, timeout):
        seen.append(payload["argv"])
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(adapters, "run_adapter", invoked)
    assert campaign_managers(launcher="gpu", project=root) == [
        {
            "partition": "kappa",
            "workspace": "cluster:runs",
            "mode": "remote",
            "returncode": 0,
            "stdout": "",
            "stderr": "",
        }
    ]
    assert seen[0] == [
        "httk",
        "workflow",
        "manager",
        "run",
        "--workspace",
        "runs",
        "--detach",
        "--launcher",
        "gpu",
    ]
    assert seen[0].count("--launcher") == 1


def test_campaign_remote_manager_failure_is_reported_and_returns_nonzero(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "project"
    initialize_project(root, name="remote-campaign-failure")
    fake_remote(root)
    context = CLIContext("httk", root)
    assert command(["workspace", "init", "cluster:runs"], context) == 0
    capsys.readouterr()
    write_campaign({"kappa": "cluster:runs"}, assignment="explicit", project=root)

    from httk.workflow import adapters

    seen: list[list[str]] = []

    def refused(_bundle, _operation, payload, *, timeout):
        seen.append(payload["argv"])
        return {"returncode": 7, "stdout": "remote output\n", "stderr": "remote failure\n"}

    monkeypatch.setattr(adapters, "run_adapter", refused)
    assert command(["campaign", "start-managers"], context) == 1
    report = json.loads(capsys.readouterr().out)[0]
    assert report["returncode"] == 7
    assert report["stdout"] == "remote output\n"
    assert report["stderr"] == "remote failure\n"
    assert "--count" not in seen[0]
