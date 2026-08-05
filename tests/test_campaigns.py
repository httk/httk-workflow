"""Campaigns: a thin partition map over many registered workspaces.

A campaign spreads a project's work across many ordinary workspaces without a new
scheduler or graph. These tests pin the convention: a root job is assigned to a
partition by policy, everything it spawns inherits its workspace, and harvest and
management simply cross the partition list. Nothing here is engine sharding — a
partition is just a name pointing at a registered workspace a manager serves and
a harvest reads exactly as any other.
"""

from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace
from httk.workflow.campaigns import (
    assign_partition,
    campaign_harvest,
    campaign_managers,
    campaign_submit,
    read_campaign,
    write_campaign,
)
from httk.workflow.projects import initialize_project
from httk.workflow.registry import LOCAL_REMOTE, register_workspace
from httk.workflow.workflow_cli import command

pytestmark = pytest.mark.xdist_group("campaign-manager-timing")

_SUCCEED = """#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("tests.campaign")


@run.step
def only(a):
    (a.workdir / "done.txt").write_text("ok", encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
"""

_SPAWN = """#!/usr/bin/env python3
from httk.workflow import ChildSpec, Runner

run = Runner("tests.spawn")


@run.step
def parent(a):
    a.spawn(
        ChildSpec(step="child", inputs={}, maximum_attempts_per_activation=1),
        label="kid",
        placement="project/children",
    )
    a.gather("finish", when="all_terminal")


@run.step
def child(a):
    a.succeed()


@run.step
def finish(a):
    a.succeed()


raise SystemExit(run.main())
"""


def _runner(tmp_path: Path, source: str, name: str) -> Path:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)
    return path


def _campaign_project(tmp_path: Path, assignment: str) -> tuple[Path, dict[str, Workspace]]:
    """A project with two local partitions, ``north`` and ``south``."""

    root = tmp_path / "project"
    initialize_project(root, name="campaign")
    workspaces: dict[str, Workspace] = {}
    for partition in ("north", "south"):
        workspace = Workspace.initialize(tmp_path / partition)
        register_workspace(partition, LOCAL_REMOTE, workspace.root, scope="project", project=root)
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


def test_submit_routes_a_root_into_its_assigned_partition(tmp_path: Path) -> None:
    """A root job is created in the workspace its key is assigned to, and nowhere
    else."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    runner = _runner(tmp_path, _SUCCEED, "succeed.py")
    job = campaign_submit(str(runner), key="south", project=root, step="only", tag="silicon")

    assert workspaces["south"].find_marker_by_id(job.job_id) is not None
    assert workspaces["north"].find_marker_by_id(job.job_id) is None


def test_campaign_submit_passes_creation_parameters_to_the_scaffold(tmp_path: Path) -> None:
    root, workspaces = _campaign_project(tmp_path, "explicit")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure\n", encoding="utf-8")
    job = campaign_submit("vasp-relax", key="north", project=root, parameters={"structure": structure})
    assert (job.payload / "files" / "POSCAR").read_text(encoding="utf-8") == "structure\n"
    assert workspaces["north"].find_marker_by_id(job.job_id) is not None


def test_campaign_cli_batch_uses_the_requested_round_robin_index(tmp_path: Path, capsys) -> None:
    root, workspaces = _campaign_project(tmp_path, "round-robin")
    structures = tmp_path / "structures"
    structures.mkdir()
    for name in ("a.vasp", "b.vasp"):
        (structures / name).write_text(
            "silicon\n1.0\n2 0 0\n0 2 0\n0 0 2\nSi\n1\nDirect\n0 0 0\n",
            encoding="utf-8",
        )

    assert (
        command(
            [
                "campaign",
                "submit",
                "--template",
                "vasp-relax",
                "--key",
                "silicon",
                "--index",
                "1",
                "--parameter-from",
                "structure",
                str(structures),
            ],
            CLIContext("httk", root),
        )
        == 0
    )
    assert len(capsys.readouterr().out.splitlines()) == 2
    assert len(list(workspaces["north"].scan_markers())) == 0
    assert len(list(workspaces["south"].scan_markers())) == 2


def test_children_stay_in_their_parents_workspace(tmp_path: Path) -> None:
    """A dynamically spawned child is scaffolded into its parent's workspace, so a
    campaign never has to re-route a subtree — the whole tree stays in the
    partition its root was assigned."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    runner = _runner(tmp_path, _SPAWN, "spawn.py")
    campaign_submit(str(runner), key="north", project=root, step="parent", tag="tree")

    with TaskManager(workspaces["north"], heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    north_states = {marker.job_key.split("--")[0]: marker.kind for marker in workspaces["north"].scan_markers()}
    assert north_states == {"tree": "succeeded", "kid": "succeeded"}
    # The other partition was never touched: the child did not leak across.
    assert list(workspaces["south"].scan_markers()) == []


def test_harvest_crosses_the_partitions_lazily_in_stable_order(tmp_path: Path) -> None:
    """A campaign spread over many workspaces harvests as one stream, visiting the
    partitions in stable order."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    runner = _runner(tmp_path, _SUCCEED, "succeed.py")
    for partition in ("north", "south"):
        campaign_submit(str(runner), key=partition, project=root, step="only", tag=partition)
        with TaskManager(workspaces[partition], heartbeat_interval=0.01) as manager:
            manager.run_until_idle(timeout=120.0)

    records = list(campaign_harvest(states=["succeeded"], project=root))
    by_workspace = [record.workspace_id for record in records]
    assert by_workspace == [workspaces["north"].workspace_id, workspaces["south"].workspace_id]

    # A subset selection harvests only the named partition.
    south_only = list(campaign_harvest(states=["succeeded"], partitions=["south"], project=root))
    assert [record.workspace_id for record in south_only] == [workspaces["south"].workspace_id]


def test_harvest_refuses_and_names_a_remote_partition(tmp_path: Path) -> None:
    """A remote workspace is harvested where it runs; the campaign harvest names any
    remote partition rather than silently skipping it."""

    root = tmp_path / "project"
    initialize_project(root, name="remote-campaign")
    from httk.workflow.adapters import add_remote

    add_remote("cluster", template="local", project=root)
    register_workspace("cluster:far", "cluster", tmp_path / "far-runs", scope="project", project=root)
    write_campaign({"far": "cluster:far"}, assignment="explicit", project=root)

    with pytest.raises(ValueError, match="remote workspace"):
        list(campaign_harvest(project=root))


def test_start_managers_runs_a_manager_per_selected_local_partition(tmp_path: Path) -> None:
    """One manager per selected partition drains its work; a partition subset
    leaves the others alone."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    runner = _runner(tmp_path, _SUCCEED, "succeed.py")
    for partition in ("north", "south"):
        campaign_submit(str(runner), key=partition, project=root, step="only", tag=partition)

    report = campaign_managers(partitions=["north"], project=root)
    assert [row["partition"] for row in report] == ["north"]
    assert all(marker.kind == "succeeded" for marker in workspaces["north"].scan_markers())
    # South was not selected, so its job is still waiting.
    assert {marker.kind for marker in workspaces["south"].scan_markers()} == {"submitted"}

    campaign_managers(project=root)
    assert all(marker.kind == "succeeded" for marker in workspaces["south"].scan_markers())
