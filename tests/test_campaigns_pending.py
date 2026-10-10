"""Campaign submission and manager launch.

Split out of ``test_campaigns.py``; the description below is the original module's.

Campaigns: a thin partition map over many registered workspaces.

A campaign spreads a project's work across many ordinary workspaces without a new
scheduler or graph. These tests pin the convention: a root job is assigned to a
partition by policy, everything it spawns inherits its workspace, and collect and
management simply cross the partition list. Nothing here is engine sharding — a
partition is just a name pointing at a registered workspace a manager serves and
a collect reads exactly as any other.
"""

from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from attempt_fixtures import every_job, package
from httk.workflow import Workspace, _kernel
from httk.workflow.campaigns import (
    campaign_managers,
    campaign_submit,
    write_campaign,
)
from httk.workflow.projects import initialize_project
from httk.workflow.registry import register_workspace
from httk.workflow.workflow_cli import command

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("campaign-manager-timing")]

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
        ChildSpec(step="child", parameters={}, maximum_attempts_per_activation=1),
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
        register_workspace(partition, workspace.root)
        workspaces[partition] = workspace
    write_campaign({"north": "north", "south": "south"}, assignment=assignment, project=root)
    return root, workspaces


def test_submit_routes_a_root_into_its_assigned_partition(tmp_path: Path) -> None:
    """A root job is created in the workspace its key is assigned to, and nowhere
    else."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    runner = _runner(tmp_path, _SUCCEED, "succeed.py")
    job = campaign_submit(str(runner), key="south", project=root, step="only", tag="silicon")

    assert [ref.job_id for ref in every_job(workspaces["south"])] == [job.job_id]
    assert every_job(workspaces["north"]) == []


def test_campaign_submit_passes_creation_parameters_to_the_scaffold(tmp_path: Path) -> None:
    root, workspaces = _campaign_project(tmp_path, "explicit")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure\n", encoding="utf-8")
    source = package(
        tmp_path / "relax", "tests.relax", ["start"], extra='\n[workflow.inputs.structure]\ndestination = "POSCAR"\n'
    )
    with pytest.raises(ValueError, match="is not installed"):
        campaign_submit(str(source), key="north", project=root)
    job = campaign_submit(str(source), key="north", project=root, inputs={"structure": structure}, install=True)
    assert (job.payload / "files" / "POSCAR").read_text(encoding="utf-8") == "structure\n"
    assert [ref.job_id for ref in _kernel.list_jobs(workspaces["north"], "ready")] == [job.job_id]


def test_campaign_cli_batch_uses_the_requested_round_robin_index(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.atomistic")
    root, workspaces = _campaign_project(tmp_path, "round-robin")
    source = package(
        tmp_path / "relax", "tests.relax", ["start"], extra='\n[workflow.inputs.structure]\ndestination = "POSCAR"\n'
    )
    structures = tmp_path / "structures"
    structures.mkdir()
    for name in ("a.POSCAR", "b.POSCAR"):
        (structures / name).write_text("silicon\n1.0\n2 0 0\n0 2 0\n0 0 2\nSi\n1\nDirect\n0 0 0\n", encoding="utf-8")
    argv = ["campaign", "submit", "--workflow", str(source), "--key", "silicon", "--index", "1"]
    argv += ["--input-from", "structure", str(structures / "a.POSCAR"), str(structures / "b.POSCAR")]

    assert command(argv, CLIContext("httk", root)) == 2
    assert "httk workflow install" in capsys.readouterr().err
    assert command([*argv, "--install"], CLIContext("httk", root)) == 0
    assert len(capsys.readouterr().out.splitlines()) == 2
    assert every_job(workspaces["north"]) == []
    assert len(every_job(workspaces["south"])) == 2


def test_start_managers_runs_a_manager_per_selected_local_partition(tmp_path: Path) -> None:
    """One manager per selected partition drains its work; a partition subset
    leaves the others alone."""

    root, workspaces = _campaign_project(tmp_path, "explicit")
    runner = _runner(tmp_path, _SUCCEED, "succeed.py")
    for partition in ("north", "south"):
        campaign_submit(str(runner), key=partition, project=root, step="only", tag=partition)

    report = campaign_managers(partitions=["north"], project=root)
    assert [row["partition"] for row in report] == ["north"]
    assert [ref.state for ref in every_job(workspaces["north"])] == ["succeeded"]
    # South was not selected, so its job is still waiting.
    assert [ref.state for ref in every_job(workspaces["south"])] == ["ready"]

    campaign_managers(project=root)
    assert [ref.state for ref in every_job(workspaces["south"])] == ["succeeded"]
