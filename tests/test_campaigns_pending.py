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
    assign_partition,
    campaign_managers,
    campaign_submit,
    write_campaign,
)
from httk.workflow.models import parse_job_key
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


def test_job_new_campaign_round_robin_numbers_the_batch_from_the_index(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.atomistic")
    root, workspaces = _campaign_project(tmp_path, "round-robin")
    source = package(
        tmp_path / "relax", "tests.relax", ["start"], extra='\n[workflow.inputs.structure]\ndestination = "POSCAR"\n'
    )
    structures = tmp_path / "structures"
    structures.mkdir()
    for name in ("a.POSCAR", "b.POSCAR", "c.POSCAR"):
        (structures / name).write_text("silicon\n1.0\n2 0 0\n0 2 0\n0 0 2\nSi\n1\nDirect\n0 0 0\n", encoding="utf-8")
    argv = ["job", "new", str(source), "--campaign", "--campaign-index", "1", "--input-from", "structure"]
    argv += [str(structures / name) for name in ("a.POSCAR", "b.POSCAR", "c.POSCAR")]

    assert command(argv, CLIContext("httk", root)) == 2
    assert "httk workflow install" in capsys.readouterr().err
    assert every_job(workspaces["north"]) == every_job(workspaces["south"]) == []
    assert command([*argv, "--install"], CLIContext("httk", root)) == 0
    assert len(capsys.readouterr().out.splitlines()) == 3
    # Positions 1, 2 and 3 of the sorted partitions (north, south): south, north, south.
    assert len(every_job(workspaces["north"])) == 1
    assert len(every_job(workspaces["south"])) == 2


def _structures(tmp_path: Path, names: tuple[str, ...]) -> Path:
    structures = tmp_path / "structures"
    structures.mkdir()
    for name in names:
        (structures / name).write_text("silicon\n1.0\n2 0 0\n0 2 0\n0 0 2\nSi\n1\nDirect\n0 0 0\n", encoding="utf-8")
    return structures


def _relax(tmp_path: Path) -> str:
    extra = '\n[workflow.inputs.structure]\ndestination = "POSCAR"\n'
    return str(package(tmp_path / "relax", "tests.relax", ["start"], extra=extra))


def test_job_new_campaign_refuses_a_taken_tag_in_any_partition_before_submitting(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.atomistic")
    root, workspaces = _campaign_project(tmp_path, "round-robin")
    source = _relax(tmp_path)
    structures = _structures(tmp_path, ("a.POSCAR", "b.POSCAR", "c.POSCAR"))
    context = CLIContext("httk", root)
    # South already holds the job b.POSCAR becomes; round-robin from 0 sends a and c north, b south.
    single = ["job", "new", source, "--campaign", "--install", "--partition", "south"]
    assert command([*single, "--input-from", "structure", str(structures / "b.POSCAR")], context) == 0
    capsys.readouterr()
    batch = ["job", "new", source, "--campaign", "--install", "--input-from", "structure"]
    batch += [str(structures / name) for name in ("a.POSCAR", "b.POSCAR", "c.POSCAR")]

    assert command(batch, context) == 2
    assert "already exists in placement" in capsys.readouterr().err
    assert every_job(workspaces["north"]) == []
    assert len(every_job(workspaces["south"])) == 1
    # The idempotent rerun creates the rest.
    assert command([*batch, "--duplicate-tag", "skip"], context) == 0
    assert (len(every_job(workspaces["north"])), len(every_job(workspaces["south"]))) == (2, 1)


def test_job_new_campaign_hashes_each_composed_batch_tag(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.atomistic")
    root, workspaces = _campaign_project(tmp_path, "hash")
    names = tuple(f"s{index}.POSCAR" for index in range(8))
    structures = _structures(tmp_path, names)
    argv = ["job", "new", _relax(tmp_path), "--campaign", "--install", "--tag", "run7"]
    assert command([*argv, "--input-from", "structure", str(structures)], CLIContext("httk", root)) == 0
    capsys.readouterr()
    landed = {partition: every_job(workspace) for partition, workspace in workspaces.items()}
    assert sum(len(refs) for refs in landed.values()) == len(names)
    for partition, refs in landed.items():
        for ref in refs:
            tag = parse_job_key(ref.job_key)[0]
            assert tag is not None and tag.startswith("run7-")
            assert assign_partition(tag, project=root) == partition


def test_job_new_campaign_hashes_the_tag_and_pins_a_named_partition(tmp_path: Path, capsys) -> None:
    root, workspaces = _campaign_project(tmp_path, "hash")
    source = str(package(tmp_path / "plain", "tests.plain", ["start"]))
    context = CLIContext("httk", root)

    assert command(["job", "new", source, "--campaign", "--install"], context) == 2
    assert "give --tag TAG" in capsys.readouterr().err
    assert (
        command(["job", "new", source, "--campaign", "--install", "--tag", "x", "--campaign-index", "1"], context) == 2
    )
    assert "--campaign-index applies only to round-robin" in capsys.readouterr().err
    assert command(["job", "new", source, "--campaign", "--install", "--tag", "silicon"], context) == 0
    capsys.readouterr()
    hashed = assign_partition("silicon", project=root)
    assert len(every_job(workspaces[hashed])) == 1
    other = "south" if hashed == "north" else "north"
    assert every_job(workspaces[other]) == []
    # --partition pins the partition whatever the policy would pick.
    argv = ["job", "new", source, "--campaign", "--install", "--tag", "silicon", "--partition", other]
    assert command([*argv, "--duplicate-tag", "allow"], context) == 0
    assert len(every_job(workspaces[other])) == 1


def test_job_new_campaign_explicit_assignment_needs_a_partition(tmp_path: Path, capsys) -> None:
    root, workspaces = _campaign_project(tmp_path, "explicit")
    source = str(package(tmp_path / "plain", "tests.plain", ["start"]))
    context = CLIContext("httk", root)

    assert command(["job", "new", source, "--campaign", "--install"], context) == 2
    assert "name one with --partition NAME" in capsys.readouterr().err
    assert command(["job", "new", source, "--campaign", "--install", "--partition", "east"], context) == 2
    assert "unknown campaign partition: east" in capsys.readouterr().err
    assert command(["job", "new", source, "--campaign", "--install", "--partition", "south"], context) == 0
    capsys.readouterr()
    assert (len(every_job(workspaces["north"])), len(every_job(workspaces["south"]))) == (0, 1)

    for refused, message in (
        (["--campaign", "-w", "north"], "cannot be combined with --workspace"),
        (["--campaign", "--partition", "south", "--campaign-index", "1"], "applies only to round-robin"),
        (["--partition", "south"], "apply only with --campaign"),
        (["--campaign-index", "1"], "apply only with --campaign"),
    ):
        assert command(["job", "new", source, *refused], context) == 2, refused
        assert message in capsys.readouterr().err, refused


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
