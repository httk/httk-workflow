"""Job scaffolding: one template, some files, and a submitted job.

Nothing here fabricates protocol state. Every job is built by
:func:`httk.workflow.scaffold.new_job` or by ``httk workflow job new`` and then
read back from the workspace it was submitted to, and the jobs that have to prove
they *run* are driven to completion by a real
:class:`httk.workflow.TaskManager` with the mock VASP of ``examples/mock_vasp.py``
standing in for VASP.
"""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import FormatError, TaskManager, Workspace
from httk.workflow._util import sha256_file
from httk.workflow.models import JobDefinition, validate_label
from httk.workflow.runners import RUNNERS, runner_path
from httk.workflow.scaffold import (
    JOB_SCAFFOLD_FORMAT,
    JobItem,
    describe_runner,
    new_job,
    new_jobs,
    packaged_template,
    payload_relative,
    registered_templates,
    resolve_template,
    structure_files,
    structure_tag,
)
from httk.workflow.vasp.runners import PACKAGE


def test_job_definition_uses_runner_executor_wire_key() -> None:
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": "12345678-1234-4234-8234-123456789abc",
        "tag": None,
        "name": "Test job",
        "workflow": "tests.example",
        "runner": {"executor": "path", "path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "run",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": None,
    }
    definition = JobDefinition.from_bytes(json.dumps(job).encode())
    assert definition.runner_executor == "path"
    assert definition.raw["runner"] == job["runner"]

    job["runner"]["executor"] = ""
    with pytest.raises(FormatError, match=r"runner\.executor"):
        JobDefinition.from_mapping(job)


from httk.workflow.workflow_cli import command

_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
"""
_SINGLE_STEP_RUNNER = """#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("tests.scaffold.single")


@run.step
def start(a):
    (a.workdir / "done.txt").write_text("done\\n", encoding="utf-8")
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
"""
_TWO_STEP_RUNNER = _SINGLE_STEP_RUNNER.replace(
    "tests.scaffold.single",
    "tests.scaffold.two",
).replace(
    "if __name__",
    '@run.step\ndef finish(a):\n    a.succeed()\n\n\nif __name__',
)


@pytest.fixture()
def structure(tmp_path: Path) -> Path:
    path = tmp_path / "POSCAR"
    path.write_text(_POSCAR, encoding="utf-8")
    return path


@pytest.fixture()
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    yield Workspace.initialize(tmp_path / "workspace")


def test_every_packaged_runner_has_a_template_that_says_what_it_implements() -> None:
    """The template table and the packaged runners cannot drift apart.

    The workflow and the steps of a packaged template are declared in the table
    rather than asked of the runner on every call, so what holds the two together
    is this: every packaged runner is described here, by running it, and its own
    answer is what the table must contain.
    """

    templates = [packaged_template(name) for name in registered_templates()]
    assert {template.packaged for template in templates if template} == set(RUNNERS)
    for template in templates:
        assert template is not None
        described = describe_runner(template.source)
        assert described["workflow"] == template.workflow
        assert described["steps"] == sorted(template.steps)
        assert template.initial_step in template.steps

    # A packaged runner is nameable by its own file name as well as by its template
    # name, and both resolve to exactly the installed bytes.
    template = resolve_template("vasp_relax.py")
    assert template.name == "vasp-relax" and template.source == runner_path("vasp_relax.py")
    assert template.workflow == "httk.vasp.relax" and template.initial_step == "prepare"
    assert template.data_mode == "transactional"
    assert resolve_template("vasp-relax") == template
    assert resolve_template("vasp-relax-static", step="static").initial_step == "static"
    with pytest.raises(ValueError, match="does not implement the step 'prepear'"):
        resolve_template("vasp-relax", step="prepear")
    with pytest.raises(ValueError, match="unknown template"):
        resolve_template("vasp-nonexistent")


def test_a_scaffolded_job_publishes_its_runner_by_content(workspace: Workspace, structure: Path) -> None:
    job = new_job(workspace, "vasp-relax", files={"POSCAR": structure}, tag="silicon", inputs={"kpoint_density": 30.0})

    # The job is submitted, its runner is in the store under a name carrying the
    # digest of its bytes, and the payload holds the structure where the runner
    # reads it.
    assert job.job_key == f"silicon--{job.job_id}"
    assert job.placement.as_posix() == "jobs"
    digest = sha256_file(runner_path("vasp_relax.py"))
    assert job.runner == {"source": "workspace", "path": f"vasp_relax.{digest[:12]}.py", "sha256": digest}
    assert sha256_file(workspace.runner_store_path(str(job.runner["path"]))) == digest
    assert (job.payload / "files" / "POSCAR").read_text(encoding="utf-8") == _POSCAR
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.workflow == "httk.vasp.relax" and definition.initial_step == "prepare"
    assert definition.data_mode == "transactional" and definition.workdir_mode == "persistent"
    assert definition.inputs == {"kpoint_density": 30.0}
    assert workspace.find_marker_by_id(job.job_id) is not None

    # Scaffolding a second job publishes nothing new: identical bytes are one
    # store entry, which is what makes one runner serve a whole campaign.
    again = new_job(workspace, "vasp-relax", files={"POSCAR": structure}, tag="silicon")
    assert again.runner == job.runner
    assert sorted(path.name for path in workspace.runners.iterdir()) == [f"vasp_relax.{digest[:12]}.py"]


def test_the_installed_form_references_a_packaged_runner_without_copying(workspace: Workspace, structure: Path) -> None:
    job = new_job(workspace, "vasp-static", files={"POSCAR": structure}, publish="installed")

    assert job.runner["source"] == "installed"
    assert job.runner["path"] == f"pkg:{PACKAGE}/vasp_static.py"
    assert not list(workspace.runners.iterdir())
    assert job.tag is None and job.job_key == job.job_id


def test_a_runner_file_of_ones_own_is_described_and_published(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "single.py"
    runner.write_text(_SINGLE_STEP_RUNNER, encoding="utf-8")

    # The runner says what it implements, so nothing has to be declared twice.
    described = describe_runner(runner)
    assert described == {"workflow": "tests.scaffold.single", "steps": ["start"]}
    job = new_job(workspace, runner, tag="own")
    assert job.workflow == "tests.scaffold.single" and job.initial_step == "start"
    # A runner of one's own defaults to data.mode none: it declared no results.
    assert JobDefinition.from_path(job.payload / "job.json").data_mode == "none"
    assert job.runner["source"] == "workspace"
    assert str(job.runner["path"]).startswith("single.")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert (job.payload / "run" / "done.txt").is_file()


def test_a_runner_with_several_steps_needs_the_starting_step_named(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "two.py"
    runner.write_text(_TWO_STEP_RUNNER, encoding="utf-8")
    assert describe_runner(runner)["steps"] == ["finish", "start"]

    # 'start' is registered here, so it is the default; naming a step the runner
    # does not implement is refused before anything is submitted.
    assert new_job(workspace, runner, tag="defaulted").initial_step == "start"
    with pytest.raises(ValueError, match="does not implement the step 'begin'"):
        new_job(workspace, runner, step="begin")
    assert new_job(workspace, runner, step="finish", tag="named").initial_step == "finish"


def test_an_undescribable_template_is_refused_by_name(tmp_path: Path, workspace: Workspace) -> None:
    broken = tmp_path / "broken.py"
    broken.write_text("raise SystemExit(3)\n", encoding="utf-8")
    with pytest.raises(ValueError, match="refused to describe itself"):
        new_job(workspace, broken)

    silent = tmp_path / "silent.py"
    silent.write_text("print('not a description')\n", encoding="utf-8")
    with pytest.raises(ValueError, match="did not print a runner description"):
        new_job(workspace, silent)

    mystery = tmp_path / "mystery.dat"
    mystery.write_text("nothing executable\n", encoding="utf-8")
    with pytest.raises(ValueError, match="names no interpreter"):
        new_job(workspace, mystery)


def test_a_transactional_template_works_in_a_core_workspace(tmp_path: Path, structure: Path) -> None:
    plain = Workspace.initialize(tmp_path / "plain")
    job = new_job(plain, "vasp-relax", files={"POSCAR": structure})
    assert JobDefinition.from_path(job.payload / "job.json").data_mode == "transactional"

    # The same template can leave results in the workdir when requested.
    job = new_job(plain, "vasp-relax", files={"POSCAR": structure}, data_mode="none")
    assert JobDefinition.from_path(job.payload / "job.json").data_mode == "none"


def test_a_campaign_publishes_one_runner_and_yields_jobs_lazily(workspace: Workspace, tmp_path: Path) -> None:
    directory = tmp_path / "structures"
    directory.mkdir()
    for name in ("POSCAR.Si2O", "POSCAR.fcc-Al", "mp-149.vasp", "notes.txt"):
        (directory / name).write_text(_POSCAR, encoding="utf-8")

    assert [path.name for path in structure_files(directory)] == ["POSCAR.Si2O", "POSCAR.fcc-Al", "mp-149.vasp"]
    items: list[JobItem] = [
        {"files": {"POSCAR": path}, "tag": structure_tag(path), "inputs": {"index": index}}
        for index, path in enumerate(structure_files(directory))
    ]
    campaign = new_jobs(
        workspace,
        "vasp-relax",
        iter(items),
        inputs={"kpoint_density": 15.0},
        placement="project/screening",
    )

    # Nothing happened yet: the campaign is a generator, which is what lets it be
    # a hundred million jobs long.
    assert not list(workspace.scan_markers())
    jobs = list(campaign)
    assert [job.tag for job in jobs] == ["si2o", "fcc-al", "mp-149"]
    assert {job.placement.as_posix() for job in jobs} == {"project/screening"}
    # One publication served every job, and per-job inputs were merged over shared.
    assert len({str(job.runner["path"]) for job in jobs}) == 1
    assert len(list(workspace.runners.iterdir())) == 1
    definition = JobDefinition.from_path(jobs[1].payload / "job.json")
    assert definition.inputs == {"kpoint_density": 15.0, "index": 1}
    assert len(list(workspace.scan_markers())) == 3


def test_a_staged_name_lands_where_the_runner_reads_it(workspace: Workspace, structure: Path, tmp_path: Path) -> None:
    assert payload_relative("POSCAR").as_posix() == "files/POSCAR"
    assert payload_relative("inputs/potcars/POTCAR").as_posix() == "inputs/potcars/POTCAR"
    for refused in ("", "../escape", "/absolute/POSCAR", "job.json", ".httk-job/state.json"):
        with pytest.raises(ValueError):
            payload_relative(refused)

    incar = tmp_path / "INCAR"
    incar.write_text("ENCUT = 300\n", encoding="utf-8")
    job = new_job(
        workspace,
        "vasp-relax",
        files={"POSCAR": structure, "INCAR": incar, "reference/notes.txt": incar},
        tag="staged",
    )
    assert (job.payload / "files" / "INCAR").read_text(encoding="utf-8") == "ENCUT = 300\n"
    assert (job.payload / "reference" / "notes.txt").is_file()

    # A directory and a file that is not there are refused before anything is
    # submitted, so a refused scaffolding leaves no half job behind.
    with pytest.raises(ValueError, match="not a directory"):
        new_job(workspace, "vasp-relax", files={"POSCAR": tmp_path})
    with pytest.raises(ValueError, match="does not exist"):
        new_job(workspace, "vasp-relax", files={"POSCAR": tmp_path / "absent"})
    assert len(list(workspace.scan_markers())) == 1


def test_structure_tags_are_derived_from_recognizable_names() -> None:
    assert structure_tag("POSCAR.Si2O") == "si2o"
    assert structure_tag("/tmp/POSCAR-fcc_Al") == "fcc_al"
    assert structure_tag("mp-149.vasp") == "mp-149"
    assert structure_tag("CONTCAR.relaxed") == "relaxed"
    assert structure_tag("Fe2 O3 (mp 24972).vasp") == "fe2-o3-mp-24972"
    # Every derived tag is a valid job-key component: it starts with a letter or a
    # digit, holds no double dash, and is bounded.
    assert structure_tag("POSCAR._weird--name") == "weird-name"
    assert len(str(structure_tag(f"POSCAR.{'x' * 80}"))) == 48
    for name in ("POSCAR.Si2O", "_weird--name.vasp", f"POSCAR.{'x' * 80}"):
        assert validate_label(structure_tag(name), "tag")
    # A name that says nothing beyond the VASP convention suggests no tag.
    assert structure_tag("POSCAR") is None
    assert structure_tag("...") is None


def test_the_command_scaffolds_one_job_and_a_whole_directory(
    tmp_path: Path,
    structure: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ws_name = "cli-workspace"
    root = tmp_path / ws_name
    assert (
        command(
            [
                "workspace",
                "init",
                ws_name,
                "--remote",
                "local",
                "--path",
                str(root),
            ],
            _context(tmp_path),
        )
        == 0
    )
    capsys.readouterr()

    assert (
        command(
            [
                "job",
                "new",
                ws_name,
                "--template",
                "vasp-relax",
                "--from",
                str(structure),
                "--tag",
                "silicon",
                "--input",
                "kpoint_density=30.0",
                "--input",
                'incar_tags={"ENCUT": 520}',
                "--input",
                "remedy_policy=reviewed-v1",
                "--placement",
                "project/si",
            ],
            _context(tmp_path),
        )
        == 0
    )
    key, payload = capsys.readouterr().out.strip().split("\t")
    assert key.startswith("silicon--")
    definition = JobDefinition.from_path(Path(payload) / "job.json")
    # A value is JSON when it parses as JSON and a string when it does not.
    assert definition.inputs == {"kpoint_density": 30.0, "incar_tags": {"ENCUT": 520}, "remedy_policy": "reviewed-v1"}
    assert (Path(payload) / "files" / "POSCAR").is_file()

    directory = tmp_path / "structures"
    directory.mkdir()
    for name in ("POSCAR.a", "POSCAR.b"):
        (directory / name).write_text(_POSCAR, encoding="utf-8")
    assert (
        command(
            ["job", "new", ws_name, "--template", "vasp-relax", "--from", str(directory), "--json"],
            _context(tmp_path),
        )
        == 0
    )
    reports = json.loads(capsys.readouterr().out)
    assert [report["tag"] for report in reports] == ["a", "b"]
    assert {report["format"] for report in reports} == {JOB_SCAFFOLD_FORMAT}
    assert {report["workflow"] for report in reports} == {"httk.vasp.relax"}


def test_the_command_reports_what_it_cannot_do(
    tmp_path: Path,
    structure: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    name = "refusals"
    root = tmp_path / name
    assert command(["workspace", "init", name, "--remote", "local", "--path", str(root)], _context(tmp_path)) == 0
    capsys.readouterr()

    # A malformed assignment, an unknown template, and an empty structure directory.
    assert command(["job", "new", name, "--template", "vasp-relax", "--input", "bare"], _context(tmp_path)) == 2
    assert "NAME=VALUE" in capsys.readouterr().err
    assert command(["job", "new", name, "--template", "nope"], _context(tmp_path)) == 2
    assert "unknown template" in capsys.readouterr().err
    empty = tmp_path / "empty"
    empty.mkdir()
    assert command(["job", "new", name, "--template", "vasp-relax", "--from", str(empty)], _context(tmp_path)) == 2
    assert "POSCAR*" in capsys.readouterr().err


def _context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)
