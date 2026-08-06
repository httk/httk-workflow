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
from typing import Any

import httk.core
import pytest
from httk.core.cli import CLIContext
from httk.core.register import register_format_serializer, register_reader, register_writer

import httk.workflow.vasp
from httk.workflow import FormatError, TaskManager, Workspace, scaffold
from httk.workflow._util import sha256_file
from httk.workflow.models import JobDefinition, validate_label
from httk.workflow.runners import RUNNERS, runner_path
from httk.workflow.scaffold import (
    JOB_SCAFFOLD_FORMAT,
    JobItem,
    TemplateProvider,
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
    job: dict[str, Any] = {
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


def _testfmt_reader(filename: str) -> dict[str, object]:
    return {"loaded": Path(filename).read_text(encoding="utf-8")}


def _testfmt_writer(destination: Path, payload: dict[str, object]) -> None:
    destination.write_text(str(payload["value"]), encoding="utf-8")


def _testfmt_serializer(value: object) -> dict[str, object]:
    if value == {"raise": True}:
        raise RuntimeError("test serializer failed")
    return {"format": "workflow-testfmt", "value": value}


# Runtime accepts callables here; the registry annotation models lazy string references.
register_reader(name="workflow-testfmt", reader=_testfmt_reader, extensions=(".testfmt",))  # type: ignore[arg-type]
register_writer(
    name="workflow-testfmt",
    writer=_testfmt_writer,
    format="workflow-testfmt",
    extensions=(".testfmt",),
)
register_format_serializer(format="workflow-testfmt", serializer=_testfmt_serializer)


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
        assert template.parameters == {"structure": "POSCAR"}
        assert template.initial_step in template.steps

    expected = {
        "vasp-relax": "vasp-relax",
        "vasp-relax-bash": "vasp-relax",
        "vasp-static": "vasp-static",
        "vasp-relax-static": "vasp-relax-static",
    }
    for name, workflow_name in expected.items():
        template = packaged_template(name)
        assert template is not None
        assert template.declarations == {
            "workflow": {"$id": f"https://schemas.httk.org/defs/v0.1/workflows/{workflow_name}"}
        }

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


def test_template_declarations_are_forwarded_and_digest_covered(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    declarations = {"workflow": {"$id": "https://example.test/workflows/v1", "nested": {"value": 7}}}
    provider = TemplateProvider(
        name="test-declarations",
        runner_package=PACKAGE,
        runner_file="vasp_relax.py",
        workflow="tests.declarations",
        initial_step="prepare",
        steps=("collect", "prepare", "run"),
        declarations=declarations,
    )
    monkeypatch.setitem(scaffold._TEMPLATE_PROVIDERS, provider.name, provider)

    job = new_job(workspace, provider.name)
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.declarations == declarations
    assert definition.digest == sha256_file(job.payload / "job.json")


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


def test_a_path_parameter_lands_at_the_declared_payload_destination(workspace: Workspace, structure: Path) -> None:
    job = new_job(workspace, "vasp-relax", parameters={"structure": structure})
    assert (job.payload / "files" / "POSCAR").read_text(encoding="utf-8") == _POSCAR
    assert "parameters" not in json.loads((job.payload / "job.json").read_text(encoding="utf-8"))


def test_parameter_validation_and_realization_fail_before_submission(tmp_path: Path, workspace: Workspace) -> None:
    with pytest.raises(ValueError, match="declared parameters: structure"):
        new_job(workspace, "vasp-relax", parameters={"unknown": object()})

    runner = tmp_path / "hook.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.hook', parameters={'x': None})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="requires an instantiate hook"):
        new_job(workspace, runner, parameters={"x": object()})
    assert not list(workspace.scan_markers())
    assert list((workspace.control / "tmp").iterdir()) == []


def test_an_instantiate_hook_stages_parameters_and_runs_once_per_campaign(tmp_path: Path, workspace: Workspace) -> None:
    counter = tmp_path / "imports"
    runner = tmp_path / "hook.py"
    runner.write_text(
        "import json\n"
        "from pathlib import Path\n"
        "from httk.workflow import Runner\n"
        f"counter = Path({str(counter)!r})\n"
        "if __name__ != '__main__':\n"
        "    counter.write_text(str(int(counter.read_text()) + 1) if counter.exists() else '1')\n"
        "run = Runner('tests.hook', parameters={'structure': 'POSCAR', 'note': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx):\n"
        "    assert (ctx.payload / 'files' / 'POSCAR').is_file()\n"
        "    (ctx.payload / 'files' / 'generated.txt').write_text(ctx.parameters['note'], encoding='utf-8')\n"
        "    ctx.inputs['derived'] = ctx.parameters['note']\n"
        "    if ctx.parameters['note'] == 'one': ctx.suggest_tag('suggested')\n"
        "    else: ctx.tag = 'direct-assignment'\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")

    jobs = list(
        new_jobs(
            workspace,
            runner,
            [{"parameters": {"note": "one"}}, {"parameters": {"note": "two"}, "tag": "explicit"}],
            parameters={"structure": structure},
            inputs={"shared": True},
        )
    )
    assert counter.read_text(encoding="utf-8") == "1"
    assert (jobs[0].payload / "files" / "generated.txt").read_text(encoding="utf-8") == "one"
    assert (jobs[1].payload / "files" / "generated.txt").read_text(encoding="utf-8") == "two"
    assert jobs[0].tag == "suggested" and jobs[1].tag == "explicit"
    assert json.loads((jobs[0].payload / "job.json").read_text(encoding="utf-8"))["inputs"] == {
        "shared": True,
        "derived": "one",
    }


def test_an_instantiate_hook_failure_leaves_no_submission_or_scratch(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "raising.py"
    runner.write_text(
        "import json\n"
        "from httk.workflow import Runner\n"
        "run = Runner('tests.raising', parameters={'note': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx): raise RuntimeError('hook failed')\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="hook failed"):
        new_job(workspace, runner, parameters={"note": "value"})
    assert not list(workspace.scan_markers())
    assert list((workspace.control / "tmp").iterdir()) == []


def test_an_instantiate_hook_must_match_the_published_runner_digest(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = tmp_path / "changing.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.changing')\n"
        "@run.instantiate\n"
        "def instantiate(ctx): pass\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    publish_runner = workspace.publish_runner

    def publish_then_change(source: Path, *, name: str) -> dict[str, object]:
        reference = publish_runner(source, name=name)
        source.write_text(source.read_text(encoding="utf-8") + "# changed\n", encoding="utf-8")
        return reference

    monkeypatch.setattr(workspace, "publish_runner", publish_then_change)
    with pytest.raises(ValueError, match="does not match pinned runner digest"):
        new_job(workspace, runner)
    assert not list(workspace.scan_markers())
    assert list((workspace.control / "tmp").iterdir()) == []


def test_an_instantiate_hook_executes_the_bytes_it_verified(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = tmp_path / "verified.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.verified', parameters={'value': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx):\n"
        "    (ctx.payload / 'verified.txt').write_text(ctx.parameters['value'], encoding='utf-8')\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    verified_bytes = runner.read_bytes()
    real_read_bytes = Path.read_bytes
    reads = 0
    publish_runner = workspace.publish_runner

    def publish_then_replace(source: Path, *, name: str) -> dict[str, object]:
        reference = publish_runner(source, name=name)
        source.write_text("raise AssertionError('unverified bytes executed')\n", encoding="utf-8")
        return reference

    def read_verified_bytes(path: Path) -> bytes:
        nonlocal reads
        if path.resolve() == runner.resolve():
            reads += 1
            return verified_bytes
        return real_read_bytes(path)

    monkeypatch.setattr(workspace, "publish_runner", publish_then_replace)
    monkeypatch.setattr(Path, "read_bytes", read_verified_bytes)
    job = new_job(workspace, runner, parameters={"value": "verified"})
    assert reads == 1
    assert (job.payload / "verified.txt").read_text(encoding="utf-8") == "verified"


def test_instantiate_resolution_rejects_bad_runner_shapes_and_bash(tmp_path: Path, workspace: Workspace) -> None:
    zero = tmp_path / "zero.py"
    zero.write_text(
        "import json\nprint(json.dumps({'workflow': 'tests.zero', 'steps': ['start'], 'instantiate': True}))\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="defines no Runner"):
        new_job(workspace, zero)

    several = tmp_path / "several.py"
    several.write_text(
        "from httk.workflow import Runner\n"
        "one = Runner('tests.one')\n"
        "two = Runner('tests.two')\n"
        "if __name__ == '__main__': print('{\"workflow\": \"tests.two\", \"steps\": [\"start\"], \"instantiate\": true}')\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="defines 2 Runner"):
        new_job(workspace, several)

    bash = tmp_path / "hook.sh"
    bash.write_text(
        "#!/bin/sh\nprintf '%s\\n' '{\"workflow\":\"tests.bash\",\"steps\":[\"start\"],\"instantiate\":true}'\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Python-SDK-only"):
        new_job(workspace, bash)


def test_hook_consumed_parameters_require_the_hook_and_old_description_defaults_false(
    tmp_path: Path, workspace: Workspace
) -> None:
    runner = tmp_path / "no-hook.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.no-hook', parameters={'x': None})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="requires an instantiate hook"):
        new_job(workspace, runner, parameters={"x": "value"})
    plain = tmp_path / "plain.py"
    plain.write_text(_SINGLE_STEP_RUNNER, encoding="utf-8")
    assert resolve_template(plain).instantiate is False


def test_an_object_parameter_is_serialized_and_a_failing_save_leaves_no_scratch(
    tmp_path: Path, workspace: Workspace
) -> None:
    runner = tmp_path / "object.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.object', parameters={'data': 'files/data.testfmt'})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    job = new_job(workspace, runner, parameters={"data": {"value": 7}})
    assert (job.payload / "files" / "data.testfmt").read_text(encoding="utf-8") == "{'value': 7}"
    with pytest.raises(RuntimeError, match="test serializer failed"):
        new_job(workspace, runner, parameters={"data": {"raise": True}})
    assert len(list(workspace.scan_markers())) == 1
    assert list((workspace.control / "tmp").iterdir()) == []


def test_an_object_parameter_requires_a_registered_writer(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "missing.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.missing', parameters={'data': 'files/data.no-writer'})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="httk-io and httk-atomistic"):
        new_job(workspace, runner, parameters={"data": object()})


def test_a_runner_without_a_parameter_description_has_an_empty_declaration(tmp_path: Path) -> None:
    runner = tmp_path / "plain.py"
    runner.write_text(_SINGLE_STEP_RUNNER, encoding="utf-8")
    assert resolve_template(runner).parameters == {}


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
    assert resolve_template(runner).declarations == {}
    job = new_job(workspace, runner, tag="own")
    assert job.workflow == "tests.scaffold.single" and job.initial_step == "start"
    # A runner of one's own defaults to data.mode none: it declared no results.
    assert JobDefinition.from_path(job.payload / "job.json").data_mode == "none"
    assert "declarations" not in json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
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
                str(root),
                "--name",
                ws_name,
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
                "--parameter",
                f"structure={structure}",
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
    for name in ("a.vasp", "b.vasp"):
        (directory / name).write_text(_POSCAR, encoding="utf-8")
    assert (
        command(
            [
                "job",
                "new",
                ws_name,
                "--template",
                "vasp-relax",
                "--parameter-from",
                "structure",
                str(directory),
                "--json",
            ],
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
    assert command(["workspace", "init", str(root)], _context(tmp_path)) == 0
    capsys.readouterr()

    # A malformed assignment, an unknown template, and an empty structure directory.
    assert command(["job", "new", name, "--template", "vasp-relax", "--input", "bare"], _context(tmp_path)) == 2
    assert "NAME=VALUE" in capsys.readouterr().err
    assert command(["job", "new", name, "--template", "nope"], _context(tmp_path)) == 2
    assert "unknown template" in capsys.readouterr().err
    empty = tmp_path / "empty"
    empty.mkdir()
    assert (
        command(
            ["job", "new", name, "--template", "vasp-relax", "--parameter-from", "structure", str(empty)],
            _context(tmp_path),
        )
        == 2
    )
    assert "no readable parameter files" in capsys.readouterr().err


def test_parameter_from_single_file_and_two_batches_are_validated(
    tmp_path: Path, structure: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    name = "parameter-cli"
    root = tmp_path / name
    assert command(["workspace", "init", str(root)], _context(tmp_path)) == 0
    capsys.readouterr()
    assert (
        command(
            [
                "job",
                "new",
                name,
                "--template",
                "vasp-relax",
                "--parameter-from",
                "structure",
                str(structure),
                "--json",
            ],
            _context(tmp_path),
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)[0]
    assert report["tag"] == "poscar"
    assert (Path(report["payload_path"]) / "files" / "POSCAR").is_file()

    directory = tmp_path / "testfmt"
    directory.mkdir()
    for name_ in ("first.testfmt", "second.testfmt"):
        (directory / name_).write_text(name_, encoding="utf-8")
    extra = tmp_path / "extra.txt"
    extra.write_text("shared extra\n", encoding="utf-8")
    runner = tmp_path / "object.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.cli-object', parameters={'data': 'data.testfmt'})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    assert (
        command(
            [
                "job",
                "new",
                name,
                "--template",
                str(runner),
                "--parameter-from",
                "data",
                str(directory),
                "--file",
                f"extra={extra}",
            ],
            _context(tmp_path),
        )
        == 0
    )
    reports = capsys.readouterr().out.splitlines()
    assert [line.split("\t")[0].split("--")[0] for line in reports] == ["first", "second"]
    assert all(
        (Path(line.split("\t")[1]) / "files" / "extra").read_text(encoding="utf-8") == "shared extra\n"
        for line in reports
    )

    # A packaged VASP template accepts the same source shape; two batch sources
    # are refused before either job is submitted.
    assert (
        command(
            [
                "job",
                "new",
                name,
                "--template",
                "vasp-relax",
                "--parameter-from",
                "structure",
                str(directory),
                "--parameter-from",
                "structure",
                str(directory),
            ],
            _context(tmp_path),
        )
        == 2
    )
    assert "only one --parameter-from" in capsys.readouterr().err
    assert command(["job", "new", name, "--template", "vasp-relax", "--from", str(structure)], _context(tmp_path)) == 2
    assert "unrecognized arguments: --from" in capsys.readouterr().err


def test_parameter_from_cif_is_written_as_a_poscar_when_domain_plugins_are_available(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("httk.io")
    pytest.importorskip("httk.atomistic")
    assert httk.core.has_writer_for("POSCAR")
    cif = tmp_path / "silicon.cif"
    cif.write_text(
        "data_si\n"
        "_cell_length_a 2\n_cell_length_b 2\n_cell_length_c 2\n"
        "_symmetry_space_group_name_h-m 'P 1'\n"
        "_cell_angle_alpha 90\n_cell_angle_beta 90\n_cell_angle_gamma 90\n"
        "loop_\n_space_group_symop_operation_xyz\n'x,y,z'\n"
        "loop_\n_atom_site_label\n_atom_site_type_symbol\n"
        "_atom_site_fract_x\n_atom_site_fract_y\n_atom_site_fract_z\n"
        "Si1 Si 0 0 0\n",
        encoding="utf-8",
    )
    workspace = tmp_path / "cif-workspace"
    assert command(["workspace", "init", str(workspace), "--name", "cif"], _context(tmp_path)) == 0
    capsys.readouterr()
    assert (
        command(
            [
                "job",
                "new",
                "cif",
                "--template",
                "vasp-relax",
                "--parameter-from",
                "structure",
                str(cif),
                "--json",
            ],
            _context(tmp_path),
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)[0]
    poscar = Path(report["payload_path"]) / "files" / "POSCAR"
    assert poscar.is_file()
    assert httk.core.load(str(poscar)) is not None


def _context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)
