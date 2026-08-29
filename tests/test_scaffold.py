"""Job scaffolding: one workflow, some files, and a submitted job.

Nothing here fabricates protocol state. Every job is built by
:func:`httk.workflow.scaffold.new_job` or by ``httk job new`` and then
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
from httk.core.digests import sha256_file
from httk.core.register import register_format_serializer, register_reader, register_writer

import httk.workflow.vasp
from conftest import register_ws
from httk.workflow import FormatError, TaskManager, Workspace, scaffold
from httk.workflow.models import JobDefinition, StateFrame, validate_label, validate_resources
from httk.workflow.runners import RUNNERS, runner_path
from httk.workflow.runtime_builders import JobSpec
from httk.workflow.scaffold import (
    JOB_SCAFFOLD_FORMAT,
    JobItem,
    WorkflowProvider,
    _has_bash_shebang,
    describe_runner,
    new_job,
    new_jobs,
    payload_relative,
    registered_workflow,
    registered_workflows,
    resolve_workflow,
    structure_files,
    structure_tag,
)
from httk.workflow.vasp.runners import PACKAGE
from httk.workflow.workflow_cli._job import _command_runner_text


def test_job_definition_uses_runner_executor_wire_key() -> None:
    job: dict[str, Any] = {
        "format": "httk-workflow-job",
        "format_version": 2,
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


@pytest.mark.parametrize(
    ("value", "expected"),
    [({}, {}), ({"procs": 4, "mem": 0}, {"procs": 4, "mem": 0})],
)
def test_resource_requirements_accept_nonnegative_integers(value: object, expected: dict[str, int]) -> None:
    assert validate_resources(value) == expected


@pytest.mark.parametrize("value", (None, [], {"bad/name": 1}, {"procs": True}, {"procs": 1.5}, {"procs": -1}))
def test_resource_requirements_reject_invalid_values(value: object) -> None:
    with pytest.raises(FormatError):
        validate_resources(value)


def test_job_definition_round_trips_step_resource_requirements() -> None:
    mapping = JobSpec(
        name="test",
        workflow="tests.example",
        runner_path="files/runner",
        initial_step="start",
        resources={"procs": 2},
        step_resources={"start": {"mem": 1024}},
    ).as_mapping()
    definition = JobDefinition.from_mapping(mapping)
    assert definition.resources == {"procs": 2}
    assert definition.step_resources == {"start": {"mem": 1024}}


def test_state_frame_resources_accessor_validates_and_round_trips() -> None:
    frame = StateFrame.replace(resources={"procs": 3})
    assert frame.resources == {"procs": 3}
    with pytest.raises(FormatError, match="non-negative"):
        invalid = StateFrame({"resources": {"procs": -1}})
        value = invalid.resources
        assert value is None


def test_a_bare_pwd_document_is_synthesized_with_a_declaration(tmp_path: Path) -> None:
    document = tmp_path / "flow.json"
    document.write_text(
        json.dumps(
            {
                "version": "0.1.0",
                "nodes": [
                    {"id": 0, "type": "function", "value": "module.run"},
                    {"id": 1, "type": "input", "name": "value"},
                    {"id": 2, "type": "output", "name": "result"},
                ],
                "edges": [
                    {"target": 0, "targetPort": "value", "source": 1, "sourcePort": None},
                    {"target": 2, "targetPort": None, "source": 0, "sourcePort": None},
                ],
            }
        ),
        encoding="utf-8",
    )

    resolved = resolve_workflow(document)

    assert resolved.language == "pwd"
    assert resolved.workflow_id == "pwd.flow"
    assert resolved.document_path == document.resolve()
    assert resolved.directory is None
    assert resolved.inputs == {"value": None}
    assert resolved.outputs == {"result": {"entry_type": "records", "role": "result"}}
    assert resolved.declarations["workflow"] == {
        "description": "the pwd document flow.json",
        "inputs": [],
        "outputs": [{"name": "result", "entry_type": "records"}],
    }


def test_a_non_workflow_json_file_does_not_match_a_language(tmp_path: Path) -> None:
    document = tmp_path / "not-a-workflow.json"
    document.write_text('{"hello": "world"}', encoding="utf-8")

    with pytest.raises(ValueError, match="not executable|suffix"):
        resolve_workflow(document)


def test_job_spec_compatibility_is_optional_and_round_trips() -> None:
    base = JobSpec(name="test", workflow="tests.example", runner_path="files/runner")
    assert "compatibility" not in base.as_mapping()

    compatibility = {"profile": "test-v1", "nested": {"value": 1}}
    mapping = JobSpec(
        name="test",
        workflow="tests.example",
        runner_path="files/runner",
        compatibility=compatibility,
    ).as_mapping()
    assert mapping["compatibility"] == compatibility
    assert JobDefinition.from_mapping(mapping).raw["compatibility"] == compatibility


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


def test_every_packaged_runner_has_a_workflow_that_says_what_it_implements() -> None:
    """The workflow table and the packaged runners cannot drift apart.

    The workflow and the steps of a packaged workflow are declared in the table
    rather than asked of the runner on every call, so what holds the two together
    is this: every packaged runner is described here, by running it, and its own
    answer is what the table must contain.
    """

    workflows = [registered_workflow(name) for name in registered_workflows()]
    assert {workflow.packaged for workflow in workflows if workflow} == set(RUNNERS)
    for workflow in workflows:
        assert workflow is not None
        described = describe_runner(workflow.source)
        assert described["workflow"] == workflow.workflow_id
        assert described["steps"] == sorted(workflow.steps)
        assert workflow.inputs == {"structure": "POSCAR"}
        assert workflow.initial_step in workflow.steps

    expected = {
        "httk.vasp.relax": "vasp-relax",
        "httk.vasp.relax-bash": "vasp-relax",
        "httk.vasp.static": "vasp-static",
        "httk.vasp.relax-static": "vasp-relax-static",
    }
    for workflow_id, workflow_name in expected.items():
        workflow = registered_workflow(workflow_id)
        assert workflow is not None
        assert (
            workflow.declarations["workflow"]["$id"] == f"https://schemas.httk.org/defs/v0.1/workflows/{workflow_name}"
        )

    # A packaged runner is nameable by its own file name as well as by its workflow
    # name, and both resolve to exactly the installed bytes.
    workflow = resolve_workflow("vasp-relax")
    assert workflow.source == runner_path("vasp_relax.py")
    assert workflow.workflow_id == "httk.vasp.relax" and workflow.initial_step == "prepare"
    assert workflow.data_mode == "transactional"
    with pytest.raises(ValueError, match="no such file: vasp_relax.py"):
        resolve_workflow("vasp_relax.py")
    assert resolve_workflow("vasp-relax-static", step="static").initial_step == "static"
    with pytest.raises(ValueError, match="does not implement the step 'prepear'"):
        resolve_workflow("vasp-relax", step="prepear")
    with pytest.raises(ValueError, match="unknown workflow"):
        resolve_workflow("vasp-nonexistent")


def test_unknown_workflow_suggests_a_close_match_and_lists_aliases() -> None:
    with pytest.raises(ValueError) as excinfo:
        resolve_workflow("vasp-relx")
    message = str(excinfo.value)
    assert "did you mean 'vasp-relax'?" in message
    assert "httk.vasp.relax (vasp-relax)" in message


def test_path_shaped_unknown_workflow_reports_no_such_file() -> None:
    with pytest.raises(ValueError, match="no such file: does/not/exist.py"):
        resolve_workflow("does/not/exist.py")


def test_workflow_declarations_are_forwarded_and_digest_covered(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    declarations = {"workflow": {"$id": "https://example.test/workflows/v1", "nested": {"value": 7}}}
    provider = WorkflowProvider(
        workflow_id="tests.declarations",
        alias="test-declarations",
        runner_package=PACKAGE,
        runner_file="vasp_relax.py",
        initial_step="prepare",
        steps=("publish", "prepare", "run"),
        declarations=declarations,
    )
    monkeypatch.setitem(scaffold._WORKFLOW_PROVIDERS, provider.workflow_id, provider)

    job = new_job(workspace, provider.alias or provider.workflow_id)
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.declarations == declarations
    assert definition.digest == sha256_file(job.payload / "job.json")


def test_a_scaffolded_job_publishes_its_runner_by_content(workspace: Workspace, structure: Path) -> None:
    job = new_job(
        workspace, "vasp-relax", files={"POSCAR": structure}, tag="silicon", parameters={"kpoint_density": 30.0}
    )

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
    assert json.loads((job.payload / "job.json").read_text(encoding="utf-8"))["runner"]["executor"] == "path"
    assert definition.data_mode == "transactional" and definition.workdir_mode == "persistent"
    assert definition.parameters == {"kpoint_density": 30.0}
    assert workspace.find_marker_by_id(job.job_id) is not None

    # Scaffolding a second job publishes nothing new: identical bytes are one
    # store entry, which is what makes one runner serve a whole campaign.
    again = new_job(workspace, "vasp-relax", files={"POSCAR": structure}, tag="silicon")
    assert again.runner == job.runner
    assert sorted(path.name for path in workspace.runners.iterdir()) == [f"vasp_relax.{digest[:12]}.py"]


def test_a_path_parameter_lands_at_the_declared_payload_destination(workspace: Workspace, structure: Path) -> None:
    job = new_job(workspace, "vasp-relax", inputs={"structure": structure})
    assert (job.payload / "files" / "POSCAR").read_text(encoding="utf-8") == _POSCAR
    assert "parameters" not in json.loads((job.payload / "job.json").read_text(encoding="utf-8"))


def test_parameter_validation_and_realization_fail_before_submission(tmp_path: Path, workspace: Workspace) -> None:
    with pytest.raises(ValueError, match="declared inputs: structure"):
        new_job(workspace, "vasp-relax", inputs={"unknown": object()})

    runner = tmp_path / "hook.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.hook', inputs={'x': None})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="requires an instantiate hook"):
        new_job(workspace, runner, inputs={"x": object()})
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
        "run = Runner('tests.hook', inputs={'structure': 'POSCAR', 'note': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx):\n"
        "    assert (ctx.payload / 'files' / 'POSCAR').is_file()\n"
        "    (ctx.payload / 'files' / 'generated.txt').write_text(ctx.inputs['note'], encoding='utf-8')\n"
        "    ctx.parameters['derived'] = ctx.inputs['note']\n"
        "    if ctx.inputs['note'] == 'one': ctx.suggest_tag('suggested')\n"
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
            [{"inputs": {"note": "one"}}, {"inputs": {"note": "two"}, "tag": "explicit"}],
            inputs={"structure": structure},
            parameters={"shared": True},
        )
    )
    assert counter.read_text(encoding="utf-8") == "1"
    assert (jobs[0].payload / "files" / "generated.txt").read_text(encoding="utf-8") == "one"
    assert (jobs[1].payload / "files" / "generated.txt").read_text(encoding="utf-8") == "two"
    assert jobs[0].tag == "suggested" and jobs[1].tag == "explicit"
    assert json.loads((jobs[0].payload / "job.json").read_text(encoding="utf-8"))["parameters"] == {
        "shared": True,
        "derived": "one",
    }


def test_an_instantiate_hook_failure_leaves_no_submission_or_scratch(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "raising.py"
    runner.write_text(
        "import json\n"
        "from httk.workflow import Runner\n"
        "run = Runner('tests.raising', inputs={'note': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx): raise RuntimeError('hook failed')\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="hook failed"):
        new_job(workspace, runner, inputs={"note": "value"})
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
        "run = Runner('tests.verified', inputs={'value': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx):\n"
        "    (ctx.payload / 'verified.txt').write_text(ctx.inputs['value'], encoding='utf-8')\n"
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
    job = new_job(workspace, runner, inputs={"value": "verified"})
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
        "run = Runner('tests.no-hook', inputs={'x': None})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="requires an instantiate hook"):
        new_job(workspace, runner, inputs={"x": "value"})
    plain = tmp_path / "plain.py"
    plain.write_text(_SINGLE_STEP_RUNNER, encoding="utf-8")
    assert resolve_workflow(plain).instantiate is False


def test_an_object_parameter_is_serialized_and_a_failing_save_leaves_no_scratch(
    tmp_path: Path, workspace: Workspace
) -> None:
    runner = tmp_path / "object.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.object', inputs={'data': 'files/data.testfmt'})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    job = new_job(workspace, runner, inputs={"data": {"value": 7}})
    assert (job.payload / "files" / "data.testfmt").read_text(encoding="utf-8") == "{'value': 7}"
    with pytest.raises(RuntimeError, match="test serializer failed"):
        new_job(workspace, runner, inputs={"data": {"raise": True}})
    assert len(list(workspace.scan_markers())) == 1
    assert list((workspace.control / "tmp").iterdir()) == []


def test_an_object_parameter_requires_a_registered_writer(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "missing.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.missing', inputs={'data': 'files/data.no-writer'})\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="httk-atomistic"):
        new_job(workspace, runner, inputs={"data": object()})


def test_a_runner_without_a_parameter_description_has_an_empty_declaration(tmp_path: Path) -> None:
    runner = tmp_path / "plain.py"
    runner.write_text(_SINGLE_STEP_RUNNER, encoding="utf-8")
    assert resolve_workflow(runner).parameters == {}


def test_the_installed_form_references_a_packaged_runner_without_copying(workspace: Workspace, structure: Path) -> None:
    job = new_job(
        workspace,
        "vasp-static",
        files={"POSCAR": structure},
        publish="installed",
        workflow_id="tests.override.static",
    )

    assert job.runner["source"] == "installed"
    assert job.runner["path"] == f"pkg:{PACKAGE}/vasp_static.py"
    assert job.workflow == "tests.override.static"
    assert not list(workspace.runners.iterdir())
    assert job.tag is None and job.job_key == job.job_id


def test_a_runner_file_of_ones_own_is_described_and_published(tmp_path: Path, workspace: Workspace) -> None:
    runner = tmp_path / "single.py"
    runner.write_text(_SINGLE_STEP_RUNNER, encoding="utf-8")

    # The runner says what it implements, so nothing has to be declared twice.
    described = describe_runner(runner)
    assert described == {"workflow": "tests.scaffold.single", "steps": ["start"]}
    assert resolve_workflow(runner).declarations == {}
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


def test_describe_runner_scrubs_shared_runner_variables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = tmp_path / "describe.py"
    runner.write_text(
        "import json, os\n"
        "if os.environ.get('HTTK_WORKFLOW_RUNNER_ROOT') or os.environ.get('HTTK_WORKFLOW_RUNNER_ARTIFACTS'):\n"
        "    raise SystemExit('runner variables leaked')\n"
        "print(json.dumps({'workflow': 'tests.scaffold.describe', 'steps': ['start']}))\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HTTK_WORKFLOW_RUNNER_ROOT", "poison-root")
    monkeypatch.setenv("HTTK_WORKFLOW_RUNNER_ARTIFACTS", "poison-artifacts")
    assert describe_runner(runner) == {"workflow": "tests.scaffold.describe", "steps": ["start"]}


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


def test_an_undescribable_workflow_is_refused_by_name(tmp_path: Path, workspace: Workspace) -> None:
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


def test_a_transactional_workflow_works_in_a_core_workspace(tmp_path: Path, structure: Path) -> None:
    plain = Workspace.initialize(tmp_path / "plain")
    job = new_job(plain, "vasp-relax", files={"POSCAR": structure})
    assert JobDefinition.from_path(job.payload / "job.json").data_mode == "transactional"

    # The same workflow can leave results in the workdir when requested.
    job = new_job(plain, "vasp-relax", files={"POSCAR": structure}, data_mode="none")
    assert JobDefinition.from_path(job.payload / "job.json").data_mode == "none"


def test_a_campaign_publishes_one_runner_and_yields_jobs_lazily(workspace: Workspace, tmp_path: Path) -> None:
    directory = tmp_path / "structures"
    directory.mkdir()
    for name in ("POSCAR.Si2O", "POSCAR.fcc-Al", "mp-149.vasp", "notes.txt"):
        (directory / name).write_text(_POSCAR, encoding="utf-8")

    assert [path.name for path in structure_files(directory)] == ["POSCAR.Si2O", "POSCAR.fcc-Al", "mp-149.vasp"]
    items: list[JobItem] = [
        {"files": {"POSCAR": path}, "tag": structure_tag(path), "parameters": {"index": index}}
        for index, path in enumerate(structure_files(directory))
    ]
    campaign = new_jobs(
        workspace,
        "vasp-relax",
        iter(items),
        parameters={"kpoint_density": 15.0},
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
    assert definition.parameters == {"kpoint_density": 15.0, "index": 1}
    assert len(list(workspace.scan_markers())) == 3


def test_a_staged_name_lands_where_the_runner_reads_it(workspace: Workspace, structure: Path, tmp_path: Path) -> None:
    assert payload_relative("POSCAR").as_posix() == "files/POSCAR"
    assert payload_relative("inputs/potcars/POTCAR").as_posix() == "inputs/potcars/POTCAR"
    assert payload_relative("files/logs/x").as_posix() == "files/logs/x"
    assert payload_relative("files/job.json").as_posix() == "files/job.json"
    for refused in (
        "",
        "../escape",
        "/absolute/POSCAR",
        ".httk-job/state.json",
        "attempts/attempt",
        "logs/x",
        "attempts/x",
    ):
        with pytest.raises(ValueError):
            payload_relative(refused)

    incar = tmp_path / "INCAR"
    incar.write_text("ENCUT = 300\n", encoding="utf-8")
    job = new_job(
        workspace,
        "vasp-relax",
        files={"POSCAR": structure, "INCAR": incar, "reference/notes.txt": incar, "files/logs/x": incar},
        tag="staged",
    )
    assert (job.payload / "files" / "INCAR").read_text(encoding="utf-8") == "ENCUT = 300\n"
    assert (job.payload / "reference" / "notes.txt").is_file()
    assert (job.payload / "files" / "logs" / "x").read_text(encoding="utf-8") == "ENCUT = 300\n"

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
                "--workspace",
                ws_name,
                "--workflow",
                "vasp-relax",
                "--input",
                f"structure={structure}",
                "--tag",
                "silicon",
                "--parameter",
                "kpoint_density=30.0",
                "--parameter",
                'incar_tags={"ENCUT": 520}',
                "--parameter",
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
    assert definition.parameters == {
        "kpoint_density": 30.0,
        "incar_tags": {"ENCUT": 520},
        "remedy_policy": "reviewed-v1",
    }
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
                "--workspace",
                ws_name,
                "--workflow",
                "vasp-relax",
                "--input-from",
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


def test_command_workflow_is_generated_published_once_and_runs(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "command-workspace")
    workspace_name = register_ws(_context(tmp_path), workspace.root, "command")
    arguments = [
        "job",
        "new",
        "--workspace",
        workspace_name,
        "--from-command",
        "echo command-sentinel-{n}",
        "--parameter",
        "n=17",
        "--json",
    ]

    assert command(arguments, _context(tmp_path)) == 0
    first = json.loads(capsys.readouterr().out)[0]
    runner = workspace.runner_store_path(str(first["runner"]["path"]))
    runner_text = runner.read_text(encoding="utf-8")
    assert "Generated by `httk job new --from-command`; edit and pass with --from-runner to customize." in runner_text
    assert 'httk_workflow_run -- echo "command-sentinel-$(httk_workflow_parameter n)"' in runner_text
    assert first["runner"]["path"].startswith("command/")
    assert first["tag"] == "n17"

    assert command(arguments, _context(tmp_path)) == 0
    second = json.loads(capsys.readouterr().out)[0]
    assert second["runner"] == first["runner"]
    assert sorted(path.relative_to(workspace.runners).as_posix() for path in workspace.runners.rglob("*.sh")) == [
        first["runner"]["path"]
    ]

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    for report in (first, second):
        payload = Path(report["payload_path"])
        marker = workspace.find_marker_by_id(report["job_id"])
        assert marker is not None and marker.kind == "succeeded"
        assert "command-sentinel-17\n" in (payload / "logs" / "stdio.out").read_text(encoding="utf-8")


def test_command_template_grammar_and_argv_value_fidelity(tmp_path: Path, capsys) -> None:
    rendered = _command_runner_text(
        r'''echo '{"x":1}' '{{n}}' '--n={n}' '{not a parameter}' ''',
        {"n": 17},
    )
    assert "'{\"x\":1}'" in rendered
    assert "'{n}'" in rendered
    assert '"--n=$(httk_workflow_parameter n)"' in rendered
    assert "'{not a parameter}'" in rendered

    workspace = Workspace.initialize(tmp_path / "command-values")
    workspace_name = register_ws(_context(tmp_path), workspace.root, "command-values")
    value = 'value-sentinel-quote"-$(`backtick`)-line\nnext'
    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace_name,
                "--from-command",
                r'''bash -c 'printf "%s\\n" "$1"' _ {value}''',
                "--parameter",
                f"value={value}",
                "--json",
            ],
            _context(tmp_path),
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)[0]
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    assert value + "\n" in (Path(report["payload_path"]) / "logs" / "stdio.out").read_text(encoding="utf-8")


def test_command_requires_all_placeholders_and_is_mutually_exclusive(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "command-refusals")
    workspace_name = register_ws(_context(tmp_path), workspace.root, "refusals")
    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace_name,
                "--from-command",
                "echo {missing}",
            ],
            _context(tmp_path),
        )
        == 2
    )
    assert "missing" in capsys.readouterr().err

    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace_name,
                "--from-command",
                "echo ok",
                "--workflow",
                "vasp-relax",
            ],
            _context(tmp_path),
        )
        == 2
    )
    assert "not allowed with argument --from-command" in capsys.readouterr().err


def test_workflow_rejects_runner_and_package_paths(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workflow-path-refusals")
    workspace_name = register_ws(_context(tmp_path), workspace.root, "workflow-paths")
    runner = tmp_path / "run.py"
    runner.write_text("# runner\n", encoding="utf-8")
    package = tmp_path / "package"
    package.mkdir()

    assert command(["job", "new", "--workspace", workspace_name, "--workflow", str(runner)], _context(tmp_path)) == 2
    assert "--from-runner" in capsys.readouterr().err
    assert command(["job", "new", "--workspace", workspace_name, "--workflow", str(package)], _context(tmp_path)) == 2
    assert "--workflow-dir" in capsys.readouterr().err
    assert command(["job", "new", "--workspace", workspace_name, "--workflow", "./missing.py"], _context(tmp_path)) == 2
    assert "--from-runner" in capsys.readouterr().err


@pytest.mark.parametrize("shebang", ["#!/bin/bash -e", "#!/usr/bin/env bash -e", "#!/usr/bin/env -S bash -e"])
def test_bash_shebang_variants_are_detected(tmp_path: Path, shebang: str) -> None:
    runner = tmp_path / "runner"
    runner.write_text(f"{shebang}\n", encoding="utf-8")
    assert _has_bash_shebang(runner)


def test_bash_runner_without_registration_names_the_missing_call(tmp_path: Path) -> None:
    runner = tmp_path / "unregistered.sh"
    runner.write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nsource \"$HTTK_WORKFLOW_BASH_API\"\nhttk_workflow_main\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="exited without calling httk_workflow_runner") as error:
        resolve_workflow(runner)
    assert str(runner.resolve()) in str(error.value)


def test_a_bash_runner_file_is_described_published_and_runs(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "bash-workspace")
    workspace_name = register_ws(_context(tmp_path), workspace.root, "bash")
    runner = tmp_path / "run.sh"
    runner.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'source "$HTTK_WORKFLOW_BASH_API"\n'
        "httk_workflow_runner bash_file zebra alpha\n"
        "\n"
        "step_zebra() {\n"
        "    echo bash-runner\n"
        "    httk_workflow_succeed\n"
        "}\n"
        "\n"
        "step_alpha() {\n"
        "    httk_workflow_succeed\n"
        "}\n"
        "\n"
        "httk_workflow_main\n",
        encoding="utf-8",
    )
    assert (
        command(
            ["job", "new", "--workspace", workspace_name, "--from-runner", str(runner), "--json"],
            _context(tmp_path),
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)[0]
    assert report["initial_step"] == "zebra"
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(report["job_id"])
    assert marker is not None and marker.kind == "succeeded"
    assert "bash-runner" in (Path(report["payload_path"]) / "logs" / "stdio.out").read_text(encoding="utf-8")


def test_the_command_reports_what_it_cannot_do(
    tmp_path: Path,
    structure: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    name = "refusals"
    root = tmp_path / name
    assert command(["workspace", "init", str(root)], _context(tmp_path)) == 0
    capsys.readouterr()

    # A malformed assignment, an unknown workflow, and an empty structure directory.
    assert (
        command(["job", "new", "--workspace", name, "--workflow", "vasp-relax", "--input", "bare"], _context(tmp_path))
        == 2
    )
    assert "NAME=VALUE" in capsys.readouterr().err
    assert command(["job", "new", "--workspace", name, "--workflow", "nope"], _context(tmp_path)) == 2
    assert "unknown workflow" in capsys.readouterr().err
    empty = tmp_path / "empty"
    empty.mkdir()
    assert (
        command(
            ["job", "new", "--workspace", name, "--workflow", "vasp-relax", "--input-from", "structure", str(empty)],
            _context(tmp_path),
        )
        == 2
    )
    assert "no readable input files" in capsys.readouterr().err


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
                "--workspace",
                name,
                "--workflow",
                "vasp-relax",
                "--input-from",
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
        "run = Runner('tests.cli-object', inputs={'data': 'data.testfmt'})\n"
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
                "--workspace",
                name,
                "--from-runner",
                str(runner),
                "--input-from",
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

    # A packaged VASP workflow accepts the same source shape; two batch sources
    # are refused before either job is submitted.
    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                name,
                "--workflow",
                "vasp-relax",
                "--input-from",
                "structure",
                str(directory),
                "--input-from",
                "structure",
                str(directory),
            ],
            _context(tmp_path),
        )
        == 2
    )
    assert "only one --input-from" in capsys.readouterr().err
    assert (
        command(
            ["job", "new", "--workspace", name, "--workflow", "vasp-relax", "--from", str(structure)],
            _context(tmp_path),
        )
        == 2
    )
    assert "ambiguous option: --from" in capsys.readouterr().err


def test_parameter_from_cif_is_written_as_a_poscar_when_domain_plugins_are_available(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
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
    assert command(["workspace", "init", "--name", "cif", str(workspace)], _context(tmp_path)) == 0
    capsys.readouterr()
    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                "cif",
                "--workflow",
                "vasp-relax",
                "--input-from",
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
