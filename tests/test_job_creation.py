"""Job creation on the v3 job format: installed workflows, ad hoc installs and the submitted payload.

The helpers here (:func:`install`, :func:`raw_package`, :data:`RAW_RUNNER`) are shared by the other
job-creation test modules.
"""

import json
import os
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow._job import JobDefinition
from httk.workflow.scaffold import new_job, new_jobs, scaffold_job

#: A runner that needs no SDK: it records its step and the runner variables, then succeeds.
RAW_RUNNER = """#!/usr/bin/env python3
import json, os
from pathlib import Path, PurePosixPath

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
names = ("HTTK_WORKFLOW_RUNNER_ROOT", "HTTK_WORKFLOW_RUNNER_ARTIFACTS", "HTTK_WORKFLOW_DATA_DIR")
record = {"step": context["step"], **{name: os.environ.get(name) for name in names}}
(Path(os.environ["HTTK_WORKFLOW_WORKDIR"]) / "ran.json").write_text(json.dumps(record))
draft = control / "outcome.tmp.test"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
(draft / "outcome.json").write_text(json.dumps(outcome))
os.rename(draft, control / "outcome.ready")
"""


def workspace_at(path: Path) -> Workspace:
    """Initialize a workspace with a short visibility deadline."""

    return Workspace.initialize(path, durable=False, policy={"visibility_deadline_seconds": 0.05})


def install(ws: Workspace, source: str | os.PathLike[str], **options: object) -> _store.Installed:
    """Install *source* as a CLI owner registered (and closed) for the call."""

    owner = _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})
    try:
        return _store.install(ws, owner, source, **options)  # type: ignore[arg-type]
    finally:
        owner.close()


def raw_package(root: Path, name: str = "tests.raw", *, runner: str = RAW_RUNNER, extra: str = "") -> Path:
    """Write a one-step directory package whose ``run`` is *runner*."""

    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "{name}"\n\n[workflow.runner]\nsteps = ["start"]\n{extra}', encoding="utf-8"
    )
    (root / "run").write_text(runner, encoding="utf-8")
    (root / "run").chmod(0o755)
    return root


def ready(ws: Workspace) -> list[_kernel.JobRef]:
    return list(_kernel.list_jobs(ws, "ready"))


def assert_nothing_left(ws: Workspace) -> None:
    """No CLI owner record and no scratch outlive a job creation call."""

    assert not [item for item in _kernel.list_owners(ws) if item.record is not None]
    tmp = ws.control / "tmp"
    assert not tmp.exists() or list(tmp.iterdir()) == []


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return workspace_at(tmp_path / "ws")


def test_an_uninstalled_workflow_is_refused_with_the_install_hint(ws: Workspace, tmp_path: Path) -> None:
    package = raw_package(tmp_path / "package")
    for workflow in ("tests.nosuch", package):
        with pytest.raises(ValueError, match="is not installed in the workspace .*httk workflow install"):
            new_job(ws, workflow)
    assert ready(ws) == [] and _store.list_installed(ws) == []
    assert_nothing_left(ws)


def test_install_true_installs_first_and_later_jobs_find_the_installation(ws: Workspace, tmp_path: Path) -> None:
    package = raw_package(tmp_path / "package")
    first = new_job(ws, package, install=True)
    (installed,) = _store.list_installed(ws)
    assert installed.id == "local:tests.raw" == first.workflow and first.workflow_name == "tests.raw"
    # The directory, the short name and the id all find the installation without installing again.
    for workflow in (package, "tests.raw", "local:tests.raw"):
        assert new_job(ws, workflow).workflow == installed.id
    assert _store.list_installed(ws)[0].record["installed_at"] == installed.record["installed_at"]
    assert len(ready(ws)) == 4
    assert_nothing_left(ws)


def test_the_payload_is_a_v3_job_with_the_installed_workflow_and_no_runner(ws: Workspace, tmp_path: Path) -> None:
    package = raw_package(
        tmp_path / "package",
        extra='\n[workflow.resources]\nmaxtime = "1:00:00"\nprocs = 2\n\n[workflow.steps.start.resources]\nmintime = "10"\n',
    )
    installed = install(ws, package)
    staged = tmp_path / "INCAR"
    staged.write_text("ENCUT = 300\n", encoding="utf-8")
    job = new_job(ws, "tests.raw", files={"INCAR": staged}, tag="silicon", placement="project/a", priority=7)

    assert job.ref.state == "ready" and job.payload == job.ref.path
    assert job.payload.parent == ws.jobs / "ready" / "project" / "a"
    assert job.job_key == f"silicon--{job.job_id}" and job.placement.as_posix() == "project/a"
    assert job.as_mapping()["workflow"] == {"id": installed.id, "name": "tests.raw"}
    assert sorted(path.relative_to(job.payload).as_posix() for path in job.payload.rglob("*")) == [
        "files",
        "files/INCAR",
        "job.json",
    ]
    document = json.loads((job.payload / "job.json").read_bytes())
    assert document["format_version"] == 3
    assert not {"runner", "workdir", "data", "requires", "calls"} & set(document)
    assert document["workflow"] == {"id": installed.id, "name": "tests.raw"}
    assert document["placement"] == "project/a" and document["priority"] == 7 and document["parent"] is None
    assert document["resources"] == {"maxtime": 3600, "procs": 2}
    assert document["step_resources"] == {"start": {"mintime": 600}}
    assert document["seal_succeeded"] is None
    assert JobDefinition.from_path(job.payload / "job.json").name == "tests.raw: silicon"
    assert_nothing_left(ws)


@pytest.mark.parametrize(("value", "expected"), [("true", True), ("false", False)])
def test_the_manifest_seal_opt_out_is_baked_into_job_json(
    ws: Workspace, tmp_path: Path, value: str, expected: bool
) -> None:
    package = raw_package(tmp_path / "package", extra=f"seal_succeeded = {value}\n")
    job = new_job(ws, package, install=True)
    assert JobDefinition.from_path(job.payload / "job.json").seal_succeeded is expected


def test_a_malformed_seal_opt_out_is_refused_at_install(ws: Workspace, tmp_path: Path) -> None:
    package = raw_package(tmp_path / "package", extra='seal_succeeded = "no"\n')
    with pytest.raises(ValueError, match=r"seal_succeeded must be a boolean"):
        install(ws, package)


def test_a_runner_file_is_installed_as_an_adhoc_workflow(ws: Workspace, tmp_path: Path) -> None:
    runner = tmp_path / "two.py"
    runner.write_text(
        "import json\nprint(json.dumps({'workflow': 'tests.two', 'steps': ['finish', 'prepare']}))\n",
        encoding="utf-8",
    )
    # Several steps and none of them 'start': the starting step must be named, and nothing is installed.
    with pytest.raises(ValueError, match="none of them is 'start'"):
        new_job(ws, runner)
    with pytest.raises(ValueError, match="does not implement the step 'begin'"):
        new_job(ws, runner, step="begin")
    assert _store.list_installed(ws) == []
    job = new_job(ws, runner, step="prepare")
    (installed,) = _store.list_installed(ws)
    assert installed.id.startswith("adhoc:two@") and installed.name == "two"
    assert job.workflow == installed.id and job.initial_step == "prepare"
    assert installed.provider().steps == ("finish", "prepare")
    # The same bytes are the same ad hoc id: installing again replaces, never duplicates.
    assert new_job(ws, runner, step="finish").workflow == installed.id
    assert len(_store.list_installed(ws)) == 1
    assert_nothing_left(ws)


def test_a_command_runner_is_installed_as_an_adhoc_workflow(ws: Workspace, tmp_path: Path) -> None:
    command = tmp_path / "command.sh"
    command.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'source "$HTTK_WORKFLOW_BASH_API"\n'
        "httk_workflow_runner command run\n"
        "step_run() {\n"
        "    httk_workflow_run -- echo hello\n"
        "    httk_workflow_succeed\n"
        "}\n"
        "httk_workflow_main\n",
        encoding="utf-8",
    )
    job = new_job(ws, command)
    (installed,) = _store.list_installed(ws)
    assert installed.id.startswith("adhoc:command@") and job.initial_step == "run"
    assert (installed.package / "run").read_bytes() == command.read_bytes()
    assert os.access(installed.package / "run", os.X_OK)


def test_an_adhoc_runner_keeps_its_inputs_and_instantiate_hook(ws: Workspace, tmp_path: Path) -> None:
    runner = tmp_path / "hooked.py"
    runner.write_text(
        "from httk.workflow import Runner\n"
        "run = Runner('tests.hooked', inputs={'structure': 'POSCAR', 'note': None})\n"
        "@run.instantiate\n"
        "def instantiate(ctx):\n"
        "    ctx.parameters['note'] = ctx.inputs['note']\n"
        "@run.step\n"
        "def start(a): a.succeed()\n"
        "if __name__ == '__main__': raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    structure = tmp_path / "POSCAR"
    structure.write_text("structure\n", encoding="utf-8")
    jobs = list(
        new_jobs(
            ws, runner, [{"inputs": {"note": "one"}}, {"inputs": {"note": "two"}}], inputs={"structure": structure}
        )
    )
    assert [JobDefinition.from_path(job.payload / "job.json").parameters for job in jobs] == [
        {"note": "one"},
        {"note": "two"},
    ]
    assert (jobs[0].payload / "files" / "POSCAR").read_text(encoding="utf-8") == "structure\n"
    (installed,) = _store.list_installed(ws)
    assert installed.provider().instantiate_file == "instantiate.py"
    with pytest.raises(ValueError, match="unknown workflow input 'other'"):
        new_job(ws, runner, inputs={"other": 1})


def test_a_failing_job_leaves_no_job_scratch_or_owner(ws: Workspace, tmp_path: Path) -> None:
    package = raw_package(tmp_path / "package")
    install(ws, package)
    with pytest.raises(ValueError, match="does not exist"):
        new_job(ws, "tests.raw", files={"INCAR": tmp_path / "absent"})
    # A campaign that fails at its second item keeps the first job.
    items = [{"tag": "one"}, {"files": {"INCAR": tmp_path / "absent"}}]
    with pytest.raises(ValueError, match="does not exist"):
        list(new_jobs(ws, "tests.raw", items))  # type: ignore[arg-type]
    assert [ref.job_key.split("--")[0] for ref in ready(ws)] == ["one"]
    assert_nothing_left(ws)


def test_an_abandoned_campaign_closes_its_owner(ws: Workspace, tmp_path: Path) -> None:
    install(ws, raw_package(tmp_path / "package"))
    campaign = new_jobs(ws, "tests.raw", ({"tag": f"j{index}"} for index in range(5)))
    next(campaign)
    campaign.close()
    assert len(ready(ws)) == 1
    assert_nothing_left(ws)


def test_scaffold_job_builds_from_an_installed_workflow_only(ws: Workspace, tmp_path: Path) -> None:
    runner = tmp_path / "runner.py"
    runner.write_text(RAW_RUNNER, encoding="utf-8")
    destination = tmp_path / "prepared"
    destination.mkdir()
    with pytest.raises(ValueError, match="is not installed"):
        scaffold_job(ws, runner, destination)
    install(ws, raw_package(tmp_path / "package"))
    definition = scaffold_job(ws, "tests.raw", destination, tag="child", maxtime_cap=60)
    assert definition.workflow_id == "local:tests.raw" and definition.placement == PurePosixPath()
    assert definition.resources == {"maxtime": 60}
    assert JobDefinition.from_path(destination / "job.json") == definition
    with pytest.raises(ValueError, match="empty directory"):
        scaffold_job(ws, "tests.raw", destination)
    assert ready(ws) == []


@pytest.mark.slow
def test_jobs_of_one_installed_workflow_run_its_installed_package(ws: Workspace, tmp_path: Path) -> None:
    installed = install(ws, raw_package(tmp_path / "package"))
    jobs = list(new_jobs(ws, installed.id, [{"tag": "first"}, {"tag": "second"}], placement="project"))
    with TaskManager(ws) as manager:
        manager.run_until_idle(timeout=60)
    done = sorted(_kernel.list_jobs(ws, "succeeded"), key=lambda ref: ref.job_key)
    assert sorted(ref.job_id for ref in done) == sorted(job.job_id for job in jobs)
    for ref in done:
        record = json.loads((ref.path / "run" / "ran.json").read_text(encoding="utf-8"))
        assert record["step"] == "start"
        assert record["HTTK_WORKFLOW_RUNNER_ROOT"] == str(installed.package)
        assert record["HTTK_WORKFLOW_RUNNER_ARTIFACTS"] is None
        assert Path(record["HTTK_WORKFLOW_DATA_DIR"]).name == "data"
