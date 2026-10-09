"""Fabricated attempts of v3 jobs for the SDK and bridge tests: a real workspace, no manager.

An attempt is a payload with a v3 ``job.json``, an ``attempts/<A>`` control
directory, a ``run/`` workdir and the environment a manager would give the
runner. The workspace beside it is initialized, so a parent job (``parent=True``)
can be found by its placement and installed workflows (``calls=``) can be called.
"""

import functools
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from httk.workflow import Workspace, _kernel, _store, scaffold
from httk.workflow._state import StateDoc, read_state_unowned
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload

BASH_API = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "languages" / "bash" / "httk-workflow.sh"

#: The runner of the ``tests.sub`` workflow the call tests install and call.
CALL_SUB_RUNNER = """#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("tests.sub")


@run.step
def run_sub(a):
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
"""


@dataclass(frozen=True)
class FabricatedAttempt:
    """One fabricated attempt a runner can be dispatched into.

    :param root: The directory holding everything of this attempt.
    :param payload: The job directory.
    :param control: ``attempts/<A>`` of the job.
    :param workdir: ``run/`` of the job.
    :param workspace: The initialized workspace beside the job.
    :param environment: The runner environment.
    """

    root: Path
    payload: Path
    control: Path
    workdir: Path
    workspace: Workspace
    environment: dict[str, str]

    def run(self, program: Path | str, *arguments: str) -> subprocess.CompletedProcess[str]:
        """Run *program* as the attempt's runner, in its workdir."""

        return subprocess.run(
            [str(program), *arguments],
            cwd=self.workdir,
            env=self.environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def outcome(self) -> dict[str, Any]:
        return json.loads((self.control / "outcome.ready" / "outcome.json").read_text(encoding="utf-8"))

    def drafts(self) -> list[Path]:
        return sorted(self.control.glob("outcome.tmp.*"))

    def breadcrumb(self) -> dict[str, Any]:
        return json.loads((self.control / "error.json").read_text(encoding="utf-8"))

    def committed(self) -> list[Path]:
        """The committed transaction trees of this attempt, in sequence order."""

        txn = self.control / "txn"
        return sorted(path for path in txn.iterdir() if path.name.isdigit()) if txn.is_dir() else []


def package(
    root: Path, name: str, steps: Sequence[str], *, runner: str = "#!/bin/sh\nexit 0\n", extra: str = ""
) -> Path:
    """Write a directory package *name* with *steps* whose ``run`` is *runner*."""

    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "{name}"\n\n[workflow.runner]\nsteps = {json.dumps(list(steps))}\n'
        f'initial_step = "{steps[0]}"\n{extra}',
        encoding="utf-8",
    )
    (root / "run").write_text(runner, encoding="utf-8")
    (root / "run").chmod(0o755)
    return root


def sub_package(root: Path) -> Path:
    """The installable package of the ``tests.sub`` workflow."""

    return package(root, "tests.sub", ["run_sub"], runner=CALL_SUB_RUNNER)


def _owner(workspace: Workspace) -> _kernel.Owner:
    return _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})


def _parent(workspace: Workspace) -> dict[str, object]:
    """Submit a waiting parent job at ``project/parent``; return a child's ``parent`` block."""

    owner = _owner(workspace)
    try:
        staging = owner.scratch("submit") / "job"
        spec = JobSpec(
            name="Parent", workflow_id="local:tests.parent", workflow_name="tests.parent", placement="project/parent"
        )
        job = prepare_job_payload(staging, spec)
        _kernel.submit(workspace, owner, staging, state="waiting")
    finally:
        owner.close()
    return {
        "workspace_id": workspace.workspace_id,
        "job_id": job.id,
        "job_key": job.job_key,
        "placement": "project/parent",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }


def fabricate(
    root: Path,
    *,
    step: str,
    workflow: str = "tests.fabricated",
    parameters: Mapping[str, object] | None = None,
    environment: Mapping[str, object] | None = None,
    settings: Mapping[str, object] | None = None,
    children: Sequence[Mapping[str, object]] = (),
    parent: bool = False,
    calls: Mapping[str, Path] | None = None,
) -> FabricatedAttempt:
    """Fabricate one attempt of one job of *workflow*, without a manager.

    :param root: An absent directory to build everything below.
    :param step: The attempt's step.
    :param workflow: The job's workflow name; with *calls* it is installed (steps: *step*).
    :param parameters: The job parameters.
    :param environment: The job's ``environment`` member.
    :param settings: The context settings.
    :param children: The context ``children`` observations.
    :param parent: Give the job a parent job, waiting in the workspace.
    :param calls: Install *workflow* declaring these calls, alias to callee package directory.
    :return: The attempt.
    """

    root.mkdir(parents=True, exist_ok=True)
    workspace = Workspace.initialize(root / "workspace", durable=False)
    workflow_id = f"local:{workflow}"
    if calls is not None:
        owner = _owner(workspace)
        try:
            # A call names an installed workflow, so each callee is installed first and named by its id.
            table = "".join(
                f'{alias} = "{_store.install(workspace, owner, path).id}"\n' for alias, path in calls.items()
            )
            source = package(root / "package", workflow, [step], extra=f"\n[workflow.calls]\n{table}")
            workflow_id = _store.install(workspace, owner, source).id
        finally:
            owner.close()
    payload = root / "payload"
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow_id=workflow_id,
            workflow_name=workflow,
            initial_step=step,
            placement="project/fabricated",
            parameters=dict(parameters or {}),
            environment=dict(environment or {}),
        ),
        parent=_parent(workspace) if parent else None,
    )
    (payload / "files").mkdir()
    control = payload / "attempts" / str(uuid.uuid4())
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context = {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        "workspace_id": workspace.workspace_id,
        "job_id": job.id,
        "job_key": job.job_key,
        "placement": "project/fabricated",
        "payload": str(payload),
        "step": step,
        "activation_id": str(uuid.uuid4()),
        "attempt_id": control.name,
        "children": list(children),
        "settings": dict(settings or {}),
        "durable": False,
        "deadline": None,
    }
    process_environment = os.environ.copy()
    for name in ("HTTK_WORKFLOW_DESCRIBE", "HTTK_WORKFLOW_RUNNER_WORKFLOW", "HTTK_WORKFLOW_RUNNER_STEPS"):
        process_environment.pop(name, None)
    process_environment.update(
        {
            "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(payload),
            "HTTK_WORKFLOW_WORKDIR": str(workdir),
            "HTTK_WORKFLOW_DATA_DIR": str(payload / "data"),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(workspace.root),
            "HTTK_WORKFLOW_STEP": step,
            "HTTK_WORKFLOW_PYTHON": sys.executable,
            "HTTK_WORKFLOW_BASH_API": str(BASH_API),
        }
    )
    return FabricatedAttempt(root, payload, control, workdir, workspace, process_environment)


def child_observation(label: str, kind: str, *, failure: Mapping[str, object] | None = None) -> dict[str, object]:
    """One join observation as the manager writes it into the context, located at ``project/children``."""

    job_key = f"{label}--{uuid.uuid4()}"
    path = f"jobs/{kind}/project/children/{job_key}~p500~aaaaaaaaaaaaaaaa"
    return {
        "label": label,
        "job_id": job_key.split("--", 1)[1],
        "job_key": job_key,
        "kind": kind,
        "state": kind,
        "failure": None if failure is None else dict(failure),
        "placement": "project/children",
        "payload_path": path,
        "workdir_path": f"{path}/run",
        "data_path": f"{path}/data",
    }


def assert_called(attempt: FabricatedAttempt, job_key: str | None = None) -> str:
    """Assert the attempt's outcome registered one ``sub`` child of the installed ``tests.sub``; return its key."""

    ready = attempt.control / "outcome.ready"
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert [entry["label"] for entry in spawn["children"]] == ["sub"]
    key = str(spawn["children"][0]["job_key"])
    assert job_key is None or job_key == key
    child = json.loads((ready / "children" / "jobs" / key / "job.json").read_text(encoding="utf-8"))
    assert child["workflow"] == {"id": "local:tests.sub", "name": "tests.sub"}
    assert child["parent"]["job_id"] == json.loads(attempt.environment["HTTK_WORKFLOW_CONTEXT"])["job_id"]
    return key


def submit_runner(
    workspace: Workspace,
    runner: Path,
    *,
    placement: str = "project/a",
    install_step: str | None = None,
    steps: Sequence[str] | None = None,
    **members: Any,
) -> _kernel.JobRef:
    """Install the runner file *runner* (ad hoc) and submit one job of it built from ``JobSpec(**members)``.

    :param workspace: The workspace.
    :param runner: The runner file; it is described to learn its steps.
    :param placement: The job's placement.
    :param install_step: The ad hoc package's default step, needed when the runner has no ``start``.
    :param steps: Install the runner as a package with these steps instead of describing it.
    :param members: Further :class:`JobSpec` members (``name`` defaults to the runner's stem).
    :return: The submitted job.
    """

    owner = _owner(workspace)
    try:
        source: Path = runner
        if steps is not None:
            source = package(runner.with_name(f"{runner.stem}-package"), runner.stem, steps, runner=runner.read_text())
        installed = _store.install(workspace, owner, source, initial_step=install_step)
        staging = owner.scratch("submit") / "job"
        members.setdefault("name", runner.stem)
        members.setdefault("initial_step", installed.provider().initial_step)
        spec = JobSpec(workflow_id=installed.id, workflow_name=installed.name, placement=placement, **members)
        prepare_job_payload(staging, spec)
        return _kernel.submit(workspace, owner, staging)
    finally:
        owner.close()


def run_manager(workspace: Workspace, timeout: float = 120.0) -> None:
    """Run a manager on *workspace* until it is idle."""

    from httk.workflow import TaskManager

    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=timeout)


def find(workspace: Workspace, job_id: str) -> _kernel.JobRef:
    """Return the one unowned job directory of *job_id*."""

    found = [
        ref for state in _kernel.UNOWNED_STATES for ref in _kernel.list_jobs(workspace, state) if ref.job_id == job_id
    ]
    assert len(found) == 1, found
    return found[0]


def every_job(workspace: Workspace) -> list[_kernel.JobRef]:
    """Return every unowned job directory of *workspace*."""

    return [ref for state in _kernel.UNOWNED_STATES for ref in _kernel.list_jobs(workspace, state)]


def state(ref: _kernel.JobRef) -> StateDoc:
    """Return the ``state.json`` of a job directory."""

    doc, damaged = read_state_unowned(ref.path / "state.json")
    assert doc is not None and not damaged
    return doc


#: ``scaffold.new_job``/``new_jobs`` installing the workflow first, for tests about what runs, not about installing.
new_job = functools.partial(scaffold.new_job, install=True)
new_jobs = functools.partial(scaffold.new_jobs, install=True)


def failure_of(ref: _kernel.JobRef) -> dict[str, Any]:
    """Return the failure of a job directory's ``state.json`` as a plain mapping (``{}`` when none)."""

    failure = state(ref).failure
    return {} if failure is None else {key: value for key, value in failure.items()}


def job_json(directory: Path) -> dict[str, Any]:
    """Return the decoded ``job.json`` of a job directory."""

    return dict(json.loads((directory / "job.json").read_text(encoding="utf-8")))
