"""``Attempt.call`` spawns a job of another installed workflow as a child job.

The end-to-end test drives a real :class:`~httk.workflow.TaskManager`: a parent
workflow calls a *second* installed workflow by the alias its ``[workflow.calls]``
declares, and the manager runs the child (that other workflow) and resumes the
parent at its gather step. The other tests call from a fabricated attempt and
inspect the child the call registered without running it.
"""

import json
from pathlib import Path

import pytest

from attempt_fixtures import (
    assert_called,
    every_job,
    fabricate,
    find,
    package,
    run_manager,
    sub_package,
)
from httk.workflow import Attempt, Workspace, _kernel, _store
from httk.workflow._job import JobDefinition
from httk.workflow.scaffold import new_job

_SUB_RUNNER = """#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("tests.sub")


@run.step
def run_sub(a):
    text = (a.payload / "files" / "input.txt").read_text(encoding="utf-8")
    (a.workdir / "seen.txt").write_text("sub-saw:" + text, encoding="utf-8")
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
"""

_PARENT_RUNNER = """#!/usr/bin/env python3
import json

from httk.workflow import Runner

run = Runner("tests.caller")


@run.step
def start(a):
    reference = a.call("sub", label="sub", files={"input.txt": "@INPUT@"})
    (a.workdir / "sub-id.txt").write_text(reference.job_id, encoding="utf-8")
    a.gather("finish", on_impossible="triage")


@run.step
def finish(a):
    sub = a.children["sub"]
    (a.workdir / "result.json").write_text(json.dumps({"kind": sub.kind, "label": sub.label}), encoding="utf-8")
    a.succeed()


@run.step
def triage(a):
    a.fail("caller.dependency", "the called workflow did not succeed")


if __name__ == "__main__":
    raise SystemExit(run.main())
"""


def _install(workspace: Workspace, source: Path, **options: object) -> _store.Installed:
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        return _store.install(workspace, owner, source, **options)  # type: ignore[arg-type]
    finally:
        owner.close()


@pytest.mark.timing
def test_call_runs_another_installed_workflow_and_resumes_at_the_gather(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    input_file = tmp_path / "input.txt"
    input_file.write_text("hello-from-parent\n", encoding="utf-8")
    sub = package(tmp_path / "sub", "tests.sub", ["run_sub"], runner=_SUB_RUNNER)
    caller = package(
        tmp_path / "caller",
        "tests.caller",
        ["start", "finish", "triage"],
        runner=_PARENT_RUNNER.replace("@INPUT@", str(input_file)),
        extra='\n[workflow.calls]\nsub = "tests.sub"\n',
    )
    _install(workspace, sub)
    _install(workspace, caller)
    job = new_job(workspace, "tests.caller", placement="project/caller")

    run_manager(workspace, timeout=60.0)

    parent = find(workspace, job.job_id)
    assert parent.state == "succeeded"
    # finish ran after the child succeeded, and saw it under a.children.
    assert json.loads((parent.path / "run" / "result.json").read_text()) == {"kind": "succeeded", "label": "sub"}

    child_id = (parent.path / "run" / "sub-id.txt").read_text(encoding="utf-8").strip()
    child = find(workspace, child_id)
    assert child.state == "succeeded"
    # The child ran the *other* installed workflow, not the parent's.
    assert JobDefinition.from_path(child.path / "job.json").workflow_id == "local:tests.sub"
    # files= reached the child payload, and the sub runner actually consumed it.
    assert (child.path / "files" / "input.txt").read_text(encoding="utf-8") == "hello-from-parent\n"
    assert (child.path / "run" / "seen.txt").read_text(encoding="utf-8") == "sub-saw:hello-from-parent\n"
    assert len(every_job(workspace)) == 2


def test_call_by_alias_or_id_builds_the_child_from_the_installed_package(tmp_path: Path) -> None:
    fabricated = fabricate(tmp_path, step="start", calls={"sub": sub_package(tmp_path / "sub")})
    attempt = Attempt.initialize(fabricated.environment)
    (attempt.workdir / "input.txt").write_text("staged\n", encoding="utf-8")

    reference = attempt.call("sub", label="sub", files={"input.txt": attempt.workdir / "input.txt"})
    attempt.gather("start")
    assert assert_called(fabricated, reference.job_key) == reference.job_key
    staged = attempt.control / "outcome.ready" / "children" / "jobs" / reference.job_key
    assert (staged / "files" / "input.txt").read_text(encoding="utf-8") == "staged\n"
    child = JobDefinition.from_path(staged / "job.json")
    assert child.initial_step == "run_sub" and child.tag == "sub"
    # The payload was built in the attempt directory and moved into the draft: nothing is left behind.
    assert sorted(path.name for path in attempt.control.iterdir()) == ["outcome.ready"]

    by_id = fabricate(tmp_path / "by-id", step="start", calls={"sub": sub_package(tmp_path / "by-id-sub")})
    reference = Attempt.initialize(by_id.environment).call("local:tests.sub", label="sub")
    assert reference.job_key.startswith("sub--")


def test_call_refuses_an_undeclared_or_uninstalled_workflow(tmp_path: Path) -> None:
    undeclared = Attempt.initialize(fabricate(tmp_path / "undeclared", step="start", calls={}).environment)
    with pytest.raises(ValueError, match=r"'other' is not declared in \[workflow.calls\] of tests.fabricated"):
        undeclared.call("other", label="other")

    fabricated = fabricate(tmp_path / "uninstalled", step="start", calls={"sub": sub_package(tmp_path / "sub")})
    owner = _kernel.register_owner(fabricated.workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        _store.uninstall(fabricated.workspace, owner, "local:tests.sub")
    finally:
        owner.close()
    attempt = Attempt.initialize(fabricated.environment)
    with pytest.raises(ValueError, match="local:tests.sub is not installed in the workspace; install it"):
        attempt.call("sub", label="sub")
    assert not list(attempt.control.glob("outcome.tmp.*"))
    assert not list(attempt.control.glob("call.*"))
