"""``Attempt.call`` spawns another registered workflow as a child job.

The end-to-end test drives a real :class:`~httk.workflow.TaskManager`: a parent
runner calls a *second* runner file, and the manager runs the child (that other
runner) and resumes the parent at its gather step. A second test calls a packaged
registered workflow by alias and inspects the child payload the call registered
without running it, because the packaged VASP workflow needs VASP to run.
"""

import json
import uuid
from collections.abc import Mapping
from pathlib import Path

import pytest

import httk.workflow.vasp  # noqa: F401  registers the packaged vasp-* workflows
from httk.workflow import Attempt, TaskManager, Workspace
from httk.workflow.models import JobDefinition

_SRC = str(Path(__file__).parents[1] / "src")

_SUB_RUNNER = """#!/usr/bin/env python3
import sys

sys.path.insert(0, "@SRC@")

from httk.workflow import Runner

run = Runner("tests.sub")


@run.step
def run_sub(a):
    text = (a.payload / "files" / "input.txt").read_text(encoding="utf-8")
    (a.workdir / "seen.txt").write_text("sub-saw:" + text, encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
"""

_PARENT_RUNNER = """#!/usr/bin/env python3
import json
import sys

sys.path.insert(0, "@SRC@")

from httk.workflow import Runner

run = Runner("tests.caller")


@run.step
def start(a):
    reference = a.call("@SUB@", label="sub", files={"input.txt": "@INPUT@"})
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


raise SystemExit(run.main())
"""

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


def _payload(root: Path, runner_source: str, *, initial_step: str) -> tuple[Path, str]:
    """Fabricate one submittable payload wrapping *runner_source*."""

    job_id = str(uuid.uuid4())
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": "caller",
        "name": "Caller",
        "workflow": "tests.caller",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"maximum_attempts_per_activation": 1, "retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _in_process_attempt(tmp_path: Path, workspace_root: Path) -> Attempt:
    """Bind an in-process attempt to a real workspace, without a manager."""

    control = tmp_path / "control"
    control.mkdir()
    (tmp_path / "run").mkdir()
    context = {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        "workspace_id": str(uuid.uuid4()),
        "job_id": str(uuid.uuid4()),
        "job_key": f"job--{uuid.uuid4()}",
        "placement": "project/a",
        "payload": str(tmp_path / "job"),
        "step": "start",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "data_generation": None,
    }
    return Attempt.initialize(
        {
            "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
            "HTTK_WORKFLOW_WORKDIR": str(tmp_path / "run"),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(workspace_root),
        }
    )


@pytest.mark.timing
def test_call_runs_another_workflow_and_resumes_at_the_gather(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    sub = tmp_path / "sub_runner.py"
    sub.write_text(_SUB_RUNNER.replace("@SRC@", _SRC), encoding="utf-8")
    sub.chmod(0o755)
    input_file = tmp_path / "input.txt"
    input_file.write_text("hello-from-parent\n", encoding="utf-8")
    parent_source = _PARENT_RUNNER.replace("@SRC@", _SRC).replace("@SUB@", str(sub)).replace("@INPUT@", str(input_file))
    payload, job_id = _payload(tmp_path / "source", parent_source, initial_step="start")
    workspace.submit(payload, "project/caller")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    parent_workdir = workspace.payload_path(parent.placement, parent.job_key) / "run"
    # finish ran after the child succeeded, and saw it under a.children.
    assert json.loads((parent_workdir / "result.json").read_text()) == {"kind": "succeeded", "label": "sub"}

    child_id = (parent_workdir / "sub-id.txt").read_text(encoding="utf-8").strip()
    child = workspace.find_marker_by_id(child_id)
    assert child is not None and child.kind == "succeeded"
    child_payload = workspace.payload_path(child.placement, child.job_key)
    # The child ran the *other* runner, not the parent's.
    assert JobDefinition.from_path(child_payload / "job.json").workflow == "tests.sub"
    # files= reached the child payload, and the sub runner actually consumed it.
    assert (child_payload / "files" / "input.txt").read_text(encoding="utf-8") == "hello-from-parent\n"
    assert (child_payload / "run" / "seen.txt").read_text(encoding="utf-8") == "sub-saw:hello-from-parent\n"


def test_call_references_a_packaged_workflow_by_alias_without_copying(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")
    attempt = _in_process_attempt(tmp_path, workspace.root)

    reference = attempt.call("vasp-relax", label="relax", files={"POSCAR": structure})

    child_json = next(attempt.control.glob("outcome.tmp.*/children/jobs/*/job.json"))
    child = json.loads(child_json.read_text(encoding="utf-8"))
    assert child["id"] == reference.job_id
    assert child["workflow"] == "httk.vasp.relax"
    runner = child["runner"]
    assert isinstance(runner, Mapping)
    # A registered packaged workflow is pinned through pkg: and copies nothing.
    assert runner["source"] == "installed" and str(runner["path"]).startswith("pkg:")
    assert (child_json.parent / "files" / "POSCAR").read_text(encoding="utf-8") == _POSCAR
    assert not (workspace.runners.exists() and list(workspace.runners.iterdir()))
