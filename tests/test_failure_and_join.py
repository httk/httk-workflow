import json
import uuid
from pathlib import Path

import pytest

from httk.workflow import (
    Attempt,
    Failure,
    FormatError,
    TaskManager,
    WorkflowWorkspace,
    validate_failure,
)

_SRC = str(Path(__file__).parents[1] / "src")


def _payload(root: Path, runner_source: str, *, initial_step: str = "prepare") -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": job_id,
        "tag": "test-job",
        "name": "Test job",
        "workflow": "tests.failure",
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


_BASH_FAIL_RUNNER = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.failure prepare

step_prepare() {
    printf '%s\\n' '{"iterations": 61}' >details.json
    httk_workflow_fail vasp.nonconvergent \\
        "electronic minimization did not converge" --details @details.json
}

httk_workflow_main
"""

_LEGACY_SHAPE_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "fail",
    "failure": {"class": "declared_failure", "summary": "an outdated failure spelling"},
}))
os.rename(temporary, control / "outcome.ready")
"""

_CHILD_RUNNER = f"""#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.child")


@run.step
def run_child(a):
    a.succeed()


raise SystemExit(run.main())
"""

_WAIT_RUNNER = f"""#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import JobSpec, Runner, prepare_job_payload

run = Runner("tests.wait")


@run.step
def branch(a):
    payload = a.workdir / "child-payload"
    files = payload / "files"
    files.mkdir(parents=True)
    child_runner = files / "runner"
    child_runner.write_text({_CHILD_RUNNER!r}, encoding="utf-8")
    child_runner.chmod(0o755)
    prepare_job_payload(
        payload,
        JobSpec(
            name="Child",
            workflow="tests.child",
            runner_path="files/runner",
            tag="child",
            initial_step="run_child",
        ),
    )
    child = a.spawn(payload, label="only", placement="project/children")
    (a.workdir / "child-id.txt").write_text(child.job_id, encoding="utf-8")
    a.gather("aggregate")


@run.step
def aggregate(a):
    a.succeed()


raise SystemExit(run.main())
"""

_GHOST_JOIN_RUNNER = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
child_id = str(uuid.uuid5(uuid.UUID(context["job_id"]), "ghost"))
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "wait",
    "next_step": "aggregate",
    "join": {
        "children": [{
            "workspace_id": context["workspace_id"],
            "job_id": child_id,
            "job_key": "child--" + child_id,
            "placement_hint": "project/ghosts",
        }],
        "condition": "all_succeeded",
    },
}))
os.rename(temporary, control / "outcome.ready")
"""


def test_validate_failure_accepts_only_the_canonical_shape() -> None:
    failure = validate_failure(
        {
            "code": "vasp.nonconvergent",
            "message": "electronic minimization did not converge",
            "details": {"iterations": 61},
            "retryable": True,
        }
    )
    assert failure == Failure(
        "vasp.nonconvergent", "electronic minimization did not converge", {"iterations": 61}, True
    )
    assert failure.as_mapping() == {
        "code": "vasp.nonconvergent",
        "message": "electronic minimization did not converge",
        "details": {"iterations": 61},
        "retryable": True,
    }
    assert Failure("timeout", "ran out of time").as_mapping() == {"code": "timeout", "message": "ran out of time"}
    for rejected in (
        {"class": "process_failure", "summary": "old python spelling"},
        {"code": "process_failure", "summary": "old bash spelling"},
        {"code": "process_failure", "message": "extra member", "exit_status": 9},
        {"code": "process failure", "message": "whitespace in code"},
        {"code": "", "message": "empty code"},
        {"code": "process_failure"},
        {"code": "process_failure", "message": "bad details", "details": "text"},
        {"code": "process_failure", "message": "bad retryable", "retryable": "yes"},
        "not an object",
    ):
        with pytest.raises(FormatError):
            validate_failure(rejected)


def test_bash_published_failure_reaches_the_failed_state_frame(tmp_path: Path) -> None:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _BASH_FAIL_RUNNER)
    workspace.submit(payload, "project/bash-failure")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    assert state["reason"] == "declared_failure"
    assert state["failure"] == {
        "code": "vasp.nonconvergent",
        "message": "electronic minimization did not converge",
        "details": {"iterations": 61},
    }


def test_malformed_published_failure_becomes_a_protocol_error(tmp_path: Path) -> None:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _LEGACY_SHAPE_RUNNER)
    workspace.submit(payload, "project/malformed-failure")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    assert state["reason"] == "protocol_error"
    assert state["failure"]["code"] == "protocol_error"
    assert "malformed failure object" in state["failure"]["message"]


def test_gather_registers_the_children_it_joins_on(tmp_path: Path) -> None:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _WAIT_RUNNER, initial_step="branch")
    workspace.submit(payload, "project/wait-parent")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    workdir = workspace.payload_path(parent.placement, parent.job_key) / "run"
    child_id = (workdir / "child-id.txt").read_text(encoding="utf-8")
    child = workspace.find_marker_by_id(child_id)
    assert child is not None and child.kind == "succeeded"
    assert child.placement.as_posix() == "project/children"
    assert len(list(workspace.scan_markers(("succeeded",)))) == 2


def test_gather_refuses_a_join_over_no_children(tmp_path: Path) -> None:
    control = tmp_path / "control"
    control.mkdir()
    context = {
        "format": "httk-workflow-attempt-context",
        "format_version": 1,
        "workspace_id": str(uuid.uuid4()),
        "job_id": str(uuid.uuid4()),
        "job_key": f"job--{uuid.uuid4()}",
        "placement": "project/a",
        "step": "branch",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "data_generation": None,
    }
    (control / "context.json").write_text(json.dumps(context), encoding="utf-8")
    (tmp_path / "run").mkdir()
    environment = {
        "HTTK_WORKFLOW_CONTEXT": str(control / "context.json"),
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
        "HTTK_WORKFLOW_WORKDIR": str(tmp_path / "run"),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
    }
    # A join is resolvable only for children the publishing bundle registers, so
    # a gather without a spawn on this attempt can never become work.
    attempt = Attempt.initialize(environment)
    with pytest.raises(ValueError, match="none were spawned"):
        attempt.gather("aggregate")
    assert not (control / "outcome.ready").exists()


def test_unresolvable_join_child_fails_instead_of_waiting_forever(tmp_path: Path) -> None:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _GHOST_JOIN_RUNNER, initial_step="branch")
    workspace.submit(payload, "project/ghost-join")
    with TaskManager(workspace, heartbeat_interval=0.01, join_grace_seconds=0.0) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    ghost_id = str(uuid.uuid5(uuid.UUID(job_id), "ghost"))
    assert state["reason"] == "join_unresolvable"
    assert state["failure"]["code"] == "dependency_failure"
    assert ghost_id in state["failure"]["message"]


def test_unresolvable_join_child_is_tolerated_within_the_grace(tmp_path: Path) -> None:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _GHOST_JOIN_RUNNER, initial_step="branch")
    workspace.submit(payload, "project/ghost-join-grace")
    with TaskManager(workspace, heartbeat_interval=0.01, join_grace_seconds=3600.0) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "waiting"
