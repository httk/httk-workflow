import json
import uuid
from pathlib import Path

import pytest

from httk.workflow import (
    Attempt,
    FormatError,
    TaskManager,
    Workspace,
)
from httk.workflow.protocol import Failure, validate_failure
from httk.workflow.runtime_builders import ChildReference

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

from httk.workflow import Runner
from httk.workflow.protocol import JobSpec, prepare_job_payload

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

_JOIN_EXTENSION_RUNNER = """#!/usr/bin/env python3
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, "@SRC@")

from httk.workflow import Runner
from httk.workflow.protocol import JobSpec, prepare_job_payload

run = Runner("tests.join_extensions")


def record(a):
    path = a.workdir / "events.json"
    events = json.loads(path.read_text()) if path.exists() else []
    events.append({"step": a.step, "children": [{"label": child.label, "kind": child.kind} for child in a.children]})
    path.write_text(json.dumps(events))


def spawn_child(a, label, delay, fail=False):
    payload = a.workdir / ("child-" + label)
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    shutil.copy2(a.payload / "files" / "runner", runner)
    runner.chmod(0o755)
    prepare_job_payload(
        payload,
        JobSpec(
            name="Child",
            workflow="tests.join_extensions",
            runner_path="files/runner",
            initial_step="child",
            parameters={"delay": delay, "fail": fail},
        ),
    )
    a.spawn(payload, label=label)


@run.step
def branch(a):
    if "@MODE@" == "failed":
        spawn_child(a, "a-child", 0.05, fail=True)
        spawn_child(a, "b-child", 0.4, fail=True)
    elif "@MODE@" == "rejoin":
        spawn_child(a, "a-child", 0.4)
        spawn_child(a, "b-child", 0.05)
    else:
        spawn_child(a, "a-child", 0.02)
    a.gather("wake", when="all_terminal" if "@MODE@" == "terminal_rejoin" else "any_terminal")


@run.step
def child(a):
    time.sleep(float(a.parameter("delay")))
    if a.parameter("fail", False):
        a.fail("child.broken", "the child declared a failure")
    else:
        a.succeed()


@run.step
def wake(a):
    record(a)
    if "@MODE@" == "rejoin" and not (a.workdir / "rejoined").exists():
        (a.workdir / "rejoined").touch()
        spawn_child(a, "c-child", 0.05)
        a.gather("wake", when="any_terminal", rejoin=("a-child",))
    elif "@MODE@" == "terminal_rejoin":
        a.gather("done", when="any_terminal", rejoin=("a-child",))
    else:
        a.succeed()


@run.step
def done(a):
    record(a)
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
    workspace = Workspace.initialize(tmp_path / "workspace")
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
    workspace = Workspace.initialize(tmp_path / "workspace")
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
    workspace = Workspace.initialize(tmp_path / "workspace")
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
    with pytest.raises(ValueError, match="neither was provided"):
        attempt.gather("aggregate")
    assert not (control / "outcome.ready").exists()


def test_any_terminal_wakes_for_a_failed_child(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    runner = _JOIN_EXTENSION_RUNNER.replace("@SRC@", _SRC).replace("@MODE@", "failed")
    payload, job_id = _payload(tmp_path / "source", runner, initial_step="branch")
    workspace.submit(payload, "project/any-terminal-failed")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    events = json.loads((workspace.payload_path(parent.placement, parent.job_key) / "run" / "events.json").read_text())
    kinds = {item["kind"] for item in events[0]["children"]}
    assert "failed" in kinds
    assert kinds & {"ready", "running"}


def test_gather_rejoins_children_from_an_earlier_activation(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    runner = _JOIN_EXTENSION_RUNNER.replace("@SRC@", _SRC).replace("@MODE@", "rejoin")
    payload, job_id = _payload(tmp_path / "source", runner, initial_step="branch")
    workspace.submit(payload, "project/rejoin")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    events = json.loads((workspace.payload_path(parent.placement, parent.job_key) / "run" / "events.json").read_text())
    assert [item["step"] for item in events] == ["wake", "wake"]
    assert {item["label"] for item in events[1]["children"]} == {"a-child", "c-child"}


def test_gather_rejoins_an_already_terminal_child_without_new_children(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    runner = _JOIN_EXTENSION_RUNNER.replace("@SRC@", _SRC).replace("@MODE@", "terminal_rejoin")
    payload, job_id = _payload(tmp_path / "source", runner, initial_step="branch")
    workspace.submit(payload, "project/terminal-rejoin")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    events = json.loads((workspace.payload_path(parent.placement, parent.job_key) / "run" / "events.json").read_text())
    assert [item["step"] for item in events] == ["wake", "done"]
    assert events[1]["children"] == [{"label": "a-child", "kind": "succeeded"}]


def _in_process_attempt(tmp_path: Path, *, label: str | None = None) -> Attempt:
    control = tmp_path / "control"
    control.mkdir()
    child_id = str(uuid.uuid4())
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
        "children": []
        if label is None
        else [
            {
                "label": label,
                "job_id": child_id,
                "job_key": f"child--{child_id}",
                "kind": "succeeded",
                "placement": "project/a",
                "payload_path": f"project/a/child--{child_id}",
            }
        ],
    }
    (control / "context.json").write_text(json.dumps(context), encoding="utf-8")
    (tmp_path / "run").mkdir()
    return Attempt.initialize(
        {
            "HTTK_WORKFLOW_CONTEXT": str(control / "context.json"),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
            "HTTK_WORKFLOW_WORKDIR": str(tmp_path / "run"),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
        }
    )


def test_gather_rejoin_unknown_label_raises_in_process(tmp_path: Path) -> None:
    attempt = _in_process_attempt(tmp_path)
    with pytest.raises(ValueError, match="missing.*known labels: none"):
        attempt.gather("aggregate", rejoin=("missing",))


def test_gather_rejects_duplicate_rejoin_label_in_process(tmp_path: Path) -> None:
    attempt = _in_process_attempt(tmp_path, label="old-child")
    with pytest.raises(ValueError, match="duplicate join child label.*old-child"):
        attempt.gather("aggregate", rejoin=("old-child", "old-child"))


def test_gather_rejects_rejoin_label_used_by_a_new_child_in_process(tmp_path: Path) -> None:
    attempt = _in_process_attempt(tmp_path, label="old-child")
    draft = attempt._require_draft()
    draft._children.append(
        (
            ChildReference(
                attempt.context.workspace_id,
                str(uuid.uuid4()),
                f"new-child--{uuid.uuid4()}",
                "project/a",
            ),
            {"label": "old-child"},
        )
    )
    with pytest.raises(ValueError, match="duplicate join child label.*old-child"):
        attempt.gather("aggregate", rejoin=("old-child",))


_FAIL_PARENT_RUNNER = """#!/usr/bin/env python3
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, "@SRC@")

from httk.workflow import Runner
from httk.workflow.protocol import JobSpec, prepare_job_payload

run = Runner("tests.fail_parent")


@run.step
def branch(a):
    payload = a.workdir / "child"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    shutil.copy2(a.payload / "files" / "runner", runner)
    runner.chmod(0o755)
    prepare_job_payload(
        payload,
        JobSpec(name="Child", workflow="tests.fail_parent", runner_path="files/runner", initial_step="child"),
    )
    a.spawn(payload, label="doomed")
    a.gather("aggregate", when="all_succeeded")


@run.step
def child(a):
    time.sleep(0.02)
    a.fail("child.broken", "the child declared a failure")


@run.step
def aggregate(a):
    a.succeed()


raise SystemExit(run.main())
"""


def test_dependency_failure_diagnosis_names_the_failed_child(tmp_path: Path) -> None:
    from httk.workflow.introspection import explain_job

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _FAIL_PARENT_RUNNER.replace("@SRC@", _SRC), initial_step="branch")
    workspace.submit(payload, "project/dep-fail")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    assert state["failure"]["code"] == "dependency_failure"

    diagnosis = explain_job(workspace, marker)
    children = [check for check in diagnosis.checks if check.name == "dependency child"]
    assert len(children) == 1
    assert "doomed" in children[0].detail
    assert "[child.broken]" in children[0].detail


def test_unresolvable_join_child_fails_instead_of_waiting_forever(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
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
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _GHOST_JOIN_RUNNER, initial_step="branch")
    workspace.submit(payload, "project/ghost-join-grace")
    with TaskManager(workspace, heartbeat_interval=0.01, join_grace_seconds=3600.0) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "waiting"
