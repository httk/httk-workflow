import json
import os
import time
import uuid
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.errors import TransitionLostError
from httk.workflow.journal import JournalWriter, read_record
from httk.workflow.models import StateFrame


def _payload(
    root: Path,
    runner_source: str,
    *,
    data_mode: str = "none",
    retry_on: list[str] | None = None,
) -> tuple[Path, str]:
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
        "tag": "test-job",
        "name": "Test job",
        "workflow": "tests.example",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": data_mode},
        "initial_step": "prepare",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": 3,
            "maximum_total_attempts": 10,
            "maximum_activations": 5,
            "retry_on": retry_on or [],
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


_TWO_STEP_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
(run / "steps.txt").open("a").write(context["step"] + "\\n")
if context["step"] == "prepare":
    body = {"action": "advance", "next_step": "collect"}
else:
    body = {"action": "succeed"}
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    **body,
}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


def test_journal_round_trip(tmp_path: Path) -> None:
    control = tmp_path / ".httk-workspace"
    (control / "journal").mkdir(parents=True)
    with JournalWriter(control) as writer:
        reference = writer.append({"answer": 42})
    assert read_record(control, reference) == {"answer": 42}
    assert len(reference) <= 106


def test_transition_verifies_destination_after_ambiguous_rename(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _ = _payload(tmp_path / "source", _TWO_STEP_RUNNER)
    marker = workspace.submit(payload, "project/rename")
    real_rename = os.rename
    injected = False

    def ambiguous_rename(source, destination) -> None:
        nonlocal injected
        real_rename(source, destination)
        if not injected and Path(source) == marker.path:
            injected = True
            raise OSError("simulated lost NFS reply")

    monkeypatch.setattr(os, "rename", ambiguous_rename)
    with JournalWriter(workspace.control) as writer:
        moved = workspace.transition(
            writer,
            marker,
            "ready",
            {
                "step": "prepare",
                "activation_id": str(uuid.uuid4()),
                "activation_ordinal": 1,
                "attempt_ordinal": 0,
                "total_attempts": 0,
                "data_generation": None,
            },
        )
    assert moved.kind == "ready"
    assert not marker.path.exists()
    assert moved.path.exists()


def test_submit_and_run_multistep_persistent_job(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _TWO_STEP_RUNNER)
    workspace.submit(payload, "project/a")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    assert marker.kind == "succeeded"
    run = workspace.payload_path(marker.placement, marker.job_key) / "run"
    assert (run / "steps.txt").read_text(encoding="utf-8").splitlines() == ["prepare", "collect"]


@pytest.mark.timing
def test_new_manager_replays_published_outcome(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(
        tmp_path / "source",
        _TWO_STEP_RUNNER.replace('"action": "advance", "next_step": "collect"', '"action": "succeed"'),
    )
    workspace.submit(payload, "project/recovery")
    first = TaskManager(workspace, heartbeat_interval=0.01)
    try:
        first.tick()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            running = workspace.find_marker_by_id(job_id)
            assert running is not None
            state = workspace.read_state(running)
            if running.kind == "running" and first._outcome_path(running, StateFrame.from_mapping(state)).exists():
                first.tick()
                break
            time.sleep(0.01)
        committing = workspace.find_marker_by_id(job_id)
        assert committing is not None and committing.kind == "committing"
    finally:
        first.close()
    with TaskManager(workspace) as replacement:
        replacement.run_until_idle()
    finished = workspace.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"


@pytest.mark.timing
def test_lost_running_transition_does_not_execute_runner(tmp_path: Path, monkeypatch) -> None:
    runner = """#!/usr/bin/env python3
from pathlib import Path
Path("runner-executed").write_text("unsafe")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner)
    workspace.submit(payload, "project/gated")
    real_transition = workspace.transition

    def lose_running_transition(writer, marker, kind, updates, *, priority=None):
        if kind == "running":
            raise TransitionLostError("simulated competing transition")
        return real_transition(writer, marker, kind, updates, priority=priority)

    monkeypatch.setattr(workspace, "transition", lose_running_transition)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        time.sleep(0.05)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "claimed"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "run"
    assert not (workdir / "runner-executed").exists()


def test_unclean_process_failure_sets_restart_context(tmp_path: Path) -> None:
    runner = """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
count_path = run / "count"
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
if count == 1:
    sys.exit(9)
assert context["is_restart"] is True
assert context["is_unclean_restart"] is True
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, retry_on=["process_failure"])
    workspace.submit(payload, "project/restart")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    assert marker.kind == "succeeded"
    assert (workspace.payload_path(marker.placement, marker.job_key) / "run" / "count").read_text() == "2"


def test_transactional_output(tmp_path: Path) -> None:
    runner = """#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
payload = temporary / "transaction" / "payload"
payload.mkdir(parents=True)
content = b"complete\\n"
(payload / "result.txt").write_bytes(content)
(temporary / "transaction" / "manifest.json").write_text(json.dumps({
    "format": "httk-workflow-transaction",
    "format_version": 2,
    "id": "transaction",
    "expected_data_generation": context["data_generation"],
    "operations": [{
        "id": "result",
        "op": "put-file",
        "source": "payload/result.txt",
        "path": "result.txt",
        "sha256": hashlib.sha256(content).hexdigest(),
    }],
}))
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "expected_data_generation": context["data_generation"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, data_mode="transactional")
    workspace.submit(payload, "project/transaction")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    assert marker.kind == "succeeded"
    data = workspace.payload_path(marker.placement, marker.job_key) / "data"
    assert (data / "result.txt").read_text(encoding="utf-8") == "complete\n"


def test_dynamic_child_join(tmp_path: Path) -> None:
    runner = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}
if context["step"] == "aggregate":
    outcome = {**base, "action": "succeed"}
else:
    child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), "child"))
    child_key = "child--" + child_id
    child_dir = temporary / "children" / "jobs" / child_key
    (child_dir / "files").mkdir(parents=True)
    child_runner = child_dir / "files" / "runner"
    child_runner.write_text('''#!/usr/bin/env python3
import json
import os
from pathlib import Path
context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.child"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
''')
    child_runner.chmod(0o755)
    (child_dir / "job.json").write_text(json.dumps({
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": child_id,
        "tag": "child",
        "name": "Child",
        "workflow": "tests.child",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "run",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": {
            "workspace_id": context["workspace_id"],
            "job_id": context["job_id"],
            "activation_id": context["activation_id"],
        },
    }))
    (temporary / "children" / "spawn.json").write_text(json.dumps({
        "children": [{
            "workspace_id": context["workspace_id"],
            "job_id": child_id,
            "job_key": child_key,
            "label": "child",
            "placement": "project/children",
        }]
    }))
    outcome = {
        **base,
        "action": "wait",
        "next_step": "aggregate",
        "join": {
            "children": [{
                "workspace_id": context["workspace_id"],
                "job_id": child_id,
                "job_key": child_key,
                "placement_hint": "project/children",
            }],
            "condition": "all_succeeded",
        },
    }
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner)
    workspace.submit(payload, "project/parent")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    markers = list(workspace.scan_markers(("succeeded",)))
    assert len(markers) == 2


def test_invalid_submission_moves_to_failed(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _TWO_STEP_RUNNER)
    (payload / "files" / "runner").unlink()
    workspace.submit(payload, "project/invalid")
    with TaskManager(workspace) as manager:
        manager.tick()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    assert marker.kind == "failed"


def test_priority_request_renames_authoritative_marker(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _TWO_STEP_RUNNER)
    submitted = workspace.submit(payload, "project/request")
    with TaskManager(workspace, pools=("other",)) as manager:
        manager.tick()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        assert ready.path.name.startswith(f"{ready.job_key}.p500.")
        workspace.publish_request(
            {
                "format": "httk-workflow-request",
                "format_version": 2,
                "job_id": job_id,
                "job_key": ready.job_key,
                "placement": ready.placement.as_posix(),
                "expected_generation": ready.generation,
                "expected_record_ref": ready.record_ref,
                "action": "set_priority",
                "priority": 25,
                "operator": "pytest",
                "reason": "test",
            }
        )
        manager.tick()
    changed = workspace.find_marker_by_id(job_id)
    assert changed is not None
    assert changed.priority == 25
    assert changed.kind == "ready"
    assert changed.path.name.startswith(f"{changed.job_key}.p025.")
    assert changed.generation > submitted.generation
