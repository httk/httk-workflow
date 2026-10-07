"""Operator requests claimed by a manager that is gone are recovered, and applied at most once."""

import json
import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import httk.workflow.manager as manager_module
from httk.workflow import TaskManager, Workspace
from httk.workflow.journal import JournalWriter, read_record
from httk.workflow.models import Marker

_RUNNER = """#!/usr/bin/env python3
raise SystemExit(1)
"""


def _ready_job(tmp_path: Path, workspace: Workspace) -> Marker:
    """Register one ready job no manager here claims (its pool is served nowhere)."""

    job_id = str(uuid.uuid4())
    payload = tmp_path / "source" / "requested"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text(_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": "requested",
        "name": "Request recovery job",
        "workflow": "tests.requests",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "run",
        "priority": 500,
        "claim": {"pool": "nowhere", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    workspace.submit(payload, "project/requested")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "ready"
    return marker


def _claimed_request(workspace: Workspace, marker: Marker, owner: str, priority: int) -> Path:
    """Publish a set_priority request and leave it claimed by *owner*, as a manager that stopped would."""

    ready = workspace.publish_request(
        {
            "format": "httk-workflow-request",
            "format_version": 2,
            "request_id": str(uuid.uuid4()),
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "expected_generation": marker.generation,
            "expected_record_ref": marker.record_ref,
            "action": "set_priority",
            "priority": priority,
            "operator": "tester",
            "reason": "recovery",
            "created_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }
    )
    claimed = workspace.control / "requests" / "claimed" / owner / ready.name
    claimed.parent.mkdir(parents=True, exist_ok=True)
    os.rename(ready, claimed)
    return claimed


def _owner(workspace: Workspace, liveness: str) -> str:
    """Return a manager id whose record is in one liveness state."""

    manager_id = str(uuid.uuid4())
    if liveness == "manager_record_absent":
        return manager_id
    directory = workspace.control / "managers" / manager_id
    directory.mkdir(parents=True)
    age = timedelta(days=30) if liveness == "lease_grace_expired" else timedelta(0)
    updated = (datetime.now(UTC) - age).isoformat(timespec="microseconds").replace("+00:00", "Z")
    (directory / "heartbeat.json").write_text(
        json.dumps({"manager_id": manager_id, "updated_at": updated}), encoding="utf-8"
    )
    (directory / "manager.json").write_text(
        json.dumps({"manager_id": manager_id, "hostname": socket.gethostname(), "pid": os.getpid()}),
        encoding="utf-8",
    )
    return manager_id


def _operator_frames(workspace: Workspace, marker: Marker) -> list[dict[str, object]]:
    frames = []
    record_ref: str | None = marker.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        if frame.get("reason") == "operator_priority":
            frames.append(frame)
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)
    return frames


@pytest.mark.parametrize("liveness", ["manager_record_absent", "lease_grace_expired"])
def test_a_request_claimed_by_a_departed_manager_is_recovered_and_applied_once(tmp_path: Path, liveness: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = _ready_job(tmp_path, workspace)
    claimed = _claimed_request(workspace, marker, _owner(workspace, liveness), priority=7)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        manager.tick()

    current = workspace.find_marker_by_id(marker.job_id)
    assert current is not None and current.priority == 7
    assert len(_operator_frames(workspace, current)) == 1
    assert not claimed.exists()
    assert not (workspace.control / "requests" / "ready" / claimed.name).exists()
    assert not (workspace.control / "requests" / "retired" / claimed.name).exists()


def test_a_live_managers_claim_is_left_alone(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = _ready_job(tmp_path, workspace)
    claimed = _claimed_request(workspace, marker, _owner(workspace, "alive"), priority=7)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    assert claimed.is_file()
    current = workspace.find_marker_by_id(marker.job_id)
    assert current is not None and current.priority == 500


def test_a_recovered_request_whose_job_moved_on_is_retired_as_stale(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = _ready_job(tmp_path, workspace)
    claimed = _claimed_request(workspace, marker, _owner(workspace, "manager_record_absent"), priority=7)
    # The departed manager had applied it (or anything else moved the job) before it stopped.
    with JournalWriter(workspace.control) as writer:
        workspace.transition(writer, marker, "ready", {"reason": "operator_priority"}, priority=9)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    current = workspace.find_marker_by_id(marker.job_id)
    assert current is not None and current.priority == 9
    retired = workspace.control / "requests" / "retired"
    assert (retired / claimed.name).is_file() and not claimed.exists()
    reason = json.loads((retired / f"{claimed.name}.retirement").read_text(encoding="utf-8"))["reason"]
    assert "generation" in reason


def test_a_claimed_directory_that_names_no_manager_is_ignored(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = _ready_job(tmp_path, workspace)
    claimed = _claimed_request(workspace, marker, "not-a-manager", priority=7)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    assert claimed.is_file()
    current = workspace.find_marker_by_id(marker.job_id)
    assert current is not None and current.priority == 500


@pytest.mark.parametrize("vanishes", ["before-retirement", "during-retirement"])
def test_a_slow_former_owner_tolerates_its_claim_being_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, vanishes: str
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = _ready_job(tmp_path, workspace)
    retired = workspace.control / "requests" / "retired"
    with TaskManager(workspace, heartbeat_interval=0.01) as slow:
        claimed = _claimed_request(workspace, marker, slow.manager_id, priority=7)
        recovered = workspace.control / "requests" / "ready" / claimed.name
        if vanishes == "before-retirement":
            os.rename(claimed, recovered)
        else:
            real_replace = os.replace

            def recovered_first(source: Any, destination: Any) -> None:
                if Path(source) == claimed:
                    os.rename(claimed, recovered)
                real_replace(source, destination)

            monkeypatch.setattr(manager_module.os, "replace", recovered_first)
        slow._retire_request(claimed, "stale")
        monkeypatch.undo()
        assert not slow._reported
    # No reason was recorded for a request this manager never retired.
    assert recovered.is_file()
    assert not (retired / claimed.name).exists()
    assert not (retired / f"{claimed.name}.retirement").exists()
