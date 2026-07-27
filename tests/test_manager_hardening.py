import json
import logging
import os
import signal
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow._logging import reset_logging
from httk.workflow.cli import main as taskmanager_main
from httk.workflow.journal import JournalWriter

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""

_SLEEPING_RUNNER = """#!/usr/bin/env python3
import time

time.sleep(120)
"""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep records propagating to the capture handlers pytest installs."""

    reset_logging()
    yield
    reset_logging()


def _payload(root: Path, runner_source: str, *, tag: str) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": job_id,
        "tag": tag,
        "name": f"Hardening job {tag}",
        "workflow": "tests.hardening",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": 1,
            "maximum_total_attempts": 1,
            "maximum_activations": 1,
            "retry_on": [],
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _register(workspace: Workspace, placement: str, payload: Path):
    """Move one submitted job to ready without running it."""

    marker = workspace.submit(payload, placement)
    job = workspace.load_job(marker)
    with JournalWriter(workspace.control) as writer:
        return workspace.transition(
            writer,
            marker,
            "ready",
            {
                "step": job.initial_step,
                "activation_id": str(uuid.uuid4()),
                "activation_ordinal": 1,
                "attempt_ordinal": 0,
                "total_attempts": 0,
                "data_generation": None,
                "reason": "submitted",
                "job_digest": job.digest,
            },
        )


def _write_lock(workspace: Workspace, *, pid: int, age: timedelta = timedelta()) -> Path:
    lock = workspace.control / "maintenance.lock"
    created = (datetime.now(UTC) - age).isoformat(timespec="microseconds").replace("+00:00", "Z")
    lock.write_text(
        json.dumps({"created": created, "hostname": socket.gethostname(), "pid": pid}),
        encoding="utf-8",
    )
    return lock


def test_tick_survives_a_corrupt_ready_job_and_foreign_state_entries(tmp_path: Path, caplog) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    broken_payload, broken_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="broken")
    healthy_payload, healthy_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="healthy")
    broken = _register(workspace, "project/broken", broken_payload)
    workspace.submit(healthy_payload, "project/healthy")
    (workspace.payload_path(broken.placement, broken.job_key) / "job.json").write_text("{ not json", encoding="utf-8")
    silly_rename = broken.path.parent / ".nfs0000abcd"
    silly_rename.write_bytes(b"an NFS silly-rename is not a marker")
    damaged_marker = broken.path.parent / "garbage.p500.g1.init"
    damaged_marker.write_text("", encoding="utf-8")

    with caplog.at_level(logging.DEBUG, logger="httk.workflow"):
        with TaskManager(workspace, heartbeat_interval=0.01) as manager:
            manager.run_until_idle(timeout=30.0)

    finished = workspace.find_marker_by_id(healthy_id)
    assert finished is not None and finished.kind == "succeeded"
    skipped = workspace.find_marker_by_id(broken_id)
    assert skipped is not None and skipped.kind == "ready"
    # Neither foreign nor damaged state entries are ever moved by a manager.
    assert silly_rename.exists() and damaged_marker.exists()
    assert not list((workspace.control / "quarantine").iterdir())
    messages = [record.getMessage() for record in caplog.records]
    assert any("unusable state entry" in message and "garbage.p500.g1.init" in message for message in messages)
    assert any("skipping ready job" in message and broken.job_key in message for message in messages)
    assert any(".nfs0000abcd" in message for message in messages)


def test_unpreparable_attempt_fails_only_its_own_job(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    blocked_payload, blocked_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="blocked")
    # A regular file where the persistent workdir belongs makes attempt
    # preparation fail after the claim has already been committed.
    (blocked_payload / "run").write_text("not a directory", encoding="utf-8")
    healthy_payload, healthy_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="healthy")
    workspace.submit(blocked_payload, "project/blocked")
    workspace.submit(healthy_payload, "project/healthy")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    blocked = workspace.find_marker_by_id(blocked_id)
    assert blocked is not None and blocked.kind == "failed"
    assert workspace.read_state(blocked)["failure"]["code"] == "process_failure"
    finished = workspace.find_marker_by_id(healthy_id)
    assert finished is not None and finished.kind == "succeeded"


def test_serve_drains_and_exits_zero_on_sigterm(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="draining")
    workspace.submit(payload, "project/draining")

    def stop_once_running() -> None:
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            marker = workspace.find_marker_by_id(job_id)
            if marker is not None and marker.kind == "running":
                break
            time.sleep(0.02)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = threading.Thread(target=stop_once_running, daemon=True)
    stopper.start()
    code = taskmanager_main(
        [
            "run",
            str(workspace.root),
            "--poll-interval",
            "0.05",
            "--drain-timeout",
            "20",
            "--json-logs",
        ]
    )
    stopper.join(timeout=5.0)

    assert code == 0
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    logs = sorted((workspace.control / "managers").glob("*/log"))
    assert len(logs) == 1
    events = [json.loads(line).get("event") for line in logs[0].read_text(encoding="utf-8").splitlines() if line]
    assert "drain_started" in events and "drain_complete" in events
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


def test_maintenance_lock_defers_claiming_until_it_is_released(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="locked")
    workspace.submit(payload, "project/locked")
    lock = _write_lock(workspace, pid=os.getpid())

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        manager.tick()
        paused = workspace.find_marker_by_id(job_id)
        assert paused is not None and paused.kind == "ready"
        lock.unlink()
        manager.run_until_idle(timeout=30.0)

    finished = workspace.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"


def test_stale_maintenance_lock_is_ignored_with_a_warning(tmp_path: Path, caplog) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="stale")
    workspace.submit(payload, "project/stale")
    _write_lock(workspace, pid=os.getpid(), age=timedelta(days=2))

    with caplog.at_level(logging.WARNING, logger="httk.workflow"):
        with TaskManager(workspace, heartbeat_interval=0.01) as manager:
            manager.run_until_idle(timeout=30.0)

    finished = workspace.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("stale maintenance lock" in message and "workspace unlock" in message for message in warnings)


def test_claimed_job_is_released_when_the_lock_appears(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="racing")
    marker = _register(workspace, "project/racing", payload)
    assert marker.kind == "ready"

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        real_transition = manager.workspace.transition

        def lock_after_claim(writer, target, kind, updates, *, priority=None):
            moved = real_transition(writer, target, kind, updates, priority=priority)
            if kind == "claimed":
                _write_lock(workspace, pid=os.getpid())
            return moved

        setattr(manager.workspace, "transition", lock_after_claim)
        manager.tick()
        released = workspace.find_marker_by_id(job_id)
        assert released is not None and released.kind == "ready"
        assert workspace.read_state(released)["reason"] == "maintenance_lock"
        assert not (workspace.payload_path(released.placement, released.job_key) / "run").exists()


def test_json_log_records_carry_structured_fields(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="logged")
    workspace.submit(payload, "project/logged")

    code = taskmanager_main(
        [
            "run",
            str(workspace.root),
            "--until-idle",
            "--poll-interval",
            "0.02",
            "--idle-timeout",
            "30",
            "--json-logs",
        ]
    )

    assert code == 0
    logs = sorted((workspace.control / "managers").glob("*/log"))
    assert len(logs) == 1
    records = [json.loads(line) for line in logs[0].read_text(encoding="utf-8").splitlines() if line]
    assert all({"ts", "level", "logger", "message"} <= set(record) for record in records)
    claims = [record for record in records if record.get("event") == "claim"]
    assert claims and claims[0]["job_id"] == job_id
    launches = [record for record in records if record.get("event") == "launch"]
    assert launches and isinstance(launches[0]["pid"], int)
    assert any(record.get("event") == "transition" and record.get("kind") == "succeeded" for record in records)
