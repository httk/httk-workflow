import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow import _logging as logging_module
from httk.workflow._logging import reset_logging
from httk.workflow.journal import JournalWriter
from httk.workflow.workflow_cli import command

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
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
        "format_version": 2,
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

    with (
        caplog.at_level(logging.DEBUG, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
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
    ws = register_ws(None, workspace.root)
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
    code = command(
        [
            "manager",
            "run",
            "--workspace",
            ws,
            "--idle",
            "--poll-interval",
            "0.05",
            "--drain-timeout",
            "20",
            "--json-logs",
        ],
        CLIContext("httk", tmp_path),
    )
    stopper.join(timeout=5.0)

    assert code == 0
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    log = workspace.control / "managers.log"
    assert log.is_file() and not list((workspace.control / "managers").iterdir())
    events = [json.loads(line).get("event") for line in log.read_text(encoding="utf-8").splitlines() if line]
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

    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
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

        setattr(manager.workspace, "transition", lock_after_claim)  # noqa: B010 - test replaces a method deliberately
        manager.tick()
        released = workspace.find_marker_by_id(job_id)
        assert released is not None and released.kind == "ready"
        assert workspace.read_state(released)["reason"] == "maintenance_lock"
        assert not (workspace.payload_path(released.placement, released.job_key) / "run").exists()


def test_json_log_records_carry_structured_fields(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    ws = register_ws(None, workspace.root)
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="logged")
    workspace.submit(payload, "project/logged")

    code = command(
        [
            "manager",
            "run",
            "--workspace",
            ws,
            "--poll-interval",
            "0.02",
            "--idle-timeout",
            "30",
            "--json-logs",
        ],
        CLIContext("httk", tmp_path),
    )

    assert code == 0
    log = workspace.control / "managers.log"
    assert log.is_file() and not list((workspace.control / "managers").iterdir())
    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]
    assert all({"ts", "level", "logger", "message"} <= set(record) for record in records)
    assert all(record["manager_id"] == records[0]["manager_id"] for record in records)
    claims = [record for record in records if record.get("event") == "claim"]
    assert claims and claims[0]["job_id"] == job_id
    launches = [record for record in records if record.get("event") == "launch"]
    assert launches and isinstance(launches[0]["pid"], int)
    # A payload-source runner records the payload digest at launch, so post-hoc
    # mutation of the runner in the mutable payload is visible in the journal.
    assert isinstance(launches[0]["payload_digest"], str) and launches[0]["payload_digest"]
    assert any(record.get("event") == "transition" and record.get("kind") == "succeeded" for record in records)


def test_shared_manager_log_rotates_once_before_startup_logging(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    ws = register_ws(None, workspace.root)
    log = workspace.control / "managers.log"
    log.write_bytes(b"x" * (16 * 1024 * 1024 + 1))

    code = command(
        ["manager", "run", "--workspace", ws, "--json-logs"],
        CLIContext("httk", tmp_path),
    )

    assert code == 0
    rotated = workspace.control / "managers.log.1"
    assert rotated.is_file() and rotated.stat().st_size == 16 * 1024 * 1024 + 1
    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]
    assert records
    assert all(record["manager_id"] == records[0]["manager_id"] for record in records)
    assert any(record.get("event") == "manager_started" for record in records)


def test_concurrent_manager_log_startup_rotation_keeps_both_managers(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    log = workspace.control / "managers.log"
    log.write_bytes(b"original-sentinel\n" + b"x" * (16 * 1024 * 1024 + 1))
    script = """
from pathlib import Path
import sys
from httk.workflow import TaskManager, Workspace
from httk.workflow._logging import add_log_file, configure_logging

workspace = Workspace(Path(sys.argv[1]))
configure_logging()
def setup(manager_id):
    add_log_file(workspace.control / "managers.log", manager_id=manager_id)
with TaskManager(workspace, on_attached=setup) as manager:
    print(manager.manager_id, flush=True)
    manager.run_until_idle(timeout=10)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(workspace.root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = [process.communicate(timeout=30) for process in processes]

    assert all(process.returncode == 0 for process in processes), results
    manager_ids = {stdout.strip() for stdout, _stderr in results}
    assert len(manager_ids) == 2
    assert list(workspace.control.glob("managers.log.1")) == [workspace.control / "managers.log.1"]
    combined = log.read_text(encoding="utf-8") + (workspace.control / "managers.log.1").read_text(encoding="utf-8")
    assert "original-sentinel" in combined
    assert all(manager_id in combined for manager_id in manager_ids)


def test_manager_log_rotates_after_1000_records(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "managers.log"
    monkeypatch.setattr(logging_module, "MANAGER_LOG_ROTATION_BYTES", 1000)
    logging_module.configure_logging(level="info")
    logging_module.add_log_file(log, manager_id="manager-test")
    logger = logging.getLogger("httk.workflow")

    for index in range(999):
        logger.info("record-%d-%s", index, "x" * 20)
    assert not log.with_name("managers.log.1").exists()
    logger.info("record-999-%s", "x" * 20)

    assert log.with_name("managers.log.1").is_file()
    assert "record-999" in log.read_text(encoding="utf-8")
