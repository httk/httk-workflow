"""A manager keeps serving through damaged jobs, foreign entries and a stop signal; its logs are its own."""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _store
from httk.workflow import _logging as logging_module
from httk.workflow._logging import reset_logging

SOURCE = Path(__file__).resolve().parents[1] / "src"


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep records propagating to the capture handlers pytest installs."""

    reset_logging()
    yield
    reset_logging()


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "demo")


@pytest.mark.slow
def test_tick_survives_a_corrupt_ready_job_and_foreign_state_entries(
    ws: Workspace, installed: _store.Installed, caplog: pytest.LogCaptureFixture
) -> None:
    broken = h.submit(ws, installed, {"start": "succeed"}, placement="project/broken")
    healthy = h.submit(ws, installed, {"start": "succeed"}, placement="project/healthy")
    (broken.path / "job.json").write_text("{ not json", encoding="utf-8")
    silly_rename = broken.path.parent / ".nfs0000abcd"
    silly_rename.write_bytes(b"an NFS silly-rename is not a job")
    foreign = broken.path.parent / "garbage"
    foreign.mkdir()

    with caplog.at_level(logging.DEBUG, logger="httk.workflow"):
        h.run(ws)

    assert h.find(ws, healthy.job_id).state == "succeeded"
    # The unreadable job is skipped, never claimed: it stays ready exactly as it was.
    skipped = h.find(ws, broken.job_id)
    assert skipped.path == broken.path and sorted(os.listdir(skipped.path)) == ["job.json"]
    # Neither foreign nor damaged entries are ever moved by a manager.
    assert silly_rename.exists() and foreign.is_dir()
    assert not list((ws.control / "quarantine").iterdir())
    messages = [record.getMessage() for record in caplog.records]
    assert any("skipping ready job" in message and broken.job_key in message for message in messages)


@pytest.mark.slow
def test_unpreparable_attempt_fails_only_its_own_job(ws: Workspace, installed: _store.Installed) -> None:
    # A regular file where the attempt's workdir belongs makes the launch fail after the claim; the job put it
    # on a control path, so it is the job's protocol error.
    blocked = h.submit(
        ws, installed, {"start": "succeed"}, placement="project/blocked", members={"run": "not a directory"}
    )
    healthy = h.submit(ws, installed, {"start": "succeed"}, placement="project/healthy")
    h.run(ws)

    failed = h.find(ws, blocked.job_id)
    assert failed.state == "failed"
    failure = h.state_of(failed).failure
    assert failure is not None and failure["code"] == "protocol_error"
    assert "cannot launch runner" in str(failure["message"])
    assert (failed.path / "run").read_text(encoding="utf-8") == "not a directory"
    assert h.find(ws, healthy.job_id).state == "succeeded"


@pytest.mark.slow
@pytest.mark.timing
def test_serve_drains_and_returns_on_sigterm(
    ws: Workspace, installed: _store.Installed, caplog: pytest.LogCaptureFixture
) -> None:
    submitted = h.submit(ws, installed, {"start": "sleep"})
    before = signal.getsignal(signal.SIGTERM)

    def stop_once_running() -> None:
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline and not any((ws.jobs / "owned").glob("*/*/run/sleeping")):
            time.sleep(0.02)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = threading.Thread(target=stop_once_running, daemon=True)
    stopper.start()
    with caplog.at_level(logging.INFO, logger="httk.workflow"), TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.serve(poll_interval=0.05, drain_timeout=20.0, drain_grace_seconds=5.0)
        assert manager.drained == "signal" and manager.running_attempts == 0
    stopper.join(timeout=5.0)

    stopped = h.find(ws, submitted.job_id)
    assert stopped.state == "failed"
    # Stopped by the drain without an outcome: lost to the manager, not failed by itself.
    failure = h.state_of(stopped).failure
    assert failure is not None and failure["code"] == "owner_lost"
    events = [getattr(record, "event", None) for record in caplog.records]
    assert "drain_started" in events and "drain_complete" in events
    # The handler is restored and the owner left nothing behind.
    assert signal.getsignal(signal.SIGTERM) == before
    assert not [path for path in (ws.control / "owners").iterdir()]


@pytest.mark.slow
def test_json_log_records_carry_structured_fields(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    logs = ws.root / "logs" / "managers"
    logging_module.configure_logging(level="info")

    def attach(manager_id: str) -> None:
        logging_module.add_log_file(logs / f"{manager_id}.log", manager_id=manager_id, json_logs=True)

    with TaskManager(ws, heartbeat_interval=0.01, on_attached=attach) as manager:
        manager.run_until_idle(timeout=60)
        manager_id = manager.manager_id

    (log,) = logs.glob("*.log")
    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]
    assert all({"ts", "level", "logger", "message"} <= set(record) for record in records)
    assert all(record["manager_id"] == manager_id for record in records)
    assert [record for record in records if record.get("event") == "manager_started"]
    launches = [record for record in records if record.get("event") == "launch"]
    assert launches and launches[0]["job_id"] == submitted.job_id and isinstance(launches[0]["pid"], int)
    assert any(
        record.get("event") == "released" and record.get("to") == "succeeded" and record["job_id"] == submitted.job_id
        for record in records
    )


def test_manager_log_rotates_once_at_attach(tmp_path: Path) -> None:
    ws = h.workspace(tmp_path / "ws")
    logs = ws.root / "logs" / "managers"
    logs.mkdir(parents=True)
    old = logs / "manager-old.log"
    old.write_bytes(b"x" * (16 * 1024 * 1024 + 1))
    logging_module.add_log_file(old, manager_id="manager-old", json_logs=True)
    logging.getLogger("httk.workflow").info("after")
    logging_module.reset_logging()

    rotated = logs / "manager-old.log.1"
    assert rotated.is_file() and rotated.stat().st_size == 16 * 1024 * 1024 + 1
    assert "after" in old.read_text(encoding="utf-8") and old.stat().st_size < 1000


@pytest.mark.slow
def test_two_managers_write_two_separate_logs(ws: Workspace) -> None:
    script = """
from pathlib import Path
import sys
from httk.workflow import TaskManager, Workspace
from httk.workflow._logging import add_log_file, configure_logging

workspace = Workspace(Path(sys.argv[1]))
configure_logging()
def setup(manager_id):
    add_log_file(workspace.root / "logs" / "managers" / f"{manager_id}.log", manager_id=manager_id)
with TaskManager(workspace, on_attached=setup) as manager:
    print(manager.manager_id, flush=True)
    manager.run_until_idle(timeout=10)
"""
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(ws.root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        for _ in range(2)
    ]
    results = [process.communicate(timeout=60) for process in processes]

    assert all(process.returncode == 0 for process in processes), results
    manager_ids = {stdout.strip() for stdout, _stderr in results}
    assert len(manager_ids) == 2
    logs = {path.name: path.read_text(encoding="utf-8") for path in (ws.root / "logs" / "managers").iterdir()}
    assert set(logs) == {f"{manager_id}.log" for manager_id in manager_ids}
    for manager_id in manager_ids:
        text = logs[f"{manager_id}.log"]
        assert text and all(line.startswith(manager_id) for line in text.splitlines() if line)


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
