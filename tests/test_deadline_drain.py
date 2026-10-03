"""Manager drains: at the allocation's drain start, on ``SIGTERM`` in both loops, and their classification."""

import os
import signal
import threading
import time
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.manager import NotIdleError
from test_manager_hardening import _write_lock
from test_manager_scheduling import _drive_until, _payload, _publish_cancel
from test_maxtime import _HEADER

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("heartbeat-timing")]

_SLEEP = "time.sleep(30)\n"
_IGNORE_TERM = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(30)\n"


def _submit(tmp_path: Path, body: str, **job: object) -> tuple[Workspace, str]:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _HEADER + body, tag="drained", **job)  # type: ignore[arg-type]
    workspace.submit(payload, "project/drained")
    return workspace, job_id


def _final(workspace: Workspace, job_id: str) -> tuple[str, dict[str, object]]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return marker.kind, workspace.read_state(marker)


def _failure(state: dict[str, object]) -> dict[str, object]:
    failure = state["failure"]
    assert isinstance(failure, dict)
    return failure


def _started(tmp_path: Path, *, ignore_term: bool) -> tuple[Path, str]:
    """Return a flag the runner writes once running, and the runner body that writes it."""

    flag = tmp_path / "runner-started"
    ignore = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_term else ""
    return flag, f"{ignore}Path({str(flag)!r}).write_text('up')\ntime.sleep(30)\n"


def _wait_for(flag: Path) -> None:
    deadline = time.monotonic() + 10.0
    while not flag.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert flag.exists()


def _manager(workspace: Workspace, *, ends_in: float | None, margin: float = 1.0) -> TaskManager:
    end_time = None if ends_in is None else time.time() + ends_in
    return TaskManager(
        workspace, end_time=end_time, deadline_margin=margin, cancel_grace_seconds=0.5, heartbeat_interval=0.01
    )


def test_deadline_drain_in_run_until_idle_is_lease_lost_and_not_bounded_by_the_timeout(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP)
    started = time.monotonic()
    with _manager(workspace, ends_in=3.0) as manager:
        # A timeout shorter than the drain point: the allocation end bounds the run instead.
        manager.run_until_idle(timeout=1.0, drain_grace_seconds=0.5)
    seconds = time.monotonic() - started
    kind, state = _final(workspace, job_id)
    assert kind == "failed" and 1.5 < seconds < 6
    failure = _failure(state)
    assert failure["code"] == "lease_lost"
    assert "deadline" in str(failure["message"])
    assert failure["details"]["exit_status"] == -signal.SIGTERM  # type: ignore[index]


def test_deadline_drain_retries_lease_lost_without_claiming_it_again(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP, retry_on=("lease_lost",))
    with _manager(workspace, ends_in=3.0) as manager:
        census = manager.run_until_idle(timeout=1.0, drain_grace_seconds=0.5)
    kind, state = _final(workspace, job_id)
    assert kind == "ready" and state["attempt_ordinal"] == 1
    assert census.ready_blocked == {"time": {"drain_point": 1}}


def test_deadline_drain_in_serve_returns_on_its_own(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP)
    started = time.monotonic()
    with _manager(workspace, ends_in=3.0) as manager:
        manager.serve(poll_interval=0.05, drain_timeout=3.0, drain_grace_seconds=0.5)
    kind, state = _final(workspace, job_id)
    assert kind == "failed" and time.monotonic() - started < 6
    assert _failure(state)["code"] == "lease_lost"


def test_an_outcome_published_during_the_drain_wins(tmp_path: Path) -> None:
    body = """
def on_term(signum, frame):
    publish("succeed")
    sys.exit(0)

signal.signal(signal.SIGTERM, on_term)
time.sleep(30)
"""
    workspace, job_id = _submit(tmp_path, body)
    with _manager(workspace, ends_in=3.0) as manager:
        manager.run_until_idle(timeout=1.0, drain_grace_seconds=0.5)
    assert _final(workspace, job_id)[0] == "succeeded"


def test_an_attempt_timed_out_before_the_drain_stays_timeout(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _IGNORE_TERM, resources={"maxtime": 1})
    with TaskManager(
        workspace, end_time=time.time() + 3600, deadline_margin=1.0, cancel_grace_seconds=30.0, heartbeat_interval=0.01
    ) as manager:
        _drive_until(workspace, manager, job_id, {"running"})
        local = next(iter(manager._running.values()))
        deadline = time.monotonic() + 5.0
        while not local.timed_out and time.monotonic() < deadline:
            manager.tick()
            time.sleep(0.02)
        assert local.timed_out
        # The drain starts now, long before the timeout's own escalation.
        manager.drain_start = time.time()
        manager.run_until_idle(timeout=1.0, drain_grace_seconds=0.5)
        assert manager.drained == "deadline"
    kind, state = _final(workspace, job_id)
    failure = _failure(state)
    assert kind == "failed" and failure["code"] == "timeout"
    assert failure["details"]["exit_status"] == -signal.SIGKILL  # type: ignore[index]


def test_maxtime_is_not_enforced_while_draining(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP, resources={"maxtime": 60})
    with _manager(workspace, ends_in=None) as manager:
        _drive_until(workspace, manager, job_id, {"running"})
        local = next(iter(manager._running.values()))
        local.started -= 120
        manager._draining = True
        assert manager._enforce_deadlines() is False
        assert not local.timed_out and local.process.poll() is None
        manager._draining = False
        manager.run_until_idle(timeout=10.0)
    assert _failure(_final(workspace, job_id)[1])["code"] == "timeout"


@pytest.mark.parametrize("ignore_term", [False, True])
def test_sigterm_drains_run_until_idle_and_restores_the_handler(tmp_path: Path, ignore_term: bool) -> None:
    flag, body = _started(tmp_path, ignore_term=ignore_term)
    workspace, job_id = _submit(tmp_path, body)
    before = signal.getsignal(signal.SIGTERM)

    def stop_once_running() -> None:
        _wait_for(flag)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = threading.Thread(target=stop_once_running, daemon=True)
    started = time.monotonic()
    with _manager(workspace, ends_in=None) as manager:
        stopper.start()
        manager.run_until_idle(timeout=20.0, drain_grace_seconds=0.5)
        assert manager.drained == "signal"
    stopper.join(timeout=5.0)
    kind, state = _final(workspace, job_id)
    assert kind == "failed" and time.monotonic() - started < 6
    failure = _failure(state)
    assert failure["code"] == "lease_lost" and "signal" in str(failure["message"])
    # The ignoring runner is only stopped by the drain grace's SIGKILL.
    expected = -signal.SIGKILL if ignore_term else -signal.SIGTERM
    assert failure["details"]["exit_status"] == expected  # type: ignore[index]
    assert signal.getsignal(signal.SIGTERM) is before


def test_a_sigterm_during_a_deadline_drain_kills_at_once(tmp_path: Path) -> None:
    flag, body = _started(tmp_path, ignore_term=True)
    workspace, job_id = _submit(tmp_path, body)
    with _manager(workspace, ends_in=3600.0) as manager:
        _drive_until(workspace, manager, job_id, {"running"})
        _wait_for(flag)
        manager.drain_start = time.time()
        stopper = threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGTERM))
        started = time.monotonic()
        stopper.start()
        manager.run_until_idle(timeout=1.0, drain_timeout=30.0, drain_grace_seconds=20.0)
        assert time.monotonic() - started < 3 and manager.drained == "deadline"
        # Forced exits leave the killed attempt unreaped; reaping it shows the drain's classification.
        _drive_until(workspace, manager, job_id, {"failed"})
    failure = _failure(_final(workspace, job_id)[1])
    assert failure["code"] == "lease_lost"
    assert failure["details"]["exit_status"] == -signal.SIGKILL  # type: ignore[index]


def test_serve_past_its_drain_point_exits_without_claiming(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP)
    started = time.monotonic()
    with _manager(workspace, ends_in=0.5, margin=1.0) as manager:
        manager.serve(poll_interval=0.05, drain_timeout=3.0, drain_grace_seconds=0.5)
        assert manager.drained == "deadline"
    assert _final(workspace, job_id)[0] == "ready" and time.monotonic() - started < 3


def test_with_an_end_time_the_idle_timeout_still_ends_a_stalled_manager(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP)
    _write_lock(workspace, pid=os.getpid())
    with _manager(workspace, ends_in=3600.0) as manager, pytest.raises(NotIdleError):
        manager.run_until_idle(timeout=0.5)
    assert _final(workspace, job_id)[0] == "ready"


def test_without_an_end_time_the_idle_timeout_still_applies(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _SLEEP)
    with _manager(workspace, ends_in=None) as manager:
        with pytest.raises(NotIdleError):
            manager.run_until_idle(timeout=0.5)
        manager._signal_running_attempts(signal.SIGKILL)
        _drive_until(workspace, manager, job_id, {"failed"})


def test_operator_cancel_during_a_timeout_grace_ends_cancelled(tmp_path: Path) -> None:
    workspace, job_id = _submit(tmp_path, _IGNORE_TERM, resources={"maxtime": 1})
    with TaskManager(workspace, cancel_grace_seconds=2.0, heartbeat_interval=0.01) as manager:
        running = _drive_until(workspace, manager, job_id, {"running"})
        local = next(iter(manager._running.values()))
        deadline = time.monotonic() + 5.0
        while not local.timed_out and time.monotonic() < deadline:
            manager.tick()
            time.sleep(0.02)
        assert local.timed_out
        _publish_cancel(workspace, running)
        _drive_until(workspace, manager, job_id, {"cancelled", "failed"})
    assert _final(workspace, job_id)[0] == "cancelled"
