"""A commit is taken over only once every launch recorded for its attempt has ended, or an operator vouches for it.

Each case kills a manager inside its commit (see ``test_commit_frozen_owner.py``), so a successor finds a
``committing`` job whose owner is gone, and adds launch records for the attempt as the owner would have left
them: on this host, or on another host with the allocation the ranks ran in.
"""

import json
import os
import signal
import socket
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from conftest import bury_manager
from httk.workflow import TaskManager, Workspace, _allocation, _manager_launches
from httk.workflow._manager_launches import LAUNCH_END_GRACE
from httk.workflow._slurm import SLURM
from httk.workflow.introspection import explain_job
from httk.workflow.journal import read_record
from httk.workflow.models import Marker
from httk.workflow.workflow_cli import _job as job_cli
from test_cli_job_request import _payload as _cli_payload
from test_commit_frozen_owner import _FINAL, _crash, _data, _plan, _submit
from test_manager_launches import _EXPIRED_AGE, _crashed_manager, _launch_record

pytestmark = pytest.mark.xdist_group("commit-ownership")

_ELSEWHERE = "elsewhere.example"
_OPERATOR = "Test Operator <operator@example.test>"


def _abandoned(tmp_path: Path) -> tuple[Workspace, str, Marker, str, Path]:
    """Return a committing job whose owner died in its replay, with its attempt and attempt-control directory."""

    root, job_id = _submit(tmp_path, _plan(tmp_path), tag="launched")
    _crash(root, job_id, "replay.trash_created", 1)
    workspace = Workspace(root)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "committing"
    state = workspace.read_state(marker)
    control = workspace.payload_path(marker.placement, marker.job_key) / str(state["attempt_control"])
    return workspace, job_id, marker, str(state["attempt_id"]), control


def _write_process(record: Path, attempt_id: str, process: dict[str, object]) -> None:
    (record / "process.json").write_text(json.dumps({**process, "attempt_id": attempt_id}), encoding="utf-8")


def _remote(workspace: Workspace, attempt_id: str, allocation: dict[str, object] | None) -> tuple[Path, str]:
    """Record a launch of the attempt whose ranks ran on another host; return its directory and record name."""

    directory = _crashed_manager(workspace, heartbeat_age=_EXPIRED_AGE)
    record = _launch_record(
        directory, workspace, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": allocation}
    )
    return record, f"{directory.name}/{record.name}"


def _owner_launches(workspace: Workspace, marker: Marker) -> Path:
    """Return the dead owner's own manager directory, with a ``launches/`` directory in it."""

    directory = workspace.control / "managers" / str(workspace.read_state(marker)["manager_id"])
    (directory / "launches").mkdir(exist_ok=True)
    return directory


def _frames(workspace: Workspace, marker: Marker) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    record_ref: str | None = marker.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        frames.append(dict(frame))
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)
    return frames


def _takeovers(workspace: Workspace, job_id: str) -> list[dict[str, Any]]:
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    assert _data(workspace, final) == _FINAL
    return [frame for frame in _frames(workspace, final) if frame.get("reason") == "commit_takeover"]


def _still_abandoned(workspace: Workspace, job_id: str, marker: Marker) -> Marker:
    waiting = workspace.find_marker_by_id(job_id)
    assert waiting is not None and waiting.kind == "committing" and waiting.generation == marker.generation
    return waiting


def test_a_takeover_waits_for_a_launch_elsewhere_until_its_allocation_end_has_passed(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, control = _abandoned(tmp_path)
    seeded = _data(workspace, marker)
    allocation: dict[str, object] = {"probe": "host", "kind": "host", "identity": None, "end_time": time.time() + 3600}
    record, name = _remote(workspace, attempt_id, allocation)
    wedge = control / "commit-wedge.json"
    with TaskManager(workspace, heartbeat_interval=0.01) as waiting_manager:
        for _ in range(3):
            waiting_manager.tick()
        waiting = _still_abandoned(workspace, job_id, marker)
        assert _data(workspace, waiting) == seeded
        assert name in waiting_manager._reported[f"takeover_pending:{marker.job_key}"]
        # The wait repeats, so it is recorded where 'job why' finds it.
        assert json.loads(wedge.read_text(encoding="utf-8"))["launch_end_pending"] == {
            "record": name,
            "rule": "launch_end_unprovable",
            "host": _ELSEWHERE,
        }
        diagnosis = explain_job(workspace, waiting)
        assert diagnosis.blocked and name in diagnosis.summary and _ELSEWHERE in diagnosis.summary
        assert any("launch_end_unprovable" in check.detail for check in diagnosis.checks)
        assert any("httk job confirm-launches-ended" in hint for hint in diagnosis.hints)
        # Nothing this manager can do: it does not count the commit as its work.
        assert waiting_manager._work_census().actionable_count == 0
    # The allocation's end and the grace pass. A successor that never saw the wait takes the commit over,
    # and removes the wait record another manager left.
    _write_process(
        record,
        attempt_id,
        {
            "pid": 4242,
            "hostname": _ELSEWHERE,
            "allocation": {**allocation, "end_time": time.time() - LAUNCH_END_GRACE - 1},
        },
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as successor:
        successor.tick()
        assert not wedge.exists()
        successor.run_until_idle(timeout=90.0)
    (takeover,) = _takeovers(workspace, job_id)
    assert takeover["launch_end_evidence"] == [{"record": name, "rule": "allocation_end_passed", "host": _ELSEWHERE}]
    assert takeover["takeover_evidence"]["evidence"] == "manager_process_dead"


def test_a_takeover_proceeds_once_the_scheduler_confirms_the_allocation_ended(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    answers: list[bool | None] = [False]
    monkeypatch.setattr(SLURM, "can_query", lambda: True)
    monkeypatch.setattr(_allocation, "allocation_ended", lambda recorded, *, timeout: answers[0])
    monkeypatch.setattr(_manager_launches, "ALLOCATION_ANSWER_SECONDS", 0.0)
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    _record, name = _remote(
        workspace, attempt_id, {"probe": "slurm", "kind": "slurm", "identity": {"job_id": "77"}, "end_time": None}
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as successor:
        for _ in range(3):
            successor.tick()
        _still_abandoned(workspace, job_id, marker)
        answers[0] = True
        successor.run_until_idle(timeout=90.0)
    (takeover,) = _takeovers(workspace, job_id)
    assert takeover["launch_end_evidence"] == [
        {"record": name, "rule": "scheduler_confirmed_ended", "host": _ELSEWHERE}
    ]


def test_a_takeover_stops_a_launch_recorded_on_this_host_first(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    host = socket.gethostname()
    live = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        # The record the dead owner left: only a gone manager's launch is stopped by another.
        directory = _owner_launches(workspace, marker)
        record = _launch_record(directory, workspace, attempt_id, {"pid": live.pid, "hostname": host})
        with TaskManager(workspace, heartbeat_interval=0.01) as successor:
            successor.tick()
            _still_abandoned(workspace, job_id, marker)
            assert live.wait(timeout=10) == -signal.SIGTERM
            successor.run_until_idle(timeout=90.0)
    finally:
        live.kill()
        live.wait()
    (takeover,) = _takeovers(workspace, job_id)
    assert takeover["launch_end_evidence"] == [
        {"record": f"{directory.name}/{record.name}", "rule": "process_group_gone", "host": host}
    ]


# -- the operator's confirmation -----------------------------------------------------------------------


def _revive(workspace: Workspace, manager_id: str) -> None:
    """Make a buried owner look alive again: a live pid and a fresh heartbeat."""

    directory = workspace.control / "managers" / manager_id
    record = json.loads((directory / "manager.json").read_text(encoding="utf-8"))
    record.pop("closed_at", None)
    (directory / "manager.json").write_text(json.dumps({**record, "pid": os.getpid()}), encoding="utf-8")
    stamp = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    (directory / "heartbeat.json").write_text(json.dumps({"manager_id": manager_id, "updated_at": stamp}))


def _stamp(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _confirm(workspace: Workspace, marker: Marker) -> str:
    document = job_cli._request_document(
        marker, action="launches_ended", reason="sacct shows the job ended", operator=_OPERATOR
    )
    workspace.publish_request(document)
    return str(document["request_id"])


def _pending_requests(workspace: Workspace) -> list[Path]:
    requests = workspace.control / "requests"
    return sorted(path for directory in ("ready", "claimed") for path in (requests / directory).rglob("*.json"))


def _retirements(workspace: Workspace) -> list[str]:
    retired = workspace.control / "requests" / "retired"
    return [json.loads(path.read_text())["reason"] for path in sorted(retired.glob("*.retirement"))]


def test_a_confirmation_waits_for_a_live_owner_and_takes_over_once_it_is_gone(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    owner = str(workspace.read_state(marker)["manager_id"])
    _remote(workspace, attempt_id, None)  # an older record: nothing but an operator proves it ended
    _revive(workspace, owner)
    request_id = _confirm(workspace, marker)
    with TaskManager(workspace, heartbeat_interval=0.01) as successor:
        for _ in range(3):
            successor.tick()
        # The owner may still commit by itself: the request stays actionable.
        _still_abandoned(workspace, job_id, marker)
        assert len(_pending_requests(workspace)) == 1 and _retirements(workspace) == []
        bury_manager(workspace.control / "managers" / owner)
        successor.run_until_idle(timeout=90.0)
    assert _pending_requests(workspace) == [] and _retirements(workspace) == []
    (takeover,) = _takeovers(workspace, job_id)
    assert takeover["previous_manager_id"] == owner
    assert takeover["launch_end_evidence"] == [
        {"rule": "operator_attested", "request_id": request_id, "operator": _OPERATOR}
    ]


def test_a_deferred_confirmation_is_retired_once_the_commit_moved_on(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    owner = str(workspace.read_state(marker)["manager_id"])
    record, _name = _remote(workspace, attempt_id, None)
    _revive(workspace, owner)
    _confirm(workspace, marker)
    with (
        TaskManager(workspace, heartbeat_interval=0.01) as first,
        TaskManager(workspace, heartbeat_interval=0.01) as second,
    ):
        for _ in range(3):
            first.tick()
        assert len(_pending_requests(workspace)) == 1
        # The owner goes and the launch is proven ended: the other manager takes over by the evidence,
        # without seeing the request the first one holds.
        bury_manager(workspace.control / "managers" / owner)
        _write_process(
            record,
            attempt_id,
            {
                "pid": 4242,
                "hostname": _ELSEWHERE,
                "allocation": {"probe": "host", "kind": "host", "identity": None, "end_time": 1.0},
            },
        )
        second.run_until_idle(timeout=90.0)
        assert [frame["launch_end_evidence"][0]["rule"] for frame in _takeovers(workspace, job_id)] == [
            "allocation_end_passed"
        ]
        first.tick()
    assert _pending_requests(workspace) == []
    (reason,) = _retirements(workspace)
    assert "not the expected one" in reason


def test_a_confirmation_for_a_commit_this_manager_owns_is_retired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, job_id, marker, _attempt_id, _control = _abandoned(tmp_path)
    # The new owner holds its commit, as it would for a launch of its own.
    monkeypatch.setattr(_manager_launches, "holds_commit", lambda manager, marker: True)
    with TaskManager(workspace, heartbeat_interval=0.01) as owner:
        owner.tick()
        taken = workspace.find_marker_by_id(job_id)
        assert taken is not None and taken.kind == "committing" and taken.generation == marker.generation + 1
        assert workspace.read_state(taken)["manager_id"] == owner.manager_id
        _confirm(workspace, taken)
        owner.tick()
        assert workspace.find_marker_by_id(job_id) == taken
    assert _pending_requests(workspace) == []
    (reason,) = _retirements(workspace)
    assert "owns the attempt" in reason


def test_an_operator_confirmation_waits_while_a_launch_runs_on_this_host(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    _remote(workspace, attempt_id, None)  # only an operator proves this one ended
    live = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        _launch_record(
            _owner_launches(workspace, marker),
            workspace,
            attempt_id,
            {"pid": live.pid, "hostname": socket.gethostname()},
        )
        request_id = _confirm(workspace, marker)
        with TaskManager(workspace, heartbeat_interval=0.01) as successor:
            successor.tick()
            # What this host can check is not vouched for: the launch is stopped first.
            _still_abandoned(workspace, job_id, marker)
            assert len(_pending_requests(workspace)) == 1
            assert live.wait(timeout=10) == -signal.SIGTERM
            for _ in range(3):
                successor.tick()
            successor.run_until_idle(timeout=90.0)
    finally:
        live.kill()
        live.wait()
    assert _pending_requests(workspace) == [] and _retirements(workspace) == []
    (takeover,) = _takeovers(workspace, job_id)
    assert takeover["launch_end_evidence"] == [
        {"rule": "operator_attested", "request_id": request_id, "operator": _OPERATOR}
    ]


# -- an outcome another manager published ----------------------------------------------------------------


def _published(tmp_path: Path, first: TaskManager, job_id: str) -> tuple[Marker, str, Path]:
    """Let *first* start the attempt, then wait without it until the attempt published its outcome."""

    workspace = first.workspace
    deadline = time.monotonic() + 60
    while (marker := workspace.find_marker_by_id(job_id)) is None or marker.kind != "running":
        assert time.monotonic() < deadline, "the attempt starts"
        first.tick()
        time.sleep(0.01)
    state = workspace.read_state(marker)
    control = workspace.payload_path(marker.placement, marker.job_key) / str(state["attempt_control"])
    while not (control / "outcome.ready").exists():
        assert time.monotonic() < deadline, "the attempt publishes its outcome"
        time.sleep(0.01)
    return marker, str(state["attempt_id"]), control


def _leave(manager: TaskManager) -> None:
    """Forget the attempts of a manager that is about to exit, once their processes are gone."""

    for local in manager._running.values():
        local.process.wait(timeout=30)
    manager._running.clear()


def test_a_live_managers_outcome_is_committed_by_that_manager(tmp_path: Path) -> None:
    root, job_id = _submit(tmp_path, _plan(tmp_path), tag="outcome")
    workspace = Workspace(root)
    with (
        TaskManager(workspace, heartbeat_interval=0.01) as first,
        TaskManager(workspace, heartbeat_interval=0.01) as second,
    ):
        running, _attempt_id, _control = _published(tmp_path, first, job_id)
        for _ in range(3):
            second.tick()
        # The outcome is the running manager's to commit, however long it takes to look.
        assert workspace.find_marker_by_id(job_id) == running
        first.tick()
        committed = workspace.find_marker_by_id(job_id)
        assert committed is not None and committed != running
        frame = next(frame for frame in _frames(workspace, committed) if frame["kind"] == "committing")
        assert frame["manager_id"] == first.manager_id and "takeover_evidence" not in frame


def test_a_gone_managers_outcome_is_committed_once_its_launches_ended(tmp_path: Path) -> None:
    root, job_id = _submit(tmp_path, _plan(tmp_path), tag="outcome")
    workspace = Workspace(root)
    with TaskManager(workspace, heartbeat_interval=0.01) as first:
        running, attempt_id, control = _published(tmp_path, first, job_id)
        _leave(first)
    # The manager owned a running marker, so it kept its record; its process is gone. A launch elsewhere
    # still holds the commit.
    bury_manager(workspace.control / "managers" / first.manager_id)
    _remote(workspace, attempt_id, None)
    with TaskManager(workspace, heartbeat_interval=0.01) as second:
        for _ in range(3):
            second.tick()
        waiting = workspace.find_marker_by_id(job_id)
        assert waiting is not None and waiting == running
        assert json.loads((control / "commit-wedge.json").read_text())["launch_end_pending"]["rule"] == (
            "launch_end_unprovable"
        )
        diagnosis = explain_job(workspace, waiting)
        assert diagnosis.blocked and "no manager begins its commit" in diagnosis.summary
        assert any("confirm-launches-ended" in hint for hint in diagnosis.hints)
        request_id = _confirm(workspace, waiting)
        second.run_until_idle(timeout=90.0)
    assert not (control / "commit-wedge.json").exists()
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    begun = [frame for frame in _frames(workspace, final) if frame.get("reason") == "outcome_published"]
    assert begun[-1]["manager_id"] != first.manager_id and begun[-1]["previous_manager_id"] == first.manager_id
    assert begun[-1]["takeover_evidence"]["evidence"] == "manager_process_dead"
    assert begun[-1]["launch_end_evidence"] == [
        {"rule": "operator_attested", "request_id": request_id, "operator": _OPERATOR}
    ]


# -- idle managers, closed managers, cancellation -----------------------------------------------------------

#: Waits for ``gate`` next to the results before publishing ``succeed``.
_GATED_RUNNER = """#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
gate = Path(os.environ["HTTK_TEST_GATE"])
(gate.parent / "started").touch()
deadline = time.monotonic() + 60
while not gate.exists() and time.monotonic() < deadline:
    time.sleep(0.01)
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


def _gated_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Workspace, str, Path]:
    gate = tmp_path / "gates" / "gate"
    gate.parent.mkdir()
    monkeypatch.setenv("HTTK_TEST_GATE", str(gate))
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _cli_payload(tmp_path / "source", "gated", _GATED_RUNNER)
    workspace.submit(payload, "project/gated")
    return workspace, job_id, gate


def _start(manager: TaskManager, gate: Path) -> None:
    deadline = time.monotonic() + 60
    while not (gate.parent / "started").exists():
        assert time.monotonic() < deadline, "the attempt starts"
        manager.tick()
        time.sleep(0.01)


def test_a_closed_managers_outcome_is_committed_but_its_running_attempt_is_not_taken_over(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, job_id, gate = _gated_job(tmp_path, monkeypatch)
    first = TaskManager(workspace, heartbeat_interval=0.01)
    _start(first, gate)
    (attempt,) = first._running.values()
    first.close()
    # An unclean close keeps the record, marked closed; its process (this one) and heartbeat look alive.
    record = json.loads((workspace.control / "managers" / first.manager_id / "manager.json").read_text())
    assert record["closed_at"] and record["pid"] == os.getpid()
    with TaskManager(workspace, heartbeat_interval=0.01) as second:
        for _ in range(3):
            second.tick()
        # A closed manager is no evidence that its attempt stopped: no lease takeover.
        running = workspace.find_marker_by_id(job_id)
        assert running is not None and running.kind == "running"
        assert workspace.read_state(running)["attempt_id"] == attempt.attempt_id
        gate.touch()
        attempt.process.wait(timeout=60)
        second.run_until_idle(timeout=60.0)
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    (begun,) = [frame for frame in _frames(workspace, final) if frame.get("reason") == "outcome_published"]
    assert begun["manager_id"] == second.manager_id and begun["previous_manager_id"] == first.manager_id
    assert begun["takeover_evidence"]["evidence"] == "manager_closed"
    assert begun["launch_end_evidence"] == []


def test_a_stale_owner_elsewhere_keeps_an_idle_manager_awake_until_its_grace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, job_id, gate = _gated_job(tmp_path, monkeypatch)
    first = TaskManager(workspace, heartbeat_interval=0.01, lease_seconds=2.0)
    _start(first, gate)
    (attempt,) = first._running.values()
    first.close()
    gate.touch()
    attempt.process.wait(timeout=60)
    # The owner ran on another host, did not close, and last heartbeated longer ago than its lease.
    directory = workspace.control / "managers" / first.manager_id
    record = json.loads((directory / "manager.json").read_text())
    record.pop("closed_at")
    (directory / "manager.json").write_text(json.dumps({**record, "hostname": _ELSEWHERE}))
    with TaskManager(workspace, heartbeat_interval=0.01) as second:
        # Freshly heartbeated: the owner commits it itself.
        census = second._work_census()
        assert census.outcomes_waiting == 1 and not census.actionable
        (directory / "heartbeat.json").write_text(
            json.dumps({"manager_id": first.manager_id, "updated_at": _stamp(time.time() - 2.5)})
        )
        # Older than its lease: it becomes gone within the grace, so this manager stays for it.
        second._liveness.clear()
        census = second._work_census()
        assert census.outcomes_waiting == 1 and census.actionable
        second.run_until_idle(timeout=60.0)
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    (begun,) = [frame for frame in _frames(workspace, final) if frame.get("reason") == "outcome_published"]
    assert begun["takeover_evidence"]["evidence"] == "lease_grace_expired"


def test_a_live_owners_outcome_does_not_keep_another_manager_awake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, job_id, gate = _gated_job(tmp_path, monkeypatch)
    gate.touch()
    with (
        TaskManager(workspace, heartbeat_interval=0.01) as first,
        TaskManager(workspace, heartbeat_interval=0.01) as second,
    ):
        _start(first, gate)
        (attempt,) = first._running.values()
        attempt.process.wait(timeout=60)
        census = second._work_census()
        assert census.outcomes_waiting == 1 and not census.actionable
        assert "1 outcome(s) waiting for another manager" in census.summary_line()
        first.run_until_idle(timeout=60.0)
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"


def test_stopping_a_launch_here_keeps_the_manager_awake(tmp_path: Path) -> None:
    root, job_id = _submit(tmp_path, _plan(tmp_path), tag="outcome")
    workspace = Workspace(root)
    with TaskManager(workspace, heartbeat_interval=0.01) as first:
        running, attempt_id, _control = _published(tmp_path, first, job_id)
        _leave(first)
    bury_manager(workspace.control / "managers" / first.manager_id)
    live = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        directory = workspace.control / "managers" / first.manager_id
        (directory / "launches").mkdir(exist_ok=True)
        record = _launch_record(directory, workspace, attempt_id, {"pid": live.pid, "hostname": socket.gethostname()})
        with TaskManager(workspace, heartbeat_interval=0.01) as second:
            # Stopping it is progress, so an until-idle manager does not exit meanwhile.
            assert second._poll_running() is True
            assert workspace.find_marker_by_id(job_id) == running
            assert live.wait(timeout=10) == -signal.SIGTERM
            second.run_until_idle(timeout=90.0)
    finally:
        live.kill()
        live.wait()
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    begun = [frame for frame in _frames(workspace, final) if frame.get("reason") == "outcome_published"]
    assert begun[-1]["launch_end_evidence"] == [
        {"record": f"{directory.name}/{record.name}", "rule": "process_group_gone", "host": socket.gethostname()}
    ]


def _cancel(workspace: Workspace, marker: Marker) -> None:
    workspace.publish_request(
        job_cli._request_document(marker, action="cancel", reason="wrong structure", operator=_OPERATOR)
    )


def test_cancelling_a_committing_job_waits_for_launch_end_evidence(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    record, name = _remote(workspace, attempt_id, None)
    _cancel(workspace, marker)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        # A launch elsewhere may still write the job: the cancellation is fenced, not verified.
        cancelling = workspace.find_marker_by_id(job_id)
        assert cancelling is not None and cancelling.kind == "cancelling"
        _write_process(
            record,
            attempt_id,
            {
                "pid": 4242,
                "hostname": _ELSEWHERE,
                "allocation": {"probe": "host", "kind": "host", "identity": None, "end_time": 1.0},
            },
        )
        for _ in range(3):
            manager.tick()
    cancelled = workspace.find_marker_by_id(job_id)
    assert cancelled is not None and cancelled.kind == "cancelled"
    cancellation = workspace.read_state(cancelled)["cancellation"]
    assert cancellation["verified"] == "process_group_absent"
    assert cancellation["launch_end_evidence"] == [
        {"record": name, "rule": "allocation_end_passed", "host": _ELSEWHERE}
    ]


def test_cancelling_a_committing_job_without_launches_records_their_evidence(tmp_path: Path) -> None:
    workspace, job_id, marker, _attempt_id, _control = _abandoned(tmp_path)
    _cancel(workspace, marker)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
    cancelled = workspace.find_marker_by_id(job_id)
    assert cancelled is not None and cancelled.kind == "cancelled"
    cancellation = workspace.read_state(cancelled)["cancellation"]
    assert cancellation["verified"] == "no_live_attempt" and cancellation["launch_end_evidence"] == []


def test_an_unloadable_job_of_a_gone_owner_waits_for_launch_end_evidence(tmp_path: Path) -> None:
    workspace, job_id, marker, attempt_id, _control = _abandoned(tmp_path)
    record, _name = _remote(workspace, attempt_id, None)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._fail_unloadable_job(marker, "the definition is refused")
        assert workspace.find_marker_by_id(job_id) == marker
        _write_process(
            record,
            attempt_id,
            {
                "pid": 4242,
                "hostname": _ELSEWHERE,
                "allocation": {"probe": "host", "kind": "host", "identity": None, "end_time": 1.0},
            },
        )
        manager._launch_records = None
        manager._fail_unloadable_job(marker, "the definition is refused")
    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"


def test_a_confirmation_that_begins_no_commit_is_retired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, job_id = _submit(tmp_path, _plan(tmp_path), tag="outcome")
    workspace = Workspace(root)
    with TaskManager(workspace, heartbeat_interval=0.01) as first:
        running, attempt_id, _control = _published(tmp_path, first, job_id)
        _leave(first)
    bury_manager(workspace.control / "managers" / first.manager_id)
    _remote(workspace, attempt_id, None)
    _confirm(workspace, running)
    # The outcome turns out unusable, say: nothing was begun, so the request is not recorded as applied.
    monkeypatch.setattr(TaskManager, "_commit_published_outcome", lambda self, *arguments, **options: True)
    with TaskManager(workspace, heartbeat_interval=0.01) as second:
        second.tick()
    assert _pending_requests(workspace) == []
    (reason,) = _retirements(workspace)
    assert "no commit was begun" in reason
