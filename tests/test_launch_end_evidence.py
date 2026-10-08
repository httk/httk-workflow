"""One ladder decides whether every launch recorded for an attempt has ended.

The records are written the way a manager writes them (``launch.json`` and ``process.json`` in a trusted launch
directory below ``managers/<id>/launches/``); launches on other hosts are records naming another host.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from httk.workflow import TaskManager, Workspace, _allocation, _manager_launches
from httk.workflow._manager_launches import LAUNCH_END_GRACE, SCHEDULER_UNAVAILABLE_SECONDS, LaunchEnd
from httk.workflow._slurm import SLURM
from httk.workflow.models import StateFrame
from test_manager_launches import _EXPIRED_AGE, _crashed_manager, _dead_writer_state, _launch_record

_HOST = socket.gethostname()
_ELSEWHERE = "elsewhere.example"


@pytest.fixture
def manager(tmp_path: Path) -> Iterator[TaskManager]:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with TaskManager(workspace, heartbeat_interval=0.01, lease_seconds=600.0, cancel_grace_seconds=0.3) as created:
        yield created


def _allocation_member(
    *, probe: str = "slurm", identity: dict[str, str] | None = None, end_time: float | None = None
) -> dict[str, object]:
    return {
        "probe": probe,
        "kind": probe if probe in ("slurm", "host") else "pbs",
        "identity": identity,
        "end_time": end_time,
    }


def _record(
    manager: TaskManager,
    attempt_id: str,
    process: dict[str, object] | None,
    *,
    age: float = 0.0,
    gone: bool = False,
) -> str:
    """Write one launch record in a manager directory of its own; return its name below ``managers/``.

    A *gone* manager has a fresh heartbeat (so online pruning keeps its records) and a ``manager.json``
    naming a dead process on this host, which is liveness evidence that it is gone.
    """

    directory = _crashed_manager(manager.workspace, heartbeat_age=age)
    if gone:
        dead = subprocess.Popen(["true"])
        dead.wait()
        (directory / "manager.json").write_text(json.dumps({"hostname": _HOST, "pid": dead.pid}))
    record = _launch_record(directory, manager.workspace, attempt_id, process)
    return f"{directory.name}/{record.name}"


def _evidence(manager: TaskManager, attempt_id: str) -> _manager_launches.LaunchEndEvidence:
    # The launch records are otherwise read once per manager tick.
    manager._launch_records = None
    return _manager_launches.launch_end_evidence(manager, attempt_id)


def _writer_dead(manager: TaskManager, state: StateFrame) -> bool:
    manager._launch_records = None
    return manager._attempt_writer_dead(state)


def _cancellation(manager: TaskManager, state: StateFrame) -> dict[str, object] | None:
    manager._launch_records = None
    return manager._cancellation_evidence(state)


@pytest.fixture
def squeue_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SLURM, "can_query", lambda: True)


def test_no_records_is_ended(manager: TaskManager) -> None:
    evidence = _evidence(manager, str(uuid.uuid4()))
    assert evidence.ended and evidence.records == () and evidence.as_json() == []


def test_a_launch_that_never_passed_its_gate_has_not_started_once_its_manager_is_gone(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    name = _record(manager, attempt_id, None, gone=True)
    assert _evidence(manager, attempt_id) == _manager_launches.LaunchEndEvidence((LaunchEnd(name, "not_started"),))


def test_a_launch_description_of_a_live_manager_may_be_starting(manager: TaskManager) -> None:
    # The manager may be between its start and the process record: the ranks run once it is durable.
    attempt_id = str(uuid.uuid4())
    name = _record(manager, attempt_id, None, age=0.0)
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_starting")
    state = _dead_writer_state(attempt_id)
    assert not _writer_dead(manager, state)
    assert _cancellation(manager, state) is None


def _malformed(manager: TaskManager, attempt_id: str, *, gone: bool) -> str:
    directory = manager.workspace.control / "managers" / _record(manager, str(uuid.uuid4()), None, gone=gone)
    directory = directory.parent
    broken = directory / "launches" / f"{attempt_id}.{uuid.uuid4().hex}"
    broken.mkdir()
    (broken / "process.json").write_text("{not json")
    return f"{directory.name}/{broken.name}"


def test_a_malformed_record_counts_as_ended_only_once_its_manager_is_gone(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    live = _malformed(manager, attempt_id, gone=False)
    # A live manager may be writing it.
    assert _evidence(manager, attempt_id).pending == LaunchEnd(live, "malformed_record_live")
    shutil.rmtree(manager.workspace.control / "managers" / live.split("/")[0])
    gone = _malformed(manager, attempt_id, gone=True)
    # A malformed record of another attempt is not this attempt's.
    other = (
        manager.workspace.control / "managers" / gone.split("/")[0] / "launches" / f"{uuid.uuid4()}.{uuid.uuid4().hex}"
    )
    other.mkdir()
    (other / "process.json").write_text("[]")
    assert _evidence(manager, attempt_id) == _manager_launches.LaunchEndEvidence((LaunchEnd(gone, "malformed_record"),))


def test_an_unusable_allocation_member_only_proves_less(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    gone = subprocess.Popen(["true"])
    gone.wait()
    name = _record(manager, attempt_id, {"pid": gone.pid, "hostname": _HOST, "allocation": {"probe": "slurm"}})
    launches = _manager_launches.recorded_launches(manager, attempt_id)
    assert launches is not None
    (launch,) = launches
    assert launch.kind == "process" and launch.allocation is None
    assert _evidence(manager, attempt_id).as_json() == [{"record": name, "rule": "process_group_gone", "host": _HOST}]


def test_a_launch_here_is_ended_once_its_process_group_is_gone(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    gone = subprocess.Popen(["true"])
    gone.wait()
    name = _record(manager, attempt_id, {"pid": gone.pid, "hostname": _HOST})
    evidence = _evidence(manager, attempt_id)
    assert evidence.ended and evidence.as_json() == [{"record": name, "rule": "process_group_gone", "host": _HOST}]


def test_a_live_launch_here_is_stopped_and_killed_after_the_grace(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    # The group ignores SIGTERM, so only the SIGKILL after the cancellation grace ends it.
    stubborn = subprocess.Popen(["sh", "-c", "trap '' TERM; sleep 60 & wait"], start_new_session=True)
    try:
        name = _record(manager, attempt_id, {"pid": stubborn.pid, "hostname": _HOST}, gone=True)
        evidence = _evidence(manager, attempt_id)
        assert evidence.pending == LaunchEnd(name, "launch_running_here", _HOST)
        assert manager._launch_signals[name][1] is False
        time.sleep(0.1)
        assert stubborn.poll() is None, "SIGTERM is ignored"
        assert not _evidence(manager, attempt_id).ended
        time.sleep(0.3)
        assert not _evidence(manager, attempt_id).ended
        assert manager._launch_signals[name][1] is True
        assert stubborn.wait(timeout=10) == -signal.SIGKILL
        deadline = time.monotonic() + 10
        while not _evidence(manager, attempt_id).ended:
            assert time.monotonic() < deadline, "the killed group is gone"
            time.sleep(0.05)
        assert _evidence(manager, attempt_id).as_json() == [
            {"record": name, "rule": "process_group_gone", "host": _HOST}
        ]
        assert name not in manager._launch_signals
    finally:
        try:
            os.killpg(stubborn.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stubborn.wait()


def test_a_live_managers_launch_here_is_left_to_that_manager(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    live = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        name = _record(manager, attempt_id, {"pid": live.pid, "hostname": _HOST})
        for _ in range(3):
            assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_running_here", _HOST)
        assert name not in manager._launch_signals
        time.sleep(0.2)
        assert live.poll() is None, "a live manager's launch is never signalled here"
    finally:
        live.kill()
        live.wait()


def test_a_reused_pid_is_not_the_recorded_launch(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    other = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        started = _manager_launches._start_time(other.pid)
        boot = _manager_launches._boot_id()
        if started is None or boot is None:
            pytest.skip("no /proc on this host")
        # The same pid with another start time, or another boot, is another process: the recorded group is gone.
        for identity in ({"process_start": started + 1, "boot_id": boot}, {"process_start": started, "boot_id": "x"}):
            name = _record(manager, attempt_id, {"pid": other.pid, "hostname": _HOST, **identity}, gone=True)
            assert _evidence(manager, attempt_id).as_json() == [
                {"record": name, "rule": "process_group_gone", "host": _HOST}
            ]
            shutil.rmtree(manager.workspace.control / "managers" / name.split("/")[0])
        # The matching identity is the launch, which is stopped.
        name = _record(
            manager,
            attempt_id,
            {"pid": other.pid, "hostname": _HOST, "process_start": started, "boot_id": boot},
            gone=True,
        )
        assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_running_here", _HOST)
        assert other.wait(timeout=10) == -signal.SIGTERM
    finally:
        other.kill()
        other.wait()


@pytest.mark.skipif(os.geteuid() == 0, reason="root may signal every process group")
def test_a_reused_pid_of_another_user_does_not_pend_forever(manager: TaskManager) -> None:
    started = _manager_launches._start_time(1)
    if started is None or _manager_launches._group_alive(1) is False:
        pytest.skip("no readable init process")
    attempt_id = str(uuid.uuid4())
    # Signalling pid 1 fails with EPERM; its start time shows it is not the recorded leader.
    name = _record(
        manager,
        attempt_id,
        {"pid": 1, "hostname": _HOST, "process_start": started + 1, "boot_id": _manager_launches._boot_id()},
        gone=True,
    )
    assert _evidence(manager, attempt_id).as_json() == [{"record": name, "rule": "process_group_gone", "host": _HOST}]


def test_a_step_of_another_allocation_needs_its_scheduler(
    manager: TaskManager, monkeypatch: pytest.MonkeyPatch, squeue_installed: None
) -> None:
    answers: list[bool | None] = [None]
    monkeypatch.setattr(_allocation, "allocation_ended", lambda recorded, *, timeout: answers[0])
    monkeypatch.setattr(_manager_launches, "ALLOCATION_ANSWER_SECONDS", 0.0)
    attempt_id = str(uuid.uuid4())
    gone = subprocess.Popen(["true"])
    gone.wait()
    member = _allocation_member(identity={"job_id": "77"})
    name = _record(manager, attempt_id, {"pid": gone.pid, "hostname": _HOST, "allocation": member})
    # srun's ranks run under slurmstepd, outside its process group: its group being gone proves nothing.
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_end_unprovable", _HOST)
    answers[0] = True
    assert _evidence(manager, attempt_id).as_json() == [
        {"record": name, "rule": "scheduler_confirmed_ended", "host": _HOST}
    ]
    # A step of this manager's own allocation keeps the accepted residual: the group decides.
    answers[0] = None
    manager.allocation = _allocation.Allocation("slurm", None, identity={"job_id": "77"})
    assert _evidence(manager, attempt_id).as_json() == [{"record": name, "rule": "process_group_gone", "host": _HOST}]


def test_a_step_of_another_site_probe_allocation_needs_its_probe(
    manager: TaskManager, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = tmp_path / "probe"
    probe.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    probe.chmod(0o755)
    monkeypatch.setattr(_allocation, "allocation_ended", lambda recorded, *, timeout: None)
    monkeypatch.setattr(_manager_launches, "ALLOCATION_ANSWER_SECONDS", 0.0)
    attempt_id = str(uuid.uuid4())
    gone = subprocess.Popen(["true"])
    gone.wait()
    member = _allocation_member(probe=f"exec:{probe}", identity={"job_id": "77.pbs"})
    name = _record(manager, attempt_id, {"pid": gone.pid, "hostname": _HOST, "allocation": member})
    # PBS mpiexec ranks run under the MOM daemons, outside the launch's process group, as srun's do.
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_end_unprovable", _HOST)


def test_a_launch_elsewhere_ends_with_its_allocation_end_and_the_grace(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    recent = time.time() - LAUNCH_END_GRACE + 60
    name = _record(
        manager,
        attempt_id,
        {"pid": 4242, "hostname": _ELSEWHERE, "allocation": _allocation_member(probe="host", end_time=recent)},
    )
    # Within the grace nothing proves the ranks are gone: Slurm's KillWait and clock skew.
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_end_unprovable", _ELSEWHERE)
    attempt_id = str(uuid.uuid4())
    passed = time.time() - LAUNCH_END_GRACE - 1
    name = _record(
        manager,
        attempt_id,
        {"pid": 4242, "hostname": _ELSEWHERE, "allocation": _allocation_member(probe="host", end_time=passed)},
    )
    evidence = _evidence(manager, attempt_id)
    assert evidence.ended and evidence.as_json() == [
        {"record": name, "rule": "allocation_end_passed", "host": _ELSEWHERE}
    ]


def test_a_launch_elsewhere_ends_when_its_scheduler_confirms_it(
    manager: TaskManager, monkeypatch: pytest.MonkeyPatch, squeue_installed: None
) -> None:
    answers: list[bool | None] = [False]
    asked: list[_allocation.RecordedAllocation] = []

    def ask(recorded: _allocation.RecordedAllocation, *, timeout: float) -> bool | None:
        asked.append(recorded)
        return answers[0]

    monkeypatch.setattr(_allocation, "allocation_ended", ask)
    attempt_id = str(uuid.uuid4())
    member = _allocation_member(identity={"job_id": "77", "cluster": "c1"})
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member})
    for _ in range(3):
        assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "allocation_active", _ELSEWHERE)
    # One answer per allocation is reused for a minute.
    assert asked == [_allocation.RecordedAllocation("slurm", "slurm", {"job_id": "77", "cluster": "c1"}, None)]
    answers[0] = True
    assert not _evidence(manager, attempt_id).ended
    monkeypatch.setattr(_manager_launches, "ALLOCATION_ANSWER_SECONDS", 0.0)
    evidence = _evidence(manager, attempt_id)
    assert evidence.ended and evidence.as_json() == [
        {"record": name, "rule": "scheduler_confirmed_ended", "host": _ELSEWHERE}
    ]
    # A scheduler that cannot tell proves nothing.
    answers[0] = None
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "launch_end_unprovable", _ELSEWHERE)


def test_an_active_allocation_outranks_its_passed_end_time(
    manager: TaskManager, monkeypatch: pytest.MonkeyPatch, squeue_installed: None
) -> None:
    """An administrator may have extended the time limit after the end time was recorded."""

    answers: list[bool | None] = [False]
    monkeypatch.setattr(_allocation, "allocation_ended", lambda recorded, *, timeout: answers[0])
    monkeypatch.setattr(_manager_launches, "ALLOCATION_ANSWER_SECONDS", 0.0)
    attempt_id = str(uuid.uuid4())
    recently = time.time() - LAUNCH_END_GRACE - 1
    long_ago = time.time() - LAUNCH_END_GRACE - SCHEDULER_UNAVAILABLE_SECONDS - 1
    member = _allocation_member(identity={"job_id": "77"}, end_time=recently)
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member})
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "allocation_active", _ELSEWHERE)
    # One failing query is no evidence: a scheduler that cannot tell counts an hour beyond the grace.
    answers[0] = None
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "scheduler_unavailable", _ELSEWHERE)
    attempt_id = str(uuid.uuid4())
    member = _allocation_member(identity={"job_id": "78"}, end_time=long_ago)
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member})
    assert _evidence(manager, attempt_id).as_json() == [
        {"record": name, "rule": "allocation_end_passed", "host": _ELSEWHERE}
    ]
    # Still active is still active, however long ago the recorded end.
    answers[0] = False
    assert _evidence(manager, attempt_id).pending == LaunchEnd(name, "allocation_active", _ELSEWHERE)


def test_without_a_scheduler_client_here_the_end_time_decides(
    manager: TaskManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[object] = []
    monkeypatch.setattr(SLURM, "can_query", lambda: False)
    monkeypatch.setattr(_allocation, "allocation_ended", lambda recorded, *, timeout: asked.append(recorded))
    attempt_id = str(uuid.uuid4())
    member = _allocation_member(identity={"job_id": "77"}, end_time=time.time() - LAUNCH_END_GRACE - 1)
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member})
    assert _evidence(manager, attempt_id).as_json() == [
        {"record": name, "rule": "allocation_end_passed", "host": _ELSEWHERE}
    ]
    assert asked == []
    # A site probe that does not exist on this host is not asked either.
    member = _allocation_member(probe="exec:/nonexistent/allocation", identity={"job_id": "1"}, end_time=1.0)
    attempt_id = str(uuid.uuid4())
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member})
    assert _evidence(manager, attempt_id).as_json() == [
        {"record": name, "rule": "allocation_end_passed", "host": _ELSEWHERE}
    ]
    assert asked == []


def test_an_older_record_elsewhere_proves_nothing(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE}, age=_EXPIRED_AGE)
    gone = subprocess.Popen(["true"])
    gone.wait()
    _record(manager, attempt_id, {"pid": gone.pid, "hostname": _HOST})
    evidence = _evidence(manager, attempt_id)
    assert evidence.pending == LaunchEnd(name, "launch_end_unprovable", _ELSEWHERE)
    assert [end.rule for end in evidence.records] == ["process_group_gone"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads files of mode 000")
def test_unreadable_records_prove_nothing(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    directory = _crashed_manager(manager.workspace, heartbeat_age=0.0)
    record = _launch_record(directory, manager.workspace, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE})
    (record / "process.json").chmod(0)
    try:
        assert _evidence(manager, attempt_id).pending == LaunchEnd(None, "launch_records_unreadable")
    finally:
        (record / "process.json").chmod(0o600)


def test_writer_death_and_cancellation_follow_the_ladder(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    state = _dead_writer_state(attempt_id)
    assert _writer_dead(manager, state)
    member = _allocation_member(probe="host", end_time=time.time() + 3600)
    directory = _crashed_manager(manager.workspace, heartbeat_age=0.0)
    record = _launch_record(
        directory, manager.workspace, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member}
    )
    assert not _writer_dead(manager, state)
    assert _cancellation(manager, state) is None
    # The allocation of the launch elsewhere ended long ago.
    (record / "process.json").write_text(
        json.dumps(
            {
                "pid": 4242,
                "hostname": _ELSEWHERE,
                "attempt_id": attempt_id,
                "allocation": {**member, "end_time": time.time() - LAUNCH_END_GRACE - 1},
            }
        )
    )
    assert _writer_dead(manager, state)
    proof = _cancellation(manager, state)
    assert proof is not None and proof["verified"] == "process_group_absent"
    assert proof["launch_end_evidence"] == [
        {"record": f"{directory.name}/{record.name}", "rule": "allocation_end_passed", "host": _ELSEWHERE}
    ]


def test_a_recorded_allocation_is_read_back(manager: TaskManager) -> None:

    allocation = _allocation.Allocation("slurm", 5000.0, identity={"job_id": "9"}, probe="slurm")
    member = _allocation.RecordedAllocation.from_allocation(allocation).as_json()
    attempt_id = str(uuid.uuid4())
    name = _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE, "allocation": member})
    launches = _manager_launches.recorded_launches(manager, attempt_id)
    assert launches is not None
    (launch,) = launches
    assert launch.name == name and launch.allocation == _allocation.RecordedAllocation(
        "slurm", "slurm", {"job_id": "9"}, 5000.0
    )


def test_only_managers_of_this_uid_are_searched(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    _record(manager, attempt_id, {"pid": 4242, "hostname": _ELSEWHERE})
    assert not _evidence(manager, attempt_id).ended
    # Another user's managers start only that user's attempts, and their launches/ is not readable.
    manager.uid = os.getuid() + 1
    assert _evidence(manager, attempt_id).ended


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads directories of mode 000")
def test_a_marker_that_cannot_be_observed_releases_no_gate(tmp_path: Path) -> None:
    hidden = tmp_path / "hidden"
    hidden.mkdir()
    attempt = SimpleNamespace(marker=SimpleNamespace(path=hidden / "marker"), attempt_id="a")
    hidden.chmod(0)
    try:
        assert not _manager_launches._still_running(SimpleNamespace(), attempt)
    finally:
        hidden.chmod(0o700)


def test_collection_keeps_a_step_of_another_allocation_until_its_end(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    gone = subprocess.Popen(["true"])
    gone.wait()
    directory = _crashed_manager(manager.workspace, heartbeat_age=_EXPIRED_AGE)
    ongoing = _allocation_member(identity={"job_id": "77"}, end_time=time.time() + 3600)
    step = _launch_record(
        directory, manager.workspace, attempt_id, {"pid": gone.pid, "hostname": _HOST, "allocation": ongoing}
    )
    plain = _launch_record(directory, manager.workspace, attempt_id, {"pid": gone.pid, "hostname": _HOST})

    def collected(own_identity: dict[str, str] | None = None) -> list[str]:
        return _manager_launches.dead_records(
            directory, hostname=_HOST, grace_seconds=1200.0, now=time.time(), own_identity=own_identity
        )

    # A gone srun group proves nothing about the step's ranks, and collection never asks a scheduler.
    assert collected() == [plain.name]
    assert sorted(collected({"job_id": "77"})) == sorted([plain.name, step.name])
    (step / "process.json").write_text(
        json.dumps(
            {
                "pid": gone.pid,
                "hostname": _HOST,
                "attempt_id": attempt_id,
                "allocation": {**ongoing, "end_time": time.time() - LAUNCH_END_GRACE - 1},
            }
        )
    )
    assert sorted(collected()) == sorted([plain.name, step.name])


def test_online_pruning_removes_a_step_once_the_ladder_says_it_ended(
    manager: TaskManager, monkeypatch: pytest.MonkeyPatch, squeue_installed: None
) -> None:
    monkeypatch.setattr(_allocation, "allocation_ended", lambda recorded, *, timeout: True)
    attempt_id = str(uuid.uuid4())
    gone = subprocess.Popen(["true"])
    gone.wait()
    directory = _crashed_manager(manager.workspace, heartbeat_age=_EXPIRED_AGE)
    member = _allocation_member(identity={"job_id": "77"})
    step = _launch_record(
        directory, manager.workspace, attempt_id, {"pid": gone.pid, "hostname": _HOST, "allocation": member}
    )
    assert [end.rule for end in _evidence(manager, attempt_id).records] == ["scheduler_confirmed_ended"]
    assert not step.exists()


def test_signalling_recorded_launches_skips_a_reused_pid(manager: TaskManager) -> None:
    attempt_id = str(uuid.uuid4())
    other = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        started = _manager_launches._start_time(other.pid)
        if started is None:
            pytest.skip("no /proc on this host")
        _record(manager, attempt_id, {"pid": other.pid, "hostname": _HOST, "process_start": started + 1})
        manager._launch_records = None
        _manager_launches.signal_recorded(manager, attempt_id, signal.SIGTERM)
        time.sleep(0.2)
        assert other.poll() is None, "another process under the recorded pid is not signalled"
    finally:
        other.kill()
        other.wait()
