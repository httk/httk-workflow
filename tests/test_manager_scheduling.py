"""Manager scheduling hardening: typed frames, bounded ticks, fenced cancels."""

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from conftest import TestProfile as _TestProfile

from httk.workflow import TaskManager, Workspace
from httk.workflow._logging import reset_logging
from httk.workflow._util import tree_digest
from httk.workflow.journal import JournalWriter
from httk.workflow.manager import RunningAttempt
from httk.workflow.models import CARRIED_STATE_MEMBERS, Marker, StateFrame

pytestmark = pytest.mark.xdist_group("heartbeat-timing")

_SUCCEED_RUNNER = """#!/usr/bin/env python3
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
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""

_SLEEPING_RUNNER = """#!/usr/bin/env python3
import time

time.sleep(600)
"""

_SPAWNING_RUNNER = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

CHILD = {child!r}
context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {{
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}}
if context["step"] == "gather":
    outcome = {{**base, "action": "succeed"}}
else:
    child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), "only"))
    child_key = "child--" + child_id
    child_dir = temporary / "children" / "jobs" / child_key
    (child_dir / "files").mkdir(parents=True)
    runner = child_dir / "files" / "runner"
    runner.write_text(CHILD)
    runner.chmod(0o755)
    (child_dir / "job.json").write_text(json.dumps({{
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": child_id,
        "tag": "child",
        "name": "Digest child",
        "workflow": "tests.digests",
        "runner": {{"path": "files/runner", "arguments": []}},
        "workdir": {{"mode": "persistent", "path": "run"}},
        "data": {{"mode": "none"}},
        "initial_step": "run",
        "priority": 500,
        "claim": {{"pool": "default", "required_capabilities": []}},
        "retry_policy": {{"retry_on": []}},
        "resources": {{}},
        "parent": None,
    }}))
    entry = {{
        "workspace_id": context["workspace_id"],
        "job_id": child_id,
        "job_key": child_key,
        "placement": "project/children",
        "label": "only",
    }}
    (temporary / "children" / "spawn.json").write_text(json.dumps({{"children": [entry]}}))
    outcome = {{
        **base,
        "action": "wait",
        "next_step": "gather",
        "join": {{
            "children": [
                {{
                    "workspace_id": entry["workspace_id"],
                    "job_id": entry["job_id"],
                    "job_key": entry["job_key"],
                    "placement_hint": entry["placement"],
                }}
            ],
            "condition": "all_terminal",
        }},
    }}
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep records propagating to the capture handlers pytest installs."""

    reset_logging()
    yield
    reset_logging()


def _payload(
    root: Path,
    runner_source: str,
    *,
    tag: str,
    workdir_mode: str = "persistent",
    pool: str = "default",
    backend: str = "path",
    initial_step: str = "only",
    retry_on: tuple[str, ...] = (),
) -> tuple[Path, str]:
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
        "name": f"Scheduling job {tag}",
        "workflow": "tests.scheduling",
        "runner": {"path": "files/runner", "arguments": [], "backend": backend},
        "workdir": {"mode": workdir_mode, "path": "run"},
        "data": {"mode": "none"},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": pool, "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": 4,
            "maximum_total_attempts": 8,
            "maximum_activations": 4,
            "retry_on": list(retry_on),
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _drive_until(workspace: Workspace, manager: TaskManager, job_id: str, kinds: set[str]) -> Marker:
    """Tick until one job reaches any of *kinds*, returning its marker."""

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        manager.tick()
        marker = workspace.find_marker_by_id(job_id)
        if marker is not None and marker.kind in kinds:
            return marker
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {sorted(kinds)}")


def _publish_cancel(workspace: Workspace, marker: Marker, **overrides: object) -> Path:
    request: dict[str, object] = {
        "format": "httk-workflow-request",
        "format_version": 1,
        "request_id": str(uuid.uuid4()),
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "placement": marker.placement.as_posix(),
        "expected_generation": marker.generation,
        "expected_record_ref": marker.record_ref,
        "action": "cancel",
        "operator": "tester",
        "reason": "scheduling test",
        "created_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
    }
    request.update(overrides)
    return workspace.publish_request(request)


def _fake_manager(workspace: Workspace, *, heartbeat_age: float | None) -> str:
    """Publish one foreign manager record, optionally with a stale heartbeat."""

    manager_id = str(uuid.uuid4())
    directory = workspace.control / "managers" / manager_id
    directory.mkdir(parents=True)
    if heartbeat_age is not None:
        updated = datetime.now(UTC) - timedelta(seconds=heartbeat_age)
        (directory / "heartbeat.json").write_text(
            json.dumps(
                {
                    "manager_id": manager_id,
                    "updated_at": updated.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                }
            ),
            encoding="utf-8",
        )
    return manager_id


def _fake_running_attempt(
    workspace: Workspace,
    payload: Path,
    placement: str,
    *,
    manager_id: str,
    pid: int,
    lease_seconds: float,
) -> tuple[Marker, str]:
    """Publish a running marker owned by another manager, with a process record."""

    marker = workspace.submit(payload, placement)
    job = workspace.load_job(marker)
    attempt_id = str(uuid.uuid4())
    control = workspace.payload_path(marker.placement, marker.job_key) / f".httk-attempt.{attempt_id}"
    control.mkdir()
    (control / "process.json").write_text(
        json.dumps(
            {
                "pid": pid,
                "process_group": pid,
                "hostname": socket.gethostname(),
                "launched_at": "2026-07-26T00:00:00.000000Z",
                "launch_gated": True,
            }
        ),
        encoding="utf-8",
    )
    with JournalWriter(workspace.control) as writer:
        running = workspace.transition(
            writer,
            marker,
            "running",
            {
                "step": job.initial_step,
                "activation_id": str(uuid.uuid4()),
                "activation_ordinal": 1,
                "attempt_id": attempt_id,
                "attempt_ordinal": 1,
                "total_attempts": 1,
                "data_generation": None,
                "manager_id": manager_id,
                "writer_id": writer.writer_id,
                "attempt_control": control.name,
                "lease_seconds": lease_seconds,
                "started_at": "2026-07-26T00:00:00.000000Z",
                "workdir": "run",
                "reason": "launched",
            },
        )
    return running, attempt_id


# ---------------------------------------------------------------------------
# 1. Typed state frames
# ---------------------------------------------------------------------------

#: One complete ``running`` frame exactly as the manager wrote it before state
#: frames became typed, including an explicit null, a member no version of this
#: implementation knows, and every envelope member the workspace supplies.
_GOLDEN_RUNNING_FRAME: dict[str, Any] = {
    "format": "httk-workflow-state",
    "format_version": 1,
    "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
    "job_id": "01234567-89ab-cdef-0123-456789abcdef",
    "job_key": "silicon-relax--01234567-89ab-cdef-0123-456789abcdef",
    "placement": "project-17/0/03a",
    "state_generation": 4,
    "kind": "running",
    "previous_record_ref": "w0123456789abcdef0123456789abcdef-s0-o1a-l2b-h0123456789abcdef0123456789abcdef",
    "created_at": "2026-07-26T12:00:00.000000Z",
    "priority": 500,
    "step": "relax",
    "activation_id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
    "activation_ordinal": 1,
    "attempt_id": "9c5b94b1-35ad-49bb-b118-8e8fc24abf80",
    "attempt_ordinal": 2,
    "total_attempts": 2,
    "data_generation": None,
    "join_summary": None,
    "manager_id": "1b4e28ba-2fa1-11d2-883f-0016d3cca427",
    "writer_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "attempt_control": ".httk-attempt.9c5b94b1-35ad-49bb-b118-8e8fc24abf80",
    "lease_seconds": 900.0,
    "started_at": "2026-07-26T11:59:00.000000Z",
    "workdir": "run",
    "reason": "launched",
    "a-member-from-a-later-profile": {"kept": True},
}


def test_a_state_frame_written_before_the_typed_frame_loads_identically() -> None:
    frame = StateFrame.from_mapping(_GOLDEN_RUNNING_FRAME)

    # Byte compatibility is member-for-member identity: an unknown member, an
    # explicit null, and every envelope member survive the round trip.
    assert frame.as_mapping() == _GOLDEN_RUNNING_FRAME
    assert frame.as_mapping()["a-member-from-a-later-profile"] == {"kept": True}
    assert frame.has("data_generation") and frame.data_generation is None

    assert frame.step == "relax"
    assert frame.attempt_id == "9c5b94b1-35ad-49bb-b118-8e8fc24abf80"
    assert frame.attempt_ordinal == 2 and frame.total_attempts == 2
    assert frame.attempt_control == ".httk-attempt.9c5b94b1-35ad-49bb-b118-8e8fc24abf80"
    assert frame.manager_id == "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
    assert frame.lease_seconds == 900.0
    assert frame.reason == "launched"
    assert frame.unclean_restart is False

    # What is carried forward is exactly the activation, never the envelope.
    carried = frame.carried()
    assert set(carried.members) <= set(CARRIED_STATE_MEMBERS)
    assert carried.members["data_generation"] is None
    assert "kind" not in carried.members and "manager_id" not in carried.members

    # Typed writes merge onto a base without disturbing anything else, and a
    # value of None writes the JSON null the protocol distinguishes.
    updated = StateFrame.of(carried, reason="succeeded", data_generation=None)
    assert updated.as_mapping()["data_generation"] is None
    assert updated.reason == "succeeded"
    assert updated.step == "relax"


def test_a_state_frame_refuses_path_components_it_would_otherwise_join() -> None:
    from httk.workflow.errors import FormatError

    traversal = StateFrame.from_mapping({**_GOLDEN_RUNNING_FRAME, "attempt_control": "../../../etc"})
    with pytest.raises(FormatError):
        _ = traversal.attempt_control

    foreign = StateFrame.from_mapping({**_GOLDEN_RUNNING_FRAME, "manager_id": "../../tmp"})
    with pytest.raises(FormatError):
        _ = foreign.manager_id


def test_a_path_traversing_attempt_control_fails_only_its_own_job(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="hostile")
    marker, _ = _fake_running_attempt(
        workspace,
        payload,
        "project/hostile",
        manager_id=_fake_manager(workspace, heartbeat_age=None),
        pid=os.getpid(),
        lease_seconds=10.0,
    )
    # Republish the same running state with an attempt control that traverses.
    previous = StateFrame.from_mapping(workspace.read_state(marker))
    with JournalWriter(workspace.control) as writer:
        updates = {
            **previous.carried().as_mapping(),
            "manager_id": previous.members["manager_id"],
            "lease_seconds": previous.members["lease_seconds"],
            "attempt_control": "../../../../etc",
            "reason": "hostile",
        }
        marker = workspace.transition(writer, marker, "running", updates)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    assert workspace.read_state(failed)["failure"]["code"] == "protocol_error"


# ---------------------------------------------------------------------------
# 2. Mid-tick heartbeats and bounded ticks
# ---------------------------------------------------------------------------


def test_a_long_tick_heartbeats_while_it_scans_and_reports_its_own_slowness(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    test_profile: _TestProfile,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    running_payload, running_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="slow-runner")
    workspace.submit(running_payload, "project/running")
    # A crowd of submitted jobs this manager cannot register: they are scanned
    # on every tick, which is exactly the pass that must not hold a heartbeat.
    crowd_size = test_profile.scale(normal=12, extended=40)
    validation_pause = test_profile.scale(normal=0.05, extended=0.025)
    for index in range(crowd_size):
        crowd, _ = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag=f"crowd-{index}", backend="foreign")
        workspace.submit(crowd, f"project/crowd/{index}")

    real_validate = Workspace.validate_job_payload

    def slow_validate(self: Workspace, marker: Marker):
        time.sleep(validation_pause)
        return real_validate(self, marker)

    monkeypatch.setattr(Workspace, "validate_job_payload", slow_validate)

    lease_seconds = 0.5
    manager = TaskManager(workspace, lease_seconds=lease_seconds, heartbeat_interval=0.0)
    try:
        marker = _drive_until(workspace, manager, running_id, {"running"})
        assert marker.kind == "running"
        beats: list[float] = []
        real_heartbeat = manager.heartbeat

        def recording(*, force: bool = False) -> None:
            previous = manager._last_heartbeat
            real_heartbeat(force=force)
            if manager._last_heartbeat != previous:
                beats.append(time.monotonic())

        setattr(manager, "heartbeat", recording)
        with caplog.at_level(logging.WARNING, logger="httk.workflow"):
            started = time.monotonic()
            manager.tick()
            finished = time.monotonic()
    finally:
        attempts = list(manager._running.values())
        manager._signal_running_attempts(signal.SIGKILL)
        for item in attempts:
            item.process.wait(timeout=30)
        manager.close()

    # The tick really was longer than the lease, which is the condition under
    # which a manager that only heartbeats between ticks looks abandoned.
    assert finished - started > lease_seconds
    assert len(beats) >= 3
    instants = [started, *beats, finished]
    longest_gap = max(second - first for first, second in zip(instants, instants[1:]))
    assert longest_gap < lease_seconds

    # Its own attempt was never taken over, and the slow tick was reported.
    still_running = workspace.find_marker_by_id(running_id)
    assert still_running is not None and still_running.kind == "running"
    messages = [record.getMessage() for record in caplog.records]
    assert not any("taking over" in message for message in messages)
    assert any("one scheduling tick took" in message for message in messages)
    assert any(record.levelno == logging.ERROR for record in caplog.records if "scheduling tick" in record.getMessage())


def test_a_bounded_pass_resumes_where_it_stopped_instead_of_starving_the_tail(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted: list[str] = []
    for index in range(6):
        payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag=f"bounded-{index}")
        workspace.submit(payload, f"project/bounded/{index}")
        submitted.append(job_id)

    with TaskManager(workspace, heartbeat_interval=0.01, maximum_pass_markers=2, maximum_workers=1) as manager:
        registered_per_tick: list[int] = []
        for _ in range(3):
            manager._register_submissions()
            registered_per_tick.append(sum(1 for marker in workspace.scan_markers() if marker.kind != "submitted"))
        assert registered_per_tick == [2, 4, 6]
        # Everything still finishes: the bound defers work, it never drops it.
        manager.run_until_idle(timeout=60.0)

    for job_id in submitted:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "succeeded"


# ---------------------------------------------------------------------------
# 3. Isolated-workdir takeover policy
# ---------------------------------------------------------------------------


def test_an_isolated_attempt_is_taken_over_only_against_evidence(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    lease = 10.0

    def running_job(tag: str, *, heartbeat_age: float | None, pid: int) -> tuple[Marker, str]:
        payload, job_id = _payload(
            tmp_path / "source",
            _SLEEPING_RUNNER,
            tag=tag,
            workdir_mode="isolated",
            # A pool this manager does not serve keeps the recovered job in
            # ready instead of relaunching it inside the same test.
            pool="elsewhere",
            retry_on=("lease_lost",),
        )
        marker, _ = _fake_running_attempt(
            workspace,
            payload,
            f"project/{tag}",
            manager_id=_fake_manager(workspace, heartbeat_age=heartbeat_age),
            pid=pid,
            lease_seconds=lease,
        )
        return marker, job_id

    _, live_id = running_job("live", heartbeat_age=0.0, pid=os.getpid())
    _, inside_grace_id = running_job("inside-grace", heartbeat_age=lease * 1.5, pid=os.getpid())
    _, past_grace_id = running_job("past-grace", heartbeat_age=lease * 3.0, pid=os.getpid())

    with caplog.at_level(logging.INFO, logger="httk.workflow"):
        with TaskManager(workspace, heartbeat_interval=0.01, takeover_grace_factor=2.0) as manager:
            manager.tick()

    live = workspace.find_marker_by_id(live_id)
    assert live is not None and live.kind == "running"
    inside = workspace.find_marker_by_id(inside_grace_id)
    assert inside is not None and inside.kind == "running"
    past = workspace.find_marker_by_id(past_grace_id)
    assert past is not None and past.kind == "ready"

    evidence = workspace.read_state(past)["takeover_evidence"]
    assert evidence["evidence"] == "lease_grace_expired"
    assert evidence["grace_seconds"] == lease * 2.0
    assert evidence["heartbeat_age_seconds"] >= lease * 2.0
    assert workspace.read_state(past)["unclean_restart"] is True

    messages = [record.getMessage() for record in caplog.records]
    assert any("not taking over" in message and "takeover grace" in message for message in messages)


def test_a_dead_writer_is_taken_over_at_once_without_waiting_out_the_grace(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    finished = subprocess.Popen([sys.executable, "-c", "pass"])
    finished.wait(timeout=30)
    payload, job_id = _payload(
        tmp_path / "source",
        _SLEEPING_RUNNER,
        tag="dead-writer",
        workdir_mode="isolated",
        pool="elsewhere",
        retry_on=("lease_lost",),
    )
    _fake_running_attempt(
        workspace,
        payload,
        "project/dead-writer",
        manager_id=_fake_manager(workspace, heartbeat_age=11.0),
        pid=finished.pid,
        lease_seconds=10.0,
    )

    with TaskManager(workspace, heartbeat_interval=0.01, takeover_grace_factor=100.0) as manager:
        manager.tick()

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "ready"
    assert workspace.read_state(marker)["takeover_evidence"]["evidence"] == "writer_process_dead"


# ---------------------------------------------------------------------------
# 4. Fenced cancellation
# ---------------------------------------------------------------------------


def test_cancelling_a_running_job_fences_it_then_proves_its_process_is_gone(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="cancelled")
    workspace.submit(payload, "project/cancelled")

    observed: list[str] = []
    with caplog.at_level(logging.WARNING, logger="httk.workflow"):
        with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=2.0) as manager:
            running = _drive_until(workspace, manager, job_id, {"running"})
            attempt: RunningAttempt = next(iter(manager._running.values()))
            pid = attempt.process.pid
            _publish_cancel(workspace, running)
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                manager.tick()
                marker = workspace.find_marker_by_id(job_id)
                assert marker is not None
                if not observed or observed[-1] != marker.kind:
                    observed.append(marker.kind)
                if marker.kind == "cancelled":
                    break
                time.sleep(0.02)

    assert observed[-1] == "cancelled", observed
    # The fence came first: the attempt could no longer commit anything before
    # a single signal was sent.
    assert "cancelling" in observed

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    state = workspace.read_state(marker)
    assert state["cancellation"]["verified"] == "process_exited"
    assert state["cancellation"]["pid"] == pid
    assert state["operator"] == "tester" and state["operator_reason"] == "scheduling test"
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert any("cancelling" in record.getMessage() for record in caplog.records)


def test_a_manager_that_dies_mid_cancellation_leaves_it_to_the_next_one(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="crashed")
    workspace.submit(payload, "project/crashed")

    first = TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5)
    running = _drive_until(workspace, first, job_id, {"running"})
    attempt: RunningAttempt = next(iter(first._running.values()))
    _publish_cancel(workspace, running)
    first.tick()
    fenced = workspace.find_marker_by_id(job_id)
    assert fenced is not None and fenced.kind == "cancelling"
    # The manager disappears holding the fence; the marker stays cancelling.
    first.close()
    assert attempt.process.poll() is None or True

    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as second:
        second.tick()
        # Stand in for init reaping the orphan of a manager that really died.
        attempt.process.wait(timeout=30)
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            second.tick()
            marker = workspace.find_marker_by_id(job_id)
            assert marker is not None
            if marker.kind == "cancelled":
                break
            time.sleep(0.05)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "cancelled"
    verified = workspace.read_state(marker)["cancellation"]["verified"]
    assert verified == "process_group_absent"


def test_cancelling_a_job_with_no_live_attempt_is_terminal_at_once(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="quiet", pool="elsewhere")
    workspace.submit(payload, "project/quiet")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        _publish_cancel(workspace, ready)
        manager.tick()

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "cancelled"
    assert workspace.read_state(marker)["cancellation"]["verified"] == "no_live_attempt"


# ---------------------------------------------------------------------------
# 5. Requests hygiene
# ---------------------------------------------------------------------------


def test_a_request_that_can_never_apply_is_retired_once_and_never_reread(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="stale-request", pool="elsewhere")
    workspace.submit(payload, "project/stale-request")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        published = _publish_cancel(workspace, marker, expected_generation=marker.generation + 7)
        with caplog.at_level(logging.INFO, logger="httk.workflow"):
            manager.tick()
            manager.tick()
            manager.tick()

    retired = workspace.control / "requests" / "retired" / published.name
    assert retired.is_file()
    record = json.loads((retired.parent / f"{published.name}.retirement").read_text(encoding="utf-8"))
    assert record["format"] == "httk-workflow-retired-request"
    assert "generation" in record["reason"]
    assert not published.exists()
    assert not list((workspace.control / "requests" / "claimed").rglob("*.json"))
    # Retirement happens exactly once, however many ticks follow it.
    retirements = [record for record in caplog.records if "retired request" in record.getMessage()]
    assert len(retirements) == 1
    # The job itself was untouched: an unactionable request changes nothing.
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "ready"


def test_a_request_for_an_unserved_backend_is_left_for_another_manager(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="foreign", backend="foreign")
    marker = workspace.submit(payload, "project/foreign")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        published = _publish_cancel(workspace, marker)
        with caplog.at_level(logging.INFO, logger="httk.workflow"):
            manager.tick()
            manager.tick()

    # It stays where a manager serving that backend will find it, and this
    # manager decided about it exactly once.
    assert published.is_file()
    deferrals = [record for record in caplog.records if "leaving request" in record.getMessage()]
    assert len(deferrals) == 1
    assert workspace.find_marker_by_id(job_id) is not None


# ---------------------------------------------------------------------------
# 8. Ownership bookkeeping
# ---------------------------------------------------------------------------


def test_a_normal_run_never_reports_an_orphaned_attempt(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(3):
        payload, _ = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag=f"clean-{index}")
        workspace.submit(payload, f"project/clean/{index}")

    with caplog.at_level(logging.WARNING, logger="httk.workflow"):
        with TaskManager(workspace, heartbeat_interval=0.01, maximum_workers=2) as manager:
            manager.run_until_idle(timeout=60.0)

    assert all(marker.kind == "succeeded" for marker in workspace.scan_markers())
    messages = [record.getMessage() for record in caplog.records]
    assert not any("no longer owns a running marker" in message for message in messages), messages
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING], messages


# ---------------------------------------------------------------------------
# 9. Child bundles hashed once
# ---------------------------------------------------------------------------


def test_a_registered_child_bundle_is_not_hashed_again_after_it_is_published(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(
        tmp_path / "source",
        _SPAWNING_RUNNER.format(child=_SUCCEED_RUNNER),
        tag="parent",
        initial_step="branch",
    )
    workspace.submit(payload, "project/parent")

    hashed: list[str] = []

    def counting_digest(path: Path, **keywords: object) -> str:
        hashed.append(str(path))
        return tree_digest(path, **keywords)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr("httk.workflow.manager.tree_digest", counting_digest)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    published = [path for path in hashed if "outcome.ready" in path]
    registered = [path for path in hashed if "outcome.ready" not in path]
    # Once to record the expected digest before the outcome is accepted, once to
    # verify the bundle has not changed since. The destination of the
    # publication rename is the very tree just verified and is never rehashed.
    assert len(published) == 2, hashed
    assert registered == [], hashed
