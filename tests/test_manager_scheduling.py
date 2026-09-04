"""Manager scheduling hardening: typed frames, bounded ticks, fenced cancels."""

import errno
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest
from httk.core.digests import tree_digest
from httk.core.identity import ensure_identity_key

import httk.workflow.manager as manager_module
from conftest import TestProfile as _TestProfile
from httk.workflow import TaskManager, Workspace, _manager_requests
from httk.workflow._logging import reset_logging
from httk.workflow.journal import JournalWriter, read_record
from httk.workflow.manager import RunningAttempt
from httk.workflow.models import CARRIED_STATE_MEMBERS, Marker, StateFrame

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("heartbeat-timing")]

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
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

_SLEEPING_RUNNER = """#!/usr/bin/env python3
import time

time.sleep(600)
"""

_PUBLISH_THEN_HANG_RUNNER = """#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
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
time.sleep(600)
"""

_PARTIAL_RETRY_RUNNER = """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
print("partial", end="", flush=True)
if context["attempt_ordinal"] == 1:
    sys.exit(7)
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

_SPAWNING_RUNNER = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

CHILD = {child!r}
context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {{
    "format": "httk-workflow-outcome",
    "format_version": 2,
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
        "format_version": 2,
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
    executor: str = "path",
    initial_step: str = "only",
    retry_on: tuple[str, ...] = (),
    resources: dict[str, int] | None = None,
    step_resources: dict[str, dict[str, int]] | None = None,
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
        "format_version": 2,
        "id": job_id,
        "tag": tag,
        "name": f"Scheduling job {tag}",
        "workflow": "tests.scheduling",
        "runner": {"path": "files/runner", "arguments": [], "executor": executor},
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
        "resources": {} if resources is None else resources,
        "step_resources": {} if step_resources is None else step_resources,
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
        "format_version": 2,
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


def _publish_pause(workspace: Workspace, marker: Marker, *, reason: str = "pause now") -> Path:
    return _publish_cancel(workspace, marker, action="pause", reason=reason)


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
    hostname: str | None = None,
) -> tuple[Marker, str]:
    """Publish a running marker owned by another manager, with a process member."""

    marker = workspace.submit(payload, placement)
    job = workspace.load_job(marker)
    attempt_id = str(uuid.uuid4())
    control = workspace.payload_path(marker.placement, marker.job_key) / f"attempts/{attempt_id}"
    control.mkdir(parents=True)
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
                "attempt_control": f"attempts/{attempt_id}",
                "lease_seconds": lease_seconds,
                "started_at": "2026-07-26T00:00:00.000000Z",
                "workdir": "run",
                "process": {
                    "pid": pid,
                    "process_group": pid,
                    "hostname": socket.gethostname() if hostname is None else hostname,
                    "launched_at": "2026-07-26T00:00:00.000000Z",
                },
                "reason": "launched",
            },
        )
    return running, attempt_id


def _publish_pending_environment(
    control: Path, *, deadline: float, job_id: str, activation_id: str, attempt_id: str
) -> None:
    """Install a pending environment marker and a successful outcome."""

    (control / ".httk-environment-resolution.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-environment-resolution-marker",
                "format_version": 2,
                "status": "resolved",
                "values": {"value": {"value": "manifest", "source": "default"}},
                "log_pending": True,
                "log_deadline": deadline,
            }
        ),
        encoding="utf-8",
    )
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    (temporary / "outcome.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-outcome",
                "format_version": 2,
                "job_id": job_id,
                "activation_id": activation_id,
                "attempt_id": attempt_id,
                "action": "succeed",
            }
        ),
        encoding="utf-8",
    )
    os.rename(temporary, control / "outcome.ready")


# ---------------------------------------------------------------------------
# 1. Typed state frames
# ---------------------------------------------------------------------------

#: One complete ``running`` frame exactly as the manager wrote it before state
#: frames became typed, including an explicit null, a member no version of this
#: implementation knows, and every envelope member the workspace supplies.
_GOLDEN_RUNNING_FRAME: dict[str, Any] = {
    "format": "httk-workflow-state",
    "format_version": 2,
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
    "attempt_control": "attempts/9c5b94b1-35ad-49bb-b118-8e8fc24abf80",
    "lease_seconds": 900.0,
    "started_at": "2026-07-26T11:59:00.000000Z",
    "workdir": "run",
    "process": {
        "pid": 12345,
        "process_group": 12345,
        "hostname": "compute-17",
        "launched_at": "2026-07-26T11:59:00.000000Z",
    },
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
    assert frame.attempt_control == "attempts/9c5b94b1-35ad-49bb-b118-8e8fc24abf80"
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
    updated = StateFrame.replace(carried, reason="succeeded", data_generation=None)
    assert updated.as_mapping()["data_generation"] is None
    assert updated.reason == "succeeded"
    assert updated.step == "relax"


def test_a_state_frame_refuses_path_components_it_would_otherwise_join() -> None:
    from httk.workflow.errors import FormatError

    for value in (
        f".httk-{'attempt'}.{uuid.uuid4()}",
        "attempts/../x",
        f"attempts/{uuid.uuid4()}/y",
    ):
        traversal = StateFrame.from_mapping({**_GOLDEN_RUNNING_FRAME, "attempt_control": value})
        with pytest.raises(FormatError):
            _ = traversal.attempt_control

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
            "process": previous.members["process"],
            "reason": "hostile",
        }
        marker = workspace.transition(writer, marker, "running", updates)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    assert workspace.read_state(failed)["failure"]["code"] == "protocol_error"


def test_running_frame_records_launcher_identity_without_attempt_metadata_file(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="running-process")
    workspace.submit(payload, "project/running-process")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running = _drive_until(workspace, manager, job_id, {"running"})
        state = workspace.read_state(running)
        process = state["process"]
        assert isinstance(process, dict)
        local = manager._running[state["attempt_id"]]
        assert process["pid"] == local.process.pid
        attempt = workspace.payload_path(running.placement, running.job_key) / str(state["attempt_control"])
        assert not list(attempt.iterdir())
        manager._signal_running_attempts(signal.SIGKILL)


@pytest.mark.parametrize("process_group", [0, "123"])
def test_malformed_process_group_is_never_signalled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, process_group: object
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _ = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag=f"bad-process-group-{process_group}")
    marker, _ = _fake_running_attempt(
        workspace,
        payload,
        "project/bad-process-group",
        manager_id=_fake_manager(workspace, heartbeat_age=None),
        pid=os.getpid(),
        lease_seconds=10.0,
    )
    called: list[tuple[int, int]] = []

    def fail_killpg(group: int, signal_number: int) -> None:
        called.append((group, signal_number))
        raise AssertionError("malformed process group reached os.killpg")

    monkeypatch.setattr(manager_module.os, "killpg", fail_killpg)
    state = StateFrame.from_mapping(workspace.read_state(marker))
    process = dict(state.members["process"])
    process["process_group"] = process_group
    bad_state = StateFrame.replace(state, process=process)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._terminate_attempt(marker, bad_state)
    assert called == []


def test_running_to_failed_retains_process_identity_on_malformed_outcome(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="failed-process")
    marker, _ = _fake_running_attempt(
        workspace,
        payload,
        "project/failed-process",
        manager_id=_fake_manager(workspace, heartbeat_age=None),
        pid=os.getpid(),
        lease_seconds=10.0,
    )
    original = workspace.read_state(marker)["process"]
    state = workspace.read_state(marker)
    outcome = workspace.payload_path(marker.placement, marker.job_key) / str(state["attempt_control"]) / "outcome.ready"
    outcome.mkdir()
    (outcome / "outcome.json").write_text("{}", encoding="utf-8")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    failed_state = workspace.read_state(failed)
    assert failed_state["failure"]["code"] == "protocol_error"
    assert failed_state["process"] == original


def test_commit_failure_rebuild_retains_process_identity(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="commit-failure-process")
    marker, _ = _fake_running_attempt(
        workspace,
        payload,
        "project/commit-failure-process",
        manager_id=_fake_manager(workspace, heartbeat_age=None),
        pid=os.getpid(),
        lease_seconds=10.0,
    )
    state = StateFrame.from_mapping(workspace.read_state(marker))
    original = state.members["process"]
    outcome = (
        workspace.payload_path(marker.placement, marker.job_key)
        / str(state.members["attempt_control"])
        / "outcome.ready"
    )
    outcome.mkdir()
    (outcome / "outcome.json").write_text("{}", encoding="utf-8")
    committing = StateFrame.replace(
        state.carried(),
        process=original,
        manager_id=str(uuid.uuid4()),
        writer_id=str(uuid.uuid4()),
        attempt_id=state.members["attempt_id"],
        attempt_control=state.members["attempt_control"],
        outcome_action="succeed",
        child_digests={},
        child_labels={},
        reason="outcome_published",
    )
    with JournalWriter(workspace.control) as writer:
        marker = workspace.transition(writer, marker, "committing", committing.as_mapping())

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        assert manager._resume_committing() is True

    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    assert workspace.read_state(failed)["process"] == original


def test_a_symlinked_attempts_directory_fails_launch_without_gc_escape(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="symlinked-attempts")
    submitted = workspace.submit(payload, "project/symlinked-attempts")
    installed = workspace.payload_path(submitted.placement, submitted.job_key)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "must-survive"
    sentinel.write_text("outside\n", encoding="utf-8")
    (installed / "attempts").symlink_to(outside, target_is_directory=True)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=15.0)

    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    assert workspace.read_state(failed)["failure"]["code"] == "protocol_error"
    workspace.set_policy({"retention": {"attempt_control_days": 0.0, "trash_days": 0.0}})
    workspace.collect_garbage()
    assert sentinel.read_text(encoding="utf-8") == "outside\n"
    assert (installed / "attempts").is_symlink()


def test_a_restarted_manager_rejects_symlinked_attempt_container(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="recovery-symlink")
    stale_manager = _fake_manager(workspace, heartbeat_age=3600.0)
    running, _ = _fake_running_attempt(
        workspace,
        payload,
        "project/recovery-symlink",
        manager_id=stale_manager,
        pid=os.getpid(),
        lease_seconds=1.0,
    )
    installed = workspace.payload_path(running.placement, running.job_key)
    attempts = installed / "attempts"
    saved = tmp_path / "saved-attempts"
    attempts.rename(saved)
    outside = tmp_path / "outside-recovery"
    outside.mkdir()
    sentinel = outside / "must-survive"
    sentinel.write_text("outside\n", encoding="utf-8")
    attempts.symlink_to(outside, target_is_directory=True)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    assert workspace.read_state(failed)["failure"]["code"] == "protocol_error"
    assert sentinel.read_text(encoding="utf-8") == "outside\n"


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
        crowd, _ = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag=f"crowd-{index}", executor="foreign")
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

        setattr(manager, "heartbeat", recording)  # noqa: B010 - test replaces a method deliberately
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
    longest_gap = max(second - first for first, second in pairwise(instants))
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

    with (
        caplog.at_level(logging.INFO, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01, takeover_grace_factor=2.0) as manager,
    ):
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


def test_a_takeover_relaunch_records_a_fresh_process_identity(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    finished = subprocess.Popen([sys.executable, "-c", "pass"])
    finished.wait(timeout=30)
    payload, job_id = _payload(
        tmp_path / "source",
        _SLEEPING_RUNNER,
        tag="fresh-takeover-process",
        workdir_mode="isolated",
        retry_on=("lease_lost",),
    )
    _fake_running_attempt(
        workspace,
        payload,
        "project/fresh-takeover-process",
        manager_id=_fake_manager(workspace, heartbeat_age=11.0),
        pid=finished.pid,
        lease_seconds=10.0,
    )

    with TaskManager(workspace, heartbeat_interval=0.01, takeover_grace_factor=100.0) as manager:
        running = _drive_until(workspace, manager, job_id, {"running"})
        state = workspace.read_state(running)
        process = state["process"]
        assert isinstance(process, dict)
        assert process["pid"] != finished.pid
        manager._signal_running_attempts(signal.SIGKILL)


def test_dead_environment_writer_is_reconciled_after_its_grace(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    finished = subprocess.Popen([sys.executable, "-c", "pass"])
    finished.wait(timeout=30)
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="dead-environment-writer")
    marker, attempt_id = _fake_running_attempt(
        workspace,
        payload,
        "project/dead-environment-writer",
        manager_id=_fake_manager(workspace, heartbeat_age=0.0),
        pid=finished.pid,
        lease_seconds=10.0,
    )
    state = workspace.read_state(marker)
    control = workspace.payload_path(marker.placement, marker.job_key) / str(state["attempt_control"])
    _publish_pending_environment(
        control,
        deadline=time.time() - 1.0,
        job_id=job_id,
        activation_id=str(state["activation_id"]),
        attempt_id=attempt_id,
    )

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._poll_running()

    committed = workspace.find_marker_by_id(job_id)
    assert committed is not None and committed.kind == "committing"
    recorded = json.loads((control / ".httk-environment-resolution.json").read_text(encoding="utf-8"))
    assert recorded["log_pending"] is False
    assert recorded["log_absent"] is True


def test_pending_environment_outcomes_do_not_sleep_per_outcome(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    pending: list[tuple[str, Marker, Path]] = []
    for index in range(12):
        payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag=f"pending-environment-{index}")
        marker, attempt_id = _fake_running_attempt(
            workspace,
            payload,
            f"project/pending-environment/{index}",
            manager_id=_fake_manager(workspace, heartbeat_age=0.0),
            pid=os.getpid(),
            lease_seconds=10.0,
        )
        state = workspace.read_state(marker)
        control = workspace.payload_path(marker.placement, marker.job_key) / str(state["attempt_control"])
        _publish_pending_environment(
            control,
            deadline=time.time() + 60.0,
            job_id=job_id,
            activation_id=str(state["activation_id"]),
            attempt_id=attempt_id,
        )
        pending.append((job_id, marker, control))

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        started = time.monotonic()
        manager._poll_running()
        elapsed = time.monotonic() - started

    assert elapsed < 0.75
    for job_id, _, control in pending:
        still_running = workspace.find_marker_by_id(job_id)
        assert still_running is not None and still_running.kind == "running"
        recorded = json.loads((control / ".httk-environment-resolution.json").read_text(encoding="utf-8"))
        assert recorded["log_pending"] is True


def test_environment_log_clear_wins_the_reconciliation_toctou(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="environment-log-toctou")
    marker, attempt_id = _fake_running_attempt(
        workspace,
        payload,
        "project/environment-log-toctou",
        manager_id=_fake_manager(workspace, heartbeat_age=0.0),
        pid=os.getpid(),
        lease_seconds=10.0,
    )
    state = workspace.read_state(marker)
    control = workspace.payload_path(marker.placement, marker.job_key) / str(state["attempt_control"])
    _publish_pending_environment(
        control,
        deadline=time.time() - 1.0,
        job_id=job_id,
        activation_id=str(state["activation_id"]),
        attempt_id=attempt_id,
    )
    marker_path = control / ".httk-environment-resolution.json"
    real_read_json = manager_module.read_json
    reads = 0

    def clear_after_first_read(path: Path) -> dict[str, Any]:
        nonlocal reads
        result = real_read_json(path)
        reads += 1
        if reads == 1:
            cleared = dict(result)
            cleared["log_pending"] = False
            real_write = marker_path.write_text
            real_write(json.dumps(cleared), encoding="utf-8")
        return result

    monkeypatch.setattr(manager_module, "read_json", clear_after_first_read)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        state_frame = manager._read_frame(marker)
        assert manager._environment_log_ready(marker, state_frame)

    recorded = json.loads(marker_path.read_text(encoding="utf-8"))
    assert recorded["log_pending"] is False
    assert "log_absent" not in recorded


def test_transient_marker_ownership_failure_preserves_a_live_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="transient-ownership")
    workspace.submit(payload, "project/transient-ownership")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running = _drive_until(workspace, manager, job_id, {"running"})
        attempt: RunningAttempt = next(iter(manager._running.values()))
        marker_path = running.path
        real_lstat = Path.lstat
        failed = False

        def flaky_lstat(path: Path):
            nonlocal failed
            if path == marker_path and not failed:
                failed = True
                raise OSError(errno.EIO, "transient filesystem failure")
            return real_lstat(path)

        monkeypatch.setattr(Path, "lstat", flaky_lstat)
        manager._poll_running()
        assert failed
        assert attempt.process.poll() is None
        assert attempt.attempt_id in manager._running

        manager._poll_running()
        assert attempt.attempt_id in manager._running
        assert attempt.process.poll() is None


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
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=2.0) as manager,
    ):
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
    assert state["process"]["pid"] == pid
    assert state["operator"] == "tester" and state["operator_reason"] == "scheduling test"
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert any("cancelling" in record.getMessage() for record in caplog.records)


def test_foreign_host_cancellation_keeps_process_identity_on_unverifiable_rewrite(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="foreign-cancel")
    marker, _ = _fake_running_attempt(
        workspace,
        payload,
        "project/foreign-cancel",
        manager_id=_fake_manager(workspace, heartbeat_age=None),
        pid=os.getpid(),
        lease_seconds=10.0,
        hostname="faraway-host",
    )

    _publish_cancel(workspace, marker)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()

    cancelling = workspace.find_marker_by_id(job_id)
    assert cancelling is not None and cancelling.kind == "cancelling"
    state = workspace.read_state(cancelling)
    assert state["process"]["hostname"] == "faraway-host"
    assert state["cancellation"]["verified"] is None
    assert state["cancellation"]["problem"] == "unverifiable_here"


def test_cancelling_committing_attempt_signals_and_retains_process_identity(
    tmp_path: Path,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _PUBLISH_THEN_HANG_RUNNER, tag="cancel-committing")
    workspace.submit(payload, "project/cancel-committing")

    first = TaskManager(workspace, heartbeat_interval=0.01)
    _drive_until(workspace, first, job_id, {"running"})
    committing = _drive_until(workspace, first, job_id, {"committing"})
    attempt = next(iter(first._running.values()))
    process = attempt.process
    process_pid = process.pid
    committing_state = workspace.read_state(committing)
    assert committing_state["process"]["pid"] == process_pid
    first.close()

    _publish_cancel(workspace, committing)
    try:
        with TaskManager(workspace, heartbeat_interval=0.01) as second:
            second.tick()
            cancelled = workspace.find_marker_by_id(job_id)
            assert cancelled is not None and cancelled.kind == "cancelled"
            process.wait(timeout=30)
            cancelled_state = workspace.read_state(cancelled)
            previous = read_record(workspace.control, str(cancelled_state["previous_record_ref"]))
            assert previous["kind"] == "committing"
            assert previous["process"] == cancelled_state["process"]
    finally:
        if process.poll() is None:
            os.killpg(process_pid, signal.SIGKILL)
            process.wait(timeout=30)


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
    assert attempt.process.poll() in (None, -signal.SIGTERM)

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


def test_deferred_pause_is_recorded_and_consumed_at_attempt_boundary(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="deferred")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running, _ = _fake_running_attempt(
            workspace,
            payload,
            "project/deferred",
            manager_id=manager.manager_id,
            pid=os.getpid(),
            lease_seconds=manager.lease_seconds,
        )
        _publish_pause(workspace, running, reason="wait for the current step")
        assert manager._handle_requests() is True
        current_running = workspace.find_marker_by_id(job_id)
        assert current_running is not None and current_running.kind == "running"
        requested = workspace.read_state(current_running)["pause_requested"]
        assert requested["operator"] == "tester"
        assert requested["reason"] == "wait for the current step"
        assert requested["request_id"]
        assert requested["requested_at"]

        state = manager._read_frame(current_running)
        committing = manager._transition(
            current_running,
            "committing",
            StateFrame.replace(state.carried(), process=state.members["process"], reason="outcome_published"),
        )
        committing_state = manager._read_frame(committing)
        paused = manager._transition(
            committing,
            "ready",
            StateFrame.replace(committing_state.carried(), process=committing_state.members["process"]),
        )

    assert paused.kind == "paused"
    final = workspace.read_state(paused)
    assert "pause_requested" not in final
    assert final["reason"] == "operator_pause_deferred"
    assert final["operator"] == "tester"
    assert final["operator_reason"] == "wait for the current step"
    assert final["request_id"] == requested["request_id"]
    assert final["process"] == committing_state.members["process"]


def test_deferred_pause_continue_resumes_the_next_activation(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="resume")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running, _ = _fake_running_attempt(
            workspace,
            payload,
            "project/resume",
            manager_id=manager.manager_id,
            pid=os.getpid(),
            lease_seconds=manager.lease_seconds,
        )
        _publish_pause(workspace, running)
        manager._handle_requests()
        current_running = workspace.find_marker_by_id(job_id)
        assert current_running is not None and current_running.kind == "running"
        state = manager._read_frame(current_running)
        committing = manager._transition(
            current_running,
            "committing",
            StateFrame.replace(state.carried(), process=state.members["process"], reason="outcome_published"),
        )
        state = manager._read_frame(committing)
        manager._advance(committing, workspace.load_job(committing), state, "next", state.carried())

        paused = workspace.find_marker_by_id(job_id)
        assert paused is not None and paused.kind == "paused"
        _publish_cancel(workspace, paused, action="continue", reason="resume it")
        manager._handle_requests()

    ready = workspace.find_marker_by_id(job_id)
    assert ready is not None and ready.kind == "ready"
    ready_state = workspace.read_state(ready)
    assert ready_state["step"] == "next"
    assert "pause_requested" not in ready_state


def test_deferred_pause_is_consumed_by_runner_pause_and_continue_resumes(
    tmp_path: Path,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="runner-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running, _ = _fake_running_attempt(
            workspace,
            payload,
            "project/runner-pause",
            manager_id=manager.manager_id,
            pid=os.getpid(),
            lease_seconds=manager.lease_seconds,
        )
        _publish_pause(workspace, running, reason="pause after this attempt")
        manager._handle_requests()
        current_running = workspace.find_marker_by_id(job_id)
        assert current_running is not None and current_running.kind == "running"
        state = manager._read_frame(current_running)
        attempt_control = state.attempt_control
        assert attempt_control is not None
        outcome_ready = workspace.payload_path(current_running.placement, current_running.job_key) / attempt_control
        outcome_ready = outcome_ready / "outcome.ready"
        outcome_ready.mkdir()
        (outcome_ready / "outcome.json").write_text(
            json.dumps(
                {
                    "format": "httk-workflow-outcome",
                    "format_version": 2,
                    "job_id": current_running.job_id,
                    "activation_id": state.activation_id,
                    "attempt_id": state.attempt_id,
                    "action": "pause",
                    "pause": {"reason": "runner requested a pause"},
                }
            ),
            encoding="utf-8",
        )
        committing = manager._transition(
            current_running,
            "committing",
            StateFrame.replace(
                state.carried(),
                process=state.members["process"],
                outcome_action="pause",
                reason="outcome_published",
            ),
        )
        manager._process_committing(committing)

        paused = workspace.find_marker_by_id(job_id)
        assert paused is not None and paused.kind == "paused"
        paused_state = workspace.read_state(paused)
        assert "pause_requested" not in paused_state
        assert paused_state["reason"] == "operator_pause_deferred"
        _publish_cancel(workspace, paused, action="continue", reason="resume it")
        assert manager._handle_requests() is True

    ready = workspace.find_marker_by_id(job_id)
    assert ready is not None and ready.kind == "ready"
    ready_state = workspace.read_state(ready)
    assert ready_state["step"] == "only"
    assert "pause_requested" not in ready_state


def test_null_pause_requested_member_is_stripped_before_claim(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="null-pause")
    workspace.submit(payload, "project/null-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        state = manager._read_frame(ready)
        ready = workspace.transition(
            manager.writer,
            ready,
            "ready",
            StateFrame.replace(state.carried(), pause_requested=None).as_mapping(),
        )
        assert manager._claim_and_launch(ready) is True
        clean_ready = workspace.find_marker_by_id(job_id)
        assert clean_ready is not None and clean_ready.kind == "ready"

    assert clean_ready.kind == "ready"
    assert "pause_requested" not in workspace.read_state(clean_ready)


def test_pause_request_for_paused_job_is_idempotent(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="paused-again")
    workspace.submit(payload, "project/paused-again")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        _publish_pause(workspace, ready)
        assert manager._handle_requests() is True
        paused = workspace.find_marker_by_id(job_id)
        assert paused is not None and paused.kind == "paused"
        before = (paused.generation, paused.record_ref)
        second_request = _publish_pause(workspace, paused, reason="still paused")
        assert manager._handle_requests() is True

    after = workspace.find_marker_by_id(job_id)
    assert after is not None and after.kind == "paused"
    assert (after.generation, after.record_ref) == before
    assert not second_request.exists()


def test_terminal_outcome_supersedes_deferred_pause(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="terminal")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running, _ = _fake_running_attempt(
            workspace,
            payload,
            "project/terminal",
            manager_id=manager.manager_id,
            pid=os.getpid(),
            lease_seconds=manager.lease_seconds,
        )
        _publish_pause(workspace, running)
        manager._handle_requests()
        current_running = workspace.find_marker_by_id(job_id)
        assert current_running is not None and current_running.kind == "running"
        terminal = manager._transition(
            current_running,
            "succeeded",
            StateFrame.replace(manager._read_frame(current_running).carried(), reason="succeeded"),
        )

    assert terminal.kind == "succeeded"
    assert "pause_requested" not in workspace.read_state(terminal)


def test_claimed_deferred_pause_releases_without_launching(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="claimed-pause")
    workspace.submit(payload, "project/claimed-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        claimed = manager._transition(
            ready,
            "claimed",
            StateFrame.replace(
                manager._read_frame(ready).carried(),
                manager_id=manager.manager_id,
                writer_id=manager.writer.writer_id,
                claim_id=str(uuid.uuid4()),
                attempt_id=str(uuid.uuid4()),
                attempt_control="attempts/00000000-0000-0000-0000-000000000001",
                lease_seconds=manager.lease_seconds,
                matched_pool="default",
                matched_capabilities=[],
                reason="claim",
            ),
        )
        _publish_pause(workspace, claimed)
        assert manager._handle_requests() is True
        assert manager._recover_abandoned_claims() is True
        paused = workspace.find_marker_by_id(job_id)
        assert paused is not None and paused.kind == "paused"
        assert not manager._running

    state = workspace.read_state(paused)
    assert state["attempt_ordinal"] == 0
    assert "pause_requested" not in state


def test_claim_path_pauses_a_ready_job_with_a_pending_request(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="ready-pending-pause")
    workspace.submit(payload, "project/ready-pending-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        # Simulate a ready frame left by an older manager that recorded the
        # additive flag without honoring it at the claim boundary.
        state = manager._read_frame(ready)
        ready = workspace.transition(
            manager.writer,
            ready,
            "ready",
            StateFrame.replace(
                state.carried(),
                pause_requested={
                    "operator": "tester",
                    "reason": "do not launch",
                    "request_id": str(uuid.uuid4()),
                    "requested_at": datetime.now(UTC).isoformat(),
                },
            ).as_mapping(),
        )
        assert manager._claim_and_launch(ready) is True

    paused = workspace.find_marker_by_id(job_id)
    assert paused is not None and paused.kind == "paused"
    assert "pause_requested" not in workspace.read_state(paused)


def test_pause_from_ready_remains_immediate(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="ready-pause")
    workspace.submit(payload, "project/ready-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        _publish_pause(workspace, ready, reason="hold before launch")
        assert manager._handle_requests() is True

    paused = workspace.find_marker_by_id(job_id)
    assert paused is not None and paused.kind == "paused"
    state = workspace.read_state(paused)
    assert state["reason"] == "operator_pause"
    assert "pause_requested" not in state


def test_second_deferred_pause_refreshes_the_sticky_request(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="refresh-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        running, _ = _fake_running_attempt(
            workspace,
            payload,
            "project/refresh-pause",
            manager_id=manager.manager_id,
            pid=os.getpid(),
            lease_seconds=manager.lease_seconds,
        )
        _publish_pause(workspace, running, reason="first reason")
        assert manager._handle_requests() is True
        current_running = workspace.find_marker_by_id(job_id)
        assert current_running is not None and current_running.kind == "running"
        first = workspace.read_state(current_running)["pause_requested"]["request_id"]
        _publish_pause(workspace, current_running, reason="second reason")
        assert manager._handle_requests() is True

    refreshed = workspace.find_marker_by_id(job_id)
    assert refreshed is not None and refreshed.kind == "running"
    pending = workspace.read_state(refreshed)["pause_requested"]
    assert pending["reason"] == "second reason"
    assert pending["request_id"] != first


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


def test_a_request_for_an_unserved_executor_is_left_for_another_manager(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="foreign", executor="foreign")
    marker = workspace.submit(payload, "project/foreign")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        published = _publish_cancel(workspace, marker)
        with caplog.at_level(logging.INFO, logger="httk.workflow"):
            manager.tick()
            manager.tick()

    # It stays where a manager serving that executor will find it, and this
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
    # A real deployment has a signing key from ``httk init``, so auto-sealing a
    # succeeded job stays quiet; a keyless workspace would warn on every job.
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(3):
        payload, _ = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag=f"clean-{index}")
        workspace.submit(payload, f"project/clean/{index}")

    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01, maximum_workers=2) as manager,
    ):
        manager.run_until_idle(timeout=60.0)

    assert all(marker.kind == "succeeded" for marker in workspace.scan_markers())
    messages = [record.getMessage() for record in caplog.records]
    assert not any("no longer owns a running marker" in message for message in messages), messages
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING], messages


def _reject_stdio_end_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make manager end-marker writes fail while leaving all other writes usable."""

    real_write = manager_module.os.write

    def write(fd: int, data: bytes) -> int:
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            target = ""
        if target.endswith("/stdio.out") and b" ended " in data:
            raise OSError("test end-marker write failure")
        return real_write(fd, data)

    monkeypatch.setattr(manager_module.os, "write", write)


def test_end_marker_failure_does_not_block_reap_or_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="end-write-reap")
    workspace.submit(payload, "project/end-write-reap")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _reject_stdio_end_writes(monkeypatch)
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_end_marker_failure_does_not_block_cancellation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="end-write-cancel")
    workspace.submit(payload, "project/end-write-cancel")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _reject_stdio_end_writes(monkeypatch)
        running = _drive_until(workspace, manager, job_id, {"running"})
        _publish_cancel(workspace, running)
        cancelled = _drive_until(workspace, manager, job_id, {"cancelled"})

    assert cancelled.kind == "cancelled"


def test_pipe_failure_closes_stdio_fd_and_fails_the_attempt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="pipe-failure")
    workspace.submit(payload, "project/pipe-failure")
    captured: list[int] = []
    real_open = manager_module.os.open

    def capture_open(path: str | os.PathLike[str], flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        fd = real_open(path, flags, mode, dir_fd=dir_fd)
        if Path(path).name == "stdio.out":
            captured.append(fd)
        return fd

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        monkeypatch.setattr(manager_module.os, "open", capture_open)

        def fail_pipe() -> tuple[int, int]:
            raise OSError("test pipe failure")

        monkeypatch.setattr(manager_module.os, "pipe", fail_pipe)
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    assert captured
    for fd in captured:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_launch_failure_marker_records_an_unexecutable_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="launch-failure")
    workspace.submit(payload, "project/launch-failure")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:

        def fail_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
            raise OSError("runner command unavailable")

        monkeypatch.setattr(manager_module.subprocess, "Popen", fail_popen)
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    root = workspace.payload_path(marker.placement, marker.job_key)
    lines = (root / "logs" / "stdio.out").read_text(encoding="utf-8").splitlines()
    attempt_id = next(path.name for path in (root / "attempts").iterdir() if path.is_dir())
    failure_lines = [line for line in lines if line.startswith("=== httk attempt") and "launch-failed" in line]
    assert len(failure_lines) == 1
    assert re.fullmatch(
        rf"=== httk attempt {attempt_id} ended \d{{4}}-\d{{2}}-\d{{2}}T[^ ]+ launch-failed runner command unavailable",
        failure_lines[0],
    )


def test_one_stdio_chronicle_has_fenced_markers_and_manager_attempt_events(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(
        tmp_path / "source",
        _PARTIAL_RETRY_RUNNER,
        tag="chronicle",
        retry_on=("process_failure",),
    )
    workspace.submit(payload, "project/chronicle")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    lines = (root / "logs" / "stdio.out").read_text(encoding="utf-8").splitlines()
    marker_lines = [line for line in lines if line.startswith("=== httk attempt")]
    assert len(marker_lines) == 4
    assert re.fullmatch(r"=== httk attempt [0-9a-f-]+ step only ordinal 1 started .+", marker_lines[0])
    assert re.fullmatch(r"=== httk attempt [0-9a-f-]+ ended .+ exit 7 outcome none", marker_lines[1])
    assert re.fullmatch(r"=== httk attempt [0-9a-f-]+ step only ordinal 2 started .+", marker_lines[2])
    assert re.fullmatch(r"=== httk attempt [0-9a-f-]+ ended .+ exit 0 outcome succeed", marker_lines[3])
    assert "partial" in lines
    assert not list(root.glob(f"attempts/*/{'stdout'}.log"))
    assert not list(root.glob(f"attempts/*/{'stderr'}.log"))

    events = [json.loads(line) for line in (root / "logs" / "runlog.jsonl").read_text().splitlines()]
    attempts = [event for event in events if event.get("kind") == "attempt"]
    assert len(attempts) == 2
    for event in attempts:
        assert event["attempt_id"]
        assert event["step"] == "only"
        assert event["runner_path"] == str(root / "files" / "runner")
        assert event["runner_sha256"]


def test_bash_post_publication_delay_is_reaped_before_cleanup(tmp_path: Path) -> None:
    runner = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.scheduling only
step_only() {
    httk_workflow_succeed
    sleep 2
}
httk_workflow_main
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, tag="bash-late-exit")
    workspace.submit(payload, "project/bash-late-exit")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    assert not (root / "attempts").exists() or not list((root / "attempts").iterdir())
    assert "no_outcome" not in (root / "logs" / "stdio.out").read_text(encoding="utf-8")


def test_python_exception_after_succeed_cannot_resurrect_attempt_control(tmp_path: Path) -> None:
    source_root = Path(__file__).parents[1] / "src"
    runner = f"""#!/usr/bin/env python3
import signal
import sys
import time
sys.path.insert(0, {str(source_root)!r})
from httk.workflow import Runner

run = Runner("tests.scheduling")

@run.step
def only(a):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    a.succeed()
    time.sleep(1.5)
    raise RuntimeError("late handler failure")

if __name__ == "__main__":
    raise SystemExit(run.main())
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, tag="python-late-exit")
    workspace.submit(payload, "project/python-late-exit")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    assert not (root / "attempts").exists() or not list((root / "attempts").iterdir())


def test_inherited_committing_success_retains_attempt_control_for_gc(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="inherited-commit")
    running, _attempt_id = _fake_running_attempt(
        workspace,
        payload,
        "project/inherited-commit",
        manager_id=_fake_manager(workspace, heartbeat_age=3600.0),
        pid=os.getpid(),
        lease_seconds=10.0,
    )
    state = StateFrame.from_mapping(workspace.read_state(running))
    control = workspace.payload_path(running.placement, running.job_key) / str(state.attempt_control)
    outcome = control / "outcome.ready"
    outcome.mkdir()
    (outcome / "outcome.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-outcome",
                "format_version": 2,
                "job_id": running.job_id,
                "activation_id": state.activation_id,
                "attempt_id": state.attempt_id,
                "action": "succeed",
            }
        ),
        encoding="utf-8",
    )
    committing = StateFrame.replace(
        state.carried(),
        manager_id=str(uuid.uuid4()),
        writer_id=str(uuid.uuid4()),
        outcome_action="succeed",
        child_digests={},
        child_labels={},
        reason="outcome_published",
    )
    with JournalWriter(workspace.control) as writer:
        workspace.transition(writer, running, "committing", committing.as_mapping())

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert control.is_dir()


def test_a_runlog_directory_is_evidence_failure_and_does_not_block_launch(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="runlog-directory")
    (payload / "logs" / "runlog.jsonl").mkdir(parents=True)
    workspace.submit(payload, "project/runlog-directory")

    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert any("attempt runlog event" in record.getMessage() for record in caplog.records)


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

    def counting_digest(path: Path, *, skip: Callable[[str], bool] | None = None) -> str:
        hashed.append(str(path))
        return tree_digest(path, skip=skip)

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


def test_marker_ownership_filters_scheduling_and_recovery(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="owned")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        workspace.submit(payload, "project/owned")
        manager.uid += 1
        assert manager._eligible_ready() == []
        assert manager._register_submissions() is False
        assert not manager._work_census().actionable
        assert workspace.find_marker_by_id(job_id).kind == "submitted"  # type: ignore[union-attr]

        manager.uid -= 1
        assert manager._register_submissions() is True
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        assert manager._eligible_ready() == [ready]
        assert manager._work_census().actionable

        manager._transition(
            ready,
            "claimed",
            StateFrame.replace(
                manager._read_frame(ready).carried(),
                manager_id=str(uuid.uuid4()),
                writer_id=manager.writer.writer_id,
                claim_id=str(uuid.uuid4()),
                attempt_id=str(uuid.uuid4()),
                attempt_control="attempts/00000000-0000-0000-0000-000000000002",
                lease_seconds=manager.lease_seconds,
                matched_pool="default",
                matched_capabilities=[],
                reason="claim",
            ),
        )
        manager.uid += 1
        assert manager._recover_abandoned_claims() is False
        assert workspace.find_marker_by_id(job_id).kind == "claimed"  # type: ignore[union-attr]


def test_request_owner_mismatch_is_retired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-owner")
    workspace.submit(payload, "project/request-owner")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        real_owner = manager._request_owner
        monkeypatch.setattr(
            manager,
            "_request_owner",
            lambda path: manager.uid + 1 if path == request_path else real_owner(path),
        )
        manager._handle_requests()

    retirement = workspace.control / "requests" / "retired" / f"{request_path.name}.retirement"
    assert retirement.is_file()
    assert "request author" in json.loads(retirement.read_text(encoding="utf-8"))["reason"]
    assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]


def test_transient_target_ownership_failure_retries_the_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-transient-owner")
    workspace.submit(payload, "project/request-transient-owner")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        real_lstat = Path.lstat
        failed = False

        def flaky_lstat(path: Path):
            nonlocal failed
            if path == marker.path and not failed:
                failed = True
                raise OSError(errno.EIO, "transient filesystem failure")
            return real_lstat(path)

        monkeypatch.setattr(Path, "lstat", flaky_lstat)
        assert manager._handle_requests() is False
        assert failed
        assert request_path.is_file()
        assert request_path.name not in manager._deferred_requests
        quarantine = workspace.control / "quarantine"
        assert not quarantine.exists() or not list(quarantine.iterdir())
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]

        assert manager._handle_requests() is True
        assert not request_path.exists()
        assert workspace.find_marker_by_id(job_id).kind == "cancelled"  # type: ignore[union-attr]


def test_transient_request_identity_stat_retries_without_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-read-transient")
    workspace.submit(payload, "project/request-read-transient")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        real_lstat = Path.lstat
        failed = False

        def flaky_lstat(path: Path):
            nonlocal failed
            if path == request_path and not failed:
                failed = True
                raise OSError(errno.EIO, "transient request identity failure")
            return real_lstat(path)

        monkeypatch.setattr(Path, "lstat", flaky_lstat)
        assert manager._handle_requests() is False
        assert failed
        assert request_path.is_file()
        quarantine = workspace.control / "quarantine"
        assert not quarantine.exists() or not list(quarantine.iterdir())
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]

        assert manager._handle_requests() is True
        assert not request_path.exists()
        assert workspace.find_marker_by_id(job_id).kind == "cancelled"  # type: ignore[union-attr]


def test_transient_claimed_identity_stat_returns_request_to_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-claimed-transient")
    workspace.submit(payload, "project/request-claimed-transient")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        claimed_path = workspace.control / "requests" / "claimed" / manager.manager_id / request_path.name
        real_lstat = Path.lstat
        failed = False

        def flaky_lstat(path: Path):
            nonlocal failed
            if path == claimed_path and not failed:
                failed = True
                raise OSError(errno.EIO, "transient claimed identity failure")
            return real_lstat(path)

        monkeypatch.setattr(Path, "lstat", flaky_lstat)
        assert manager._handle_requests() is True
        assert failed
        assert request_path.is_file()
        assert not claimed_path.exists()
        quarantine = workspace.control / "quarantine"
        assert not quarantine.exists() or not list(quarantine.iterdir())
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]

        assert manager._handle_requests() is True
        assert not request_path.exists()
        assert workspace.find_marker_by_id(job_id).kind == "cancelled"  # type: ignore[union-attr]


def test_failed_claim_rename_that_landed_is_processed_by_the_claimer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-claim-uncertain")
    workspace.submit(payload, "project/request-claim-uncertain")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        claimed_path = workspace.control / "requests" / "claimed" / manager.manager_id / request_path.name
        real_rename = os.rename
        reported = False

        def rename(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
            nonlocal reported
            real_rename(source, destination)
            if Path(source) == request_path and Path(destination) == claimed_path and not reported:
                reported = True
                raise OSError(errno.EIO, "rename reply was lost")

        monkeypatch.setattr(_manager_requests.os, "rename", rename)
        assert manager._handle_requests() is True
        assert reported
        assert not request_path.exists()
        assert not claimed_path.exists()
        assert workspace.find_marker_by_id(job_id).kind == "cancelled"  # type: ignore[union-attr]
        assert not list((workspace.control / "quarantine").iterdir())


def test_failed_claim_restoration_is_retried_by_the_live_manager(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-restore-uncertain")
    workspace.submit(payload, "project/request-restore-uncertain")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        claimed_path = workspace.control / "requests" / "claimed" / manager.manager_id / request_path.name
        real_lstat = Path.lstat
        real_rename = os.rename
        stat_failed = False
        restore_failed = False

        def flaky_lstat(path: Path):
            nonlocal stat_failed
            if path == claimed_path and not stat_failed:
                stat_failed = True
                raise OSError(errno.EIO, "claimed identity unavailable")
            return real_lstat(path)

        def rename(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
            nonlocal restore_failed
            if Path(source) == claimed_path and Path(destination) == request_path and not restore_failed:
                restore_failed = True
                raise OSError(errno.EIO, "restore reply was lost")
            real_rename(source, destination)

        monkeypatch.setattr(Path, "lstat", flaky_lstat)
        monkeypatch.setattr(_manager_requests.os, "rename", rename)
        assert manager._handle_requests() is True
        assert stat_failed and restore_failed
        assert not request_path.exists()
        assert claimed_path.is_file()
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]

        assert manager._handle_requests() is True
        assert not request_path.exists()
        assert not claimed_path.exists()
        assert workspace.find_marker_by_id(job_id).kind == "cancelled"  # type: ignore[union-attr]
        assert not list((workspace.control / "quarantine").iterdir())


def test_apply_io_failure_leaves_request_recoverable_for_next_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-apply-uncertain")
    workspace.submit(payload, "project/request-apply-uncertain")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        claimed_path = workspace.control / "requests" / "claimed" / manager.manager_id / request_path.name
        real_apply = manager._apply_request
        failed = False

        def apply(request):
            nonlocal failed
            if not failed:
                failed = True
                raise OSError(errno.EIO, "state write reply was lost")
            return real_apply(request)

        monkeypatch.setattr(manager, "_apply_request", apply)
        assert manager._handle_requests() is True
        assert failed
        assert not request_path.exists()
        assert claimed_path.is_file()
        assert not list((workspace.control / "quarantine").iterdir())
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]

        assert manager._handle_requests() is True
        assert not request_path.exists()
        assert not claimed_path.exists()
        assert workspace.find_marker_by_id(job_id).kind == "cancelled"  # type: ignore[union-attr]


def test_malformed_expected_generation_is_quarantined(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-bad-generation")
    workspace.submit(payload, "project/request-bad-generation")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker, expected_generation={})
        assert manager._handle_requests() is True

    assert not request_path.exists()
    reports = list((workspace.control / "quarantine").glob("*/report.json"))
    assert len(reports) == 1
    assert "expected_generation" in json.loads(reports[0].read_text(encoding="utf-8"))["reason"]


def test_owner_request_waits_for_owner_manager(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="request-deferred")
    workspace.submit(payload, "project/request-deferred")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        request_path = _publish_cancel(workspace, marker)
        manager.uid += 1
        assert manager._handle_requests() is False
        assert request_path.is_file()
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]


def _attempt_contexts(workspace: Workspace, job_id: str) -> list[dict[str, object]]:
    """Read the claimed-attempt snapshots retained in the job journal."""

    from httk.workflow.introspection._reading import job_frames

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return [
        {"step": frame.get("step"), "resources": frame.get("resources", {})}
        for frame in job_frames(workspace, marker)
        if frame.get("kind") == "claimed"
    ]


def test_resource_capacity_blocks_never_fitting_jobs_and_census_reports_them(tmp_path: Path) -> None:
    for capacity in ({"procs": 1}, {}):
        workspace = Workspace.initialize(tmp_path / ("workspace-" + ("small" if capacity else "none")))
        payload, job_id = _payload(
            tmp_path / ("source-" + ("small" if capacity else "none")),
            _SUCCEED_RUNNER,
            tag="too-large",
            resources={"procs": 2},
        )
        workspace.submit(payload, "project/too-large")
        with TaskManager(workspace, resources=capacity, heartbeat_interval=0.01) as manager:
            manager.run_until_idle(timeout=5.0)
            census = manager._work_census()
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]
        assert census.ready_blocked["resources"] == {"procs": 1}


def test_exhausted_procs_skip_the_ready_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="exhausted")
    workspace.submit(payload, "project/exhausted")
    with TaskManager(workspace, resources={"procs": 1}, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        calls = 0

        def record_scan() -> list[tuple[Marker, dict[str, int]]]:
            nonlocal calls
            calls += 1
            return []

        monkeypatch.setattr(manager, "_eligible_ready_with_requirements", record_scan)
        monkeypatch.setattr(manager, "_available_resources", lambda: {"procs": 0})
        manager.tick()

    assert calls == 0
    assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]


def test_resource_packing_limits_concurrent_attempts(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    jobs: list[tuple[Path, str]] = []
    sleeping = _SUCCEED_RUNNER.replace("context = json.loads", "import time\ntime.sleep(0.25)\n\ncontext = json.loads")
    for index in range(3):
        payload, job_id = _payload(
            tmp_path / "source",
            sleeping,
            tag=f"packed-{index}",
            resources={"procs": 2, "mem": 4},
        )
        workspace.submit(payload, f"project/packed/{index}")
        jobs.append((payload, job_id))
    with TaskManager(
        workspace,
        resources={"procs": 4, "mem": 8},
        maximum_workers=4,
        heartbeat_interval=0.01,
    ) as manager:
        maximum_running = 0
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            manager.tick()
            maximum_running = max(maximum_running, len(manager._running))
            if all(workspace.find_marker_by_id(job_id).kind == "succeeded" for _, job_id in jobs):  # type: ignore[union-attr]
                break
            time.sleep(0.01)
        assert maximum_running <= 2
    assert all(workspace.find_marker_by_id(job_id).kind == "succeeded" for _, job_id in jobs)  # type: ignore[union-attr]


def test_undeclared_resources_use_fair_share(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    jobs: list[tuple[Path, str]] = []
    sleeping = _SUCCEED_RUNNER.replace("context = json.loads", "import time\ntime.sleep(0.2)\n\ncontext = json.loads")
    for index in range(3):
        payload, job_id = _payload(tmp_path / "source", sleeping, tag=f"fair-{index}")
        workspace.submit(payload, f"project/fair/{index}")
        jobs.append((payload, job_id))
    with TaskManager(workspace, resources={"procs": 4}, maximum_workers=2, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    contexts = [context for _, job_id in jobs for context in _attempt_contexts(workspace, job_id)]
    assert len(contexts) == 3
    assert all(context["resources"] == {"procs": 2} for context in contexts)


def test_dynamic_resources_apply_to_advance_and_clear_on_next_advance(tmp_path: Path) -> None:
    runner = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {"format": "httk-workflow-outcome", "format_version": 2,
        "job_id": context["job_id"], "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"]}
if context["step"] == "first":
    outcome = {**base, "action": "advance", "next_step": "second", "resources": {"procs": 3}}
elif context["step"] == "second":
    outcome = {**base, "action": "advance", "next_step": "third"}
else:
    outcome = {**base, "action": "succeed"}
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, tag="dynamic", initial_step="first")
    workspace.submit(payload, "project/dynamic")
    with TaskManager(
        workspace, resources={"procs": 4, "mem": 8}, maximum_workers=2, heartbeat_interval=0.01
    ) as manager:
        manager.run_until_idle(timeout=30.0)
        contexts = {context["step"]: context["resources"] for context in _attempt_contexts(workspace, job_id)}
    assert contexts == {
        "first": {"procs": 2, "mem": 4},
        "second": {"procs": 3, "mem": 4},
        "third": {"procs": 2, "mem": 4},
    }


def test_wait_resources_apply_to_post_join_activation(tmp_path: Path) -> None:
    runner = _SPAWNING_RUNNER.format(child=_SUCCEED_RUNNER).replace(
        '"next_step": "gather",', '"next_step": "gather",\n        "resources": {"procs": 3},'
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, tag="wait-resources")
    workspace.submit(payload, "project/wait-resources")
    with TaskManager(
        workspace, resources={"procs": 4, "mem": 8}, maximum_workers=2, heartbeat_interval=0.01
    ) as manager:
        manager.run_until_idle(timeout=30.0)
    contexts = _attempt_contexts(workspace, job_id)
    assert {context["step"]: context["resources"] for context in contexts} == {
        "only": {"procs": 2, "mem": 4},
        "gather": {"procs": 3, "mem": 4},
    }


def test_retry_keeps_dynamic_resource_requirement(tmp_path: Path) -> None:
    runner = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {"format": "httk-workflow-outcome", "format_version": 2,
        "job_id": context["job_id"], "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"]}
if context["step"] == "first":
    outcome = {**base, "action": "advance", "next_step": "retry", "resources": {"procs": 3}}
elif context["attempt_ordinal"] == 1:
    outcome = {**base, "action": "fail", "failure": {"code": "temporary", "message": "try again", "retryable": True}}
else:
    outcome = {**base, "action": "succeed"}
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner, tag="retry-resources", initial_step="first")
    workspace.submit(payload, "project/retry-resources")
    with TaskManager(workspace, resources={"procs": 4}, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
        contexts = [context for context in _attempt_contexts(workspace, job_id) if context["step"] == "retry"]
    assert [context["resources"] for context in contexts] == [{"procs": 3}, {"procs": 3}]
