"""Manager scheduling hardening: bounded passes, attempt boundaries, cancels, requests and resources.

The shared helpers here install a one-runner workflow package and submit v3
jobs of it, so the timing, binding and drain suites reuse them.
"""

import errno
import json
import logging
import os
import re
import signal
import subprocess
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

import httk.workflow.manager as manager_module
from conftest import configure_identity
from httk.workflow import TaskManager, Workspace, _kernel, _manager_scheduling, _requests, _store
from httk.workflow._logging import reset_logging
from httk.workflow._state import StateDoc
from httk.workflow.manager import RunningAttempt
from v3_helpers import cli_owner, find, job_mapping, state_of, submit_mapping
from v3_helpers import workspace as initialize_workspace

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("heartbeat-timing")]

#: The steps every test workflow declares; jobs start at ``only`` unless they say otherwise.
STEPS = ("only", "first", "second", "third", "retry", "next", "gather", "child")

#: The head of every test runner: it reads its context, records the attempt (step, ordinal, resources and
#: deadline) in ``attempts.jsonl`` of its working directory, and defines ``publish(action, **members)``.
_HEADER = '''#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
import uuid
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
with open(Path(os.environ["HTTK_WORKFLOW_WORKDIR"]) / "attempts.jsonl", "a") as _stream:
    _stream.write(json.dumps({key: context.get(key) for key in ("step", "attempt_ordinal", "resources")}) + "\\n")


def publish(action, **members):
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    (temporary / "outcome.json").write_text(json.dumps({
        "format": "httk-workflow-outcome",
        "format_version": 2,
        "job_id": context["job_id"],
        "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"],
        "action": action,
        **members,
    }))
    os.rename(temporary, control / "outcome.ready")
'''

_SUCCEED = 'publish("succeed")\n'
_SLEEPING = "time.sleep(600)\n"
_PUBLISH_THEN_HANG = 'publish("succeed")\ntime.sleep(600)\n'
#: Waits for a ``go`` file in its working directory, then advances to ``next``; ``next`` succeeds.
_WAIT_THEN_ADVANCE = """
if context["step"] == "only":
    Path("waiting").touch()
    while not Path("go").exists():
        time.sleep(0.02)
    publish("advance", next_step="next")
else:
    publish("succeed")
"""
_PARTIAL_RETRY = """
print("partial", end="", flush=True)
if context["attempt_ordinal"] == 1:
    sys.exit(7)
publish("succeed")
"""
#: The parent (step ``only``) spawns one child (label ``c``) that runs step ``child`` and waits on it; ``gather``
#: records the placements of its children in ``seen.json`` and succeeds. A body may first set ``CHILD_PLACEMENT``
#: (default: the parent's placement plus ``/children``) and ``WAIT_MEMBERS`` (further members of the wait outcome).
_SPAWNING = """
if context["step"] == "only":
    job = json.loads((Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
    temporary = control / "outcome.tmp.spawn"
    child_id, spawn_id = str(uuid.uuid4()), str(uuid.uuid4())
    key, placement = "child--" + child_id, globals().get("CHILD_PLACEMENT", job["placement"] + "/children")
    child = dict(job, id=child_id, tag="child", name="child", placement=placement, initial_step="child")
    child["parent"] = {
        "workspace_id": context["workspace_id"], "job_id": job["id"], "job_key": context["job_key"],
        "placement": job["placement"], "activation_id": context["activation_id"], "spawn_id": spawn_id,
    }
    (temporary / "children" / "jobs" / key).mkdir(parents=True)
    (temporary / "children" / "jobs" / key / "job.json").write_text(json.dumps(child))
    entry = {"job_key": key, "label": "c", "placement": placement, "spawn_id": spawn_id}
    spawn = {"format": "httk-workflow-spawn", "format_version": 2, "children": [entry]}
    (temporary / "children" / "spawn.json").write_text(json.dumps(spawn))
    reference = {"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": key,
                 "placement_hint": placement}
    outcome = {
        "format": "httk-workflow-outcome", "format_version": 2, "job_id": context["job_id"],
        "activation_id": context["activation_id"], "attempt_id": context["attempt_id"],
        "action": "wait", "next_step": "gather", "join": {"children": [reference], "condition": "all_terminal"},
        **globals().get("WAIT_MEMBERS", {}),
    }
    (temporary / "outcome.json").write_text(json.dumps(outcome))
    os.rename(temporary, control / "outcome.ready")
else:
    if context["step"] == "gather":
        Path("seen.json").write_text(json.dumps([child["placement"] for child in context["children"]]))
    publish("succeed")
"""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep records propagating to the capture handlers pytest installs."""

    reset_logging()
    yield
    reset_logging()


def _install(
    workspace: Workspace, package: Path, body: str, *, name: str = "tests.scheduling", workflow: str = ""
) -> _store.Installed:
    """Install a workflow package at *package* whose runner is :data:`_HEADER` followed by *body*.

    *workflow* holds further ``[workflow]`` members, as TOML lines.
    """

    package.mkdir(parents=True)
    manifest = (
        f'[workflow]\nname = "{name}"\n{workflow}\n'
        f'[workflow.runner]\nsteps = {json.dumps(list(STEPS))}\ninitial_step = "only"\n'
    )
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (package / "run").write_text(_HEADER + body, encoding="utf-8")
    (package / "run").chmod(0o755)
    with cli_owner(workspace) as owner:
        return _store.install(workspace, owner, package)


def _submit(
    workspace: Workspace,
    installed: _store.Installed,
    *,
    tag: str,
    placement: str | None = None,
    pool: str = "default",
    initial_step: str = "only",
    retry_on: tuple[str, ...] = (),
    resources: Mapping[str, int] | None = None,
    step_resources: Mapping[str, Mapping[str, int]] | None = None,
) -> str:
    """Submit one job of *installed* at ``project/<tag>`` (or *placement*) and return its id."""

    mapping = job_mapping(
        installed,
        {},
        tag=tag,
        placement=f"project/{tag}" if placement is None else placement,
        pool=pool,
        initial_step=initial_step,
        retry_policy={
            "maximum_attempts_per_activation": 4,
            "maximum_total_attempts": 8,
            "maximum_activations": 4,
            "retry_on": list(retry_on),
        },
        resources=resources,
        step_resources=step_resources,
    )
    return submit_mapping(workspace, mapping).job_id


def _job(workspace: Workspace, root: Path, body: str, *, tag: str, **options: Any) -> str:
    """Install *body* as the ``tests.scheduling`` runner and submit one job of it; return the job id."""

    return _submit(workspace, _install(workspace, root / tag, body), tag=tag, **options)


def _state(workspace: Workspace, job_id: str) -> str:
    """The state a job is in now (``owned`` while a manager holds it)."""

    return find(workspace, job_id).state


def _doc(workspace: Workspace, job_id: str) -> StateDoc:
    return state_of(find(workspace, job_id))


def _failure(workspace: Workspace, job_id: str) -> Mapping[str, Any]:
    failure = _doc(workspace, job_id).failure
    assert failure is not None
    return failure


def _attempt_records(workspace: Workspace, job_id: str) -> list[dict[str, Any]]:
    """The attempts the runner recorded: step, ordinal and the resources of each attempt's context."""

    path = find(workspace, job_id).path / "run" / "attempts.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _history(workspace: Workspace, job_id: str) -> list[Mapping[str, Any]]:
    return list(_doc(workspace, job_id).history_tail)


def _drive_until(workspace: Workspace, manager: TaskManager, job_id: str, states: set[str]) -> _kernel.JobRef:
    """Tick until one job reaches any of *states*, returning its reference."""

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        manager.tick()
        ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
        if ref is not None and ref.state in states:
            return ref
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {sorted(states)}")


def _drive_until_running(manager: TaskManager) -> RunningAttempt:
    """Tick until the manager runs an attempt, returning it."""

    deadline = time.monotonic() + 30.0
    while not manager._running:
        assert time.monotonic() < deadline, "no attempt was launched"
        manager.tick()
        time.sleep(0.02)
    return next(iter(manager._running.values()))


def _wait_for(path_of: Any, seconds: float = 30.0) -> None:
    deadline = time.monotonic() + seconds
    while not path_of().exists():
        assert time.monotonic() < deadline, "the runner never got there"
        time.sleep(0.02)


def _post(workspace: Workspace, job_id: str, action: str, *, reason: str = "scheduling test", **extra: Any) -> Path:
    """Post one operator request for a job of this workspace."""

    ref = find(workspace, job_id)
    placement = ref.placement if ref.placement is not None else str(_doc_placement(ref))
    return _requests.post(
        workspace, action=action, job_id=job_id, placement=placement, operator="tester", reason=reason, **extra
    )


def _doc_placement(ref: _kernel.JobRef) -> str:
    return str(json.loads((ref.path / "job.json").read_text(encoding="utf-8"))["placement"])


def _requests_left(workspace: Workspace) -> list[str]:
    return sorted(path.name for path in (workspace.control / "requests").iterdir())


def _crash(workspace: Workspace, monkeypatch: pytest.MonkeyPatch, manager: TaskManager, name: str) -> None:
    """Make ``TaskManager.<name>`` raise OwnerLost once, tick *manager* until it fail-stops, then recover it."""

    real = getattr(TaskManager, name)
    crashed: list[bool] = []

    def crash(*arguments: object, **options: object) -> object:
        if not crashed:
            crashed.append(True)
            raise _kernel.OwnerLost("simulated crash")
        return real(*arguments, **options)

    monkeypatch.setattr(TaskManager, name, crash)
    with pytest.raises(_kernel.OwnerLost):
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            manager.tick()
            time.sleep(0.02)
    manager.close()
    monkeypatch.setattr(TaskManager, name, real)
    _kernel.attest_dead(workspace, manager.manager_id, by="operator", evidence=[], reason="test crash")
    with cli_owner(workspace) as owner:
        _kernel.recover(workspace, owner, manager.manager_id)


# ---------------------------------------------------------------------------
# 1. Launch records and hostile attempt containers
# ---------------------------------------------------------------------------


def test_running_frame_records_launcher_identity_without_attempt_metadata_file(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    _job(workspace, tmp_path, _SLEEPING, tag="running-process")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        local = _drive_until_running(manager)
        launch = manager.manager_directory / "launches" / f"{local.attempt_id}.0" / "process.json"
        process = json.loads(launch.read_text(encoding="utf-8"))
        assert process["pgid"] == local.process.pid and process["attempt_id"] == local.attempt_id
        assert state_of(local.owned.ref).phase == {"kind": "running", "attempt_id": local.attempt_id}
        assert not list(local.control.iterdir())
        manager._signal_running_attempts(signal.SIGKILL)


def test_a_malformed_outcome_fails_the_job_with_protocol_error(tmp_path: Path) -> None:
    body = """
(control / "outcome.tmp.test").mkdir()
(control / "outcome.tmp.test" / "outcome.json").write_text("{}")
os.rename(control / "outcome.tmp.test", control / "outcome.ready")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="malformed-outcome")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    assert _state(workspace, job_id) == "failed"
    assert _failure(workspace, job_id)["code"] == "protocol_error"


def test_a_symlinked_attempts_directory_fails_launch_without_gc_escape(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="symlinked-attempts")
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "must-survive"
    sentinel.write_text("outside\n", encoding="utf-8")
    (find(workspace, job_id).path / "attempts").symlink_to(outside, target_is_directory=True)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=15.0)

    assert _state(workspace, job_id) == "failed"
    assert _failure(workspace, job_id)["code"] == "protocol_error"
    assert sentinel.read_text(encoding="utf-8") == "outside\n"
    assert sorted(path.name for path in outside.iterdir()) == ["must-survive"]
    assert (find(workspace, job_id).path / "attempts").is_symlink()


def test_a_restarted_manager_rejects_symlinked_attempt_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="recovery-symlink")
    # The first manager dies once its attempt has published and exited, before it commits.
    _crash(workspace, monkeypatch, TaskManager(workspace, heartbeat_interval=0.01), "_finish_attempt")
    recovered = find(workspace, job_id)
    assert recovered.state == "ready" and state_of(recovered).phase["kind"] == "running"
    attempts = recovered.path / "attempts"
    attempts.rename(tmp_path / "saved-attempts")
    outside = tmp_path / "outside-recovery"
    outside.mkdir()
    sentinel = outside / "must-survive"
    sentinel.write_text("outside\n", encoding="utf-8")
    attempts.symlink_to(outside, target_is_directory=True)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=15.0)

    # The published outcome is behind the symlink, so it is never read: the attempt is lost with its owner.
    assert _state(workspace, job_id) == "failed"
    assert _failure(workspace, job_id)["code"] == "owner_lost"
    assert sentinel.read_text(encoding="utf-8") == "outside\n"
    assert sorted(path.name for path in outside.iterdir()) == ["must-survive"]


# ---------------------------------------------------------------------------
# 2. Bounded passes
# ---------------------------------------------------------------------------


def test_a_bounded_pass_resumes_where_it_stopped_instead_of_starving_the_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    installed = _install(workspace, tmp_path / "package", _SUCCEED)
    # The head of the queue is work for another pool; only the tail is this manager's.
    submitted = {
        index: _submit(
            workspace,
            installed,
            tag=f"bounded{index}",
            placement=f"project/bounded/{index}",
            pool="elsewhere" if index < 4 else "default",
        )
        for index in range(6)
    }
    read: list[str] = []
    real_read = _manager_scheduling.read_ready

    def recording(manager: TaskManager, ref: _kernel.JobRef) -> object:
        read.append(ref.job_key.split("--")[0])
        return real_read(manager, ref)

    monkeypatch.setattr(_manager_scheduling, "read_ready", recording)
    with TaskManager(workspace, heartbeat_interval=0.01, discovery_budget=2, maximum_workers=1) as manager:
        windows: list[list[str]] = []
        for _ in range(3):
            read.clear()
            manager.tick()
            windows.append(list(read))
        assert windows == [["bounded0", "bounded1"], ["bounded2", "bounded3"], ["bounded4", "bounded5"]]
        # Everything this manager serves still finishes: the bound defers work, it never drops it.
        manager.run_until_idle(timeout=60.0)

    assert [_state(workspace, submitted[index]) for index in range(6)] == [*["ready"] * 4, *["succeeded"] * 2]


# ---------------------------------------------------------------------------
# 3. Cancellation
# ---------------------------------------------------------------------------


def test_cancelling_a_running_job_stops_it_then_proves_its_process_is_gone(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="cancelled")

    with (
        caplog.at_level(logging.INFO, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=2.0) as manager,
    ):
        local = _drive_until_running(manager)
        pid = local.process.pid
        request = _post(workspace, job_id, "cancel")
        _drive_until(workspace, manager, job_id, {"cancelled"})

    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert not _requests_left(workspace)
    ref = find(workspace, job_id)
    log = [json.loads(line) for line in (ref.path / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()]
    (applied,) = [entry for entry in log if entry["event"] == "request_applied"]
    assert applied["detail"] == {"request_id": request.name.split(".")[1], "action": "cancel"}
    assert state_of(ref).applied_requests == (applied["detail"]["request_id"],)
    assert any("cancelling attempt" in record.getMessage() for record in caplog.records)


@pytest.mark.xfail(
    strict=True,
    reason="a cancel the dying owner was applying is dropped as 'terminal' once recovery fails the attempt "
    "with owner_lost: reconcile commits the lost attempt before it applies the pending requests",
)
def test_a_manager_that_dies_mid_cancellation_leaves_it_to_the_next_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="crashed")

    first = TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5)
    local = _drive_until_running(first)
    _post(workspace, job_id, "cancel")
    # The manager dies as it starts to cancel the attempt; its fail-stop kills the attempt.
    _crash(workspace, monkeypatch, first, "_cancel_attempt")
    local.process.wait(timeout=30)

    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as second:
        second.run_until_idle(timeout=30.0)

    assert _state(workspace, job_id) == "cancelled"
    assert not _requests_left(workspace)


def test_cancelling_a_job_with_no_live_attempt_is_terminal_at_once(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="quiet", pool="elsewhere")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        assert _state(workspace, job_id) == "ready"
        _post(workspace, job_id, "cancel")
        manager.tick()

    assert _state(workspace, job_id) == "cancelled"
    assert _doc(workspace, job_id).attempt is None
    assert not _requests_left(workspace)


# ---------------------------------------------------------------------------
# 4. Pause requests at attempt boundaries
# ---------------------------------------------------------------------------


def _start_waiting(workspace: Workspace, manager: TaskManager, job_id: str) -> RunningAttempt:
    """Launch the job's ``only`` attempt and wait until its runner waits for ``go``."""

    local = _drive_until_running(manager)
    _wait_for(lambda: find(workspace, job_id).path / "run" / "waiting")
    return local


def _let_go(workspace: Workspace, manager: TaskManager, job_id: str) -> None:
    """Let the waiting runner publish, and tick until its attempt is committed."""

    (find(workspace, job_id).path / "run" / "go").touch()
    deadline = time.monotonic() + 30.0
    while manager._running:
        assert time.monotonic() < deadline, "the attempt never ended"
        manager.tick()
        time.sleep(0.02)


def test_deferred_pause_is_recorded_and_consumed_at_attempt_boundary(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _WAIT_THEN_ADVANCE, tag="deferred")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _start_waiting(workspace, manager, job_id)
        request = _post(workspace, job_id, "pause", reason="wait for the current step")
        manager.tick()
        # The pause waits for the attempt's boundary: the attempt keeps running and the request stays posted.
        assert _state(workspace, job_id) == "owned" and manager._running and request.is_file()
        _let_go(workspace, manager, job_id)

    assert _state(workspace, job_id) == "paused"
    assert not request.exists()
    doc = _doc(workspace, job_id)
    assert doc.activation is not None and doc.activation["step"] == "next"
    applied = [entry for entry in doc.history_tail if entry.get("event") == "request_applied"]
    assert len(applied) == 1 and applied[0]["action"] == "pause"
    assert applied[0]["operator"] == "tester" and applied[0]["reason"] == "wait for the current step"
    assert applied[0]["request_id"] == request.name.split(".")[1]


def test_deferred_pause_continue_resumes_the_next_activation(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _WAIT_THEN_ADVANCE, tag="resume")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _start_waiting(workspace, manager, job_id)
        _post(workspace, job_id, "pause")
        manager.tick()
        _let_go(workspace, manager, job_id)
        assert _state(workspace, job_id) == "paused"
        _post(workspace, job_id, "continue", reason="resume it")
        manager.run_until_idle(timeout=30.0)

    assert _state(workspace, job_id) == "succeeded"
    assert [record["step"] for record in _attempt_records(workspace, job_id)] == ["only", "next"]


def test_deferred_pause_is_consumed_by_runner_pause_and_continue_resumes(tmp_path: Path) -> None:
    body = """
if context["attempt_ordinal"] == 1:
    Path("waiting").touch()
    while not Path("go").exists():
        time.sleep(0.02)
    publish("pause", pause={"reason": "runner requested a pause"})
else:
    publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="runner-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _start_waiting(workspace, manager, job_id)
        request = _post(workspace, job_id, "pause", reason="pause after this attempt")
        manager.tick()
        _let_go(workspace, manager, job_id)
        assert _state(workspace, job_id) == "paused"
        # The runner's own pause took effect; the operator's pause found the job paused and was used up.
        assert not request.exists()
        events = [entry.get("event") for entry in _history(workspace, job_id)]
        assert "paused" in events and "request_dropped" in events
        _post(workspace, job_id, "continue", reason="resume it")
        manager.run_until_idle(timeout=30.0)

    assert _state(workspace, job_id) == "succeeded"
    assert [(record["step"], record["attempt_ordinal"]) for record in _attempt_records(workspace, job_id)] == [
        ("only", 1),
        ("only", 2),
    ]


def test_pause_request_for_paused_job_is_idempotent(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="paused-again", pool="elsewhere")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _post(workspace, job_id, "pause")
        assert manager.tick() is True
        assert _state(workspace, job_id) == "paused"
        before = _doc(workspace, job_id)
        second_request = _post(workspace, job_id, "pause", reason="still paused")
        assert manager.tick() is True

    after = _doc(workspace, job_id)
    assert _state(workspace, job_id) == "paused"
    assert (after.activation, after.attempt, after.counters) == (before.activation, before.attempt, before.counters)
    assert after.history_tail[-2]["event"] == "request_dropped"
    assert not second_request.exists()


def test_terminal_outcome_supersedes_deferred_pause(tmp_path: Path) -> None:
    body = """
Path("waiting").touch()
while not Path("go").exists():
    time.sleep(0.02)
publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="terminal")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _start_waiting(workspace, manager, job_id)
        request = _post(workspace, job_id, "pause")
        manager.tick()
        _let_go(workspace, manager, job_id)

    assert _state(workspace, job_id) == "succeeded"
    assert not request.exists()
    assert "request_dropped" in [entry.get("event") for entry in _history(workspace, job_id)]


def test_claim_path_pauses_a_ready_job_with_a_pending_request(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="ready-pending-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _post(workspace, job_id, "pause", reason="do not launch")
        # The claim itself honors the pending request: the job is paused, never launched.
        assert manager._claim_and_launch(find(workspace, job_id)) is True
        assert not manager._running

    assert _state(workspace, job_id) == "paused"
    assert _doc(workspace, job_id).attempt is None
    assert not _requests_left(workspace)


def test_pause_from_ready_remains_immediate(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="ready-pause")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _post(workspace, job_id, "pause", reason="hold before launch")
        assert manager.tick() is True
        assert not manager._running

    assert _state(workspace, job_id) == "paused"
    (applied,) = [entry for entry in _history(workspace, job_id) if entry.get("event") == "request_applied"]
    assert applied["action"] == "pause" and applied["reason"] == "hold before launch" and applied["to"] == "paused"


# ---------------------------------------------------------------------------
# 5. Ownership bookkeeping and the stdio chronicle
# ---------------------------------------------------------------------------


def test_a_normal_run_never_reports_an_orphaned_attempt(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A real deployment has a signing key from ``httk init``, so sealing a
    # succeeded job stays quiet; a keyless workspace would warn on every job.
    configure_identity()
    workspace = initialize_workspace(tmp_path / "workspace")
    installed = _install(workspace, tmp_path / "package", _SUCCEED)
    jobs = [_submit(workspace, installed, tag=f"clean{index}") for index in range(3)]

    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow"),
        TaskManager(workspace, heartbeat_interval=0.01, maximum_workers=2) as manager,
    ):
        manager.run_until_idle(timeout=60.0)

    assert [_state(workspace, job_id) for job_id in jobs] == ["succeeded"] * 3
    messages = [record.getMessage() for record in caplog.records]
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
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="end-write-reap")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _reject_stdio_end_writes(monkeypatch)
        manager.run_until_idle(timeout=60.0)

    assert _state(workspace, job_id) == "succeeded"


def test_end_marker_failure_does_not_block_cancellation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="end-write-cancel")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        _reject_stdio_end_writes(monkeypatch)
        _drive_until_running(manager)
        _post(workspace, job_id, "cancel")
        cancelled = _drive_until(workspace, manager, job_id, {"cancelled"})

    assert cancelled.state == "cancelled"


def test_pipe_failure_closes_stdio_fd_and_fails_the_attempt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="pipe-failure")
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

    assert _state(workspace, job_id) == "failed"
    assert _failure(workspace, job_id)["code"] == "process_failure"
    assert captured
    for fd in captured:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_launch_failure_marker_records_an_unexecutable_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="launch-failure")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:

        def fail_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
            raise OSError("runner command unavailable")

        monkeypatch.setattr(manager_module.subprocess, "Popen", fail_popen)
        manager.run_until_idle(timeout=60.0)

    assert _state(workspace, job_id) == "failed"
    root = find(workspace, job_id).path
    lines = (root / "logs" / "stdio.out").read_text(encoding="utf-8").splitlines()
    attempt_id = next(path.name for path in (root / "attempts").iterdir() if path.is_dir())
    failure_lines = [line for line in lines if line.startswith("=== httk attempt") and "launch-failed" in line]
    assert len(failure_lines) == 1
    assert re.fullmatch(
        rf"=== httk attempt {attempt_id} ended \d{{4}}-\d{{2}}-\d{{2}}T[^ ]+ launch-failed runner command unavailable",
        failure_lines[0],
    )


def test_one_stdio_chronicle_has_fenced_markers_and_manager_attempt_events(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    installed = _install(workspace, tmp_path / "package", _PARTIAL_RETRY)
    job_id = _submit(workspace, installed, tag="chronicle", retry_on=("process_failure",))

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    assert _state(workspace, job_id) == "succeeded"
    root = find(workspace, job_id).path
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
    started = [event for event in events if event["event"] == "attempt_started"]
    assert len(started) == 2
    for event in started:
        assert event["attempt_id"]
        assert event["step"] == "only"
        assert event["detail"]["command"] == [str(installed.package / "run")]


def test_bash_post_publication_delay_is_reaped_before_cleanup(tmp_path: Path) -> None:
    body = """
publish("succeed")
time.sleep(2)
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="late-exit")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    assert _state(workspace, job_id) == "succeeded"
    root = find(workspace, job_id).path
    assert not (root / "attempts").exists()
    assert "outcome none" not in (root / "logs" / "stdio.out").read_text(encoding="utf-8")


def test_python_exception_after_succeed_cannot_resurrect_attempt_control(tmp_path: Path) -> None:
    body = """
signal.signal(signal.SIGTERM, signal.SIG_IGN)
publish("succeed")
time.sleep(1.5)
(control / "late").mkdir()
raise RuntimeError("late handler failure")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="python-late-exit")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)

    assert _state(workspace, job_id) == "succeeded"
    assert not (find(workspace, job_id).path / "attempts").exists()


def test_marker_ownership_filters_scheduling_and_recovery(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SLEEPING, tag="owned")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.uid += 1
        assert _manager_scheduling.claim_pass(manager) is False
        census = manager._work_census()
        assert census.ready_claimable == 0 and not census.actionable
        assert _state(workspace, job_id) == "ready"

        manager.uid -= 1
        assert manager._work_census().actionable
        assert _manager_scheduling.claim_pass(manager) is True
        assert _state(workspace, job_id) == "owned" and manager._running
        manager._signal_running_attempts(signal.SIGKILL)


# ---------------------------------------------------------------------------
# 6. Requests hygiene
# ---------------------------------------------------------------------------


def test_transient_target_ownership_failure_retries_the_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="request-transient-owner", pool="elsewhere")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        request_path = _post(workspace, job_id, "cancel")
        target = find(workspace, job_id).path
        real_lstat = manager_module.os.lstat
        failed = False

        def flaky_lstat(path: Any, *arguments: Any, **options: Any) -> os.stat_result:
            nonlocal failed
            if Path(path) == target and not failed:
                failed = True
                raise OSError(errno.EIO, "transient filesystem failure")
            return real_lstat(path, *arguments, **options)

        monkeypatch.setattr(manager_module.os, "lstat", flaky_lstat)
        manager.tick()
        assert failed
        assert request_path.is_file()
        assert _state(workspace, job_id) == "ready"

        manager.tick()
        assert not request_path.exists()
        assert _state(workspace, job_id) == "cancelled"


def test_apply_io_failure_leaves_request_recoverable_for_next_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="request-apply-uncertain", pool="elsewhere")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        request_path = _post(workspace, job_id, "cancel")
        real_apply = _requests.apply
        failed = False

        def apply(*arguments: Any, **options: Any) -> object:
            nonlocal failed
            if not failed:
                failed = True
                raise OSError(errno.EIO, "state write reply was lost")
            return real_apply(*arguments, **options)

        monkeypatch.setattr(manager_module._requests, "apply", apply)
        manager.tick()
        assert failed
        # The job stays with this manager, its request unapplied, until the next pass heals it.
        assert request_path.is_file()
        assert _state(workspace, job_id) == "owned"

        manager.tick()
        assert not request_path.exists()
        assert _state(workspace, job_id) == "cancelled"


def test_owner_request_waits_for_owner_manager(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="request-deferred", pool="elsewhere")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        request_path = _post(workspace, job_id, "cancel")
        manager.uid += 1
        assert manager.tick() is False
        assert request_path.is_file()
        assert _state(workspace, job_id) == "ready"


# ---------------------------------------------------------------------------
# 7. Resources
# ---------------------------------------------------------------------------


def test_resource_capacity_blocks_never_fitting_jobs_and_census_reports_them(tmp_path: Path) -> None:
    for capacity in ({"procs": 1}, {}):
        name = "small" if capacity else "none"
        workspace = initialize_workspace(tmp_path / f"workspace-{name}")
        job_id = _job(workspace, tmp_path / name, _SUCCEED, tag="too-large", resources={"procs": 2})
        with TaskManager(workspace, resources=capacity, heartbeat_interval=0.01) as manager:
            manager.run_until_idle(timeout=5.0)
            census = manager._work_census()
        assert _state(workspace, job_id) == "ready"
        assert census.ready_blocked["resources"] == {"procs": 1}


def test_exhausted_procs_skip_the_ready_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, _SUCCEED, tag="exhausted")
    with TaskManager(workspace, resources={"procs": 1}, heartbeat_interval=0.01) as manager:
        scanned: list[str] = []
        real_list = _kernel.list_jobs

        def record_scan(workspace: Workspace, state: str, **options: Any) -> Iterator[_kernel.JobRef]:
            scanned.append(state)
            return real_list(workspace, state, **options)

        monkeypatch.setattr(_manager_scheduling._kernel, "list_jobs", record_scan)
        monkeypatch.setattr(manager, "_available_resources", lambda: {"procs": 0})
        manager.tick()

    assert "ready" not in scanned
    assert _state(workspace, job_id) == "ready"


def test_resource_packing_limits_concurrent_attempts(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    installed = _install(workspace, tmp_path / "package", "time.sleep(0.25)\n" + _SUCCEED)
    jobs = [_submit(workspace, installed, tag=f"packed{index}", resources={"procs": 2, "mem": 4}) for index in range(3)]
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
            if all(_state(workspace, job_id) == "succeeded" for job_id in jobs):
                break
            time.sleep(0.01)
        assert maximum_running <= 2
    assert all(_state(workspace, job_id) == "succeeded" for job_id in jobs)


def test_undeclared_resources_use_fair_share(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    installed = _install(workspace, tmp_path / "package", "time.sleep(0.2)\n" + _SUCCEED)
    jobs = [_submit(workspace, installed, tag=f"fair{index}") for index in range(3)]
    with TaskManager(workspace, resources={"procs": 4}, maximum_workers=2, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    records = [record for job_id in jobs for record in _attempt_records(workspace, job_id)]
    assert len(records) == 3
    assert all(record["resources"] == {"procs": 2} for record in records)


def test_dynamic_resources_apply_to_advance_and_clear_on_next_advance(tmp_path: Path) -> None:
    body = """
if context["step"] == "first":
    publish("advance", next_step="second", resources={"procs": 3})
elif context["step"] == "second":
    publish("advance", next_step="third")
else:
    publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="dynamic", initial_step="first")
    with TaskManager(
        workspace, resources={"procs": 4, "mem": 8}, maximum_workers=2, heartbeat_interval=0.01
    ) as manager:
        manager.run_until_idle(timeout=30.0)
    assert {record["step"]: record["resources"] for record in _attempt_records(workspace, job_id)} == {
        "first": {"procs": 2, "mem": 4},
        "second": {"procs": 3, "mem": 4},
        "third": {"procs": 2, "mem": 4},
    }


def test_wait_resources_apply_to_post_join_activation(tmp_path: Path) -> None:
    body = 'WAIT_MEMBERS = {"resources": {"procs": 3}}\n' + _SPAWNING
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="wait-resources")
    with TaskManager(
        workspace, resources={"procs": 4, "mem": 8}, maximum_workers=2, heartbeat_interval=0.01
    ) as manager:
        manager.run_until_idle(timeout=30.0)
    assert _state(workspace, job_id) == "succeeded"
    assert {record["step"]: record["resources"] for record in _attempt_records(workspace, job_id)} == {
        "only": {"procs": 2, "mem": 4},
        "gather": {"procs": 3, "mem": 4},
    }


def test_retry_keeps_dynamic_resource_requirement(tmp_path: Path) -> None:
    body = """
if context["step"] == "first":
    publish("advance", next_step="retry", resources={"procs": 3})
elif context["attempt_ordinal"] == 1:
    publish("fail", failure={"code": "temporary", "message": "try again", "retryable": True})
else:
    publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="retry-resources", initial_step="first")
    with TaskManager(workspace, resources={"procs": 4}, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    records = [record for record in _attempt_records(workspace, job_id) if record["step"] == "retry"]
    assert [record["resources"] for record in records] == [{"procs": 3}, {"procs": 3}]


def test_released_or_retried_claim_is_not_pinned_to_fair_share(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = """
if context["attempt_ordinal"] == 1:
    publish("fail", failure={"code": "temporary", "message": "try again", "retryable": True})
else:
    publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="unpinned")
    with TaskManager(workspace, resources={"procs": 4}, maximum_workers=2, heartbeat_interval=0.01) as manager:
        _drive_until_running(manager)
        # Stop claiming so the retry is left ready for the smaller manager.
        monkeypatch.setattr(_manager_scheduling, "claim_pass", lambda _manager: False)
        _drive_until(workspace, manager, job_id, {"ready"})
    between = _doc(workspace, job_id)
    assert between.attempt is not None and between.attempt["ordinal"] == 2 and between.attempt["started_at"] is None
    assert between.resources is None
    monkeypatch.undo()
    with TaskManager(workspace, resources={"procs": 1}, maximum_workers=1, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    assert _state(workspace, job_id) == "succeeded"
    assert [record["resources"] for record in _attempt_records(workspace, job_id)] == [{"procs": 2}, {"procs": 1}]
    assert _doc(workspace, job_id).resources is None


def test_time_resources_are_never_counted_against_capacity(tmp_path: Path) -> None:
    for capacity, resources in (({"procs": 1}, {"maxtime": 3600, "procs": 1}), ({}, {"maxtime": 60})):
        name = "procs" if capacity else "empty"
        workspace = initialize_workspace(tmp_path / f"workspace-{name}")
        job_id = _job(workspace, tmp_path / name, _SUCCEED, tag="timed", resources=resources)
        with TaskManager(workspace, resources=capacity, heartbeat_interval=0.01) as manager:
            manager.run_until_idle(timeout=30.0)
        assert _state(workspace, job_id) == "succeeded"
        assert [record["resources"] for record in _attempt_records(workspace, job_id)] == [resources]


def test_time_resources_resolve_per_label(tmp_path: Path) -> None:
    body = """
if context["step"] == "first":
    publish("advance", next_step="second")
elif context["step"] == "second":
    publish("advance", next_step="third", resources={"maxtime": 600, "mintime": 60, "procs": 1})
else:
    publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(
        workspace,
        tmp_path,
        body,
        tag="per-label",
        initial_step="first",
        resources={"maxtime": 3600},
        step_resources={"first": {"procs": 1}, "second": {"maxtime": 1200, "procs": 1}},
    )
    with TaskManager(workspace, resources={"procs": 2}, maximum_workers=1, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    assert {record["step"]: record["resources"] for record in _attempt_records(workspace, job_id)} == {
        "first": {"maxtime": 3600, "procs": 1},
        "second": {"maxtime": 1200, "procs": 1},
        "third": {"maxtime": 600, "mintime": 60, "procs": 1},
    }


def test_time_resources_are_not_manager_capacity(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with pytest.raises(ValueError, match="job requirement, not a manager capacity"):
        TaskManager(workspace, resources={"mintime": 1})


def test_a_time_only_mapping_leaves_the_consumable_selection_to_the_next_level(tmp_path: Path) -> None:
    body = """
following = {"first": ("second", {"maxtime": 120}), "second": ("third", {})}
if context["step"] in following:
    step, resources = following[context["step"]]
    publish("advance", next_step=step, resources=resources)
else:
    publish("succeed")
"""
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(
        workspace,
        tmp_path,
        body,
        tag="time-only",
        initial_step="first",
        resources={"procs": 2, "maxtime": 3600},
        step_resources={"first": {"maxtime": 600}},
    )
    with TaskManager(workspace, resources={"procs": 4}, maximum_workers=4, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    assert {record["step"]: record["resources"] for record in _attempt_records(workspace, job_id)} == {
        "first": {"procs": 2, "maxtime": 600},
        "second": {"procs": 2, "maxtime": 120},
        "third": {"procs": 1, "maxtime": 3600},
    }


def test_a_resolved_mintime_above_the_resolved_maxtime_is_lowered_to_it(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(
        workspace,
        tmp_path,
        _SUCCEED,
        tag="cross-level",
        resources={"maxtime": 3600},
        step_resources={"only": {"mintime": 7200}},
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    assert _state(workspace, job_id) == "succeeded"
    assert [record["resources"] for record in _attempt_records(workspace, job_id)] == [
        {"maxtime": 3600, "mintime": 3600}
    ]
